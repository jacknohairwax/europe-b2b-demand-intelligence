"""Client for HMRC's public UK Trade Info OData trader activity feed."""

import asyncio
import hashlib
import json
import logging
import re
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from demand_intelligence.storage import SnapshotReference, SnapshotStore

LOGGER = logging.getLogger(__name__)
UKTI_TRADE_URL = "https://api.uktradeinfo.com/Trade"
UKTI_SOURCE_PAGE = "https://www.uktradeinfo.com/api-documentation/"

UKTI_SELECT = "TraderId,CommodityId,MonthId,TradeTypeId"
UKTI_EXPAND = (
    "Trader($select=TraderId,CompanyName,Address1,Address2,Address3,Address4,"
    "Address5,PostCode),"
    "Commodity($select=CommodityId,Cn8Code,Hs2Code,Hs4Code,Hs6Code,"
    "Hs2Description,Hs4Description,Hs6Description,Cn8LongDescription),"
    "TradeType($select=TradeTypeId,TradeTypeDescription)"
)


class UkTradeInfoRequest(BaseModel):
    hs_prefix: str
    year: int = Field(ge=2016, le=2100)
    trader_filter: Literal["All", "Importers", "Exporters"] = "Importers"
    # Omit $top by default so the service can provide @odata.nextLink.  An
    # explicit top is a total-result cap in OData, not a page-size request.
    top: int | None = Field(default=None, ge=1, le=40_000)

    @field_validator("hs_prefix")
    @classmethod
    def valid_hs_prefix(cls, value: str) -> str:
        compact = re.sub(r"\s+", "", value)
        if not re.fullmatch(r"\d{2,8}", compact):
            raise ValueError("hs_prefix must contain 2 to 8 digits")
        return compact

    def odata_params(self) -> dict[str, str]:
        clauses = [
            f"MonthId ge {self.year}01",
            f"MonthId le {self.year}12",
            f"startswith(Commodity/Cn8Code,'{self.hs_prefix}')",
        ]
        if self.trader_filter == "Importers":
            clauses.append("TradeTypeId eq 1")
        elif self.trader_filter == "Exporters":
            clauses.append("TradeTypeId eq 2")
        params = {
            "$filter": " and ".join(clauses),
            "$select": UKTI_SELECT,
            "$expand": UKTI_EXPAND,
        }
        if self.top is not None:
            params["$top"] = str(self.top)
        return params


class UkTradeInfoPage(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    records: list[dict[str, Any]] = Field(default_factory=list, alias="value")
    next_link: str | None = Field(default=None, alias="@odata.nextLink")

    @field_validator("records", mode="before")
    @classmethod
    def null_records_is_empty(cls, value: object) -> object:
        return [] if value is None else value


class FetchedUkTradeInfoPage(BaseModel):
    page: UkTradeInfoPage
    snapshot: SnapshotReference
    request_url: str


class UkTradeInfoClient:
    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        snapshot_dir: Path = Path("data/raw"),
        state_file: Path = Path("data/snapshots/uk-trade-info-state.json"),
        max_attempts: int = 4,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(60.0))
        self._owns_client = client is None
        self._snapshots = SnapshotStore(snapshot_dir)
        self._state_file = state_file
        self._max_attempts = max_attempts

    async def __aenter__(self) -> "UkTradeInfoClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def search(self, request: UkTradeInfoRequest) -> FetchedUkTradeInfoPage:
        return await self._fetch(UKTI_TRADE_URL, params=request.odata_params())

    async def iter_pages(
        self,
        request: UkTradeInfoRequest,
        *,
        max_pages: int | None = None,
    ) -> AsyncIterator[FetchedUkTradeInfoPage]:
        fetched = await self.search(request)
        page_number = 0
        seen_links: set[str] = set()
        while True:
            yield fetched
            page_number += 1
            if not fetched.page.next_link or (
                max_pages is not None and page_number >= max_pages
            ):
                return
            # The official API pre-populates all original parameters in nextLink.
            next_link = fetched.page.next_link
            if next_link in seen_links:
                LOGGER.warning("UK Trade Info returned a repeated nextLink; stopping")
                return
            seen_links.add(next_link)
            fetched = await self._fetch(next_link)

    async def _fetch(
        self, url: str, *, params: dict[str, str] | None = None
    ) -> FetchedUkTradeInfoPage:
        response: httpx.Response | None = None
        for attempt in range(self._max_attempts):
            try:
                response = await self._client.get(
                    url, params=params, headers={"Accept": "application/json"}
                )
                if response.status_code not in {429, 500, 502, 503, 504}:
                    response.raise_for_status()
                    break
                if attempt == self._max_attempts - 1:
                    response.raise_for_status()
                delay = _retry_delay(response, attempt)
                LOGGER.warning("UKTI transient status %s; retrying", response.status_code)
                await asyncio.sleep(delay)
            except (httpx.TimeoutException, httpx.NetworkError):
                if attempt == self._max_attempts - 1:
                    raise
                await asyncio.sleep(min(2**attempt, 30))
        if response is None:  # pragma: no cover - defensive invariant
            raise RuntimeError("UK Trade Info request completed without a response")

        payload = response.json()
        page = UkTradeInfoPage.model_validate(payload)
        request_url = str(response.request.url)
        page_id = hashlib.sha256(request_url.encode()).hexdigest()[:16]
        snapshot = self._snapshots.write_json(
            source="uk-trade-info",
            source_record_id=f"page-{page_id}",
            payload=payload,
            request={"url": request_url},
            source_url=request_url,
        )
        self._write_success_state(page, request_url)
        return FetchedUkTradeInfoPage(
            page=page, snapshot=snapshot, request_url=request_url
        )

    def _write_success_state(self, page: UkTradeInfoPage, request_url: str) -> None:
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        self._state_file.write_text(
            json.dumps(
                {
                    "connector": "uk-trade-info",
                    "last_success_at": datetime.now(UTC).isoformat(),
                    "last_page_record_count": len(page.records),
                    "last_request_url": request_url,
                },
                indent=2,
            )
            + "\n"
        )


def _retry_delay(response: httpx.Response, attempt: int) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return min(max(float(retry_after), 0.0), 120.0)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=UTC)
                return min(max((retry_at - datetime.now(UTC)).total_seconds(), 0.0), 120.0)
            except (TypeError, ValueError):
                pass
    if response.status_code == 429:
        return 60.0
    return float(min(2**attempt, 30))
