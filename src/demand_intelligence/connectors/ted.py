"""Resilient client for the public TED Search API v3."""

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from demand_intelligence.storage import SnapshotReference, SnapshotStore

LOGGER = logging.getLogger(__name__)
TED_SEARCH_URL = "https://api.ted.europa.eu/v3/notices/search"

# Kept in one drift-sensitive preset. `links` is returned implicitly by TED and
# is intentionally omitted because explicitly requesting it has produced empty responses.
TED_FIELDS = [
    "publication-number",
    "notice-identifier",
    "publication-date",
    "deadline",
    "buyer-name",
    "buyer-country",
    "buyer-city",
    "title-proc",
    "description-proc",
    "main-classification-proc",
    "estimated-value-proc",
    "estimated-value-cur-proc",
    "notice-type",
    "procedure-type",
]


class TedSearchRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    query: str
    fields: list[str] = Field(default_factory=lambda: list(TED_FIELDS))
    page: int = Field(default=1, ge=1)
    limit: int = Field(default=100, ge=1, le=250)
    scope: Literal["ALL", "ACTIVE", "LATEST"] = "ALL"
    checkQuerySyntax: bool = False
    paginationMode: Literal["PAGE_NUMBER", "ITERATION"] = "PAGE_NUMBER"
    iterationNextToken: str | None = None


class TedSearchPage(BaseModel):
    model_config = ConfigDict(extra="allow")

    totalNoticeCount: int = 0
    notices: list[dict[str, Any]] = Field(default_factory=list)
    iterationNextToken: str | None = None

    @field_validator("totalNoticeCount", mode="before")
    @classmethod
    def null_total_is_zero(cls, value: object) -> object:
        return 0 if value is None else value

    @field_validator("notices", mode="before")
    @classmethod
    def null_notices_is_empty(cls, value: object) -> object:
        return [] if value is None else value


class FetchedTedPage(BaseModel):
    page: TedSearchPage
    snapshot: SnapshotReference


class TedSearchClient:
    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        snapshot_dir: Path = Path("data/raw"),
        state_file: Path = Path("data/snapshots/ted-state.json"),
        max_attempts: int = 4,
    ) -> None:
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(45.0))
        self._owns_client = client is None
        self._snapshots = SnapshotStore(snapshot_dir)
        self._state_file = state_file
        self._max_attempts = max_attempts

    async def __aenter__(self) -> "TedSearchClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def search(self, request: TedSearchRequest) -> FetchedTedPage:
        body = request.model_dump(exclude_none=True)
        response: httpx.Response | None = None
        for attempt in range(self._max_attempts):
            try:
                response = await self._client.post(TED_SEARCH_URL, json=body)
                if response.status_code not in {429, 500, 502, 503, 504}:
                    response.raise_for_status()
                    break
                if attempt == self._max_attempts - 1:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else min(2**attempt, 30)
                LOGGER.warning("TED transient status %s; retrying", response.status_code)
                await asyncio.sleep(delay)
            except (httpx.TimeoutException, httpx.NetworkError):
                if attempt == self._max_attempts - 1:
                    raise
                await asyncio.sleep(min(2**attempt, 30))
        if response is None:  # pragma: no cover - defensive invariant
            raise RuntimeError("TED request completed without a response")

        payload = response.json()
        page = TedSearchPage.model_validate(payload)
        record_id = (
            f"page-{request.page}"
            if request.paginationMode == "PAGE_NUMBER"
            else f"iteration-{request.iterationNextToken or 'first'}"
        )
        snapshot = self._snapshots.write_json(
            source="ted",
            source_record_id=record_id,
            payload=payload,
            request=body,
            source_url=TED_SEARCH_URL,
        )
        self._write_success_state(page)
        return FetchedTedPage(page=page, snapshot=snapshot)

    async def iter_pages(
        self, request: TedSearchRequest, *, max_pages: int | None = None
    ) -> AsyncIterator[FetchedTedPage]:
        current = request.model_copy(deep=True)
        fetched_count = 0
        page_count = 0
        while max_pages is None or page_count < max_pages:
            fetched = await self.search(current)
            yield fetched
            page_count += 1
            fetched_count += len(fetched.page.notices)
            if not fetched.page.notices:
                break
            if current.paginationMode == "ITERATION":
                token = fetched.page.iterationNextToken
                if not token:
                    break
                current.iterationNextToken = token
            else:
                if fetched_count >= fetched.page.totalNoticeCount:
                    break
                current.page += 1

    def _write_success_state(self, page: TedSearchPage) -> None:
        now = datetime.now(UTC)
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        self._state_file.write_text(
            json.dumps(
                {
                    "connector": "ted",
                    "last_success_at": now.isoformat(),
                    "last_total_notice_count": page.totalNoticeCount,
                },
                indent=2,
            )
            + "\n"
        )
