"""Resilient client for the public Find a Tender OCDS release-package API."""

import asyncio
import hashlib
import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Self

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from demand_intelligence.storage import SnapshotReference, SnapshotStore

LOGGER = logging.getLogger(__name__)
FIND_TENDER_RELEASE_PACKAGES_URL = (
    "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages"
)
TRANSIENT_STATUSES = {429, 500, 502, 503, 504}

FindTenderStage = Literal["planning", "tender", "award"]


class FindTenderRequest(BaseModel):
    """Supported filters for the anonymous OCDS release-package endpoint."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    limit: int = Field(default=100, ge=1, le=100)
    # The service defines the cursor as opaque.  Only enforce its documented
    # length; do not assume a Base64 alphabet or attempt to decode it.
    cursor: str | None = Field(default=None, max_length=300)
    updated_from: datetime | None = Field(default=None, alias="updatedFrom")
    updated_to: datetime | None = Field(default=None, alias="updatedTo")
    stage: FindTenderStage | None = Field(default=None, alias="stages")

    @model_validator(mode="after")
    def dates_are_ordered(self) -> Self:
        if (
            self.updated_from is not None
            and self.updated_to is not None
            and _api_datetime(self.updated_from) > _api_datetime(self.updated_to)
        ):
            raise ValueError("updated_from must not be later than updated_to")
        return self

    def as_query_params(self) -> dict[str, str | int]:
        params: dict[str, str | int] = {"limit": self.limit}
        if self.cursor:
            params["cursor"] = self.cursor
        if self.updated_from is not None:
            params["updatedFrom"] = _api_datetime(self.updated_from)
        if self.updated_to is not None:
            params["updatedTo"] = _api_datetime(self.updated_to)
        if self.stage:
            params["stages"] = self.stage
        return params


def _api_datetime(value: datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.isoformat(timespec="seconds")


class FindTenderLinks(BaseModel):
    model_config = ConfigDict(extra="allow")

    next: str | None = None


class FindTenderReleasePackage(BaseModel):
    """The envelope fields needed for ingestion; release content remains lossless."""

    model_config = ConfigDict(extra="allow")

    uri: str | None = None
    version: str | None = None
    publishedDate: str | None = None
    links: FindTenderLinks | None = None
    releases: list[dict[str, Any]] = Field(default_factory=list)

    @field_validator("releases", mode="before")
    @classmethod
    def null_releases_are_empty(cls, value: object) -> object:
        return [] if value is None else value


class FetchedFindTenderPage(BaseModel):
    package: FindTenderReleasePackage
    snapshot: SnapshotReference


class FindTenderClient:
    """Fetch release packages with bounded retries and immutable provenance."""

    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        snapshot_dir: Path = Path("data/raw"),
        state_file: Path = Path("data/snapshots/find-tender-state.json"),
        max_attempts: int = 4,
        base_url: str = FIND_TENDER_RELEASE_PACKAGES_URL,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(45.0))
        self._owns_client = client is None
        self._snapshots = SnapshotStore(snapshot_dir)
        self._state_file = state_file
        self._max_attempts = max_attempts
        self._base_url = base_url

    async def __aenter__(self) -> "FindTenderClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def fetch(
        self, request: FindTenderRequest, *, continuation_url: str | None = None
    ) -> FetchedFindTenderPage:
        """Fetch one page, optionally using the service's exact continuation URL."""

        params = None if continuation_url else request.as_query_params()
        request_url = continuation_url or self._base_url
        response = await self._get_with_retries(request_url, params=params)
        payload_object = response.json()
        if not isinstance(payload_object, dict):
            raise ValueError("Find a Tender returned a non-object JSON response")
        payload: dict[str, Any] = payload_object
        package = FindTenderReleasePackage.model_validate(payload)
        effective_url = str(response.request.url)
        request_record: dict[str, Any] = {
            "method": "GET",
            "url": effective_url,
            "params": params,
        }
        page_digest = hashlib.sha256(effective_url.encode()).hexdigest()[:20]
        snapshot = self._snapshots.write_json(
            source="find-tender",
            source_record_id=f"page-{page_digest}",
            payload=payload,
            request=request_record,
            source_url=effective_url,
        )
        self._write_success_state(package)
        return FetchedFindTenderPage(package=package, snapshot=snapshot)

    async def iter_pages(
        self, request: FindTenderRequest, *, max_pages: int | None = None
    ) -> AsyncIterator[FetchedFindTenderPage]:
        continuation_url: str | None = None
        seen_urls: set[str] = set()
        page_count = 0
        while max_pages is None or page_count < max_pages:
            fetched = await self.fetch(request, continuation_url=continuation_url)
            yield fetched
            page_count += 1
            next_url = fetched.package.links.next if fetched.package.links else None
            if not next_url:
                break
            if next_url in seen_urls:
                LOGGER.warning("Find a Tender returned a repeated continuation URL; stopping")
                break
            seen_urls.add(next_url)
            continuation_url = next_url

    async def _get_with_retries(
        self, url: str, *, params: dict[str, str | int] | None
    ) -> httpx.Response:
        response: httpx.Response | None = None
        for attempt in range(self._max_attempts):
            try:
                response = await self._client.get(
                    url, params=params, headers={"Accept": "application/json"}
                )
                if response.status_code not in TRANSIENT_STATUSES:
                    response.raise_for_status()
                    return response
                if attempt == self._max_attempts - 1:
                    response.raise_for_status()
                delay = _retry_delay(response, attempt)
                LOGGER.warning(
                    "Find a Tender transient status %s; retrying", response.status_code
                )
                await asyncio.sleep(delay)
            except (httpx.TimeoutException, httpx.NetworkError):
                if attempt == self._max_attempts - 1:
                    raise
                await asyncio.sleep(min(2**attempt, 30))
        if response is None:  # pragma: no cover - defensive invariant
            raise RuntimeError("Find a Tender request completed without a response")
        response.raise_for_status()  # pragma: no cover - loop raises on final transient
        return response

    def _write_success_state(self, package: FindTenderReleasePackage) -> None:
        now = datetime.now(UTC)
        next_url = package.links.next if package.links else None
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        self._state_file.write_text(
            json.dumps(
                {
                    "connector": "find-tender",
                    "last_success_at": now.isoformat(),
                    "last_published_date": package.publishedDate or None,
                    "last_release_count": len(package.releases),
                    "next_url": next_url,
                },
                indent=2,
            )
            + "\n"
        )


def _retry_delay(response: httpx.Response, attempt: int) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return min(max(float(retry_after), 0.0), 30.0)
        except ValueError:
            pass
    return float(min(2**attempt, 30))


__all__ = [
    "FIND_TENDER_RELEASE_PACKAGES_URL",
    "FetchedFindTenderPage",
    "FindTenderClient",
    "FindTenderLinks",
    "FindTenderReleasePackage",
    "FindTenderRequest",
    "FindTenderStage",
]
