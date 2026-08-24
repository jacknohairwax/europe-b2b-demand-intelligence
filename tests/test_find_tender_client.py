import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError

from demand_intelligence.connectors.find_tender import (
    FIND_TENDER_RELEASE_PACKAGES_URL,
    FindTenderClient,
    FindTenderRequest,
)


def test_request_serializes_only_supported_official_filters() -> None:
    request = FindTenderRequest(
        limit=25,
        cursor="TWlsZXN0b25lMg==",
        updatedFrom=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        updatedTo=datetime(2026, 1, 31, 23, 59, 59, tzinfo=UTC),
        stage="tender",
    )

    assert request.as_query_params() == {
        "limit": 25,
        "cursor": "TWlsZXN0b25lMg==",
        "updatedFrom": "2026-01-01T00:00:00",
        "updatedTo": "2026-01-31T23:59:59",
        "stages": "tender",
    }


def test_request_preserves_opaque_cursor_characters() -> None:
    request = FindTenderRequest(cursor="server_owned-/+_==")

    assert request.as_query_params()["cursor"] == "server_owned-/+_=="


def test_request_rejects_invalid_ranges_and_unsupported_stage() -> None:
    with pytest.raises(ValidationError):
        FindTenderRequest(
            updated_from=datetime(2026, 2, 1),
            updated_to=datetime(2026, 1, 1),
        )
    with pytest.raises(ValidationError):
        FindTenderRequest(stage="contract")  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        FindTenderRequest(stages=["tender", "award"])  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_follows_links_next_and_writes_snapshots_and_success_state(
    tmp_path: Path,
) -> None:
    requested_urls: list[str] = []
    continuation = f"{FIND_TENDER_RELEASE_PACKAGES_URL}?limit=1&cursor=TmV4dA=="

    def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        assert request.headers["Accept"] == "application/json"
        if len(requested_urls) == 1:
            assert request.url.params["limit"] == "1"
            assert request.url.params["stages"] == "tender"
            assert request.url.params["updatedFrom"] == "2026-01-01T00:00:00"
            return httpx.Response(
                200,
                json={
                    "publishedDate": "2026-01-02T00:00:00Z",
                    "links": {"next": continuation},
                    "releases": [{"id": "000001-2026"}],
                },
            )
        assert str(request.url) == continuation
        return httpx.Response(
            200,
            json={
                "publishedDate": "2026-01-03T00:00:00Z",
                "releases": [{"id": "000002-2026"}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        async with FindTenderClient(
            client=http_client,
            snapshot_dir=tmp_path / "raw",
            state_file=tmp_path / "state.json",
        ) as client:
            pages = [
                page
                async for page in client.iter_pages(
                    FindTenderRequest(
                        limit=1,
                        stage="tender",
                        updated_from=datetime(2026, 1, 1),
                    )
                )
            ]

    assert len(requested_urls) == 2
    assert [page.package.releases[0]["id"] for page in pages] == [
        "000001-2026",
        "000002-2026",
    ]
    assert all(Path(page.snapshot.path).exists() for page in pages)
    assert all(page.snapshot.payload_sha256 for page in pages)
    assert pages[1].snapshot.source_url == continuation
    state = json.loads((tmp_path / "state.json").read_text())
    assert state["connector"] == "find-tender"
    assert state["last_release_count"] == 1
    assert state["last_published_date"] == "2026-01-03T00:00:00Z"
    assert state["next_url"] is None


@pytest.mark.asyncio
async def test_retries_documented_transient_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, text="Too Many Requests", headers={"Retry-After": "0"})
        return httpx.Response(200, json={"publishedDate": "", "releases": None})

    async def no_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("demand_intelligence.connectors.find_tender.asyncio.sleep", no_sleep)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        async with FindTenderClient(
            client=http_client,
            snapshot_dir=tmp_path / "raw",
            state_file=tmp_path / "state.json",
        ) as client:
            fetched = await client.fetch(FindTenderRequest())

    assert calls == 2
    assert fetched.package.releases == []
    assert Path(fetched.snapshot.path).exists()


@pytest.mark.asyncio
async def test_max_pages_bounds_cursor_pagination(tmp_path: Path) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={
                "links": {
                    "next": (
                        f"{FIND_TENDER_RELEASE_PACKAGES_URL}?limit=1&cursor=UGFnZTI="
                    )
                },
                "releases": [{"id": str(calls)}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        async with FindTenderClient(
            client=http_client,
            snapshot_dir=tmp_path / "raw",
            state_file=tmp_path / "state.json",
        ) as client:
            pages = [
                page
                async for page in client.iter_pages(FindTenderRequest(limit=1), max_pages=1)
            ]

    assert calls == 1
    assert len(pages) == 1
