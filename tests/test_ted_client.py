from pathlib import Path

import httpx
import pytest

from demand_intelligence.connectors import TedSearchClient, TedSearchRequest
from demand_intelligence.connectors.ted import TED_FIELDS


def test_default_field_preset_does_not_request_implicit_links_field() -> None:
    # TED includes links automatically; explicitly requesting it can yield an
    # empty or non-JSON response despite an otherwise valid query.
    assert "links" not in TED_FIELDS
    assert TedSearchRequest(query="publication-date >= 20250101").checkQuerySyntax is False


@pytest.mark.asyncio
async def test_page_number_pagination_and_snapshot(tmp_path: Path) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        body = {"totalNoticeCount": 2, "notices": [{"publication-number": str(calls)}]}
        return httpx.Response(200, json=body)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        async with TedSearchClient(
            client=http_client,
            snapshot_dir=tmp_path / "raw",
            state_file=tmp_path / "state.json",
        ) as client:
            pages = [
                page
                async for page in client.iter_pages(
                    TedSearchRequest(query="publication-date >= 20260101", limit=1),
                    max_pages=None,
                )
            ]

    assert calls == 2
    assert len(pages) == 2
    assert Path(pages[0].snapshot.path).exists()
    assert (tmp_path / "state.json").exists()


@pytest.mark.asyncio
async def test_retries_transient_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, text="temporarily unavailable", headers={"Retry-After": "0"})
        return httpx.Response(200, json={"totalNoticeCount": 0, "notices": []})

    async def no_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("demand_intelligence.connectors.ted.asyncio.sleep", no_sleep)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        async with TedSearchClient(
            client=http_client,
            snapshot_dir=tmp_path / "raw",
            state_file=tmp_path / "state.json",
        ) as client:
            await client.search(TedSearchRequest(query="publication-date >= 20260101"))

    assert calls == 2


@pytest.mark.asyncio
async def test_live_api_null_empty_envelope_is_normalized(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"totalNoticeCount": None, "notices": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        async with TedSearchClient(
            client=http_client,
            snapshot_dir=tmp_path / "raw",
            state_file=tmp_path / "state.json",
        ) as client:
            fetched = await client.search(
                TedSearchRequest(query="publication-number = 000000-1900")
            )

    assert fetched.page.totalNoticeCount == 0
    assert fetched.page.notices == []
    assert Path(fetched.snapshot.path).exists()
