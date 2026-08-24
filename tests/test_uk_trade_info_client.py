from pathlib import Path

import httpx
import pytest

from demand_intelligence.connectors.uk_trade_info import (
    UKTI_TRADE_URL,
    UkTradeInfoClient,
    UkTradeInfoRequest,
)


def test_request_builds_bounded_role_and_product_filter() -> None:
    params = UkTradeInfoRequest(
        hs_prefix="82 03", year=2025, trader_filter="Importers"
    ).odata_params()

    assert "startswith(Commodity/Cn8Code,'8203')" in params["$filter"]
    assert "TradeTypeId eq 1" in params["$filter"]
    assert "MonthId ge 202501" in params["$filter"]
    assert "$top" not in params

    bounded = UkTradeInfoRequest(hs_prefix="8203", year=2025, top=25).odata_params()
    assert bounded["$top"] == "25"


@pytest.mark.asyncio
async def test_follows_official_next_link_verbatim_and_snapshots(tmp_path: Path) -> None:
    next_url = f"{UKTI_TRADE_URL}?$skip=40000&opaque=server-owned"
    requested_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        if len(requested_urls) == 1:
            return httpx.Response(
                200, json={"value": [{"TraderId": 1}], "@odata.nextLink": next_url}
            )
        return httpx.Response(200, json={"value": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        async with UkTradeInfoClient(
            client=http_client,
            snapshot_dir=tmp_path / "raw",
            state_file=tmp_path / "state.json",
        ) as client:
            pages = [
                page
                async for page in client.iter_pages(
                    UkTradeInfoRequest(hs_prefix="8203", year=2025)
                )
            ]

    assert len(pages) == 2
    assert requested_urls[1] == next_url
    assert all(Path(page.snapshot.path).exists() for page in pages)
    assert pages[0].snapshot.payload_sha256 is not None
    assert (tmp_path / "state.json").exists()


@pytest.mark.asyncio
async def test_retries_rate_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, text="rate limited", headers={"Retry-After": "0"})
        return httpx.Response(200, json={"value": []})

    async def no_sleep(_: float) -> None:
        return None

    monkeypatch.setattr(
        "demand_intelligence.connectors.uk_trade_info.asyncio.sleep", no_sleep
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        async with UkTradeInfoClient(
            client=http_client,
            snapshot_dir=tmp_path / "raw",
            state_file=tmp_path / "state.json",
        ) as client:
            await client.search(UkTradeInfoRequest(hs_prefix="8203", year=2025))

    assert calls == 2
