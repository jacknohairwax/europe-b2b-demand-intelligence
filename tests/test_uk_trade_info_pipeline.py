import json
from datetime import UTC, datetime
from pathlib import Path

from demand_intelligence.connectors.uk_trade_info import UkTradeInfoPage
from demand_intelligence.models import TradeRole
from demand_intelligence.normalize.uk_trade_info import normalize_uk_trade_info_record
from demand_intelligence.pipelines import process_uk_trade_info_page
from demand_intelligence.storage import Database, SnapshotReference, SnapshotStore

ROOT = Path(__file__).parents[1]


def test_repeated_months_create_a_conservative_recurrent_account(tmp_path: Path) -> None:
    payload = json.loads((ROOT / "tests/fixtures/uk_trade_info_page.json").read_text())
    snapshot = SnapshotStore(tmp_path / "raw").write_json(
        source="uk-trade-info-fixture",
        source_record_id="page-1",
        payload=payload,
        request=None,
        source_url="https://api.uktradeinfo.com/Trade?fixture=true",
    )
    database = Database(tmp_path / "demand.duckdb")
    try:
        database.initialize()
        first = process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(payload), snapshot, database
        )
        second = process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(payload), snapshot, database
        )
        database.connection.execute(
            """UPDATE recurrent_accounts
               SET procurement_evidence = '["ev-fts-linked"]',
                   buyer_role = 'Find a Tender supplier'"""
        )
        third = process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(payload), snapshot, database
        )

        assert len(first) == len(second) == len(third) == 1
        account = first[0]
        assert account.country == "Not verified"
        assert account.months_active == 2
        assert account.company_type == "Importer"
        assert account.activity_year == 2025
        assert account.recurrent_product_roles == ["2025:CN8:82011000:Importer"]
        assert account.china_sourcing_evidence == []
        assert account.procurement_evidence == []
        assert account.buyer_role == "Not verified"
        persisted_link = database.connection.execute(
            "SELECT procurement_evidence, buyer_role FROM recurrent_accounts"
        ).fetchone()
        assert persisted_link == ('["ev-fts-linked"]', "Find a Tender supplier")
        assert database.connection.execute("SELECT count(*) FROM trade_activity").fetchone()[0] == 3
        assert database.connection.execute("SELECT count(*) FROM evidence").fetchone()[0] == 3
        recurrent_count = database.connection.execute(
            "SELECT count(*) FROM recurrent_accounts"
        ).fetchone()
        assert recurrent_count is not None and recurrent_count[0] == 1
        values = database.connection.execute(
            "SELECT value, quantity, partner_country FROM trade_activity"
        ).fetchall()
        assert all(row == (None, None, None) for row in values)
    finally:
        database.close()


def test_row_identity_is_stable_across_different_page_envelopes() -> None:
    payload = json.loads((ROOT / "tests/fixtures/uk_trade_info_page.json").read_text())
    raw = payload["value"][0]
    first_snapshot = SnapshotReference(
        source="uk-trade-info",
        source_record_id="page-a",
        source_url="https://api.uktradeinfo.com/Trade?page=a",
        retrieved_at=datetime(2026, 1, 1, tzinfo=UTC),
        content_sha256="a" * 64,
        payload_sha256="b" * 64,
        path="page-a.json",
    )
    second_snapshot = first_snapshot.model_copy(
        update={
            "source_record_id": "page-b",
            "source_url": "https://api.uktradeinfo.com/Trade?page=b",
            "content_sha256": "c" * 64,
            "payload_sha256": "d" * 64,
            "path": "page-b.json",
        }
    )

    first_activity, first_evidence, _ = normalize_uk_trade_info_record(
        raw, first_snapshot
    )
    second_activity, second_evidence, _ = normalize_uk_trade_info_record(
        raw, second_snapshot
    )

    assert first_activity.payload_sha256 == second_activity.payload_sha256
    assert first_evidence.evidence_id == second_evidence.evidence_id


def test_unknown_trade_type_remains_explicitly_unresolved() -> None:
    payload = json.loads((ROOT / "tests/fixtures/uk_trade_info_page.json").read_text())
    raw = {**payload["value"][0], "TradeTypeId": 0}
    snapshot = SnapshotReference(
        source="uk-trade-info",
        source_record_id="page-unresolved",
        source_url="https://api.uktradeinfo.com/Trade?fixture=unresolved",
        retrieved_at=datetime(2026, 1, 1, tzinfo=UTC),
        content_sha256="a" * 64,
        payload_sha256="b" * 64,
        path="page-unresolved.json",
    )

    activity, evidence, _ = normalize_uk_trade_info_record(raw, snapshot)

    assert activity.trade_role is TradeRole.UNRESOLVED
    assert evidence.inference is None


def test_disclosure_level_hs2_is_not_fabricated_as_cn8() -> None:
    payload = json.loads((ROOT / "tests/fixtures/uk_trade_info_page.json").read_text())
    raw = {
        **payload["value"][0],
        "CommodityId": 82,
        "Commodity": {
            "CommodityId": 82,
            "Cn8Code": "82",
            "Hs2Code": "82",
            "Hs4Code": "-",
            "Hs6Code": "-",
            "Cn8LongDescription": "-",
            "Hs2Description": "Base metals and tools",
        },
    }
    snapshot = SnapshotReference(
        source="uk-trade-info",
        source_record_id="page-hs2",
        source_url="https://api.uktradeinfo.com/Trade?fixture=hs2",
        retrieved_at=datetime(2026, 1, 1, tzinfo=UTC),
        content_sha256="a" * 64,
        payload_sha256="b" * 64,
        path="page-hs2.json",
    )

    activity, evidence, _ = normalize_uk_trade_info_record(raw, snapshot)

    assert activity.commodity_code == "82"
    assert activity.commodity_code_level == "HS2"
    assert activity.product_description == "Base metals and tools"
    assert "HS2 82" in evidence.observed_fact
    assert "CN8 00000082" not in evidence.observed_fact


def test_stricter_recurrence_threshold_removes_stale_account(tmp_path: Path) -> None:
    payload = json.loads((ROOT / "tests/fixtures/uk_trade_info_page.json").read_text())
    snapshot = SnapshotStore(tmp_path / "raw").write_json(
        source="uk-trade-info-fixture",
        source_record_id="page-threshold",
        payload=payload,
        request=None,
        source_url="https://api.uktradeinfo.com/Trade?fixture=threshold",
    )
    with Database(tmp_path / "threshold.duckdb") as database:
        assert process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(payload),
            snapshot,
            database,
            minimum_months=2,
        )
        assert process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(payload),
            snapshot,
            database,
            minimum_months=3,
        ) == []
        assert database.connection.execute(
            "SELECT count(*) FROM recurrent_accounts"
        ).fetchone() == (0,)
        assert database.connection.execute(
            "SELECT count(*) FROM recurrent_account_scopes"
        ).fetchone() == (0,)


def test_multi_year_scopes_roll_up_without_overwriting_company(tmp_path: Path) -> None:
    payload_2025 = json.loads(
        (ROOT / "tests/fixtures/uk_trade_info_page.json").read_text()
    )
    payload_2024 = json.loads(json.dumps(payload_2025))
    for row in payload_2024["value"]:
        row["MonthId"] -= 100
    snapshot = SnapshotStore(tmp_path / "raw").write_json(
        source="uk-trade-info-fixture",
        source_record_id="multi-year",
        payload={"2024": payload_2024, "2025": payload_2025},
        request=None,
        source_url="https://api.uktradeinfo.com/Trade?fixture=multi-year",
    )

    with Database(tmp_path / "multi-year.duckdb") as database:
        assert process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(payload_2025), snapshot, database
        )
        assert process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(payload_2024), snapshot, database
        )

        account = database.connection.execute(
            """SELECT activity_year, recurrent_product_roles
               FROM recurrent_accounts"""
        ).fetchone()
        assert account is not None
        assert account[0] is None
        assert json.loads(account[1]) == [
            "2024:CN8:82011000:Importer",
            "2025:CN8:82011000:Importer",
        ]
        scopes = database.connection.execute(
            """SELECT activity_year, commodity_code, trade_role
               FROM recurrent_account_scopes ORDER BY activity_year"""
        ).fetchall()
        assert scopes == [
            (2024, "82011000", "Importer"),
            (2025, "82011000", "Importer"),
        ]


def test_unrelated_same_year_recurrence_is_not_returned_for_affected_group(
    tmp_path: Path,
) -> None:
    fixture = json.loads((ROOT / "tests/fixtures/uk_trade_info_page.json").read_text())
    recurring_payload = json.loads(json.dumps(fixture))
    recurring_payload["value"] = recurring_payload["value"][:2]
    for row in recurring_payload["value"]:
        row["CommodityId"] = 83021000
        row["Commodity"]["CommodityId"] = 83021000
        row["Commodity"]["Cn8Code"] = "83021000"
        row["Commodity"]["Cn8LongDescription"] = "Hinges of base metal"

    selected_payload = json.loads(json.dumps(fixture))
    selected_payload["value"] = selected_payload["value"][:1]
    selected_row = selected_payload["value"][0]
    selected_row["CommodityId"] = 82031000
    selected_row["Commodity"]["CommodityId"] = 82031000
    selected_row["Commodity"]["Cn8Code"] = "82031000"
    selected_row["Commodity"]["Cn8LongDescription"] = "Files and rasps"

    snapshot = SnapshotStore(tmp_path / "raw").write_json(
        source="uk-trade-info-fixture",
        source_record_id="affected-group",
        payload={"recurring": recurring_payload, "selected": selected_payload},
        request=None,
        source_url="https://api.uktradeinfo.com/Trade?fixture=affected-group",
    )
    with Database(tmp_path / "affected-group.duckdb") as database:
        assert process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(recurring_payload), snapshot, database
        )

        current_results = process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(selected_payload), snapshot, database
        )

        assert current_results == []
        assert database.connection.execute(
            "SELECT commodity_code, trade_role FROM recurrent_account_scopes"
        ).fetchall() == [("83021000", "Importer")]
        assert database.connection.execute(
            "SELECT recurrent_product_roles FROM recurrent_accounts"
        ).fetchone() == ('["2025:CN8:83021000:Importer"]',)
