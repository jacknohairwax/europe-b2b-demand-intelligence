import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import demand_intelligence.pipelines as pipeline_module
from demand_intelligence.connectors.uk_trade_info import UkTradeInfoPage
from demand_intelligence.pipelines import (
    finalize_uk_trade_info_run,
    process_uk_trade_info_page,
    rebuild_recurrent_accounts,
)
from demand_intelligence.reconciliation import (
    UK_TRADE_INFO_CONNECTOR,
    fail_scope_run,
    finish_scope_run,
    start_scope_run,
    uk_trade_info_scope_key,
)
from demand_intelligence.storage import Database, SnapshotStore

ROOT = Path(__file__).parents[1]


def _fixture() -> dict[str, object]:
    return json.loads((ROOT / "tests/fixtures/uk_trade_info_page.json").read_text())


def _snapshot(
    tmp_path: Path, payload: dict[str, object], name: str, observed_at: datetime
):
    snapshot = SnapshotStore(tmp_path / "raw").write_json(
        source="uk-trade-info-fixture",
        source_record_id=name,
        payload=payload,
        request=None,
        source_url=f"https://api.uktradeinfo.com/Trade?fixture={name}",
    )
    return snapshot.model_copy(update={"retrieved_at": observed_at})


def _start(database: Database, run_id: str, scope_key: str, at: datetime) -> str:
    return start_scope_run(
        database.connection,
        connector=UK_TRADE_INFO_CONNECTOR,
        scope_key=scope_key,
        scope_parameters={
            "year": 2025,
            "hs_prefix": "82",
            "trader_filter": "All",
            "minimum_months": 2,
        },
        run_id=run_id,
        started_at=at,
    )


def test_only_complete_scope_run_tombstones_and_rebuilds_recurrence(
    tmp_path: Path,
) -> None:
    payload = _fixture()
    records = payload["value"]
    assert isinstance(records, list)
    first_only = {**payload, "value": records[:1]}
    scope_key = uk_trade_info_scope_key(
        year=2025, hs_prefix="82", trader_filter="All"
    )
    first_at = datetime(2026, 1, 1, tzinfo=UTC)

    with Database(tmp_path / "current.duckdb") as database:
        run1 = _start(database, "run-complete-1", scope_key, first_at)
        assert process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(payload),
            _snapshot(tmp_path, payload, "complete-1", first_at),
            database,
            run_id=run1,
        )
        finish_scope_run(
            database.connection,
            run_id=run1,
            authoritative=True,
            completion_reason="source_exhausted",
            completed_at=first_at,
        )

        # A bounded run observes one row but has no authority to remove the rest.
        second_at = first_at + timedelta(days=1)
        partial = _start(database, "run-partial", scope_key, second_at)
        process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(first_only),
            _snapshot(tmp_path, first_only, "partial", second_at),
            database,
            run_id=partial,
        )
        partial_result = finish_scope_run(
            database.connection,
            run_id=partial,
            authoritative=False,
            completion_reason="max_pages",
            completed_at=second_at,
        )
        assert partial_result.removed_source_record_ids == ()
        assert database.connection.execute(
            "SELECT count(*) FROM trade_activity WHERE is_current"
        ).fetchone() == (3,)
        assert database.connection.execute(
            "SELECT status, is_authoritative FROM ingestion_run_scopes "
            "WHERE run_id = 'run-partial'"
        ).fetchone() == ("partial", False)

        # A failed run likewise carries no negative evidence.
        failed = _start(
            database, "run-failed", scope_key, second_at + timedelta(hours=1)
        )
        fail_scope_run(
            database.connection,
            run_id=failed,
            error="simulated transport failure",
            completed_at=second_at + timedelta(hours=1),
        )
        assert database.connection.execute(
            "SELECT count(*) FROM trade_activity WHERE is_current"
        ).fetchone() == (3,)

        # Natural exhaustion is an authoritative statement of scope membership.
        third_at = first_at + timedelta(days=2)
        run3 = _start(database, "run-complete-2", scope_key, third_at)
        process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(first_only),
            _snapshot(tmp_path, first_only, "complete-2", third_at),
            database,
            run_id=run3,
        )
        result = finish_scope_run(
            database.connection,
            run_id=run3,
            authoritative=True,
            completion_reason="source_exhausted",
            completed_at=third_at,
        )
        assert len(result.removed_source_record_ids) == 2
        rebuild_recurrent_accounts(
            database, set(result.affected_company_ids), minimum_months=2
        )

        assert database.connection.execute(
            "SELECT count(*) FROM trade_activity WHERE is_current"
        ).fetchone() == (1,)
        assert database.connection.execute(
            "SELECT count(*) FROM trade_activity WHERE NOT is_current"
        ).fetchone() == (2,)
        assert database.connection.execute(
            "SELECT count(*) FROM recurrent_accounts"
        ).fetchone() == (0,)
        assert database.connection.execute(
            "SELECT count(*) FROM recurrent_account_scopes"
        ).fetchone() == (0,)
        assert database.connection.execute(
            """SELECT observation_state, count(*)
               FROM ingestion_run_records WHERE run_id = ?
               GROUP BY observation_state ORDER BY observation_state""",
            [run3],
        ).fetchall() == [("removed", 2), ("unchanged", 1)]


def test_active_scope_lease_rejects_concurrent_run_and_is_released(
    tmp_path: Path,
) -> None:
    scope_key = uk_trade_info_scope_key(
        year=2025, hs_prefix="82", trader_filter="All"
    )
    at = datetime(2026, 5, 1, tzinfo=UTC)
    with Database(tmp_path / "leases.duckdb") as database:
        first = _start(database, "run-lease-first", scope_key, at)

        with pytest.raises(ValueError, match="active run"):
            _start(database, "run-lease-conflict", scope_key, at)

        assert database.connection.execute(
            "SELECT run_id FROM active_scope_leases"
        ).fetchone() == (first,)
        assert database.connection.execute(
            "SELECT run_id FROM ingestion_run_scopes ORDER BY run_id"
        ).fetchall() == [(first,)]

        fail_scope_run(
            database.connection,
            run_id=first,
            error="release fixture lease",
            completed_at=at,
        )
        second = _start(database, "run-lease-second", scope_key, at)
        finish_scope_run(
            database.connection,
            run_id=second,
            authoritative=False,
            completion_reason="max_pages",
            completed_at=at,
        )
        assert database.connection.execute(
            "SELECT count(*) FROM active_scope_leases"
        ).fetchone() == (0,)


def test_batched_page_retains_only_audited_positive_rows_before_invalid_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _fixture()
    invalid_payload = copy.deepcopy(payload)
    invalid_records = invalid_payload["value"]
    assert isinstance(invalid_records, list)
    invalid_records[1]["Trader"]["CompanyName"] = None
    at = datetime(2026, 5, 2, tzinfo=UTC)
    scope_key = uk_trade_info_scope_key(
        year=2025, hs_prefix="82", trader_filter="All"
    )
    monkeypatch.setattr(pipeline_module, "_UKTI_TRANSACTION_BATCH_SIZE", 1)

    with Database(tmp_path / "page-atomic.duckdb") as database:
        run_id = _start(database, "run-invalid-page", scope_key, at)
        with pytest.raises(ValueError, match="no trader company name"):
            process_uk_trade_info_page(
                UkTradeInfoPage.model_validate(invalid_payload),
                _snapshot(tmp_path, invalid_payload, "invalid-page", at),
                database,
                run_id=run_id,
            )

        for table in (
            "trade_activity",
            "evidence",
            "source_records",
            "canonical_companies",
            "ingestion_run_records",
            "source_scope_membership",
        ):
            assert database.connection.execute(
                f"SELECT count(*) FROM {table}"
            ).fetchone() == (1,)
        assert database.connection.execute(
            "SELECT pages_fetched, records_seen FROM ingestion_run_scopes"
        ).fetchone() == (0, 1)
        fail_scope_run(
            database.connection,
            run_id=run_id,
            error="invalid fixture row",
            completed_at=at,
        )


def test_large_source_page_uses_bounded_batches_but_counts_one_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _fixture()
    at = datetime(2026, 5, 2, 1, tzinfo=UTC)
    scope_key = uk_trade_info_scope_key(
        year=2025, hs_prefix="82", trader_filter="All"
    )
    monkeypatch.setattr(pipeline_module, "_UKTI_TRANSACTION_BATCH_SIZE", 2)

    with Database(tmp_path / "page-batches.duckdb") as database:
        run_id = _start(database, "run-batched-page", scope_key, at)
        accounts = process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(payload),
            _snapshot(tmp_path, payload, "batched-page", at),
            database,
            run_id=run_id,
        )

        assert len(accounts) == 1
        assert database.connection.execute(
            "SELECT pages_fetched, records_seen FROM ingestion_run_scopes"
        ).fetchone() == (1, 3)
        assert database.connection.execute(
            "SELECT count(*) FROM ingestion_run_records WHERE run_id = ?",
            [run_id],
        ).fetchone() == (3,)
        fail_scope_run(
            database.connection,
            run_id=run_id,
            error="end batched fixture",
            completed_at=at,
        )


def test_production_finalize_rolls_back_tombstones_and_recurrence_together(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _fixture()
    empty = {**payload, "value": []}
    at = datetime(2026, 5, 3, tzinfo=UTC)
    scope_key = uk_trade_info_scope_key(
        year=2025, hs_prefix="82", trader_filter="All"
    )

    with Database(tmp_path / "finalize-atomic.duckdb") as database:
        first = _start(database, "run-finalize-first", scope_key, at)
        process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(payload),
            _snapshot(tmp_path, payload, "finalize-first", at),
            database,
            run_id=first,
        )
        finalize_uk_trade_info_run(
            database,
            run_id=first,
            authoritative=True,
            completion_reason="source_exhausted",
            minimum_months=2,
            completed_at=at,
        )

        second_at = at + timedelta(days=1)
        second = _start(database, "run-finalize-failure", scope_key, second_at)
        process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(empty),
            _snapshot(tmp_path, empty, "finalize-empty", second_at),
            database,
            run_id=second,
        )

        def fail_rebuild(*_: object, **__: object) -> list[object]:
            raise RuntimeError("synthetic recurrence rebuild failure")

        monkeypatch.setattr(
            pipeline_module, "rebuild_recurrent_accounts", fail_rebuild
        )
        with pytest.raises(RuntimeError, match="synthetic recurrence"):
            finalize_uk_trade_info_run(
                database,
                run_id=second,
                authoritative=True,
                completion_reason="source_exhausted",
                minimum_months=2,
                completed_at=second_at,
            )

        assert database.connection.execute(
            "SELECT count(*) FROM trade_activity WHERE is_current"
        ).fetchone() == (3,)
        assert database.connection.execute(
            "SELECT count(*) FROM recurrent_accounts"
        ).fetchone() == (1,)
        assert database.connection.execute(
            "SELECT count(*) FROM source_scope_membership WHERE is_present"
        ).fetchone() == (3,)
        assert database.connection.execute(
            "SELECT status FROM ingestion_run_scopes WHERE run_id = ?", [second]
        ).fetchone() == ("failed",)
        assert database.connection.execute(
            "SELECT count(*) FROM active_scope_leases"
        ).fetchone() == (0,)


def test_correction_and_restoration_are_explicit_and_preserve_history(
    tmp_path: Path,
) -> None:
    payload = _fixture()
    records = payload["value"]
    assert isinstance(records, list)
    original = {**payload, "value": records[:1]}
    corrected = copy.deepcopy(original)
    corrected_records = corrected["value"]
    assert isinstance(corrected_records, list)
    corrected_records[0]["Trader"]["Address1"] = "Corrected address"
    empty = {**payload, "value": []}
    scope_key = uk_trade_info_scope_key(
        year=2025, hs_prefix="82", trader_filter="All"
    )
    first_at = datetime(2026, 2, 1, tzinfo=UTC)

    with Database(tmp_path / "correction.duckdb") as database:
        run1 = _start(database, "run-original", scope_key, first_at)
        process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(original),
            _snapshot(tmp_path, original, "original", first_at),
            database,
            run_id=run1,
        )
        finish_scope_run(
            database.connection,
            run_id=run1,
            authoritative=True,
            completion_reason="source_exhausted",
            completed_at=first_at,
        )

        corrected_at = first_at + timedelta(days=1)
        run2 = _start(database, "run-corrected", scope_key, corrected_at)
        process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(corrected),
            _snapshot(tmp_path, corrected, "corrected", corrected_at),
            database,
            run_id=run2,
        )
        finish_scope_run(
            database.connection,
            run_id=run2,
            authoritative=True,
            completion_reason="source_exhausted",
            completed_at=corrected_at,
        )
        membership = database.connection.execute(
            """SELECT current_payload_sha256, previous_payload_sha256,
                      last_change_type, is_present
               FROM source_scope_membership"""
        ).fetchone()
        assert membership is not None
        assert membership[0] != membership[1]
        assert membership[2:] == ("corrected", True)
        assert database.connection.execute(
            "SELECT observation_state FROM ingestion_run_records WHERE run_id = ?",
            [run2],
        ).fetchone() == ("corrected",)
        assert database.connection.execute(
            "SELECT count(*) FROM source_records"
        ).fetchone() == (2,)
        activity_state = database.connection.execute(
            """SELECT first_seen_run_id, last_seen_run_id, address_lines
               FROM trade_activity"""
        ).fetchone()
        assert activity_state is not None
        assert activity_state[:2] == ("run-original", "run-corrected")
        assert json.loads(activity_state[2])[0] == "Corrected address"

        removed_at = first_at + timedelta(days=2)
        run3 = _start(database, "run-removed", scope_key, removed_at)
        process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(empty),
            _snapshot(tmp_path, empty, "removed", removed_at),
            database,
            run_id=run3,
        )
        finish_scope_run(
            database.connection,
            run_id=run3,
            authoritative=True,
            completion_reason="source_exhausted",
            completed_at=removed_at,
        )
        assert database.connection.execute(
            "SELECT is_current FROM trade_activity"
        ).fetchone() == (False,)

        restored_at = first_at + timedelta(days=3)
        run4 = _start(database, "run-restored", scope_key, restored_at)
        process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(corrected),
            _snapshot(tmp_path, corrected, "restored", restored_at),
            database,
            run_id=run4,
        )
        finish_scope_run(
            database.connection,
            run_id=run4,
            authoritative=True,
            completion_reason="source_exhausted",
            completed_at=restored_at,
        )
        assert database.connection.execute(
            "SELECT last_change_type, is_present FROM source_scope_membership"
        ).fetchone() == ("restored", True)
        assert database.connection.execute(
            "SELECT is_current, first_seen_run_id FROM trade_activity"
        ).fetchone() == (True, "run-original")


def test_overlapping_scope_removal_does_not_hide_present_activity(
    tmp_path: Path,
) -> None:
    payload = _fixture()
    records = payload["value"]
    assert isinstance(records, list)
    one_record = {**payload, "value": records[:1]}
    empty = {**payload, "value": []}
    at = datetime(2026, 3, 1, tzinfo=UTC)

    with Database(tmp_path / "overlap.duckdb") as database:
        for index, prefix in enumerate(("82", "8201"), start=1):
            scope_key = uk_trade_info_scope_key(
                year=2025, hs_prefix=prefix, trader_filter="All"
            )
            run_id = _start(database, f"run-scope-{index}", scope_key, at)
            process_uk_trade_info_page(
                UkTradeInfoPage.model_validate(one_record),
                _snapshot(tmp_path, one_record, f"scope-{index}", at),
                database,
                run_id=run_id,
            )
            finish_scope_run(
                database.connection,
                run_id=run_id,
                authoritative=True,
                completion_reason="source_exhausted",
                completed_at=at,
            )

        narrow_key = uk_trade_info_scope_key(
            year=2025, hs_prefix="8201", trader_filter="All"
        )
        removal_at = at + timedelta(days=1)
        removal_run = _start(
            database, "run-narrow-removal", narrow_key, removal_at
        )
        process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(empty),
            _snapshot(tmp_path, empty, "narrow-removal", removal_at),
            database,
            run_id=removal_run,
        )
        finish_scope_run(
            database.connection,
            run_id=removal_run,
            authoritative=True,
            completion_reason="source_exhausted",
            completed_at=removal_at,
        )

        assert database.connection.execute(
            """SELECT is_present, count(*) FROM source_scope_membership
               GROUP BY is_present ORDER BY is_present"""
        ).fetchall() == [(False, 1), (True, 1)]
        assert database.connection.execute(
            "SELECT is_current FROM trade_activity"
        ).fetchone() == (True,)


def test_first_tracked_complete_run_reconciles_legacy_activity(tmp_path: Path) -> None:
    payload = _fixture()
    empty = {**payload, "value": []}
    at = datetime(2026, 4, 1, tzinfo=UTC)
    snapshot = _snapshot(tmp_path, payload, "legacy", at)
    scope_key = uk_trade_info_scope_key(
        year=2025, hs_prefix="82", trader_filter="All"
    )

    with Database(tmp_path / "legacy.duckdb") as database:
        # Phase-2 rows had no run or scope metadata.
        process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(payload), snapshot, database
        )
        assert database.connection.execute(
            "SELECT count(*) FROM source_scope_membership"
        ).fetchone() == (0,)

        run_id = _start(database, "run-first-tracked", scope_key, at)
        assert database.connection.execute(
            "SELECT count(*) FROM source_scope_membership"
        ).fetchone() == (3,)
        process_uk_trade_info_page(
            UkTradeInfoPage.model_validate(empty),
            _snapshot(tmp_path, empty, "first-tracked-empty", at),
            database,
            run_id=run_id,
        )
        result = finish_scope_run(
            database.connection,
            run_id=run_id,
            authoritative=True,
            completion_reason="source_exhausted",
            completed_at=at,
        )
        rebuild_recurrent_accounts(
            database, set(result.affected_company_ids), minimum_months=2
        )

        assert len(result.removed_source_record_ids) == 3
        assert database.connection.execute(
            "SELECT count(*) FROM trade_activity WHERE is_current"
        ).fetchone() == (0,)
        assert database.connection.execute(
            "SELECT count(*) FROM recurrent_accounts"
        ).fetchone() == (0,)
