import json
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import demand_intelligence.changes as changes_module
from demand_intelligence.changes import ChangeEvent, detect_material_changes
from demand_intelligence.storage import Database

SOURCE = "Find a Tender OCDS API"
OBSERVED = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)


def _detect(
    database: Database,
    run_id: str,
    *,
    at: datetime = OBSERVED,
    authoritative: bool = False,
    connector: str = "test orchestration",
    scope_key: str = "test-scope",
) -> list[ChangeEvent]:
    return detect_material_changes(
        database.connection,
        run_id=run_id,
        connector=connector,
        scope_key=scope_key,
        detected_at=at,
        completed_authoritative=authoritative,
        recurrence_threshold=3,
    )


def _insert_release(
    database: Database,
    *,
    release_id: str = "012345-2026",
    stages: list[str] | None = None,
    status: str = "active",
    deadline: datetime | None = None,
    value: Decimal = Decimal("100"),
    ocid: str | None = None,
    observed_at: datetime = OBSERVED,
) -> None:
    database.connection.execute(
        """INSERT INTO procurement_releases (
             source_name, release_id, ocid, release_payload_sha256,
             retrieved_at, publication_date, stages, tender_status, deadline,
             value, currency, title, buyer_id, buyer_name, cpv_codes,
             product_category, evidence_ids, source_url, is_current
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)""",
        [
            SOURCE,
            release_id,
            ocid or f"ocds-test-{release_id}",
            (release_id.encode().hex() + "0" * 64)[:64],
            observed_at,
            observed_at,
            json.dumps(stages or ["tender"]),
            status,
            deadline or datetime(2026, 9, 1, tzinfo=UTC),
            value,
            "gbp",
            "Supply of hand tools",
            "buyer-1",
            "Tools Buyer Ltd",
            json.dumps(["44511000", "44510000"]),
            "Hand tools",
            json.dumps([f"ev-{release_id}"]),
            f"https://example.test/{release_id}",
        ],
    )


def _event_types(events: list[ChangeEvent]) -> list[str]:
    return sorted(event.event_type for event in events)


def _insert_recurrent_account(database: Database, *, months_active: int) -> None:
    database.connection.execute(
        """INSERT INTO recurrent_accounts (
             company_id, company_name, country, company_type, products,
             relevant_hs_codes, procurement_evidence, trade_evidence,
             import_evidence, china_sourcing_evidence, months_active,
             estimated_scale, buyer_role, contact_status, buying_signals,
             evidence_strength, confidence, next_action, activity_year,
             recurrent_product_roles
           ) VALUES (
             'company-threshold', 'Threshold Tools Ltd', 'Not verified',
             'Importer', '[\"Hand tools\"]', '[\"82011000\"]', '[]',
             '[\"ev-trade-1\", \"ev-trade-2\"]',
             '[\"ev-trade-1\", \"ev-trade-2\"]', '[]', ?, 'Observed months',
             'Not verified', 'Not verified', '[]', 'Tier 1', 0.9, 'Review',
             2026, '[\"2026:CN8:82011000:Importer\"]'
           )""",
        [months_active],
    )


def _insert_recurrence_scope(database: Database, months: list[int]) -> None:
    database.connection.execute(
        """INSERT INTO recurrent_account_scopes VALUES (
             'company-threshold', 2026, '82011000', 'CN8', 'Importer', ?, ?
           )""",
        [json.dumps(months), json.dumps([f"ev-trade-{month}" for month in months])],
    )


def test_tender_corrections_close_and_equivalent_values_are_silent(
    tmp_path: Path,
) -> None:
    with Database(tmp_path / "changes.duckdb") as database:
        _insert_release(database)

        assert _event_types(_detect(database, "run-1")) == ["new_tender"]
        assert _detect(database, "run-2") == []

        # DuckDB normalizes equivalent instants, and Decimal scale is immaterial.
        equivalent_deadline = datetime(
            2026, 9, 1, 1, 0, tzinfo=timezone(timedelta(hours=1))
        )
        database.connection.execute(
            """UPDATE procurement_releases
               SET deadline = ?, value = ? WHERE release_id = '012345-2026'""",
            [equivalent_deadline, Decimal("100.000000")],
        )
        assert _detect(database, "run-3") == []

        database.connection.execute(
            """UPDATE procurement_releases
               SET deadline = ?, value = ? WHERE release_id = '012345-2026'""",
            [datetime(2026, 10, 1, tzinfo=UTC), Decimal("125")],
        )
        corrected = _detect(database, "run-4")
        assert _event_types(corrected) == ["deadline_changed", "value_changed"]
        assert {event.field_name for event in corrected} == {
            "deadline",
            "tender_value",
        }

        database.connection.execute(
            """UPDATE procurement_releases SET tender_status = 'complete'
               WHERE release_id = '012345-2026'"""
        )
        closed = _detect(database, "run-5")
        assert _event_types(closed) == ["tender_closed"]
        assert closed[0].details == {"reason": "source_status:complete"}
        assert _detect(database, "run-6") == []
        assert database.connection.execute(
            "SELECT count(*) FROM change_events"
        ).fetchone() == (4,)


def test_expiry_closes_once_but_incremental_absence_never_closes(tmp_path: Path) -> None:
    with Database(tmp_path / "expiry.duckdb") as database:
        _insert_release(
            database,
            release_id="expiry-2026",
            deadline=datetime(2026, 8, 2, tzinfo=UTC),
        )
        assert _event_types(_detect(database, "expiry-1")) == ["new_tender"]
        expired = _detect(
            database,
            "expiry-2",
            at=datetime(2026, 8, 3, tzinfo=UTC),
        )
        assert _event_types(expired) == ["tender_closed"]
        assert expired[0].new_value == "expired"
        assert _detect(
            database,
            "expiry-3",
            at=datetime(2026, 8, 4, tzinfo=UTC),
        ) == []

    with Database(tmp_path / "absence.duckdb") as database:
        _insert_release(database, release_id="removed-2026")
        assert _event_types(_detect(database, "absence-1")) == ["new_tender"]
        database.connection.execute(
            "DELETE FROM procurement_releases WHERE release_id = 'removed-2026'"
        )
        assert _detect(database, "absence-partial", authoritative=False) == []
        assert _detect(database, "absence-complete", authoritative=True) == []
        assert _detect(database, "absence-repeat", authoritative=True) == []
        baseline = database.connection.execute(
            """SELECT is_present FROM material_change_state
               WHERE namespace = 'tender'"""
        ).fetchone()
        assert baseline == (True,)


def test_process_release_closes_prior_tender_and_historical_closure_is_not_new(
    tmp_path: Path,
) -> None:
    with Database(tmp_path / "process-close.duckdb") as database:
        _insert_release(database, release_id="tender-2026", ocid="ocds-process-1")
        assert _event_types(_detect(database, "process-1")) == ["new_tender"]

        _insert_release(
            database,
            release_id="award-2026",
            ocid="ocds-process-1",
            stages=["award"],
            status="complete",
            observed_at=OBSERVED + timedelta(days=1),
        )
        closed = _detect(
            database,
            "process-2",
            at=OBSERVED + timedelta(days=2),
        )
        assert _event_types(closed) == ["tender_closed"]
        assert closed[0].details == {
            "reason": "process_release:award-2026:source_status:complete"
        }

    with Database(tmp_path / "historical.duckdb") as database:
        _insert_release(database, release_id="closed-2026", status="complete")
        _insert_release(
            database,
            release_id="expired-first-2026",
            deadline=OBSERVED - timedelta(days=1),
        )
        assert _detect(database, "historical-1") == []
        baselines = database.connection.execute(
            """SELECT state_payload FROM material_change_state
               WHERE namespace = 'tender' ORDER BY subject_id"""
        ).fetchall()
        assert len(baselines) == 2
        assert {
            json.loads(row[0])["lifecycle"] for row in baselines
        } == {"closed", "expired"}


def test_new_award_supplier_and_award_value_change_are_set_stable(
    tmp_path: Path,
) -> None:
    with Database(tmp_path / "awards.duckdb") as database:
        _insert_release(database, stages=["award"])
        release_row = database.connection.execute(
            "SELECT release_payload_sha256 FROM procurement_releases"
        ).fetchone()
        assert release_row is not None
        release_sha = release_row[0]
        suppliers = [
            {"party_id": "GB-COH-00000001", "name": "One Tools Ltd"},
            {"party_id": "GB-COH-00000002", "name": "Two Tools Ltd"},
        ]
        database.connection.execute(
            """INSERT INTO procurement_awards (
                 source_name, release_id, release_payload_sha256, award_index,
                 award_id, title, status, award_date, value, currency,
                 suppliers, evidence_id
               ) VALUES (?, ?, ?, 0, 'award-1', 'Tool award', 'active', ?, ?, ?, ?, ?)""",
            [
                SOURCE,
                "012345-2026",
                release_sha,
                OBSERVED,
                Decimal("1000"),
                "GBP",
                json.dumps(suppliers),
                "ev-award",
            ],
        )

        first = _detect(database, "award-1")
        assert _event_types(first) == ["new_award", "new_supplier", "new_supplier"]
        assert _detect(database, "award-2") == []

        database.connection.execute(
            "UPDATE procurement_awards SET suppliers = ?",
            [json.dumps(list(reversed(suppliers)))],
        )
        assert _detect(database, "award-reordered") == []

        suppliers.append(
            {"party_id": "GB-COH-00000003", "name": "Three Tools Ltd"}
        )
        database.connection.execute(
            "UPDATE procurement_awards SET suppliers = ?, value = ?",
            [json.dumps(suppliers), Decimal("1200")],
        )
        changed = _detect(database, "award-3")
        assert _event_types(changed) == ["new_supplier", "value_changed"]
        value_event = next(
            event for event in changed if event.event_type == "value_changed"
        )
        assert value_event.field_name == "award_value"


def test_recurrence_new_link_and_contradiction_emit_once(tmp_path: Path) -> None:
    with Database(tmp_path / "recurrent.duckdb") as database:
        database.connection.execute(
            """INSERT INTO recurrent_accounts (
                 company_id, company_name, country, company_type, products,
                 relevant_hs_codes, procurement_evidence, trade_evidence,
                 import_evidence, china_sourcing_evidence, months_active,
                 estimated_scale, buyer_role, contact_status, buying_signals,
                 evidence_strength, confidence, next_action, activity_year,
                 recurrent_product_roles
               ) VALUES (
                 'company-1', 'Acme Tools Ltd', 'Not verified', 'Importer', ?, ?,
                 '[]', ?, ?, '[]', 3, 'Three months', 'Not verified',
                 'Not verified', '[]', 'Tier 1', 0.9, 'Review', 2026, ?
               )""",
            [
                json.dumps(["Hand tools"]),
                json.dumps(["82011000"]),
                json.dumps(["ev-trade-1", "ev-trade-2", "ev-trade-3"]),
                json.dumps(["ev-trade-1", "ev-trade-2", "ev-trade-3"]),
                json.dumps(["2026:CN8:82011000:Importer"]),
            ],
        )
        database.connection.execute(
            """INSERT INTO recurrent_account_scopes VALUES (
                 'company-1', 2026, '82011000', 'CN8', 'Importer', ?, ?
               )""",
            [
                json.dumps([3, 1, 2]),
                json.dumps(["ev-trade-3", "ev-trade-1", "ev-trade-2"]),
            ],
        )
        database.connection.execute(
            """INSERT INTO entity_link_decisions (
                 decision_id, left_company_id, right_company_id,
                 normalized_name, status, match_type, confidence, reasoning,
                 evidence_ids, decided_at, is_current
               ) VALUES (
                 'decision-1', 'company-1', 'fts-company-1', 'acme tools',
                 'accepted', 'exact_name_and_postcode', 0.98,
                 'Postcodes agree', ?, ?, TRUE
               )""",
            [json.dumps(["ev-trade-1", "ev-proc-1"]), OBSERVED],
        )

        initial = _detect(database, "recurrence-1")
        assert _event_types(initial) == [
            "new_recurrent_trader",
            "recurrence_threshold_crossed",
        ]
        assert _detect(database, "recurrence-2") == []

        database.connection.execute(
            """UPDATE recurrent_accounts
               SET procurement_evidence = '[\"ev-proc-1\"]',
                   buyer_role = 'Find a Tender supplier'
               WHERE company_id = 'company-1'"""
        )
        database.connection.execute(
            """UPDATE entity_link_decisions
               SET status = 'rejected', match_type = 'exact_name_postcode_mismatch',
                   reasoning = 'Observed postcodes conflict'
               WHERE decision_id = 'decision-1'"""
        )
        changed = _detect(database, "recurrence-3")
        assert _event_types(changed) == [
            "evidence_contradiction",
            "procurement_evidence_newly_linked",
        ]
        database.connection.execute(
            """UPDATE entity_link_decisions
               SET reasoning = 'Same mismatch, clarified prose',
                   evidence_ids = '[\"ev-proc-1\", \"ev-trade-1\", \"ev-extra\"]'
               WHERE decision_id = 'decision-1'"""
        )
        assert _detect(database, "recurrence-4") == []
        database.connection.execute(
            """UPDATE entity_link_decisions
               SET match_type = 'strong_identifier_mismatch',
                   reasoning = 'Company numbers conflict'
               WHERE decision_id = 'decision-1'"""
        )
        assert _event_types(_detect(database, "recurrence-5")) == [
            "evidence_contradiction"
        ]


def test_recurrence_baselines_below_threshold_then_crosses_without_scope_leakage(
    tmp_path: Path,
) -> None:
    with Database(tmp_path / "threshold-crossing.duckdb") as database:
        _insert_release(database, release_id="unrelated-tender-2026")
        _insert_recurrent_account(database, months_active=2)
        _insert_recurrence_scope(database, [1, 2])

        initial = _detect(database, "threshold-1")
        assert _event_types(initial) == ["new_tender"]
        baseline = database.connection.execute(
            """SELECT state_payload FROM material_change_state
               WHERE namespace = 'recurrence_scope'"""
        ).fetchone()
        assert baseline is not None
        assert json.loads(baseline[0])["qualifies"] is False
        assert database.connection.execute(
            """SELECT count(*) FROM material_change_state
               WHERE namespace = 'recurrent_trader'"""
        ).fetchone() == (0,)

        database.connection.execute(
            """UPDATE recurrent_account_scopes
               SET active_months = '[1, 2, 3]',
                   evidence_ids = '[\"ev-trade-1\", \"ev-trade-2\", \"ev-trade-3\"]'"""
        )
        database.connection.execute(
            """UPDATE recurrent_accounts SET months_active = 3,
                   trade_evidence = '[\"ev-trade-1\", \"ev-trade-2\", \"ev-trade-3\"]'"""
        )
        crossed = _detect(database, "threshold-2")
        assert _event_types(crossed) == [
            "new_recurrent_trader",
            "recurrence_threshold_crossed",
        ]
        threshold_event = next(
            event
            for event in crossed
            if event.event_type == "recurrence_threshold_crossed"
        )
        assert threshold_event.old_value == 2
        assert threshold_event.new_value == 3

        database.connection.execute("DELETE FROM recurrent_account_scopes")
        database.connection.execute("DELETE FROM recurrent_accounts")
        database.connection.execute("DELETE FROM procurement_releases")
        assert _detect(
            database,
            "threshold-3",
            authoritative=True,
            connector="UK Trade Info",
            scope_key="uk-trade-info:2026:8201:importers",
        ) == []
        presence = dict(
            database.connection.execute(
                """SELECT namespace, is_present FROM material_change_state
                   WHERE namespace IN ('tender', 'recurrence_scope')"""
            ).fetchall()
        )
        assert presence == {"tender": True, "recurrence_scope": False}


def test_event_and_baseline_updates_rollback_together(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with Database(tmp_path / "rollback.duckdb") as database:
        _insert_release(database)
        original = changes_module._persist_material_state

        def fail_after_event_insert(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("synthetic baseline failure")

        monkeypatch.setattr(
            changes_module, "_persist_material_state", fail_after_event_insert
        )
        with pytest.raises(RuntimeError, match="synthetic baseline failure"):
            _detect(database, "rollback-1")

        assert database.connection.execute(
            "SELECT count(*) FROM change_events"
        ).fetchone() == (0,)
        assert database.connection.execute(
            "SELECT count(*) FROM material_change_state"
        ).fetchone() == (0,)

        monkeypatch.setattr(changes_module, "_persist_material_state", original)
        assert _event_types(_detect(database, "rollback-1")) == ["new_tender"]
        assert _detect(database, "rollback-1") == []
        assert database.connection.execute(
            "SELECT count(*) FROM change_events"
        ).fetchone() == (1,)
        assert database.connection.execute(
            """SELECT event_count FROM change_detection_runs
               WHERE run_id = 'rollback-1'"""
        ).fetchone() == (0,)
