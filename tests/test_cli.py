import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from demand_intelligence.cli import (
    _count_current_run_accepted_links,
    _current_recurrence_threshold,
    _latest_authoritative_runs,
    _refresh_operational_state,
    _validate_authoritative_trader_filter,
    _validate_recurrence_policy,
    build_parser,
)
from demand_intelligence.models import EntityLinkDecision
from demand_intelligence.storage import Database


def test_find_tender_cli_accepts_separate_stage_streams_and_window() -> None:
    args = build_parser().parse_args(
        [
            "find-tender",
            "--stage",
            "tender",
            "--stage",
            "award",
            "--updated-from",
            "2026-08-01T00:00:00",
            "--updated-to",
            "2026-08-23T12:00:00",
        ]
    )

    assert args.stage == ["tender", "award"]
    assert args.updated_from == datetime(2026, 8, 1)
    assert args.updated_to == datetime(2026, 8, 23, 12)


def test_find_tender_cli_rejects_invalid_datetime() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["find-tender", "--updated-from", "not-a-datetime"]
        )


def test_trade_cli_requires_at_least_two_months_for_recurrence() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                "uk-trade-info",
                "--hs-prefix",
                "8203",
                "--year",
                "2025",
                "--minimum-months",
                "1",
            ]
        )


def test_trade_cli_counts_accepted_links_only_for_current_run_accounts() -> None:
    def decision(decision_id: str, company_id: str, status: str) -> EntityLinkDecision:
        return EntityLinkDecision(
            decision_id=decision_id,
            left_company_id=company_id,
            right_company_id=f"fts-{company_id}",
            normalized_name=company_id.upper(),
            status=status,
            match_type="exact_name_and_postcode",
            confidence=0.98 if status == "accepted" else 0.0,
            reasoning="fixture",
            evidence_ids=[],
            decided_at=datetime(2026, 8, 24, tzinfo=UTC),
        )

    decisions = [
        decision("current-accepted", "company-current", "accepted"),
        decision("current-rejected", "company-current", "rejected"),
        decision("prior-accepted", "company-prior", "accepted"),
    ]

    assert _count_current_run_accepted_links(decisions, {"company-current"}) == 1


def test_commercial_export_requires_exactly_one_selection_family() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "commercial-export",
            "--year",
            "2025",
            "--hs-prefix",
            "8203",
            "--trader-filter",
            "Importers",
        ]
    )

    assert args.year == 2025
    assert args.hs_prefix == ["8203"]
    assert args.category is None

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "commercial-export",
                "--year",
                "2025",
                "--hs-prefix",
                "8203",
                "--category",
                "non_powered_hand_tools",
            ]
        )


def test_cli_rejects_non_positive_page_bounds() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                "uk-trade-info",
                "--year",
                "2025",
                "--hs-prefix",
                "8203",
                "--max-pages",
                "0",
            ]
        )


def test_commercial_export_resolves_latest_narrow_authoritative_scope(
    tmp_path: Path,
) -> None:
    with Database(tmp_path / "coverage.duckdb") as database:
        for run_id, prefix, completed_at in [
            ("broad", "82", "2026-08-24T00:00:00Z"),
            ("narrow-old", "8203", "2026-08-24T01:00:00Z"),
            ("narrow-latest", "8203", "2026-08-25T01:00:00Z"),
        ]:
            database.connection.execute(
                """INSERT INTO ingestion_run_scopes (
                     run_id, connector, scope_key, scope_parameters, started_at,
                     completed_at, status, completion_reason, is_authoritative,
                     pages_fetched, records_seen
                   ) VALUES (?, 'UK Trade Info', ?, ?, ?, ?, 'completed',
                             'source_exhausted', TRUE, 1, 1)""",
                [
                    run_id,
                    f"uk-trade-info:2025:{prefix}:importers",
                    json.dumps(
                        {
                            "year": 2025,
                            "hs_prefix": prefix,
                            "trader_filter": "Importers",
                            "minimum_months": 2,
                        }
                    ),
                    completed_at,
                    completed_at,
                ],
            )

        assert _latest_authoritative_runs(
            database,
            year=2025,
            prefixes=["8203"],
            trader_filter="Importers",
        ) == ["narrow-latest"]
        _validate_authoritative_trader_filter(
            database, ["narrow-latest"], requested_filter="Importers"
        )
        _validate_recurrence_policy(database, 2)
        with pytest.raises(ValueError, match="fixed recurrence threshold"):
            _validate_recurrence_policy(database, 3)
        with pytest.raises(ValueError, match="does not cover"):
            _validate_authoritative_trader_filter(
                database, ["narrow-latest"], requested_filter="Exporters"
            )


def test_unrelated_operational_run_reuses_persisted_recurrence_threshold(
    tmp_path: Path,
) -> None:
    with Database(tmp_path / "threshold.duckdb") as database:
        database.connection.execute(
            """INSERT INTO change_detection_runs VALUES (
                 'ukti-three', 'UK Trade Info', 'fixture', ?, TRUE, 3, 0
               )""",
            [datetime(2026, 8, 25, tzinfo=UTC)],
        )

        assert _current_recurrence_threshold(database) == 3
        _refresh_operational_state(
            database,
            detector_run_id="find-tender-after-ukti",
            connector="Find a Tender OCDS API",
            scope_key="find-tender:fixture",
        )
        assert database.connection.execute(
            """SELECT recurrence_threshold FROM change_detection_runs
               WHERE run_id = 'find-tender-after-ukti'"""
        ).fetchone() == (3,)
