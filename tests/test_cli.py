from datetime import UTC, datetime

import pytest

from demand_intelligence.cli import _count_current_run_accepted_links, build_parser
from demand_intelligence.models import EntityLinkDecision


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
