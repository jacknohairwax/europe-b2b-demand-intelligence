import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from demand_intelligence.commercial import (
    refresh_entity_review_queue,
    refresh_existing_selected_category_commercial_view,
    refresh_selected_category_commercial_view,
)
from demand_intelligence.exports import export_all
from demand_intelligence.storage import Database

ROOT = Path(__file__).parents[1]
NOW = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)


def _seed_evidence(
    database: Database, evidence_id: str, *, source_name: str
) -> None:
    database.connection.execute(
        """INSERT OR IGNORE INTO evidence (
             evidence_id, source_name, source_type, source_tier, source_url,
             retrieved_at, raw_source_id, raw_text_or_payload_reference,
             observed_fact, confidence, limitations
           ) VALUES (?, ?, 'fixture', 1, ?, ?, ?, ?, ?, 0.95, '[]')""",
        [
            evidence_id,
            source_name,
            f"https://example.test/{evidence_id}",
            NOW,
            evidence_id,
            f"fixture.json#{evidence_id}",
            f"Fixture fact for {evidence_id}",
        ],
    )


def _seed_authoritative_run(
    database: Database,
    *,
    run_id: str,
    year: int,
    hs_prefix: str,
    trader_filter: str = "All",
    status: str = "completed",
    authoritative: bool = True,
) -> None:
    database.connection.execute(
        """INSERT INTO ingestion_run_scopes (
             run_id, connector, scope_key, scope_parameters, started_at,
             completed_at, status, completion_reason, is_authoritative,
             pages_fetched, records_seen
           ) VALUES (?, 'UK Trade Info', ?, ?, ?, ?, ?, 'source_exhausted',
                     ?, 1, 1)""",
        [
            run_id,
            f"uk-trade-info:{year}:{hs_prefix}:{trader_filter.lower()}",
            json.dumps(
                {
                    "year": year,
                    "hs_prefix": hs_prefix,
                    "trader_filter": trader_filter,
                    "minimum_months": 2,
                }
            ),
            NOW,
            NOW,
            status,
            authoritative,
        ],
    )


def _seed_trade_company(
    database: Database,
    *,
    company_id: str,
    name: str,
    trader_id: int,
    postcode: str,
    confidence: float = 0.9,
    identifiers: dict[str, str] | None = None,
) -> None:
    database.connection.execute(
        """INSERT INTO canonical_companies (
             company_id, preferred_name, normalized_name, country,
             original_names, identifiers, addresses, evidence_ids,
             first_seen_at, last_seen_at
           ) VALUES (?, ?, ?, NULL, ?, ?, ?, '[]', ?, ?)""",
        [
            company_id,
            name,
            name.lower(),
            json.dumps([name]),
            json.dumps(identifiers or {"UKTI TraderId": str(trader_id)}),
            json.dumps([f"10 High Street, London, {postcode}"]),
            NOW,
            NOW,
        ],
    )
    database.connection.execute(
        """INSERT INTO recurrent_accounts (
             company_id, company_name, country, company_type, products,
             relevant_hs_codes, procurement_evidence, trade_evidence,
             import_evidence, china_sourcing_evidence, months_active,
             estimated_scale, buyer_role, contact_status, buying_signals,
             evidence_strength, confidence, next_action,
             recurrent_product_roles
           ) VALUES (?, ?, 'Not verified', 'Importer', '[]', '[]', '[]',
                     '[]', '[]', '[]', 0, 'fixture', 'Not verified',
                     'Not verified', '[]', 'fixture', ?, 'Review account', '[]')""",
        [company_id, name, confidence],
    )


def _seed_trade_scope(
    database: Database,
    *,
    company_id: str,
    name: str,
    trader_id: int,
    postcode: str,
    year: int,
    code: str,
    level: str,
    role: str,
    months: list[int],
    is_current: bool = True,
) -> list[str]:
    evidence_ids: list[str] = []
    for month in months:
        evidence_id = (
            f"ev-trade-{company_id}-{year}-{code}-{role.lower()}-{month}"
        )
        evidence_ids.append(evidence_id)
        _seed_evidence(database, evidence_id, source_name="UK Trade Info")
        activity_id = f"activity-{company_id}-{year}-{code}-{role}-{month}"
        database.connection.execute(
            """INSERT INTO trade_activity (
                 activity_id, source_record_id, company_id, trader_id,
                 company_name, address_lines, postcode, commodity_code,
                 commodity_code_level, hs2_code, hs4_code, hs6_code,
                 product_description, year, month, trade_role, source_url,
                 evidence_id, raw_payload_reference, payload_sha256, is_current
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '82', ?, ?, ?, ?, ?, ?,
                         'https://api.uktradeinfo.com/Trade', ?, ?, ?, ?)""",
            [
                activity_id,
                activity_id,
                company_id,
                trader_id,
                name,
                json.dumps(["10 High Street", "London"]),
                postcode,
                code,
                level,
                code[:4] if len(code) >= 4 else None,
                code[:6] if len(code) >= 6 else None,
                f"Fixture product {code}",
                year,
                month,
                role,
                evidence_id,
                f"fixture.json#{activity_id}",
                f"sha-{activity_id}",
                is_current,
            ],
        )
    database.connection.execute(
        """INSERT INTO recurrent_account_scopes VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            company_id,
            year,
            code,
            level,
            role,
            json.dumps(months),
            json.dumps(evidence_ids),
        ],
    )
    return evidence_ids


def _seed_procurement_candidate(
    database: Database,
    *,
    company_id: str,
    name: str,
    release_id: str,
    postcode: str,
    role: str,
    is_current: bool = True,
    identifiers: list[dict[str, str]] | None = None,
    contract_value: int | None = None,
) -> str:
    evidence_id = f"ev-fts-{release_id}"
    payload_sha = f"sha-{release_id}"
    _seed_evidence(database, evidence_id, source_name="Find a Tender OCDS API")
    database.connection.execute(
        """INSERT OR IGNORE INTO canonical_companies (
             company_id, preferred_name, normalized_name, country,
             original_names, identifiers, addresses, evidence_ids,
             first_seen_at, last_seen_at
           ) VALUES (?, ?, ?, 'United Kingdom', ?, '{}', ?, ?, ?, ?)""",
        [
            company_id,
            name,
            name.lower(),
            json.dumps([name]),
            json.dumps([f"99 Procurement Road, Manchester, {postcode}"]),
            json.dumps([evidence_id]),
            NOW,
            NOW,
        ],
    )
    database.connection.execute(
        """INSERT INTO procurement_releases (
             source_name, release_id, ocid, release_payload_sha256,
             package_payload_sha256, snapshot_reference,
             snapshot_content_sha256, retrieved_at, publication_date, stages,
             tender_status, buyer_name, title, cpv_codes, deadline, value,
             currency, award_statuses, awarded_suppliers, source_url,
             evidence_ids, raw_release, is_current, version_state
           ) VALUES ('Find a Tender OCDS API', ?, ?, ?, ?, 'fixture.json', ?, ?,
                     ?, '[\"award\"]', 'complete', 'Fixture buyer', ?,
                     '[\"44511000\"]', ?, 60000, 'GBP', '[\"active\"]',
                     '[]', ?, ?, '{}', ?, ?)""",
        [
            release_id,
            f"ocds-{release_id}",
            payload_sha,
            payload_sha,
            payload_sha,
            NOW,
            NOW,
            f"Fixture procurement {release_id}",
            NOW,
            f"https://www.find-tender.service.gov.uk/Notice/{release_id}",
            json.dumps([evidence_id]),
            is_current,
            "current" if is_current else "replaced",
        ],
    )
    database.connection.execute(
        """INSERT INTO procurement_parties (
             source_name, release_id, release_payload_sha256, party_index,
             party_id, name, roles, identifiers, location, evidence_id,
             raw_party
           ) VALUES ('Find a Tender OCDS API', ?, ?, 0, ?, ?, ?, ?, ?, ?, '{}')""",
        [
            release_id,
            payload_sha,
            f"party-{release_id}",
            name,
            json.dumps([role]),
            json.dumps(identifiers or []),
            json.dumps(
                {
                    "street_address": "99 Procurement Road",
                    "locality": "Manchester",
                    "postal_code": postcode,
                    "country_name": "United Kingdom",
                }
            ),
            evidence_id,
        ],
    )
    database.connection.execute(
        """INSERT INTO company_source_roles VALUES (?, ?, ?, ?, ?, ?)""",
        [
            company_id,
            "Find a Tender OCDS API",
            f"{release_id}:{payload_sha}:0",
            name,
            role,
            evidence_id,
        ],
    )
    if contract_value is not None:
        award_evidence_id = f"ev-award-{release_id}"
        _seed_evidence(
            database,
            award_evidence_id,
            source_name="Find a Tender OCDS API",
        )
        database.connection.execute(
            """INSERT INTO procurement_awards (
                 source_name, release_id, release_payload_sha256, award_index,
                 award_id, title, status, award_date, value, currency,
                 value_basis, contract_status, contract_value,
                 contract_currency, contract_value_basis, contract_period,
                 suppliers, evidence_id, raw_award
               ) VALUES (
                 'Find a Tender OCDS API', ?, ?, 0, ?, 'Fixture award',
                 'active', ?, 55000, 'GBP', 'award', 'active', ?, 'GBP',
                 'contract', '{}', ?, ?, '{}'
               )""",
            [
                release_id,
                payload_sha,
                f"award-{release_id}",
                NOW,
                contract_value,
                json.dumps(
                    [
                        {
                            "party_id": f"party-{release_id}",
                            "name": name,
                        }
                    ]
                ),
                award_evidence_id,
            ],
        )
    return evidence_id


def _seed_decision(
    database: Database,
    *,
    decision_id: str,
    left_company_id: str,
    right_company_id: str,
    status: str,
    match_type: str,
    confidence: float,
    is_current: bool = True,
    evidence_ids: list[str] | None = None,
) -> None:
    database.connection.execute(
        """INSERT INTO entity_link_decisions (
             decision_id, left_company_id, right_company_id, normalized_name,
             status, match_type, confidence, reasoning, evidence_ids,
             decided_at, is_current
           ) VALUES (?, ?, ?, 'acme limited', ?, ?, ?, ?, ?, ?, ?)""",
        [
            decision_id,
            left_company_id,
            right_company_id,
            status,
            match_type,
            confidence,
            f"Fixture {status}: {match_type}",
            json.dumps(evidence_ids or []),
            NOW,
            is_current,
        ],
    )


def test_review_queue_groups_ambiguous_candidates_and_never_merges(
    tmp_path: Path,
) -> None:
    with Database(tmp_path / "review-ambiguous.duckdb") as database:
        _seed_trade_company(
            database,
            company_id="ukti-acme",
            name="Acme Limited",
            trader_id=42,
            postcode="SW1A 1AA",
            identifiers={"UKTI TraderId": "42", "GB-COH": "11111111"},
        )
        _seed_trade_scope(
            database,
            company_id="ukti-acme",
            name="Acme Limited",
            trader_id=42,
            postcode="SW1A 1AA",
            year=2025,
            code="82032000",
            level="CN8",
            role="Importer",
            months=[1, 2],
        )
        for index in (1, 2):
            right_id = f"fts-acme-{index}"
            _seed_procurement_candidate(
                database,
                company_id=right_id,
                name="ACME LIMITED",
                release_id=f"00000{index}-2026",
                postcode="SW1A 1AA" if index == 1 else "M1 1AA",
                role="supplier",
                identifiers=(
                    [{"scheme": "GB-COH", "value": "22222222"}]
                    if index == 2
                    else None
                ),
            )
            _seed_decision(
                database,
                decision_id=f"ambiguous-{index}",
                left_company_id="ukti-acme",
                right_company_id=right_id,
                status="ambiguous",
                match_type="exact_name_ambiguous",
                confidence=0.0,
            )

        assert refresh_entity_review_queue(database.connection, as_of=NOW) == 2
        rows = database.connection.execute(
            """SELECT case_id, right_company_id, automatic_status,
                      candidate_roles, supporting_evidence, conflicting_evidence
               FROM entity_review_queue WHERE is_current
               ORDER BY right_company_id"""
        ).fetchall()
        assert len({row[0] for row in rows}) == 1
        assert [row[1] for row in rows] == ["fts-acme-1", "fts-acme-2"]
        assert {row[2] for row in rows} == {"ambiguous"}
        assert all(json.loads(row[3]) == ["supplier"] for row in rows)
        assert all(
            json.loads(row[4])["candidate_basis"]["value"] == "acme limited"
            for row in rows
        )
        second_conflicts = {
            item["field"] for item in json.loads(rows[1][5])
        }
        assert second_conflicts == {
            "identifier:GB-COH",
            "postcode",
            "normalized_address",
        }
        assert database.connection.execute(
            "SELECT procurement_evidence FROM recurrent_accounts"
        ).fetchone() == ("[]",)
        assert database.connection.execute(
            "SELECT count(*) FROM canonical_companies"
        ).fetchone() == (3,)


def test_review_queue_preserves_manual_state_and_excludes_accepted_link(
    tmp_path: Path,
) -> None:
    with Database(tmp_path / "review-conflict.duckdb") as database:
        _seed_trade_company(
            database,
            company_id="ukti-acme",
            name="Acme Limited",
            trader_id=42,
            postcode="SW1A 1AA",
        )
        _seed_trade_scope(
            database,
            company_id="ukti-acme",
            name="Acme Limited",
            trader_id=42,
            postcode="SW1A 1AA",
            year=2025,
            code="82032000",
            level="CN8",
            role="Importer",
            months=[1, 2],
        )
        _seed_procurement_candidate(
            database,
            company_id="fts-acme",
            name="ACME LIMITED",
            release_id="123456-2026",
            postcode="M1 1AA",
            role="buyer",
        )
        _seed_decision(
            database,
            decision_id="rejected-1",
            left_company_id="ukti-acme",
            right_company_id="fts-acme",
            status="rejected",
            match_type="exact_name_postcode_mismatch",
            confidence=0.0,
        )

        assert refresh_entity_review_queue(database.connection, as_of=NOW) == 1
        original_output = database.connection.execute(
            "SELECT * FROM entity_review_queue"
        ).fetchall()
        later = datetime(2026, 8, 25, 13, 0, tzinfo=UTC)
        assert refresh_entity_review_queue(database.connection, as_of=later) == 1
        assert database.connection.execute(
            "SELECT * FROM entity_review_queue"
        ).fetchall() == original_output
        first = database.connection.execute(
            """SELECT review_id, conflicting_evidence, rejection_reason,
                      created_at
               FROM entity_review_queue WHERE is_current"""
        ).fetchone()
        assert first is not None
        conflict = json.loads(first[1])
        assert conflict[0]["field"] == "postcode"
        assert conflict[0]["left_values"] == ["SW1A1AA"]
        assert conflict[0]["right_values"] == ["M11AA"]
        assert "postcode_mismatch" in first[2]
        database.connection.execute(
            """UPDATE entity_review_queue
               SET queue_status = 'deferred', reviewer = 'analyst@example.test',
                   review_notes = 'Need Companies House evidence', reviewed_at = ?
               WHERE review_id = ?""",
            [NOW, first[0]],
        )

        assert refresh_entity_review_queue(database.connection, as_of=later) == 1
        preserved = database.connection.execute(
            """SELECT queue_status, reviewer, review_notes, reviewed_at,
                      created_at, updated_at
               FROM entity_review_queue WHERE review_id = ?""",
            [first[0]],
        ).fetchone()
        assert preserved is not None
        assert preserved[:3] == (
            "deferred",
            "analyst@example.test",
            "Need Companies House evidence",
        )
        assert preserved[4] == first[3]
        assert preserved[5] == NOW

        changed_at = datetime(2026, 8, 25, 14, 0, tzinfo=UTC)
        database.connection.execute(
            """UPDATE entity_link_decisions
               SET reasoning = 'Materially revised automatic reasoning'
               WHERE decision_id = 'rejected-1'"""
        )
        assert refresh_entity_review_queue(database.connection, as_of=changed_at) == 1
        assert database.connection.execute(
            "SELECT updated_at FROM entity_review_queue"
        ).fetchone() == (changed_at,)

        database.connection.execute(
            "UPDATE entity_link_decisions SET is_current = FALSE"
        )
        _seed_decision(
            database,
            decision_id="accepted-reviewed",
            left_company_id="ukti-acme",
            right_company_id="fts-acme",
            status="accepted",
            match_type="human_reviewed_identity",
            confidence=0.95,
        )
        assert refresh_entity_review_queue(database.connection, as_of=changed_at) == 0
        assert database.connection.execute(
            "SELECT count(*) FROM entity_review_queue WHERE is_current"
        ).fetchone() == (0,)
        assert database.connection.execute(
            "SELECT queue_status, is_current FROM entity_review_queue"
        ).fetchone() == ("deferred", False)
        # Queue refreshes never attach evidence or merge the two source identities.
        assert database.connection.execute(
            "SELECT procurement_evidence FROM recurrent_accounts"
        ).fetchone() == ("[]",)
        assert database.connection.execute(
            "SELECT count(*) FROM canonical_companies"
        ).fetchone() == (2,)
        export_all(database.connection, tmp_path / "review-exports")
        review_csv = (tmp_path / "review-exports/entity_review_queue.csv").read_text()
        assert len(review_csv.splitlines()) == 1


def test_selected_category_view_is_current_exact_and_auditable(
    tmp_path: Path,
) -> None:
    with Database(tmp_path / "commercial.duckdb") as database:
        coverage_run_ids = [f"coverage-{prefix}" for prefix in ("8203", "8204", "8205", "8206")]
        for run_id, prefix in zip(
            coverage_run_ids, ("8203", "8204", "8205", "8206"), strict=True
        ):
            _seed_authoritative_run(
                database,
                run_id=run_id,
                year=2025,
                hs_prefix=prefix,
            )
        companies = [
            ("ukti-linked", "Linked Tools Ltd", 101, 0.9),
            ("ukti-review", "Review Tools Ltd", 102, 0.88),
            ("ukti-trade", "Trade Only Ltd", 103, 0.85),
            ("ukti-removed", "Removed Tools Ltd", 104, 0.99),
        ]
        for company_id, name, trader_id, confidence in companies:
            _seed_trade_company(
                database,
                company_id=company_id,
                name=name,
                trader_id=trader_id,
                postcode="SW1A 1AA",
                confidence=confidence,
            )
        linked_trade_evidence = _seed_trade_scope(
            database,
            company_id="ukti-linked",
            name="Linked Tools Ltd",
            trader_id=101,
            postcode="SW1A 1AA",
            year=2025,
            code="82032000",
            level="CN8",
            role="Importer",
            months=[1, 2, 3],
        )
        _seed_trade_scope(
            database,
            company_id="ukti-linked",
            name="Linked Tools Ltd",
            trader_id=101,
            postcode="SW1A 1AA",
            year=2025,
            code="82032000",
            level="CN8",
            role="Exporter",
            months=[4, 5],
        )
        _seed_trade_scope(
            database,
            company_id="ukti-linked",
            name="Linked Tools Ltd",
            trader_id=101,
            postcode="SW1A 1AA",
            year=2025,
            code="83021000",
            level="CN8",
            role="Importer",
            months=[6, 7],
        )
        _seed_trade_scope(
            database,
            company_id="ukti-linked",
            name="Linked Tools Ltd",
            trader_id=101,
            postcode="SW1A 1AA",
            year=2024,
            code="82032000",
            level="CN8",
            role="Importer",
            months=[1, 2],
        )
        _seed_trade_scope(
            database,
            company_id="ukti-review",
            name="Review Tools Ltd",
            trader_id=102,
            postcode="SW1A 1AA",
            year=2025,
            code="82034000",
            level="CN8",
            role="Importer",
            months=[1, 2, 3],
        )
        _seed_trade_scope(
            database,
            company_id="ukti-trade",
            name="Trade Only Ltd",
            trader_id=103,
            postcode="SW1A 1AA",
            year=2025,
            code="82031000",
            level="CN8",
            role="Importer",
            months=[1, 2],
        )
        _seed_trade_scope(
            database,
            company_id="ukti-removed",
            name="Removed Tools Ltd",
            trader_id=104,
            postcode="SW1A 1AA",
            year=2025,
            code="82032000",
            level="CN8",
            role="Importer",
            months=[1, 2, 3, 4],
            is_current=False,
        )

        linked_procurement_evidence = _seed_procurement_candidate(
            database,
            company_id="fts-linked",
            name="LINKED TOOLS LTD",
            release_id="076913-2026",
            postcode="SW1A 1AA",
            role="supplier",
            contract_value=60000,
        )
        _seed_decision(
            database,
            decision_id="accepted-linked",
            left_company_id="ukti-linked",
            right_company_id="fts-linked",
            status="accepted",
            match_type="exact_name_and_postcode",
            confidence=0.98,
            evidence_ids=[
                linked_trade_evidence[0],
                linked_procurement_evidence,
            ],
        )
        # A replaced release for the accepted identity must not leak into the view.
        _seed_procurement_candidate(
            database,
            company_id="fts-linked",
            name="LINKED TOOLS LTD",
            release_id="obsolete-2025",
            postcode="SW1A 1AA",
            role="buyer",
            is_current=False,
        )
        _seed_procurement_candidate(
            database,
            company_id="fts-review",
            name="REVIEW TOOLS LTD",
            release_id="review-2026",
            postcode="M1 1AA",
            role="buyer",
        )
        _seed_decision(
            database,
            decision_id="unresolved-review",
            left_company_id="ukti-review",
            right_company_id="fts-review",
            status="unresolved",
            match_type="exact_name_insufficient_location_evidence",
            confidence=0.3,
        )

        count = refresh_selected_category_commercial_view(
            database.connection,
            year=2025,
            taxonomy_path=ROOT / "config/product_clusters.json",
            authoritative_run_ids=coverage_run_ids,
            category_ids=["non_powered_hand_tools"],
            as_of=NOW,
        )
        assert count == 4
        rows = database.connection.execute(
            """SELECT priority_rank, company_id, commodity_code, trade_role,
                      active_months, has_procurement_evidence,
                      procurement_roles, procurement_evidence,
                      evidence_source_count, confidence, validation_status,
                      accepted_link_evidence, authoritative_run_ids,
                      authoritative_scope_coverage
               FROM selected_category_commercial_view
               ORDER BY priority_rank"""
        ).fetchall()
        assert [(row[1], row[3]) for row in rows[:2]] == [
            ("ukti-linked", "Importer"),
            ("ukti-linked", "Exporter"),
        ]
        assert json.loads(rows[0][4]) == [1, 2, 3]
        assert rows[0][5] is True
        assert json.loads(rows[0][6]) == ["supplier"]
        procurement = json.loads(rows[0][7])
        assert [item["release_id"] for item in procurement] == ["076913-2026"]
        assert procurement[0]["value"] == "60000.000000"
        assert procurement[0]["awards"][0]["contract_value"] == "60000.000000"
        assert procurement[0]["awards"][0]["suppliers"] == [
            {
                "party_id": "party-076913-2026",
                "name": "LINKED TOOLS LTD",
            }
        ]
        assert rows[0][8] == 2
        assert rows[0][9] == pytest.approx(0.9)
        assert rows[0][10] == (
            "Validated entity link; taxonomy candidate requires review"
        )
        link_evidence = json.loads(rows[0][11])
        assert link_evidence[0]["decision_id"] == "accepted-linked"
        assert link_evidence[0]["right_company_id"] == "fts-linked"
        assert link_evidence[0]["status"] == "accepted"
        assert link_evidence[0]["match_type"] == "exact_name_and_postcode"
        assert link_evidence[0]["confidence"] == pytest.approx(0.98)
        assert set(link_evidence[0]["evidence_ids"]) == {
            linked_trade_evidence[0],
            linked_procurement_evidence,
        }
        assert link_evidence[0]["right_identity"]["company_id"] == "fts-linked"
        assert json.loads(rows[0][12]) == coverage_run_ids
        scope_coverage = json.loads(rows[0][13])
        assert {item["run_id"] for item in scope_coverage} == {"coverage-8203"}
        review_row = next(row for row in rows if row[1] == "ukti-review")
        assert review_row[5] is False
        assert review_row[8] == 1
        assert review_row[10].startswith("Entity link requires human review")
        trade_only_row = next(row for row in rows if row[1] == "ukti-trade")
        assert trade_only_row[10].startswith("Trade recurrence verified")
        assert {row[2] for row in rows} <= {"82031000", "82032000", "82034000"}
        assert "ukti-removed" not in {row[1] for row in rows}

        before = database.connection.execute(
            """SELECT * FROM selected_category_commercial_view
               ORDER BY priority_rank"""
        ).fetchall()
        assert refresh_selected_category_commercial_view(
            database.connection,
            year=2025,
            taxonomy_path=ROOT / "config/product_clusters.json",
            authoritative_run_ids=coverage_run_ids,
            category_ids=["non_powered_hand_tools"],
            as_of=NOW,
        ) == 4
        after = database.connection.execute(
            """SELECT * FROM selected_category_commercial_view
               ORDER BY priority_rank"""
        ).fetchall()
        assert before == after
        assert refresh_selected_category_commercial_view(
            database.connection,
            year=2025,
            taxonomy_path=ROOT / "config/product_clusters.json",
            authoritative_run_ids=coverage_run_ids,
            trader_filter="Importers",
            category_ids=["non_powered_hand_tools"],
            as_of=NOW,
        ) == 3
        assert database.connection.execute(
            "SELECT DISTINCT trade_role FROM selected_category_commercial_view"
        ).fetchall() == [("Importer",)]
        assert refresh_selected_category_commercial_view(
            database.connection,
            year=2025,
            taxonomy_path=ROOT / "config/product_clusters.json",
            authoritative_run_ids=coverage_run_ids,
            recurrence_threshold=3,
            category_ids=["non_powered_hand_tools"],
            as_of=NOW,
        ) == 2
        assert database.connection.execute(
            """SELECT min(recurrent_month_count), min(recurrence_threshold)
               FROM selected_category_commercial_view"""
        ).fetchone() == (3, 3)
        assert refresh_selected_category_commercial_view(
            database.connection,
            year=2025,
            taxonomy_path=ROOT / "config/product_clusters.json",
            authoritative_run_ids=["coverage-8203"],
            hs_prefixes=["8203"],
            as_of=NOW,
        ) == 4
        assert refresh_existing_selected_category_commercial_view(
            database.connection, as_of=NOW
        ) == 4
        assert database.connection.execute(
            """SELECT activity_year, selector_type, selectors,
                      authoritative_run_ids
               FROM selected_category_commercial_view_state"""
        ).fetchone() == (
            2025,
            "hs_prefix",
            '["8203"]',
            '["coverage-8203"]',
        )
        paths = export_all(database.connection, tmp_path / "commercial-exports")
        assert tmp_path / "commercial-exports/selected_category_commercial_view.csv" in paths
        commercial_csv = (
            tmp_path / "commercial-exports/selected_category_commercial_view.csv"
        ).read_text()
        assert len(commercial_csv.splitlines()) == 5
        review_csv = (tmp_path / "commercial-exports/entity_review_queue.csv").read_text()
        assert "ukti-review" in review_csv
        assert "ukti-linked" not in review_csv


def test_commercial_view_requires_explicit_selection_and_complete_run(
    tmp_path: Path,
) -> None:
    with Database(tmp_path / "selection.duckdb") as database:
        with pytest.raises(ValueError, match="exactly one"):
            refresh_selected_category_commercial_view(
                database.connection,
                year=2025,
                taxonomy_path=ROOT / "config/product_clusters.json",
                authoritative_run_ids=[],
                as_of=NOW,
            )
        with pytest.raises(ValueError, match="unknown taxonomy"):
            refresh_selected_category_commercial_view(
                database.connection,
                year=2025,
                taxonomy_path=ROOT / "config/product_clusters.json",
                authoritative_run_ids=[],
                category_ids=["not-a-category"],
                as_of=NOW,
            )
        _seed_authoritative_run(
            database,
            run_id="partial-run",
            year=2025,
            hs_prefix="8203",
            status="partial",
            authoritative=False,
        )
        with pytest.raises(ValueError, match="not a completed authoritative"):
            refresh_selected_category_commercial_view(
                database.connection,
                year=2025,
                taxonomy_path=ROOT / "config/product_clusters.json",
                authoritative_run_ids=["partial-run"],
                hs_prefixes=["8203"],
                as_of=NOW,
            )
        _seed_authoritative_run(
            database, run_id="only-8203", year=2025, hs_prefix="8203"
        )
        with pytest.raises(ValueError, match="missing completed authoritative coverage"):
            refresh_selected_category_commercial_view(
                database.connection,
                year=2025,
                taxonomy_path=ROOT / "config/product_clusters.json",
                authoritative_run_ids=["only-8203"],
                category_ids=["non_powered_hand_tools"],
                as_of=NOW,
            )
        _seed_authoritative_run(
            database, run_id="wrong-year", year=2024, hs_prefix="8203"
        )
        with pytest.raises(ValueError, match="covers 2024"):
            refresh_selected_category_commercial_view(
                database.connection,
                year=2025,
                taxonomy_path=ROOT / "config/product_clusters.json",
                authoritative_run_ids=["wrong-year"],
                hs_prefixes=["8203"],
                as_of=NOW,
            )
        _seed_authoritative_run(
            database, run_id="unrelated", year=2025, hs_prefix="8302"
        )
        with pytest.raises(ValueError, match="does not cover"):
            refresh_selected_category_commercial_view(
                database.connection,
                year=2025,
                taxonomy_path=ROOT / "config/product_clusters.json",
                authoritative_run_ids=["unrelated"],
                hs_prefixes=["8203"],
                as_of=NOW,
            )

        _seed_trade_company(
            database,
            company_id="ukti-exporter",
            name="Exporter Tools Ltd",
            trader_id=501,
            postcode="SW1A 1AA",
        )
        _seed_trade_scope(
            database,
            company_id="ukti-exporter",
            name="Exporter Tools Ltd",
            trader_id=501,
            postcode="SW1A 1AA",
            year=2025,
            code="82032000",
            level="CN8",
            role="Exporter",
            months=[1, 2],
        )
        _seed_authoritative_run(
            database,
            run_id="importers-only",
            year=2025,
            hs_prefix="8203",
            trader_filter="Importers",
        )
        with pytest.raises(ValueError, match="with role Exporter"):
            refresh_selected_category_commercial_view(
                database.connection,
                year=2025,
                taxonomy_path=ROOT / "config/product_clusters.json",
                authoritative_run_ids=["importers-only"],
                hs_prefixes=["8203"],
                as_of=NOW,
            )
