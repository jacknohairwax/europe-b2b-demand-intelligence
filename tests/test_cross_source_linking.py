import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

import demand_intelligence.linking as linking_module
from demand_intelligence.entity_resolution import normalize_legal_name
from demand_intelligence.linking import link_recurrent_accounts_to_procurement
from demand_intelligence.storage import Database

DECIDED_AT = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)


def _seed_recurrent_account(
    database: Database,
    *,
    company_id: str = "company-ukti-acme",
    name: str = "Acme & Sons Ltd",
    address: str = "10 High Street, London, SW1A 1AA",
    identifiers: dict[str, str] | None = None,
) -> None:
    database.connection.execute(
        """INSERT INTO canonical_companies VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )""",
        [
            company_id,
            name,
            normalize_legal_name(name),
            "United Kingdom",
            json.dumps([name]),
            json.dumps(identifiers or {"UKTI TraderId": "42"}),
            json.dumps([address]),
            json.dumps(["ev-trade-1", "ev-trade-2"]),
            DECIDED_AT,
            DECIDED_AT,
        ],
    )
    database.connection.execute(
        """INSERT INTO recurrent_accounts VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )""",
        [
            company_id,
            name,
            "United Kingdom",
            None,
            "Importer",
            json.dumps(["Hand tools"]),
            json.dumps(["82011000"]),
            json.dumps([]),
            json.dumps(["ev-trade-1", "ev-trade-2"]),
            json.dumps(["ev-trade-1", "ev-trade-2"]),
            json.dumps([]),
            2,
            "2 distinct active months",
            "Not verified",
            "Not verified",
            json.dumps(["Repeated importing activity"]),
            None,
            "Tier 1 HMRC evidence",
            0.9,
            "Verify legal identity",
            2026,
            json.dumps(["2026:CN8:82011000:Importer"]),
        ],
    )


def _seed_procurement_party(
    database: Database,
    *,
    release_id: str,
    party_index: int,
    party_id: str,
    name: str,
    roles: list[str],
    postcode: str | None,
    evidence_id: str,
    identifiers: list[dict[str, str | None]] | None = None,
    street_address: str = "10 High Street",
    locality: str = "London",
) -> None:
    database.connection.execute(
        """INSERT INTO procurement_parties (
        source_name, release_id, release_payload_sha256, party_index, party_id,
        name, roles, identifiers, location, evidence_id, raw_party
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            "Find a Tender OCDS API",
            release_id,
            (release_id.encode().hex() + "0" * 64)[:64],
            party_index,
            party_id,
            name,
            json.dumps(roles),
            json.dumps(identifiers or []),
            json.dumps(
                {
                    "street_address": street_address,
                    "locality": locality,
                    "region": None,
                    "postal_code": postcode,
                    "country": "GB",
                    "country_name": "United Kingdom",
                }
            ),
            evidence_id,
            json.dumps({"id": party_id, "name": name, "roles": roles}),
        ],
    )


@pytest.mark.parametrize(
    ("roles", "expected_role"),
    [
        (["buyer"], "Find a Tender buyer"),
        (["supplier"], "Find a Tender supplier"),
        (["buyer", "supplier"], "Find a Tender buyer and supplier"),
    ],
)
def test_unique_name_and_postcode_accepts_without_merging_source_identity(
    tmp_path: Path, roles: list[str], expected_role: str
) -> None:
    with Database(tmp_path / "linking.duckdb") as database:
        _seed_recurrent_account(database)
        _seed_procurement_party(
            database,
            release_id="release-accepted",
            party_index=0,
            party_id="party-acme",
            name="ACME AND SONS LIMITED",
            roles=roles,
            postcode="SW1A 1AA",
            evidence_id="ev-fts-accepted",
        )

        decisions = link_recurrent_accounts_to_procurement(
            database, decided_at=DECIDED_AT
        )
        repeated = link_recurrent_accounts_to_procurement(
            database, decided_at=DECIDED_AT
        )

        assert len(decisions) == len(repeated) == 1
        decision = decisions[0]
        assert decision.status == "accepted"
        assert decision.match_type == "exact_name_and_postcode"
        assert decision.left_company_id == "company-ukti-acme"
        assert decision.right_company_id != decision.left_company_id
        assert set(decision.evidence_ids) == {
            "ev-trade-1",
            "ev-trade-2",
            "ev-fts-accepted",
        }
        assert database.connection.execute(
            "SELECT count(*) FROM entity_link_decisions"
        ).fetchone() == (1,)
        assert database.connection.execute(
            "SELECT is_current FROM entity_link_decisions"
        ).fetchone() == (True,)
        account_row = database.connection.execute(
            """SELECT procurement_evidence, buyer_role
               FROM recurrent_accounts WHERE company_id = ?""",
            [decision.left_company_id],
        ).fetchone()
        assert account_row is not None
        assert json.loads(account_row[0]) == ["ev-fts-accepted"]
        assert account_row[1] == expected_role
        assert database.connection.execute(
            "SELECT count(*) FROM canonical_companies"
        ).fetchone() == (2,)
        stored_roles = {
            row[0]
            for row in database.connection.execute(
                """SELECT role FROM company_source_roles
                   WHERE company_id = ? ORDER BY role""",
                [decision.right_company_id],
            ).fetchall()
        }
        assert stored_roles == set(roles)


def test_same_name_with_multiple_source_identities_stays_ambiguous(
    tmp_path: Path,
) -> None:
    with Database(tmp_path / "ambiguous.duckdb") as database:
        _seed_recurrent_account(
            database,
            name="Shared Services Ltd",
            address="1 Market Street, Manchester, M1 1AA",
        )
        for index, role in enumerate(("buyer", "supplier")):
            _seed_procurement_party(
                database,
                release_id=f"release-{index}",
                party_index=0,
                party_id=f"local-party-{index}",
                name="Shared Services Limited",
                roles=[role],
                postcode="M1 1AA",
                evidence_id=f"ev-fts-{index}",
                street_address="1 Market Street",
                locality="Manchester",
            )

        decisions = link_recurrent_accounts_to_procurement(
            database, decided_at=DECIDED_AT
        )

        assert len(decisions) == 2
        assert {decision.status for decision in decisions} == {"ambiguous"}
        assert {decision.match_type for decision in decisions} == {
            "exact_name_ambiguous"
        }
        assert len({decision.right_company_id for decision in decisions}) == 2
        account_row = database.connection.execute(
            """SELECT procurement_evidence, buyer_role
               FROM recurrent_accounts WHERE company_id = 'company-ukti-acme'"""
        ).fetchone()
        assert account_row is not None
        assert json.loads(account_row[0]) == []
        assert account_row[1] == "Not verified"
        assert database.connection.execute(
            "SELECT count(*) FROM canonical_companies"
        ).fetchone() == (3,)


def test_unique_name_with_postcode_mismatch_is_rejected(
    tmp_path: Path,
) -> None:
    with Database(tmp_path / "mismatch.duckdb") as database:
        _seed_recurrent_account(database)
        _seed_procurement_party(
            database,
            release_id="release-mismatch",
            party_index=0,
            party_id="party-acme",
            name="Acme and Sons Limited",
            roles=["buyer"],
            postcode="M1 1AA",
            evidence_id="ev-fts-mismatch",
            street_address="99 Other Road",
            locality="Manchester",
        )

        decisions = link_recurrent_accounts_to_procurement(
            database, decided_at=DECIDED_AT
        )

        assert len(decisions) == 1
        assert decisions[0].status == "rejected"
        assert decisions[0].match_type == "exact_name_postcode_mismatch"
        account_row = database.connection.execute(
            """SELECT procurement_evidence, buyer_role
               FROM recurrent_accounts WHERE company_id = 'company-ukti-acme'"""
        ).fetchone()
        assert account_row is not None
        assert json.loads(account_row[0]) == []
        assert account_row[1] == "Not verified"


def test_matching_strong_typed_identifier_can_corroborate_changed_address(
    tmp_path: Path,
) -> None:
    with Database(tmp_path / "identifier.duckdb") as database:
        _seed_recurrent_account(
            database, identifiers={"GB-COH": "00123456", "UKTI TraderId": "42"}
        )
        _seed_procurement_party(
            database,
            release_id="release-identifier",
            party_index=0,
            party_id="GB-COH-00123456",
            name="Acme and Sons Ltd",
            roles=["supplier"],
            postcode="M1 1AA",
            evidence_id="ev-fts-identifier",
            identifiers=[
                {
                    "scheme": "GB-COH",
                    "value": "00123456",
                    "legal_name": "Acme and Sons Limited",
                    "uri": None,
                }
            ],
            street_address="99 Other Road",
            locality="Manchester",
        )

        decisions = link_recurrent_accounts_to_procurement(
            database, decided_at=DECIDED_AT
        )

        assert len(decisions) == 1
        assert decisions[0].status == "accepted"
        assert decisions[0].match_type == "exact_name_and_strong_identifier"
        assert decisions[0].confidence == 1.0


def test_previous_acceptance_is_superseded_when_candidate_becomes_ambiguous(
    tmp_path: Path,
) -> None:
    with Database(tmp_path / "superseded.duckdb") as database:
        _seed_recurrent_account(database)
        _seed_procurement_party(
            database,
            release_id="release-first",
            party_index=0,
            party_id="local-first",
            name="ACME AND SONS LIMITED",
            roles=["supplier"],
            postcode="SW1A 1AA",
            evidence_id="ev-fts-first",
        )
        first = link_recurrent_accounts_to_procurement(
            database, decided_at=DECIDED_AT
        )
        assert first[0].status == "accepted"

        _seed_procurement_party(
            database,
            release_id="release-second",
            party_index=0,
            party_id="local-second",
            name="Acme & Sons Ltd",
            roles=["buyer"],
            postcode="SW1A 1AA",
            evidence_id="ev-fts-second",
        )
        current = link_recurrent_accounts_to_procurement(
            database, decided_at=DECIDED_AT
        )

        assert {item.status for item in current} == {"ambiguous"}
        status_rows = database.connection.execute(
            """SELECT status, is_current, count(*)
               FROM entity_link_decisions
               GROUP BY status, is_current ORDER BY status, is_current"""
        ).fetchall()
        assert status_rows == [("accepted", False, 1), ("ambiguous", True, 2)]
        account = database.connection.execute(
            """SELECT procurement_evidence, buyer_role
               FROM recurrent_accounts"""
        ).fetchone()
        assert account == ("[]", "Not verified")


def test_link_recomputation_rolls_back_if_rebuild_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with Database(tmp_path / "rollback.duckdb") as database:
        _seed_recurrent_account(database)
        _seed_procurement_party(
            database,
            release_id="release-rollback",
            party_index=0,
            party_id="party-rollback",
            name="ACME AND SONS LIMITED",
            roles=["supplier"],
            postcode="SW1A 1AA",
            evidence_id="ev-fts-rollback",
        )
        initial = link_recurrent_accounts_to_procurement(
            database, decided_at=DECIDED_AT
        )
        assert initial[0].status == "accepted"

        def fail_after_reset(_: Database) -> dict[str, object]:
            raise RuntimeError("synthetic rebuild failure")

        monkeypatch.setattr(
            linking_module, "_load_procurement_candidates", fail_after_reset
        )
        with pytest.raises(RuntimeError, match="synthetic rebuild failure"):
            link_recurrent_accounts_to_procurement(
                database, decided_at=DECIDED_AT
            )

        assert database.connection.execute(
            "SELECT status, is_current FROM entity_link_decisions"
        ).fetchone() == ("accepted", True)
        assert database.connection.execute(
            "SELECT procurement_evidence, buyer_role FROM recurrent_accounts"
        ).fetchone() == ('["ev-fts-rollback"]', "Find a Tender supplier")
