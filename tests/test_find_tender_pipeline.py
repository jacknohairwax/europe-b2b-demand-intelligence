import copy
import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from demand_intelligence.connectors.find_tender import FindTenderReleasePackage
from demand_intelligence.connectors.ted import TedSearchPage
from demand_intelligence.current_views import reconcile_find_tender_opportunities
from demand_intelligence.pipelines import process_find_tender_page, process_ted_page
from demand_intelligence.storage import Database, SnapshotReference

ROOT = Path(__file__).parents[1]
TAXONOMY = ROOT / "config/product_clusters.json"


def _fixture_package() -> FindTenderReleasePackage:
    payload = json.loads(
        (ROOT / "tests/fixtures/find_tender_release_package.json").read_text()
    )
    return FindTenderReleasePackage.model_validate(payload)


def _single_release_page(index: int) -> FindTenderReleasePackage:
    package = _fixture_package()
    return package.model_copy(update={"releases": [package.releases[index]], "links": None})


def _snapshot(page: FindTenderReleasePackage, retrieved_at: datetime) -> SnapshotReference:
    payload = json.dumps(
        page.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, default=str
    ).encode()
    payload_hash = hashlib.sha256(payload).hexdigest()
    return SnapshotReference(
        path=f"data/raw/find-tender/{payload_hash[:12]}.json",
        content_sha256=hashlib.sha256(b"envelope|" + payload).hexdigest(),
        payload_sha256=payload_hash,
        retrieved_at=retrieved_at,
        source_url=page.uri
        or "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages",
    )


def _count(database: Database, table: str) -> int:
    row = database.connection.execute(f"SELECT count(*) FROM {table}").fetchone()
    assert row is not None
    return int(row[0])


def test_persists_active_tender_and_awards_without_scoring_award(tmp_path: Path) -> None:
    active_page = _single_release_page(0)
    award_page = _single_release_page(1)
    active_snapshot = _snapshot(active_page, datetime(2025, 8, 14, tzinfo=UTC))
    award_snapshot = _snapshot(award_page, datetime(2026, 1, 7, tzinfo=UTC))

    with Database(tmp_path / "demand.duckdb") as database:
        first = process_find_tender_page(
            active_page, active_snapshot, database, taxonomy_path=TAXONOMY
        )
        repeated = process_find_tender_page(
            active_page, active_snapshot, database, taxonomy_path=TAXONOMY
        )
        award_results = process_find_tender_page(
            award_page, award_snapshot, database, taxonomy_path=TAXONOMY
        )
        repeated_awards = process_find_tender_page(
            award_page, award_snapshot, database, taxonomy_path=TAXONOMY
        )

        assert len(first) == len(repeated) == 1
        assert award_results == repeated_awards == []
        demand = first[0]
        assert demand.notice_id == "find-tender:048243-2025"
        assert demand.product_category == "Non-chemical janitorial tools"
        assert demand.candidate_hs_codes == ["9603", "6307"]

        assert _count(database, "procurement_releases") == 2
        assert _count(database, "procurement_parties") == 4
        assert _count(database, "procurement_awards") == 2
        assert _count(database, "explicit_demand") == 0
        assert _count(database, "opportunity_scores") == 0
        assert _count(database, "evidence") == 2
        assert _count(database, "source_records") == 2

        release = database.connection.execute(
            """SELECT release_id, release_payload_sha256, package_payload_sha256,
                      package_version, product_category, candidate_hs_codes,
                      framework_stage, raw_release
               FROM procurement_releases WHERE release_id = '048243-2025'"""
        ).fetchone()
        assert release is not None
        assert release[0] == "048243-2025"
        assert len(release[1]) == len(release[2]) == 64
        assert release[1] != release[2]
        assert release[3] == "1.1"
        assert release[4] == "Non-chemical janitorial tools"
        assert json.loads(release[5]) == ["9603", "6307"]
        assert release[6] == "establishment"
        assert json.loads(release[7])["ocid"] == "ocds-h6vhtk-050918"

        stored_demand = database.connection.execute(
            "SELECT notice_id, source, product_category FROM explicit_demand"
        ).fetchone()
        assert stored_demand is None
        score_id = database.connection.execute(
            "SELECT opportunity_id FROM opportunity_scores"
        ).fetchone()
        assert score_id is None

        award_rows = database.connection.execute(
            """SELECT award_id, status, value, suppliers, raw_award
               FROM procurement_awards ORDER BY award_index"""
        ).fetchall()
        assert [(row[0], row[1], row[2]) for row in award_rows] == [
            ("1", "pending", Decimal("288000.000000")),
            ("2", "pending", Decimal("528000.000000")),
        ]
        assert len(json.loads(award_rows[0][3])) == 2
        assert json.loads(award_rows[0][4])["title"] == "Lot 1. Brooms and Brushes"
        supplier_party = database.connection.execute(
            """SELECT roles, identifiers, raw_party FROM procurement_parties
               WHERE party_id = 'GB-COH-01099088'"""
        ).fetchone()
        assert supplier_party is not None
        assert json.loads(supplier_party[0]) == ["supplier"]
        assert json.loads(supplier_party[1])[0] == {
            "scheme": "GB-COH",
            "value": "01099088",
            "legal_name": None,
            "uri": None,
        }
        assert json.loads(supplier_party[2])["name"] == "ROBERT SCOTT & SONS LIMITED"


def test_same_release_id_retains_changed_payload_versions(tmp_path: Path) -> None:
    first_page = _single_release_page(0)
    changed_raw = copy.deepcopy(first_page.releases[0])
    changed_raw["description"] = "Published correction retained as a second release payload."
    changed_page = first_page.model_copy(update={"releases": [changed_raw]})

    with Database(tmp_path / "demand.duckdb") as database:
        process_find_tender_page(
            first_page,
            _snapshot(first_page, datetime(2025, 8, 14, tzinfo=UTC)),
            database,
            taxonomy_path=TAXONOMY,
        )
        process_find_tender_page(
            changed_page,
            _snapshot(changed_page, datetime(2025, 8, 15, tzinfo=UTC)),
            database,
            taxonomy_path=TAXONOMY,
        )

        versions = database.connection.execute(
            """SELECT release_payload_sha256, package_version, raw_release
               FROM procurement_releases WHERE release_id = '048243-2025'
               ORDER BY release_payload_sha256"""
        ).fetchall()
        assert len(versions) == 2
        assert len({row[0] for row in versions}) == 2
        assert {row[1] for row in versions} == {"1.1"}
        assert any("Published correction" in row[2] for row in versions)
        assert _count(database, "procurement_parties") == 2
        assert _count(database, "evidence") == 2
        assert _count(database, "explicit_demand") == 1
        assert _count(database, "opportunity_scores") == 1


def test_corrected_closed_release_retracts_current_opportunity(tmp_path: Path) -> None:
    active_page = _single_release_page(0)
    closed_raw = copy.deepcopy(active_page.releases[0])
    tender = closed_raw["tender"]
    assert isinstance(tender, dict)
    tender["status"] = "complete"
    closed_page = active_page.model_copy(update={"releases": [closed_raw]})

    with Database(tmp_path / "retracted.duckdb") as database:
        assert process_find_tender_page(
            active_page,
            _snapshot(active_page, datetime(2025, 8, 14, tzinfo=UTC)),
            database,
            taxonomy_path=TAXONOMY,
        )
        assert process_find_tender_page(
            closed_page,
            _snapshot(closed_page, datetime(2025, 8, 15, tzinfo=UTC)),
            database,
            taxonomy_path=TAXONOMY,
        ) == []

        assert _count(database, "procurement_releases") == 2
        assert _count(database, "explicit_demand") == 0
        assert _count(database, "opportunity_scores") == 0


def test_wall_clock_reconciliation_expires_untouched_opportunity(tmp_path: Path) -> None:
    page = _single_release_page(0)
    with Database(tmp_path / "wall-clock.duckdb") as database:
        assert process_find_tender_page(
            page,
            _snapshot(page, datetime(2025, 8, 14, tzinfo=UTC)),
            database,
            taxonomy_path=TAXONOMY,
        )

        removed = reconcile_find_tender_opportunities(
            database.connection,
            as_of=datetime(2025, 9, 15, 16, 0, tzinfo=UTC),
        )

        assert removed == 1
        assert _count(database, "explicit_demand") == 0
        assert _count(database, "opportunity_scores") == 0


def test_award_without_explicit_closure_does_not_retract_before_deadline(
    tmp_path: Path,
) -> None:
    active_page = _single_release_page(0)
    award_page = _single_release_page(1)
    award_raw = copy.deepcopy(award_page.releases[0])
    award_raw["id"] = "049999-2025"
    award_raw["date"] = "2025-08-20T10:00:00+01:00"
    tender = award_raw.get("tender")
    assert isinstance(tender, dict)
    tender.pop("status", None)
    award_page = award_page.model_copy(update={"releases": [award_raw]})

    with Database(tmp_path / "missing-closure.duckdb") as database:
        assert process_find_tender_page(
            active_page,
            _snapshot(active_page, datetime(2025, 8, 14, tzinfo=UTC)),
            database,
            taxonomy_path=TAXONOMY,
        )
        assert process_find_tender_page(
            award_page,
            _snapshot(award_page, datetime(2025, 8, 20, tzinfo=UTC)),
            database,
            taxonomy_path=TAXONOMY,
        ) == []

        assert _count(database, "explicit_demand") == 1
        assert _count(database, "opportunity_scores") == 1


def test_out_of_category_active_tender_is_evidence_only(tmp_path: Path) -> None:
    page = _single_release_page(0)
    raw = copy.deepcopy(page.releases[0])
    tender = raw["tender"]
    assert isinstance(tender, dict)
    tender["title"] = "Specialist service outside selected product scope"
    tender["description"] = "An unrelated specialist service."
    tender["items"] = [
        {
            "id": "1",
            "additionalClassifications": [{"scheme": "CPV", "id": "99999999"}],
        }
    ]
    page = page.model_copy(update={"releases": [raw]})

    with Database(tmp_path / "demand.duckdb") as database:
        results = process_find_tender_page(
            page,
            _snapshot(page, datetime(2025, 8, 14, tzinfo=UTC)),
            database,
            taxonomy_path=TAXONOMY,
        )

        assert results == []
        assert _count(database, "procurement_releases") == 1
        assert _count(database, "procurement_parties") == 1
        assert _count(database, "evidence") == 1
        assert _count(database, "explicit_demand") == 0
        assert _count(database, "opportunity_scores") == 0
        category = database.connection.execute(
            "SELECT product_category FROM procurement_releases"
        ).fetchone()
        assert category == (None,)


def test_source_scoped_ids_do_not_collide_with_ted(tmp_path: Path) -> None:
    ted_payload = json.loads((ROOT / "tests/fixtures/ted_search_page.json").read_text())
    ted_page = TedSearchPage.model_validate(ted_payload)
    ted_snapshot = SnapshotReference(
        path="data/raw/ted/fixture.json",
        content_sha256="a" * 64,
        payload_sha256="b" * 64,
        retrieved_at=datetime(2025, 8, 14, tzinfo=UTC),
        source_url="https://api.ted.europa.eu/v3/notices/search",
    )
    find_tender_page = _single_release_page(0)
    raw = copy.deepcopy(find_tender_page.releases[0])
    raw["id"] = "123456-2026"
    find_tender_page = find_tender_page.model_copy(update={"releases": [raw]})

    with Database(tmp_path / "demand.duckdb") as database:
        process_ted_page(
            ted_page, ted_snapshot, database, taxonomy_path=TAXONOMY
        )
        process_find_tender_page(
            find_tender_page,
            _snapshot(find_tender_page, datetime(2025, 8, 14, tzinfo=UTC)),
            database,
            taxonomy_path=TAXONOMY,
        )

        demand_ids = database.connection.execute(
            "SELECT notice_id FROM explicit_demand ORDER BY notice_id"
        ).fetchall()
        assert demand_ids == [
            ("123456-2026",),
            ("find-tender:123456-2026",),
        ]
        opportunity_ids = database.connection.execute(
            "SELECT opportunity_id FROM opportunity_scores ORDER BY opportunity_id"
        ).fetchall()
        assert opportunity_ids == [
            ("opp-find-tender-123456-2026",),
            ("opp-ted-123456-2026",),
        ]
