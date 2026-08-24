import json
from datetime import timedelta
from pathlib import Path

from demand_intelligence.connectors import TedSearchPage
from demand_intelligence.exports import TABLES, export_all
from demand_intelligence.pipelines import process_ted_page
from demand_intelligence.storage import Database, SnapshotStore

ROOT = Path(__file__).parents[1]


def test_fixture_to_database_and_exports(tmp_path: Path) -> None:
    raw = json.loads((ROOT / "tests/fixtures/ted_search_page.json").read_text())
    snapshot = SnapshotStore(tmp_path / "raw").write_json(
        source="ted-fixture",
        source_record_id="page-1",
        payload=raw,
        request=None,
        source_url="https://api.ted.europa.eu/v3/notices/search",
    )
    database = Database(tmp_path / "demand.duckdb")
    try:
        database.initialize()
        first = process_ted_page(
            TedSearchPage.model_validate(raw),
            snapshot,
            database,
            taxonomy_path=ROOT / "config/product_clusters.json",
        )
        second = process_ted_page(
            TedSearchPage.model_validate(raw),
            snapshot,
            database,
            taxonomy_path=ROOT / "config/product_clusters.json",
        )
        later_snapshot = snapshot.model_copy(
            update={
                "path": "data/raw/ted-fixture/later-envelope.json",
                "content_sha256": "f" * 64,
                "retrieved_at": snapshot.retrieved_at + timedelta(hours=1),
            }
        )
        third = process_ted_page(
            TedSearchPage.model_validate(raw),
            later_snapshot,
            database,
            taxonomy_path=ROOT / "config/product_clusters.json",
        )
        paths = export_all(database.connection, tmp_path / "exports")

        assert len(first) == len(second) == len(third) == 1
        demand_count = database.connection.execute(
            "SELECT count(*) FROM explicit_demand"
        ).fetchone()
        assert demand_count is not None and demand_count[0] == 1
        assert database.connection.execute("SELECT count(*) FROM evidence").fetchone()[0] == 1
        assert database.connection.execute(
            "SELECT count(*) FROM source_records"
        ).fetchone() == (1,)
        assert len(paths) == len(TABLES) * 2 + 1
        for table in TABLES:
            assert (tmp_path / f"exports/{table}.csv").exists()
            assert (tmp_path / f"exports/{table}.parquet").exists()
        report = (tmp_path / "exports/daily_report.md").read_text()
        assert "123456-2026" in report
        assert "Needs Further Validation" in report
    finally:
        database.close()
