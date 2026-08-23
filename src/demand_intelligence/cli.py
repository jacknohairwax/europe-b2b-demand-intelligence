"""Command-line entry points for reproducible local and scheduled runs."""

import argparse
import asyncio
import json
import logging
from pathlib import Path

from demand_intelligence.connectors import TedSearchClient, TedSearchPage, TedSearchRequest
from demand_intelligence.exports import export_all
from demand_intelligence.pipelines import process_ted_page
from demand_intelligence.storage import Database, SnapshotStore


def _paths(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    return Path(args.database), Path(args.raw_dir), Path(args.exports_dir)


def _process_fixture(args: argparse.Namespace) -> int:
    database_path, raw_dir, exports_dir = _paths(args)
    payload = json.loads(Path(args.fixture_path).read_text())
    snapshot = SnapshotStore(raw_dir).write_json(
        source="ted-fixture",
        source_record_id="page-1",
        payload=payload,
        request=None,
        source_url="https://api.ted.europa.eu/v3/notices/search",
    )
    with Database(database_path) as database:
        records = process_ted_page(
            TedSearchPage.model_validate(payload),
            snapshot,
            database,
            taxonomy_path=Path(args.taxonomy),
        )
        export_all(database.connection, exports_dir)
    print(f"Processed {len(records)} TED notice(s); exports: {exports_dir}")
    return 0


async def _process_ted(args: argparse.Namespace) -> int:
    database_path, raw_dir, exports_dir = _paths(args)
    request = TedSearchRequest(
        query=args.query,
        scope=args.scope,
        limit=args.limit,
        paginationMode=args.pagination_mode,
    )
    count = 0
    with Database(database_path) as database:
        async with TedSearchClient(
            snapshot_dir=raw_dir, state_file=Path(args.state_file)
        ) as client:
            async for fetched in client.iter_pages(request, max_pages=args.max_pages):
                records = process_ted_page(
                    fetched.page,
                    fetched.snapshot,
                    database,
                    taxonomy_path=Path(args.taxonomy),
                )
                count += len(records)
        export_all(database.connection, exports_dir)
    print(f"Processed {count} TED notice(s); exports: {exports_dir}")
    return 0


def _init_db(args: argparse.Namespace) -> int:
    with Database(Path(args.database)):
        pass
    print(f"Initialized {args.database}")
    return 0


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--database", default="data/demand.duckdb")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--exports-dir", default="data/exports")
    parser.add_argument("--taxonomy", default="config/product_clusters.json")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="European B2B demand intelligence harness")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init-db", help="Initialize the DuckDB schema")
    init.add_argument("--database", default="data/demand.duckdb")
    init.set_defaults(handler=_init_db)

    fixture = subparsers.add_parser("fixture", help="Process an offline TED JSON fixture")
    fixture.add_argument("fixture_path")
    _common(fixture)
    fixture.set_defaults(handler=_process_fixture)

    ted = subparsers.add_parser("ted", help="Fetch and process TED Search API records")
    ted.add_argument("--query", required=True)
    ted.add_argument("--scope", choices=["ALL", "ACTIVE", "LATEST"], default="ACTIVE")
    ted.add_argument("--limit", type=int, default=100)
    ted.add_argument("--max-pages", type=int, default=1)
    ted.add_argument(
        "--pagination-mode", choices=["PAGE_NUMBER", "ITERATION"], default="PAGE_NUMBER"
    )
    ted.add_argument("--state-file", default="data/snapshots/ted-state.json")
    _common(ted)
    ted.set_defaults(handler=_process_ted)
    return parser


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    result = args.handler(args)
    return asyncio.run(result) if asyncio.iscoroutine(result) else int(result)


if __name__ == "__main__":
    raise SystemExit(main())
