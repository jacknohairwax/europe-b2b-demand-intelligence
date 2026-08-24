"""Command-line entry points for reproducible local and scheduled runs."""

import argparse
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path

from demand_intelligence.connectors import TedSearchClient, TedSearchPage, TedSearchRequest
from demand_intelligence.connectors.find_tender import (
    FIND_TENDER_RELEASE_PACKAGES_URL,
    FindTenderClient,
    FindTenderRequest,
    FindTenderStage,
)
from demand_intelligence.connectors.uk_trade_info import (
    UkTradeInfoClient,
    UkTradeInfoRequest,
)
from demand_intelligence.exports import export_all
from demand_intelligence.linking import link_recurrent_accounts_to_procurement
from demand_intelligence.models import EntityLinkDecision
from demand_intelligence.pipelines import (
    process_find_tender_page,
    process_ted_page,
    process_uk_trade_info_page,
)
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


async def _process_uk_trade_info(args: argparse.Namespace) -> int:
    database_path, raw_dir, exports_dir = _paths(args)
    activity_count = 0
    recurrent_company_ids: set[str] = set()
    with Database(database_path) as database:
        async with UkTradeInfoClient(
            snapshot_dir=raw_dir, state_file=Path(args.state_file)
        ) as client:
            for hs_prefix in args.hs_prefix:
                request = UkTradeInfoRequest(
                    hs_prefix=hs_prefix,
                    year=args.year,
                    trader_filter=args.trader_filter,
                )
                async for fetched in client.iter_pages(
                    request, max_pages=args.max_pages
                ):
                    activity_count += len(fetched.page.records)
                    accounts = process_uk_trade_info_page(
                        fetched.page,
                        fetched.snapshot,
                        database,
                        minimum_months=args.minimum_months,
                    )
                    recurrent_company_ids.update(item.company_id for item in accounts)
        link_decisions = link_recurrent_accounts_to_procurement(database)
        export_all(database.connection, exports_dir)
    accepted_links = _count_current_run_accepted_links(
        link_decisions, recurrent_company_ids
    )
    print(
        f"Processed {activity_count} UK Trade Info activity row(s); "
        f"{len(recurrent_company_ids)} recurrent account(s); "
        f"{accepted_links} accepted procurement link(s); exports: {exports_dir}"
    )
    return 0


def _count_current_run_accepted_links(
    decisions: list[EntityLinkDecision], recurrent_company_ids: set[str]
) -> int:
    return sum(
        item.status == "accepted" and item.left_company_id in recurrent_company_ids
        for item in decisions
    )


def _api_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "expected an ISO datetime such as 2026-08-01T00:00:00"
        ) from error


def _minimum_months(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("minimum months must be an integer") from error
    if parsed < 2:
        raise argparse.ArgumentTypeError(
            "minimum months must be at least 2 for repeated activity"
        )
    return parsed


def _stage_state_file(path: Path, stage: FindTenderStage | None) -> Path:
    if stage is None:
        return path
    return path.with_name(f"{path.stem}-{stage}{path.suffix}")


async def _process_find_tender(args: argparse.Namespace) -> int:
    database_path, raw_dir, exports_dir = _paths(args)
    stages: list[FindTenderStage | None]
    if args.stage:
        stages = list(dict.fromkeys(args.stage))
    elif args.notice_id:
        stages = [None]
    else:
        stages = ["tender", "award"]

    base_url = FIND_TENDER_RELEASE_PACKAGES_URL
    if args.notice_id:
        base_url = f"{base_url}/{args.notice_id}"
    release_count = 0
    active_demand_count = 0
    with Database(database_path) as database:
        for stage in stages:
            request = FindTenderRequest(
                limit=args.limit,
                updatedFrom=args.updated_from,
                updatedTo=args.updated_to,
                stages=stage,
            )
            async with FindTenderClient(
                snapshot_dir=raw_dir,
                state_file=_stage_state_file(Path(args.state_file), stage),
                base_url=base_url,
            ) as client:
                async for fetched in client.iter_pages(
                    request, max_pages=args.max_pages
                ):
                    release_count += len(fetched.package.releases)
                    active_demand_count += len(
                        process_find_tender_page(
                            fetched.package,
                            fetched.snapshot,
                            database,
                            taxonomy_path=Path(args.taxonomy),
                        )
                    )
        link_decisions = link_recurrent_accounts_to_procurement(database)
        export_all(database.connection, exports_dir)
    accepted_links = sum(item.status == "accepted" for item in link_decisions)
    print(
        f"Processed {release_count} Find a Tender release(s); "
        f"{active_demand_count} active categorized demand record(s); "
        f"{accepted_links} accepted trader link(s); "
        f"exports: {exports_dir}"
    )
    return 0


def _init_db(args: argparse.Namespace) -> int:
    with Database(Path(args.database)):
        pass
    print(f"Initialized {args.database}")
    return 0


def _link_sources(args: argparse.Namespace) -> int:
    with Database(Path(args.database)) as database:
        decisions = link_recurrent_accounts_to_procurement(database)
        export_all(database.connection, Path(args.exports_dir))
    accepted = sum(item.status == "accepted" for item in decisions)
    print(
        f"Evaluated {len(decisions)} procurement link candidate(s); "
        f"accepted {accepted}; exports: {args.exports_dir}"
    )
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

    link = subparsers.add_parser(
        "link", help="Evaluate conservative UK trader-to-procurement entity links"
    )
    link.add_argument("--database", default="data/demand.duckdb")
    link.add_argument("--exports-dir", default="data/exports")
    link.set_defaults(handler=_link_sources)

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

    find_tender = subparsers.add_parser(
        "find-tender", help="Fetch official UK OCDS tender and award releases"
    )
    find_tender.add_argument(
        "--stage",
        action="append",
        choices=["planning", "tender", "award"],
        help="Repeat for separate streams; defaults to tender plus award",
    )
    find_tender.add_argument("--updated-from", type=_api_datetime)
    find_tender.add_argument("--updated-to", type=_api_datetime)
    find_tender.add_argument("--notice-id")
    find_tender.add_argument("--limit", type=int, default=100)
    find_tender.add_argument("--max-pages", type=int, default=1)
    find_tender.add_argument(
        "--state-file", default="data/snapshots/find-tender-state.json"
    )
    _common(find_tender)
    find_tender.set_defaults(handler=_process_find_tender)

    ukti = subparsers.add_parser(
        "uk-trade-info", help="Fetch named HMRC trader/product/month observations"
    )
    ukti.add_argument("--hs-prefix", action="append", required=True)
    ukti.add_argument("--year", type=int, required=True)
    ukti.add_argument(
        "--trader-filter",
        choices=["All", "Importers", "Exporters"],
        default="Importers",
    )
    ukti.add_argument("--max-pages", type=int)
    ukti.add_argument("--minimum-months", type=_minimum_months, default=2)
    ukti.add_argument("--state-file", default="data/snapshots/uk-trade-info-state.json")
    _common(ukti)
    ukti.set_defaults(handler=_process_uk_trade_info)
    return parser


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    result = args.handler(args)
    return asyncio.run(result) if asyncio.iscoroutine(result) else int(result)


if __name__ == "__main__":
    raise SystemExit(main())
