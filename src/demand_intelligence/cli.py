"""Command-line entry points for reproducible local and scheduled runs."""

import argparse
import asyncio
import json
import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path

from demand_intelligence.changes import ChangeEvent, detect_material_changes
from demand_intelligence.commercial import (
    ensure_operational_export_schema,
    refresh_entity_review_queue,
    refresh_existing_selected_category_commercial_view,
    refresh_selected_category_commercial_view,
)
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
from demand_intelligence.current_views import reconcile_find_tender_opportunities
from demand_intelligence.exports import export_all
from demand_intelligence.linking import link_recurrent_accounts_to_procurement
from demand_intelligence.models import EntityLinkDecision
from demand_intelligence.pipelines import (
    finalize_uk_trade_info_run,
    process_find_tender_page,
    process_ted_page,
    process_uk_trade_info_page,
)
from demand_intelligence.reconciliation import (
    UK_TRADE_INFO_CONNECTOR,
    fail_scope_run,
    start_scope_run,
    uk_trade_info_scope_key,
)
from demand_intelligence.storage import Database, SnapshotStore


def _paths(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    return Path(args.database), Path(args.raw_dir), Path(args.exports_dir)


def _operation_run_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _refresh_operational_state(
    database: Database,
    *,
    detector_run_id: str,
    connector: str,
    scope_key: str,
    completed_authoritative: bool = False,
    recurrence_threshold: int | None = None,
    observed_at: datetime | None = None,
) -> tuple[list[EntityLinkDecision], list[ChangeEvent], int]:
    """Refresh current state, links, material changes, and review candidates."""

    timestamp = observed_at or datetime.now(UTC)
    effective_threshold = (
        recurrence_threshold
        if recurrence_threshold is not None
        else _current_recurrence_threshold(database)
    )
    reconcile_find_tender_opportunities(database.connection, as_of=timestamp)
    decisions = link_recurrent_accounts_to_procurement(
        database, decided_at=timestamp
    )
    events = detect_material_changes(
        database.connection,
        run_id=detector_run_id,
        connector=connector,
        scope_key=scope_key,
        detected_at=timestamp,
        completed_authoritative=completed_authoritative,
        recurrence_threshold=effective_threshold,
    )
    review_count = refresh_entity_review_queue(
        database.connection, as_of=timestamp
    )
    return decisions, events, review_count


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
        _, changes, review_count = _refresh_operational_state(
            database,
            detector_run_id=_operation_run_id("fixture"),
            connector="TED fixture",
            scope_key=str(Path(args.fixture_path).resolve()),
        )
        export_all(database.connection, exports_dir)
    print(
        f"Processed {len(records)} TED notice(s); {len(changes)} material change(s); "
        f"{review_count} entity review candidate(s); exports: {exports_dir}"
    )
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
        _, changes, review_count = _refresh_operational_state(
            database,
            detector_run_id=_operation_run_id("ted"),
            connector="TED Search API",
            scope_key=f"ted:{args.scope}:{args.query}",
        )
        export_all(database.connection, exports_dir)
    print(
        f"Processed {count} TED notice(s); {len(changes)} material change(s); "
        f"{review_count} entity review candidate(s); exports: {exports_dir}"
    )
    return 0


async def _process_uk_trade_info(args: argparse.Namespace) -> int:
    database_path, raw_dir, exports_dir = _paths(args)
    activity_count = 0
    recurrent_company_ids: set[str] = set()
    completed_scopes: list[tuple[str, str, bool]] = []
    change_count = 0
    review_count = 0
    link_decisions: list[EntityLinkDecision] = []
    commercial_count: int | None = None
    with Database(database_path) as database:
        _validate_recurrence_policy(database, args.minimum_months)
        async with UkTradeInfoClient(
            snapshot_dir=raw_dir, state_file=Path(args.state_file)
        ) as client:
            for hs_prefix in args.hs_prefix:
                request = UkTradeInfoRequest(
                    hs_prefix=hs_prefix,
                    year=args.year,
                    trader_filter=args.trader_filter,
                )
                scope_key = uk_trade_info_scope_key(
                    year=request.year,
                    hs_prefix=request.hs_prefix,
                    trader_filter=request.trader_filter,
                )
                run_id = start_scope_run(
                    database.connection,
                    connector=UK_TRADE_INFO_CONNECTOR,
                    scope_key=scope_key,
                    scope_parameters={
                        "year": request.year,
                        "hs_prefix": request.hs_prefix,
                        "trader_filter": request.trader_filter,
                        "minimum_months": args.minimum_months,
                    },
                )
                last_next_link: str | None = None
                pages_fetched = 0
                try:
                    async for fetched in client.iter_pages(
                        request, max_pages=args.max_pages
                    ):
                        pages_fetched += 1
                        last_next_link = fetched.page.next_link
                        activity_count += len(fetched.page.records)
                        accounts = process_uk_trade_info_page(
                            fetched.page,
                            fetched.snapshot,
                            database,
                            minimum_months=args.minimum_months,
                            run_id=run_id,
                        )
                        recurrent_company_ids.update(
                            item.company_id for item in accounts
                        )
                except Exception as error:
                    fail_scope_run(
                        database.connection,
                        run_id=run_id,
                        error=f"{type(error).__name__}: {error}",
                    )
                    raise
                authoritative = pages_fetched > 0 and last_next_link is None
                if authoritative:
                    completion_reason = "source_exhausted"
                elif (
                    args.max_pages is not None
                    and pages_fetched >= args.max_pages
                ):
                    completion_reason = "max_pages"
                else:
                    completion_reason = "pagination_stopped_with_next_link"
                run_result, rebuilt = finalize_uk_trade_info_run(
                    database,
                    run_id=run_id,
                    authoritative=authoritative,
                    completion_reason=completion_reason,
                    minimum_months=args.minimum_months,
                )
                recurrent_company_ids.difference_update(
                    run_result.affected_company_ids
                )
                recurrent_company_ids.update(item.company_id for item in rebuilt)
                completed_scopes.append(
                    (run_result.run_id, scope_key, run_result.authoritative)
                )
                link_decisions, changes, review_count = _refresh_operational_state(
                    database,
                    detector_run_id=run_result.run_id,
                    connector=UK_TRADE_INFO_CONNECTOR,
                    scope_key=scope_key,
                    completed_authoritative=run_result.authoritative,
                    recurrence_threshold=args.minimum_months,
                )
                change_count += len(changes)

        authoritative_run_ids = [
            run_id for run_id, _, authoritative in completed_scopes if authoritative
        ]
        if (
            completed_scopes
            and len(authoritative_run_ids) == len(completed_scopes)
        ):
            commercial_count = refresh_selected_category_commercial_view(
                database.connection,
                year=args.year,
                taxonomy_path=Path(args.taxonomy),
                authoritative_run_ids=authoritative_run_ids,
                recurrence_threshold=args.minimum_months,
                trader_filter=args.trader_filter,
                hs_prefixes=args.hs_prefix,
            )
        export_all(database.connection, exports_dir)
    accepted_links = _count_current_run_accepted_links(
        link_decisions, recurrent_company_ids
    )
    commercial_status = (
        str(commercial_count)
        if commercial_count is not None
        else "not refreshed (partial run)"
    )
    print(
        f"Processed {activity_count} UK Trade Info activity row(s); "
        f"{len(recurrent_company_ids)} recurrent account(s); "
        f"{accepted_links} accepted procurement link(s); "
        f"{change_count} material change(s); {review_count} review candidate(s); "
        f"commercial rows: {commercial_status}; exports: {exports_dir}"
    )
    return 0


def _count_current_run_accepted_links(
    decisions: list[EntityLinkDecision], recurrent_company_ids: set[str]
) -> int:
    return sum(
        item.status == "accepted" and item.left_company_id in recurrent_company_ids
        for item in decisions
    )


def _find_tender_scope_key(
    args: argparse.Namespace, stages: list[FindTenderStage | None]
) -> str:
    if args.notice_id:
        return f"find-tender:notice:{args.notice_id}"
    rendered_stages = ",".join(str(stage) for stage in stages if stage)
    start = args.updated_from.isoformat() if args.updated_from else "open"
    end = args.updated_to.isoformat() if args.updated_to else "server-cutoff"
    return f"find-tender:stages:{rendered_stages}:updated:{start}:{end}"


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected a positive integer") from error
    if parsed < 1:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return parsed


def _activity_year(value: str) -> int:
    parsed = _positive_int(value)
    if parsed < 2000 or parsed > 2100:
        raise argparse.ArgumentTypeError("year must be between 2000 and 2100")
    return parsed


def _hs_prefix(value: str) -> str:
    normalized = "".join(value.split())
    if not normalized.isdigit() or not 2 <= len(normalized) <= 8:
        raise argparse.ArgumentTypeError(
            "HS/CN prefix must contain between 2 and 8 digits"
        )
    return normalized


def _selection_prefixes(
    *,
    taxonomy_path: Path,
    category_ids: list[str] | None,
    hs_prefixes: list[str] | None,
) -> list[str]:
    if hs_prefixes:
        return sorted(set(hs_prefixes))
    requested = set(category_ids or [])
    payload = json.loads(taxonomy_path.read_text())
    clusters = payload.get("clusters", []) if isinstance(payload, dict) else []
    found: dict[str, list[str]] = {}
    for raw in clusters:
        if not isinstance(raw, dict) or raw.get("id") not in requested:
            continue
        found[str(raw["id"])] = [
            _hs_prefix(str(item)) for item in raw.get("candidate_hs_codes", [])
        ]
    missing = sorted(requested - set(found))
    if missing:
        raise ValueError(f"unknown taxonomy category ID(s): {', '.join(missing)}")
    return sorted({prefix for values in found.values() for prefix in values})


def _run_parameters(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return {str(key): item for key, item in parsed.items()}
    return {}


def _current_recurrence_threshold(database: Database) -> int:
    row = database.connection.execute(
        """SELECT recurrence_threshold FROM change_detection_runs
           ORDER BY detected_at DESC, run_id DESC LIMIT 1"""
    ).fetchone()
    threshold = int(row[0]) if row and row[0] is not None else 2
    if threshold < 2:
        raise ValueError("persisted recurrence threshold must be at least 2")
    return threshold


def _validate_recurrence_policy(database: Database, requested_threshold: int) -> None:
    row = database.connection.execute(
        """SELECT scope_parameters FROM ingestion_run_scopes
           WHERE connector = ? AND status IN ('completed', 'partial')
           ORDER BY coalesce(completed_at, started_at) DESC, run_id DESC
           LIMIT 1""",
        [UK_TRADE_INFO_CONNECTOR],
    ).fetchone()
    if row is None:
        return
    existing_threshold = int(
        str(_run_parameters(row[0]).get("minimum_months", 2))
    )
    if requested_threshold != existing_threshold:
        raise ValueError(
            "this database uses a fixed recurrence threshold of "
            f"{existing_threshold}; requested {requested_threshold}. "
            "Use a new database to establish a different policy."
        )


def _latest_authoritative_runs(
    database: Database,
    *,
    year: int,
    prefixes: list[str],
    trader_filter: str,
) -> list[str]:
    rows = database.connection.execute(
        """SELECT run_id, scope_parameters, completed_at
           FROM ingestion_run_scopes
           WHERE connector = ? AND status = 'completed' AND is_authoritative
           ORDER BY completed_at DESC, run_id DESC""",
        [UK_TRADE_INFO_CONNECTOR],
    ).fetchall()
    selected: set[str] = set()
    missing: list[str] = []
    for requested_prefix in prefixes:
        candidates: list[tuple[int, object, str]] = []
        for run_id, raw_parameters, completed_at in rows:
            parameters = _run_parameters(raw_parameters)
            if int(str(parameters.get("year", 0))) != year:
                continue
            observed_filter = str(parameters.get("trader_filter", ""))
            compatible_filter = observed_filter == trader_filter or (
                trader_filter != "All" and observed_filter == "All"
            )
            if not compatible_filter:
                continue
            run_prefix = str(parameters.get("hs_prefix", ""))
            if requested_prefix.startswith(run_prefix):
                candidates.append((len(run_prefix), completed_at, str(run_id)))
        if not candidates:
            missing.append(requested_prefix)
            continue
        # Prefer the narrowest compatible scope, then its latest completed run.
        best_width = max(item[0] for item in candidates)
        selected.add(
            max(
                (item for item in candidates if item[0] == best_width),
                key=lambda item: (item[1], item[2]),
            )[2]
        )
    if missing:
        raise ValueError(
            "no completed authoritative UK Trade Info run covers "
            f"{year} {trader_filter} prefix(es): {', '.join(missing)}"
        )
    return sorted(selected)


def _authoritative_recurrence_threshold(
    database: Database, run_ids: list[str]
) -> int:
    thresholds: set[int] = set()
    for run_id in run_ids:
        row = database.connection.execute(
            "SELECT scope_parameters FROM ingestion_run_scopes WHERE run_id = ?",
            [run_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"unknown completed-run scope: {run_id}")
        parameters = _run_parameters(row[0])
        thresholds.add(int(str(parameters.get("minimum_months", 2))))
    if len(thresholds) != 1:
        raise ValueError(
            "selected authoritative runs use different recurrence thresholds"
        )
    threshold = next(iter(thresholds))
    if threshold < 2:
        raise ValueError("authoritative recurrence threshold must be at least 2")
    return threshold


def _validate_authoritative_trader_filter(
    database: Database,
    run_ids: list[str],
    *,
    requested_filter: str,
) -> None:
    for run_id in run_ids:
        row = database.connection.execute(
            "SELECT scope_parameters FROM ingestion_run_scopes WHERE run_id = ?",
            [run_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"unknown completed-run scope: {run_id}")
        observed_filter = str(
            _run_parameters(row[0]).get("trader_filter", "")
        )
        compatible = observed_filter == requested_filter or (
            requested_filter != "All" and observed_filter == "All"
        )
        if not compatible:
            raise ValueError(
                f"run {run_id} trader filter {observed_filter!r} does not cover "
                f"requested {requested_filter!r}"
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
        scope_key = _find_tender_scope_key(args, stages)
        link_decisions, changes, review_count = _refresh_operational_state(
            database,
            detector_run_id=_operation_run_id("find-tender"),
            connector="Find a Tender OCDS API",
            scope_key=scope_key,
        )
        commercial_count = refresh_existing_selected_category_commercial_view(
            database.connection
        )
        export_all(database.connection, exports_dir)
    accepted_links = sum(item.status == "accepted" for item in link_decisions)
    print(
        f"Processed {release_count} Find a Tender release(s); "
        f"{active_demand_count} active categorized demand record(s); "
        f"{accepted_links} accepted trader link(s); "
        f"{len(changes)} material change(s); "
        f"{review_count} entity review candidate(s); "
        f"commercial rows: "
        f"{commercial_count if commercial_count is not None else 'not yet selected'}; "
        f"exports: {exports_dir}"
    )
    return 0


def _init_db(args: argparse.Namespace) -> int:
    with Database(Path(args.database)) as database:
        ensure_operational_export_schema(database.connection)
    print(f"Initialized {args.database}")
    return 0


def _link_sources(args: argparse.Namespace) -> int:
    with Database(Path(args.database)) as database:
        decisions, changes, review_count = _refresh_operational_state(
            database,
            detector_run_id=_operation_run_id("link"),
            connector="Cross-source entity linking",
            scope_key="cross-source:uk-trade-info:find-tender",
        )
        commercial_count = refresh_existing_selected_category_commercial_view(
            database.connection
        )
        export_all(database.connection, Path(args.exports_dir))
    accepted = sum(item.status == "accepted" for item in decisions)
    print(
        f"Evaluated {len(decisions)} procurement link candidate(s); "
        f"accepted {accepted}; {len(changes)} material change(s); "
        f"{review_count} review candidate(s); commercial rows: "
        f"{commercial_count if commercial_count is not None else 'not yet selected'}; "
        f"exports: {args.exports_dir}"
    )
    return 0


def _commercial_export(args: argparse.Namespace) -> int:
    database_path, _, exports_dir = _paths(args)
    timestamp = datetime.now(UTC)
    taxonomy_path = Path(args.taxonomy)
    prefixes = _selection_prefixes(
        taxonomy_path=taxonomy_path,
        category_ids=args.category,
        hs_prefixes=args.hs_prefix,
    )
    with Database(database_path) as database:
        authoritative_run_ids = args.authoritative_run_id or _latest_authoritative_runs(
            database,
            year=args.year,
            prefixes=prefixes,
            trader_filter=args.trader_filter,
        )
        recurrence_threshold = _authoritative_recurrence_threshold(
            database, authoritative_run_ids
        )
        _validate_recurrence_policy(database, recurrence_threshold)
        _validate_authoritative_trader_filter(
            database,
            authoritative_run_ids,
            requested_filter=args.trader_filter,
        )
        decisions, changes, review_count = _refresh_operational_state(
            database,
            detector_run_id=_operation_run_id("commercial-export"),
            connector="Commercial current view",
            scope_key=(
                f"commercial:{args.year}:{args.trader_filter}:"
                + ",".join(prefixes)
            ),
            recurrence_threshold=recurrence_threshold,
            observed_at=timestamp,
        )
        row_count = refresh_selected_category_commercial_view(
            database.connection,
            year=args.year,
            taxonomy_path=taxonomy_path,
            authoritative_run_ids=authoritative_run_ids,
            recurrence_threshold=recurrence_threshold,
            trader_filter=args.trader_filter,
            category_ids=args.category,
            hs_prefixes=args.hs_prefix,
            as_of=timestamp,
        )
        export_all(database.connection, exports_dir)
    accepted = sum(item.status == "accepted" for item in decisions)
    print(
        f"Materialized {row_count} selected-category commercial row(s); "
        f"{accepted} accepted link(s); {review_count} review candidate(s); "
        f"{len(changes)} material change(s); authoritative run(s): "
        f"{', '.join(authoritative_run_ids)}; exports: {exports_dir}"
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
    ted.add_argument("--limit", type=_positive_int, default=100)
    ted.add_argument("--max-pages", type=_positive_int, default=1)
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
    find_tender.add_argument("--limit", type=_positive_int, default=100)
    find_tender.add_argument("--max-pages", type=_positive_int, default=1)
    find_tender.add_argument(
        "--state-file", default="data/snapshots/find-tender-state.json"
    )
    _common(find_tender)
    find_tender.set_defaults(handler=_process_find_tender)

    ukti = subparsers.add_parser(
        "uk-trade-info", help="Fetch named HMRC trader/product/month observations"
    )
    ukti.add_argument("--hs-prefix", action="append", type=_hs_prefix, required=True)
    ukti.add_argument("--year", type=_activity_year, required=True)
    ukti.add_argument(
        "--trader-filter",
        choices=["All", "Importers", "Exporters"],
        default="Importers",
    )
    ukti.add_argument("--max-pages", type=_positive_int)
    ukti.add_argument("--minimum-months", type=_minimum_months, default=2)
    ukti.add_argument("--state-file", default="data/snapshots/uk-trade-info-state.json")
    _common(ukti)
    ukti.set_defaults(handler=_process_uk_trade_info)

    commercial = subparsers.add_parser(
        "commercial-export",
        help="Materialize an authoritative selected-category commercial current view",
    )
    commercial.add_argument("--year", type=_activity_year, required=True)
    selection = commercial.add_mutually_exclusive_group(required=True)
    selection.add_argument("--category", action="append")
    selection.add_argument("--hs-prefix", action="append", type=_hs_prefix)
    commercial.add_argument(
        "--trader-filter",
        choices=["All", "Importers", "Exporters"],
        default="Importers",
        help="Select compatible completed UK Trade Info scope coverage",
    )
    commercial.add_argument(
        "--authoritative-run-id",
        action="append",
        help="Use an explicit completed UKTI run; otherwise resolve the latest coverage",
    )
    _common(commercial)
    commercial.set_defaults(handler=_commercial_export)
    return parser


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    result = args.handler(args)
    return asyncio.run(result) if asyncio.iscoroutine(result) else int(result)


if __name__ == "__main__":
    raise SystemExit(main())
