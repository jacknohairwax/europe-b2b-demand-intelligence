"""Portable table and concise Markdown exports."""

from pathlib import Path

import duckdb

from demand_intelligence.current_views import reconcile_find_tender_opportunities

TABLES = [
    "explicit_demand",
    "procurement_releases",
    "procurement_awards",
    "procurement_parties",
    "trade_activity",
    "canonical_companies",
    "company_source_roles",
    "entity_link_decisions",
    "recurrent_accounts",
    "recurrent_account_scopes",
    "country_category",
    "evidence",
    "signals",
    "opportunity_scores",
]


def _escape_sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _daily_report(connection: duckdb.DuckDBPyConnection) -> str:
    rows = connection.execute(
        """SELECT d.notice_id, d.buyer, d.requirement, d.deadline,
                  s.total_score, s.recommendation_status
           FROM explicit_demand d
           LEFT JOIN opportunity_scores s USING (notice_id)
           ORDER BY d.publication_date DESC NULLS LAST, d.notice_id
           LIMIT 10"""
    ).fetchall()
    recurrent_rows = connection.execute(
        """SELECT company_name, company_type, months_active, buyer_role,
                  json_array_length(procurement_evidence) AS procurement_count
           FROM recurrent_accounts
           ORDER BY procurement_count DESC, months_active DESC, company_name
           LIMIT 10"""
    ).fetchall()
    lines = ["# Daily Demand Intelligence Report", "", "## New Explicit Demand", ""]
    if rows:
        for notice_id, buyer, requirement, deadline, _total, _status in rows:
            lines.append(
                f"- **{notice_id}** — {buyer or 'Buyer not verified'} — "
                f"{requirement or 'Requirement not verified'}; deadline {deadline or 'Unknown'}."
            )
    else:
        lines.append("No new explicit demand in this run.")
    lines.extend(["", "## New Recurrent Buyer Signals", ""])
    if recurrent_rows:
        for company_name, company_type, months_active, buyer_role, procurement_count in (
            recurrent_rows
        ):
            procurement = (
                f"{procurement_count} linked procurement evidence record(s); "
                f"role {buyer_role}"
                if procurement_count
                else "public procurement role not verified"
            )
            lines.append(
                f"- **{company_name}** — {company_type}; {months_active} active month(s); "
                f"{procurement}."
            )
    else:
        lines.append("No recurrent trader accounts in this run.")
    lines.extend(["", "## High-Priority Validation Queue", ""])
    if rows:
        for notice_id, buyer, _requirement, _deadline, total, status in rows:
            lines.append(
                f"- {notice_id} ({buyer or 'Buyer not verified'}): {status}; score {total or 0}/50."
            )
    else:
        lines.append("Queue is empty.")
    lines.extend(
        [
            "",
            "## Evidence Gaps",
            "",
            "- Purchasing contacts, recurrence, eligibility, compliance, and "
            "supplier-switching evidence are not yet verified.",
            "",
        ]
    )
    return "\n".join(lines)


def export_all(connection: duckdb.DuckDBPyConnection, output_dir: Path) -> list[Path]:
    reconcile_find_tender_opportunities(connection)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for table in TABLES:
        csv_path = output_dir / f"{table}.csv"
        parquet_path = output_dir / f"{table}.parquet"
        csv_target = _escape_sql_path(csv_path)
        connection.execute(f"COPY (SELECT * FROM {table}) TO '{csv_target}' (HEADER)")
        connection.execute(
            f"COPY (SELECT * FROM {table}) TO '{_escape_sql_path(parquet_path)}' (FORMAT PARQUET)"
        )
        paths.extend([csv_path, parquet_path])
    report_path = output_dir / "daily_report.md"
    report_path.write_text(_daily_report(connection))
    paths.append(report_path)
    return paths


__all__ = ["TABLES", "export_all"]
