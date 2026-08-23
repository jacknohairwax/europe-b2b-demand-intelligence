"""Portable table and concise Markdown exports."""

from pathlib import Path

import duckdb

TABLES = [
    "explicit_demand",
    "recurrent_accounts",
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
    lines = ["# Daily Demand Intelligence Report", "", "## New Explicit Demand", ""]
    if rows:
        for notice_id, buyer, requirement, deadline, _total, _status in rows:
            lines.append(
                f"- **{notice_id}** — {buyer or 'Buyer not verified'} — "
                f"{requirement or 'Requirement not verified'}; deadline {deadline or 'Unknown'}."
            )
    else:
        lines.append("No new explicit demand in this run.")
    lines.extend(["", "## New Recurrent Buyer Signals", "", "No new validated signals."])
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


__all__ = ["export_all"]
