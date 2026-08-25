"""Portable table and concise Markdown exports."""

from pathlib import Path

import duckdb

from demand_intelligence.commercial import ensure_operational_export_schema

TABLES = [
    "ingestion_runs",
    "ingestion_run_scopes",
    "ingestion_run_records",
    "active_scope_leases",
    "source_scope_membership",
    "source_records",
    "explicit_demand",
    "procurement_releases",
    "procurement_release_observations",
    "release_transitions",
    "procurement_awards",
    "procurement_parties",
    "opportunity_current_state",
    "opportunity_state_history",
    "trade_activity",
    "canonical_companies",
    "company_source_roles",
    "entity_link_decisions",
    "entity_review_queue",
    "recurrent_accounts",
    "recurrent_account_scopes",
    "selected_category_commercial_view_state",
    "selected_category_commercial_view",
    "country_category",
    "evidence",
    "signals",
    "opportunity_scores",
    "material_change_state",
    "change_detection_runs",
    "change_events",
]


def _escape_sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _export_query(table: str) -> str:
    if table == "entity_review_queue":
        return (
            "SELECT * FROM entity_review_queue WHERE is_current "
            "ORDER BY case_id, right_company_id"
        )
    if table == "selected_category_commercial_view":
        return "SELECT * FROM selected_category_commercial_view ORDER BY priority_rank"
    if table == "change_events":
        return "SELECT * FROM change_events ORDER BY detected_at, event_id"
    return f"SELECT * FROM {table}"


def _daily_report(connection: duckdb.DuckDBPyConnection) -> str:
    latest = connection.execute(
        """SELECT run_id, detected_at, event_count
           FROM change_detection_runs
           ORDER BY detected_at DESC, run_id DESC LIMIT 1"""
    ).fetchone()
    lines = ["# Material Change Report", ""]
    if latest is None:
        lines.extend(["No change-detection run has been recorded.", ""])
    else:
        run_id = str(latest[0])
        lines.extend(
            [
                f"Latest run: `{run_id}` at {latest[1]}; "
                f"{int(latest[2])} material change(s).",
                "",
                "## Material changes",
                "",
            ]
        )
        events = connection.execute(
            """SELECT event_type, subject_id, field_name, old_value, new_value,
                      confidence
               FROM change_events WHERE run_id = ?
               ORDER BY event_type, subject_id, event_id""",
            [run_id],
        ).fetchall()
        if not events:
            lines.append("No material changes detected in the latest run.")
        for event_type, subject_id, field_name, old_value, new_value, confidence in events:
            field = f" ({field_name})" if field_name else ""
            lines.append(
                f"- **{event_type}** — `{subject_id}`{field}; "
                f"{_preview(old_value)} → {_preview(new_value)}; "
                f"confidence {float(confidence):.2f}."
            )

    commercial_rows = connection.execute(
        """SELECT company_name, commodity_code_level, commodity_code,
                  recurrent_month_count, recurrence_threshold, trade_role,
                  has_procurement_evidence, evidence_source_count, confidence,
                  validation_status
           FROM selected_category_commercial_view
           ORDER BY priority_rank LIMIT 10"""
    ).fetchall()
    lines.extend(["", "## Selected-category current view", ""])
    if not commercial_rows:
        lines.append("No selected-category current view has been materialized.")
    for row in commercial_rows:
        procurement = "procurement linked" if row[6] else "procurement not verified"
        lines.append(
            f"- **{row[0]}** — {row[1]} {row[2]}; {row[3]} recurrent month(s); "
            f"threshold {row[4]}; {row[5]}; {procurement}; "
            f"{row[7]} evidence source(s); confidence {float(row[8]):.2f}; "
            f"{row[9]}."
        )

    review_count = connection.execute(
        "SELECT count(*) FROM entity_review_queue WHERE is_current"
    ).fetchone()
    lines.extend(
        [
            "",
            "## Human entity-review queue",
            "",
            f"{int(review_count[0]) if review_count else 0} current candidate(s) require review.",
            "",
            "## Evidence boundary",
            "",
            "- Trade recurrence is product/month presence evidence, not "
            "trader-level value or volume.",
            "- Procurement links remain separate source identities and ambiguous "
            "candidates are never merged.",
            "- CPV-to-HS mappings remain candidate mappings requiring validation.",
            "",
        ]
    )
    return "\n".join(lines)


def _preview(value: object, *, limit: int = 180) -> str:
    rendered = "null" if value is None else " ".join(str(value).split())
    return rendered if len(rendered) <= limit else rendered[: limit - 1] + "…"


def export_all(connection: duckdb.DuckDBPyConnection, output_dir: Path) -> list[Path]:
    ensure_operational_export_schema(connection)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for table in TABLES:
        csv_path = output_dir / f"{table}.csv"
        parquet_path = output_dir / f"{table}.parquet"
        csv_target = _escape_sql_path(csv_path)
        query = _export_query(table)
        connection.execute(f"COPY ({query}) TO '{csv_target}' (HEADER)")
        connection.execute(
            f"COPY ({query}) TO '{_escape_sql_path(parquet_path)}' (FORMAT PARQUET)"
        )
        paths.extend([csv_path, parquet_path])
    report_path = output_dir / "daily_report.md"
    report_path.write_text(_daily_report(connection))
    paths.append(report_path)
    return paths


__all__ = ["TABLES", "export_all"]
