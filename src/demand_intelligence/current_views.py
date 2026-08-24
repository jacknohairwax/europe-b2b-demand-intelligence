"""Reconcile derived current views against versioned source observations."""

import json
from datetime import UTC, datetime

import duckdb

FIND_TENDER_SOURCE = "Find a Tender OCDS API"
CLOSED_TENDER_STATUSES = {"cancelled", "complete", "unsuccessful", "withdrawn"}


def _timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _stages(value: object) -> set[str]:
    if not isinstance(value, str):
        return set()
    try:
        parsed: object = json.loads(value)
    except json.JSONDecodeError:
        return set()
    if not isinstance(parsed, list):
        return set()
    return {str(item) for item in parsed}


def reconcile_find_tender_opportunities(
    connection: duckdb.DuckDBPyConnection, *, as_of: datetime | None = None
) -> int:
    """Remove FTS opportunities that are expired or superseded by closure/award facts."""

    reference_time = as_of or datetime.now(UTC)
    if reference_time.tzinfo is None:
        reference_time = reference_time.replace(tzinfo=UTC)
    internal_ids = [
        str(row[0])
        for row in connection.execute(
            "SELECT notice_id FROM explicit_demand WHERE source = ?",
            [FIND_TENDER_SOURCE],
        ).fetchall()
    ]
    stale_ids: set[str] = set()
    for internal_id in internal_ids:
        release_id = internal_id.removeprefix("find-tender:")
        versions = connection.execute(
            """SELECT ocid, tender_status, stages, CAST(deadline AS VARCHAR),
                      CAST(publication_date AS VARCHAR), CAST(retrieved_at AS VARCHAR)
               FROM procurement_releases
               WHERE source_name = ? AND release_id = ?
               ORDER BY retrieved_at DESC, release_payload_sha256 DESC""",
            [FIND_TENDER_SOURCE, release_id],
        ).fetchall()
        if not versions:
            continue
        current = versions[0]
        deadline = _timestamp(current[3])
        current_status = str(current[1]) if current[1] else None
        if current_status in CLOSED_TENDER_STATUSES or (
            deadline is not None and deadline <= reference_time
        ):
            stale_ids.add(internal_id)
            continue

        ocid = str(current[0]) if current[0] else None
        if ocid is None:
            continue
        active_observed_at = _timestamp(current[4]) or _timestamp(current[5])
        process_releases = connection.execute(
            """SELECT release_id, tender_status, stages,
                      CAST(publication_date AS VARCHAR), CAST(retrieved_at AS VARCHAR)
               FROM procurement_releases
               WHERE source_name = ? AND ocid = ? AND release_id != ?""",
            [FIND_TENDER_SOURCE, ocid, release_id],
        ).fetchall()
        for process_release in process_releases:
            stages = _stages(process_release[2])
            observed_at = _timestamp(process_release[3]) or _timestamp(
                process_release[4]
            )
            is_later = (
                active_observed_at is None
                or observed_at is None
                or observed_at >= active_observed_at
            )
            if (
                stages & {"award", "contract"}
                and str(process_release[1] or "") in CLOSED_TENDER_STATUSES
                and is_later
            ):
                stale_ids.add(internal_id)
                break

    for internal_id in stale_ids:
        connection.execute(
            "DELETE FROM opportunity_scores WHERE notice_id = ?", [internal_id]
        )
        connection.execute(
            "DELETE FROM explicit_demand WHERE notice_id = ?", [internal_id]
        )
    return len(stale_ids)


__all__ = ["reconcile_find_tender_opportunities"]
