"""Reconcile derived current views against versioned source observations."""

import hashlib
import json
from datetime import UTC, datetime
from typing import TypedDict

import duckdb

FIND_TENDER_SOURCE = "Find a Tender OCDS API"
CLOSED_TENDER_STATUSES = {"cancelled", "complete", "unsuccessful", "withdrawn"}


class _ReleaseState(TypedDict):
    release_id: str
    ocid: str | None
    status: str | None
    deadline: datetime | None
    publication_at: datetime | None
    observed_at: datetime | None
    payload_sha256: str
    evidence_ids: list[str]
    product_category: str | None
    stages: set[str]


def _timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif value is None:
        return None
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _json_strings(value: object) -> list[str]:
    parsed: object = value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if item is not None]


def reconcile_find_tender_opportunities(
    connection: duckdb.DuckDBPyConnection, *, as_of: datetime | None = None
) -> int:
    """Materialize active/closed/expired FTS state and retract inactive demand."""

    reference_time = _timestamp(as_of or datetime.now(UTC))
    assert reference_time is not None
    existing_release_ids = {
        str(row[0])
        for row in connection.execute(
            """SELECT release_id FROM opportunity_current_state
               WHERE source_name = ?""",
            [FIND_TENDER_SOURCE],
        ).fetchall()
    }
    rows = connection.execute(
        """SELECT release.release_id, release.ocid, release.tender_status,
                  release.deadline, release.publication_date,
                  coalesce(
                    (SELECT max(observation.observed_at)
                     FROM procurement_release_observations AS observation
                     WHERE observation.source_name = release.source_name
                       AND observation.release_id = release.release_id
                       AND observation.release_payload_sha256 =
                           release.release_payload_sha256),
                    release.retrieved_at
                  ) AS observed_at,
                  release.release_payload_sha256, release.evidence_ids,
                  release.product_category, release.stages
           FROM procurement_releases AS release
           WHERE release.source_name = ? AND release.is_current
           ORDER BY release.release_id""",
        [FIND_TENDER_SOURCE],
    ).fetchall()
    releases: list[_ReleaseState] = [
        _ReleaseState(
            release_id=str(row[0]),
            ocid=str(row[1]) if row[1] else None,
            status=str(row[2]).lower() if row[2] else None,
            deadline=_timestamp(row[3]),
            publication_at=_timestamp(row[4]),
            observed_at=_timestamp(row[5]),
            payload_sha256=str(row[6]),
            evidence_ids=_json_strings(row[7]),
            product_category=str(row[8]) if row[8] else None,
            stages=set(_json_strings(row[9])),
        )
        for row in rows
    ]
    closures_by_ocid: dict[str, list[_ReleaseState]] = {}
    for release in releases:
        ocid = release["ocid"]
        if isinstance(ocid, str) and release["status"] in CLOSED_TENDER_STATUSES:
            closures_by_ocid.setdefault(ocid, []).append(release)

    inactive_ids: set[str] = set()
    for release in releases:
        release_id = str(release["release_id"])
        stages = release["stages"]
        is_tender = isinstance(stages, set) and bool(
            stages & {"tender", "tenderUpdate"}
        )
        was_materialized = release_id in existing_release_ids
        if not is_tender and not was_materialized:
            continue
        lifecycle, reason, state_source = _opportunity_lifecycle(
            release,
            closures_by_ocid=closures_by_ocid,
            reference_time=reference_time,
            was_materialized=was_materialized,
        )
        _persist_opportunity_state(
            connection,
            release=release,
            state_source=state_source,
            lifecycle=lifecycle,
            reason=reason,
            recorded_at=reference_time,
        )
        if lifecycle != "active":
            inactive_ids.add(f"find-tender:{release_id}")

    for internal_id in inactive_ids:
        connection.execute(
            "DELETE FROM opportunity_scores WHERE notice_id = ?", [internal_id]
        )
        connection.execute(
            "DELETE FROM explicit_demand WHERE notice_id = ?", [internal_id]
        )
    return len(inactive_ids)


def _opportunity_lifecycle(
    release: _ReleaseState,
    *,
    closures_by_ocid: dict[str, list[_ReleaseState]],
    reference_time: datetime,
    was_materialized: bool,
) -> tuple[str, str | None, _ReleaseState]:
    status = release["status"]
    if status in CLOSED_TENDER_STATUSES:
        return "closed", f"source_status:{status}", release
    if release["product_category"] is None and was_materialized:
        return "closed", "removed_from_selected_category", release

    observed_at = release["observed_at"]
    publication_at = release["publication_at"]
    active_at = observed_at if isinstance(observed_at, datetime) else publication_at
    ocid = release["ocid"]
    if isinstance(ocid, str):
        later_closures = []
        for candidate in closures_by_ocid.get(ocid, []):
            if candidate["release_id"] == release["release_id"]:
                continue
            candidate_observed = candidate["observed_at"]
            candidate_published = candidate["publication_at"]
            closed_at = (
                candidate_observed
                if isinstance(candidate_observed, datetime)
                else candidate_published
            )
            if active_at is None or closed_at is None or closed_at >= active_at:
                later_closures.append(candidate)
        if later_closures:
            minimum = datetime.min.replace(tzinfo=UTC)
            closure = max(
                later_closures,
                key=lambda item: (
                    item["observed_at"] or item["publication_at"] or minimum,
                    str(item["release_id"]),
                ),
            )
            return (
                "closed",
                f"same_ocid:{closure['release_id']}:source_status:{closure['status']}",
                closure,
            )

    deadline = release["deadline"]
    if isinstance(deadline, datetime) and deadline <= reference_time:
        return "expired", "deadline_elapsed", release
    return "active", None, release


def _persist_opportunity_state(
    connection: duckdb.DuckDBPyConnection,
    *,
    release: _ReleaseState,
    state_source: _ReleaseState,
    lifecycle: str,
    reason: str | None,
    recorded_at: datetime,
) -> None:
    release_id = str(release["release_id"])
    opportunity_id = f"find-tender:{release_id}"
    existing = connection.execute(
        """SELECT lifecycle, state_reason, state_changed_at
           FROM opportunity_current_state WHERE opportunity_id = ?""",
        [opportunity_id],
    ).fetchone()
    changed = existing is None or (existing[0], existing[1]) != (lifecycle, reason)
    source_observed_at = state_source["observed_at"]
    if not isinstance(source_observed_at, datetime):
        source_observed_at = state_source["publication_at"]
    prior_changed_at = _timestamp(existing[2]) if existing is not None else None
    state_changed_at = recorded_at if changed else prior_changed_at or recorded_at
    evidence_ids = sorted(
        set(str(item) for item in release["evidence_ids"])
        | set(str(item) for item in state_source["evidence_ids"])
    )
    connection.execute(
        """INSERT INTO opportunity_current_state VALUES (
             ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
           )
           ON CONFLICT (opportunity_id) DO UPDATE SET
             source_name = excluded.source_name,
             release_id = excluded.release_id,
             ocid = excluded.ocid,
             lifecycle = excluded.lifecycle,
             state_reason = excluded.state_reason,
             source_status = excluded.source_status,
             deadline = excluded.deadline,
             release_payload_sha256 = excluded.release_payload_sha256,
             source_observed_at = excluded.source_observed_at,
             state_changed_at = excluded.state_changed_at,
             evidence_ids = excluded.evidence_ids""",
        [
            opportunity_id,
            FIND_TENDER_SOURCE,
            release_id,
            release["ocid"],
            lifecycle,
            reason,
            state_source["status"],
            release["deadline"],
            state_source["payload_sha256"],
            source_observed_at,
            state_changed_at,
            json.dumps(evidence_ids),
        ],
    )
    if changed:
        prior_lifecycle = str(existing[0]) if existing is not None else None
        identity = (
            f"{opportunity_id}|{prior_lifecycle}|{lifecycle}|{reason}|"
            f"{state_source['payload_sha256']}|{source_observed_at}"
        )
        history_id = "opp-state-" + hashlib.sha256(identity.encode()).hexdigest()[:32]
        connection.execute(
            """INSERT OR IGNORE INTO opportunity_state_history VALUES (
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
               )""",
            [
                history_id,
                opportunity_id,
                prior_lifecycle,
                lifecycle,
                reason,
                FIND_TENDER_SOURCE,
                release_id,
                state_source["payload_sha256"],
                source_observed_at,
                recorded_at,
                json.dumps(evidence_ids),
            ],
        )


__all__ = ["reconcile_find_tender_opportunities"]
