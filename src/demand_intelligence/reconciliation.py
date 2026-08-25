"""Completed-run source scopes and versioned current-state reconciliation."""

import hashlib
import json
import re
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import duckdb

UK_TRADE_INFO_CONNECTOR = "UK Trade Info"


@dataclass(frozen=True)
class ScopeRunResult:
    run_id: str
    status: str
    authoritative: bool
    removed_source_record_ids: tuple[str, ...]
    affected_company_ids: tuple[str, ...]


def uk_trade_info_scope_key(
    *, year: int, hs_prefix: str, trader_filter: str
) -> str:
    """Return the stable identity of one complete UKTI OData result set."""

    normalized_prefix = re.sub(r"\s+", "", hs_prefix)
    return (
        f"uk-trade-info:{year}:{normalized_prefix}:"
        f"{trader_filter.strip().lower()}"
    )


def start_scope_run(
    connection: duckdb.DuckDBPyConnection,
    *,
    connector: str,
    scope_key: str,
    scope_parameters: dict[str, Any],
    run_id: str | None = None,
    started_at: datetime | None = None,
) -> str:
    """Start a source-scoped run that is non-authoritative until completed."""

    timestamp = _aware(started_at or datetime.now(UTC))
    identifier = run_id or f"run-{uuid.uuid4()}"
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(
            """INSERT INTO ingestion_run_scopes (
                 run_id, connector, scope_key, scope_parameters, started_at,
                 completed_at, status, completion_reason, is_authoritative,
                 pages_fetched, records_seen, error
               ) VALUES (?, ?, ?, ?, ?, NULL, 'running', NULL, FALSE, 0, 0, NULL)""",
            [
                identifier,
                connector,
                scope_key,
                json.dumps(scope_parameters, ensure_ascii=False, sort_keys=True),
                timestamp,
            ],
        )
        connection.execute(
            """INSERT INTO active_scope_leases
               VALUES (?, ?, ?, ?)""",
            [connector, scope_key, identifier, timestamp],
        )
        if connector == UK_TRADE_INFO_CONNECTOR:
            _bootstrap_uk_trade_info_scope(
                connection,
                scope_key=scope_key,
                scope_parameters=scope_parameters,
                bootstrapped_at=timestamp,
            )
    except Exception as error:
        connection.execute("ROLLBACK")
        if isinstance(error, duckdb.ConstraintException):
            raise ValueError(
                f"scope already has an active run: {connector} {scope_key}"
            ) from error
        raise
    connection.execute("COMMIT")
    return identifier


def record_scope_members(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    members: Iterable[tuple[str, str]],
    observed_at: datetime | None = None,
    page_completed: bool = True,
) -> None:
    """Record one successfully processed page without inferring any removals."""

    connector, scope_key = _running_scope(connection, run_id)
    timestamp = _aware(observed_at or datetime.now(UTC))
    unique_members = dict(members)
    connection.execute(
        """CREATE OR REPLACE TEMP TABLE _scope_member_batch (
             source_record_id VARCHAR PRIMARY KEY, payload_sha256 VARCHAR
           )"""
    )
    if unique_members:
        connection.executemany(
            "INSERT INTO _scope_member_batch VALUES (?, ?)",
            list(unique_members.items()),
        )
        connection.execute(
            """INSERT INTO ingestion_run_records
               SELECT ?, batch.source_record_id, batch.payload_sha256,
                      CASE
                        WHEN membership.source_record_id IS NULL THEN 'new'
                        WHEN NOT membership.is_present THEN 'restored'
                        WHEN membership.current_payload_sha256
                             != batch.payload_sha256 THEN 'corrected'
                        ELSE 'unchanged'
                      END
               FROM _scope_member_batch AS batch
               LEFT JOIN source_scope_membership AS membership
                 ON membership.connector = ?
                AND membership.scope_key = ?
                AND membership.source_record_id = batch.source_record_id
               ON CONFLICT DO NOTHING""",
            [run_id, connector, scope_key],
        )
        connection.execute(
            """INSERT INTO source_scope_membership AS membership (
                 connector, scope_key, source_record_id, current_payload_sha256,
                 previous_payload_sha256, is_present, first_seen_run_id,
                 last_seen_run_id, first_seen_at, last_seen_at, removed_at,
                 removed_by_run_id, last_change_type, state_changed_at
               )
               SELECT ?, ?, batch.source_record_id, batch.payload_sha256,
                      NULL, TRUE, ?, ?, ?, ?, NULL, NULL, 'new', ?
               FROM _scope_member_batch AS batch
               ON CONFLICT (connector, scope_key, source_record_id) DO UPDATE SET
                 previous_payload_sha256 = CASE
                   WHEN membership.current_payload_sha256
                        != excluded.current_payload_sha256
                     THEN membership.current_payload_sha256
                   ELSE membership.previous_payload_sha256
                 END,
                 current_payload_sha256 = excluded.current_payload_sha256,
                 is_present = TRUE,
                 last_seen_run_id = excluded.last_seen_run_id,
                 last_seen_at = excluded.last_seen_at,
                 removed_at = NULL,
                 removed_by_run_id = NULL,
                 last_change_type = CASE
                   WHEN NOT membership.is_present THEN 'restored'
                   WHEN membership.current_payload_sha256
                        != excluded.current_payload_sha256 THEN 'corrected'
                   ELSE 'unchanged'
                 END,
                 state_changed_at = CASE
                   WHEN NOT membership.is_present
                     OR membership.current_payload_sha256
                        != excluded.current_payload_sha256
                     THEN excluded.state_changed_at
                   ELSE membership.state_changed_at
                 END""",
            [
                connector,
                scope_key,
                run_id,
                run_id,
                timestamp,
                timestamp,
                timestamp,
            ],
        )
        connection.execute(
            """UPDATE trade_activity AS activity
               SET is_current = TRUE,
                   first_seen_run_id = coalesce(activity.first_seen_run_id, ?),
                   last_seen_run_id = ?,
                   removed_at = NULL
               FROM _scope_member_batch AS batch
               WHERE activity.source_record_id = batch.source_record_id""",
            [run_id, run_id],
        )
    connection.execute(
        """UPDATE ingestion_run_scopes
           SET pages_fetched = pages_fetched + ?,
               records_seen = (
                 SELECT count(*) FROM ingestion_run_records
                 WHERE run_id = ? AND observation_state != 'removed'
               )
           WHERE run_id = ?""",
        [1 if page_completed else 0, run_id, run_id],
    )


def record_scope_page_completion(
    connection: duckdb.DuckDBPyConnection, *, run_id: str
) -> None:
    """Count one source page after all of its bounded persistence batches commit."""

    _running_scope(connection, run_id)
    connection.execute(
        """UPDATE ingestion_run_scopes
           SET pages_fetched = pages_fetched + 1 WHERE run_id = ?""",
        [run_id],
    )


def finish_scope_run(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    authoritative: bool,
    completion_reason: str,
    completed_at: datetime | None = None,
) -> ScopeRunResult:
    """Atomically finish a run and release its active-scope lease."""

    connection.execute("BEGIN TRANSACTION")
    try:
        result = apply_scope_run_completion(
            connection,
            run_id=run_id,
            authoritative=authoritative,
            completion_reason=completion_reason,
            completed_at=completed_at,
        )
    except Exception:
        connection.execute("ROLLBACK")
        raise
    connection.execute("COMMIT")
    return result


def apply_scope_run_completion(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    authoritative: bool,
    completion_reason: str,
    completed_at: datetime | None = None,
) -> ScopeRunResult:
    """Apply completion inside a transaction owned by the caller."""

    connector, scope_key = _running_scope(connection, run_id)
    timestamp = _aware(completed_at or datetime.now(UTC))
    removed_ids: list[str] = []
    affected_company_ids: set[str] = set()
    if authoritative:
        removed_ids = [
            str(row[0])
            for row in connection.execute(
                """SELECT source_record_id
                   FROM source_scope_membership
                   WHERE connector = ? AND scope_key = ? AND is_present
                     AND last_seen_run_id != ?
                   ORDER BY source_record_id""",
                [connector, scope_key, run_id],
            ).fetchall()
        ]
        if removed_ids:
            connection.execute(
                """INSERT INTO ingestion_run_records
                   SELECT ?, source_record_id, current_payload_sha256, 'removed'
                   FROM source_scope_membership
                   WHERE connector = ? AND scope_key = ? AND is_present
                     AND last_seen_run_id != ?
                   ON CONFLICT DO NOTHING""",
                [run_id, connector, scope_key, run_id],
            )
            affected_company_ids = {
                str(row[0])
                for row in connection.execute(
                    """SELECT DISTINCT activity.company_id
                       FROM trade_activity AS activity
                       JOIN source_scope_membership AS membership
                         ON membership.source_record_id = activity.source_record_id
                       WHERE membership.connector = ?
                         AND membership.scope_key = ?
                         AND membership.is_present
                         AND membership.last_seen_run_id != ?""",
                    [connector, scope_key, run_id],
                ).fetchall()
            }
            connection.execute(
                """UPDATE source_scope_membership
                   SET is_present = FALSE, removed_at = ?, removed_by_run_id = ?,
                       last_change_type = 'removed', state_changed_at = ?
                   WHERE connector = ? AND scope_key = ? AND is_present
                     AND last_seen_run_id != ?""",
                [timestamp, run_id, timestamp, connector, scope_key, run_id],
            )
            _refresh_trade_activity_state(connection, removed_ids)

    status = "completed" if authoritative else "partial"
    connection.execute(
        """UPDATE ingestion_run_scopes
           SET completed_at = ?, status = ?, completion_reason = ?,
               is_authoritative = ?, records_seen = (
                 SELECT count(*) FROM ingestion_run_records
                 WHERE run_id = ? AND observation_state != 'removed'
               )
           WHERE run_id = ?""",
        [timestamp, status, completion_reason, authoritative, run_id, run_id],
    )
    connection.execute(
        """DELETE FROM active_scope_leases
           WHERE connector = ? AND scope_key = ? AND run_id = ?""",
        [connector, scope_key, run_id],
    )
    return ScopeRunResult(
        run_id=run_id,
        status=status,
        authoritative=authoritative,
        removed_source_record_ids=tuple(removed_ids),
        affected_company_ids=tuple(sorted(affected_company_ids)),
    )


def fail_scope_run(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    error: str,
    completed_at: datetime | None = None,
) -> None:
    """Mark an interrupted run failed without changing unseen memberships."""

    connection.execute("BEGIN TRANSACTION")
    try:
        connector, scope_key = _running_scope(connection, run_id)
        connection.execute(
            """UPDATE ingestion_run_scopes
               SET completed_at = ?, status = 'failed', completion_reason = 'error',
                   is_authoritative = FALSE, error = ?, records_seen = (
                     SELECT count(*) FROM ingestion_run_records
                     WHERE run_id = ? AND observation_state != 'removed'
                   )
               WHERE run_id = ?""",
            [_aware(completed_at or datetime.now(UTC)), error, run_id, run_id],
        )
        connection.execute(
            """DELETE FROM active_scope_leases
               WHERE connector = ? AND scope_key = ? AND run_id = ?""",
            [connector, scope_key, run_id],
        )
    except Exception:
        connection.execute("ROLLBACK")
        raise
    connection.execute("COMMIT")


def record_procurement_release_observation(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_name: str,
    release_id: str,
    release_payload_sha256: str,
    observed_at: datetime,
    snapshot_reference: str,
    snapshot_content_sha256: str,
) -> str:
    """Append an immutable observation and record a material payload transition."""

    timestamp = _aware(observed_at)
    observation_id = hashlib.sha256(
        (
            f"{snapshot_content_sha256}|{timestamp.isoformat()}|"
            f"{release_payload_sha256}"
        ).encode()
    ).hexdigest()
    before = _current_release_observation(connection, source_name, release_id)
    existed = connection.execute(
        """SELECT 1 FROM procurement_release_observations
           WHERE source_name = ? AND release_id = ? AND observation_id = ?""",
        [source_name, release_id, observation_id],
    ).fetchone()
    connection.execute(
        """INSERT OR IGNORE INTO procurement_release_observations
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            source_name,
            release_id,
            observation_id,
            release_payload_sha256,
            timestamp,
            snapshot_reference,
            snapshot_content_sha256,
        ],
    )
    after = _current_release_observation(connection, source_name, release_id)
    if existed is None and before is not None and after is not None and before[0] != after[0]:
        prior_count = connection.execute(
            """SELECT count(*) FROM procurement_release_observations
               WHERE source_name = ? AND release_id = ?
                 AND release_payload_sha256 = ? AND observation_id != ?""",
            [source_name, release_id, after[0], observation_id],
        ).fetchone()
        transition_type = (
            "restored" if prior_count is not None and int(prior_count[0]) else "corrected"
        )
        transition_id = hashlib.sha256(
            (
                f"{source_name}|{release_id}|{before[0]}|{after[0]}|"
                f"{observation_id}"
            ).encode()
        ).hexdigest()
        connection.execute(
            """INSERT OR IGNORE INTO release_transitions
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                transition_id,
                source_name,
                release_id,
                before[0],
                after[0],
                transition_type,
                after[1],
                observation_id,
            ],
        )
    reconcile_procurement_release_versions(
        connection, source_name=source_name, release_id=release_id
    )
    return observation_id


def reconcile_procurement_release_versions(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_name: str,
    release_id: str,
) -> str | None:
    """Mark exactly one chronologically latest payload current for a release ID."""

    observation_count = connection.execute(
        """SELECT count(*) FROM procurement_release_observations
           WHERE source_name = ? AND release_id = ?""",
        [source_name, release_id],
    ).fetchone()
    if observation_count is None or int(observation_count[0]) == 0:
        _bootstrap_procurement_release_observations(
            connection, source_name, release_id
        )
    winner = connection.execute(
        """SELECT release_payload_sha256, observed_at
           FROM procurement_release_observations
           WHERE source_name = ? AND release_id = ?
           ORDER BY observed_at DESC, observation_id DESC
           LIMIT 1""",
        [source_name, release_id],
    ).fetchone()
    if winner is None:
        return None
    winner_sha = str(winner[0])
    connection.execute(
        """UPDATE procurement_releases
           SET is_current = release_payload_sha256 = ?,
               version_state = CASE
                 WHEN release_payload_sha256 = ? THEN 'current' ELSE 'replaced'
               END,
               superseded_at = CASE
                 WHEN release_payload_sha256 = ? THEN NULL ELSE ?
               END,
               superseded_by_sha256 = CASE
                 WHEN release_payload_sha256 = ? THEN NULL ELSE ?
               END
           WHERE source_name = ? AND release_id = ?""",
        [
            winner_sha,
            winner_sha,
            winner_sha,
            winner[1],
            winner_sha,
            winner_sha,
            source_name,
            release_id,
        ],
    )
    return winner_sha


def _refresh_trade_activity_state(
    connection: duckdb.DuckDBPyConnection, source_record_ids: list[str]
) -> None:
    if not source_record_ids:
        return
    connection.execute(
        """CREATE OR REPLACE TEMP TABLE _affected_source_records (
             source_record_id VARCHAR PRIMARY KEY
           )"""
    )
    connection.executemany(
        "INSERT INTO _affected_source_records VALUES (?)",
        [(record_id,) for record_id in source_record_ids],
    )
    connection.execute(
        """UPDATE trade_activity AS activity
           SET is_current = latest.is_present,
               last_seen_run_id = coalesce(
                 latest.present_run_id, activity.last_seen_run_id
               ),
               removed_at = CASE
                 WHEN latest.is_present THEN NULL ELSE latest.event_at
               END
           FROM (
             SELECT membership.source_record_id,
                    bool_or(membership.is_present) AS is_present,
                    arg_max(membership.last_seen_run_id, membership.last_seen_at)
                      FILTER (WHERE membership.is_present) AS present_run_id,
                    max(membership.removed_at) AS event_at
             FROM source_scope_membership AS membership
             JOIN _affected_source_records AS affected
               ON affected.source_record_id = membership.source_record_id
             GROUP BY membership.source_record_id
           ) latest
           WHERE activity.source_record_id = latest.source_record_id"""
    )


def _running_scope(
    connection: duckdb.DuckDBPyConnection, run_id: str
) -> tuple[str, str]:
    row = connection.execute(
        """SELECT run.connector, run.scope_key, run.status, lease.run_id
           FROM ingestion_run_scopes AS run
           LEFT JOIN active_scope_leases AS lease
             ON lease.connector = run.connector
            AND lease.scope_key = run.scope_key
           WHERE run.run_id = ?""",
        [run_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"unknown scope run: {run_id}")
    if row[2] != "running":
        raise ValueError(f"scope run {run_id} is not running (status={row[2]})")
    if row[3] != run_id:
        raise ValueError(f"scope run {run_id} does not own its active lease")
    return str(row[0]), str(row[1])


def _current_release_observation(
    connection: duckdb.DuckDBPyConnection, source_name: str, release_id: str
) -> tuple[str, datetime] | None:
    row = connection.execute(
        """SELECT release_payload_sha256, observed_at
           FROM procurement_release_observations
           WHERE source_name = ? AND release_id = ?
           ORDER BY observed_at DESC, observation_id DESC LIMIT 1""",
        [source_name, release_id],
    ).fetchone()
    if row is None:
        return None
    return str(row[0]), row[1]


def _bootstrap_procurement_release_observations(
    connection: duckdb.DuckDBPyConnection, source_name: str, release_id: str
) -> None:
    connection.execute(
        """INSERT OR IGNORE INTO procurement_release_observations
           SELECT source_name, release_id,
                  sha256(
                    coalesce(snapshot_content_sha256, '') || '|' ||
                    CAST(retrieved_at AS VARCHAR) || '|' || release_payload_sha256
                  ),
                  release_payload_sha256, retrieved_at, snapshot_reference,
                  snapshot_content_sha256
           FROM procurement_releases AS release
           WHERE release.source_name = ? AND release.release_id = ?
             AND NOT EXISTS (
               SELECT 1 FROM procurement_release_observations AS observation
               WHERE observation.source_name = release.source_name
                 AND observation.release_id = release.release_id
                 AND observation.release_payload_sha256 =
                     release.release_payload_sha256
                 AND observation.observed_at = release.retrieved_at
             )""",
        [source_name, release_id],
    )


def _bootstrap_uk_trade_info_scope(
    connection: duckdb.DuckDBPyConnection,
    *,
    scope_key: str,
    scope_parameters: dict[str, Any],
    bootstrapped_at: datetime,
) -> None:
    """Claim pre-Phase-3 current rows so the first complete run can reconcile them."""

    try:
        year = int(scope_parameters["year"])
        hs_prefix = str(scope_parameters["hs_prefix"])
        trader_filter = str(scope_parameters["trader_filter"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("invalid UK Trade Info scope parameters") from error
    role: str | None
    if trader_filter == "Importers":
        role = "Importer"
    elif trader_filter == "Exporters":
        role = "Exporter"
    else:
        role = None
    legacy_run_id = f"legacy-bootstrap:{scope_key}"
    connection.execute(
        """INSERT INTO source_scope_membership (
             connector, scope_key, source_record_id, current_payload_sha256,
             previous_payload_sha256, is_present, first_seen_run_id,
             last_seen_run_id, first_seen_at, last_seen_at, removed_at,
             removed_by_run_id, last_change_type, state_changed_at
           )
           SELECT ?, ?, source_record_id, payload_sha256, NULL, TRUE, ?, ?, ?, ?,
                  NULL, NULL, 'legacy_bootstrap', ?
           FROM trade_activity
           WHERE is_current AND year = ?
             AND starts_with(commodity_code, ?)
             AND (? IS NULL OR trade_role = ?)
           ON CONFLICT DO NOTHING""",
        [
            UK_TRADE_INFO_CONNECTOR,
            scope_key,
            legacy_run_id,
            legacy_run_id,
            bootstrapped_at,
            bootstrapped_at,
            bootstrapped_at,
            year,
            hs_prefix,
            role,
            role,
        ],
    )


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


__all__ = [
    "ScopeRunResult",
    "UK_TRADE_INFO_CONNECTOR",
    "apply_scope_run_completion",
    "fail_scope_run",
    "finish_scope_run",
    "reconcile_procurement_release_versions",
    "record_procurement_release_observation",
    "record_scope_page_completion",
    "record_scope_members",
    "start_scope_run",
    "uk_trade_info_scope_key",
]
