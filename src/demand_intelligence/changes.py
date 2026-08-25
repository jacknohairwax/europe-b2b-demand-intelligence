"""Persistent, material-only change detection over finalized current views."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import cast

import duckdb

CLOSED_TENDER_STATUSES = {"cancelled", "complete", "unsuccessful", "withdrawn"}
_RecurrenceKey = tuple[str, int, str, str, str]


@dataclass(frozen=True)
class ChangeEvent:
    event_id: str
    run_id: str
    connector: str
    scope_key: str
    event_type: str
    subject_type: str
    subject_id: str
    field_name: str | None
    old_value: object | None
    new_value: object | None
    evidence_ids: tuple[str, ...]
    confidence: float
    detected_at: datetime
    details: dict[str, object]


@dataclass(frozen=True)
class _MaterialState:
    namespace: str
    subject_type: str
    subject_id: str
    connector: str
    payload: dict[str, object]
    evidence_ids: tuple[str, ...]
    confidence: float

    @property
    def state_hash(self) -> str:
        return hashlib.sha256(_canonical_json(self.payload).encode()).hexdigest()


@dataclass(frozen=True)
class _Baseline:
    payload: dict[str, object]
    is_present: bool


@dataclass(frozen=True)
class _AwardObservation:
    state: _MaterialState
    suppliers: tuple[dict[str, object], ...]
    rank: tuple[str, str, str, int]


@dataclass(frozen=True)
class _UkTradeScope:
    year: int
    commodity_prefix: str
    trade_role: str | None


def detect_material_changes(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    connector: str,
    scope_key: str,
    detected_at: datetime | None = None,
    completed_authoritative: bool = False,
    recurrence_threshold: int = 2,
) -> list[ChangeEvent]:
    """Compare current material facts to the last committed detector baseline.

    The event insert and baseline advance are one transaction. Missing records only
    retire matching UKTI recurrence-scope baselines after a completed authoritative
    result set. FTS incremental absence is never treated as closure; explicit source
    closure and wall-clock expiry remain positive current facts on partial runs.
    """

    if not run_id.strip():
        raise ValueError("run_id must not be empty")
    if recurrence_threshold < 2:
        raise ValueError("recurrence_threshold must be at least 2")
    timestamp = _aware(detected_at or datetime.now(UTC))
    current = _project_current_state(
        connection,
        as_of=timestamp,
        recurrence_threshold=recurrence_threshold,
    )
    baselines = _load_baselines(connection)

    connection.execute("BEGIN TRANSACTION")
    try:
        events = _build_events(
            current,
            baselines,
            run_id=run_id,
            scope_key=scope_key,
            detected_at=timestamp,
        )
        _persist_events(connection, events)
        _persist_material_state(
            connection,
            current,
            baselines,
            run_id=run_id,
            connector=connector,
            scope_key=scope_key,
            observed_at=timestamp,
            completed_authoritative=completed_authoritative,
        )
        connection.execute(
            """INSERT INTO change_detection_runs VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT (run_id) DO UPDATE SET
                 connector = excluded.connector,
                 scope_key = excluded.scope_key,
                 detected_at = excluded.detected_at,
                 completed_authoritative = excluded.completed_authoritative,
                 recurrence_threshold = excluded.recurrence_threshold,
                 event_count = excluded.event_count""",
            [
                run_id,
                connector,
                scope_key,
                timestamp,
                completed_authoritative,
                recurrence_threshold,
                len(events),
            ],
        )
    except Exception:
        connection.execute("ROLLBACK")
        raise
    connection.execute("COMMIT")
    return events


def _project_current_state(
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of: datetime,
    recurrence_threshold: int,
) -> dict[tuple[str, str], _MaterialState]:
    states: dict[tuple[str, str], _MaterialState] = {}
    projected = [
        *_project_tenders(connection, as_of=as_of),
        *_project_awards_and_suppliers(connection),
        *_project_recurrence(connection, recurrence_threshold=recurrence_threshold),
        *_project_linked_evidence(connection),
        *_project_contradictions(connection),
    ]
    for state in projected:
        key = (state.namespace, state.subject_id)
        existing = states.get(key)
        if existing is not None and existing.state_hash != state.state_hash:
            raise ValueError(f"conflicting material state for {key!r}")
        states[key] = state
    return states


def _project_tenders(
    connection: duckdb.DuckDBPyConnection, *, as_of: datetime
) -> list[_MaterialState]:
    rows = connection.execute(
        """SELECT release.source_name, release.release_id, release.ocid,
                  release.stages, release.tender_status, release.deadline,
                  release.value, release.currency, release.title,
                  release.buyer_id, release.buyer_name, release.cpv_codes,
                  release.product_category, release.evidence_ids,
                  release.source_url, release.publication_date,
                  release.retrieved_at,
                  coalesce(observation.last_observed_at, release.retrieved_at)
                    AS source_observed_at
           FROM procurement_releases AS release
           LEFT JOIN (
             SELECT source_name, release_id, release_payload_sha256,
                    max(observed_at) AS last_observed_at
             FROM procurement_release_observations
             GROUP BY source_name, release_id, release_payload_sha256
           ) AS observation
             ON observation.source_name = release.source_name
            AND observation.release_id = release.release_id
            AND observation.release_payload_sha256 = release.release_payload_sha256
           WHERE release.is_current
           ORDER BY release.source_name, release.release_id"""
    ).fetchall()
    process_rows: dict[
        tuple[str, str], list[tuple[tuple[str, str, str], tuple[object, ...]]]
    ] = {}
    for row in rows:
        ocid = _text(row[2])
        if ocid is None:
            continue
        process_rows.setdefault((str(row[0]), ocid), []).append(
            (
                _release_rank(row[17], row[15], str(row[1])),
                row,
            )
        )
    states: list[_MaterialState] = []
    for row in rows:
        if not ({"tender", "tenderUpdate"} & set(_json_strings(row[3]))):
            continue
        source_name = str(row[0])
        release_id = str(row[1])
        status = _text(row[4])
        deadline = _datetime(row[5])
        lifecycle, closure_reason = _tender_lifecycle(status, deadline, as_of)
        evidence = set(_json_strings(row[13]))
        tender_rank = _release_rank(row[17], row[15], release_id)
        closing_release_id: str | None = None
        ocid = _text(row[2])
        if lifecycle not in {"closed", "expired"} and ocid is not None:
            later_closures = [
                (rank, process_row)
                for rank, process_row in process_rows.get((source_name, ocid), [])
                if rank > tender_rank
                and str(process_row[4] or "").strip().lower()
                in CLOSED_TENDER_STATUSES
            ]
            if later_closures:
                _, closing_row = max(later_closures, key=lambda item: item[0])
                closing_status = str(closing_row[4]).strip().lower()
                closing_release_id = str(closing_row[1])
                lifecycle = "closed"
                closure_reason = (
                    f"process_release:{closing_release_id}:"
                    f"source_status:{closing_status}"
                )
                evidence.update(_json_strings(closing_row[13]))
        evidence_ids = tuple(sorted(evidence))
        subject_id = _source_subject(source_name, release_id)
        payload: dict[str, object] = {
            "source_name": source_name,
            "release_id": release_id,
            "ocid": _text(row[2]),
            "lifecycle": lifecycle,
            "closure_reason": closure_reason,
            "closure_release_id": closing_release_id,
            "tender_status": status.lower() if status else None,
            "deadline": _timestamp(deadline),
            "value": _number(row[6]),
            "currency": _currency(row[7]),
            "title": _text(row[8]),
            "buyer_id": _text(row[9]),
            "buyer_name": _text(row[10]),
            "cpv_codes": _json_strings(row[11]),
            "product_category": _text(row[12]),
            "evidence_ids": list(evidence_ids),
            "source_url": _text(row[14]),
        }
        states.append(
            _MaterialState(
                namespace="tender",
                subject_type="tender",
                subject_id=subject_id,
                connector=source_name,
                payload=cast(dict[str, object], _canonical(payload)),
                evidence_ids=evidence_ids,
                confidence=0.95,
            )
        )
    return states


def _project_awards_and_suppliers(
    connection: duckdb.DuckDBPyConnection,
) -> list[_MaterialState]:
    rows = connection.execute(
        """SELECT release.source_name, release.release_id, release.ocid,
                  release.publication_date, release.retrieved_at,
                  award.award_index, award.award_id, award.title, award.status,
                  award.award_date, award.value, award.currency,
                  award.contract_status, award.contract_value,
                  award.contract_currency, award.suppliers, award.evidence_id
           FROM procurement_awards AS award
           JOIN procurement_releases AS release
             ON release.source_name = award.source_name
            AND release.release_id = award.release_id
            AND release.release_payload_sha256 = award.release_payload_sha256
           WHERE release.is_current
           ORDER BY release.source_name, release.release_id, award.award_index"""
    ).fetchall()
    latest: dict[str, _AwardObservation] = {}
    for row in rows:
        source_name = str(row[0])
        release_id = str(row[1])
        ocid = _text(row[2])
        award_index = int(row[5])
        award_id = _text(row[6])
        context = ocid or release_id
        award_token = award_id or f"release:{release_id}:index:{award_index}"
        subject_id = f"{_source_subject(source_name, context)}:award:{award_token}"
        evidence_ids = tuple([str(row[16])]) if row[16] else ()
        suppliers = tuple(_json_mappings(row[15]))
        payload: dict[str, object] = {
            "source_name": source_name,
            "release_id": release_id,
            "ocid": ocid,
            "award_id": award_id,
            "title": _text(row[7]),
            "status": _text(row[8]),
            "award_date": _timestamp(_datetime(row[9])),
            "value": _number(row[10]),
            "currency": _currency(row[11]),
            "contract_status": _text(row[12]),
            "contract_value": _number(row[13]),
            "contract_currency": _currency(row[14]),
            "evidence_ids": list(evidence_ids),
        }
        state = _MaterialState(
            namespace="award",
            subject_type="award",
            subject_id=subject_id,
            connector=source_name,
            payload=cast(dict[str, object], _canonical(payload)),
            evidence_ids=evidence_ids,
            confidence=0.95 if award_id else 0.75,
        )
        observation = _AwardObservation(
            state=state,
            suppliers=suppliers,
            rank=(
                _timestamp(_datetime(row[3])) or "",
                _timestamp(_datetime(row[4])) or "",
                release_id,
                award_index,
            ),
        )
        previous = latest.get(subject_id)
        if previous is None or observation.rank > previous.rank:
            latest[subject_id] = observation

    states: list[_MaterialState] = []
    for observation in latest.values():
        states.append(observation.state)
        for supplier in observation.suppliers:
            party_id = _text(supplier.get("party_id")) or _text(supplier.get("id"))
            name = _text(supplier.get("name"))
            identity = _supplier_identity(party_id, name)
            if identity is None:
                continue
            supplier_subject = f"{observation.state.subject_id}:supplier:{identity}"
            supplier_payload: dict[str, object] = {
                "award_subject_id": observation.state.subject_id,
                "party_id": party_id,
                "name": name,
                "evidence_ids": list(observation.state.evidence_ids),
            }
            states.append(
                _MaterialState(
                    namespace="supplier",
                    subject_type="supplier",
                    subject_id=supplier_subject,
                    connector=observation.state.connector,
                    payload=cast(dict[str, object], _canonical(supplier_payload)),
                    evidence_ids=observation.state.evidence_ids,
                    confidence=0.95 if party_id else 0.75,
                )
            )
    return states


def _project_recurrence(
    connection: duckdb.DuckDBPyConnection, *, recurrence_threshold: int
) -> list[_MaterialState]:
    account_rows = connection.execute(
        """SELECT company_id, company_name, company_type, products,
                  relevant_hs_codes, months_active, trade_evidence, confidence
           FROM recurrent_accounts ORDER BY company_id"""
    ).fetchall()
    accounts = {str(row[0]): row for row in account_rows}
    trade_rows = connection.execute(
        """SELECT company_id, company_name, year, commodity_code,
                  commodity_code_level, trade_role, month, evidence_id
           FROM trade_activity
           WHERE is_current
           ORDER BY company_id, year, commodity_code,
                    commodity_code_level, trade_role, month"""
    ).fetchall()
    scope_months: dict[_RecurrenceKey, set[int]] = {}
    scope_evidence: dict[_RecurrenceKey, set[str]] = {}
    trade_names: dict[str, str] = {}
    for row in trade_rows:
        company_id = str(row[0])
        key = (company_id, int(row[2]), str(row[3]), str(row[4]), str(row[5]))
        scope_months.setdefault(key, set()).add(int(row[6]))
        if row[7]:
            scope_evidence.setdefault(key, set()).add(str(row[7]))
        trade_names.setdefault(company_id, str(row[1]))

    # Legacy or hand-built databases can have derived scopes without activity rows.
    # Preserve them as a fallback, while normal ingestion always derives from current
    # activity so below-threshold scopes remain visible to the detector baseline.
    derived_rows = connection.execute(
        """SELECT company_id, activity_year, commodity_code,
                  commodity_code_level, trade_role, active_months, evidence_ids
           FROM recurrent_account_scopes"""
    ).fetchall()
    for row in derived_rows:
        key = (str(row[0]), int(row[1]), str(row[2]), str(row[3]), str(row[4]))
        if key not in scope_months:
            scope_months[key] = {int(value) for value in _json_strings(row[5])}
            scope_evidence[key] = set(_json_strings(row[6]))

    qualifying_by_company: dict[str, list[_RecurrenceKey]] = {}
    for key, months in scope_months.items():
        if len(months) >= recurrence_threshold:
            qualifying_by_company.setdefault(key[0], []).append(key)

    states: list[_MaterialState] = []
    for company_id, qualifying_scopes in sorted(qualifying_by_company.items()):
        account_row = accounts.get(company_id)
        evidence_ids = tuple(
            sorted(
                {
                    evidence_id
                    for key in qualifying_scopes
                    for evidence_id in scope_evidence.get(key, set())
                }
            )
        )
        if account_row is not None:
            company_name = _text(account_row[1])
            company_type = _text(account_row[2])
            products = _json_strings(account_row[3])
            commodity_codes = _json_strings(account_row[4])
            months_active = int(account_row[5])
            confidence = float(account_row[7])
        else:
            company_name = trade_names.get(company_id)
            roles = {scope[4] for scope in qualifying_scopes}
            company_type = " and ".join(sorted(roles))
            products = []
            commodity_codes = sorted({scope[2] for scope in qualifying_scopes})
            months_active = len(
                {
                    (scope[1], month)
                    for scope in qualifying_scopes
                    for month in scope_months[scope]
                }
            )
            confidence = 0.9 if months_active >= 3 else 0.8
        payload: dict[str, object] = {
            "company_id": company_id,
            "company_name": company_name,
            "company_type": company_type,
            "products": products,
            "commodity_codes": commodity_codes,
            "months_active": months_active,
            "qualifying_scopes": [
                _recurrence_subject_id(scope) for scope in qualifying_scopes
            ],
            "evidence_ids": list(evidence_ids),
        }
        states.append(
            _MaterialState(
                namespace="recurrent_trader",
                subject_type="company",
                subject_id=company_id,
                connector="UK Trade Info",
                payload=cast(dict[str, object], _canonical(payload)),
                evidence_ids=evidence_ids,
                confidence=confidence,
            )
        )

    for key, observed_months in sorted(scope_months.items()):
        company_id, year, commodity_code, commodity_level, trade_role = key
        sorted_months = sorted(observed_months)
        evidence_ids = tuple(sorted(scope_evidence.get(key, set())))
        qualifies = len(sorted_months) >= recurrence_threshold
        subject_id = _recurrence_subject_id(key)
        payload = {
            "company_id": company_id,
            "activity_year": year,
            "commodity_code": commodity_code,
            "commodity_code_level": commodity_level,
            "trade_role": trade_role,
            "active_months": sorted_months,
            "month_count": len(sorted_months),
            "threshold": recurrence_threshold,
            "qualifies": qualifies,
            "evidence_ids": list(evidence_ids),
        }
        states.append(
            _MaterialState(
                namespace="recurrence_scope",
                subject_type="recurrent_scope",
                subject_id=subject_id,
                connector="UK Trade Info",
                payload=cast(dict[str, object], _canonical(payload)),
                evidence_ids=evidence_ids,
                confidence=0.9 if qualifies and len(sorted_months) >= 3 else 0.8,
            )
        )
    return states


def _project_linked_evidence(
    connection: duckdb.DuckDBPyConnection,
) -> list[_MaterialState]:
    states: list[_MaterialState] = []
    rows = connection.execute(
        """SELECT company_id, buyer_role, procurement_evidence, confidence
           FROM recurrent_accounts ORDER BY company_id"""
    ).fetchall()
    for company_id, buyer_role, raw_evidence, confidence in rows:
        for evidence_id in sorted(set(_json_strings(raw_evidence))):
            payload: dict[str, object] = {
                "company_id": str(company_id),
                "evidence_id": evidence_id,
                "procurement_role": _text(buyer_role),
                "evidence_ids": [evidence_id],
            }
            states.append(
                _MaterialState(
                    namespace="linked_procurement_evidence",
                    subject_type="company_procurement_evidence",
                    subject_id=f"{company_id}:{evidence_id}",
                    connector="Cross-source entity linking",
                    payload=cast(dict[str, object], _canonical(payload)),
                    evidence_ids=(evidence_id,),
                    confidence=float(confidence),
                )
            )
    return states


def _project_contradictions(
    connection: duckdb.DuckDBPyConnection,
) -> list[_MaterialState]:
    rows = connection.execute(
        """SELECT left_company_id, right_company_id, status, match_type,
                  reasoning, evidence_ids
           FROM entity_link_decisions
           WHERE is_current AND status = 'rejected'
           ORDER BY left_company_id, right_company_id"""
    ).fetchall()
    states: list[_MaterialState] = []
    for row in rows:
        match_type = str(row[3])
        if "mismatch" not in match_type:
            continue
        evidence_ids = tuple(sorted(set(_json_strings(row[5]))))
        subject_id = f"{row[0]}:{row[1]}"
        payload: dict[str, object] = {
            "left_company_id": str(row[0]),
            "right_company_id": str(row[1]),
            "match_type": match_type,
            "reasoning": str(row[4]),
            "evidence_ids": list(evidence_ids),
        }
        states.append(
            _MaterialState(
                namespace="link_contradiction",
                subject_type="entity_link",
                subject_id=subject_id,
                connector="Cross-source entity linking",
                payload=cast(dict[str, object], _canonical(payload)),
                evidence_ids=evidence_ids,
                confidence=0.95 if "identifier" in match_type else 0.9,
            )
        )
    return states


def _build_events(
    current: dict[tuple[str, str], _MaterialState],
    baselines: dict[tuple[str, str], _Baseline],
    *,
    run_id: str,
    scope_key: str,
    detected_at: datetime,
) -> list[ChangeEvent]:
    events: list[ChangeEvent] = []
    for key in sorted(current):
        state = current[key]
        baseline = baselines.get(key)
        old = baseline.payload if baseline and baseline.is_present else None
        events.extend(
            _transition_events(
                state,
                old,
                run_id=run_id,
                scope_key=scope_key,
                detected_at=detected_at,
            )
        )
    return events


def _transition_events(
    state: _MaterialState,
    old: dict[str, object] | None,
    *,
    run_id: str,
    scope_key: str,
    detected_at: datetime,
) -> list[ChangeEvent]:
    if old is None:
        initial_events = {
            "tender": ("new_tender", None, None, state.payload),
            "award": ("new_award", None, None, state.payload),
            "supplier": ("new_supplier", None, None, state.payload),
            "recurrent_trader": (
                "new_recurrent_trader",
                None,
                None,
                state.payload,
            ),
            "recurrence_scope": (
                "recurrence_threshold_crossed",
                "month_count",
                0,
                state.payload.get("month_count"),
            ),
            "linked_procurement_evidence": (
                "procurement_evidence_newly_linked",
                "evidence_id",
                None,
                state.payload.get("evidence_id"),
            ),
            "link_contradiction": (
                "evidence_contradiction",
                "match_type",
                None,
                state.payload.get("match_type"),
            ),
        }
        descriptor = initial_events[state.namespace]
        details: dict[str, object] = {}
        if state.namespace == "recurrence_scope":
            details["threshold"] = state.payload.get("threshold")
            details["active_months"] = state.payload.get("active_months")
        if state.namespace == "link_contradiction":
            details["reasoning"] = state.payload.get("reasoning")
        if state.namespace == "tender" and state.payload.get("lifecycle") in {
            "closed",
            "expired",
        }:
            return []
        if (
            state.namespace == "recurrence_scope"
            and state.payload.get("qualifies") is not True
        ):
            return []
        return [
            _event(
                state,
                event_type=descriptor[0],
                field_name=descriptor[1],
                old_value=descriptor[2],
                new_value=descriptor[3],
                details=details,
                run_id=run_id,
                scope_key=scope_key,
                detected_at=detected_at,
            )
        ]

    events: list[ChangeEvent] = []
    if state.namespace == "tender":
        if old.get("deadline") != state.payload.get("deadline"):
            events.append(
                _event(
                    state,
                    event_type="deadline_changed",
                    field_name="deadline",
                    old_value=old.get("deadline"),
                    new_value=state.payload.get("deadline"),
                    details={},
                    run_id=run_id,
                    scope_key=scope_key,
                    detected_at=detected_at,
                )
            )
        old_value = {"amount": old.get("value"), "currency": old.get("currency")}
        new_value = {
            "amount": state.payload.get("value"),
            "currency": state.payload.get("currency"),
        }
        if old_value != new_value:
            events.append(
                _event(
                    state,
                    event_type="value_changed",
                    field_name="tender_value",
                    old_value=old_value,
                    new_value=new_value,
                    details={},
                    run_id=run_id,
                    scope_key=scope_key,
                    detected_at=detected_at,
                )
            )
        closed = {"closed", "expired"}
        if old.get("lifecycle") not in closed and state.payload.get("lifecycle") in closed:
            events.append(
                _event(
                    state,
                    event_type="tender_closed",
                    field_name="lifecycle",
                    old_value=old.get("lifecycle"),
                    new_value=state.payload.get("lifecycle"),
                    details={"reason": state.payload.get("closure_reason")},
                    run_id=run_id,
                    scope_key=scope_key,
                    detected_at=detected_at,
                )
            )
    elif state.namespace == "award":
        for prefix, field_name in (("", "award_value"), ("contract_", "contract_value")):
            old_value = {
                "amount": old.get(f"{prefix}value"),
                "currency": old.get(f"{prefix}currency"),
            }
            new_value = {
                "amount": state.payload.get(f"{prefix}value"),
                "currency": state.payload.get(f"{prefix}currency"),
            }
            if old_value != new_value:
                events.append(
                    _event(
                        state,
                        event_type="value_changed",
                        field_name=field_name,
                        old_value=old_value,
                        new_value=new_value,
                        details={},
                        run_id=run_id,
                        scope_key=scope_key,
                        detected_at=detected_at,
                    )
                )
    elif state.namespace == "recurrence_scope":
        if old.get("qualifies") is not True and state.payload.get("qualifies") is True:
            events.append(
                _event(
                    state,
                    event_type="recurrence_threshold_crossed",
                    field_name="month_count",
                    old_value=old.get("month_count"),
                    new_value=state.payload.get("month_count"),
                    details={
                        "threshold": state.payload.get("threshold"),
                        "active_months": state.payload.get("active_months"),
                    },
                    run_id=run_id,
                    scope_key=scope_key,
                    detected_at=detected_at,
                )
            )
    elif state.namespace == "link_contradiction":
        if old.get("match_type") != state.payload.get("match_type"):
            events.append(
                _event(
                    state,
                    event_type="evidence_contradiction",
                    field_name="match_type",
                    old_value=old.get("match_type"),
                    new_value=state.payload.get("match_type"),
                    details={"reasoning": state.payload.get("reasoning")},
                    run_id=run_id,
                    scope_key=scope_key,
                    detected_at=detected_at,
                )
            )
    return events


def _event(
    state: _MaterialState,
    *,
    event_type: str,
    field_name: str | None,
    old_value: object | None,
    new_value: object | None,
    details: dict[str, object],
    run_id: str,
    scope_key: str,
    detected_at: datetime,
) -> ChangeEvent:
    identity = {
        "run_id": run_id,
        "event_type": event_type,
        "subject_type": state.subject_type,
        "subject_id": state.subject_id,
        "field_name": field_name,
        "old_value": old_value,
        "new_value": new_value,
    }
    event_id = "change-" + hashlib.sha256(_canonical_json(identity).encode()).hexdigest()[:32]
    return ChangeEvent(
        event_id=event_id,
        run_id=run_id,
        connector=state.connector,
        scope_key=scope_key,
        event_type=event_type,
        subject_type=state.subject_type,
        subject_id=state.subject_id,
        field_name=field_name,
        old_value=_canonical(old_value),
        new_value=_canonical(new_value),
        evidence_ids=state.evidence_ids,
        confidence=state.confidence,
        detected_at=detected_at,
        details=cast(dict[str, object], _canonical(details)),
    )


def _load_baselines(
    connection: duckdb.DuckDBPyConnection,
) -> dict[tuple[str, str], _Baseline]:
    rows = connection.execute(
        """SELECT namespace, subject_id, state_payload, is_present
           FROM material_change_state"""
    ).fetchall()
    baselines: dict[tuple[str, str], _Baseline] = {}
    for namespace, subject_id, payload, is_present in rows:
        parsed = _json_object(payload)
        baselines[(str(namespace), str(subject_id))] = _Baseline(
            payload=parsed,
            is_present=bool(is_present),
        )
    return baselines


def _persist_events(
    connection: duckdb.DuckDBPyConnection, events: list[ChangeEvent]
) -> None:
    for event in events:
        connection.execute(
            """INSERT OR IGNORE INTO change_events VALUES (
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
               )""",
            [
                event.event_id,
                event.run_id,
                event.connector,
                event.scope_key,
                event.event_type,
                event.subject_type,
                event.subject_id,
                event.field_name,
                _canonical_json(event.old_value),
                _canonical_json(event.new_value),
                _canonical_json(event.evidence_ids),
                event.confidence,
                event.detected_at,
                _canonical_json(event.details),
            ],
        )


def _persist_material_state(
    connection: duckdb.DuckDBPyConnection,
    current: dict[tuple[str, str], _MaterialState],
    baselines: dict[tuple[str, str], _Baseline],
    *,
    run_id: str,
    connector: str,
    scope_key: str,
    observed_at: datetime,
    completed_authoritative: bool,
) -> None:
    for state in current.values():
        connection.execute(
            """INSERT INTO material_change_state (
                 namespace, subject_id, state_hash, state_payload, is_present,
                 first_seen_run_id, last_seen_run_id, first_seen_at, last_seen_at
               ) VALUES (?, ?, ?, ?, TRUE, ?, ?, ?, ?)
               ON CONFLICT (namespace, subject_id) DO UPDATE SET
                 state_hash = excluded.state_hash,
                 state_payload = excluded.state_payload,
                 is_present = TRUE,
                 last_seen_run_id = excluded.last_seen_run_id,
                 last_seen_at = excluded.last_seen_at""",
            [
                state.namespace,
                state.subject_id,
                state.state_hash,
                _canonical_json(state.payload),
                run_id,
                run_id,
                observed_at,
                observed_at,
            ],
        )
    authoritative_scope = (
        _parse_uk_trade_scope(scope_key)
        if completed_authoritative and connector.strip().casefold() == "uk trade info"
        else None
    )
    if authoritative_scope is not None:
        for namespace, subject_id in set(baselines) - set(current):
            baseline = baselines[(namespace, subject_id)]
            if namespace != "recurrence_scope" or not _recurrence_in_scope(
                baseline.payload, authoritative_scope
            ):
                continue
            connection.execute(
                """UPDATE material_change_state
                   SET is_present = FALSE, last_seen_run_id = ?, last_seen_at = ?
                   WHERE namespace = ? AND subject_id = ? AND is_present""",
                [run_id, observed_at, namespace, subject_id],
            )


def _tender_lifecycle(
    status: str | None, deadline: datetime | None, as_of: datetime
) -> tuple[str, str | None]:
    normalized_status = status.lower() if status else None
    if normalized_status in CLOSED_TENDER_STATUSES:
        return "closed", f"source_status:{normalized_status}"
    if deadline is not None and _aware(deadline) <= as_of:
        return "expired", "deadline_elapsed"
    if normalized_status == "active":
        return "active", None
    return normalized_status or "unknown", None


def _release_rank(
    source_observed_at: object, publication_date: object, release_id: str
) -> tuple[str, str, str]:
    return (
        _timestamp(_datetime(source_observed_at)) or "",
        _timestamp(_datetime(publication_date)) or "",
        release_id,
    )


def _recurrence_subject_id(scope: _RecurrenceKey) -> str:
    company_id, year, commodity_code, commodity_level, trade_role = scope
    return ":".join(
        [company_id, str(year), commodity_level, commodity_code, trade_role]
    )


def _parse_uk_trade_scope(scope_key: str) -> _UkTradeScope | None:
    match = re.fullmatch(
        r"uk-trade-info:(\d{4}):(\d{2,8}):(all|importers|exporters)",
        scope_key.strip().casefold(),
    )
    if match is None:
        return None
    trader_filter = match.group(3)
    if trader_filter == "importers":
        role = "Importer"
    elif trader_filter == "exporters":
        role = "Exporter"
    else:
        role = None
    return _UkTradeScope(
        year=int(match.group(1)),
        commodity_prefix=match.group(2),
        trade_role=role,
    )


def _recurrence_in_scope(
    payload: dict[str, object], scope: _UkTradeScope
) -> bool:
    try:
        activity_year = int(str(payload["activity_year"]))
    except (KeyError, ValueError):
        return False
    commodity_code = str(payload.get("commodity_code") or "")
    trade_role = str(payload.get("trade_role") or "")
    return (
        activity_year == scope.year
        and commodity_code.startswith(scope.commodity_prefix)
        and (scope.trade_role is None or trade_role == scope.trade_role)
    )


def _source_subject(source_name: str, source_id: str) -> str:
    source = re.sub(r"[^a-z0-9]+", "-", source_name.casefold()).strip("-")
    return f"{source}:{source_id}"


def _supplier_identity(party_id: str | None, name: str | None) -> str | None:
    if party_id:
        return "id:" + re.sub(r"[^A-Z0-9]+", "", party_id.upper())
    if name:
        normalized = re.sub(r"[^a-z0-9]+", "-", name.casefold()).strip("-")
        return f"name:{normalized}" if normalized else None
    return None


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return _aware(value)
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _aware(parsed)


def _timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _aware(value).isoformat().replace("+00:00", "Z")


def _number(value: object) -> str | None:
    if value is None:
        return None
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return str(value)
    if not number.is_finite():
        return str(number)
    if number == 0:
        return "0"
    normalized = number.normalize()
    return format(normalized, "f")


def _currency(value: object) -> str | None:
    text = _text(value)
    return text.upper() if text else None


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_json(value: object) -> object:
    if isinstance(value, str):
        try:
            parsed: object = json.loads(value)
            return parsed
        except json.JSONDecodeError:
            return None
    return value


def _json_strings(value: object) -> list[str]:
    parsed = _parse_json(value)
    if not isinstance(parsed, (list, tuple)):
        return []
    return [str(item) for item in parsed if item is not None]


def _json_mappings(value: object) -> list[dict[str, object]]:
    parsed = _parse_json(value)
    if not isinstance(parsed, list):
        return []
    return [
        {str(key): item for key, item in cast(dict[object, object], entry).items()}
        for entry in parsed
        if isinstance(entry, dict)
    ]


def _json_object(value: object) -> dict[str, object]:
    parsed = _parse_json(value)
    if not isinstance(parsed, dict):
        return {}
    return {
        str(key): item for key, item in cast(dict[object, object], parsed).items()
    }


def _canonical(value: object) -> object:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Decimal):
        return _number(value)
    if isinstance(value, float):
        return _number(value)
    if isinstance(value, datetime):
        return _timestamp(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)
        sorted_items = sorted(mapping.items(), key=lambda pair: str(pair[0]))
        return {str(key): _canonical(item) for key, item in sorted_items}
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [_canonical(item) for item in value]
        return sorted(items, key=_canonical_json)
    return str(value)


def _canonical_json(value: object) -> str:
    return json.dumps(
        _canonical(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


__all__ = ["ChangeEvent", "detect_material_changes"]
