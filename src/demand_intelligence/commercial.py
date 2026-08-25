"""Human-review and selected-category commercial current views.

The tables in this module are derived views.  They never coalesce canonical
companies or change automatic entity-link decisions.  A human review status is
workflow metadata only; applying a reviewed link requires a separate, explicit
operation.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import duckdb

FIND_TENDER_SOURCE = "Find a Tender OCDS API"
_STRONG_IDENTIFIER_SCHEMES = ("GB-COH", "GB-PPON", "GB-CHC", "GB-NHS")
_POSTCODE_RE = re.compile(
    r"\b(?:GIR\s?0AA|[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2})\b",
    flags=re.IGNORECASE,
)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS entity_review_queue (
  review_id VARCHAR PRIMARY KEY, case_id VARCHAR, decision_id VARCHAR,
  left_company_id VARCHAR, right_company_id VARCHAR, normalized_name VARCHAR,
  left_identity JSON, right_identity JSON, candidate_roles JSON,
  supporting_evidence JSON, conflicting_evidence JSON,
  automatic_status VARCHAR, match_type VARCHAR, confidence DOUBLE,
  rejection_reason VARCHAR, queue_status VARCHAR DEFAULT 'pending',
  reviewer VARCHAR, review_notes VARCHAR, reviewed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ,
  is_current BOOLEAN DEFAULT TRUE, material_hash VARCHAR
);
CREATE TABLE IF NOT EXISTS selected_category_commercial_view (
  view_key VARCHAR PRIMARY KEY, priority_rank INTEGER,
  as_of_run_id VARCHAR, generated_at TIMESTAMPTZ, activity_year INTEGER,
  category_id VARCHAR, product_category VARCHAR,
  taxonomy_mapping_status VARCHAR, commodity_code_level VARCHAR,
  commodity_code VARCHAR, product_description VARCHAR,
  company_id VARCHAR, company_name VARCHAR, active_months JSON,
  recurrent_month_count INTEGER, recurrence_threshold INTEGER,
  trade_role VARCHAR,
  has_procurement_evidence BOOLEAN, procurement_roles JSON,
  procurement_evidence JSON, evidence_ids JSON,
  evidence_source_count INTEGER, confidence DOUBLE,
  validation_status VARCHAR, next_action VARCHAR,
  authoritative_run_ids JSON, authoritative_scope_coverage JSON,
  accepted_link_evidence JSON
);
CREATE TABLE IF NOT EXISTS selected_category_commercial_view_state (
  view_id VARCHAR PRIMARY KEY, activity_year INTEGER, selector_type VARCHAR,
  selectors JSON, taxonomy_path VARCHAR, authoritative_run_ids JSON,
  trader_filter VARCHAR, recurrence_threshold INTEGER,
  refreshed_at TIMESTAMPTZ
);
ALTER TABLE entity_review_queue
  ADD COLUMN IF NOT EXISTS material_hash VARCHAR;
ALTER TABLE selected_category_commercial_view
  ADD COLUMN IF NOT EXISTS authoritative_run_ids JSON;
ALTER TABLE selected_category_commercial_view
  ADD COLUMN IF NOT EXISTS authoritative_scope_coverage JSON;
ALTER TABLE selected_category_commercial_view
  ADD COLUMN IF NOT EXISTS accepted_link_evidence JSON;
ALTER TABLE selected_category_commercial_view
  ADD COLUMN IF NOT EXISTS recurrence_threshold INTEGER DEFAULT 2;
ALTER TABLE selected_category_commercial_view_state
  ADD COLUMN IF NOT EXISTS recurrence_threshold INTEGER DEFAULT 2;
ALTER TABLE selected_category_commercial_view_state
  ADD COLUMN IF NOT EXISTS trader_filter VARCHAR DEFAULT 'All';
"""


@dataclass
class _Identity:
    source_name: str
    company_id: str
    names: set[str] = field(default_factory=set)
    identifiers: dict[str, set[str]] = field(default_factory=dict)
    addresses: set[str] = field(default_factory=set)
    postcodes: set[str] = field(default_factory=set)
    evidence_ids: set[str] = field(default_factory=set)
    roles: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class _Category:
    category_id: str
    label: str
    prefixes: tuple[str, ...]
    mapping_status: str


@dataclass
class _ProcurementSummary:
    records: list[dict[str, object]] = field(default_factory=list)
    links: list[dict[str, object]] = field(default_factory=list)
    roles: set[str] = field(default_factory=set)
    evidence_ids: set[str] = field(default_factory=set)
    link_confidences: list[float] = field(default_factory=list)


@dataclass(frozen=True)
class _CoverageRun:
    run_id: str
    hs_prefix: str
    trader_filter: str
    covered_roles: frozenset[str]


def ensure_operational_export_schema(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """Create the two derived-view tables idempotently."""

    connection.execute(_SCHEMA_SQL)


def refresh_entity_review_queue(
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of: datetime | None = None,
) -> int:
    """Refresh current non-accepted entity candidates without changing links.

    Generated identity and evidence fields are refreshed.  Human-owned workflow
    columns are deliberately omitted from the conflict update and therefore
    survive repeated refreshes.
    """

    ensure_operational_export_schema(connection)
    timestamp = _aware(as_of or datetime.now(UTC))
    connection.execute("BEGIN TRANSACTION")
    try:
        decisions = connection.execute(
            """SELECT decision.decision_id, decision.left_company_id,
                      decision.right_company_id, decision.normalized_name,
                      decision.status, decision.match_type, decision.confidence,
                      decision.reasoning
               FROM entity_link_decisions AS decision
               JOIN recurrent_accounts AS account
                 ON account.company_id = decision.left_company_id
               WHERE decision.is_current AND decision.status != 'accepted'
               ORDER BY decision.left_company_id, decision.right_company_id,
                        decision.decision_id"""
        ).fetchall()
        current_review_ids: set[str] = set()
        for decision in decisions:
            left_company_id = str(decision[1])
            right_company_id = str(decision[2])
            left = _current_trade_identity(connection, left_company_id)
            right = _current_procurement_identity(connection, right_company_id)
            # A stale automatic decision must not keep a removed source identity
            # in the current review queue.
            if left is None or right is None:
                continue
            normalized_name = str(decision[3])
            case_id = "review-case-" + _digest(
                f"{left_company_id}|{normalized_name}"
            )
            review_id = "review-candidate-" + _digest(
                f"{case_id}|{right_company_id}"
            )
            supporting = _supporting_evidence(
                normalized_name=normalized_name, left=left, right=right
            )
            conflicts = _conflicting_evidence(left=left, right=right)
            generated = {
                "decision_id": str(decision[0]),
                "normalized_name": normalized_name,
                "left_identity": _identity_payload(left),
                "right_identity": _identity_payload(right),
                "candidate_roles": sorted(right.roles),
                "supporting_evidence": supporting,
                "conflicting_evidence": conflicts,
                "automatic_status": str(decision[4]),
                "match_type": str(decision[5]),
                "confidence": float(decision[6]),
                "rejection_reason": str(decision[7]),
            }
            material_hash = hashlib.sha256(_json(generated).encode()).hexdigest()
            connection.execute(
                """INSERT INTO entity_review_queue (
                     review_id, case_id, decision_id, left_company_id,
                     right_company_id, normalized_name, left_identity,
                     right_identity, candidate_roles, supporting_evidence,
                     conflicting_evidence, automatic_status, match_type,
                     confidence, rejection_reason, queue_status, reviewer,
                     review_notes, reviewed_at, created_at, updated_at,
                     is_current, material_hash
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                             'pending', NULL, NULL, NULL, ?, ?, TRUE, ?)
                   ON CONFLICT (review_id) DO UPDATE SET
                     decision_id = excluded.decision_id,
                     normalized_name = excluded.normalized_name,
                     left_identity = excluded.left_identity,
                     right_identity = excluded.right_identity,
                     candidate_roles = excluded.candidate_roles,
                     supporting_evidence = excluded.supporting_evidence,
                     conflicting_evidence = excluded.conflicting_evidence,
                     automatic_status = excluded.automatic_status,
                     match_type = excluded.match_type,
                     confidence = excluded.confidence,
                     rejection_reason = excluded.rejection_reason,
                     updated_at = CASE
                       WHEN entity_review_queue.material_hash
                              IS DISTINCT FROM excluded.material_hash
                         OR NOT entity_review_queue.is_current
                         THEN excluded.updated_at
                       ELSE entity_review_queue.updated_at
                     END,
                     is_current = TRUE,
                     material_hash = excluded.material_hash""",
                [
                    review_id,
                    case_id,
                    str(decision[0]),
                    left_company_id,
                    right_company_id,
                    normalized_name,
                    _json(_identity_payload(left)),
                    _json(_identity_payload(right)),
                    _json(sorted(right.roles)),
                    _json(supporting),
                    _json(conflicts),
                    str(decision[4]),
                    str(decision[5]),
                    float(decision[6]),
                    str(decision[7]),
                    timestamp,
                    timestamp,
                    material_hash,
                ],
            )
            current_review_ids.add(review_id)
        if current_review_ids:
            placeholders = ", ".join("?" for _ in current_review_ids)
            connection.execute(
                f"""UPDATE entity_review_queue
                    SET is_current = FALSE, updated_at = ?
                    WHERE is_current AND review_id NOT IN ({placeholders})""",
                [timestamp, *sorted(current_review_ids)],
            )
        else:
            connection.execute(
                """UPDATE entity_review_queue
                   SET is_current = FALSE, updated_at = ? WHERE is_current""",
                [timestamp],
            )
    except Exception:
        connection.execute("ROLLBACK")
        raise
    connection.execute("COMMIT")
    return len(current_review_ids)


def refresh_selected_category_commercial_view(
    connection: duckdb.DuckDBPyConnection,
    *,
    year: int,
    taxonomy_path: Path,
    authoritative_run_ids: Iterable[str],
    recurrence_threshold: int = 2,
    trader_filter: str = "All",
    category_ids: Iterable[str] | None = None,
    hs_prefixes: Iterable[str] | None = None,
    as_of: datetime | None = None,
) -> int:
    """Build one exact-scope, current-only commercial answer.

    Selection is explicit: callers provide either taxonomy category IDs or HS/CN
    prefixes.  Rows remain separated by calendar year, disclosed code level/code,
    and importer/exporter role.
    """

    if year < 2000:
        raise ValueError("year must be a four-digit publication year")
    if recurrence_threshold < 2:
        raise ValueError("recurrence_threshold must be at least 2")
    allowed_roles = _covered_trade_roles(trader_filter)
    requested_categories = tuple(category_ids or ())
    requested_prefixes = tuple(hs_prefixes or ())
    categories = _selected_categories(
        taxonomy_path,
        category_ids=requested_categories,
        hs_prefixes=requested_prefixes,
    )
    coverage, selected_run_ids = _require_authoritative_coverage(
        connection,
        authoritative_run_ids=authoritative_run_ids,
        year=year,
        categories=categories,
    )
    ensure_operational_export_schema(connection)
    timestamp = _aware(as_of or datetime.now(UTC))
    refresh_entity_review_queue(connection, as_of=timestamp)

    rows = connection.execute(
        """SELECT scope.company_id, account.company_name, account.confidence,
                  scope.commodity_code, scope.commodity_code_level,
                  scope.trade_role
           FROM recurrent_account_scopes AS scope
           JOIN recurrent_accounts AS account
             ON account.company_id = scope.company_id
           WHERE scope.activity_year = ?
           ORDER BY scope.company_id, scope.commodity_code_level,
                    scope.commodity_code, scope.trade_role""",
        [year],
    ).fetchall()
    procurement_cache: dict[str, _ProcurementSummary] = {}
    projected: list[dict[str, object]] = []
    for row in rows:
        company_id = str(row[0])
        code = str(row[3])
        level = str(row[4])
        role = str(row[5])
        if role not in allowed_roles:
            continue
        matches = [
            category
            for category in categories
            if any(
                len(code) >= len(prefix) and code.startswith(prefix)
                for prefix in category.prefixes
            )
        ]
        if not matches:
            continue
        scope_coverage: dict[str, list[dict[str, object]]] = {}
        for category in matches:
            matching_prefixes = tuple(
                prefix
                for prefix in category.prefixes
                if len(code) >= len(prefix) and code.startswith(prefix)
            )
            scope_coverage[category.category_id] = _compatible_scope_coverage(
                coverage,
                prefixes=matching_prefixes,
                role=role,
                company_id=company_id,
                code=code,
            )
        activity = _current_scope_activity(
            connection,
            company_id=company_id,
            year=year,
            code=code,
            level=level,
            role=role,
        )
        # Repeated activity is never reconstructed from a stale scope row.
        activity_months = cast(set[int], activity["months"])
        if len(activity_months) < recurrence_threshold:
            continue
        procurement = procurement_cache.get(company_id)
        if procurement is None:
            procurement = _current_procurement_summary(connection, company_id)
            procurement_cache[company_id] = procurement
        has_procurement = bool(procurement.records)
        has_review = bool(
            connection.execute(
                """SELECT 1 FROM entity_review_queue
                   WHERE left_company_id = ? AND is_current LIMIT 1""",
                [company_id],
            ).fetchone()
        )
        account_confidence = float(row[2] or 0.0)
        confidence = account_confidence
        if procurement.link_confidences:
            confidence = min(account_confidence, min(procurement.link_confidences))
        evidence_ids = sorted(
            cast(set[str], activity["evidence_ids"]) | procurement.evidence_ids
        )
        source_count = _evidence_source_count(connection, evidence_ids)
        validation_status, next_action = _validation_and_action(
            has_procurement=has_procurement, has_review=has_review
        )
        for category in matches:
            view_key = "commercial-" + _digest(
                "|".join(
                    [
                        str(year),
                        category.category_id,
                        company_id,
                        level,
                        code,
                        role,
                    ]
                )
            )
            projected.append(
                {
                    "view_key": view_key,
                    "as_of_run_id": (
                        selected_run_ids[0] if len(selected_run_ids) == 1 else None
                    ),
                    "authoritative_run_ids": selected_run_ids,
                    "authoritative_scope_coverage": scope_coverage[
                        category.category_id
                    ],
                    "generated_at": timestamp,
                    "activity_year": year,
                    "category_id": category.category_id,
                    "product_category": category.label,
                    "taxonomy_mapping_status": category.mapping_status,
                    "commodity_code_level": level,
                    "commodity_code": code,
                    "product_description": activity["product_description"],
                    "company_id": company_id,
                    "company_name": str(row[1]),
                    "active_months": sorted(activity_months),
                    "recurrent_month_count": len(activity_months),
                    "recurrence_threshold": recurrence_threshold,
                    "trade_role": role,
                    "has_procurement_evidence": has_procurement,
                    "procurement_roles": sorted(procurement.roles),
                    "procurement_evidence": procurement.records,
                    "accepted_link_evidence": procurement.links,
                    "evidence_ids": evidence_ids,
                    "evidence_source_count": source_count,
                    "confidence": confidence,
                    "validation_status": validation_status,
                    "next_action": next_action,
                }
            )

    projected.sort(key=_commercial_sort_key)
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute("DELETE FROM selected_category_commercial_view")
        for rank, item in enumerate(projected, start=1):
            connection.execute(
                """INSERT INTO selected_category_commercial_view (
                     view_key, priority_rank, as_of_run_id, generated_at,
                     activity_year, category_id, product_category,
                     taxonomy_mapping_status, commodity_code_level,
                     commodity_code, product_description, company_id,
                     company_name, active_months, recurrent_month_count,
                     recurrence_threshold, trade_role,
                     has_procurement_evidence, procurement_roles,
                     procurement_evidence, evidence_ids, evidence_source_count,
                     confidence, validation_status, next_action,
                     authoritative_run_ids, authoritative_scope_coverage,
                     accepted_link_evidence
                   ) VALUES (
                     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                     ?, ?, ?, ?, ?, ?, ?, ?, ?
                   )""",
                [
                    item["view_key"],
                    rank,
                    item["as_of_run_id"],
                    item["generated_at"],
                    item["activity_year"],
                    item["category_id"],
                    item["product_category"],
                    item["taxonomy_mapping_status"],
                    item["commodity_code_level"],
                    item["commodity_code"],
                    item["product_description"],
                    item["company_id"],
                    item["company_name"],
                    _json(item["active_months"]),
                    item["recurrent_month_count"],
                    item["recurrence_threshold"],
                    item["trade_role"],
                    item["has_procurement_evidence"],
                    _json(item["procurement_roles"]),
                    _json(item["procurement_evidence"]),
                    _json(item["evidence_ids"]),
                    item["evidence_source_count"],
                    item["confidence"],
                    item["validation_status"],
                    item["next_action"],
                    _json(item["authoritative_run_ids"]),
                    _json(item["authoritative_scope_coverage"]),
                    _json(item["accepted_link_evidence"]),
                ],
            )
        selector_type = "category" if requested_categories else "hs_prefix"
        selectors = sorted(
            set(requested_categories if requested_categories else requested_prefixes)
        )
        connection.execute(
            """INSERT INTO selected_category_commercial_view_state (
                 view_id, activity_year, selector_type, selectors,
                 taxonomy_path, authoritative_run_ids, trader_filter,
                 recurrence_threshold, refreshed_at
               ) VALUES (
                 'current', ?, ?, ?, ?, ?, ?, ?, ?
               )
               ON CONFLICT (view_id) DO UPDATE SET
                 activity_year = excluded.activity_year,
                 selector_type = excluded.selector_type,
                 selectors = excluded.selectors,
                 taxonomy_path = excluded.taxonomy_path,
                 authoritative_run_ids = excluded.authoritative_run_ids,
                 trader_filter = excluded.trader_filter,
                 recurrence_threshold = excluded.recurrence_threshold,
                 refreshed_at = excluded.refreshed_at""",
            [
                year,
                selector_type,
                _json(selectors),
                str(taxonomy_path.resolve()),
                _json(selected_run_ids),
                trader_filter,
                recurrence_threshold,
                timestamp,
            ],
        )
    except Exception:
        connection.execute("ROLLBACK")
        raise
    connection.execute("COMMIT")
    return len(projected)


def refresh_existing_selected_category_commercial_view(
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of: datetime | None = None,
) -> int | None:
    """Refresh the last explicit selection after links or procurement facts change."""

    ensure_operational_export_schema(connection)
    row = connection.execute(
        """SELECT activity_year, selector_type, selectors, taxonomy_path,
                  authoritative_run_ids, trader_filter, recurrence_threshold
           FROM selected_category_commercial_view_state
           WHERE view_id = 'current'"""
    ).fetchone()
    if row is None:
        return None
    selectors = _json_strings(row[2])
    run_ids = _json_strings(row[4])
    selector_type = str(row[1])
    if selector_type not in {"category", "hs_prefix"}:
        raise ValueError(f"unsupported persisted selector type: {selector_type!r}")
    return refresh_selected_category_commercial_view(
        connection,
        year=int(row[0]),
        taxonomy_path=Path(str(row[3])),
        authoritative_run_ids=run_ids,
        recurrence_threshold=int(row[6]),
        trader_filter=str(row[5]),
        category_ids=selectors if selector_type == "category" else None,
        hs_prefixes=selectors if selector_type == "hs_prefix" else None,
        as_of=as_of,
    )


def _current_trade_identity(
    connection: duckdb.DuckDBPyConnection, company_id: str
) -> _Identity | None:
    rows = connection.execute(
        """SELECT company_name, trader_id, address_lines, postcode, evidence_id
           FROM trade_activity
           WHERE company_id = ? AND is_current
           ORDER BY year, month, commodity_code, trade_role""",
        [company_id],
    ).fetchall()
    if not rows:
        return None
    identity = _Identity(source_name="UK Trade Info", company_id=company_id)
    for name, trader_id, raw_addresses, postcode, evidence_id in rows:
        if name:
            identity.names.add(str(name))
        if trader_id is not None:
            identity.identifiers.setdefault("UKTI-TRADERID", set()).add(str(trader_id))
        address_parts = _json_strings(raw_addresses)
        if postcode:
            address_parts.append(str(postcode))
            identity.postcodes.add(_normalize_postcode(str(postcode)))
        rendered = ", ".join(part for part in address_parts if part)
        if rendered:
            identity.addresses.add(rendered)
            identity.postcodes.update(_extract_postcodes(rendered))
        if evidence_id:
            identity.evidence_ids.add(str(evidence_id))

    canonical = connection.execute(
        """SELECT identifiers FROM canonical_companies WHERE company_id = ?""",
        [company_id],
    ).fetchone()
    if canonical:
        _merge_identifiers(identity.identifiers, _identifier_mapping(canonical[0]))
    return identity


def _current_procurement_identity(
    connection: duckdb.DuckDBPyConnection, company_id: str
) -> _Identity | None:
    rows = _current_procurement_rows(connection, company_id)
    if not rows:
        return None
    identity = _Identity(source_name=FIND_TENDER_SOURCE, company_id=company_id)
    for row in rows:
        if row[0]:
            identity.names.add(str(row[0]))
        identity.roles.add(str(row[1]))
        if row[2]:
            identity.evidence_ids.add(str(row[2]))
        _merge_identifiers(identity.identifiers, _party_identifiers(row[3], row[5]))
        location = _json_object(row[4])
        address = _location_address(location)
        if address:
            identity.addresses.add(address)
            identity.postcodes.update(_extract_postcodes(address))
        postcode = _mapping_text(location, "postal_code", "postalCode")
        if postcode:
            identity.postcodes.add(_normalize_postcode(postcode))
    return identity


def _current_procurement_rows(
    connection: duckdb.DuckDBPyConnection, company_id: str
) -> list[tuple[object, ...]]:
    return connection.execute(
        """SELECT role.original_name, role.role, party.evidence_id,
                  party.identifiers, party.location, party.party_id,
                  release.source_name, release.release_id, release.ocid,
                  release.title, release.tender_status, release.publication_date,
                  release.deadline, release.value, release.currency,
                  release.source_url, release.evidence_ids,
                  release.award_statuses
           FROM company_source_roles AS role
           JOIN procurement_releases AS release
             ON lower(release.source_name) = lower(role.source_name)
            AND release.release_id = split_part(role.source_record_id, ':', 1)
            AND release.release_payload_sha256 =
                split_part(role.source_record_id, ':', 2)
            AND release.is_current
           JOIN procurement_parties AS party
             ON party.source_name = release.source_name
            AND party.release_id = release.release_id
            AND party.release_payload_sha256 = release.release_payload_sha256
            AND party.party_index =
                try_cast(split_part(role.source_record_id, ':', 3) AS INTEGER)
           WHERE role.company_id = ?
             AND lower(role.source_name) = lower(?)
           ORDER BY release.release_id, party.party_index, role.role""",
        [company_id, FIND_TENDER_SOURCE],
    ).fetchall()


def _current_scope_activity(
    connection: duckdb.DuckDBPyConnection,
    *,
    company_id: str,
    year: int,
    code: str,
    level: str,
    role: str,
) -> dict[str, object]:
    rows = connection.execute(
        """SELECT month, evidence_id, product_description
           FROM trade_activity
           WHERE company_id = ? AND year = ? AND commodity_code = ?
             AND commodity_code_level = ? AND trade_role = ? AND is_current
           ORDER BY month, evidence_id""",
        [company_id, year, code, level, role],
    ).fetchall()
    months = {int(row[0]) for row in rows}
    evidence_ids = {str(row[1]) for row in rows if row[1]}
    descriptions = sorted({str(row[2]) for row in rows if row[2]})
    return {
        "months": months,
        "evidence_ids": evidence_ids,
        "product_description": "; ".join(descriptions) if descriptions else None,
    }


def _current_procurement_summary(
    connection: duckdb.DuckDBPyConnection, left_company_id: str
) -> _ProcurementSummary:
    decisions = connection.execute(
        """SELECT decision_id, right_company_id, status, match_type,
                  confidence, reasoning, evidence_ids
           FROM entity_link_decisions
           WHERE left_company_id = ? AND is_current AND status = 'accepted'
           ORDER BY right_company_id, decision_id""",
        [left_company_id],
    ).fetchall()
    summary = _ProcurementSummary()
    by_release: dict[tuple[str, str], dict[str, object]] = {}
    confidence_by_candidate: dict[str, float] = {}
    for decision in decisions:
        candidate_id = str(decision[1])
        rows = _current_procurement_rows(connection, candidate_id)
        right_identity = _current_procurement_identity(connection, candidate_id)
        if not rows or right_identity is None:
            continue
        decision_evidence_ids = sorted(_json_strings(decision[6]))
        confidence_by_candidate[candidate_id] = float(decision[4])
        summary.evidence_ids.update(decision_evidence_ids)
        summary.links.append(
            {
                "decision_id": str(decision[0]),
                "left_company_id": left_company_id,
                "right_company_id": candidate_id,
                "status": str(decision[2]),
                "match_type": str(decision[3]),
                "confidence": float(decision[4]),
                "reasoning": str(decision[5]),
                "evidence_ids": decision_evidence_ids,
                "right_identity": _identity_payload(right_identity),
            }
        )
        for row in rows:
            source_name = str(row[6])
            release_id = str(row[7])
            key = (source_name, release_id)
            roles = by_release.setdefault(
                key,
                {
                    "source_name": source_name,
                    "release_id": release_id,
                    "ocid": _text(row[8]),
                    "title": _text(row[9]),
                    "tender_status": _text(row[10]),
                    "publication_date": _render_temporal(row[11]),
                    "deadline": _render_temporal(row[12]),
                    "value": str(row[13]) if row[13] is not None else None,
                    "currency": _text(row[14]),
                    "source_url": _text(row[15]),
                    "award_statuses": sorted(_json_strings(row[17])),
                    "roles": set(),
                    "evidence_ids": set(),
                },
            )
            cast(set[str], roles["roles"]).add(str(row[1]))
            evidence_ids = cast(set[str], roles["evidence_ids"])
            if row[2]:
                evidence_ids.add(str(row[2]))
            evidence_ids.update(_json_strings(row[16]))
            summary.roles.add(str(row[1]))

    summary.link_confidences = list(confidence_by_candidate.values())
    summary.links.sort(key=lambda item: str(item["decision_id"]))
    for key in sorted(by_release):
        record = by_release[key]
        record["roles"] = sorted(cast(set[str], record["roles"]))
        awards, award_evidence_ids = _current_award_facts(
            connection, source_name=key[0], release_id=key[1]
        )
        cast(set[str], record["evidence_ids"]).update(award_evidence_ids)
        record["awards"] = awards
        record["evidence_ids"] = sorted(cast(set[str], record["evidence_ids"]))
        summary.evidence_ids.update(cast(list[str], record["evidence_ids"]))
        summary.records.append(record)
    return summary


def _current_award_facts(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_name: str,
    release_id: str,
) -> tuple[list[dict[str, object]], set[str]]:
    rows = connection.execute(
        """SELECT award.award_index, award.award_id, award.title,
                  award.status, award.award_date, award.value, award.currency,
                  award.value_basis, award.contract_status,
                  award.contract_value, award.contract_currency,
                  award.contract_value_basis, award.contract_period,
                  award.suppliers, award.evidence_id
           FROM procurement_awards AS award
           JOIN procurement_releases AS release
             ON release.source_name = award.source_name
            AND release.release_id = award.release_id
            AND release.release_payload_sha256 = award.release_payload_sha256
            AND release.is_current
           WHERE award.source_name = ? AND award.release_id = ?
           ORDER BY award.award_index""",
        [source_name, release_id],
    ).fetchall()
    evidence_ids = {str(row[14]) for row in rows if row[14]}
    awards = [
        {
            "award_index": int(row[0]),
            "award_id": _text(row[1]),
            "title": _text(row[2]),
            "status": _text(row[3]),
            "award_date": _render_temporal(row[4]),
            "value": str(row[5]) if row[5] is not None else None,
            "currency": _text(row[6]),
            "value_basis": _text(row[7]),
            "contract_status": _text(row[8]),
            "contract_value": str(row[9]) if row[9] is not None else None,
            "contract_currency": _text(row[10]),
            "contract_value_basis": _text(row[11]),
            "contract_period": _parse_json(row[12]),
            "suppliers": _json_mappings(row[13]),
            "evidence_id": _text(row[14]),
        }
        for row in rows
    ]
    return awards, evidence_ids


def _selected_categories(
    taxonomy_path: Path,
    *,
    category_ids: Iterable[str] | None,
    hs_prefixes: Iterable[str] | None,
) -> list[_Category]:
    requested_categories = sorted(
        {value.strip() for value in category_ids or [] if value.strip()}
    )
    requested_prefixes = sorted(
        {_normalize_code(value) for value in hs_prefixes or [] if value.strip()}
    )
    if bool(requested_categories) == bool(requested_prefixes):
        raise ValueError("provide exactly one of category_ids or hs_prefixes")
    payload: object = json.loads(taxonomy_path.read_text())
    if not isinstance(payload, dict):
        raise ValueError("taxonomy root must be an object")
    raw_clusters = cast(dict[str, object], payload).get("clusters")
    if not isinstance(raw_clusters, list):
        raise ValueError("taxonomy clusters must be a list")
    available: list[_Category] = []
    for raw in cast(list[object], raw_clusters):
        if not isinstance(raw, dict):
            continue
        cluster = cast(dict[str, object], raw)
        category_id = _text(cluster.get("id"))
        label = _text(cluster.get("label"))
        prefixes = tuple(
            sorted(
                {
                    _normalize_code(value)
                    for value in _string_list(cluster.get("candidate_hs_codes"))
                }
            )
        )
        if category_id and label and prefixes:
            available.append(
                _Category(
                    category_id=category_id,
                    label=label,
                    prefixes=prefixes,
                    mapping_status=_text(cluster.get("mapping_status"))
                    or "candidate_requires_review",
                )
            )
    if requested_categories:
        by_id = {item.category_id: item for item in available}
        missing = sorted(set(requested_categories) - set(by_id))
        if missing:
            raise ValueError(f"unknown taxonomy category ID(s): {', '.join(missing)}")
        return [by_id[item] for item in requested_categories]

    selected_prefixes: dict[str, set[str]] = {}
    selected_categories: dict[str, _Category] = {}
    missing_prefixes: list[str] = []
    for requested in requested_prefixes:
        matched = False
        for category in available:
            if any(requested.startswith(candidate) for candidate in category.prefixes):
                selected_categories[category.category_id] = category
                selected_prefixes.setdefault(category.category_id, set()).add(requested)
                matched = True
        if not matched:
            missing_prefixes.append(requested)
    if missing_prefixes:
        raise ValueError(
            "HS/CN prefix is outside the selected taxonomy: "
            + ", ".join(missing_prefixes)
        )
    return [
        _Category(
            category_id=category_id,
            label=selected_categories[category_id].label,
            prefixes=tuple(sorted(prefixes)),
            mapping_status=selected_categories[category_id].mapping_status,
        )
        for category_id, prefixes in sorted(selected_prefixes.items())
    ]


def _require_authoritative_coverage(
    connection: duckdb.DuckDBPyConnection,
    *,
    authoritative_run_ids: Iterable[str],
    year: int,
    categories: list[_Category],
) -> tuple[dict[str, list[_CoverageRun]], list[str]]:
    run_ids = sorted({value.strip() for value in authoritative_run_ids if value.strip()})
    if not run_ids:
        raise ValueError("authoritative_run_ids must contain completed UK Trade Info runs")
    requested_prefixes = sorted(
        {prefix for category in categories for prefix in category.prefixes}
    )
    coverage: dict[str, list[_CoverageRun]] = {
        prefix: [] for prefix in requested_prefixes
    }
    for run_id in run_ids:
        row = connection.execute(
            """SELECT connector, status, is_authoritative, scope_parameters
               FROM ingestion_run_scopes WHERE run_id = ?""",
            [run_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"unknown completed-run scope: {run_id}")
        if str(row[0]) != "UK Trade Info":
            raise ValueError(f"run {run_id} is not a UK Trade Info run")
        if str(row[1]) != "completed" or not bool(row[2]):
            raise ValueError(
                f"run {run_id} is not a completed authoritative scope"
            )
        parameters = _json_object(row[3])
        try:
            observed_year = int(str(parameters.get("year")))
        except ValueError as error:
            raise ValueError(
                f"completed run {run_id} does not declare a valid activity year"
            ) from error
        if observed_year != year:
            raise ValueError(
                f"completed run {run_id} covers {observed_year}, not requested year {year}"
            )
        run_prefix = _normalize_code(str(parameters.get("hs_prefix", "")))
        trader_filter = str(parameters.get("trader_filter", "")).strip()
        covered_roles = _covered_trade_roles(trader_filter)
        covered_requested = [
            prefix for prefix in requested_prefixes if prefix.startswith(run_prefix)
        ]
        if not covered_requested:
            raise ValueError(
                f"completed run {run_id} prefix {run_prefix} does not cover "
                "the selected HS/CN prefixes"
            )
        coverage_run = _CoverageRun(
            run_id=run_id,
            hs_prefix=run_prefix,
            trader_filter=trader_filter,
            covered_roles=covered_roles,
        )
        for prefix in covered_requested:
            coverage[prefix].append(coverage_run)
    missing = [prefix for prefix, runs in coverage.items() if not runs]
    if missing:
        raise ValueError(
            "missing completed authoritative coverage for selected HS/CN prefix(es): "
            + ", ".join(missing)
        )
    return coverage, run_ids


def _covered_trade_roles(trader_filter: str) -> frozenset[str]:
    normalized = trader_filter.lower()
    if normalized == "all":
        return frozenset({"Importer", "Exporter", "Unresolved"})
    if normalized == "importers":
        return frozenset({"Importer"})
    if normalized == "exporters":
        return frozenset({"Exporter"})
    raise ValueError(f"unsupported UK Trade Info trader filter: {trader_filter!r}")


def _compatible_scope_coverage(
    coverage: dict[str, list[_CoverageRun]],
    *,
    prefixes: tuple[str, ...],
    role: str,
    company_id: str,
    code: str,
) -> list[dict[str, object]]:
    compatible = {
        (run.run_id, prefix): run
        for prefix in prefixes
        for run in coverage[prefix]
        if role in run.covered_roles
    }
    if not compatible:
        raise ValueError(
            f"no authoritative scope covers {company_id} {code} with role {role}"
        )
    return [
        {
            "run_id": run.run_id,
            "selected_prefix": prefix,
            "source_hs_prefix": run.hs_prefix,
            "trader_filter": run.trader_filter,
            "covered_roles": sorted(run.covered_roles),
        }
        for (run_id, prefix), run in sorted(compatible.items())
    ]


def _supporting_evidence(
    *, normalized_name: str, left: _Identity, right: _Identity
) -> dict[str, object]:
    matching_ids: dict[str, list[str]] = {}
    for scheme in _STRONG_IDENTIFIER_SCHEMES:
        shared = left.identifiers.get(scheme, set()) & right.identifiers.get(
            scheme, set()
        )
        if shared:
            matching_ids[scheme] = sorted(shared)
    return {
        "candidate_basis": {
            "field": "normalized_legal_name",
            "value": normalized_name,
        },
        "left_evidence_ids": sorted(left.evidence_ids),
        "right_evidence_ids": sorted(right.evidence_ids),
        "shared_postcodes": sorted(left.postcodes & right.postcodes),
        "shared_strong_identifiers": matching_ids,
    }


def _conflicting_evidence(
    *, left: _Identity, right: _Identity
) -> list[dict[str, object]]:
    conflicts: list[dict[str, object]] = []
    for scheme in _STRONG_IDENTIFIER_SCHEMES:
        left_values = left.identifiers.get(scheme, set())
        right_values = right.identifiers.get(scheme, set())
        if left_values and right_values and not left_values & right_values:
            conflicts.append(
                _conflict_payload(
                    field_name=f"identifier:{scheme}",
                    left_values=left_values,
                    right_values=right_values,
                    left=left,
                    right=right,
                )
            )
    if left.postcodes and right.postcodes and not left.postcodes & right.postcodes:
        conflicts.append(
            _conflict_payload(
                field_name="postcode",
                left_values=left.postcodes,
                right_values=right.postcodes,
                left=left,
                right=right,
            )
        )
    left_addresses = {
        value for item in left.addresses if (value := _normalize_address(item))
    }
    right_addresses = {
        value for item in right.addresses if (value := _normalize_address(item))
    }
    if left_addresses and right_addresses and not left_addresses & right_addresses:
        conflicts.append(
            _conflict_payload(
                field_name="normalized_address",
                left_values=left_addresses,
                right_values=right_addresses,
                left=left,
                right=right,
            )
        )
    return conflicts


def _conflict_payload(
    *,
    field_name: str,
    left_values: set[str],
    right_values: set[str],
    left: _Identity,
    right: _Identity,
) -> dict[str, object]:
    return {
        "field": field_name,
        "left_values": sorted(left_values),
        "right_values": sorted(right_values),
        "left_evidence_ids": sorted(left.evidence_ids),
        "right_evidence_ids": sorted(right.evidence_ids),
    }


def _identity_payload(identity: _Identity) -> dict[str, object]:
    return {
        "source_name": identity.source_name,
        "company_id": identity.company_id,
        "names": sorted(identity.names),
        "identifiers": {
            key: sorted(values) for key, values in sorted(identity.identifiers.items())
        },
        "addresses": sorted(identity.addresses),
        "postcodes": sorted(identity.postcodes),
        "evidence_ids": sorted(identity.evidence_ids),
        "roles": sorted(identity.roles),
    }


def _party_identifiers(value: object, party_id: object) -> dict[str, set[str]]:
    found: dict[str, set[str]] = {}
    parsed = _parse_json(value)
    if isinstance(parsed, list):
        for raw in cast(list[object], parsed):
            if not isinstance(raw, dict):
                continue
            item = cast(dict[object, object], raw)
            scheme = _normalize_identifier_scheme(_text(item.get("scheme")))
            identifier = _normalize_identifier_value(_text(item.get("value")))
            if scheme and identifier:
                found.setdefault(scheme, set()).add(identifier)
    party_text = _text(party_id)
    if party_text:
        normalized = unicodedata.normalize("NFKC", party_text).upper()
        for scheme in _STRONG_IDENTIFIER_SCHEMES:
            prefix = scheme + "-"
            if normalized.startswith(prefix):
                identifier = _normalize_identifier_value(normalized.removeprefix(prefix))
                if identifier:
                    found.setdefault(scheme, set()).add(identifier)
    return found


def _identifier_mapping(value: object) -> dict[str, set[str]]:
    parsed = _json_object(value)
    found: dict[str, set[str]] = {}
    for raw_scheme, raw_identifier in parsed.items():
        scheme = _normalize_identifier_scheme(str(raw_scheme).split("#", maxsplit=1)[0])
        identifier = _normalize_identifier_value(_text(raw_identifier))
        if scheme and identifier:
            found.setdefault(scheme, set()).add(identifier)
    return found


def _merge_identifiers(
    target: dict[str, set[str]], incoming: dict[str, set[str]]
) -> None:
    for scheme, values in incoming.items():
        target.setdefault(scheme, set()).update(values)


def _location_address(location: dict[str, object]) -> str | None:
    values = [
        _mapping_text(location, "street_address", "streetAddress"),
        _mapping_text(location, "locality"),
        _mapping_text(location, "region"),
        _mapping_text(location, "postal_code", "postalCode"),
    ]
    rendered = ", ".join(value for value in values if value)
    return rendered or None


def _mapping_text(mapping: dict[str, object], *keys: str) -> str | None:
    for key in keys:
        value = _text(mapping.get(key))
        if value:
            return value
    return None


def _extract_postcodes(value: str) -> set[str]:
    return {_normalize_postcode(match) for match in _POSTCODE_RE.findall(value.upper())}


def _normalize_postcode(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", value.upper())


def _normalize_address(value: str) -> str:
    text = unicodedata.normalize("NFKC", value).upper().replace("&", " AND ")
    text = _POSTCODE_RE.sub(" ", text)
    return " ".join(re.sub(r"[^A-Z0-9]+", " ", text).split())


def _normalize_identifier_scheme(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"[^A-Z0-9]+", "-", value.upper()).strip("-")
    return normalized or None


def _normalize_identifier_value(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"[^A-Z0-9]+", "", unicodedata.normalize("NFKC", value).upper())
    return normalized or None


def _validation_and_action(
    *, has_procurement: bool, has_review: bool
) -> tuple[str, str]:
    if has_procurement:
        return (
            "Validated entity link; taxonomy candidate requires review",
            "Validate procurement/category fit and identify the purchasing contact.",
        )
    if has_review:
        return (
            "Entity link requires human review; taxonomy candidate requires review",
            "Review the entity candidate before attaching procurement evidence.",
        )
    return (
        "Trade recurrence verified; procurement not verified; taxonomy candidate requires review",
        "Search for a procurement identity and validate the legal entity.",
    )


def _evidence_source_count(
    connection: duckdb.DuckDBPyConnection, evidence_ids: list[str]
) -> int:
    if not evidence_ids:
        return 0
    placeholders = ", ".join("?" for _ in evidence_ids)
    row = connection.execute(
        f"""SELECT count(DISTINCT source_name) FROM evidence
            WHERE evidence_id IN ({placeholders})""",
        evidence_ids,
    ).fetchone()
    return int(row[0]) if row else 0


def _commercial_sort_key(item: dict[str, object]) -> tuple[object, ...]:
    return (
        -int(bool(item["has_procurement_evidence"])),
        -cast(int, item["evidence_source_count"]),
        -cast(int, item["recurrent_month_count"]),
        -cast(float, item["confidence"]),
        str(item["company_name"]),
        str(item["commodity_code"]),
        str(item["trade_role"]),
    )


def _render_temporal(value: object) -> str | None:
    return str(value) if value is not None else None


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:24]


def _normalize_code(value: str) -> str:
    normalized = re.sub(r"\s+", "", value)
    if not normalized.isdigit():
        raise ValueError(f"invalid HS/CN prefix: {value!r}")
    return normalized


def _text(value: object) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _parse_json(value: object) -> object:
    if isinstance(value, str):
        try:
            parsed: object = json.loads(value)
            return parsed
        except json.JSONDecodeError:
            return None
    return value


def _json_object(value: object) -> dict[str, object]:
    parsed = _parse_json(value)
    if not isinstance(parsed, dict):
        return {}
    return {str(key): item for key, item in cast(dict[object, object], parsed).items()}


def _json_strings(value: object) -> list[str]:
    parsed = _parse_json(value)
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in cast(list[object], parsed) if item is not None]


def _json_mappings(value: object) -> list[dict[str, object]]:
    parsed = _parse_json(value)
    if not isinstance(parsed, list):
        return []
    return [
        {str(key): item for key, item in cast(dict[object, object], raw).items()}
        for raw in cast(list[object], parsed)
        if isinstance(raw, dict)
    ]


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in cast(list[object], value) if item is not None]


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


__all__ = [
    "ensure_operational_export_schema",
    "refresh_entity_review_queue",
    "refresh_existing_selected_category_commercial_view",
    "refresh_selected_category_commercial_view",
]
