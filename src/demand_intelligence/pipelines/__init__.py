"""End-to-end source pipelines."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel

from demand_intelligence.classification import ProductMatch, match_product_cluster
from demand_intelligence.connectors import TedSearchPage
from demand_intelligence.connectors.find_tender import FindTenderReleasePackage
from demand_intelligence.connectors.uk_trade_info import UkTradeInfoPage
from demand_intelligence.current_views import reconcile_find_tender_opportunities
from demand_intelligence.models import (
    CanonicalCompany,
    Evidence,
    ExplicitDemand,
    OpportunityScore,
    RecurrentDemandAccount,
    TradeActivity,
)
from demand_intelligence.normalize import normalize_ted_notice, normalize_uk_trade_info_record
from demand_intelligence.normalize.find_tender import (
    ProcurementRecord,
    normalize_find_tender_release,
    procurement_to_explicit_demand,
)
from demand_intelligence.normalize.ted import ted_payload_sha256
from demand_intelligence.reconciliation import (
    ScopeRunResult,
    apply_scope_run_completion,
    fail_scope_run,
    reconcile_procurement_release_versions,
    record_procurement_release_observation,
    record_scope_members,
    record_scope_page_completion,
)
from demand_intelligence.scoring import build_initial_opportunity
from demand_intelligence.storage import Database, SnapshotReference

_TradeActivityScope = tuple[int, str, str, str]
_UKTI_TRANSACTION_BATCH_SIZE = 500
_UKTI_RECURRENCE_BATCH_SIZE = 100


def _json_default(value: object) -> object:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    return str(value)


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, default=_json_default)


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


def _persist_evidence(database: Database, evidence: Evidence) -> None:
    database.connection.execute(
        """INSERT OR REPLACE INTO evidence VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            evidence.evidence_id,
            evidence.source_name,
            evidence.source_type,
            evidence.source_tier,
            evidence.source_url,
            evidence.retrieved_at,
            evidence.data_period,
            evidence.raw_source_id,
            evidence.raw_text_or_payload_reference,
            evidence.observed_fact,
            evidence.inference,
            evidence.confidence,
            _json(evidence.limitations),
        ],
    )


def _persist_demand(database: Database, item: ExplicitDemand) -> None:
    database.connection.execute(
        """INSERT OR REPLACE INTO explicit_demand (
        notice_id, buyer, buyer_country, buyer_city, product_category, cpv_codes,
        candidate_hs_codes, demand_type, publication_date, deadline, value, currency,
        quantity, requirement, procurement_type, qualifications, delivery_requirements,
        source, source_url, current_status, evidence_strength, confidence, next_action,
        evidence_ids
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            item.notice_id,
            item.buyer,
            item.buyer_country,
            item.buyer_city,
            item.product_category,
            _json(item.cpv_codes),
            _json(item.candidate_hs_codes),
            item.demand_type,
            item.publication_date,
            item.deadline,
            item.value,
            item.currency,
            item.quantity,
            item.requirement,
            item.procurement_type,
            item.qualifications,
            item.delivery_requirements,
            item.source,
            item.source_url,
            item.current_status.value,
            item.evidence_strength,
            item.confidence,
            item.next_action,
            _json(item.evidence_ids),
        ],
    )


def _persist_score(database: Database, score: OpportunityScore) -> None:
    database.connection.execute(
        """INSERT OR REPLACE INTO opportunity_scores VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )""",
        [
            score.opportunity_id,
            score.notice_id,
            score.account_id,
            score.buyer_identifiability,
            score.buyer_contactability,
            score.procurement_recurrence,
            score.procurement_visibility,
            score.supplier_switchability,
            score.chinese_supply_advantage,
            score.regulatory_accessibility,
            score.sales_cycle_accessibility,
            score.repeat_revenue_potential,
            score.margin_potential,
            score.total_score,
            _json(score.reasoning),
            _json(score.evidence_ids),
            _json(score.gate_answers),
            score.recommendation_status.value,
            score.scored_at,
        ],
    )


def _delete_find_tender_opportunity(database: Database, release_id: str) -> None:
    notice_id = f"find-tender:{release_id}"
    database.connection.execute(
        "DELETE FROM opportunity_scores WHERE notice_id = ?", [notice_id]
    )
    database.connection.execute(
        "DELETE FROM explicit_demand WHERE notice_id = ?", [notice_id]
    )


def _persist_trade_activity(
    database: Database,
    activity: TradeActivity,
    company_id: str,
    *,
    run_id: str | None = None,
) -> None:
    database.connection.execute(
        """INSERT INTO trade_activity (
        activity_id, source_record_id, company_id, trader_id, company_name,
        address_lines, postcode, commodity_code, hs2_code, hs4_code, hs6_code,
        commodity_code_level, product_description, year, month, trade_role, value, quantity,
        partner_country, source_url, evidence_id, raw_payload_reference,
        payload_sha256, is_current, first_seen_run_id, last_seen_run_id, removed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  TRUE, ?, ?, NULL)
        ON CONFLICT (activity_id) DO UPDATE SET
          source_record_id = excluded.source_record_id,
          company_id = excluded.company_id,
          trader_id = excluded.trader_id,
          company_name = excluded.company_name,
          address_lines = excluded.address_lines,
          postcode = excluded.postcode,
          commodity_code = excluded.commodity_code,
          hs2_code = excluded.hs2_code,
          hs4_code = excluded.hs4_code,
          hs6_code = excluded.hs6_code,
          commodity_code_level = excluded.commodity_code_level,
          product_description = excluded.product_description,
          year = excluded.year,
          month = excluded.month,
          trade_role = excluded.trade_role,
          value = excluded.value,
          quantity = excluded.quantity,
          partner_country = excluded.partner_country,
          source_url = excluded.source_url,
          evidence_id = excluded.evidence_id,
          raw_payload_reference = excluded.raw_payload_reference,
          payload_sha256 = excluded.payload_sha256,
          is_current = TRUE,
          first_seen_run_id = coalesce(
            trade_activity.first_seen_run_id, excluded.first_seen_run_id
          ),
          last_seen_run_id = excluded.last_seen_run_id,
          removed_at = NULL""",
        [
            activity.activity_id,
            activity.source_record_id,
            company_id,
            activity.trader_id,
            activity.company_name,
            _json(activity.address_lines),
            activity.postcode,
            activity.commodity_code,
            activity.hs2_code,
            activity.hs4_code,
            activity.hs6_code,
            activity.commodity_code_level,
            activity.product_description,
            activity.year,
            activity.month,
            activity.trade_role.value,
            activity.value,
            activity.quantity,
            activity.partner_country,
            activity.source_url,
            activity.evidence_id,
            activity.raw_payload_reference,
            activity.payload_sha256,
            run_id,
            run_id,
        ],
    )


def _persist_company(database: Database, company: CanonicalCompany) -> None:
    existing = database.connection.execute(
        """SELECT original_names, identifiers, addresses, evidence_ids,
                  CAST(first_seen_at AS VARCHAR), CAST(last_seen_at AS VARCHAR)
           FROM canonical_companies WHERE company_id = ?""",
        [company.company_id],
    ).fetchone()
    if existing:
        original_names = sorted(set(json.loads(existing[0])) | set(company.original_names))
        identifiers = {**json.loads(existing[1]), **company.identifiers}
        addresses = sorted(set(json.loads(existing[2])) | set(company.addresses))
        evidence_ids = sorted(set(json.loads(existing[3])) | set(company.evidence_ids))
        first_seen_at = min(datetime.fromisoformat(existing[4]), company.first_seen_at)
        last_seen_at = max(datetime.fromisoformat(existing[5]), company.last_seen_at)
    else:
        original_names = company.original_names
        identifiers = company.identifiers
        addresses = company.addresses
        evidence_ids = company.evidence_ids
        first_seen_at = company.first_seen_at
        last_seen_at = company.last_seen_at
    database.connection.execute(
        """INSERT OR REPLACE INTO canonical_companies VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )""",
        [
            company.company_id,
            company.preferred_name,
            company.normalized_name,
            company.country,
            _json(original_names),
            _json(identifiers),
            _json(addresses),
            _json(evidence_ids),
            first_seen_at,
            last_seen_at,
        ],
    )


def _raw_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [cast(dict[str, Any], item) for item in value if isinstance(item, dict)]


def _persist_procurement_release(
    database: Database,
    record: ProcurementRecord,
    raw: dict[str, Any],
    page: FindTenderReleasePackage,
    snapshot: SnapshotReference,
    product_match: ProductMatch | None,
) -> None:
    database.connection.execute(
        """INSERT OR IGNORE INTO procurement_releases (
        source_name, release_id, ocid, release_payload_sha256,
        package_payload_sha256, package_version, package_uri, snapshot_reference,
        snapshot_content_sha256, retrieved_at, publication_date, stages,
        tender_status, buyer_id, buyer_name, buyer_location, title, description,
        cpv_codes, deadline, procurement_type, procurement_category, is_framework,
        framework_stage, framework_periods, value, currency, value_basis,
        award_statuses, awarded_suppliers, product_category, candidate_hs_codes,
        taxonomy_mapping_status, source_url, evidence_ids, limitations, raw_release
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            record.source,
            record.notice_id,
            record.ocid,
            record.raw_payload_sha256,
            snapshot.payload_sha256 or snapshot.content_sha256,
            page.version,
            page.uri,
            snapshot.path,
            snapshot.content_sha256,
            snapshot.retrieved_at,
            record.publication_date,
            _json(record.stages),
            record.tender_status,
            record.buyer_id,
            record.buyer,
            _json(record.buyer_location) if record.buyer_location else None,
            record.title,
            record.description,
            _json(record.cpv_codes),
            record.deadline,
            record.procurement_type,
            record.procurement_category,
            record.is_framework,
            record.framework_stage,
            _json(record.framework_periods),
            record.value,
            record.currency,
            record.value_basis,
            _json(record.award_statuses),
            _json(record.awarded_suppliers),
            product_match.label if product_match else None,
            _json(product_match.candidate_hs_codes if product_match else []),
            product_match.mapping_status if product_match else None,
            record.source_url,
            _json(record.evidence_ids),
            _json(record.limitations),
            _json(raw),
        ],
    )
    record_procurement_release_observation(
        database.connection,
        source_name=record.source,
        release_id=record.notice_id,
        release_payload_sha256=record.raw_payload_sha256,
        observed_at=snapshot.retrieved_at,
        snapshot_reference=snapshot.path,
        snapshot_content_sha256=snapshot.content_sha256,
    )


def _persist_procurement_awards(
    database: Database, record: ProcurementRecord, raw: dict[str, Any]
) -> None:
    raw_awards = _raw_mappings(raw.get("awards"))
    evidence_id = record.evidence_ids[0] if record.evidence_ids else None
    for index, award in enumerate(record.awards):
        raw_award = raw_awards[index] if index < len(raw_awards) else {}
        database.connection.execute(
            """INSERT OR IGNORE INTO procurement_awards (
            source_name, release_id, release_payload_sha256, award_index,
            award_id, title, status, award_date, value, currency, value_basis,
            contract_status, contract_value, contract_currency,
            contract_value_basis, contract_period, suppliers, evidence_id, raw_award
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                record.source,
                record.notice_id,
                record.raw_payload_sha256,
                index,
                award.award_id,
                award.title,
                award.status,
                award.award_date,
                award.value,
                award.currency,
                award.value_basis,
                award.contract_status,
                award.contract_value,
                award.contract_currency,
                award.contract_value_basis,
                _json(award.contract_period) if award.contract_period else None,
                _json(award.suppliers),
                evidence_id,
                _json(raw_award),
            ],
        )


def _persist_procurement_parties(
    database: Database, record: ProcurementRecord, raw: dict[str, Any]
) -> None:
    raw_parties = _raw_mappings(raw.get("parties"))
    evidence_id = record.evidence_ids[0] if record.evidence_ids else None
    for index, party in enumerate(record.source_parties):
        raw_party = raw_parties[index] if index < len(raw_parties) else {}
        database.connection.execute(
            """INSERT OR IGNORE INTO procurement_parties (
            source_name, release_id, release_payload_sha256, party_index,
            party_id, name, roles, identifiers, location, evidence_id, raw_party
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                record.source,
                record.notice_id,
                record.raw_payload_sha256,
                index,
                party.party_id,
                party.name,
                _json(party.roles),
                _json(party.identifiers),
                _json(party.location) if party.location else None,
                evidence_id,
                _json(raw_party),
            ],
        )


def _rebuild_recurrent_account(
    database: Database,
    company_id: str,
    *,
    affected_groups: set[_TradeActivityScope],
    minimum_months: int,
) -> RecurrentDemandAccount | None:
    rows = database.connection.execute(
        """SELECT company_name, product_description, commodity_code,
                  commodity_code_level, year, month, trade_role, evidence_id
           FROM trade_activity WHERE company_id = ? AND is_current""",
        [company_id],
    ).fetchall()
    group_months: dict[tuple[int, str, str, str], set[int]] = {}
    group_evidence: dict[tuple[int, str, str, str], set[str]] = {}
    for row in rows:
        group = (int(row[4]), str(row[2]), str(row[3]), str(row[6]))
        group_months.setdefault(group, set()).add(int(row[5]))
        group_evidence.setdefault(group, set()).add(str(row[7]))
    qualifying_groups = {
        group for group, months in group_months.items() if len(months) >= minimum_months
    }
    database.connection.execute(
        "DELETE FROM recurrent_account_scopes WHERE company_id = ?", [company_id]
    )
    for year, code, level, role in sorted(qualifying_groups):
        group = (year, code, level, role)
        database.connection.execute(
            """INSERT INTO recurrent_account_scopes VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                company_id,
                year,
                code,
                level,
                role,
                _json(sorted(group_months[group])),
                _json(sorted(group_evidence[group])),
            ],
        )
    if not qualifying_groups:
        database.connection.execute(
            "DELETE FROM recurrent_accounts WHERE company_id = ?", [company_id]
        )
        return None
    qualifying_rows = [
        row
        for row in rows
        if (int(row[4]), str(row[2]), str(row[3]), str(row[6]))
        in qualifying_groups
    ]
    active_months = {(int(row[4]), int(row[5])) for row in qualifying_rows}
    activity_years = sorted({int(row[4]) for row in qualifying_rows})
    roles = {str(row[6]) for row in qualifying_rows}
    if roles == {"Importer"}:
        company_type = "Importer"
    elif roles == {"Exporter"}:
        company_type = "Exporter"
    elif "Importer" in roles and "Exporter" in roles:
        company_type = (
            "Importer and exporter with unresolved records"
            if "Unresolved" in roles
            else "Importer and exporter"
        )
    elif "Importer" in roles:
        company_type = "Importer with unresolved records"
    elif "Exporter" in roles:
        company_type = "Exporter with unresolved records"
    else:
        company_type = "Unresolved trader"
    evidence_ids = sorted({str(row[7]) for row in qualifying_rows})
    import_evidence = sorted(
        {str(row[7]) for row in qualifying_rows if row[6] == "Importer"}
    )
    products = sorted({str(row[1]) for row in qualifying_rows if row[1]})
    hs_codes = sorted({str(row[2]) for row in qualifying_rows})
    recurrence_groups = sorted(
        f"{year}:{level}:{code}:{role}"
        for year, code, level, role in qualifying_groups
    )
    existing_link = database.connection.execute(
        """SELECT procurement_evidence, buyer_role
           FROM recurrent_accounts WHERE company_id = ?""",
        [company_id],
    ).fetchone()
    procurement_evidence = (
        sorted(set(_json_strings(existing_link[0]))) if existing_link else []
    )
    buyer_role = (
        str(existing_link[1])
        if existing_link and existing_link[1] and procurement_evidence
        else "Not verified"
    )
    account = RecurrentDemandAccount(
        company_id=company_id,
        company_name=str(qualifying_rows[0][0]),
        country="Not verified",
        company_type=company_type,
        products=products,
        relevant_hs_codes=hs_codes,
        procurement_evidence=procurement_evidence,
        trade_evidence=evidence_ids,
        import_evidence=import_evidence,
        china_sourcing_evidence=[],
        months_active=len(active_months),
        estimated_scale=(
            f"{len(active_months)} distinct year-months across "
            f"{', '.join(str(year) for year in activity_years)} for "
            f"{len(qualifying_groups)} repeated commodity/role group(s); "
            "trader-level values and "
            "quantities are not disclosed."
        ),
        buyer_role=buyer_role,
        contact_status="Not verified",
        buying_signals=[
            f"Repeated {company_type.lower()} activity across "
            f"{len(active_months)} year-months."
        ],
        switching_hypothesis=None,
        evidence_strength="Tier 1 HMRC trader/product/month observations",
        confidence=0.9 if len(active_months) >= 3 else 0.8,
        next_action=(
            "Verify legal identity and purchasing contact; assess product and supplier fit."
        ),
        activity_year=activity_years[0] if len(activity_years) == 1 else None,
        recurrent_product_roles=recurrence_groups,
    )
    database.connection.execute(
        """INSERT OR REPLACE INTO recurrent_accounts (
        company_id, company_name, country, website, company_type, products,
        relevant_hs_codes, procurement_evidence, trade_evidence, import_evidence,
        china_sourcing_evidence, months_active, estimated_scale, buyer_role,
        contact_status, buying_signals, switching_hypothesis, evidence_strength,
        confidence, next_action, activity_year, recurrent_product_roles
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            account.company_id,
            account.company_name,
            account.country,
            account.website,
            account.company_type,
            _json(account.products),
            _json(account.relevant_hs_codes),
            _json(account.procurement_evidence),
            _json(account.trade_evidence),
            _json(account.import_evidence),
            _json(account.china_sourcing_evidence),
            account.months_active,
            account.estimated_scale,
            account.buyer_role,
            account.contact_status,
            _json(account.buying_signals),
            account.switching_hypothesis,
            account.evidence_strength,
            account.confidence,
            account.next_action,
            account.activity_year,
            _json(account.recurrent_product_roles),
        ],
    )
    return account if qualifying_groups & affected_groups else None


def process_ted_page(
    page: TedSearchPage,
    snapshot: SnapshotReference,
    database: Database,
    *,
    taxonomy_path: Path,
) -> list[ExplicitDemand]:
    results: list[ExplicitDemand] = []
    for raw in page.notices:
        demand, evidence = normalize_ted_notice(raw, snapshot, taxonomy_path=taxonomy_path)
        score = build_initial_opportunity(demand)
        _persist_evidence(database, evidence)
        _persist_demand(database, demand)
        _persist_score(database, score)
        database.connection.execute(
            "INSERT OR IGNORE INTO source_records VALUES (?, ?, ?, ?, ?)",
            [
                "TED Search API",
                demand.notice_id,
                snapshot.path,
                ted_payload_sha256(raw),
                snapshot.retrieved_at,
            ],
        )
        results.append(demand)
    return results


def process_uk_trade_info_page(
    page: UkTradeInfoPage,
    snapshot: SnapshotReference,
    database: Database,
    *,
    minimum_months: int = 2,
    run_id: str | None = None,
) -> list[RecurrentDemandAccount]:
    """Persist a page, bounding transaction memory for large official responses.

    Small pages remain one transaction. Large pages commit audited positive
    observations in bounded batches, then rebuild affected recurrence accounts in
    bounded batches. If a later batch fails, the run is non-authoritative, so prior
    positive observations remain valid while no unseen record can be tombstoned.
    """

    if run_id is None:
        return _process_uk_trade_info_page(
            page,
            snapshot,
            database,
            minimum_months=minimum_months,
            run_id=None,
        )
    if len(page.records) > _UKTI_TRANSACTION_BATCH_SIZE:
        affected_groups: dict[str, set[_TradeActivityScope]] = {}
        for offset in range(0, len(page.records), _UKTI_TRANSACTION_BATCH_SIZE):
            batch_page = page.model_copy(
                update={
                    "records": page.records[
                        offset : offset + _UKTI_TRANSACTION_BATCH_SIZE
                    ],
                    "next_link": None,
                }
            )
            database.connection.execute("BEGIN TRANSACTION")
            try:
                batch_groups = _persist_uk_trade_info_records(
                    batch_page,
                    snapshot,
                    database,
                    run_id=run_id,
                    page_completed=False,
                )
            except Exception:
                database.connection.execute("ROLLBACK")
                raise
            database.connection.execute("COMMIT")
            for company_id, groups in batch_groups.items():
                affected_groups.setdefault(company_id, set()).update(groups)

        accounts: list[RecurrentDemandAccount] = []
        company_ids = sorted(affected_groups)
        for offset in range(0, len(company_ids), _UKTI_RECURRENCE_BATCH_SIZE):
            database.connection.execute("BEGIN TRANSACTION")
            try:
                accounts.extend(
                    _rebuild_affected_groups(
                        database,
                        affected_groups,
                        company_ids=company_ids[
                            offset : offset + _UKTI_RECURRENCE_BATCH_SIZE
                        ],
                        minimum_months=minimum_months,
                    )
                )
            except Exception:
                database.connection.execute("ROLLBACK")
                raise
            database.connection.execute("COMMIT")
        record_scope_page_completion(database.connection, run_id=run_id)
        return accounts
    database.connection.execute("BEGIN TRANSACTION")
    try:
        accounts = _process_uk_trade_info_page(
            page,
            snapshot,
            database,
            minimum_months=minimum_months,
            run_id=run_id,
        )
    except Exception:
        database.connection.execute("ROLLBACK")
        raise
    database.connection.execute("COMMIT")
    return accounts


def _process_uk_trade_info_page(
    page: UkTradeInfoPage,
    snapshot: SnapshotReference,
    database: Database,
    *,
    minimum_months: int = 2,
    run_id: str | None = None,
    page_completed: bool = True,
) -> list[RecurrentDemandAccount]:
    affected_groups = _persist_uk_trade_info_records(
        page,
        snapshot,
        database,
        run_id=run_id,
        page_completed=page_completed,
    )
    return _rebuild_affected_groups(
        database,
        affected_groups,
        company_ids=sorted(affected_groups),
        minimum_months=minimum_months,
    )


def _persist_uk_trade_info_records(
    page: UkTradeInfoPage,
    snapshot: SnapshotReference,
    database: Database,
    *,
    run_id: str | None,
    page_completed: bool,
) -> dict[str, set[_TradeActivityScope]]:
    affected_groups: dict[str, set[_TradeActivityScope]] = {}
    scope_members: list[tuple[str, str]] = []
    for raw in page.records:
        activity, evidence, company = normalize_uk_trade_info_record(raw, snapshot)
        _persist_evidence(database, evidence)
        _persist_company(database, company)
        _persist_trade_activity(
            database, activity, company.company_id, run_id=run_id
        )
        database.connection.execute(
            """INSERT OR IGNORE INTO company_source_roles
               VALUES (?, ?, ?, ?, ?, ?)""",
            [
                company.company_id,
                "UK Trade Info",
                str(activity.trader_id),
                activity.company_name,
                activity.trade_role.value,
                activity.evidence_id,
            ],
        )
        database.connection.execute(
            "INSERT OR IGNORE INTO source_records VALUES (?, ?, ?, ?, ?)",
            [
                "UK Trade Info",
                activity.source_record_id,
                snapshot.path,
                activity.payload_sha256,
                snapshot.retrieved_at,
            ],
        )
        affected_groups.setdefault(company.company_id, set()).add(
            (
                activity.year,
                activity.commodity_code,
                activity.commodity_code_level,
                activity.trade_role.value,
            )
        )
        scope_members.append((activity.source_record_id, activity.payload_sha256))

    if run_id is not None:
        record_scope_members(
            database.connection,
            run_id=run_id,
            members=scope_members,
            observed_at=snapshot.retrieved_at,
            page_completed=page_completed,
        )
    return affected_groups


def _rebuild_affected_groups(
    database: Database,
    affected_groups: dict[str, set[_TradeActivityScope]],
    *,
    company_ids: list[str],
    minimum_months: int,
) -> list[RecurrentDemandAccount]:
    accounts: list[RecurrentDemandAccount] = []
    for company_id in company_ids:
        account = _rebuild_recurrent_account(
            database,
            company_id,
            affected_groups=affected_groups[company_id],
            minimum_months=minimum_months,
        )
        if account:
            accounts.append(account)
    return accounts


def rebuild_recurrent_accounts(
    database: Database,
    company_ids: set[str],
    *,
    minimum_months: int,
) -> list[RecurrentDemandAccount]:
    """Rebuild company recurrence after current activity was removed or restored."""

    accounts: list[RecurrentDemandAccount] = []
    for company_id in sorted(company_ids):
        current_groups = {
            (int(row[0]), str(row[1]), str(row[2]), str(row[3]))
            for row in database.connection.execute(
                """SELECT DISTINCT year, commodity_code, commodity_code_level,
                                  trade_role
                   FROM trade_activity
                   WHERE company_id = ? AND is_current""",
                [company_id],
            ).fetchall()
        }
        account = _rebuild_recurrent_account(
            database,
            company_id,
            affected_groups=current_groups,
            minimum_months=minimum_months,
        )
        if account is not None:
            accounts.append(account)
    return accounts


def finalize_uk_trade_info_run(
    database: Database,
    *,
    run_id: str,
    authoritative: bool,
    completion_reason: str,
    minimum_months: int,
    completed_at: datetime | None = None,
) -> tuple[ScopeRunResult, list[RecurrentDemandAccount]]:
    """Atomically reconcile removals, rebuild recurrence, and complete the run."""

    database.connection.execute("BEGIN TRANSACTION")
    try:
        result = apply_scope_run_completion(
            database.connection,
            run_id=run_id,
            authoritative=authoritative,
            completion_reason=completion_reason,
            completed_at=completed_at,
        )
        accounts = rebuild_recurrent_accounts(
            database,
            set(result.affected_company_ids),
            minimum_months=minimum_months,
        )
    except Exception as error:
        database.connection.execute("ROLLBACK")
        try:
            fail_scope_run(
                database.connection,
                run_id=run_id,
                error=f"{type(error).__name__}: {error}",
                completed_at=completed_at,
            )
        except ValueError:
            pass
        raise
    database.connection.execute("COMMIT")
    return result, accounts


def process_find_tender_page(
    page: FindTenderReleasePackage,
    snapshot: SnapshotReference,
    database: Database,
    *,
    taxonomy_path: Path,
) -> list[ExplicitDemand]:
    """Persist a full FTS page atomically, including current-version children."""

    database.connection.execute("BEGIN TRANSACTION")
    try:
        results = _process_find_tender_page(
            page,
            snapshot,
            database,
            taxonomy_path=taxonomy_path,
        )
    except Exception:
        database.connection.execute("ROLLBACK")
        raise
    database.connection.execute("COMMIT")
    return results


def _process_find_tender_page(
    page: FindTenderReleasePackage,
    snapshot: SnapshotReference,
    database: Database,
    *,
    taxonomy_path: Path,
) -> list[ExplicitDemand]:
    """Persist all procurement evidence and score only categorized active tenders."""

    results: list[ExplicitDemand] = []
    for raw in page.releases:
        record, evidence = normalize_find_tender_release(raw, snapshot)
        product_text = "\n".join(
            value
            for value in [
                record.title,
                record.description,
                *(award.title for award in record.awards),
            ]
            if value
        )
        product_match = match_product_cluster(
            cpv_codes=record.cpv_codes,
            text=product_text,
            taxonomy_path=taxonomy_path,
        )
        if product_match and product_match.candidate_hs_codes:
            mapping_status = product_match.mapping_status or "status not supplied"
            mapping_limitation = (
                "HS/CN codes are candidate taxonomy mappings "
                f"({mapping_status}); they require review."
            )
            evidence = evidence.model_copy(
                update={"limitations": [*evidence.limitations, mapping_limitation]}
            )
            record = record.model_copy(update={"limitations": evidence.limitations})

        _persist_evidence(database, evidence)
        _persist_procurement_release(
            database, record, raw, page, snapshot, product_match
        )
        _persist_procurement_awards(database, record, raw)
        _persist_procurement_parties(database, record, raw)
        database.connection.execute(
            "INSERT OR IGNORE INTO source_records VALUES (?, ?, ?, ?, ?)",
            [
                record.source,
                record.notice_id,
                snapshot.path,
                record.raw_payload_sha256,
                snapshot.retrieved_at,
            ],
        )

        current_sha = reconcile_procurement_release_versions(
            database.connection,
            source_name=record.source,
            release_id=record.notice_id,
        )
        # An old snapshot may be replayed after a correction. It remains valid
        # history but must not replace the newer current view.
        if current_sha != record.raw_payload_sha256:
            continue
        # Re-evaluating the current official payload always retracts the old
        # derived row before corrected category/status/deadline facts are applied.
        _delete_find_tender_opportunity(database, record.notice_id)
        demand = procurement_to_explicit_demand(record, evidence)
        if demand is None or product_match is None:
            continue
        demand = demand.model_copy(
            update={
                "notice_id": f"find-tender:{record.notice_id}",
                "product_category": product_match.label,
                "candidate_hs_codes": product_match.candidate_hs_codes,
            }
        )
        score = build_initial_opportunity(demand).model_copy(
            update={"opportunity_id": f"opp-find-tender-{record.notice_id}"}
        )
        _persist_demand(database, demand)
        _persist_score(database, score)
        results.append(demand)
    reconcile_find_tender_opportunities(
        database.connection, as_of=snapshot.retrieved_at
    )
    current_ids = {
        str(row[0])
        for row in database.connection.execute(
            """SELECT notice_id FROM explicit_demand
               WHERE source = 'Find a Tender OCDS API'"""
        ).fetchall()
    }
    return [item for item in results if item.notice_id in current_ids]


__all__ = [
    "finalize_uk_trade_info_run",
    "process_find_tender_page",
    "process_ted_page",
    "process_uk_trade_info_page",
    "rebuild_recurrent_accounts",
]
