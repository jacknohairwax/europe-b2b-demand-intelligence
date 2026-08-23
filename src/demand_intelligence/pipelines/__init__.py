"""End-to-end source pipelines."""

import json
from pathlib import Path

from pydantic import BaseModel

from demand_intelligence.connectors import TedSearchPage
from demand_intelligence.models import Evidence, ExplicitDemand, OpportunityScore
from demand_intelligence.normalize import normalize_ted_notice
from demand_intelligence.scoring import build_initial_opportunity
from demand_intelligence.storage import Database, SnapshotReference


def _json(value: object) -> str:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    return json.dumps(value, ensure_ascii=False, default=str)


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
                snapshot.content_sha256,
                snapshot.retrieved_at,
            ],
        )
        results.append(demand)
    return results


__all__ = ["process_ted_page"]
