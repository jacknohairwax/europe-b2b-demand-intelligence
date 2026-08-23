"""Explainable, deliberately conservative opportunity scoring."""

from datetime import UTC, datetime

from demand_intelligence.models import (
    ExplicitDemand,
    GateAnswer,
    OpportunityScore,
    RecommendationStatus,
)

QUESTIONS = [
    "Who buys?",
    "What do they buy?",
    "How often?",
    "Approximate scale?",
    "From whom?",
    "Is China accepted?",
    "Who controls purchasing?",
    "Can they be contacted?",
    "Why might they add/change suppliers?",
    "Can Chinese supply realistically fulfill?",
    "What compliance constraints apply?",
    "What evidence proves the opportunity?",
]


def build_initial_opportunity(demand: ExplicitDemand) -> OpportunityScore:
    answers = [GateAnswer(question_number=i, question=q) for i, q in enumerate(QUESTIONS, 1)]

    def answer(number: int, value: str | None) -> None:
        if value:
            answers[number - 1].answer = value
            answers[number - 1].is_evidence_backed = True
            answers[number - 1].evidence_ids = list(demand.evidence_ids)

    answer(1, demand.buyer)
    answer(2, demand.requirement)
    scale = f"{demand.value} {demand.currency or ''}".strip() if demand.value is not None else None
    answer(4, scale)
    answer(12, f"Official {demand.source} notice {demand.notice_id}")
    backed_count = sum(item.is_evidence_backed for item in answers)

    components = {
        "buyer_identifiability": 5 if demand.buyer else 0,
        "buyer_contactability": 0,
        "procurement_recurrence": 0,
        "procurement_visibility": 5 if demand.source_url else 0,
        "supplier_switchability": 0,
        "chinese_supply_advantage": 0,
        "regulatory_accessibility": 0,
        "sales_cycle_accessibility": 1 if demand.deadline else 0,
        "repeat_revenue_potential": 0,
        "margin_potential": 0,
    }
    reasoning = {
        "buyer_identifiability": "Named in the official notice." if demand.buyer else "Unknown.",
        "buyer_contactability": "No verified purchasing contact in this slice.",
        "procurement_recurrence": "No historical recurrence evidence loaded.",
        "procurement_visibility": (
            "Official notice URL available." if demand.source_url else "Unknown."
        ),
        "supplier_switchability": "Not verified.",
        "chinese_supply_advantage": "Not verified.",
        "regulatory_accessibility": "Tender documents not reviewed.",
        "sales_cycle_accessibility": (
            "Deadline observed." if demand.deadline else "Deadline unknown."
        ),
        "repeat_revenue_potential": "No framework or recurrence evidence loaded.",
        "margin_potential": "Costs and commercial terms unknown.",
    }
    recommendation = (
        RecommendationStatus.RECOMMENDED
        if backed_count >= 7
        else RecommendationStatus.NEEDS_VALIDATION
    )
    return OpportunityScore(
        opportunity_id=f"opp-ted-{demand.notice_id}",
        notice_id=demand.notice_id,
        account_id=None,
        **components,
        total_score=sum(components.values()),
        reasoning=reasoning,
        evidence_ids=list(demand.evidence_ids),
        gate_answers=answers,
        recommendation_status=recommendation,
        scored_at=datetime.now(UTC),
    )


__all__ = ["build_initial_opportunity"]
