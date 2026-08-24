"""Shared, source-independent records for the demand harness."""

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class DemandStatus(StrEnum):
    VERIFIED = "Verified Explicit Demand"
    PARTIAL = "Partially Verified Explicit Demand"
    UNVERIFIED = "Unverified Lead"


class RecommendationStatus(StrEnum):
    RECOMMENDED = "Recommended Opportunity"
    NEEDS_VALIDATION = "Needs Further Validation"


class TradeRole(StrEnum):
    IMPORTER = "Importer"
    EXPORTER = "Exporter"
    UNRESOLVED = "Unresolved"


class Evidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence_id: str
    source_name: str
    source_type: str
    source_tier: int = Field(ge=1, le=4)
    source_url: str
    retrieved_at: datetime
    data_period: str | None = None
    raw_source_id: str
    raw_text_or_payload_reference: str
    observed_fact: str
    inference: str | None = None
    confidence: float = Field(ge=0, le=1)
    limitations: list[str] = Field(default_factory=list)


class ExplicitDemand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    notice_id: str
    buyer: str | None = None
    buyer_country: str | None = None
    buyer_city: str | None = None
    product_category: str | None = None
    cpv_codes: list[str] = Field(default_factory=list)
    candidate_hs_codes: list[str] = Field(default_factory=list)
    demand_type: str = "Explicit Demand"
    publication_date: date | None = None
    deadline: date | None = None
    value: Decimal | None = None
    currency: str | None = None
    quantity: str | None = None
    requirement: str | None = None
    procurement_type: str | None = None
    qualifications: str | None = None
    delivery_requirements: str | None = None
    source: str
    source_url: str
    current_status: DemandStatus
    evidence_strength: str
    confidence: float = Field(ge=0, le=1)
    next_action: str
    evidence_ids: list[str] = Field(default_factory=list)


class GateAnswer(BaseModel):
    question_number: int = Field(ge=1, le=12)
    question: str
    answer: str | None = None
    is_evidence_backed: bool = False
    evidence_ids: list[str] = Field(default_factory=list)


class OpportunityScore(BaseModel):
    opportunity_id: str
    notice_id: str | None = None
    account_id: str | None = None
    buyer_identifiability: int = Field(ge=0, le=5)
    buyer_contactability: int = Field(ge=0, le=5)
    procurement_recurrence: int = Field(ge=0, le=5)
    procurement_visibility: int = Field(ge=0, le=5)
    supplier_switchability: int = Field(ge=0, le=5)
    chinese_supply_advantage: int = Field(ge=0, le=5)
    regulatory_accessibility: int = Field(ge=0, le=5)
    sales_cycle_accessibility: int = Field(ge=0, le=5)
    repeat_revenue_potential: int = Field(ge=0, le=5)
    margin_potential: int = Field(ge=0, le=5)
    total_score: int = Field(ge=0, le=50)
    reasoning: dict[str, str]
    evidence_ids: list[str]
    gate_answers: list[GateAnswer]
    recommendation_status: RecommendationStatus
    scored_at: datetime


class TradeActivity(BaseModel):
    """One observed trader/product/month/role tuple from UK Trade Info."""

    model_config = ConfigDict(extra="forbid")

    activity_id: str
    source_record_id: str
    trader_id: int
    company_name: str
    address_lines: list[str] = Field(default_factory=list)
    postcode: str | None = None
    commodity_code: str
    commodity_code_level: str
    hs2_code: str | None = None
    hs4_code: str | None = None
    hs6_code: str | None = None
    product_description: str | None = None
    year: int
    month: int = Field(ge=1, le=12)
    trade_role: TradeRole
    value: Decimal | None = None
    quantity: Decimal | None = None
    partner_country: str | None = None
    source_url: str
    evidence_id: str
    raw_payload_reference: str
    payload_sha256: str


class RecurrentDemandAccount(BaseModel):
    model_config = ConfigDict(extra="forbid")

    company_id: str
    company_name: str
    country: str
    website: str | None = None
    company_type: str | None = None
    products: list[str] = Field(default_factory=list)
    relevant_hs_codes: list[str] = Field(default_factory=list)
    procurement_evidence: list[str] = Field(default_factory=list)
    trade_evidence: list[str] = Field(default_factory=list)
    import_evidence: list[str] = Field(default_factory=list)
    china_sourcing_evidence: list[str] = Field(default_factory=list)
    months_active: int
    estimated_scale: str
    buyer_role: str
    contact_status: str
    buying_signals: list[str] = Field(default_factory=list)
    switching_hypothesis: str | None = None
    evidence_strength: str
    confidence: float = Field(ge=0, le=1)
    next_action: str
    activity_year: int | None = None
    recurrent_product_roles: list[str] = Field(default_factory=list)


class CanonicalCompany(BaseModel):
    model_config = ConfigDict(extra="forbid")

    company_id: str
    preferred_name: str
    normalized_name: str
    country: str | None = None
    original_names: list[str] = Field(default_factory=list)
    identifiers: dict[str, str] = Field(default_factory=dict)
    addresses: list[str] = Field(default_factory=list)
    evidence_ids: list[str] = Field(default_factory=list)
    first_seen_at: datetime
    last_seen_at: datetime


class EntityLinkDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_id: str
    left_company_id: str
    right_company_id: str
    normalized_name: str
    status: str
    match_type: str
    confidence: float = Field(ge=0, le=1)
    reasoning: str
    evidence_ids: list[str] = Field(default_factory=list)
    decided_at: datetime
    is_current: bool = True


__all__ = [
    "DemandStatus",
    "CanonicalCompany",
    "EntityLinkDecision",
    "Evidence",
    "ExplicitDemand",
    "GateAnswer",
    "OpportunityScore",
    "RecurrentDemandAccount",
    "RecommendationStatus",
    "TradeActivity",
    "TradeRole",
]
