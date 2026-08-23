import json
from datetime import UTC, datetime
from pathlib import Path

from demand_intelligence.connectors import TedSearchPage
from demand_intelligence.models import DemandStatus, RecommendationStatus
from demand_intelligence.normalize import normalize_ted_notice
from demand_intelligence.scoring import build_initial_opportunity
from demand_intelligence.storage import SnapshotReference

ROOT = Path(__file__).parents[1]


def test_normalizes_ted_notice_without_promoting_unsupported_inferences() -> None:
    payload = json.loads((ROOT / "tests/fixtures/ted_search_page.json").read_text())
    raw = TedSearchPage.model_validate(payload).notices[0]
    snapshot = SnapshotReference(
        path="data/raw/ted/fixture.json",
        content_sha256="a" * 64,
        retrieved_at=datetime(2026, 8, 23, tzinfo=UTC),
        source_url="https://api.ted.europa.eu/v3/notices/search",
    )

    demand, evidence = normalize_ted_notice(
        raw, snapshot, taxonomy_path=ROOT / "config/product_clusters.json"
    )
    opportunity = build_initial_opportunity(demand)

    assert demand.notice_id == "123456-2026"
    assert demand.buyer == "Example Municipal Authority"
    assert demand.current_status == DemandStatus.VERIFIED
    assert demand.product_category == "Non-powered hand tools"
    assert demand.candidate_hs_codes == ["8203", "8204", "8205", "8206"]
    assert evidence.inference is None
    assert "candidate taxonomy mappings" in " ".join(evidence.limitations)
    assert opportunity.recommendation_status == RecommendationStatus.NEEDS_VALIDATION
    assert sum(answer.is_evidence_backed for answer in opportunity.gate_answers) == 4


def test_missing_values_remain_null() -> None:
    raw = {
        "publication-number": "654321-2026",
        "publication-date": "20260821",
        "buyer-name": {"fra": ["Acheteur Exemple"]},
    }
    snapshot = SnapshotReference(
        path="fixture.json",
        content_sha256="b" * 64,
        retrieved_at=datetime(2026, 8, 23, tzinfo=UTC),
        source_url="https://api.ted.europa.eu/v3/notices/search",
    )

    demand, _ = normalize_ted_notice(
        raw, snapshot, taxonomy_path=ROOT / "config/product_clusters.json"
    )

    assert demand.deadline is None
    assert demand.value is None
    assert demand.requirement is None
    assert demand.current_status == DemandStatus.PARTIAL

