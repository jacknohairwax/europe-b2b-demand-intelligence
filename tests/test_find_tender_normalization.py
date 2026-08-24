import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from demand_intelligence.connectors.find_tender import FindTenderReleasePackage
from demand_intelligence.models import DemandStatus
from demand_intelligence.normalize.find_tender import (
    normalize_find_tender_release,
    procurement_to_explicit_demand,
)
from demand_intelligence.storage import SnapshotReference

ROOT = Path(__file__).parents[1]


def _fixture_releases() -> list[dict[str, object]]:
    payload = json.loads(
        (ROOT / "tests/fixtures/find_tender_release_package.json").read_text()
    )
    package = FindTenderReleasePackage.model_validate(payload)
    return package.releases


def _snapshot(retrieved_at: datetime) -> SnapshotReference:
    return SnapshotReference(
        path="data/raw/find-tender/fixture.json",
        content_sha256="c" * 64,
        payload_sha256="d" * 64,
        retrieved_at=retrieved_at,
        source_url=(
            "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages"
        ),
    )


def test_normalizes_active_tender_with_observed_framework_and_party_fields() -> None:
    raw = _fixture_releases()[0]
    record, evidence = normalize_find_tender_release(
        raw,  # type: ignore[arg-type]
        _snapshot(datetime(2025, 8, 14, tzinfo=UTC)),
    )

    assert record.notice_id == "048243-2025"
    assert record.ocid == "ocds-h6vhtk-050918"
    assert record.stages == ["tender"]
    assert record.tender_status == "active"
    assert record.buyer == "Leicestershire County Council, trading as ESPO"
    assert record.buyer_location is not None
    assert record.buyer_location.locality == "Leicester"
    assert record.buyer_location.country == "GB"
    assert record.title == "52_26 Janitorial, Hygiene and Cleaning Equipment"
    assert record.cpv_codes == ["39224100", "39224200"]
    assert record.publication_date == datetime.fromisoformat("2025-08-13T09:21:47+01:00")
    assert record.deadline == datetime.fromisoformat("2025-09-15T12:00:00+01:00")
    assert record.procurement_type == "Open procedure"
    assert record.procurement_category == "goods"
    assert record.is_framework is True
    assert record.framework_stage == "establishment"
    assert record.value == Decimal("2600000")
    assert record.currency == "GBP"
    assert record.value_basis == "amountGross"
    assert len(record.framework_periods) == 2
    assert {item.source_context for item in record.framework_periods} == {
        "tender.lots[0].contractPeriod",
        "tender.lots[1].contractPeriod",
    }
    assert record.framework_periods[0].max_extent_date == datetime.fromisoformat(
        "2030-02-28T23:59:59+00:00"
    )
    assert record.raw_snapshot_reference == "data/raw/find-tender/fixture.json"
    expected_payload_hash = hashlib.sha256(
        json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str).encode()
    ).hexdigest()
    assert record.raw_payload_sha256 == expected_payload_hash
    assert record.source_parties[0].roles == ["buyer"]
    assert [(item.scheme, item.value) for item in record.source_parties[0].identifiers] == [
        ("GB-PPON", "PYDD-1122-TCVN"),
        ("GB-COH", "00123456"),
    ]
    assert evidence.inference is None
    assert evidence.raw_text_or_payload_reference == record.raw_snapshot_reference
    assert evidence.evidence_id in record.evidence_ids

    demand = procurement_to_explicit_demand(record, evidence)
    assert demand is not None
    assert demand.current_status == DemandStatus.VERIFIED
    assert demand.deadline is not None and demand.deadline.isoformat() == "2025-09-15"
    assert demand.evidence_ids == [evidence.evidence_id]


def test_awards_remain_procurement_evidence_and_retain_supplier_identifiers() -> None:
    raw = _fixture_releases()[1]
    record, evidence = normalize_find_tender_release(
        raw,  # type: ignore[arg-type]
        _snapshot(datetime(2026, 1, 7, tzinfo=UTC)),
    )

    assert record.stages == ["award", "contract"]
    assert record.tender_status == "complete"
    assert record.value is None
    assert record.award_statuses == ["pending"]
    assert len(record.awards) == 2
    assert record.awards[0].value == Decimal("288000")
    assert record.awards[0].value_basis == "amountGross"
    assert record.awards[1].value == Decimal("528000")
    assert [supplier.party_id for supplier in record.awarded_suppliers] == [
        "GB-COH-06355141",
        "GB-COH-01099088",
    ]
    supplier_party = next(
        party for party in record.source_parties if party.party_id == "GB-COH-01099088"
    )
    assert supplier_party.roles == ["supplier"]
    assert supplier_party.identifiers[0].scheme == "GB-COH"
    assert supplier_party.identifiers[0].value == "01099088"
    assert "were not summed" in " ".join(record.limitations)
    assert "2 award record(s)" in evidence.observed_fact
    assert evidence.inference is None
    assert procurement_to_explicit_demand(record, evidence) is None


def test_missing_fields_stay_null_and_no_award_is_promoted_to_demand() -> None:
    record, evidence = normalize_find_tender_release(
        {"id": "099999-2026", "ocid": "ocds-h6vhtk-ffffff", "tag": ["award"]},
        _snapshot(datetime(2026, 8, 23, tzinfo=UTC)),
    )

    assert record.buyer is None
    assert record.buyer_location is None
    assert record.title is None
    assert record.deadline is None
    assert record.value is None
    assert record.is_framework is None
    assert record.source_url.endswith("/Notice/099999-2026")
    assert "constructed" in " ".join(evidence.limitations)
    assert evidence.inference is None
    assert procurement_to_explicit_demand(record, evidence) is None


def test_active_tender_with_unknown_deadline_is_only_partially_verified() -> None:
    raw = {
        "id": "099998-2026",
        "tag": ["tender"],
        "buyer": {"name": "Example Buyer"},
        "tender": {"status": "active", "title": "Example hand tools"},
    }
    record, evidence = normalize_find_tender_release(
        raw, _snapshot(datetime(2026, 8, 23, tzinfo=UTC))
    )

    demand = procurement_to_explicit_demand(record, evidence)

    assert demand is not None
    assert demand.deadline is None
    assert demand.current_status == DemandStatus.PARTIAL


def test_expired_tender_release_is_not_converted_even_if_source_status_says_active() -> None:
    raw = _fixture_releases()[0]
    record, evidence = normalize_find_tender_release(
        raw,  # type: ignore[arg-type]
        _snapshot(datetime(2026, 1, 1, tzinfo=UTC)),
    )

    assert record.tender_status == "active"
    assert procurement_to_explicit_demand(record, evidence) is None


def test_tender_that_expired_earlier_same_day_is_not_converted() -> None:
    raw = _fixture_releases()[0]
    record, evidence = normalize_find_tender_release(
        raw,  # type: ignore[arg-type]
        _snapshot(datetime(2025, 9, 15, 16, 0, tzinfo=UTC)),
    )

    assert record.deadline is not None
    assert procurement_to_explicit_demand(record, evidence) is None


def test_tender_update_and_framework_call_off_are_classified_conservatively() -> None:
    raw = {
        "id": "012346-2026",
        "ocid": "ocds-h6vhtk-call-off",
        "tag": ["tenderUpdate"],
        "date": "2026-08-20T10:00:00+01:00",
        "buyer": {"id": "buyer-1", "name": "Example Buyer"},
        "parties": [
            {
                "id": "buyer-1",
                "name": "Example Buyer",
                "roles": ["buyer"],
                "address": {"countryName": "United Kingdom"},
            }
        ],
        "relatedProcesses": [
            {"relationship": ["framework"], "identifier": "parent-framework"}
        ],
        "tender": {
            "status": "active",
            "title": "Hand tools call-off",
            "techniques": {"hasFrameworkAgreement": True},
            "tenderPeriod": {"endDate": "2026-09-20T12:00:00+01:00"},
        },
    }

    record, evidence = normalize_find_tender_release(
        raw, _snapshot(datetime(2026, 8, 23, tzinfo=UTC))
    )

    assert record.framework_stage == "call_off"
    assert procurement_to_explicit_demand(record, evidence) is not None


def test_evidence_identity_uses_payload_hash_not_retrieval_envelope_hash() -> None:
    raw = _fixture_releases()[0]
    first_snapshot = _snapshot(datetime(2025, 8, 14, tzinfo=UTC))
    second_snapshot = SnapshotReference(
        path="data/raw/find-tender/same-payload-later.json",
        content_sha256="e" * 64,
        payload_sha256=first_snapshot.payload_sha256,
        retrieved_at=datetime(2025, 8, 15, tzinfo=UTC),
        source_url=first_snapshot.source_url,
    )

    first_record, first_evidence = normalize_find_tender_release(
        raw,  # type: ignore[arg-type]
        first_snapshot,
    )
    second_record, second_evidence = normalize_find_tender_release(
        raw,  # type: ignore[arg-type]
        second_snapshot,
    )

    assert first_snapshot.content_sha256 != second_snapshot.content_sha256
    assert first_evidence.evidence_id == second_evidence.evidence_id
    assert first_record.raw_payload_sha256 == second_record.raw_payload_sha256
    assert first_record.raw_payload_sha256 != "d" * 64


def test_award_and_contract_values_remain_separate() -> None:
    raw = {
        "id": "012345-2026",
        "ocid": "ocds-h6vhtk-contract-value",
        "tag": ["award", "contract"],
        "awards": [
            {
                "id": "award-1",
                "status": "active",
                "suppliers": [{"id": "supplier-1", "name": "Example Supplier Ltd"}],
            }
        ],
        "contracts": [
            {
                "id": "contract-1",
                "awardID": "award-1",
                "status": "active",
                "value": {"amount": 125000, "currency": "GBP"},
                "period": {"startDate": "2026-09-01", "endDate": "2028-08-31"},
            }
        ],
    }

    record, _ = normalize_find_tender_release(
        raw, _snapshot(datetime(2026, 8, 23, tzinfo=UTC))
    )

    assert record.awards[0].value is None
    assert record.awards[0].contract_value == Decimal("125000")
    assert record.awards[0].contract_value_basis == "amount"
    assert record.awards[0].contract_status == "active"
    assert record.awards[0].contract_period is not None


def test_equal_periods_keep_distinct_source_contexts() -> None:
    period = {"startDate": "2026-09-01", "endDate": "2027-08-31"}
    raw = {
        "id": "012347-2026",
        "tag": ["tender"],
        "tender": {
            "contractPeriod": period,
            "lots": [{"id": "lot-1", "contractPeriod": period}],
        },
    }

    record, _ = normalize_find_tender_release(
        raw, _snapshot(datetime(2026, 8, 23, tzinfo=UTC))
    )

    assert [item.source_context for item in record.framework_periods] == [
        "tender.contractPeriod",
        "tender.lots[0].contractPeriod",
    ]
