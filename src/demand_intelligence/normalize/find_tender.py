"""Conservative normalization for Find a Tender OCDS releases."""

import hashlib
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, Field

from demand_intelligence.models import DemandStatus, Evidence, ExplicitDemand
from demand_intelligence.storage import SnapshotReference

SOURCE_NAME = "Find a Tender OCDS API"
NOTICE_URL_ROOT = "https://www.find-tender.service.gov.uk/Notice"


class SourceIdentifier(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scheme: str | None = None
    value: str | None = None
    legal_name: str | None = None
    uri: str | None = None


class ProcurementLocation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    street_address: str | None = None
    locality: str | None = None
    region: str | None = None
    postal_code: str | None = None
    country: str | None = None
    country_name: str | None = None


class SourceParty(BaseModel):
    model_config = ConfigDict(extra="forbid")

    party_id: str | None = None
    name: str | None = None
    roles: list[str] = Field(default_factory=list)
    identifiers: list[SourceIdentifier] = Field(default_factory=list)
    location: ProcurementLocation | None = None


class ProcurementSupplier(BaseModel):
    model_config = ConfigDict(extra="forbid")

    party_id: str | None = None
    name: str | None = None


class ObservedPeriod(BaseModel):
    """A source period; dates are not converted into an inferred duration."""

    model_config = ConfigDict(extra="forbid")

    source_context: str
    start_date: datetime | None = None
    end_date: datetime | None = None
    max_extent_date: datetime | None = None
    duration_in_days: int | None = None


class ProcurementAward(BaseModel):
    model_config = ConfigDict(extra="forbid")

    award_id: str | None = None
    title: str | None = None
    status: str | None = None
    award_date: datetime | None = None
    value: Decimal | None = None
    currency: str | None = None
    value_basis: str | None = None
    contract_status: str | None = None
    contract_value: Decimal | None = None
    contract_currency: str | None = None
    contract_value_basis: str | None = None
    contract_period: ObservedPeriod | None = None
    suppliers: list[ProcurementSupplier] = Field(default_factory=list)


class ProcurementRecord(BaseModel):
    """One observed OCDS release, including tender and award-stage facts."""

    model_config = ConfigDict(extra="forbid")

    notice_id: str
    ocid: str | None = None
    stages: list[str] = Field(default_factory=list)
    tender_status: str | None = None
    buyer: str | None = None
    buyer_id: str | None = None
    buyer_location: ProcurementLocation | None = None
    title: str | None = None
    description: str | None = None
    cpv_codes: list[str] = Field(default_factory=list)
    publication_date: datetime | None = None
    deadline: datetime | None = None
    procurement_type: str | None = None
    procurement_category: str | None = None
    is_framework: bool | None = None
    framework_stage: str = "none"
    framework_periods: list[ObservedPeriod] = Field(default_factory=list)
    value: Decimal | None = None
    currency: str | None = None
    value_basis: str | None = None
    award_statuses: list[str] = Field(default_factory=list)
    awarded_suppliers: list[ProcurementSupplier] = Field(default_factory=list)
    awards: list[ProcurementAward] = Field(default_factory=list)
    source: str = SOURCE_NAME
    source_url: str
    raw_source_id: str
    raw_snapshot_reference: str
    raw_payload_sha256: str
    source_parties: list[SourceParty] = Field(default_factory=list)
    evidence_ids: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)


def normalize_find_tender_release(
    raw: dict[str, Any], snapshot: SnapshotReference
) -> tuple[ProcurementRecord, Evidence]:
    """Normalize one release without collapsing award facts into active demand."""

    notice_id = _string(raw.get("id"))
    if not notice_id:
        digest = hashlib.sha256(_stable_raw_bytes(raw)).hexdigest()[:20]
        notice_id = f"find-tender-unidentified-{digest}"
    ocid = _string(raw.get("ocid"))
    stages = _strings(raw.get("tag"))
    tender = _mapping(raw.get("tender"))
    source_parties = [_normalize_party(party) for party in _mappings(raw.get("parties"))]
    buyer_reference = _mapping(raw.get("buyer"))
    buyer_id = _string(buyer_reference.get("id"))
    buyer_name = _string(buyer_reference.get("name"))
    buyer_party = _buyer_party(source_parties, buyer_id, buyer_name)
    if buyer_party is not None:
        buyer_id = buyer_id or buyer_party.party_id
        buyer_name = buyer_name or buyer_party.name

    contracts = _mappings(raw.get("contracts"))
    awards = [
        _normalize_award(award, contracts) for award in _mappings(raw.get("awards"))
    ]
    awarded_suppliers = _unique_suppliers(
        supplier for award in awards for supplier in award.suppliers
    )
    title = _string(tender.get("title")) or _first_award_title(awards)
    description = _string(tender.get("description")) or _string(raw.get("description"))
    publication_date = _publication_date(raw, notice_id)
    source_url, constructed_url = _source_url(raw, notice_id)
    value, currency, value_basis = _money(tender.get("value"))
    is_framework = _has_framework_agreement(tender)
    framework_stage = _framework_stage(raw, is_framework)
    limitations = ["Fields not present in the OCDS release remain null or empty."]
    if constructed_url:
        limitations.append(
            "The notice URL was constructed from the official notice ID because no notice "
            "document URL was present."
        )
    if value is None and any(award.value is not None for award in awards):
        limitations.append(
            "No release-level tender value was observed; award values remain at award level "
            "and were not summed."
        )

    payload_sha256 = hashlib.sha256(_stable_raw_bytes(raw)).hexdigest()
    evidence_id = "ev-find-tender-" + hashlib.sha256(
        f"{notice_id}|{ocid or ''}|{payload_sha256}".encode()
    ).hexdigest()[:24]
    identifying_fields = [
        not notice_id.startswith("find-tender-unidentified-"),
        bool(buyer_name),
        bool(title),
        bool(source_url),
    ]
    if all(identifying_fields):
        confidence = 0.95
    elif any(identifying_fields):
        confidence = 0.7
    else:  # pragma: no cover - a generated ID and canonical URL make this defensive
        confidence = 0.4

    observed = f"Find a Tender published release {notice_id}"
    if ocid:
        observed += f" for procurement {ocid}"
    if stages:
        observed += f" at stage(s) {', '.join(stages)}"
    if buyer_name:
        observed += f", naming {buyer_name} as buyer"
    if title:
        observed += f", titled {title}"
    if awards:
        observed += f", with {len(awards)} award record(s)"
    evidence = Evidence(
        evidence_id=evidence_id,
        source_name=SOURCE_NAME,
        source_type="official procurement",
        source_tier=1,
        source_url=source_url,
        retrieved_at=snapshot.retrieved_at,
        data_period=publication_date.date().isoformat() if publication_date else None,
        raw_source_id=notice_id,
        raw_text_or_payload_reference=snapshot.path,
        observed_fact=observed + ".",
        inference=None,
        confidence=confidence,
        limitations=limitations,
    )
    record = ProcurementRecord(
        notice_id=notice_id,
        ocid=ocid,
        stages=stages,
        tender_status=_string(tender.get("status")),
        buyer=buyer_name,
        buyer_id=buyer_id,
        buyer_location=buyer_party.location if buyer_party else None,
        title=title,
        description=description,
        cpv_codes=_cpv_codes(raw),
        publication_date=publication_date,
        deadline=_datetime(_mapping(tender.get("tenderPeriod")).get("endDate")),
        procurement_type=(
            _string(tender.get("procurementMethodDetails"))
            or _string(tender.get("procurementMethod"))
        ),
        procurement_category=_string(tender.get("mainProcurementCategory")),
        is_framework=is_framework,
        framework_stage=framework_stage,
        framework_periods=_observed_periods(raw),
        value=value,
        currency=currency,
        value_basis=value_basis,
        award_statuses=_unique_strings(
            award.status for award in awards if award.status is not None
        ),
        awarded_suppliers=awarded_suppliers,
        awards=awards,
        source_url=source_url,
        raw_source_id=notice_id,
        raw_snapshot_reference=snapshot.path,
        raw_payload_sha256=payload_sha256,
        source_parties=source_parties,
        evidence_ids=[evidence_id],
        limitations=limitations,
    )
    return record, evidence


def procurement_to_explicit_demand(
    record: ProcurementRecord, evidence: Evidence
) -> ExplicitDemand | None:
    """Convert only a still-active tender observation into explicit demand."""

    if not ({"tender", "tenderUpdate"} & set(record.stages)) or (
        record.tender_status != "active"
    ):
        return None
    if record.deadline is not None and _deadline_has_passed(
        record.deadline, evidence.retrieved_at
    ):
        return None
    required = [
        bool(record.buyer),
        bool(record.title),
        bool(record.source_url),
        record.deadline is not None,
    ]
    status = DemandStatus.VERIFIED if all(required) else DemandStatus.PARTIAL
    confidence = 0.95 if status == DemandStatus.VERIFIED else 0.7
    location = record.buyer_location
    return ExplicitDemand(
        notice_id=record.notice_id,
        buyer=record.buyer,
        buyer_country=(location.country_name or location.country) if location else None,
        buyer_city=location.locality if location else None,
        cpv_codes=record.cpv_codes,
        publication_date=record.publication_date.date() if record.publication_date else None,
        deadline=record.deadline.date() if record.deadline else None,
        value=record.value,
        currency=record.currency,
        requirement=record.title,
        procurement_type=record.procurement_type,
        source=SOURCE_NAME,
        source_url=record.source_url,
        current_status=status,
        evidence_strength="Tier 1 active Find a Tender release with immutable raw provenance",
        confidence=confidence,
        next_action="Verify the current notice, tender documents, eligibility, and buyer contact.",
        evidence_ids=[evidence.evidence_id],
    )


def _stable_raw_bytes(raw: dict[str, Any]) -> bytes:
    import json

    return json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str).encode()


def _deadline_has_passed(deadline: datetime, retrieved_at: datetime) -> bool:
    if deadline.tzinfo is None or retrieved_at.tzinfo is None:
        return deadline.replace(tzinfo=None) <= retrieved_at.replace(tzinfo=None)
    return deadline <= retrieved_at


def _mapping(value: Any) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [cast(dict[str, Any], item) for item in value if isinstance(item, dict)]


def _walk_mappings(value: Any) -> Iterator[dict[str, Any]]:
    if isinstance(value, dict):
        mapping = cast(dict[str, Any], value)
        yield mapping
        for child in mapping.values():
            yield from _walk_mappings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_mappings(child)


def _string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _strings(value: Any) -> list[str]:
    if isinstance(value, list):
        return _unique_strings(
            text for item in value if (text := _string(item)) is not None
        )
    text = _string(value)
    return [text] if text else []


def _unique_strings(values: Iterator[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    text = _string(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _integer(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _money(value: Any) -> tuple[Decimal | None, str | None, str | None]:
    money = _mapping(value)
    for basis in ("amount", "amountGross"):
        amount = _decimal(money.get(basis))
        if amount is not None:
            return amount, _string(money.get("currency")), basis
    return None, _string(money.get("currency")), None


def _location(value: Any) -> ProcurementLocation | None:
    address = _mapping(value)
    if not address:
        return None
    location = ProcurementLocation(
        street_address=_string(address.get("streetAddress")),
        locality=_string(address.get("locality")),
        region=_string(address.get("region")),
        postal_code=_string(address.get("postalCode")),
        country=_string(address.get("country")),
        country_name=_string(address.get("countryName")),
    )
    return location if any(location.model_dump().values()) else None


def _source_identifier(value: dict[str, Any]) -> SourceIdentifier:
    return SourceIdentifier(
        scheme=_string(value.get("scheme")),
        value=_string(value.get("id")),
        legal_name=_string(value.get("legalName")),
        uri=_string(value.get("uri")),
    )


def _normalize_party(raw: dict[str, Any]) -> SourceParty:
    identifier_values: list[dict[str, Any]] = []
    primary_identifier = _mapping(raw.get("identifier"))
    if primary_identifier:
        identifier_values.append(primary_identifier)
    identifier_values.extend(_mappings(raw.get("additionalIdentifiers")))
    return SourceParty(
        party_id=_string(raw.get("id")),
        name=_string(raw.get("name")),
        roles=_strings(raw.get("roles")),
        identifiers=[_source_identifier(item) for item in identifier_values],
        location=_location(raw.get("address")),
    )


def _buyer_party(
    parties: list[SourceParty], buyer_id: str | None, buyer_name: str | None
) -> SourceParty | None:
    if buyer_id:
        for party in parties:
            if party.party_id == buyer_id:
                return party
    if buyer_name:
        for party in parties:
            if party.name == buyer_name:
                return party
    return next((party for party in parties if "buyer" in party.roles), None)


def _normalize_supplier(raw: dict[str, Any]) -> ProcurementSupplier:
    return ProcurementSupplier(
        party_id=_string(raw.get("id")),
        name=_string(raw.get("name")),
    )


def _normalize_award(
    raw: dict[str, Any], contracts: list[dict[str, Any]]
) -> ProcurementAward:
    value, currency, value_basis = _money(raw.get("value"))
    award_id = _string(raw.get("id"))
    linked_contract = next(
        (
            contract
            for contract in contracts
            if award_id is not None and _string(contract.get("awardID")) == award_id
        ),
        {},
    )
    contract_value, contract_currency, contract_value_basis = _money(
        linked_contract.get("value")
    )
    return ProcurementAward(
        award_id=award_id,
        title=_string(raw.get("title")),
        status=_string(raw.get("status")),
        award_date=_datetime(raw.get("date")),
        value=value,
        currency=currency,
        value_basis=value_basis,
        contract_status=_string(linked_contract.get("status")),
        contract_value=contract_value,
        contract_currency=contract_currency,
        contract_value_basis=contract_value_basis,
        contract_period=_period(linked_contract.get("period"), "contract.period"),
        suppliers=[_normalize_supplier(item) for item in _mappings(raw.get("suppliers"))],
    )


def _has_framework_agreement(tender: dict[str, Any]) -> bool | None:
    observed: list[bool] = []
    technique_sources = [_mapping(tender.get("techniques"))]
    technique_sources.extend(
        _mapping(lot.get("techniques")) for lot in _mappings(tender.get("lots"))
    )
    for techniques in technique_sources:
        value = techniques.get("hasFrameworkAgreement")
        if isinstance(value, bool):
            observed.append(value)
    if any(observed):
        return True
    if observed:
        return False
    return None


def _framework_stage(raw: dict[str, Any], is_framework: bool | None) -> str:
    for process in _mappings(raw.get("relatedProcesses")):
        if "framework" in _strings(process.get("relationship")):
            return "call_off"
    if is_framework is True:
        return "establishment"
    return "none"


def _unique_suppliers(values: Iterator[ProcurementSupplier]) -> list[ProcurementSupplier]:
    found: dict[tuple[str | None, str | None], ProcurementSupplier] = {}
    for supplier in values:
        found.setdefault((supplier.party_id, supplier.name), supplier)
    return list(found.values())


def _first_award_title(awards: list[ProcurementAward]) -> str | None:
    return next((award.title for award in awards if award.title), None)


def _cpv_codes(raw: dict[str, Any]) -> list[str]:
    codes: list[str] = []
    for mapping in _walk_mappings(raw):
        if _string(mapping.get("scheme")) == "CPV":
            code = _string(mapping.get("id"))
            if code:
                codes.append(code)
    return list(dict.fromkeys(codes))


def _publication_date(raw: dict[str, Any], notice_id: str) -> datetime | None:
    for mapping in _walk_mappings(raw):
        if _string(mapping.get("id")) == notice_id:
            published = _datetime(mapping.get("datePublished"))
            if published:
                return published
    return _datetime(raw.get("date"))


def _source_url(raw: dict[str, Any], notice_id: str) -> tuple[str, bool]:
    for mapping in _walk_mappings(raw):
        url = _string(mapping.get("url"))
        if url and "/Notice/" in url and notice_id in url:
            return url, False
    return f"{NOTICE_URL_ROOT}/{notice_id}", True


def _period(value: Any, source_context: str) -> ObservedPeriod | None:
    period = _mapping(value)
    if not period:
        return None
    observed = ObservedPeriod(
        source_context=source_context,
        start_date=_datetime(period.get("startDate")),
        end_date=_datetime(period.get("endDate")),
        max_extent_date=_datetime(period.get("maxExtentDate")),
        duration_in_days=_integer(period.get("durationInDays")),
    )
    observed_values = observed.model_dump(exclude={"source_context"})
    return observed if any(value is not None for value in observed_values.values()) else None


def _observed_periods(raw: dict[str, Any]) -> list[ObservedPeriod]:
    tender = _mapping(raw.get("tender"))
    candidates: list[tuple[Any, str]] = [(tender.get("contractPeriod"), "tender.contractPeriod")]
    candidates.extend(
        (lot.get("contractPeriod"), f"tender.lots[{index}].contractPeriod")
        for index, lot in enumerate(_mappings(tender.get("lots")))
    )
    candidates.extend(
        (award.get("contractPeriod"), f"awards[{index}].contractPeriod")
        for index, award in enumerate(_mappings(raw.get("awards")))
    )
    candidates.extend(
        (contract.get("period"), f"contracts[{index}].period")
        for index, contract in enumerate(_mappings(raw.get("contracts")))
    )
    periods: list[ObservedPeriod] = []
    for value, source_context in candidates:
        observed = _period(value, source_context)
        if observed is not None:
            periods.append(observed)
    return periods


__all__ = [
    "ObservedPeriod",
    "ProcurementAward",
    "ProcurementLocation",
    "ProcurementRecord",
    "ProcurementSupplier",
    "SourceIdentifier",
    "SourceParty",
    "normalize_find_tender_release",
    "procurement_to_explicit_demand",
]
