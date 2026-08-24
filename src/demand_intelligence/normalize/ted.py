"""Conservative TED notice normalization."""

import hashlib
import json
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from demand_intelligence.classification import match_product_cluster
from demand_intelligence.models import DemandStatus, Evidence, ExplicitDemand
from demand_intelligence.storage import SnapshotReference


def _first(value: Any) -> Any:
    if isinstance(value, list):
        return _first(value[0]) if value else None
    if isinstance(value, dict):
        for language in ("eng", "ENG"):
            if language in value:
                return _first(value[language])
        return _first(next(iter(value.values()))) if value else None
    return value


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item is not None]
    return [str(value)]


def _date(value: Any) -> date | None:
    raw = _first(value)
    if raw is None:
        return None
    text = str(raw)[:10]
    if len(text) == 8 and text.isdigit():
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _decimal(value: Any) -> Decimal | None:
    raw = _first(value)
    if raw is None:
        return None
    try:
        return Decimal(str(raw))
    except InvalidOperation:
        return None


def _source_url(raw: dict[str, Any], notice_id: str) -> str:
    links = raw.get("links")
    if isinstance(links, dict):
        html = links.get("html")
        found = _first(html)
        if found:
            return str(found)
    return f"https://ted.europa.eu/en/notice/-/detail/{notice_id}"


def _taxonomy_match(
    raw: dict[str, Any], cpv_codes: list[str], taxonomy_path: Path
) -> tuple[str | None, list[str], str | None]:
    haystack = " ".join(
        str(_first(raw.get(key)) or "") for key in ("title-proc", "description-proc")
    )
    match = match_product_cluster(
        cpv_codes=cpv_codes, text=haystack, taxonomy_path=taxonomy_path
    )
    if match is None:
        return None, [], None
    return match.label, match.candidate_hs_codes, match.mapping_status


def ted_payload_sha256(raw: dict[str, Any]) -> str:
    encoded = json.dumps(
        raw, ensure_ascii=False, sort_keys=True, default=str
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def normalize_ted_notice(
    raw: dict[str, Any], snapshot: SnapshotReference, *, taxonomy_path: Path
) -> tuple[ExplicitDemand, Evidence]:
    notice_id = str(
        _first(raw.get("publication-number")) or _first(raw.get("notice-identifier")) or ""
    )
    if not notice_id:
        digest = hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()[:20]
        notice_id = f"ted-unidentified-{digest}"
    buyer = _first(raw.get("buyer-name"))
    title = _first(raw.get("title-proc"))
    description = _first(raw.get("description-proc"))
    requirement = str(title) if title else (str(description) if description else None)
    cpv_codes = _strings(raw.get("main-classification-proc"))
    category, hs_codes, mapping_status = _taxonomy_match(raw, cpv_codes, taxonomy_path)
    source_url = _source_url(raw, notice_id)
    required = [
        not notice_id.startswith("ted-unidentified-"),
        bool(buyer),
        bool(requirement),
        bool(source_url),
    ]
    if all(required):
        status = DemandStatus.VERIFIED
        confidence = 0.9
        strength = "Tier 1 procurement record with buyer, requirement, ID, and source link"
    elif any(required):
        status = DemandStatus.PARTIAL
        confidence = 0.6
        strength = "Tier 1 procurement record with material fields missing"
    else:
        status = DemandStatus.UNVERIFIED
        confidence = 0.3
        strength = "Insufficient identifying procurement fields"

    evidence_id = "ev-ted-" + hashlib.sha256(
        f"{notice_id}|{ted_payload_sha256(raw)}".encode()
    ).hexdigest()[:24]
    limitations = ["Fields not present in the source response remain null."]
    if hs_codes:
        limitations.append(
            f"HS/CN codes are candidate taxonomy mappings ({mapping_status}); they require review."
        )
    observed = f"TED published notice {notice_id}"
    if buyer:
        observed += f" naming {buyer}"
    if requirement:
        observed += f" for {requirement}"
    evidence = Evidence(
        evidence_id=evidence_id,
        source_name="TED Search API",
        source_type="official procurement",
        source_tier=1,
        source_url=source_url,
        retrieved_at=snapshot.retrieved_at,
        data_period=str(_date(raw.get("publication-date")) or "Unknown"),
        raw_source_id=notice_id,
        raw_text_or_payload_reference=snapshot.path,
        observed_fact=observed + ".",
        inference=None,
        confidence=confidence,
        limitations=limitations,
    )
    demand = ExplicitDemand(
        notice_id=notice_id,
        buyer=str(buyer) if buyer else None,
        buyer_country=str(_first(raw.get("buyer-country"))) if raw.get("buyer-country") else None,
        buyer_city=str(_first(raw.get("buyer-city"))) if raw.get("buyer-city") else None,
        product_category=category,
        cpv_codes=cpv_codes,
        candidate_hs_codes=hs_codes,
        publication_date=_date(raw.get("publication-date")),
        deadline=_date(raw.get("deadline")),
        value=_decimal(raw.get("estimated-value-proc")),
        currency=(
            str(_first(raw.get("estimated-value-cur-proc")))
            if raw.get("estimated-value-cur-proc")
            else None
        ),
        requirement=requirement,
        procurement_type=(
            str(_first(raw.get("procedure-type"))) if raw.get("procedure-type") else None
        ),
        source="TED Search API",
        source_url=source_url,
        current_status=status,
        evidence_strength=strength,
        confidence=confidence,
        next_action="Verify tender documents, eligibility, quantities, and purchasing contact.",
        evidence_ids=[evidence_id],
    )
    return demand, evidence


__all__ = ["normalize_ted_notice", "ted_payload_sha256"]
