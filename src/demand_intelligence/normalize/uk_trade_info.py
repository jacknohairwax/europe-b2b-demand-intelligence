"""Normalize public UK Trade Info trader-month observations."""

import hashlib
import json
from typing import Any

from demand_intelligence.entity_resolution import normalize_legal_name, source_company_id
from demand_intelligence.models import CanonicalCompany, Evidence, TradeActivity, TradeRole
from demand_intelligence.storage import SnapshotReference

UKTI_DOCUMENTATION_URL = "https://www.uktradeinfo.com/api-documentation/"


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _meaningful_text(value: object) -> str | None:
    text = _text(value)
    return None if text in {None, "-"} else text


def _commodity_identity(
    commodity: dict[str, Any], commodity_id: int
) -> tuple[str, str]:
    cn8 = _meaningful_text(commodity.get("Cn8Code"))
    hs6 = _meaningful_text(commodity.get("Hs6Code"))
    hs4 = _meaningful_text(commodity.get("Hs4Code"))
    hs2 = _meaningful_text(commodity.get("Hs2Code"))
    if cn8 and len(cn8) == 8:
        return cn8, "CN8"
    if hs6 and len(hs6) == 6:
        return hs6, "HS6"
    if hs4 and len(hs4) == 4:
        return hs4, "HS4"
    if hs2 and len(hs2) == 2:
        return hs2, "HS2"
    if cn8:
        return cn8, "Published commodity code"
    return str(commodity_id), "Unresolved commodity level"


def normalize_uk_trade_info_record(
    raw: dict[str, Any], snapshot: SnapshotReference
) -> tuple[TradeActivity, Evidence, CanonicalCompany]:
    trader_id = int(raw["TraderId"])
    commodity_id = int(raw["CommodityId"])
    month_id = int(raw["MonthId"])
    trade_type_id = int(raw.get("TradeTypeId", 0))
    trader_value = raw.get("Trader")
    commodity_value = raw.get("Commodity")
    trader: dict[str, Any] = trader_value if isinstance(trader_value, dict) else {}
    commodity: dict[str, Any] = (
        commodity_value if isinstance(commodity_value, dict) else {}
    )
    company_name = _text(trader.get("CompanyName"))
    if company_name is None:
        raise ValueError("UK Trade Info record has no trader company name")
    commodity_code, commodity_code_level = _commodity_identity(
        commodity, commodity_id
    )
    source_record_id = f"{trader_id}:{commodity_id}:{month_id}:{trade_type_id}"
    activity_id = f"ukti:{source_record_id}"
    # The immutable page snapshot proves capture.  A row-level digest separately
    # provides stable idempotency when the same activity moves between OData pages.
    payload_sha256 = hashlib.sha256(
        json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str).encode()
    ).hexdigest()
    evidence_id = "ev-ukti-" + hashlib.sha256(
        f"{activity_id}|{payload_sha256}".encode()
    ).hexdigest()[:24]
    if trade_type_id == 1:
        role = TradeRole.IMPORTER
    elif trade_type_id == 2:
        role = TradeRole.EXPORTER
    else:
        role = TradeRole.UNRESOLVED

    address_lines = [
        value
        for key in ("Address1", "Address2", "Address3", "Address4", "Address5")
        if (value := _text(trader.get(key))) is not None
    ]
    company_id = source_company_id("ukti", str(trader_id))
    description = (
        _meaningful_text(commodity.get("Cn8LongDescription"))
        or _meaningful_text(commodity.get("Hs6Description"))
        or _meaningful_text(commodity.get("Hs4Description"))
        or _meaningful_text(commodity.get("Hs2Description"))
    )
    activity = TradeActivity(
        activity_id=activity_id,
        source_record_id=source_record_id,
        trader_id=trader_id,
        company_name=company_name,
        address_lines=address_lines,
        postcode=_text(trader.get("PostCode")),
        commodity_code=commodity_code,
        commodity_code_level=commodity_code_level,
        hs2_code=_text(commodity.get("Hs2Code")),
        hs4_code=_text(commodity.get("Hs4Code")),
        hs6_code=_text(commodity.get("Hs6Code")),
        product_description=description,
        year=month_id // 100,
        month=month_id % 100,
        trade_role=role,
        value=None,
        quantity=None,
        partner_country=None,
        source_url=snapshot.source_url,
        evidence_id=evidence_id,
        raw_payload_reference=f"{snapshot.path}#activity={activity_id}",
        payload_sha256=payload_sha256,
    )
    observed = (
        f"HMRC UK Trade Info records {company_name} with role {role.value} for "
        f"{commodity_code_level} {commodity_code} in "
        f"{activity.year}-{activity.month:02d}."
    )
    evidence = Evidence(
        evidence_id=evidence_id,
        source_name="UK Trade Info",
        source_type="government trader activity",
        source_tier=1,
        source_url=snapshot.source_url,
        retrieved_at=snapshot.retrieved_at,
        data_period=f"{activity.year}-{activity.month:02d}",
        raw_source_id=source_record_id,
        raw_text_or_payload_reference=activity.raw_payload_reference,
        observed_fact=observed,
        inference=None,
        confidence=0.95 if role is not TradeRole.UNRESOLVED else 0.6,
        limitations=[
            "Trader-level value, quantity, partner, origin, VAT and EORI are not public.",
            "Named-trader publication is incomplete because disclosure controls and "
            "opt-outs apply.",
            "This record does not evidence China sourcing or UK incorporation.",
        ],
    )
    company = CanonicalCompany(
        company_id=company_id,
        preferred_name=company_name,
        normalized_name=normalize_legal_name(company_name),
        # Publication in the UK named-trader feed does not itself prove UK
        # incorporation or legal domicile.
        country=None,
        original_names=[company_name],
        identifiers={"UKTI TraderId": str(trader_id)},
        addresses=[", ".join([*address_lines, activity.postcode or ""]).strip(", ")],
        evidence_ids=[evidence_id],
        first_seen_at=snapshot.retrieved_at,
        last_seen_at=snapshot.retrieved_at,
    )
    return activity, evidence, company
