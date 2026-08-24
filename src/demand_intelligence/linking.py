"""Conservative, auditable links between recurrent traders and procurement parties."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast

from demand_intelligence.entity_resolution import normalize_legal_name, source_company_id
from demand_intelligence.models import EntityLinkDecision
from demand_intelligence.storage import Database

FIND_TENDER_SOURCE = "Find a Tender OCDS API"
_STRONG_IDENTIFIER_SCHEMES = ("GB-COH", "GB-PPON", "GB-CHC", "GB-NHS")
_POSTCODE_RE = re.compile(
    r"\b(?:GIR\s?0AA|[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2})\b",
    flags=re.IGNORECASE,
)
_POSTCODE_COMPACT_RE = re.compile(
    r"^(?:GIR0AA|[A-Z]{1,2}\d[A-Z\d]?\d[A-Z]{2})$",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True)
class _PartyObservation:
    source_name: str
    source_record_id: str
    original_name: str
    roles: frozenset[str]
    evidence_id: str | None


@dataclass
class _ProcurementCandidate:
    company_id: str
    preferred_name: str
    normalized_names: set[str] = field(default_factory=set)
    original_names: set[str] = field(default_factory=set)
    identifiers: dict[str, set[str]] = field(default_factory=dict)
    addresses: set[str] = field(default_factory=set)
    postcodes: set[str] = field(default_factory=set)
    evidence_ids: set[str] = field(default_factory=set)
    roles: set[str] = field(default_factory=set)
    role_evidence: dict[str, set[str]] = field(default_factory=dict)
    countries: set[str] = field(default_factory=set)
    observations: list[_PartyObservation] = field(default_factory=list)


@dataclass(frozen=True)
class _AccountIdentity:
    company_id: str
    preferred_name: str
    normalized_name: str
    identifiers: dict[str, set[str]]
    addresses: frozenset[str]
    postcodes: frozenset[str]
    evidence_ids: frozenset[str]


def link_recurrent_accounts_to_procurement(
    database: Database, *, decided_at: datetime | None = None
) -> list[EntityLinkDecision]:
    """Link UKTI recurrent accounts to FTS parties without coalescing identities.

    Exact normalized legal names create candidates. A candidate is accepted only when it
    is unique for that name and is corroborated by a matching strong typed identifier,
    exact normalized postcode, or exact normalized address. Ambiguity, identifier
    conflicts, location conflicts, and missing corroboration remain explicit non-links.
    """

    database.connection.execute("BEGIN TRANSACTION")
    try:
        decisions = _recompute_links(
            database, decided_at=decided_at or datetime.now(UTC)
        )
    except Exception:
        database.connection.execute("ROLLBACK")
        raise
    database.connection.execute("COMMIT")
    return decisions


def _recompute_links(
    database: Database, *, decided_at: datetime
) -> list[EntityLinkDecision]:
    _supersede_previous_links(database)
    _reset_find_tender_enrichment(database)
    accounts = _load_recurrent_accounts(database)
    recurrent_names = {account.normalized_name for account in accounts}
    candidates = {
        company_id: candidate
        for company_id, candidate in _load_procurement_candidates(database).items()
        if candidate.normalized_names & recurrent_names
    }
    for candidate in candidates.values():
        _persist_procurement_identity(database, candidate, observed_at=decided_at)

    by_name: dict[str, list[_ProcurementCandidate]] = {}
    for candidate in candidates.values():
        for normalized_name in candidate.normalized_names:
            by_name.setdefault(normalized_name, []).append(candidate)

    decisions: list[EntityLinkDecision] = []
    for account in accounts:
        matches = sorted(
            by_name.get(account.normalized_name, []), key=lambda item: item.company_id
        )
        if not matches:
            continue
        if len(matches) > 1:
            for candidate in matches:
                decision = _make_decision(
                    account,
                    candidate,
                    status="ambiguous",
                    match_type="exact_name_ambiguous",
                    confidence=0.0,
                    reasoning=(
                        f"Exact normalized legal name matched {len(matches)} distinct "
                        "Find a Tender source identities; no automatic link was accepted."
                    ),
                    decided_at=decided_at,
                )
                _persist_decision(database, decision)
                decisions.append(decision)
            continue

        candidate = matches[0]
        status, match_type, confidence, reasoning = _evaluate_unique_candidate(
            account, candidate
        )
        decision = _make_decision(
            account,
            candidate,
            status=status,
            match_type=match_type,
            confidence=confidence,
            reasoning=reasoning,
            decided_at=decided_at,
        )
        _persist_decision(database, decision)
        decisions.append(decision)
        if status == "accepted":
            _enrich_recurrent_account(database, account.company_id, candidate)
    return decisions


def _load_procurement_candidates(
    database: Database,
) -> dict[str, _ProcurementCandidate]:
    rows = database.connection.execute(
        """SELECT source_name, release_id, release_payload_sha256, party_index,
                  party_id, name, roles, identifiers, location, evidence_id
           FROM procurement_parties
           WHERE lower(source_name) = lower(?)
           ORDER BY release_id, release_payload_sha256, party_index""",
        [FIND_TENDER_SOURCE],
    ).fetchall()
    candidates: dict[str, _ProcurementCandidate] = {}
    for row in rows:
        name = _text(row[5])
        if name is None:
            continue
        source_name = _text(row[0]) or FIND_TENDER_SOURCE
        release_id = str(row[1])
        payload_sha256 = str(row[2])
        party_index = int(row[3])
        party_id = _text(row[4])
        roles = {_normalize_role(value) for value in _json_strings(row[6])}
        roles.discard("")
        identifiers = _party_identifiers(row[7], party_id)
        location = _json_mapping(row[8])
        source_key = _procurement_source_key(
            release_id=release_id,
            party_index=party_index,
            party_id=party_id,
            identifiers=identifiers,
        )
        company_id = source_company_id("find-tender", source_key)
        candidate = candidates.setdefault(
            company_id,
            _ProcurementCandidate(company_id=company_id, preferred_name=name),
        )
        candidate.original_names.add(name)
        candidate.normalized_names.add(normalize_legal_name(name))
        _merge_identifier_maps(candidate.identifiers, identifiers)
        address = _location_address(location)
        if address:
            candidate.addresses.add(address)
        postcode = _normalize_postcode(_mapping_text(location, "postal_code", "postalCode"))
        if postcode:
            candidate.postcodes.add(postcode)
        if address:
            candidate.postcodes.update(_extract_postcodes(address))
        country = _mapping_text(location, "country_name", "countryName", "country")
        if country:
            candidate.countries.add(country)
        evidence_id = _text(row[9])
        if evidence_id:
            candidate.evidence_ids.add(evidence_id)
        candidate.roles.update(roles)
        for role in roles:
            if evidence_id:
                candidate.role_evidence.setdefault(role, set()).add(evidence_id)
        source_record_id = f"{release_id}:{payload_sha256}:{party_index}"
        candidate.observations.append(
            _PartyObservation(
                source_name=source_name,
                source_record_id=source_record_id,
                original_name=name,
                roles=frozenset(roles),
                evidence_id=evidence_id,
            )
        )
    return candidates


def _load_recurrent_accounts(database: Database) -> list[_AccountIdentity]:
    rows = database.connection.execute(
        """SELECT r.company_id, c.preferred_name, c.normalized_name,
                  c.identifiers, c.addresses, c.evidence_ids, r.trade_evidence
           FROM recurrent_accounts AS r
           JOIN canonical_companies AS c ON c.company_id = r.company_id
           ORDER BY r.company_id"""
    ).fetchall()
    accounts: list[_AccountIdentity] = []
    for row in rows:
        addresses = frozenset(_json_strings(row[4]))
        normalized_name = _text(row[2]) or normalize_legal_name(str(row[1]))
        evidence_ids = set(_json_strings(row[5])) | set(_json_strings(row[6]))
        accounts.append(
            _AccountIdentity(
                company_id=str(row[0]),
                preferred_name=str(row[1]),
                normalized_name=normalized_name,
                identifiers=_canonical_identifiers(row[3]),
                addresses=addresses,
                postcodes=frozenset(
                    postcode
                    for address in addresses
                    for postcode in _extract_postcodes(address)
                ),
                evidence_ids=frozenset(evidence_ids),
            )
        )
    return accounts


def _evaluate_unique_candidate(
    account: _AccountIdentity, candidate: _ProcurementCandidate
) -> tuple[str, str, float, str]:
    matching_ids, conflicting_ids = _compare_strong_identifiers(
        account.identifiers, candidate.identifiers
    )
    if conflicting_ids:
        schemes = ", ".join(sorted(conflicting_ids))
        return (
            "rejected",
            "strong_identifier_mismatch",
            0.0,
            f"Exact legal name matched, but strong identifier values conflict for {schemes}.",
        )
    if matching_ids:
        schemes = ", ".join(sorted(matching_ids))
        return (
            "accepted",
            "exact_name_and_strong_identifier",
            1.0,
            (
                "Unique exact normalized legal-name candidate with matching strong typed "
                f"identifier evidence for {schemes}. Source identities remain separate."
            ),
        )

    if account.postcodes and candidate.postcodes:
        shared_postcodes = account.postcodes & candidate.postcodes
        if shared_postcodes:
            return (
                "accepted",
                "exact_name_and_postcode",
                0.98,
                (
                    "Unique exact normalized legal-name candidate with exact normalized "
                    f"postcode {sorted(shared_postcodes)[0]}. Source identities remain separate."
                ),
            )
        return (
            "rejected",
            "exact_name_postcode_mismatch",
            0.0,
            "Exact legal name matched, but the observed normalized postcodes conflict.",
        )

    left_addresses = {
        normalized
        for value in account.addresses
        if (normalized := _normalize_address(value)) is not None
    }
    right_addresses = {
        normalized
        for value in candidate.addresses
        if (normalized := _normalize_address(value)) is not None
    }
    if left_addresses & right_addresses:
        return (
            "accepted",
            "exact_name_and_address",
            0.95,
            (
                "Unique exact normalized legal-name candidate with exact normalized address "
                "evidence. Source identities remain separate."
            ),
        )
    if left_addresses and right_addresses:
        return (
            "rejected",
            "exact_name_address_mismatch",
            0.0,
            "Exact legal name matched, but the observed normalized addresses conflict.",
        )
    return (
        "unresolved",
        "exact_name_insufficient_location_evidence",
        0.3,
        (
            "Exact legal name matched uniquely, but neither compatible strong identifiers "
            "nor exact postcode/address evidence was available."
        ),
    )


def _make_decision(
    account: _AccountIdentity,
    candidate: _ProcurementCandidate,
    *,
    status: str,
    match_type: str,
    confidence: float,
    reasoning: str,
    decided_at: datetime,
) -> EntityLinkDecision:
    evidence_ids = sorted(account.evidence_ids | candidate.evidence_ids)
    identity_material = "|".join(
        [
            account.company_id,
            candidate.company_id,
            account.normalized_name,
            status,
            match_type,
            *evidence_ids,
        ]
    )
    decision_id = "link-" + hashlib.sha256(identity_material.encode()).hexdigest()[:24]
    return EntityLinkDecision(
        decision_id=decision_id,
        left_company_id=account.company_id,
        right_company_id=candidate.company_id,
        normalized_name=account.normalized_name,
        status=status,
        match_type=match_type,
        confidence=confidence,
        reasoning=reasoning,
        evidence_ids=evidence_ids,
        decided_at=decided_at,
    )


def _persist_procurement_identity(
    database: Database, candidate: _ProcurementCandidate, *, observed_at: datetime
) -> None:
    existing = database.connection.execute(
        """SELECT preferred_name, country, original_names, identifiers, addresses,
                  evidence_ids, CAST(first_seen_at AS VARCHAR),
                  CAST(last_seen_at AS VARCHAR)
           FROM canonical_companies WHERE company_id = ?""",
        [candidate.company_id],
    ).fetchone()
    identifiers = _flatten_identifiers(candidate.identifiers)
    if existing:
        preferred_name = str(existing[0])
        country = _text(existing[1]) or _preferred_country(candidate.countries)
        original_names = set(_json_strings(existing[2])) | candidate.original_names
        identifiers = _merge_flat_identifiers(_json_mapping(existing[3]), identifiers)
        addresses = set(_json_strings(existing[4])) | candidate.addresses
        evidence_ids = set(_json_strings(existing[5])) | candidate.evidence_ids
        first_seen_at = min(datetime.fromisoformat(str(existing[6])), observed_at)
        last_seen_at = max(datetime.fromisoformat(str(existing[7])), observed_at)
    else:
        preferred_name = candidate.preferred_name
        country = _preferred_country(candidate.countries)
        original_names = candidate.original_names
        addresses = candidate.addresses
        evidence_ids = candidate.evidence_ids
        first_seen_at = observed_at
        last_seen_at = observed_at
    normalized_name = normalize_legal_name(preferred_name)
    database.connection.execute(
        """INSERT OR REPLACE INTO canonical_companies VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )""",
        [
            candidate.company_id,
            preferred_name,
            normalized_name,
            country,
            _json(sorted(original_names)),
            _json(identifiers),
            _json(sorted(addresses)),
            _json(sorted(evidence_ids)),
            first_seen_at,
            last_seen_at,
        ],
    )
    for observation in candidate.observations:
        for role in sorted(observation.roles):
            database.connection.execute(
                """INSERT OR REPLACE INTO company_source_roles
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [
                    candidate.company_id,
                    observation.source_name,
                    observation.source_record_id,
                    observation.original_name,
                    role,
                    observation.evidence_id,
                ],
            )


def _persist_decision(database: Database, decision: EntityLinkDecision) -> None:
    database.connection.execute(
        """INSERT OR REPLACE INTO entity_link_decisions VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )""",
        [
            decision.decision_id,
            decision.left_company_id,
            decision.right_company_id,
            decision.normalized_name,
            decision.status,
            decision.match_type,
            decision.confidence,
            decision.reasoning,
            _json(decision.evidence_ids),
            decision.decided_at,
            decision.is_current,
        ],
    )


def _supersede_previous_links(database: Database) -> None:
    database.connection.execute(
        "UPDATE entity_link_decisions SET is_current = FALSE WHERE is_current"
    )


def _reset_find_tender_enrichment(database: Database) -> None:
    procurement_party_evidence = {
        str(row[0])
        for row in database.connection.execute(
            """SELECT DISTINCT evidence_id FROM procurement_parties
               WHERE lower(source_name) = lower(?) AND evidence_id IS NOT NULL""",
            [FIND_TENDER_SOURCE],
        ).fetchall()
    }
    rows = database.connection.execute(
        """SELECT company_id, procurement_evidence, buyer_role
           FROM recurrent_accounts"""
    ).fetchall()
    for company_id, raw_evidence, raw_role in rows:
        evidence_ids = _json_strings(raw_evidence)
        retained_evidence: list[str] = []
        if evidence_ids:
            placeholders = ", ".join("?" for _ in evidence_ids)
            source_rows = database.connection.execute(
                f"""SELECT evidence_id, source_name FROM evidence
                    WHERE evidence_id IN ({placeholders})""",
                evidence_ids,
            ).fetchall()
            sources = {str(row[0]): str(row[1]) for row in source_rows}
            retained_evidence = sorted(
                evidence_id
                for evidence_id in evidence_ids
                if evidence_id not in procurement_party_evidence
                and sources.get(evidence_id) != FIND_TENDER_SOURCE
            )
        buyer_role = _without_find_tender_role(_text(raw_role))
        database.connection.execute(
            """UPDATE recurrent_accounts
               SET procurement_evidence = ?, buyer_role = ?
               WHERE company_id = ?""",
            [_json(retained_evidence), buyer_role, str(company_id)],
        )


def _without_find_tender_role(value: str | None) -> str:
    if not value:
        return "Not verified"
    retained = [
        part.strip()
        for part in value.split(";")
        if part.strip() and not part.strip().startswith("Find a Tender ")
    ]
    return "; ".join(retained) if retained else "Not verified"


def _enrich_recurrent_account(
    database: Database, company_id: str, candidate: _ProcurementCandidate
) -> None:
    row = database.connection.execute(
        """SELECT procurement_evidence, buyer_role
           FROM recurrent_accounts WHERE company_id = ?""",
        [company_id],
    ).fetchone()
    if row is None:  # pragma: no cover - caller loads the account from this table
        return
    procurement_evidence = set(_json_strings(row[0])) | candidate.evidence_ids
    role_label = _find_tender_role_label(candidate.roles)
    buyer_role = _merge_buyer_role(_text(row[1]), role_label)
    database.connection.execute(
        """UPDATE recurrent_accounts
           SET procurement_evidence = ?, buyer_role = ?
           WHERE company_id = ?""",
        [_json(sorted(procurement_evidence)), buyer_role, company_id],
    )


def _find_tender_role_label(roles: set[str]) -> str:
    is_buyer = "buyer" in roles
    is_supplier = "supplier" in roles
    if is_buyer and is_supplier:
        return "Find a Tender buyer and supplier"
    if is_buyer:
        return "Find a Tender buyer"
    if is_supplier:
        return "Find a Tender supplier"
    return "Find a Tender procurement party"


def _merge_buyer_role(existing: str | None, new_role: str) -> str:
    if not existing or existing == "Not verified" or existing.startswith("Find a Tender "):
        return new_role
    if new_role in existing:
        return existing
    return f"{existing}; {new_role}"


def _compare_strong_identifiers(
    left: dict[str, set[str]], right: dict[str, set[str]]
) -> tuple[set[str], set[str]]:
    matching: set[str] = set()
    conflicting: set[str] = set()
    for scheme in _STRONG_IDENTIFIER_SCHEMES:
        left_values = left.get(scheme, set())
        right_values = right.get(scheme, set())
        if not left_values or not right_values:
            continue
        if left_values & right_values:
            matching.add(scheme)
        else:
            conflicting.add(scheme)
    return matching, conflicting


def _procurement_source_key(
    *,
    release_id: str,
    party_index: int,
    party_id: str | None,
    identifiers: dict[str, set[str]],
) -> str:
    for scheme in _STRONG_IDENTIFIER_SCHEMES:
        values = identifiers.get(scheme, set())
        if len(values) == 1:
            return f"identifier:{scheme}:{next(iter(values))}"
    if party_id:
        typed = _typed_party_identifier(party_id)
        if typed:
            return f"identifier:{typed[0]}:{typed[1]}"
        return f"release:{release_id}:party:{party_id}"
    return f"release:{release_id}:party-index:{party_index}"


def _typed_party_identifier(party_id: str) -> tuple[str, str] | None:
    normalized = unicodedata.normalize("NFKC", party_id).strip().upper()
    for scheme in _STRONG_IDENTIFIER_SCHEMES:
        prefix = scheme + "-"
        if normalized.startswith(prefix):
            value = _normalize_identifier_value(normalized.removeprefix(prefix))
            if value:
                return scheme, value
    return None


def _party_identifiers(
    value: object, party_id: str | None
) -> dict[str, set[str]]:
    found: dict[str, set[str]] = {}
    parsed = _parse_json(value)
    if isinstance(parsed, list):
        for item in cast(list[object], parsed):
            if not isinstance(item, dict):
                continue
            mapping = cast(dict[object, object], item)
            scheme = _normalize_identifier_scheme(_text(mapping.get("scheme")))
            identifier = _normalize_identifier_value(_text(mapping.get("value")))
            if scheme and identifier:
                found.setdefault(scheme, set()).add(identifier)
    if party_id and (source_party_id := _normalize_identifier_value(party_id)):
        found.setdefault("FTS-PARTY-ID", set()).add(source_party_id)
    if party_id and (typed := _typed_party_identifier(party_id)):
        found.setdefault(typed[0], set()).add(typed[1])
    return found


def _canonical_identifiers(value: object) -> dict[str, set[str]]:
    parsed = _parse_json(value)
    found: dict[str, set[str]] = {}
    if not isinstance(parsed, dict):
        return found
    for raw_scheme, raw_value in cast(dict[object, object], parsed).items():
        scheme_text = str(raw_scheme).split("#", maxsplit=1)[0]
        scheme = _normalize_identifier_scheme(scheme_text)
        identifier = _normalize_identifier_value(_text(raw_value))
        if scheme and identifier:
            found.setdefault(scheme, set()).add(identifier)
    return found


def _merge_identifier_maps(
    target: dict[str, set[str]], incoming: dict[str, set[str]]
) -> None:
    for scheme, values in incoming.items():
        target.setdefault(scheme, set()).update(values)


def _flatten_identifiers(value: dict[str, set[str]]) -> dict[str, str]:
    flattened: dict[str, str] = {}
    for scheme, values in sorted(value.items()):
        for index, identifier in enumerate(sorted(values), start=1):
            key = scheme if index == 1 else f"{scheme}#{index}"
            flattened[key] = identifier
    return flattened


def _merge_flat_identifiers(
    existing: dict[str, object], incoming: dict[str, str]
) -> dict[str, str]:
    merged = {str(key): str(value) for key, value in existing.items()}
    for key, value in incoming.items():
        if key not in merged or merged[key] == value:
            merged[key] = value
            continue
        suffix = hashlib.sha256(value.encode()).hexdigest()[:8]
        merged[f"{key}#{suffix}"] = value
    return merged


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


def _location_address(location: dict[str, object]) -> str | None:
    parts = [
        _mapping_text(location, "street_address", "streetAddress"),
        _mapping_text(location, "locality"),
        _mapping_text(location, "region"),
        _mapping_text(location, "postal_code", "postalCode"),
    ]
    rendered = ", ".join(value for value in parts if value)
    return rendered or None


def _normalize_address(value: str) -> str | None:
    text = unicodedata.normalize("NFKC", value).upper().replace("&", " AND ")
    text = _POSTCODE_RE.sub(" ", text)
    normalized = re.sub(r"[^A-Z0-9]+", " ", text)
    normalized = " ".join(normalized.split())
    return normalized or None


def _extract_postcodes(value: str) -> set[str]:
    return {
        postcode
        for match in _POSTCODE_RE.findall(value.upper())
        if (postcode := _normalize_postcode(match)) is not None
    }


def _normalize_postcode(value: str | None) -> str | None:
    if value is None:
        return None
    compact = re.sub(r"[^A-Z0-9]", "", value.upper())
    return compact if _POSTCODE_COMPACT_RE.fullmatch(compact) else None


def _preferred_country(countries: set[str]) -> str | None:
    if not countries:
        return None
    return sorted(countries)[0]


def _normalize_role(value: str) -> str:
    return re.sub(r"[^a-z]+", "_", value.lower()).strip("_")


def _mapping_text(mapping: dict[str, object], *keys: str) -> str | None:
    for key in keys:
        value = _text(mapping.get(key))
        if value:
            return value
    return None


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_json(value: object) -> object:
    if isinstance(value, str):
        try:
            parsed: object = json.loads(value)
            return parsed
        except json.JSONDecodeError:
            return None
    return value


def _json_strings(value: object) -> list[str]:
    parsed = _parse_json(value)
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in cast(list[object], parsed) if item is not None]


def _json_mapping(value: object) -> dict[str, object]:
    parsed = _parse_json(value)
    if not isinstance(parsed, dict):
        return {}
    return {str(key): item for key, item in cast(dict[object, object], parsed).items()}


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


__all__ = ["link_recurrent_accounts_to_procurement"]
