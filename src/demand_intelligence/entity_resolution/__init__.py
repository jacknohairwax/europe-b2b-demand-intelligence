"""Conservative company-name normalization and source-scoped identities."""

import hashlib
import re
import unicodedata


def normalize_legal_name(value: str) -> str:
    """Normalize exact legal-name variants without removing legal suffixes."""

    text = unicodedata.normalize("NFKC", value).upper().replace("&", " AND ")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    tokens = text.split()
    normalized_tokens: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if tokens[index : index + 3] == ["L", "T", "D"]:
            normalized_tokens.append("LIMITED")
            index += 3
            continue
        if tokens[index : index + 3] == ["P", "L", "C"]:
            normalized_tokens.extend(["PUBLIC", "LIMITED", "COMPANY"])
            index += 3
            continue
        if tokens[index : index + 4] == ["G", "M", "B", "H"]:
            normalized_tokens.append("GMBH")
            index += 4
            continue
        if token == "LTD":
            normalized_tokens.append("LIMITED")
        elif token == "PLC":
            normalized_tokens.extend(["PUBLIC", "LIMITED", "COMPANY"])
        else:
            normalized_tokens.append(token)
        index += 1
    return " ".join(normalized_tokens)


def source_company_id(source: str, source_identifier: str) -> str:
    safe_source = re.sub(r"[^a-z0-9]+", "-", source.lower()).strip("-")
    digest = hashlib.sha256(source_identifier.encode()).hexdigest()[:20]
    return f"company-{safe_source}-{digest}"


__all__ = ["normalize_legal_name", "source_company_id"]
