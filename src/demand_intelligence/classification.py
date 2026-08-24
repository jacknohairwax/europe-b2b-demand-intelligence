"""Configuration-driven product classification helpers."""

import json
import re
from pathlib import Path

from pydantic import BaseModel, Field


class ProductMatch(BaseModel):
    cluster_id: str
    label: str
    candidate_hs_codes: list[str] = Field(default_factory=list)
    mapping_status: str | None = None


def _matches_keyword(haystack: str, keyword: object) -> bool:
    phrase = " ".join(str(keyword).lower().split())
    if not phrase:
        return False
    # Product words must be complete tokens.  In particular, "pliers" must
    # never classify the ubiquitous procurement word "suppliers".
    pattern = r"(?<![a-z0-9])" + re.escape(phrase).replace(r"\ ", r"\s+")
    pattern += r"(?![a-z0-9])"
    return re.search(pattern, haystack) is not None


def match_product_cluster(
    *, cpv_codes: list[str], text: str | None, taxonomy_path: Path
) -> ProductMatch | None:
    taxonomy = json.loads(taxonomy_path.read_text())
    haystack = (text or "").lower()
    for cluster in taxonomy.get("clusters", []):
        cpv_match = any(
            code.startswith(prefix)
            for code in cpv_codes
            for prefix in cluster.get("cpv_prefixes", [])
        )
        keyword_match = any(
            _matches_keyword(haystack, keyword)
            for keyword in cluster.get("keywords", [])
        )
        if cpv_match or keyword_match:
            return ProductMatch(
                cluster_id=str(cluster["id"]),
                label=str(cluster["label"]),
                candidate_hs_codes=[
                    str(code) for code in cluster.get("candidate_hs_codes", [])
                ],
                mapping_status=(
                    str(cluster["mapping_status"])
                    if cluster.get("mapping_status") is not None
                    else None
                ),
            )
    return None


__all__ = ["ProductMatch", "match_product_cluster"]
