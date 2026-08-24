import json
from pathlib import Path

from demand_intelligence.classification import match_product_cluster


def _taxonomy(tmp_path: Path) -> Path:
    path = tmp_path / "taxonomy.json"
    path.write_text(
        json.dumps(
            {
                "clusters": [
                    {
                        "id": "hand_tools",
                        "label": "Hand tools",
                        "keywords": ["hand tools", "pliers"],
                        "cpv_prefixes": ["4451"],
                        "candidate_hs_codes": ["8203"],
                    }
                ]
            }
        )
    )
    return path


def test_keyword_matching_uses_word_boundaries(tmp_path: Path) -> None:
    taxonomy = _taxonomy(tmp_path)

    assert (
        match_product_cluster(
            cpv_codes=[], text="Suppliers must submit online.", taxonomy_path=taxonomy
        )
        is None
    )
    assert (
        match_product_cluster(
            cpv_codes=[], text="A framework for pliers.", taxonomy_path=taxonomy
        )
        is not None
    )
    assert (
        match_product_cluster(
            cpv_codes=[], text="Supply of hand   tools", taxonomy_path=taxonomy
        )
        is not None
    )
