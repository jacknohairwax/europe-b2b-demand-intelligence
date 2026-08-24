from demand_intelligence.entity_resolution import normalize_legal_name


def test_known_legal_suffix_variants_normalize_without_dropping_suffixes() -> None:
    assert normalize_legal_name("Acme & Sons Ltd.") == (
        normalize_legal_name("ACME AND SONS LIMITED")
    )
    assert normalize_legal_name("Example P.L.C.") == (
        normalize_legal_name("Example PLC")
    )
    assert normalize_legal_name("Werkzeug G.M.B.H.") == (
        normalize_legal_name("Werkzeug GmbH")
    )
