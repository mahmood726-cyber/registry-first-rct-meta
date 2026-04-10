from registry_first_ma.normalize import (
    canonicalize_title,
    extract_nct_ids,
    keyword_terms,
    normalize_doi,
    normalize_nct_id,
    normalize_pmid,
)


def test_normalize_identifiers() -> None:
    assert normalize_pmid("PMID: 12345678") == "12345678"
    assert normalize_nct_id("trial NCT01234567") == "NCT01234567"
    assert normalize_doi("https://doi.org/10.1000/AbC-123") == "10.1000/abc-123"


def test_extract_nct_ids() -> None:
    text = "NCT01234567 and nct07654321"
    assert extract_nct_ids(text) == ["NCT01234567", "NCT07654321"]


def test_canonicalize_title() -> None:
    title = "The Randomized Trial of Treatment in Patients"
    canon = canonicalize_title(title)
    assert "randomized" in canon
    assert "treatment" in canon
    assert "the" not in canon


def test_keyword_terms_excludes_id_like_tokens() -> None:
    terms = keyword_terms(["CD000219_pub5 trial 2010 outcomes"], cap_terms=10)
    assert all(not any(ch.isdigit() for ch in t) for t in terms)
