"""Identifier normalization and fuzzy helpers."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Iterable

NCT_RE = re.compile(r"\bNCT\d{8}\b", re.IGNORECASE)
PMID_RE = re.compile(r"\b\d{4,9}\b")
DOI_RE = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.IGNORECASE)
ISRCTN_RE = re.compile(r"\bISRCTN\d{8}\b", re.IGNORECASE)
EUDRACT_RE = re.compile(r"\b\d{4}-\d{6}-\d{2}\b")

STOPWORDS = {
    "the",
    "a",
    "an",
    "of",
    "to",
    "in",
    "for",
    "with",
    "and",
    "on",
    "at",
    "trial",
    "study",
}


def normalize_pmid(value: str | int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    m = PMID_RE.search(text)
    return m.group(0) if m else None


def normalize_nct_id(value: str | None) -> str | None:
    if value is None:
        return None
    m = NCT_RE.search(str(value).upper())
    return m.group(0).upper() if m else None


def normalize_isrctn(value: str | None) -> str | None:
    if value is None:
        return None
    m = ISRCTN_RE.search(str(value).upper())
    return m.group(0).upper() if m else None


def normalize_eudract(value: str | None) -> str | None:
    if value is None:
        return None
    m = EUDRACT_RE.search(str(value))
    return m.group(0) if m else None


def normalize_doi(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    m = DOI_RE.search(text)
    return m.group(0).lower() if m else None


def extract_nct_ids(text: str | None) -> list[str]:
    if not text:
        return []
    return sorted({m.group(0).upper() for m in NCT_RE.finditer(text)})


def extract_pmids(text: str | None) -> list[str]:
    if not text:
        return []
    return sorted({m.group(0) for m in PMID_RE.finditer(text)})


def extract_dois(text: str | None) -> list[str]:
    if not text:
        return []
    return sorted({m.group(0).lower() for m in DOI_RE.finditer(text)})


def canonicalize_title(title: str | None) -> str:
    if not title:
        return ""
    ascii_text = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
    clean = re.sub(r"[^a-zA-Z0-9\s]", " ", ascii_text.lower())
    tokens = [tok for tok in clean.split() if tok and tok not in STOPWORDS]
    return " ".join(tokens)


def normalize_study_label(title: str | None) -> str:
    """Normalize short citation labels (often 'Author 1999') for fallback matching."""
    canon = canonicalize_title(title)
    if not canon:
        return ""
    tokens = [tok for tok in canon.split() if not tok.isdigit()]
    return " ".join(tokens)


def keyword_terms(texts: Iterable[str], cap_terms: int = 12) -> list[str]:
    counts: dict[str, int] = {}
    for text in texts:
        canon = canonicalize_title(text)
        for token in canon.split():
            if len(token) < 4:
                continue
            if token.isdigit() or any(ch.isdigit() for ch in token):
                continue
            counts[token] = counts.get(token, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [token for token, _ in ranked[:cap_terms]]


def stable_json_hash(payload: dict | list | str) -> str:
    if isinstance(payload, str):
        encoded = payload.encode("utf-8")
    else:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def dedupe_list(values: Iterable[str | None]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if not value:
            continue
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def normalize_identifier_set(identifiers: Iterable[str | None]) -> set[str]:
    out: set[str] = set()
    for raw in identifiers:
        if not raw:
            continue
        text = str(raw).strip()
        nct = normalize_nct_id(text)
        pmid = normalize_pmid(text)
        doi = normalize_doi(text)
        if nct:
            out.add(nct)
        elif doi:
            out.add(doi)
        elif pmid:
            out.add(pmid)
    return out
