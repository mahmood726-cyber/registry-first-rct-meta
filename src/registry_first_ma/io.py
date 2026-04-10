"""Input/output utilities for Cochrane review datasets and topic configs."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import pandas as pd

try:
    import yaml
except Exception:  # pragma: no cover - optional fallback
    yaml = None

from .models import IncludedStudy, ReviewRecord
from .normalize import (
    canonicalize_title,
    extract_nct_ids,
    normalize_doi,
    normalize_nct_id,
    normalize_pmid,
)

LOGGER = logging.getLogger(__name__)


def _first_matching_column(columns: list[str], needles: list[str]) -> str | None:
    lowered = {col: col.lower() for col in columns}
    for needle in needles:
        for col, low in lowered.items():
            if needle in low:
                return col
    return None


def _extract_year(text: str | None) -> int | None:
    if not text:
        return None
    m = re.search(r"\b(19\d{2}|20\d{2})\b", str(text))
    if not m:
        return None
    year = int(m.group(1))
    return year if 1900 <= year <= 2100 else None


def _build_study_from_row(row: pd.Series, idx: int, cols: dict[str, str | None]) -> IncludedStudy:
    raw_title = row.get(cols["study_title"], None) if cols["study_title"] else None
    raw_title = str(raw_title).strip() if raw_title is not None else None

    pmid_raw = row.get(cols["pmid"], None) if cols["pmid"] else None
    nct_raw = row.get(cols["nct"], None) if cols["nct"] else None
    doi_raw = row.get(cols["doi"], None) if cols["doi"] else None

    pmid = normalize_pmid(pmid_raw)
    nct_candidates = extract_nct_ids(str(nct_raw)) if nct_raw is not None else []
    nct = nct_candidates[0] if nct_candidates else normalize_nct_id(str(nct_raw) if nct_raw else None)
    doi = normalize_doi(str(doi_raw) if doi_raw else None)

    year_raw = row.get(cols["study_year"], None) if cols["study_year"] else None
    year = _extract_year(str(year_raw) if year_raw else raw_title)

    journal = row.get(cols["journal"], None) if cols["journal"] else None
    journal_clean = str(journal).strip() if journal is not None else None

    study_id = nct or pmid or doi or f"study_{idx + 1}"
    return IncludedStudy(
        study_id=study_id,
        citation_title=raw_title,
        pmid=pmid,
        nct_id=nct,
        doi=doi,
        year=year,
        journal=journal_clean,
    )


def parse_cochrane_csv(path: Path) -> list[ReviewRecord]:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    if df.empty:
        return []

    columns = list(df.columns)
    col_map = {
        "review_id": _first_matching_column(columns, ["review_id", "cochrane_id", "review", "id"]),
        "review_title": _first_matching_column(columns, ["review_title", "cochrane_title", "review title", "title"]),
        "condition_terms": _first_matching_column(columns, ["condition", "population"]),
        "intervention_terms": _first_matching_column(columns, ["intervention", "treatment"]),
        "study_title": _first_matching_column(columns, ["citation", "study_title", "study title", "title"]),
        "pmid": _first_matching_column(columns, ["pmid"]),
        "nct": _first_matching_column(columns, ["nct", "clinicaltrials", "isrctn", "eudract"]),
        "doi": _first_matching_column(columns, ["doi"]),
        "study_year": _first_matching_column(columns, ["study_year", "publication_year", "year"]),
        "journal": _first_matching_column(columns, ["journal"]),
    }

    if col_map["review_id"]:
        grouped = df.groupby(col_map["review_id"], dropna=False)
    else:
        grouped = [(path.stem, df)]

    reviews: list[ReviewRecord] = []
    for group_key, part in grouped:
        part = part.reset_index(drop=True)
        rid = str(group_key).strip() or path.stem
        rtitle = (
            str(part.iloc[0][col_map["review_title"]]).strip()
            if col_map["review_title"]
            else path.stem.replace("_", " ")
        )
        cond_text = str(part.iloc[0][col_map["condition_terms"]]).strip() if col_map["condition_terms"] else ""
        int_text = str(part.iloc[0][col_map["intervention_terms"]]).strip() if col_map["intervention_terms"] else ""
        cond_terms = [tok for tok in canonicalize_title(cond_text).split() if tok]
        int_terms = [tok for tok in canonicalize_title(int_text).split() if tok]

        included = [_build_study_from_row(row, idx, col_map) for idx, row in part.iterrows()]
        year_candidates = [s.year for s in included if s.year]
        year_range = None
        if year_candidates:
            year_range = f"{min(year_candidates)}-{max(year_candidates)}"

        reviews.append(
            ReviewRecord(
                review_id=rid,
                review_title=rtitle,
                year_range=year_range,
                condition_terms=cond_terms,
                intervention_terms=int_terms,
                included_studies=included,
            )
        )

    return reviews


def _study_from_json(obj: dict[str, Any], idx: int) -> IncludedStudy:
    pmid = normalize_pmid(str(obj.get("pmid", "")))
    nct = normalize_nct_id(str(obj.get("nct_id") or obj.get("nct") or ""))
    doi = normalize_doi(str(obj.get("doi", "")))
    title = obj.get("citation_title") or obj.get("title") or obj.get("citation")
    year = _extract_year(str(obj.get("year") or title or ""))
    explicit_id = str(obj.get("study_id") or "").strip()
    study_id = explicit_id or nct or pmid or doi or f"study_{idx + 1}"
    return IncludedStudy(
        study_id=study_id,
        citation_title=str(title).strip() if title else None,
        pmid=pmid,
        nct_id=nct,
        doi=doi,
        year=year,
        journal=str(obj.get("journal")).strip() if obj.get("journal") else None,
    )


def parse_cochrane_json(path: Path) -> list[ReviewRecord]:
    payload = json.loads(path.read_text(encoding="utf-8"))

    review_items: list[dict[str, Any]]
    if isinstance(payload, dict) and "reviews" in payload and isinstance(payload["reviews"], list):
        review_items = payload["reviews"]
    elif isinstance(payload, dict) and "included_studies" in payload:
        review_items = [payload]
    elif isinstance(payload, list):
        # Either a list of reviews or a flat list of studies.
        if payload and isinstance(payload[0], dict) and "included_studies" in payload[0]:
            review_items = payload
        else:
            review_items = [
                {
                    "review_id": path.stem,
                    "review_title": path.stem.replace("_", " "),
                    "included_studies": payload,
                }
            ]
    else:
        return []

    out: list[ReviewRecord] = []
    for idx, item in enumerate(review_items):
        rid = str(item.get("review_id") or item.get("id") or f"{path.stem}_{idx + 1}")
        title = str(item.get("review_title") or item.get("title") or rid)
        included_raw = item.get("included_studies") or item.get("studies") or []
        if isinstance(included_raw, dict):
            included_raw = [included_raw]
        included = [
            _study_from_json(study, s_idx)
            for s_idx, study in enumerate(included_raw)
            if isinstance(study, dict)
        ]
        year_candidates = [s.year for s in included if s.year]
        year_range = item.get("year_range")
        if not year_range and year_candidates:
            year_range = f"{min(year_candidates)}-{max(year_candidates)}"

        cond_terms = item.get("condition_terms") or []
        int_terms = item.get("intervention_terms") or []
        if isinstance(cond_terms, str):
            cond_terms = canonicalize_title(cond_terms).split()
        if isinstance(int_terms, str):
            int_terms = canonicalize_title(int_terms).split()

        out.append(
            ReviewRecord(
                review_id=rid,
                review_title=title,
                year_range=str(year_range) if year_range else None,
                condition_terms=[str(x) for x in cond_terms if x],
                intervention_terms=[str(x) for x in int_terms if x],
                included_studies=included,
            )
        )
    return out


def parse_revman_xml(path: Path) -> list[ReviewRecord]:
    try:
        root = ET.fromstring(path.read_text(encoding="utf-8", errors="ignore"))
    except ET.ParseError:
        LOGGER.warning("Failed to parse XML file: %s", path)
        return []

    review_id = path.stem
    review_title = path.stem.replace("_", " ")

    title_node = root.find(".//TITLE")
    if title_node is not None and title_node.text:
        review_title = title_node.text.strip()

    included: list[IncludedStudy] = []
    for idx, node in enumerate(root.findall(".//STUDY")):
        text = " ".join([t.strip() for t in node.itertext() if t and t.strip()])
        pmid = normalize_pmid(text)
        ncts = extract_nct_ids(text)
        nct_id = ncts[0] if ncts else None
        doi = normalize_doi(text)
        year = _extract_year(text)
        title = text[:300] if text else None
        study_id = nct_id or pmid or doi or f"study_{idx + 1}"
        included.append(
            IncludedStudy(
                study_id=study_id,
                citation_title=title,
                pmid=pmid,
                nct_id=nct_id,
                doi=doi,
                year=year,
                journal=None,
            )
        )

    return [
        ReviewRecord(
            review_id=review_id,
            review_title=review_title,
            year_range=None,
            condition_terms=[],
            intervention_terms=[],
            included_studies=included,
        )
    ]


def _passes_after_year_filter(review: ReviewRecord, after_year: int | None) -> bool:
    if after_year is None:
        return True

    year_candidates = [study.year for study in review.included_studies if study.year]
    if year_candidates:
        return max(year_candidates) >= after_year

    if review.year_range:
        maybe_year = _extract_year(review.year_range)
        if maybe_year:
            return maybe_year >= after_year
    return True


def _is_probably_rct(review: ReviewRecord) -> bool:
    text = f"{review.review_title} " + " ".join(
        s.citation_title or "" for s in review.included_studies[:20]
    )
    check = canonicalize_title(text)
    tokens = set(check.split())
    rct_terms = {"randomized", "randomised", "randomly", "controlled", "rct"}
    return bool(tokens.intersection(rct_terms)) or len(review.included_studies) > 0


def load_cochrane_datasets(
    data_dir: str | Path,
    *,
    max_reviews: int = 501,
    after_year: int | None = None,
    rct_filter_toggle: bool = True,
) -> list[ReviewRecord]:
    data_path = Path(data_dir)
    files = sorted(
        [
            p
            for p in data_path.rglob("*")
            if p.is_file() and p.suffix.lower() in {".csv", ".json", ".xml"}
        ]
    )

    reviews: list[ReviewRecord] = []
    for file_path in files:
        try:
            if file_path.suffix.lower() == ".csv":
                parsed = parse_cochrane_csv(file_path)
            elif file_path.suffix.lower() == ".json":
                parsed = parse_cochrane_json(file_path)
            else:
                parsed = parse_revman_xml(file_path)
        except Exception as exc:  # pragma: no cover - robust fallback
            LOGGER.warning("Failed to parse %s: %s", file_path, exc)
            continue

        for review in parsed:
            if not _passes_after_year_filter(review, after_year):
                continue
            if rct_filter_toggle and not _is_probably_rct(review):
                continue
            reviews.append(review)
            if len(reviews) >= max_reviews:
                return reviews

    return reviews


def load_topic_config(path: str | Path) -> dict[str, Any]:
    cfg_path = Path(path)
    text = cfg_path.read_text(encoding="utf-8")

    if cfg_path.suffix.lower() == ".json":
        payload = json.loads(text)
    elif yaml is not None:
        payload = yaml.safe_load(text)
    else:
        # Minimal fallback parser for simple key: value YAML files.
        payload = {}
        current_list_key: str | None = None
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("-") and current_list_key:
                payload.setdefault(current_list_key, []).append(line.lstrip("- ").strip())
            elif ":" in line:
                key, value = line.split(":", 1)
                key = key.strip()
                value = value.strip()
                if value:
                    payload[key] = value
                    current_list_key = None
                else:
                    payload[key] = []
                    current_list_key = key

    if not isinstance(payload, dict):
        raise ValueError(f"Topic config must parse to an object: {cfg_path}")

    payload.setdefault("review_id", cfg_path.stem)
    payload.setdefault("review_title", payload.get("review_id", cfg_path.stem))
    payload.setdefault("condition_terms", [])
    payload.setdefault("intervention_terms", [])
    payload.setdefault("include_filters", {})
    payload.setdefault("exclude_filters", {})
    return payload


def ensure_runtime_dirs(out_dir: str | Path, cache_dir: str | Path) -> tuple[Path, Path]:
    out_path = Path(out_dir)
    cache_path = Path(cache_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    cache_path.mkdir(parents=True, exist_ok=True)
    (out_path / "plots").mkdir(parents=True, exist_ok=True)
    return out_path, cache_path
