"""Gap taxonomy assignment for registry-first validation."""

from __future__ import annotations

from typing import Any

from rapidfuzz import fuzz

from .models import IncludedStudy, TrialUniverseRecord
from .normalize import normalize_doi, normalize_identifier_set, normalize_nct_id, normalize_pmid, normalize_study_label

GAP_DESCRIPTIONS = {
    "G1": "Not rediscovered (no identifier / non-indexed)",
    "G2": "Identified but no registry record",
    "G3": "Registered but no results posted",
    "G4": "Results posted but endpoint/timepoint mismatch",
    "G5": "Data only in paywalled paper (no OA/regulatory alternative)",
    "G6": "Complex effect measure (HR) not extractable from registry",
    "G7": "Discrepancy between registry and publication",
    "OK": "No major gap for main-outcome extraction",
}


def _study_ids(study: IncludedStudy) -> set[str]:
    values = [study.pmid, study.nct_id, study.doi]
    out = normalize_identifier_set(values)
    if study.nct_id:
        nct = normalize_nct_id(study.nct_id)
        if nct:
            out.add(nct)
    if study.pmid:
        pmid = normalize_pmid(study.pmid)
        if pmid:
            out.add(pmid)
    if study.doi:
        doi = normalize_doi(study.doi)
        if doi:
            out.add(doi)
    return out


def _trial_id_from_study(study: IncludedStudy) -> str | None:
    if study.nct_id:
        return normalize_nct_id(study.nct_id)
    return None


def build_gap_report(
    *,
    review_id: str,
    included_studies: list[IncludedStudy],
    found_identifiers: set[str],
    trial_universe: list[TrialUniverseRecord],
    results_rows: list[dict[str, Any]],
    publication_links: dict[str, list[str]],
    oa_available_identifiers: set[str] | None = None,
    discrepancy_trial_ids: set[str] | None = None,
    found_trial_titles: dict[str, list[str]] | None = None,
) -> list[dict[str, Any]]:
    by_trial_id = {t.trial_id: t for t in trial_universe}
    rows_by_trial: dict[str, list[dict[str, Any]]] = {}
    for row in results_rows:
        tid = str(row.get("trial_id", ""))
        if not tid:
            continue
        rows_by_trial.setdefault(tid, []).append(row)

    oa_available_identifiers = oa_available_identifiers or set()
    discrepancy_trial_ids = discrepancy_trial_ids or set()
    found_trial_titles = found_trial_titles or {}

    indexed_titles: list[tuple[str, str, set[str]]] = []
    for tid, title_list in found_trial_titles.items():
        for raw in title_list:
            norm = normalize_study_label(raw)
            if not norm:
                continue
            toks = set(norm.split())
            if not toks:
                continue
            indexed_titles.append((tid, norm, toks))

    out: list[dict[str, Any]] = []
    for idx, study in enumerate(included_studies):
        identifiers = _study_ids(study)
        trial_id = _trial_id_from_study(study)

        matched_trial_by_title: str | None = None
        study_title_norm = normalize_study_label(study.citation_title)
        if study_title_norm:
            stoks = set(study_title_norm.split())
            best_score = -1
            for tid, cand, ctoks in indexed_titles:
                if not (stoks & ctoks):
                    continue
                score = fuzz.token_set_ratio(study_title_norm, cand)
                if score > best_score:
                    best_score = score
                    matched_trial_by_title = tid
            if best_score < 88:
                matched_trial_by_title = None

        rediscovered = bool(identifiers.intersection(found_identifiers)) or bool(matched_trial_by_title)
        if not trial_id and matched_trial_by_title:
            trial_id = matched_trial_by_title

        gap_code = "OK"

        if not rediscovered:
            gap_code = "G1"
        elif not trial_id or trial_id not in by_trial_id:
            gap_code = "G2"
        else:
            trial = by_trial_id[trial_id]
            trial_rows = rows_by_trial.get(trial_id, [])
            has_binary_rows = any(
                (row.get("events") is not None)
                and (row.get("total") is not None)
                and ("binary" in str(row.get("measure_type", "")).lower())
                for row in trial_rows
            )

            if trial_id in discrepancy_trial_ids:
                gap_code = "G7"
            elif not trial.has_results:
                gap_code = "G3"
            elif trial.has_results and not has_binary_rows:
                has_unmatched_only = any(
                    "unmatched" in str(row.get("measure_type", "")).lower() for row in trial_rows
                )
                gap_code = "G4" if has_unmatched_only else "G6"
            elif publication_links.get(trial_id):
                linked = set(publication_links.get(trial_id, []))
                if linked and not linked.intersection(oa_available_identifiers):
                    gap_code = "G5"

        out.append(
            {
                "review_id": review_id,
                "study_idx": idx + 1,
                "study_id": study.study_id,
                "citation_title": study.citation_title,
                "pmid": study.pmid,
                "nct_id": study.nct_id,
                "doi": study.doi,
                "gap_code": gap_code,
                "gap_description": GAP_DESCRIPTIONS[gap_code],
            }
        )

    return out
