"""Validation harness over Cochrane-style review datasets."""

from __future__ import annotations

import json
import logging
import re
import socket
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from rapidfuzz import fuzz

from .crossref import CrossrefClient
from .engine import RegistryFirstEngine
from .io import load_cochrane_datasets
from .models import ReviewRecord
from .normalize import (
    canonicalize_title,
    extract_dois,
    extract_nct_ids,
    keyword_terms,
    normalize_doi,
    normalize_identifier_set,
    normalize_nct_id,
    normalize_study_label,
)

LOGGER = logging.getLogger(__name__)


def _review_max_year(review: ReviewRecord) -> int | None:
    years = [s.year for s in review.included_studies if s.year]
    if years:
        return max(years)
    if review.year_range:
        vals = [int(tok) for tok in str(review.year_range).replace("-", " ").split() if tok.isdigit()]
        if vals:
            return max(vals)
    return None


def _gold_identifier_set(review: ReviewRecord) -> set[str]:
    ids: set[str] = set()
    review_level_dois = _likely_review_level_dois(review)
    for study in review.included_studies:
        ids.update(normalize_identifier_set([study.pmid, study.nct_id]))
        doi = normalize_doi(study.doi)
        if doi and doi not in review_level_dois:
            ids.add(doi)
        text = " ".join(
            [
                str(study.study_id or ""),
                str(study.citation_title or ""),
            ]
        )
        ids.update(extract_nct_ids(text))
        for doi in extract_dois(text):
            if doi not in review_level_dois:
                ids.add(doi)
    return ids


def _likely_review_level_dois(review: ReviewRecord) -> set[str]:
    dois = [normalize_doi(s.doi) for s in review.included_studies if s.doi]
    dois = [d for d in dois if d]
    if not dois:
        return set()

    rid = review.review_id.lower().replace("_", ".")
    counter = Counter(dois)
    min_repeat = max(2, int(0.80 * max(1, len(review.included_studies))))
    out: set[str] = set()
    for doi, count in counter.items():
        if count < min_repeat:
            continue
        if rid in doi or "14651858.cd" in doi:
            out.add(doi)
    return out


def _gold_title_set(review: ReviewRecord) -> list[str]:
    titles: list[str] = []
    for study in review.included_studies:
        norm = normalize_study_label(study.citation_title)
        # Keep only informative labels.
        if not norm:
            continue
        if len(norm.split()) < 2:
            continue
        titles.append(norm)
    # deterministic de-dup order
    dedup: list[str] = []
    seen: set[str] = set()
    for t in titles:
        if t not in seen:
            seen.add(t)
            dedup.append(t)
    return dedup


def _looks_like_review_id_title(title: str | None, review_id: str) -> bool:
    if not title:
        return True
    t = title.strip().lower()
    rid = review_id.strip().lower()
    if t == rid:
        return True
    if re.fullmatch(r"cd\d{6}(?:_pub\d+)?", t):
        return True
    canon = canonicalize_title(t)
    return canon in {canonicalize_title(rid), ""}


def _enrich_review_from_crossref(review: ReviewRecord, crossref: CrossrefClient) -> None:
    """Use likely review DOI metadata to improve query terms when local metadata is sparse."""
    review_level_dois = sorted(_likely_review_level_dois(review))
    if not review_level_dois:
        return
    meta = crossref.lookup_doi(review_level_dois[0])
    if not meta:
        return

    title = str(meta.get("title") or "").strip()
    subjects = [str(x).strip() for x in (meta.get("subjects") or []) if str(x).strip()]

    if title and _looks_like_review_id_title(review.review_title, review.review_id):
        review.review_title = title

    if not review.condition_terms:
        seeds = []
        if title:
            seeds.append(title)
        seeds.extend(subjects[:6])
        terms = keyword_terms(seeds, cap_terms=10)
        if terms:
            review.condition_terms = terms



def _match_gold_titles_to_trials(
    gold_titles: list[str],
    found_trial_titles: dict[str, list[str]],
    *,
    threshold: int = 88,
) -> dict[str, str]:
    if not gold_titles or not found_trial_titles:
        return {}

    indexed: list[tuple[str, str, set[str]]] = []
    for trial_id, title_list in found_trial_titles.items():
        for raw_title in title_list:
            norm = normalize_study_label(raw_title)
            if not norm:
                continue
            toks = set(norm.split())
            if not toks:
                continue
            indexed.append((trial_id, norm, toks))

    matches: dict[str, str] = {}
    for g in gold_titles:
        g_toks = set(g.split())
        if not g_toks:
            continue
        best_score = -1
        best_trial: str | None = None
        for trial_id, cand, cand_toks in indexed:
            # Fast guard to avoid fuzzy scoring unrelated labels.
            if not (g_toks & cand_toks):
                continue
            score = fuzz.token_set_ratio(g, cand)
            if score > best_score:
                best_score = score
                best_trial = trial_id
        if best_trial and best_score >= threshold:
            matches[g] = best_trial
    return matches


def _gold_nct_set(review: ReviewRecord) -> set[str]:
    out: set[str] = set()
    for study in review.included_studies:
        nct = normalize_nct_id(study.nct_id)
        if nct:
            out.add(nct)
    return out


def _safe_ratio(n: float, d: float) -> float:
    if d <= 0:
        return float("nan")
    return float(n) / float(d)


def _resolve_missing_pmids_by_title(
    review: ReviewRecord,
    engine: RegistryFirstEngine,
    cap: int,
    *,
    pubmed_available: bool,
) -> None:
    if not pubmed_available:
        return
    resolved = 0
    for study in review.included_studies:
        if study.pmid:
            continue
        if not study.citation_title:
            continue
        if resolved >= cap:
            break
        try:
            pmid = engine.pubmed.title_to_pmid_best_effort(study.citation_title)
        except Exception:
            pmid = None
        if pmid:
            study.pmid = pmid
            resolved += 1


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _summary_block(df: pd.DataFrame, label: str) -> dict[str, Any]:
    if df.empty:
        return {
            "stratum": label,
            "n_reviews": 0,
        }

    metrics = [
        "study_rediscovery_recall",
        "main_outcome_extractability",
        "ecr_trials",
        "ecr_participants",
        "results_posting_rate",
    ]

    row: dict[str, Any] = {"stratum": label, "n_reviews": int(len(df))}
    for metric in metrics:
        if metric not in df.columns:
            row[f"{metric}_mean"] = np.nan
            row[f"{metric}_median"] = np.nan
            row[f"{metric}_p10"] = np.nan
            row[f"{metric}_worst"] = np.nan
            continue
        vals = pd.to_numeric(df[metric], errors="coerce").dropna()
        if vals.empty:
            row[f"{metric}_mean"] = np.nan
            row[f"{metric}_median"] = np.nan
            row[f"{metric}_p10"] = np.nan
            row[f"{metric}_worst"] = np.nan
        else:
            row[f"{metric}_mean"] = float(vals.mean())
            row[f"{metric}_median"] = float(vals.median())
            row[f"{metric}_p10"] = float(vals.quantile(0.10))
            row[f"{metric}_worst"] = float(vals.min())

    if "operational_cutoff_passed" in df.columns:
        row["operational_cutoff_pass_rate"] = float(
            pd.to_numeric(df["operational_cutoff_passed"], errors="coerce").fillna(0).mean()
        )
    else:
        row["operational_cutoff_pass_rate"] = np.nan
    return row


def _save_plots(per_review_df: pd.DataFrame, out_dir: Path) -> None:
    try:
        import matplotlib.pyplot as plt
    except Exception as exc:  # pragma: no cover
        LOGGER.warning("matplotlib unavailable; skipping plots (%s)", exc)
        return

    plot_dir = out_dir / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    specs = [
        ("study_rediscovery_recall", "Recall Distribution", "recall_distribution.png"),
        ("main_outcome_extractability", "Extractability Distribution", "extractability_distribution.png"),
        ("ecr_participants", "ECR Participants Distribution", "ecr_participants_distribution.png"),
    ]

    for col, title, fname in specs:
        vals = pd.to_numeric(per_review_df[col], errors="coerce").dropna()
        if vals.empty:
            continue
        plt.figure(figsize=(7, 4))
        plt.hist(vals, bins=20)
        plt.title(title)
        plt.xlabel(col)
        plt.ylabel("Count")
        plt.tight_layout()
        plt.savefig(plot_dir / fname, dpi=150)
        plt.close()


def run_validation_workflow(
    *,
    data_dir: str,
    out_dir: str,
    cache_dir: str,
    max_reviews: int,
    after_year: int | None,
    main_outcome_only: bool,
    cap_ncts: int,
    cap_seed_pmids: int,
    grace_months: int,
    rct_filter_toggle: bool,
    use_openalex: bool,
    use_unpaywall: bool,
    use_europepmc: bool,
    use_aact_fallback: bool = True,
    aact_env_file: str | None = None,
    use_crossref: bool = True,
    ncbi_api_key: str | None = None,
    measure: str = "RR",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    output_root = Path(out_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    universe_dir = output_root / "per_review_trial_universe"
    results_dir = output_root / "per_review_results_extract"
    profile_dir = output_root / "per_review_transparency_profile"
    gap_dir = output_root / "per_review_gap_report"
    meta_dir = output_root / "per_review_meta_pack"
    for d in [universe_dir, results_dir, profile_dir, gap_dir, meta_dir]:
        d.mkdir(parents=True, exist_ok=True)

    reviews = load_cochrane_datasets(
        data_dir,
        max_reviews=max_reviews,
        after_year=after_year,
        rct_filter_toggle=rct_filter_toggle,
    )
    LOGGER.info("Loaded %d candidate reviews from %s", len(reviews), data_dir)

    engine = RegistryFirstEngine(
        cache_dir=cache_dir,
        grace_months=grace_months,
        use_openalex=use_openalex,
        use_unpaywall=use_unpaywall,
        use_europepmc=use_europepmc,
        use_aact_fallback=use_aact_fallback,
        aact_env_file=aact_env_file,
        ncbi_api_key=ncbi_api_key,
    )
    crossref = CrossrefClient(engine.http)
    try:
        socket.gethostbyname("eutils.ncbi.nlm.nih.gov")
        pubmed_available = True
    except Exception:
        pubmed_available = False
        LOGGER.warning("PubMed host not resolvable. Skipping title-based PMID resolution.")

    metric_rows: list[dict[str, Any]] = []

    for review in reviews:
        try:
            if use_crossref:
                _enrich_review_from_crossref(review, crossref)
            _resolve_missing_pmids_by_title(
                review,
                engine,
                cap=cap_seed_pmids,
                pubmed_available=pubmed_available,
            )
            run = engine.run_review(
                review,
                cap_ncts=cap_ncts,
                cap_seed_pmids=cap_seed_pmids,
                main_outcome_only=main_outcome_only,
                rct_filter_toggle=rct_filter_toggle,
                measure=measure,
            )
        except Exception as exc:
            LOGGER.exception("Review failed: %s (%s)", review.review_id, exc)
            metric_rows.append(
                {
                    "review_id": review.review_id,
                    "review_title": review.review_title,
                    "error": str(exc),
                }
            )
            continue

        trial_df = pd.DataFrame([asdict(t) for t in run.trial_universe])
        results_df = pd.DataFrame([asdict(r) for r in run.results_rows])
        gaps_df = pd.DataFrame(run.gap_rows)

        if trial_df.empty:
            trial_df = pd.DataFrame(
                columns=[
                    "review_id",
                    "trial_id",
                    "overall_status",
                    "study_type",
                    "allocation",
                    "start_date",
                    "primary_completion_date",
                    "enrollment",
                    "sponsor_type",
                    "has_results",
                    "is_registered",
                ]
            )
        if results_df.empty:
            results_df = pd.DataFrame(
                columns=[
                    "review_id",
                    "trial_id",
                    "outcome_name",
                    "timepoint",
                    "arm",
                    "events",
                    "total",
                    "measure_type",
                    "source",
                    "provenance_link",
                    "matched_main_outcome",
                    "effect_estimate",
                    "effect_ci_low",
                    "effect_ci_high",
                    "effect_metric",
                ]
            )
        if gaps_df.empty:
            gaps_df = pd.DataFrame(
                columns=[
                    "review_id",
                    "study_idx",
                    "study_id",
                    "citation_title",
                    "pmid",
                    "nct_id",
                    "doi",
                    "gap_code",
                    "gap_description",
                ]
            )

        trial_df.drop(columns=["raw"], errors="ignore").to_csv(universe_dir / f"{review.review_id}.csv", index=False)
        results_df.to_csv(results_dir / f"{review.review_id}.csv", index=False)
        gaps_df.to_csv(gap_dir / f"{review.review_id}.csv", index=False)
        _write_json(profile_dir / f"{review.review_id}.json", run.transparency_profile)
        _write_json(meta_dir / f"{review.review_id}.json", run.meta_pack)

        gold_ids = _gold_identifier_set(review)
        gold_ncts = _gold_nct_set(review)
        gold_titles = _gold_title_set(review)

        found_ids = run.found_identifiers
        title_matches = _match_gold_titles_to_trials(gold_titles, run.found_trial_titles)
        if gold_ids:
            recall = _safe_ratio(len(found_ids.intersection(gold_ids)), len(gold_ids))
            recall_basis = "identifier"
        elif gold_titles:
            recall = _safe_ratio(len(title_matches), len(gold_titles))
            recall_basis = "title_fallback"
        else:
            recall = float("nan")
            recall_basis = "unavailable"

        extracted_main_ncts = set(
            results_df[
                (results_df["matched_main_outcome"].fillna(False))
                & (results_df["events"].notna())
                & (results_df["total"].notna())
            ]["trial_id"].dropna().astype(str)
        ) if not results_df.empty else set()
        extracted_main_hr_trials = set(
            results_df[
                (results_df["matched_main_outcome"].fillna(False))
                & (results_df.get("effect_metric", pd.Series(dtype=str)).fillna("").str.upper() == "HR")
                & (results_df.get("effect_estimate", pd.Series(dtype=float)).notna())
            ]["trial_id"].dropna().astype(str)
        ) if not results_df.empty else set()
        extractable_trial_ids = extracted_main_ncts.union(extracted_main_hr_trials)

        if gold_ncts:
            main_extractability = _safe_ratio(len(extractable_trial_ids.intersection(gold_ncts)), len(gold_ncts))
        elif gold_titles:
            n_extractable_titles = sum(1 for g in gold_titles if title_matches.get(g) in extractable_trial_ids)
            main_extractability = _safe_ratio(n_extractable_titles, len(gold_titles))
        else:
            # Fallback when no trial registry identifiers are available in gold data.
            main_extractability = float("nan")

        coverage = run.transparency_profile.get("coverage", {})
        pub_bias = run.transparency_profile.get("publication_bias", {})
        cutoff = run.transparency_profile.get("operational_cutoff", {})
        review_year = _review_max_year(review)

        observed_meta = run.meta_pack.get("observed", {}).get("random") or {}
        observed_hr = run.meta_pack.get("observed_hr", {}).get("random") or {}

        metric_rows.append(
            {
                "review_id": review.review_id,
                "review_title": review.review_title,
                "review_year": review_year,
                "n_gold_studies": len(review.included_studies),
                "n_gold_identifiers": len(gold_ids),
                "n_gold_titles": len(gold_titles),
                "n_found_identifiers": len(found_ids),
                "study_rediscovery_recall": recall,
                "study_rediscovery_recall_basis": recall_basis,
                "main_outcome_extractability": main_extractability,
                "pooled_effect_reproducibility": np.nan,  # dataset-dependent; unavailable in generic parser
                "observed_random_rr": observed_meta.get("rr"),
                "observed_random_rr_ci_low": observed_meta.get("rr_ci_low"),
                "observed_random_rr_ci_high": observed_meta.get("rr_ci_high"),
                "observed_random_hr": observed_hr.get("hr"),
                "observed_random_hr_ci_low": observed_hr.get("hr_ci_low"),
                "observed_random_hr_ci_high": observed_hr.get("hr_ci_high"),
                "ecr_trials": coverage.get("ecr_trials"),
                "ecr_participants": coverage.get("ecr_participants"),
                "results_posting_rate": pub_bias.get("results_posting_rate"),
                "publication_link_rate": pub_bias.get("publication_link_rate"),
                "post_2015_participant_share": pub_bias.get("post_2015_participant_share"),
                "operational_cutoff_passed": cutoff.get("passed"),
                "operational_cutoff_decision": cutoff.get("decision"),
            }
        )

    per_review_columns = [
        "review_id",
        "review_title",
        "review_year",
        "n_gold_studies",
        "n_gold_identifiers",
        "n_gold_titles",
        "n_found_identifiers",
        "study_rediscovery_recall",
        "study_rediscovery_recall_basis",
        "main_outcome_extractability",
        "pooled_effect_reproducibility",
        "observed_random_rr",
        "observed_random_rr_ci_low",
        "observed_random_rr_ci_high",
        "observed_random_hr",
        "observed_random_hr_ci_low",
        "observed_random_hr_ci_high",
        "ecr_trials",
        "ecr_participants",
        "results_posting_rate",
        "publication_link_rate",
        "post_2015_participant_share",
        "operational_cutoff_passed",
        "operational_cutoff_decision",
        "error",
    ]
    per_review = pd.DataFrame(metric_rows, columns=per_review_columns)
    per_review.to_csv(output_root / "per_review_validation_metrics.csv", index=False)

    summary_rows = [_summary_block(per_review, "all_reviews")]
    if not per_review.empty and "review_year" in per_review.columns:
        post_2010 = per_review[pd.to_numeric(per_review["review_year"], errors="coerce") >= 2010]
        post_2015 = per_review[pd.to_numeric(per_review["review_year"], errors="coerce") >= 2015]
        summary_rows.append(_summary_block(post_2010, "post_2010"))
        summary_rows.append(_summary_block(post_2015, "post_2015"))

    summary = pd.DataFrame(summary_rows)
    summary.to_csv(output_root / "summary_validation_metrics.csv", index=False)

    _save_plots(per_review, output_root)

    return per_review, summary
