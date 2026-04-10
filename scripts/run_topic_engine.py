"""CLI: run registry-first engine for one topic config."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

import pandas as pd

if str(Path(__file__).resolve().parents[1] / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from registry_first_ma.engine import RegistryFirstEngine
from registry_first_ma.io import ensure_runtime_dirs, load_topic_config


def str_to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run registry-first RCT-only meta-analysis for a topic config.")
    parser.add_argument("--config", type=str, required=True, help="Path to topic YAML/JSON config.")
    parser.add_argument("--out_dir", type=str, default="outputs", help="Output directory.")
    parser.add_argument("--cache_dir", type=str, default="cache", help="Deterministic HTTP cache directory.")
    parser.add_argument("--main_outcome_only", type=str_to_bool, default=True, help="If true, only main outcome extraction is executed.")
    parser.add_argument("--cap_ncts", type=int, default=500, help="Max CT.gov records to keep for trial universe.")
    parser.add_argument("--cap_seed_pmids", type=int, default=20, help="Max PubMed seed PMIDs per trial for linkage.")
    parser.add_argument("--grace_months", type=int, default=24, help="Grace period in months for transparency metrics.")
    parser.add_argument("--rct_filter_toggle", type=str_to_bool, default=True, help="Require interventional/randomized filters.")
    parser.add_argument("--use_openalex", type=str_to_bool, default=True, help="Enable OpenAlex mapping.")
    parser.add_argument("--use_unpaywall", type=str_to_bool, default=False, help="Enable Unpaywall OA lookup.")
    parser.add_argument("--use_europepmc", type=str_to_bool, default=False, help="Enable Europe PMC OA lookup.")
    parser.add_argument("--use_aact_fallback", type=str_to_bool, default=True, help="Enable AACT PostgreSQL fallback if CT.gov API is unavailable.")
    parser.add_argument("--aact_env_file", type=str, default=None, help="Optional path to .env containing AACT_USER/AACT_PASSWORD.")
    parser.add_argument("--ncbi_api_key", type=str, default=None, help="Optional NCBI E-utilities API key to reduce PubMed throttling.")
    parser.add_argument("--measure", choices=["RR", "OR"], default="RR", help="Binary effect measure for pooling.")
    return parser


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    args = build_parser().parse_args()

    out_dir, _ = ensure_runtime_dirs(args.out_dir, args.cache_dir)
    topic = load_topic_config(args.config)

    engine = RegistryFirstEngine(
        cache_dir=args.cache_dir,
        grace_months=args.grace_months,
        use_openalex=args.use_openalex,
        use_unpaywall=args.use_unpaywall,
        use_europepmc=args.use_europepmc,
        use_aact_fallback=args.use_aact_fallback,
        aact_env_file=args.aact_env_file,
        ncbi_api_key=args.ncbi_api_key,
    )

    run = engine.run_topic(
        topic,
        cap_ncts=args.cap_ncts,
        cap_seed_pmids=args.cap_seed_pmids,
        main_outcome_only=args.main_outcome_only,
        rct_filter_toggle=args.rct_filter_toggle,
        measure=args.measure,
    )

    rid = run.review.review_id
    topic_dir = Path(out_dir) / "topic_runs" / rid
    topic_dir.mkdir(parents=True, exist_ok=True)

    trial_df = pd.DataFrame([asdict(t) for t in run.trial_universe]).drop(columns=["raw"], errors="ignore")
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
    trial_df.to_csv(topic_dir / "trial_universe.csv", index=False)

    results_df = pd.DataFrame([asdict(r) for r in run.results_rows])
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
    results_df.to_csv(topic_dir / "results_extract.csv", index=False)

    gap_df = pd.DataFrame(run.gap_rows)
    if gap_df.empty:
        gap_df = pd.DataFrame(
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
    gap_df.to_csv(topic_dir / "gap_report.csv", index=False)

    (topic_dir / "transparency_profile.json").write_text(
        json.dumps(run.transparency_profile, indent=2, default=str),
        encoding="utf-8",
    )
    (topic_dir / "meta_analysis_pack.json").write_text(
        json.dumps(run.meta_pack, indent=2, default=str),
        encoding="utf-8",
    )

    logging.info("Topic run complete: %s", topic_dir)


if __name__ == "__main__":
    main()
