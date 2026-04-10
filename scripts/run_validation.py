"""CLI: run registry-first validation over Cochrane-like datasets."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

if str(Path(__file__).resolve().parents[1] / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from registry_first_ma.io import ensure_runtime_dirs
from registry_first_ma.validation import run_validation_workflow


def str_to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run registry-first RCT-only validation against Cochrane-style datasets.",
    )
    parser.add_argument("--data_dir", type=str, required=True, help="Directory containing Cochrane dataset files (CSV/JSON/XML).")
    parser.add_argument("--out_dir", type=str, default="outputs", help="Output directory for per-review and summary artifacts.")
    parser.add_argument("--cache_dir", type=str, default="cache", help="Deterministic HTTP cache directory.")
    parser.add_argument("--max_reviews", type=int, default=501, help="Maximum number of reviews to process.")
    parser.add_argument("--after_year", type=int, default=2010, help="Filter reviews to those with evidence year >= this year.")
    parser.add_argument("--main_outcome_only", type=str_to_bool, default=True, help="If true, extract only the single/main outcome (default true).")
    parser.add_argument("--cap_ncts", type=int, default=500, help="Max CT.gov records to keep per review query.")
    parser.add_argument("--cap_seed_pmids", type=int, default=20, help="Max PubMed seed PMIDs per trial for linkage.")
    parser.add_argument("--grace_months", type=int, default=24, help="Grace period in months for unreported completed-trial flag.")
    parser.add_argument("--rct_filter_toggle", type=str_to_bool, default=True, help="Require interventional/randomized trial filters.")
    parser.add_argument("--use_openalex", type=str_to_bool, default=True, help="Enable OpenAlex identifier mapping.")
    parser.add_argument("--use_unpaywall", type=str_to_bool, default=False, help="Enable Unpaywall OA lookup by DOI.")
    parser.add_argument("--use_europepmc", type=str_to_bool, default=False, help="Enable Europe PMC OA lookup.")
    parser.add_argument("--use_aact_fallback", type=str_to_bool, default=True, help="Enable AACT PostgreSQL fallback if CT.gov API is unavailable.")
    parser.add_argument("--aact_env_file", type=str, default=None, help="Optional path to .env containing AACT_USER/AACT_PASSWORD.")
    parser.add_argument("--use_crossref", type=str_to_bool, default=True, help="Enable Crossref DOI enrichment for sparse review metadata.")
    parser.add_argument("--ncbi_api_key", type=str, default=None, help="Optional NCBI E-utilities API key to reduce PubMed throttling.")
    parser.add_argument("--measure", choices=["RR", "OR"], default="RR", help="Binary effect measure for pooling.")
    return parser


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    args = build_parser().parse_args()

    ensure_runtime_dirs(args.out_dir, args.cache_dir)

    per_review, summary = run_validation_workflow(
        data_dir=args.data_dir,
        out_dir=args.out_dir,
        cache_dir=args.cache_dir,
        max_reviews=args.max_reviews,
        after_year=args.after_year,
        main_outcome_only=args.main_outcome_only,
        cap_ncts=args.cap_ncts,
        cap_seed_pmids=args.cap_seed_pmids,
        grace_months=args.grace_months,
        rct_filter_toggle=args.rct_filter_toggle,
        use_openalex=args.use_openalex,
        use_unpaywall=args.use_unpaywall,
        use_europepmc=args.use_europepmc,
        use_aact_fallback=args.use_aact_fallback,
        aact_env_file=args.aact_env_file,
        use_crossref=args.use_crossref,
        ncbi_api_key=args.ncbi_api_key,
        measure=args.measure,
    )

    logging.info("Validation complete: %d reviews processed", len(per_review))
    logging.info("Summary rows: %d", len(summary))


if __name__ == "__main__":
    main()
