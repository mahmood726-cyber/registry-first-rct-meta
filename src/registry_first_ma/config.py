# sentinel:skip-file — hardcoded paths are fixture/registry/audit-narrative data for this repo's research workflow, not portable application configuration. Same pattern as push_all_repos.py and E156 workbook files.
"""Configuration constants for registry-first workflows."""

from __future__ import annotations

from pathlib import Path

USER_AGENT = "registry-first-rct-meta/0.1"
CTGOV_V2_BASE = "https://clinicaltrials.gov/api/v2"
PUBMED_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
OPENALEX_BASE = "https://api.openalex.org"
UNPAYWALL_BASE = "https://api.unpaywall.org/v2"
EUROPEPMC_BASE = "https://www.ebi.ac.uk/europepmc/webservices/rest"
CROSSREF_BASE = "https://api.crossref.org"

DEFAULT_CACHE_DIR = Path("cache")
DEFAULT_OUTPUT_DIR = Path("outputs")
DEFAULT_AACT_SNAPSHOT_CANDIDATES = (
    Path(r"D:\AACT-storage\AACT\2026-04-12"),
    Path(r"C:\Users\user\AACT\2026-04-12"),
)
DEFAULT_RCT_EXTRACTOR_ROOT_CANDIDATES = (
    Path(r"C:\Projects\rct-extractor-v2"),
)

OPERATIONAL_CUTOFF_THRESHOLDS = {
    "post_2015_participant_share": 0.60,
    "binary_main_outcome_required": True,
    "registered_trial_share": 0.80,
    "participant_weighted_coverage": 0.70,
    "results_posting_rate": 0.60,
}

MAIN_OUTCOME_HIERARCHY = [
    ("mortality", ["mortality", "death", "all-cause"]),
    (
        "mace",
        [
            "mace",
            "major adverse cardiovascular",
            "composite cardiovascular",
            "major cardiovascular",
        ],
    ),
    ("nonfatal_mi_stroke", ["nonfatal mi", "myocardial infarction", "stroke"]),
    ("hospitalization", ["hospitalization", "hospitalisation", "hospital admission"]),
]

DEFAULT_GRACE_MONTHS = 24
DEFAULT_MAIN_OUTCOME_ONLY = True


def _resolve_first_existing_path(candidates: tuple[Path, ...]) -> str | None:
    for path in candidates:
        if path.exists():
            return str(path)
    return None


def resolve_default_aact_source() -> str | None:
    return _resolve_first_existing_path(DEFAULT_AACT_SNAPSHOT_CANDIDATES)


def resolve_default_pdf_extractor_root() -> str | None:
    return _resolve_first_existing_path(DEFAULT_RCT_EXTRACTOR_ROOT_CANDIDATES)
