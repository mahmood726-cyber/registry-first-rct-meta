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
