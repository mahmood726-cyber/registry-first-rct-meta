# Registry-First RCT Meta-Analysis (Protocol-Based Transparency)

A complete Python project for **registry-first, RCT-only meta-analysis** with a protocol-based transparency layer and Cochrane-style validation harness.

## What This Project Does
This engine starts from registry protocols (primarily ClinicalTrials.gov v2), builds a denominator trial universe, links publications from open identifiers, extracts main binary outcome data conservatively, and computes:

- Direct publication bias metrics from completed-trial reporting behavior.
- Outcome-reporting-bias signals from protocol vs results modules.
- Evidence coverage ratios (`ECR_trials`, `ECR_participants`).
- Binary pooled effects (RR/OR; fixed + random DL).
- MNAR sensitivity scenarios for missing completed trials.
- Gap taxonomy labels (G1-G7) for non-reproducible evidence elements.

A validation harness processes up to 501 Cochrane-style datasets and outputs per-review + summary performance files.

## Final Recommendation: Operational Cutoff Table
Use this exact decision table:

| Criterion | Threshold |
|---|---:|
| Post-2015 participant share | >= 60% |
| Binary main outcome | Required |
| Registered trial share | >= 80% |
| Participant-weighted coverage | >= 70% |
| Results-posting rate | >= 60% |

If all criteria are met, the engine returns: **`RUN registry-first meta`**.
If not, it returns: **`exclude or flag as non-compatible`**.

## Legal/Source Scope
Implemented public/open sources only (no paywall bypass):

- ClinicalTrials.gov v2 API (primary)
- AACT snapshots (optional offline module: `src/registry_first_ma/aact.py`)
- AACT PostgreSQL fallback (automatic when CT.gov API is unavailable and AACT credentials are configured)
- PubMed E-utilities
- OpenAlex API (optional)
- Unpaywall API (optional)
- Europe PMC API (optional)
- Crossref API (optional DOI metadata enrichment)
- Regulatory module: metadata/link-out scope only (see `src/registry_first_ma/regulatory.py`)

## Install
```bash
cd registry_first_rct_meta
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

Run tests:
```bash
pytest
```

## Data Placement
- Cochrane review datasets: `data/cochrane_501/`
  - CSV and JSON are first-class.
  - RevMan XML is parsed best-effort.
- Topic configs (YAML/JSON): `data/topics/`

Example topic config included:
- `data/topics/hfpef_sglt2.yaml`

## CLI Usage
Validation harness:
```bash
python -m scripts.run_validation \
  --data_dir data/cochrane_501 \
  --out_dir outputs \
  --max_reviews 501 \
  --after_year 2010 \
  --main_outcome_only true
```

Topic engine:
```bash
python -m scripts.run_topic_engine \
  --config data/topics/hfpef_sglt2.yaml \
  --out_dir outputs
```

Useful options (`--help` on either command):
- `--cap_ncts`
- `--cap_seed_pmids`
- `--grace_months`
- `--rct_filter_toggle`
- `--use_openalex`
- `--use_unpaywall`
- `--use_europepmc`
- `--use_aact_fallback`
- `--aact_env_file`
- `--use_crossref`
- `--ncbi_api_key`
- `--measure {RR,OR}`

AACT fallback credentials:
- Set `AACT_USER` and `AACT_PASSWORD` in environment, or pass `--aact_env_file /path/to/.env`.
- If CT.gov is not reachable, engine automatically tries AACT fallback when enabled.

PubMed throughput:
- Set `NCBI_API_KEY` in environment (or pass `--ncbi_api_key`) to reduce E-utilities throttling.

## Output Artifacts
Generated under `outputs/`:

- `per_review_validation_metrics.csv`
  - Includes `study_rediscovery_recall_basis` (`identifier` vs `title_fallback`) and optional HR pooled fields.
- `summary_validation_metrics.csv`
- `per_review_trial_universe/*.csv`
- `per_review_results_extract/*.csv`
- `per_review_transparency_profile/*.json`
- `per_review_gap_report/*.csv`
- `per_review_meta_pack/*.json`
- `plots/*.png` (recall, extractability, ECR distributions)

Topic run outputs:
- `outputs/topic_runs/<review_id>/trial_universe.csv`
- `outputs/topic_runs/<review_id>/results_extract.csv`
- `outputs/topic_runs/<review_id>/transparency_profile.json`
- `outputs/topic_runs/<review_id>/meta_analysis_pack.json`
- `outputs/topic_runs/<review_id>/gap_report.csv`

## Core Pipeline Rules
1. Trial universe (denominator): registry-defined interventional randomized trials where available.
2. Main outcome rule (single outcome):
   - Mortality -> MACE -> nonfatal MI/stroke -> hospitalization -> first primary outcome.
   - Prefer longest follow-up timeframe.
3. Conservative extraction:
   - If uncertain, output unmatched rows (`events/total = null`) rather than invent values.
   - Main-outcome extraction now enforces stricter endpoint/timepoint matching (no fallback to arbitrary first reported endpoint).
4. Complex effect extension:
   - Time-to-event/hazard-ratio rows are extracted when hazard context is detectable.
   - If HR is indicated but not numerically recoverable, rows are flagged as `hazard_ratio_unmatched` for G6 taxonomy.
5. MNAR sensitivity scenarios for missing completed trials:
   - `S0` delta = 0.00 (MAR-like)
   - `S1` delta = +0.10 log scale
   - `S2` delta = +0.20 log scale
   - Worst plausible bound: +0.40

## Limitations
- Registry data are heterogeneous; binary extraction may remain unmatched in complex layouts.
- Endpoint/timepoint mismatch is common between protocols and posted results.
- HR/time-to-event extraction is partial and conservative; many records remain `hazard_ratio_unmatched`.
- Regulatory extraction is metadata/link-out only in this baseline implementation.
- Cochrane pooled-effect reproducibility requires dataset-level pooled estimates not always present in source files.
- Some Cochrane-like datasets may not contain true study-level PMIDs/NCTs; in those cases validation falls back to title-based rediscovery and may remain low when citations are short labels (e.g., "Surname 1998").
- Crossref DOI enrichment improves query terms only when review-level DOI metadata is resolvable and informative.

## Extend to New Registries
Add a new source client similar to `ctgov.py` and expose in `engine.py`:
1. Implement search + record normalization to `TrialUniverseRecord`.
2. Implement protocol outcome parsing and results extraction to `OutcomeRow`.
3. Keep source provenance fields populated (`source`, `provenance_link`).
4. Add source-aware gap-code logic if needed.

## Reproducibility and Caching
`cache/` stores deterministic request responses by hashed URL/params for:
- CT.gov searches
- PubMed lookups
- OpenAlex lookups
- Unpaywall/Europe PMC lookups

Runs are deterministic relative to the cache state.
