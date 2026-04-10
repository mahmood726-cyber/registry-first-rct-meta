"""Toy dataset fixtures for smoke testing."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def write_toy_cochrane_csv(base_dir: Path) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    path = base_dir / "toy_review.csv"
    df = pd.DataFrame(
        [
            {
                "review_id": "toy_review_1",
                "review_title": "SGLT2 inhibitors in HFpEF randomized trials",
                "condition_terms": "heart failure preserved ejection fraction",
                "intervention_terms": "sglt2 inhibitor",
                "citation_title": "Randomized trial of empagliflozin in HFpEF",
                "pmid": "12345678",
                "nct_id": "NCT01234567",
                "doi": "10.1000/xyz123",
                "year": "2018",
                "journal": "J Cardiology",
            },
            {
                "review_id": "toy_review_1",
                "review_title": "SGLT2 inhibitors in HFpEF randomized trials",
                "condition_terms": "heart failure preserved ejection fraction",
                "intervention_terms": "sglt2 inhibitor",
                "citation_title": "Randomized trial of dapagliflozin in HFpEF",
                "pmid": "22345678",
                "nct_id": "NCT07654321",
                "doi": "10.1000/xyz124",
                "year": "2020",
                "journal": "Heart",
            },
        ]
    )
    df.to_csv(path, index=False)
    return path


def write_toy_cochrane_json(base_dir: Path) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    path = base_dir / "toy_review.json"
    payload = {
        "review_id": "toy_review_2",
        "review_title": "Antiplatelet RCTs in post-MI care",
        "condition_terms": ["myocardial", "infarction"],
        "intervention_terms": ["antiplatelet"],
        "included_studies": [
            {
                "study_id": "study_a",
                "citation_title": "Randomized antiplatelet trial",
                "pmid": "32345678",
                "nct_id": "NCT11112222",
                "doi": "10.1000/abc111",
                "year": 2017,
            }
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def write_toy_topic_yaml(base_dir: Path) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    path = base_dir / "hfpef_sglt2.yaml"
    path.write_text(
        """
review_id: hfpef_sglt2
review_title: HFpEF and SGLT2 inhibitors
condition_terms:
  - heart failure
  - hfpef
intervention_terms:
  - sglt2
  - empagliflozin
  - dapagliflozin
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path
