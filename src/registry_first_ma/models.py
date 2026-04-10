"""Shared typed data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass(slots=True)
class IncludedStudy:
    study_id: str
    citation_title: str | None = None
    pmid: str | None = None
    nct_id: str | None = None
    doi: str | None = None
    year: int | None = None
    journal: str | None = None


@dataclass(slots=True)
class ReviewRecord:
    review_id: str
    review_title: str
    year_range: str | None = None
    condition_terms: list[str] = field(default_factory=list)
    intervention_terms: list[str] = field(default_factory=list)
    included_studies: list[IncludedStudy] = field(default_factory=list)


@dataclass(slots=True)
class TrialUniverseRecord:
    review_id: str
    trial_id: str
    overall_status: str | None
    study_type: str | None
    allocation: str | None
    start_date: date | None
    primary_completion_date: date | None
    enrollment: int | None
    sponsor_type: str | None
    has_results: bool
    is_registered: bool = True
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OutcomeRow:
    review_id: str
    trial_id: str
    outcome_name: str
    timepoint: str | None
    arm: str
    events: int | None
    total: int | None
    measure_type: str
    source: str
    provenance_link: str
    matched_main_outcome: bool
    effect_estimate: float | None = None
    effect_ci_low: float | None = None
    effect_ci_high: float | None = None
    effect_metric: str | None = None


@dataclass(slots=True)
class ReviewRunResult:
    review: ReviewRecord
    trial_universe: list[TrialUniverseRecord]
    results_rows: list[OutcomeRow]
    transparency_profile: dict[str, Any]
    meta_pack: dict[str, Any]
    gap_rows: list[dict[str, Any]]
    found_identifiers: set[str]
    found_trial_titles: dict[str, list[str]] = field(default_factory=dict)
