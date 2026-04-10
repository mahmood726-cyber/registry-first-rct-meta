"""Protocol-based transparency and compatibility metrics."""

from __future__ import annotations

from dataclasses import asdict
from datetime import date
from typing import Any

from .config import OPERATIONAL_CUTOFF_THRESHOLDS
from .models import TrialUniverseRecord


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _default_enrollment(enrollment: int | None) -> int:
    return enrollment if enrollment and enrollment > 0 else 0


def _outcome_name_list_from_protocol(raw: dict[str, Any]) -> list[str]:
    module = raw.get("protocolSection", {}).get("outcomesModule", {})
    out: list[str] = []
    for key in ("primaryOutcomes", "secondaryOutcomes"):
        for item in module.get(key, []) or []:
            if isinstance(item, dict):
                title = item.get("measure") or item.get("title")
                if title:
                    out.append(str(title).strip())
    return out


def _outcome_name_list_from_results(raw: dict[str, Any]) -> list[str]:
    module = raw.get("resultsSection", {}).get("outcomeMeasuresModule", {})
    out: list[str] = []
    for item in module.get("outcomeMeasures", []) or []:
        if isinstance(item, dict):
            title = item.get("title") or item.get("measure")
            if title:
                out.append(str(title).strip())
    return out


def _outcome_timeframe_map_protocol(raw: dict[str, Any]) -> dict[str, str]:
    module = raw.get("protocolSection", {}).get("outcomesModule", {})
    out: dict[str, str] = {}
    for key in ("primaryOutcomes", "secondaryOutcomes"):
        for item in module.get(key, []) or []:
            if isinstance(item, dict):
                name = item.get("measure") or item.get("title")
                timeframe = item.get("timeFrame") or item.get("timeframe")
                if name and timeframe:
                    out[str(name).strip().lower()] = str(timeframe).strip().lower()
    return out


def _outcome_timeframe_map_results(raw: dict[str, Any]) -> dict[str, str]:
    module = raw.get("resultsSection", {}).get("outcomeMeasuresModule", {})
    out: dict[str, str] = {}
    for item in module.get("outcomeMeasures", []) or []:
        if isinstance(item, dict):
            name = item.get("title") or item.get("measure")
            timeframe = item.get("timeFrame")
            if name and timeframe:
                out[str(name).strip().lower()] = str(timeframe).strip().lower()
    return out


def compute_publication_bias(
    trial_universe: list[TrialUniverseRecord],
    trial_publication_links: dict[str, list[str]],
    *,
    grace_months: int = 24,
    today: date | None = None,
) -> dict[str, Any]:
    today = today or date.today()
    completed = [t for t in trial_universe if (t.overall_status or "").upper() == "COMPLETED"]

    completed_with_results = [t for t in completed if t.has_results]
    completed_with_publication = [
        t for t in completed if trial_publication_links.get(t.trial_id)
    ]

    unreported_completed: list[TrialUniverseRecord] = []
    for trial in completed:
        if not trial.primary_completion_date:
            continue
        age_days = (today - trial.primary_completion_date).days
        if age_days < int(grace_months * 30.44):
            continue
        has_pub = bool(trial_publication_links.get(trial.trial_id))
        if (not trial.has_results) and (not has_pub):
            unreported_completed.append(trial)

    sponsor_counts: dict[str, dict[str, int]] = {}
    for trial in completed:
        sponsor = trial.sponsor_type or "unknown"
        bucket = sponsor_counts.setdefault(sponsor, {"completed": 0, "with_results": 0, "with_publication": 0})
        bucket["completed"] += 1
        if trial.has_results:
            bucket["with_results"] += 1
        if trial_publication_links.get(trial.trial_id):
            bucket["with_publication"] += 1

    enrollment_bins = {
        "small_lt_100": {"completed": 0, "with_results": 0},
        "mid_100_499": {"completed": 0, "with_results": 0},
        "large_ge_500": {"completed": 0, "with_results": 0},
        "unknown": {"completed": 0, "with_results": 0},
    }
    for trial in completed:
        n = _default_enrollment(trial.enrollment)
        if n == 0:
            key = "unknown"
        elif n < 100:
            key = "small_lt_100"
        elif n < 500:
            key = "mid_100_499"
        else:
            key = "large_ge_500"
        enrollment_bins[key]["completed"] += 1
        if trial.has_results:
            enrollment_bins[key]["with_results"] += 1

    completed_participants = sum(_default_enrollment(t.enrollment) for t in completed)
    post_2015_participants = sum(
        _default_enrollment(t.enrollment)
        for t in completed
        if t.primary_completion_date and t.primary_completion_date.year >= 2015
    )

    return {
        "completed_trials_count": len(completed),
        "completed_with_posted_results_count": len(completed_with_results),
        "completed_with_linked_publication_count": len(completed_with_publication),
        "unreported_completed_trials_count": len(unreported_completed),
        "results_posting_rate": _safe_ratio(len(completed_with_results), len(completed)),
        "publication_link_rate": _safe_ratio(len(completed_with_publication), len(completed)),
        "unreported_completed_rate": _safe_ratio(len(unreported_completed), len(completed)),
        "sponsor_strata": sponsor_counts,
        "enrollment_strata": enrollment_bins,
        "completed_participants": completed_participants,
        "post_2015_participants": post_2015_participants,
        "post_2015_participant_share": _safe_ratio(post_2015_participants, completed_participants),
    }


def compute_outcome_reporting_bias(
    trial_universe: list[TrialUniverseRecord],
    *,
    main_outcome_by_trial: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    trial_rows: list[dict[str, Any]] = []
    omission_total = 0
    protocol_total = 0
    timeframe_mismatch = 0
    primary_switching_unknown = 0

    for trial in trial_universe:
        protocol_outcomes = _outcome_name_list_from_protocol(trial.raw)
        reported_outcomes = _outcome_name_list_from_results(trial.raw)

        protocol_set = {x.lower() for x in protocol_outcomes}
        reported_set = {x.lower() for x in reported_outcomes}
        omitted = sorted(protocol_set - reported_set)

        protocol_total += len(protocol_set)
        omission_total += len(omitted)

        proto_time = _outcome_timeframe_map_protocol(trial.raw)
        rep_time = _outcome_timeframe_map_results(trial.raw)

        main = (main_outcome_by_trial or {}).get(trial.trial_id, {})
        main_name = str(main.get("outcome_name") or "").strip().lower()
        main_proto_time = proto_time.get(main_name)
        main_rep_time = rep_time.get(main_name)
        tf_mismatch = bool(main_proto_time and main_rep_time and main_proto_time != main_rep_time)
        if tf_mismatch:
            timeframe_mismatch += 1

        protocol_changes = trial.raw.get("protocolSection", {}).get("outcomesModule", {}).get("outcomesHaveChanged")
        if protocol_changes is None:
            primary_switching_unknown += 1

        trial_rows.append(
            {
                "trial_id": trial.trial_id,
                "prespecified_outcome_count": len(protocol_set),
                "reported_outcome_count": len(reported_set),
                "omitted_outcomes_count": len(omitted),
                "omitted_outcomes": omitted,
                "timeframe_mismatch_main_outcome": tf_mismatch,
                "primary_outcome_switching_flag": "unknown" if protocol_changes is None else bool(protocol_changes),
            }
        )

    return {
        "outcome_omission_rate": _safe_ratio(omission_total, protocol_total),
        "timeframe_mismatch_rate": _safe_ratio(timeframe_mismatch, len(trial_universe)),
        "primary_switching_unknown_rate": _safe_ratio(primary_switching_unknown, len(trial_universe)),
        "trial_level": trial_rows,
    }


def compute_evidence_coverage(
    trial_universe: list[TrialUniverseRecord],
    contributing_trial_ids: set[str],
) -> dict[str, Any]:
    completed = [t for t in trial_universe if (t.overall_status or "").upper() == "COMPLETED"]
    completed_ids = {t.trial_id for t in completed}
    contributing_completed_ids = completed_ids.intersection(contributing_trial_ids)

    comp_enrollment = sum(_default_enrollment(t.enrollment) for t in completed)
    contrib_enrollment = sum(
        _default_enrollment(t.enrollment) for t in completed if t.trial_id in contributing_completed_ids
    )

    return {
        "eligible_completed_trials": len(completed_ids),
        "contributing_trials": len(contributing_completed_ids),
        "ecr_trials": _safe_ratio(len(contributing_completed_ids), len(completed_ids)),
        "eligible_completed_participants": comp_enrollment,
        "contributing_participants": contrib_enrollment,
        "ecr_participants": _safe_ratio(contrib_enrollment, comp_enrollment),
    }


def apply_operational_cutoff(
    *,
    post_2015_participant_share: float,
    binary_main_outcome: bool,
    registered_trial_share: float,
    participant_weighted_coverage: float,
    results_posting_rate: float,
) -> dict[str, Any]:
    checks = {
        "post_2015_participant_share": post_2015_participant_share >= OPERATIONAL_CUTOFF_THRESHOLDS["post_2015_participant_share"],
        "binary_main_outcome_required": bool(binary_main_outcome) is OPERATIONAL_CUTOFF_THRESHOLDS["binary_main_outcome_required"],
        "registered_trial_share": registered_trial_share >= OPERATIONAL_CUTOFF_THRESHOLDS["registered_trial_share"],
        "participant_weighted_coverage": participant_weighted_coverage >= OPERATIONAL_CUTOFF_THRESHOLDS["participant_weighted_coverage"],
        "results_posting_rate": results_posting_rate >= OPERATIONAL_CUTOFF_THRESHOLDS["results_posting_rate"],
    }
    passed = all(checks.values())
    return {
        "thresholds": OPERATIONAL_CUTOFF_THRESHOLDS,
        "inputs": {
            "post_2015_participant_share": post_2015_participant_share,
            "binary_main_outcome": binary_main_outcome,
            "registered_trial_share": registered_trial_share,
            "participant_weighted_coverage": participant_weighted_coverage,
            "results_posting_rate": results_posting_rate,
        },
        "checks": checks,
        "decision": "RUN registry-first meta" if passed else "exclude or flag as non-compatible",
        "passed": passed,
    }


def build_transparency_profile(
    trial_universe: list[TrialUniverseRecord],
    trial_publication_links: dict[str, list[str]],
    contributing_trial_ids: set[str],
    *,
    main_outcome_by_trial: dict[str, dict[str, Any]],
    grace_months: int,
) -> dict[str, Any]:
    pub_bias = compute_publication_bias(
        trial_universe,
        trial_publication_links,
        grace_months=grace_months,
    )
    out_bias = compute_outcome_reporting_bias(
        trial_universe,
        main_outcome_by_trial=main_outcome_by_trial,
    )
    coverage = compute_evidence_coverage(trial_universe, contributing_trial_ids)

    registered_share = _safe_ratio(
        sum(1 for t in trial_universe if t.is_registered),
        len(trial_universe),
    )

    any_binary_main = any(
        bool(meta.get("is_binary_main_outcome")) for meta in main_outcome_by_trial.values()
    )

    cutoff = apply_operational_cutoff(
        post_2015_participant_share=pub_bias["post_2015_participant_share"],
        binary_main_outcome=any_binary_main,
        registered_trial_share=registered_share,
        participant_weighted_coverage=coverage["ecr_participants"],
        results_posting_rate=pub_bias["results_posting_rate"],
    )

    return {
        "publication_bias": pub_bias,
        "outcome_reporting_bias": out_bias,
        "coverage": coverage,
        "operational_cutoff": cutoff,
        "trial_universe_summary": [asdict(t) for t in trial_universe],
    }
