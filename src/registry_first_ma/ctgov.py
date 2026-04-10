"""ClinicalTrials.gov v2 search and extraction helpers."""

from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any, Iterable

from dateutil import parser as date_parser

from .config import CTGOV_V2_BASE, MAIN_OUTCOME_HIERARCHY
from .http import CachedHttpClient
from .models import OutcomeRow, TrialUniverseRecord
from .normalize import dedupe_list, extract_dois, extract_nct_ids, extract_pmids, normalize_nct_id

LOGGER = logging.getLogger(__name__)

EVENT_KEYS = {
    "events",
    "event",
    "numevents",
    "numaffected",
    "participantswithvalue",
    "count",
    "value",
}
TOTAL_KEYS = {
    "total",
    "n",
    "numatrisk",
    "participantsanalyzed",
    "numsubjects",
    "denominator",
}
ARM_KEYS = {
    "groupid",
    "group",
    "arm",
    "armgrouplabel",
    "groupdescription",
    "title",
    "name",
    "groupname",
}
HR_POINT_KEYS = {
    "hazardratio",
    "hr",
    "pointestimate",
    "estimate",
    "value",
}
CI_LOW_KEYS = {
    "cilowerlimit",
    "ci95lower",
    "lowerci",
    "lowerlimit",
}
CI_HIGH_KEYS = {
    "ciupperlimit",
    "ci95upper",
    "upperci",
    "upperlimit",
}


def _nested_get(obj: dict[str, Any], *path: str) -> Any:
    cur: Any = obj
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
        if cur is None:
            return None
    return cur


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, dict):
        value = value.get("date") or value.get("value")
    if not value:
        return None
    try:
        return date_parser.parse(str(value)).date()
    except (TypeError, ValueError, OverflowError):
        return None


def _walk_dicts(obj: Any) -> Iterable[dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _walk_dicts(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_dicts(item)


def _normalize_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _parse_timeframe_to_days(text: str | None) -> int | None:
    if not text:
        return None
    low = text.lower()
    m = re.search(r"(\d+(?:\.\d+)?)\s*(day|days|week|weeks|month|months|year|years)", low)
    if not m:
        return None
    val = float(m.group(1))
    unit = m.group(2)
    if "day" in unit:
        return int(round(val))
    if "week" in unit:
        return int(round(val * 7))
    if "month" in unit:
        return int(round(val * 30.44))
    if "year" in unit:
        return int(round(val * 365.25))
    return None


def _is_binary_outcome_name(name: str | None) -> bool:
    if not name:
        return False
    low = name.lower()
    binary_markers = [
        "mortality",
        "death",
        "mace",
        "stroke",
        "myocardial infarction",
        "hospitalization",
        "hospitalisation",
        "participants with",
        "serious adverse",
        "adverse event",
        "event",
    ]
    return any(marker in low for marker in binary_markers)


def _is_survival_outcome_name(name: str | None) -> bool:
    if not name:
        return False
    low = name.lower()
    markers = [
        "overall survival",
        "progression free survival",
        "event free survival",
        "time to event",
        "hazard ratio",
        "hazard",
    ]
    return any(marker in low for marker in markers)


def _best_arm_counts(obj: dict[str, Any]) -> tuple[str | None, int | None, int | None]:
    arm: str | None = None
    events: int | None = None
    total: int | None = None
    for key, value in obj.items():
        norm = _normalize_key(key)
        if arm is None and norm in ARM_KEYS and value not in (None, ""):
            arm = str(value)
        if events is None and norm in EVENT_KEYS:
            events = _to_int(value)
        if total is None and norm in TOTAL_KEYS:
            total = _to_int(value)
    if events is not None and total is not None and events > total:
        return arm, None, None
    return arm, events, total


def _timeframe_distance_days(outcome_time: str | None, target_time: str | None) -> int:
    """Smaller is better; unknown target prefers longest available follow-up."""
    out_days = _parse_timeframe_to_days(outcome_time)
    target_days = _parse_timeframe_to_days(target_time)
    if target_days is None:
        return -1 * (out_days or 0)
    if out_days is None:
        return 10_000_000
    return abs(out_days - target_days)


def _parse_ci_text(text: str) -> tuple[float | None, float | None]:
    m = re.search(r"([0-9]*\.?[0-9]+)\s*(?:to|,|-)\s*([0-9]*\.?[0-9]+)", text)
    if not m:
        return None, None
    return _to_float(m.group(1)), _to_float(m.group(2))


def _extract_hr_from_node(node: dict[str, Any]) -> tuple[float | None, float | None, float | None]:
    est: float | None = None
    ci_low: float | None = None
    ci_high: float | None = None
    for key, value in node.items():
        norm = _normalize_key(key)
        if norm in HR_POINT_KEYS:
            est = _to_float(value)
        elif norm in CI_LOW_KEYS:
            ci_low = _to_float(value)
        elif norm in CI_HIGH_KEYS:
            ci_high = _to_float(value)
        elif isinstance(value, str):
            low = value.lower()
            if "hazard" in low and est is None:
                # Example: "Hazard Ratio 0.82 (95% CI 0.70 to 0.96)"
                nums = re.findall(r"[0-9]*\.?[0-9]+", value)
                if nums:
                    est = _to_float(nums[0])
                lo, hi = _parse_ci_text(value)
                ci_low = ci_low if ci_low is not None else lo
                ci_high = ci_high if ci_high is not None else hi
            elif ("ci" in low or "confidence interval" in low) and (ci_low is None or ci_high is None):
                lo, hi = _parse_ci_text(value)
                ci_low = ci_low if ci_low is not None else lo
                ci_high = ci_high if ci_high is not None else hi
    if est is None:
        return None, None, None
    return est, ci_low, ci_high


class ClinicalTrialsGovClient:
    """ClinicalTrials.gov v2 API client with extraction helpers."""

    def __init__(self, http: CachedHttpClient) -> None:
        self.http = http

    def search_studies(self, query_term: str, cap_ncts: int = 500, page_size: int = 100) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        next_token: str | None = None

        while True:
            params = {
                "query.term": query_term,
                "pageSize": str(page_size),
                "format": "json",
            }
            if next_token:
                params["pageToken"] = next_token

            payload = self.http.get_json(
                f"{CTGOV_V2_BASE}/studies",
                params=params,
                namespace="ctgov_search",
            )
            studies = payload.get("studies", []) if isinstance(payload, dict) else []
            out.extend([s for s in studies if isinstance(s, dict)])

            if len(out) >= cap_ncts:
                return out[:cap_ncts]

            next_token = payload.get("nextPageToken") if isinstance(payload, dict) else None
            if not next_token:
                break

        return out

    def trial_id(self, study: dict[str, Any]) -> str | None:
        nct = _nested_get(study, "protocolSection", "identificationModule", "nctId")
        return normalize_nct_id(str(nct)) if nct else None

    def is_completed(self, study: dict[str, Any]) -> bool:
        status = _nested_get(study, "protocolSection", "statusModule", "overallStatus")
        return str(status).upper() == "COMPLETED"

    def has_results(self, study: dict[str, Any]) -> bool:
        return bool(study.get("resultsSection"))

    def completion_date(self, study: dict[str, Any]) -> date | None:
        return _to_date(
            _nested_get(study, "protocolSection", "statusModule", "primaryCompletionDateStruct")
            or _nested_get(study, "protocolSection", "statusModule", "completionDateStruct")
        )

    def start_date(self, study: dict[str, Any]) -> date | None:
        return _to_date(_nested_get(study, "protocolSection", "statusModule", "startDateStruct"))

    def enrollment(self, study: dict[str, Any]) -> int | None:
        return _to_int(_nested_get(study, "protocolSection", "designModule", "enrollmentInfo", "count"))

    def sponsor_type(self, study: dict[str, Any]) -> str | None:
        sponsor = _nested_get(study, "protocolSection", "sponsorCollaboratorsModule", "leadSponsor", "class")
        return str(sponsor).lower() if sponsor else None

    def study_type(self, study: dict[str, Any]) -> str | None:
        val = _nested_get(study, "protocolSection", "designModule", "studyType")
        return str(val) if val else None

    def allocation(self, study: dict[str, Any]) -> str | None:
        val = _nested_get(study, "protocolSection", "designModule", "designInfo", "allocation")
        return str(val) if val else None

    def trial_titles(self, study: dict[str, Any]) -> list[str]:
        ident = _nested_get(study, "protocolSection", "identificationModule") or {}
        if not isinstance(ident, dict):
            return []
        titles: list[str] = []
        for key in ("briefTitle", "officialTitle", "acronym"):
            val = ident.get(key)
            if isinstance(val, str) and val.strip():
                titles.append(val.strip())
        return dedupe_list(titles)

    def references_identifiers(self, study: dict[str, Any]) -> dict[str, list[str]]:
        refs = _nested_get(study, "protocolSection", "referencesModule", "references") or []
        if not isinstance(refs, list):
            refs = []

        blobs: list[str] = []
        for item in refs:
            if isinstance(item, dict):
                citation = item.get("citation")
                if citation:
                    blobs.append(str(citation))
                pmid = item.get("pmid")
                if pmid:
                    blobs.append(str(pmid))
                url = item.get("url")
                if url:
                    blobs.append(str(url))

        id_blob = "\n".join(blobs)
        return {
            "pmids": extract_pmids(id_blob),
            "nct_ids": extract_nct_ids(id_blob),
            "dois": extract_dois(id_blob),
        }

    def extract_universe_records(
        self,
        studies: list[dict[str, Any]],
        *,
        review_id: str,
        rct_only: bool = True,
    ) -> list[TrialUniverseRecord]:
        rows: list[TrialUniverseRecord] = []
        for study in studies:
            trial_id = self.trial_id(study)
            if not trial_id:
                continue

            study_type = self.study_type(study)
            allocation = self.allocation(study)

            if rct_only:
                if study_type and study_type.lower() != "interventional":
                    continue
                if allocation and "random" not in allocation.lower():
                    continue

            status = _nested_get(study, "protocolSection", "statusModule", "overallStatus")
            rows.append(
                TrialUniverseRecord(
                    review_id=review_id,
                    trial_id=trial_id,
                    overall_status=str(status) if status else None,
                    study_type=study_type,
                    allocation=allocation,
                    start_date=self.start_date(study),
                    primary_completion_date=self.completion_date(study),
                    enrollment=self.enrollment(study),
                    sponsor_type=self.sponsor_type(study),
                    has_results=self.has_results(study),
                    is_registered=True,
                    raw=study,
                )
            )

        unique: dict[str, TrialUniverseRecord] = {}
        for row in rows:
            unique[row.trial_id] = row
        return list(unique.values())

    def protocol_outcomes(self, study: dict[str, Any]) -> list[dict[str, Any]]:
        outcomes_module = _nested_get(study, "protocolSection", "outcomesModule") or {}
        primary = outcomes_module.get("primaryOutcomes", []) if isinstance(outcomes_module, dict) else []
        secondary = outcomes_module.get("secondaryOutcomes", []) if isinstance(outcomes_module, dict) else []
        all_outcomes: list[dict[str, Any]] = []
        for item in primary:
            if isinstance(item, dict):
                all_outcomes.append({**item, "kind": "primary"})
        for item in secondary:
            if isinstance(item, dict):
                all_outcomes.append({**item, "kind": "secondary"})
        return all_outcomes

    def reported_outcomes(self, study: dict[str, Any]) -> list[dict[str, Any]]:
        module = _nested_get(study, "resultsSection", "outcomeMeasuresModule") or {}
        raw = module.get("outcomeMeasures", []) if isinstance(module, dict) else []
        return [item for item in raw if isinstance(item, dict)]

    def choose_main_outcome(self, study: dict[str, Any]) -> dict[str, Any]:
        protocol_outcomes = self.protocol_outcomes(study)
        primary = [o for o in protocol_outcomes if o.get("kind") == "primary"]

        def _title(outcome: dict[str, Any]) -> str:
            return str(outcome.get("measure") or outcome.get("title") or "").strip()

        def _timeframe(outcome: dict[str, Any]) -> str | None:
            tf = outcome.get("timeFrame") or outcome.get("timeframe")
            return str(tf).strip() if tf else None

        chosen_title: str | None = None
        chosen_timeframe: str | None = None
        rule_path = "fallback:first_primary"

        for label, needles in MAIN_OUTCOME_HIERARCHY:
            matched = [o for o in primary if any(needle in _title(o).lower() for needle in needles)]
            if matched:
                matched.sort(key=lambda x: _parse_timeframe_to_days(_timeframe(x)) or -1, reverse=True)
                pick = matched[0]
                chosen_title = _title(pick)
                chosen_timeframe = _timeframe(pick)
                rule_path = f"hierarchy:{label}"
                break

        if not chosen_title and primary:
            primary_sorted = sorted(primary, key=lambda x: _parse_timeframe_to_days(_timeframe(x)) or -1, reverse=True)
            chosen_title = _title(primary_sorted[0])
            chosen_timeframe = _timeframe(primary_sorted[0])
            rule_path = "fallback:first_primary"

        if not chosen_title:
            reported = self.reported_outcomes(study)
            if reported:
                reported.sort(
                    key=lambda x: _parse_timeframe_to_days(str(x.get("timeFrame") or "")) or -1,
                    reverse=True,
                )
                chosen_title = str(reported[0].get("title") or "unspecified_outcome")
                chosen_timeframe = str(reported[0].get("timeFrame") or "") or None
                rule_path = "fallback:first_reported"

        return {
            "outcome_name": chosen_title or "unspecified_outcome",
            "timepoint": chosen_timeframe,
            "rule_path": rule_path,
            "is_binary_main_outcome": _is_binary_outcome_name(chosen_title),
        }

    @staticmethod
    def trial_url(trial_id: str) -> str:
        return f"https://clinicaltrials.gov/study/{trial_id}"

    def _match_outcome(self, title: str, target: str) -> bool:
        t1 = title.lower().strip()
        t2 = target.lower().strip()
        if not t1 or not t2:
            return False
        if t2 in t1 or t1 in t2:
            return True
        t1_tokens = set(re.findall(r"[a-z0-9]+", t1))
        t2_tokens = set(re.findall(r"[a-z0-9]+", t2))
        if not t1_tokens or not t2_tokens:
            return False
        overlap = len(t1_tokens & t2_tokens) / max(1, len(t2_tokens))
        return overlap >= 0.6

    def extract_binary_outcome_rows(
        self,
        study: dict[str, Any],
        *,
        review_id: str,
        trial_id: str,
        main_outcome_name: str,
        main_outcome_timepoint: str | None = None,
    ) -> list[OutcomeRow]:
        outcomes = self.reported_outcomes(study)
        matching = [
            outcome
            for outcome in outcomes
            if self._match_outcome(
                str(outcome.get("title") or outcome.get("measure") or ""),
                main_outcome_name,
            )
        ]
        if not matching:
            return [
                OutcomeRow(
                    review_id=review_id,
                    trial_id=trial_id,
                    outcome_name=main_outcome_name,
                    timepoint=main_outcome_timepoint,
                    arm="UNMATCHED",
                    events=None,
                    total=None,
                    measure_type="binary_unmatched",
                    source="clinicaltrials_gov",
                    provenance_link=self.trial_url(trial_id),
                    matched_main_outcome=False,
                )
            ]

        matching.sort(
            key=lambda x: _timeframe_distance_days(
                str(x.get("timeFrame") or "") or None,
                main_outcome_timepoint,
            )
        )
        matching = [matching[0]]

        all_rows: list[OutcomeRow] = []
        for outcome in matching:
            title = str(outcome.get("title") or outcome.get("measure") or main_outcome_name)
            time_frame = str(outcome.get("timeFrame") or "") or None

            seen: set[tuple[str, int, int]] = set()
            extracted = 0
            for node in _walk_dicts(outcome):
                arm, events, total = _best_arm_counts(node)
                if arm and events is not None and total is not None:
                    key = (arm, events, total)
                    if key in seen:
                        continue
                    seen.add(key)
                    extracted += 1
                    all_rows.append(
                        OutcomeRow(
                            review_id=review_id,
                            trial_id=trial_id,
                            outcome_name=title,
                            timepoint=time_frame,
                            arm=arm,
                            events=events,
                            total=total,
                            measure_type="binary_main_outcome",
                            source="clinicaltrials_gov",
                            provenance_link=self.trial_url(trial_id),
                            matched_main_outcome=self._match_outcome(title, main_outcome_name),
                        )
                    )

            if extracted == 0:
                all_rows.append(
                    OutcomeRow(
                        review_id=review_id,
                        trial_id=trial_id,
                        outcome_name=title,
                        timepoint=time_frame,
                        arm="UNMATCHED",
                        events=None,
                        total=None,
                        measure_type="binary_unmatched",
                        source="clinicaltrials_gov",
                        provenance_link=self.trial_url(trial_id),
                        matched_main_outcome=self._match_outcome(title, main_outcome_name),
                    )
                )

        return all_rows

    def extract_hr_rows(
        self,
        study: dict[str, Any],
        *,
        review_id: str,
        trial_id: str,
        main_outcome_name: str,
        main_outcome_timepoint: str | None = None,
    ) -> list[OutcomeRow]:
        outcomes = self.reported_outcomes(study)
        if not outcomes:
            return []

        main_survival = _is_survival_outcome_name(main_outcome_name)
        candidates = [
            o
            for o in outcomes
            if _is_survival_outcome_name(str(o.get("title") or o.get("measure") or ""))
            or (
                main_survival
                and self._match_outcome(str(o.get("title") or o.get("measure") or ""), main_outcome_name)
            )
        ]
        if not candidates:
            return []

        candidates.sort(
            key=lambda x: _timeframe_distance_days(
                str(x.get("timeFrame") or "") or None,
                main_outcome_timepoint,
            )
        )

        rows: list[OutcomeRow] = []
        for outcome in candidates[:1]:
            title = str(outcome.get("title") or outcome.get("measure") or main_outcome_name)
            timeframe = str(outcome.get("timeFrame") or "") or None
            best_est: float | None = None
            best_low: float | None = None
            best_high: float | None = None

            for node in _walk_dicts(outcome):
                blob = " ".join(f"{k} {v}" for k, v in node.items()).lower()
                if "hazard" not in blob:
                    continue
                est, low, high = _extract_hr_from_node(node)
                if est is not None and 0.05 <= est <= 20:
                    best_est = est
                    best_low = low if (low is None or 0.01 <= low <= 20) else None
                    best_high = high if (high is None or 0.01 <= high <= 20) else None
                    break

            rows.append(
                OutcomeRow(
                    review_id=review_id,
                    trial_id=trial_id,
                    outcome_name=title,
                    timepoint=timeframe,
                    arm="TRIAL_LEVEL",
                    events=None,
                    total=None,
                    measure_type="hazard_ratio" if best_est is not None else "hazard_ratio_unmatched",
                    source="clinicaltrials_gov",
                    provenance_link=self.trial_url(trial_id),
                    matched_main_outcome=self._match_outcome(title, main_outcome_name),
                    effect_estimate=best_est,
                    effect_ci_low=best_low,
                    effect_ci_high=best_high,
                    effect_metric="HR",
                )
            )
        return rows

    def extract_ae_rows(self, study: dict[str, Any], *, review_id: str, trial_id: str) -> list[OutcomeRow]:
        ae_module = _nested_get(study, "resultsSection", "adverseEventsModule") or {}
        if not isinstance(ae_module, dict):
            return []

        out: list[OutcomeRow] = []
        for section_name in ("seriousEvents", "otherEvents"):
            events = ae_module.get(section_name, [])
            if not isinstance(events, list):
                continue
            for event in events:
                if not isinstance(event, dict):
                    continue
                term = str(event.get("term") or event.get("title") or "adverse_event")
                timeframe = str(event.get("timeFrame") or "") or None
                seen: set[tuple[str, int, int]] = set()
                for node in _walk_dicts(event):
                    arm, num_aff, num_risk = _best_arm_counts(node)
                    if arm and num_aff is not None and num_risk is not None:
                        key = (arm, num_aff, num_risk)
                        if key in seen:
                            continue
                        seen.add(key)
                        out.append(
                            OutcomeRow(
                                review_id=review_id,
                                trial_id=trial_id,
                                outcome_name=f"AE:{term}",
                                timepoint=timeframe,
                                arm=arm,
                                events=num_aff,
                                total=num_risk,
                                measure_type="serious_ae" if section_name == "seriousEvents" else "other_ae",
                                source="clinicaltrials_gov",
                                provenance_link=self.trial_url(trial_id),
                                matched_main_outcome=False,
                            )
                        )

        return out

    def link_pmids_from_trial(self, study: dict[str, Any]) -> list[str]:
        refs = self.references_identifiers(study)
        return dedupe_list(refs.get("pmids", []))

    def link_dois_from_trial(self, study: dict[str, Any]) -> list[str]:
        refs = self.references_identifiers(study)
        return dedupe_list(refs.get("dois", []))
