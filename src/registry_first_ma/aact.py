"""AACT database and snapshot helpers for registry-first fallback."""

from __future__ import annotations

import logging
import os
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from .config import MAIN_OUTCOME_HIERARCHY
from .models import OutcomeRow, TrialUniverseRecord
from .normalize import dedupe_list, extract_dois, normalize_nct_id, normalize_pmid

try:  # pragma: no cover - optional dependency in some environments
    import psycopg2
except Exception:  # pragma: no cover - optional dependency in some environments
    psycopg2 = None

LOGGER = logging.getLogger(__name__)


def _parse_env_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key and value and key not in os.environ:
            os.environ[key] = value


def _parse_timeframe_to_days(text: str | None) -> int | None:
    if not text:
        return None
    low = text.lower()
    m = re.search(r"(\d+(?:\.\d+)?)\s*(day|days|week|weeks|month|months|year|years)", low)
    if not m:
        return None
    value = float(m.group(1))
    unit = m.group(2)
    if "day" in unit:
        return int(round(value))
    if "week" in unit:
        return int(round(value * 7))
    if "month" in unit:
        return int(round(value * 30.44))
    if "year" in unit:
        return int(round(value * 365.25))
    return None


def _is_binary_param(param_type: str | None) -> bool:
    if not param_type:
        return False
    val = param_type.upper().strip()
    return val in {"NUMBER", "COUNT_OF_PARTICIPANTS", "COUNT_OF_UNITS"}


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
        "remission",
        "response",
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


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


class AACTSnapshot:
    """Lightweight helper for local AACT CSV extracts (offline mode)."""

    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)

    def load_table(self, name: str) -> pd.DataFrame:
        candidates = [self.root_dir / f"{name}.csv", self.root_dir / f"{name}.txt"]
        for path in candidates:
            if path.exists():
                sep = "|" if path.suffix.lower() == ".txt" else ","
                return pd.read_csv(path, sep=sep)
        raise FileNotFoundError(f"AACT table not found for {name} under {self.root_dir}")

    def studies(self) -> pd.DataFrame:
        return self.load_table("studies")


class AACTDatabaseClient:
    """AACT PostgreSQL client used as fallback when CT.gov API is constrained."""

    def __init__(
        self,
        *,
        host: str = "aact-db.ctti-clinicaltrials.org",
        port: int = 5432,
        database: str = "aact",
        user: str | None = None,
        password: str | None = None,
        connect_timeout: int = 12,
        env_file: str | Path | None = None,
    ) -> None:
        self.host = host
        self.port = int(port)
        self.database = database
        self.connect_timeout = int(connect_timeout)

        self._load_env_defaults(env_file)
        self.user = user or os.getenv("AACT_USER", "")
        self.password = password or os.getenv("AACT_PASSWORD", "")

        self._available_cache: bool | None = None
        self._payload_cache: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _candidate_env_files(env_file: str | Path | None) -> list[Path]:
        out: list[Path] = []
        if env_file:
            out.append(Path(env_file))

        env_override = os.getenv("AACT_ENV_FILE")
        if env_override:
            out.append(Path(env_override))

        home = Path.home()
        out.extend(
            [
                Path(".env"),
                home / "Downloads/ctgov-search-strategies/.env",
                home / "Downloads/ctgov-search-strategies_backup_20260114-110157/.env",
                home / "Downloads/ctgov-search-strategies_backup_20260113_193020/.env",
            ]
        )

        dedup: list[Path] = []
        seen: set[str] = set()
        for p in out:
            key = str(p)
            if key not in seen:
                seen.add(key)
                dedup.append(p)
        return dedup

    def _load_env_defaults(self, env_file: str | Path | None) -> None:
        for path in self._candidate_env_files(env_file):
            _parse_env_file(path)

    @property
    def configured(self) -> bool:
        return bool(self.user and self.password and psycopg2 is not None)

    @contextmanager
    def _connect(self) -> Iterator[Any]:
        if psycopg2 is None:
            raise RuntimeError("psycopg2 is not installed; cannot use AACT database fallback")
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
            connect_timeout=self.connect_timeout,
        )
        try:
            yield conn
        finally:
            conn.close()

    def available(self) -> bool:
        if self._available_cache is not None:
            return self._available_cache
        if not self.configured:
            self._available_cache = False
            return False
        try:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchone()
                cur.close()
            self._available_cache = True
        except Exception as exc:
            LOGGER.warning("AACT fallback unavailable: %s", exc)
            self._available_cache = False
        return self._available_cache

    @staticmethod
    def _patterns(query_term: str) -> list[str]:
        tokens = [tok for tok in re.findall(r"[a-zA-Z0-9]+", query_term.lower()) if len(tok) >= 4]
        ordered: list[str] = []
        seen: set[str] = set()
        for tok in tokens:
            if tok not in seen:
                seen.add(tok)
                ordered.append(tok)
        if not ordered:
            ordered = ["trial"]
        return [f"%{tok}%" for tok in ordered[:12]]

    def search_trial_universe(
        self,
        *,
        review_id: str,
        query_term: str,
        cap_ncts: int = 500,
        rct_only: bool = True,
    ) -> list[TrialUniverseRecord]:
        if not self.available():
            return []

        patterns = self._patterns(query_term)
        sql = """
            SELECT DISTINCT
                s.nct_id,
                s.overall_status,
                s.study_type,
                d.allocation,
                s.brief_title,
                s.official_title,
                s.start_date,
                s.primary_completion_date,
                s.enrollment,
                COALESCE(sp.agency_class, s.source_class, 'unknown') AS sponsor_type,
                (
                    EXISTS (SELECT 1 FROM ctgov.outcomes o WHERE o.nct_id = s.nct_id)
                    OR EXISTS (SELECT 1 FROM ctgov.reported_events re WHERE re.nct_id = s.nct_id)
                ) AS has_results
            FROM ctgov.studies s
            LEFT JOIN ctgov.designs d ON s.nct_id = d.nct_id
            LEFT JOIN ctgov.sponsors sp
              ON s.nct_id = sp.nct_id
             AND LOWER(COALESCE(sp.lead_or_collaborator, '')) = 'lead'
            WHERE (
                EXISTS (
                    SELECT 1
                    FROM ctgov.conditions c
                    WHERE c.nct_id = s.nct_id
                      AND c.name ILIKE ANY(%s)
                )
                OR EXISTS (
                    SELECT 1
                    FROM ctgov.interventions i
                    WHERE i.nct_id = s.nct_id
                      AND i.name ILIKE ANY(%s)
                )
                OR s.brief_title ILIKE ANY(%s)
                OR COALESCE(s.official_title, '') ILIKE ANY(%s)
            )
            AND (
                %s = FALSE
                OR UPPER(COALESCE(s.study_type, '')) = 'INTERVENTIONAL'
            )
            AND (
                %s = FALSE
                OR UPPER(COALESCE(d.allocation, '')) LIKE '%%RANDOM%%'
                OR d.allocation IS NULL
            )
            ORDER BY s.nct_id
            LIMIT %s
        """

        out: list[TrialUniverseRecord] = []
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(sql, (patterns, patterns, patterns, patterns, rct_only, rct_only, int(cap_ncts)))
            for row in cur.fetchall():
                nct_id = normalize_nct_id(row[0])
                if not nct_id:
                    continue
                out.append(
                    TrialUniverseRecord(
                        review_id=review_id,
                        trial_id=nct_id,
                        overall_status=str(row[1]) if row[1] else None,
                        study_type=str(row[2]) if row[2] else None,
                        allocation=str(row[3]) if row[3] else None,
                        start_date=row[6],
                        primary_completion_date=row[7],
                        enrollment=_coerce_int(row[8]),
                        sponsor_type=str(row[9]).lower() if row[9] else None,
                        has_results=bool(row[10]),
                        is_registered=True,
                        raw={
                            "source": "aact",
                            "nct_id": nct_id,
                            "brief_title": str(row[4]).strip() if row[4] else None,
                            "official_title": str(row[5]).strip() if row[5] else None,
                        },
                    )
                )
            cur.close()

        return out

    def _fetch_payload(self, nct_id: str) -> dict[str, Any]:
        sql_meta = """
            SELECT brief_title, official_title
            FROM ctgov.studies
            WHERE nct_id = %s
            LIMIT 1
        """
        sql_outcomes = """
            SELECT id, outcome_type, title, time_frame, units, param_type
            FROM ctgov.outcomes
            WHERE nct_id = %s
        """
        sql_counts = """
            SELECT outcome_id, result_group_id, ctgov_group_code, scope, units, count
            FROM ctgov.outcome_counts
            WHERE nct_id = %s
        """
        sql_groups = """
            SELECT id, result_type, title, ctgov_group_code, outcome_id
            FROM ctgov.result_groups
            WHERE nct_id = %s
        """
        sql_baseline_counts = """
            SELECT result_group_id, ctgov_group_code, scope, units, count
            FROM ctgov.baseline_counts
            WHERE nct_id = %s
        """
        sql_events = """
            SELECT result_group_id, ctgov_group_code, time_frame, event_type,
                   subjects_affected, subjects_at_risk, organ_system, adverse_event_term
            FROM ctgov.reported_events
            WHERE nct_id = %s
        """
        sql_refs = """
            SELECT pmid, citation
            FROM ctgov.study_references
            WHERE nct_id = %s
        """

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(sql_meta, (nct_id,))
            meta_row = cur.fetchone()
            brief_title = str(meta_row[0]).strip() if meta_row and meta_row[0] else None
            official_title = str(meta_row[1]).strip() if meta_row and meta_row[1] else None

            cur.execute(sql_outcomes, (nct_id,))
            outcomes = [
                {
                    "id": row[0],
                    "outcome_type": row[1],
                    "title": row[2],
                    "time_frame": row[3],
                    "units": row[4],
                    "param_type": row[5],
                }
                for row in cur.fetchall()
            ]

            cur.execute(sql_counts, (nct_id,))
            outcome_counts = [
                {
                    "outcome_id": row[0],
                    "result_group_id": row[1],
                    "ctgov_group_code": row[2],
                    "scope": row[3],
                    "units": row[4],
                    "count": row[5],
                }
                for row in cur.fetchall()
            ]

            cur.execute(sql_groups, (nct_id,))
            result_groups = [
                {
                    "id": row[0],
                    "result_type": row[1],
                    "title": row[2],
                    "ctgov_group_code": row[3],
                    "outcome_id": row[4],
                }
                for row in cur.fetchall()
            ]

            cur.execute(sql_baseline_counts, (nct_id,))
            baseline_counts = [
                {
                    "result_group_id": row[0],
                    "ctgov_group_code": row[1],
                    "scope": row[2],
                    "units": row[3],
                    "count": row[4],
                }
                for row in cur.fetchall()
            ]

            cur.execute(sql_events, (nct_id,))
            reported_events = [
                {
                    "result_group_id": row[0],
                    "ctgov_group_code": row[1],
                    "time_frame": row[2],
                    "event_type": row[3],
                    "subjects_affected": row[4],
                    "subjects_at_risk": row[5],
                    "organ_system": row[6],
                    "adverse_event_term": row[7],
                }
                for row in cur.fetchall()
            ]

            cur.execute(sql_refs, (nct_id,))
            references = [
                {
                    "pmid": row[0],
                    "citation": row[1],
                }
                for row in cur.fetchall()
            ]
            cur.close()

        return {
            "source": "aact",
            "nct_id": nct_id,
            "brief_title": brief_title,
            "official_title": official_title,
            "outcomes": outcomes,
            "outcome_counts": outcome_counts,
            "result_groups": result_groups,
            "baseline_counts": baseline_counts,
            "reported_events": reported_events,
            "references": references,
        }

    def get_trial_payload(self, nct_id: str, *, seed: dict[str, Any] | None = None) -> dict[str, Any]:
        norm = normalize_nct_id(nct_id)
        if not norm:
            return seed or {}
        if norm in self._payload_cache:
            return self._payload_cache[norm]

        payload = self._fetch_payload(norm)
        if seed:
            payload.update(seed)
        self._payload_cache[norm] = payload
        return payload

    def link_identifiers(self, payload: dict[str, Any]) -> dict[str, list[str]]:
        pmids: list[str] = []
        dois: list[str] = []

        for ref in payload.get("references", []):
            if not isinstance(ref, dict):
                continue
            pmid = normalize_pmid(ref.get("pmid"))
            if pmid:
                pmids.append(pmid)
            citation = str(ref.get("citation") or "")
            dois.extend(extract_dois(citation))

        return {
            "pmids": dedupe_list(pmids),
            "dois": dedupe_list(dois),
        }

    @staticmethod
    def trial_url(trial_id: str) -> str:
        return f"https://clinicaltrials.gov/study/{trial_id}"

    @staticmethod
    def trial_titles(payload: dict[str, Any]) -> list[str]:
        out: list[str] = []
        for key in ("brief_title", "official_title"):
            val = payload.get(key)
            if isinstance(val, str) and val.strip():
                out.append(val.strip())
        return dedupe_list(out)

    def choose_main_outcome(self, payload: dict[str, Any]) -> dict[str, Any]:
        outcomes = [o for o in payload.get("outcomes", []) if isinstance(o, dict)]
        primary = [o for o in outcomes if str(o.get("outcome_type", "")).upper() == "PRIMARY"]

        def _title(o: dict[str, Any]) -> str:
            return str(o.get("title") or "").strip()

        def _time(o: dict[str, Any]) -> str | None:
            t = o.get("time_frame")
            return str(t).strip() if t else None

        chosen: dict[str, Any] | None = None
        rule_path = "fallback:first_primary"

        for label, needles in MAIN_OUTCOME_HIERARCHY:
            matched = [o for o in primary if any(n in _title(o).lower() for n in needles)]
            if matched:
                matched.sort(key=lambda o: _parse_timeframe_to_days(_time(o)) or -1, reverse=True)
                chosen = matched[0]
                rule_path = f"hierarchy:{label}"
                break

        if chosen is None and primary:
            primary.sort(key=lambda o: _parse_timeframe_to_days(_time(o)) or -1, reverse=True)
            chosen = primary[0]

        if chosen is None and outcomes:
            chosen = outcomes[0]
            rule_path = "fallback:first_outcome"

        name = _title(chosen or {}) or "unspecified_outcome"
        return {
            "outcome_name": name,
            "timepoint": _time(chosen or {}),
            "rule_path": rule_path,
            "is_binary_main_outcome": _is_binary_outcome_name(name)
            or _is_binary_param(str((chosen or {}).get("param_type") or "")),
        }

    @staticmethod
    def _match_outcome(title: str, target: str) -> bool:
        t1 = title.lower().strip()
        t2 = target.lower().strip()
        if not t1 or not t2:
            return False
        if t1 in t2 or t2 in t1:
            return True
        tok1 = set(re.findall(r"[a-z0-9]+", t1))
        tok2 = set(re.findall(r"[a-z0-9]+", t2))
        if not tok1 or not tok2:
            return False
        overlap = len(tok1 & tok2) / max(1, len(tok2))
        return overlap >= 0.6

    def extract_binary_outcome_rows(
        self,
        payload: dict[str, Any],
        *,
        review_id: str,
        trial_id: str,
        main_outcome_name: str,
    ) -> list[OutcomeRow]:
        outcomes = [o for o in payload.get("outcomes", []) if isinstance(o, dict)]
        outcome_counts = [o for o in payload.get("outcome_counts", []) if isinstance(o, dict)]
        result_groups = [g for g in payload.get("result_groups", []) if isinstance(g, dict)]
        baseline_counts = [b for b in payload.get("baseline_counts", []) if isinstance(b, dict)]

        groups_by_id = {g.get("id"): g for g in result_groups}

        baseline_title_totals: dict[str, int] = {}
        baseline_groups = {
            g.get("id"): str(g.get("title") or "")
            for g in result_groups
            if str(g.get("result_type") or "").lower() == "baseline"
        }
        for b in baseline_counts:
            rid = b.get("result_group_id")
            title = baseline_groups.get(rid)
            count = _coerce_int(b.get("count"))
            if title and count is not None:
                key = title.strip().lower()
                if key not in baseline_title_totals or baseline_title_totals[key] < count:
                    baseline_title_totals[key] = count

        matching_outcomes = [
            o
            for o in outcomes
            if self._match_outcome(str(o.get("title") or ""), main_outcome_name)
            and (_is_binary_param(str(o.get("param_type") or "")) or _is_binary_outcome_name(str(o.get("title") or "")))
        ]
        if not matching_outcomes:
            matching_outcomes = [
                o
                for o in outcomes
                if str(o.get("outcome_type", "")).upper() == "PRIMARY"
                and (_is_binary_param(str(o.get("param_type") or "")) or _is_binary_outcome_name(str(o.get("title") or "")))
            ]

        rows: list[OutcomeRow] = []
        for outcome in matching_outcomes:
            out_id = outcome.get("id")
            out_title = str(outcome.get("title") or main_outcome_name)
            timeframe = str(outcome.get("time_frame") or "") or None
            sub_counts = [c for c in outcome_counts if c.get("outcome_id") == out_id]
            seen_arms: set[str] = set()
            for c in sub_counts:
                group = groups_by_id.get(c.get("result_group_id"), {})
                arm = str(group.get("title") or "").strip() or str(c.get("ctgov_group_code") or "").strip()
                if not arm or arm in seen_arms:
                    continue
                seen_arms.add(arm)
                events = _coerce_int(c.get("count"))
                total = baseline_title_totals.get(arm.lower())
                if events is None or total is None or total < events:
                    rows.append(
                        OutcomeRow(
                            review_id=review_id,
                            trial_id=trial_id,
                            outcome_name=out_title,
                            timepoint=timeframe,
                            arm=arm or "UNMATCHED",
                            events=None,
                            total=None,
                            measure_type="binary_unmatched",
                            source="aact",
                            provenance_link=self.trial_url(trial_id),
                            matched_main_outcome=self._match_outcome(out_title, main_outcome_name),
                        )
                    )
                else:
                    rows.append(
                        OutcomeRow(
                            review_id=review_id,
                            trial_id=trial_id,
                            outcome_name=out_title,
                            timepoint=timeframe,
                            arm=arm,
                            events=events,
                            total=total,
                            measure_type="binary_main_outcome",
                            source="aact",
                            provenance_link=self.trial_url(trial_id),
                            matched_main_outcome=self._match_outcome(out_title, main_outcome_name),
                        )
                    )

        if not rows:
            rows.append(
                OutcomeRow(
                    review_id=review_id,
                    trial_id=trial_id,
                    outcome_name=main_outcome_name,
                    timepoint=None,
                    arm="UNMATCHED",
                    events=None,
                    total=None,
                    measure_type="binary_unmatched",
                    source="aact",
                    provenance_link=self.trial_url(trial_id),
                    matched_main_outcome=False,
                )
            )

        return rows

    def extract_hr_rows(
        self,
        payload: dict[str, Any],
        *,
        review_id: str,
        trial_id: str,
        main_outcome_name: str,
    ) -> list[OutcomeRow]:
        outcomes = [o for o in payload.get("outcomes", []) if isinstance(o, dict)]
        main_survival = _is_survival_outcome_name(main_outcome_name)
        candidates = [
            o
            for o in outcomes
            if _is_survival_outcome_name(str(o.get("title") or ""))
            or (main_survival and self._match_outcome(str(o.get("title") or ""), main_outcome_name))
        ]
        if not candidates:
            return []

        chosen = candidates[0]
        title = str(chosen.get("title") or main_outcome_name)
        timeframe = str(chosen.get("time_frame") or "") or None
        return [
            OutcomeRow(
                review_id=review_id,
                trial_id=trial_id,
                outcome_name=title,
                timepoint=timeframe,
                arm="TRIAL_LEVEL",
                events=None,
                total=None,
                measure_type="hazard_ratio_unmatched",
                source="aact",
                provenance_link=self.trial_url(trial_id),
                matched_main_outcome=self._match_outcome(title, main_outcome_name),
                effect_metric="HR",
            )
        ]

    def extract_ae_rows(
        self,
        payload: dict[str, Any],
        *,
        review_id: str,
        trial_id: str,
    ) -> list[OutcomeRow]:
        events = [e for e in payload.get("reported_events", []) if isinstance(e, dict)]
        groups_by_id = {
            g.get("id"): str(g.get("title") or "")
            for g in payload.get("result_groups", [])
            if isinstance(g, dict)
        }

        out: list[OutcomeRow] = []
        seen: set[tuple[str, str, int, int]] = set()
        for e in events:
            arm = groups_by_id.get(e.get("result_group_id"), "") or str(e.get("ctgov_group_code") or "")
            arm = arm.strip() or "UNKNOWN_ARM"
            affected = _coerce_int(e.get("subjects_affected"))
            at_risk = _coerce_int(e.get("subjects_at_risk"))
            if affected is None or at_risk is None or at_risk < affected:
                continue
            term = str(e.get("adverse_event_term") or "adverse_event")
            tf = str(e.get("time_frame") or "") or None
            ev_type = str(e.get("event_type") or "other")
            key = (arm, term, affected, at_risk)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                OutcomeRow(
                    review_id=review_id,
                    trial_id=trial_id,
                    outcome_name=f"AE:{term}",
                    timepoint=tf,
                    arm=arm,
                    events=affected,
                    total=at_risk,
                    measure_type="serious_ae" if ev_type.lower() == "serious" else "other_ae",
                    source="aact",
                    provenance_link=self.trial_url(trial_id),
                    matched_main_outcome=False,
                )
            )

        return out
