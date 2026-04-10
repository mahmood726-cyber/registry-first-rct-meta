"""End-to-end registry-first engine for one review/topic."""

from __future__ import annotations

import logging
import re
import socket
from dataclasses import asdict
from typing import Any

import pandas as pd
from tqdm import tqdm

from .aact import AACTDatabaseClient
from .ctgov import ClinicalTrialsGovClient
from .europepmc import EuropePMCClient
from .gaps import build_gap_report
from .http import CachedHttpClient
from .meta import meta_analyze_binary, meta_analyze_effect_measure_rows, mnar_sensitivity
from .models import IncludedStudy, OutcomeRow, ReviewRecord, ReviewRunResult, TrialUniverseRecord
from .normalize import dedupe_list, normalize_identifier_set, keyword_terms
from .openalex import OpenAlexClient
from .pubmed import PubMedClient
from .transparency import build_transparency_profile
from .unpaywall import UnpaywallClient

LOGGER = logging.getLogger(__name__)


class RegistryFirstEngine:
    """Coordinates identifier linkage, extraction, transparency, and pooling."""

    def __init__(
        self,
        *,
        cache_dir: str,
        grace_months: int = 24,
        use_openalex: bool = True,
        use_unpaywall: bool = False,
        use_europepmc: bool = False,
        use_aact_fallback: bool = True,
        aact_env_file: str | None = None,
        ncbi_api_key: str | None = None,
    ) -> None:
        self.http = CachedHttpClient(cache_dir=cache_dir)
        self.ctgov = ClinicalTrialsGovClient(self.http)
        self.aact = AACTDatabaseClient(env_file=aact_env_file)
        self.pubmed = PubMedClient(self.http, api_key=ncbi_api_key)
        self.openalex = OpenAlexClient(self.http)
        self.unpaywall = UnpaywallClient(self.http)
        self.europepmc = EuropePMCClient(self.http)
        self.grace_months = grace_months
        self.use_openalex = use_openalex
        self.use_unpaywall = use_unpaywall
        self.use_europepmc = use_europepmc
        self.use_aact_fallback = use_aact_fallback
        self._ctgov_available: bool | None = None

    def _check_ctgov_available(self) -> bool:
        if self._ctgov_available is not None:
            return self._ctgov_available
        try:
            socket.gethostbyname("clinicaltrials.gov")
            self._ctgov_available = True
        except Exception:
            self._ctgov_available = False
        return self._ctgov_available

    @staticmethod
    def _derive_query_term(review: ReviewRecord) -> str:
        def _looks_like_review_id(text: str) -> bool:
            t = text.strip().lower()
            if not t:
                return False
            if re.fullmatch(r"cd\d{6}(?:_pub\d+)?", t):
                return True
            if re.fullmatch(r"10\.1002[/._-]14651858[._-]cd\d{6}(?:[._-]pub\d+)?", t):
                return True
            return False

        terms = []
        terms.extend(review.condition_terms)
        terms.extend(review.intervention_terms)
        if not terms:
            if review.review_title and not _looks_like_review_id(review.review_title):
                terms = keyword_terms([review.review_title], cap_terms=10)
        if not terms:
            title_texts = [s.citation_title for s in review.included_studies if s.citation_title]
            terms = keyword_terms(title_texts, cap_terms=10)
        if not terms:
            terms = ["randomized", "trial"]
        return " ".join(dict.fromkeys(terms))

    def _link_trial_identifiers(
        self,
        trial: TrialUniverseRecord,
        *,
        cap_seed_pmids: int = 20,
    ) -> dict[str, list[str]]:
        raw = trial.raw
        pmids = self.ctgov.link_pmids_from_trial(raw)
        dois = self.ctgov.link_dois_from_trial(raw)

        # PubMed linkage by NCT when references module is sparse.
        if len(pmids) < cap_seed_pmids:
            try:
                pmids.extend(self.pubmed.search_by_nct(trial.trial_id, retmax=max(5, cap_seed_pmids - len(pmids))))
            except Exception as exc:
                LOGGER.warning("PubMed linkage failed for %s: %s", trial.trial_id, exc)

        if self.use_openalex:
            for pmid in list(pmids):
                try:
                    doi = self.openalex.pmid_to_doi(pmid)
                    if doi:
                        dois.append(doi)
                except Exception:
                    continue

        return {
            "pmids": dedupe_list(pmids),
            "dois": dedupe_list(dois),
        }

    def _oa_availability(
        self,
        trial_links: dict[str, list[str]],
    ) -> set[str]:
        available: set[str] = set()

        if self.use_unpaywall:
            for doi in trial_links.get("dois", []):
                try:
                    payload = self.unpaywall.lookup_doi(doi)
                    if payload and self.unpaywall.best_oa_url(payload):
                        available.add(doi)
                except Exception:
                    continue

        if self.use_europepmc:
            for pmid in trial_links.get("pmids", []):
                try:
                    results = self.europepmc.search(f"EXT_ID:{pmid} AND SRC:MED")
                    if any(str(r.get("isOpenAccess", "")).upper() == "Y" for r in results):
                        available.add(pmid)
                except Exception:
                    continue

        return available

    def run_review(
        self,
        review: ReviewRecord,
        *,
        cap_ncts: int = 500,
        cap_seed_pmids: int = 20,
        main_outcome_only: bool = True,
        rct_filter_toggle: bool = True,
        measure: str = "RR",
    ) -> ReviewRunResult:
        query_term = self._derive_query_term(review)
        LOGGER.info("[%s] CT.gov query.term: %s", review.review_id, query_term)

        studies: list[dict[str, Any]]
        data_source = "ctgov"
        if not self._check_ctgov_available():
            if self.use_aact_fallback and self.aact.available():
                LOGGER.warning(
                    "[%s] CT.gov host is not resolvable. Using AACT fallback.",
                    review.review_id,
                )
                data_source = "aact"
                studies = []
            else:
                LOGGER.warning(
                    "[%s] CT.gov host is not resolvable in current environment. "
                    "Continuing with empty trial universe.",
                    review.review_id,
                )
                studies = []
        else:
            try:
                studies = self.ctgov.search_studies(query_term=query_term, cap_ncts=cap_ncts)
            except Exception as exc:
                if self.use_aact_fallback and self.aact.available():
                    LOGGER.warning(
                        "[%s] CT.gov query failed (%s). Using AACT fallback.",
                        review.review_id,
                        exc,
                    )
                    data_source = "aact"
                    studies = []
                else:
                    LOGGER.warning(
                        "[%s] CT.gov query failed (%s). Continuing with empty trial universe.",
                        review.review_id,
                        exc,
                    )
                    studies = []

        if data_source == "aact":
            trial_universe = self.aact.search_trial_universe(
                review_id=review.review_id,
                query_term=query_term,
                cap_ncts=cap_ncts,
                rct_only=rct_filter_toggle,
            )
        else:
            trial_universe = self.ctgov.extract_universe_records(
                studies,
                review_id=review.review_id,
                rct_only=rct_filter_toggle,
            )

        trial_publication_links: dict[str, list[str]] = {}
        oa_available_identifiers: set[str] = set()
        found_identifiers: set[str] = set()
        found_trial_titles: dict[str, list[str]] = {}
        main_outcome_by_trial: dict[str, dict[str, Any]] = {}
        results_rows: list[OutcomeRow] = []

        for trial in tqdm(trial_universe, desc=f"{review.review_id}: trials", leave=False):
            found_identifiers.add(trial.trial_id)

            if data_source == "aact":
                payload = self.aact.get_trial_payload(trial.trial_id, seed=trial.raw)
                trial.raw = payload
                titles = self.aact.trial_titles(payload)
                if titles:
                    found_trial_titles[trial.trial_id] = titles
                links = self.aact.link_identifiers(payload)
                if len(links["pmids"]) < cap_seed_pmids:
                    try:
                        links["pmids"].extend(
                            self.pubmed.search_by_nct(
                                trial.trial_id,
                                retmax=max(5, cap_seed_pmids - len(links["pmids"])),
                            )
                        )
                        links["pmids"] = dedupe_list(links["pmids"])
                    except Exception as exc:
                        LOGGER.warning("PubMed linkage failed for %s: %s", trial.trial_id, exc)
            else:
                titles = self.ctgov.trial_titles(trial.raw)
                if titles:
                    found_trial_titles[trial.trial_id] = titles
                links = self._link_trial_identifiers(trial, cap_seed_pmids=cap_seed_pmids)
            trial_publication_links[trial.trial_id] = dedupe_list(links["pmids"] + links["dois"])
            found_identifiers.update(normalize_identifier_set(trial_publication_links[trial.trial_id]))

            oa_available_identifiers.update(self._oa_availability(links))

            if data_source == "aact":
                main_meta = self.aact.choose_main_outcome(trial.raw)
            else:
                main_meta = self.ctgov.choose_main_outcome(trial.raw)
            main_outcome_by_trial[trial.trial_id] = main_meta

            if trial.has_results:
                if data_source == "aact":
                    main_rows = self.aact.extract_binary_outcome_rows(
                        trial.raw,
                        review_id=review.review_id,
                        trial_id=trial.trial_id,
                        main_outcome_name=main_meta["outcome_name"],
                    )
                else:
                    main_rows = self.ctgov.extract_binary_outcome_rows(
                        trial.raw,
                        review_id=review.review_id,
                        trial_id=trial.trial_id,
                        main_outcome_name=main_meta["outcome_name"],
                        main_outcome_timepoint=main_meta.get("timepoint"),
                    )
                results_rows.extend(main_rows)
                if data_source == "aact":
                    results_rows.extend(
                        self.aact.extract_hr_rows(
                            trial.raw,
                            review_id=review.review_id,
                            trial_id=trial.trial_id,
                            main_outcome_name=main_meta["outcome_name"],
                        )
                    )
                else:
                    results_rows.extend(
                        self.ctgov.extract_hr_rows(
                            trial.raw,
                            review_id=review.review_id,
                            trial_id=trial.trial_id,
                            main_outcome_name=main_meta["outcome_name"],
                            main_outcome_timepoint=main_meta.get("timepoint"),
                        )
                    )
                if not main_outcome_only:
                    if data_source == "aact":
                        results_rows.extend(
                            self.aact.extract_ae_rows(
                                trial.raw,
                                review_id=review.review_id,
                                trial_id=trial.trial_id,
                            )
                        )
                    else:
                        results_rows.extend(
                            self.ctgov.extract_ae_rows(
                                trial.raw,
                                review_id=review.review_id,
                                trial_id=trial.trial_id,
                            )
                        )

        results_df = pd.DataFrame([asdict(r) for r in results_rows])
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

        binary_main = results_df[
            (results_df["measure_type"].str.contains("binary", case=False, na=False))
            & (results_df["matched_main_outcome"].fillna(False))
            & (results_df["events"].notna())
            & (results_df["total"].notna())
        ]
        contributing_trial_ids = set(binary_main["trial_id"].unique())

        meta_observed = meta_analyze_binary(binary_main, measure=measure)
        hr_observed = meta_analyze_effect_measure_rows(results_df, effect_metric="HR")
        completed_trials = [t for t in trial_universe if (t.overall_status or "").upper() == "COMPLETED"]
        n_missing_completed = max(0, len(completed_trials) - len(contributing_trial_ids))

        sensitivity = mnar_sensitivity(
            meta_observed,
            study_effects=pd.DataFrame(meta_observed.get("study_effects", [])),
            n_missing_trials=n_missing_completed,
            measure=measure,
        )

        meta_pack = {
            "observed": meta_observed,
            "observed_hr": hr_observed,
            "mnar": sensitivity,
            "n_missing_completed_trials": n_missing_completed,
        }

        transparency_profile = build_transparency_profile(
            trial_universe=trial_universe,
            trial_publication_links=trial_publication_links,
            contributing_trial_ids=contributing_trial_ids,
            main_outcome_by_trial=main_outcome_by_trial,
            grace_months=self.grace_months,
        )

        gap_rows = build_gap_report(
            review_id=review.review_id,
            included_studies=review.included_studies,
            found_identifiers=found_identifiers,
            trial_universe=trial_universe,
            results_rows=results_df.to_dict(orient="records"),
            publication_links=trial_publication_links,
            oa_available_identifiers=oa_available_identifiers,
            found_trial_titles=found_trial_titles,
        )

        return ReviewRunResult(
            review=review,
            trial_universe=trial_universe,
            results_rows=results_rows,
            transparency_profile=transparency_profile,
            meta_pack=meta_pack,
            gap_rows=gap_rows,
            found_identifiers=found_identifiers,
            found_trial_titles=found_trial_titles,
        )

    def run_topic(
        self,
        topic_payload: dict[str, Any],
        *,
        cap_ncts: int = 500,
        cap_seed_pmids: int = 20,
        main_outcome_only: bool = True,
        rct_filter_toggle: bool = True,
        measure: str = "RR",
    ) -> ReviewRunResult:
        review = ReviewRecord(
            review_id=str(topic_payload.get("review_id")),
            review_title=str(topic_payload.get("review_title") or topic_payload.get("review_id")),
            year_range=str(topic_payload.get("year_range")) if topic_payload.get("year_range") else None,
            condition_terms=[str(x) for x in topic_payload.get("condition_terms", [])],
            intervention_terms=[str(x) for x in topic_payload.get("intervention_terms", [])],
            included_studies=[
                IncludedStudy(
                    study_id=str(s.get("study_id", f"study_{i + 1}")),
                    citation_title=s.get("citation_title"),
                    pmid=s.get("pmid"),
                    nct_id=s.get("nct_id"),
                    doi=s.get("doi"),
                    year=s.get("year"),
                    journal=s.get("journal"),
                )
                for i, s in enumerate(topic_payload.get("included_studies", []) or [])
                if isinstance(s, dict)
            ],
        )

        return self.run_review(
            review,
            cap_ncts=cap_ncts,
            cap_seed_pmids=cap_seed_pmids,
            main_outcome_only=main_outcome_only,
            rct_filter_toggle=rct_filter_toggle,
            measure=measure,
        )
