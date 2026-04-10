"""PubMed E-utilities client helpers."""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from rapidfuzz import fuzz

from .config import PUBMED_BASE
from .http import CachedHttpClient
from .normalize import canonicalize_title, dedupe_list

LOGGER = logging.getLogger(__name__)


class PubMedClient:
    """Minimal PubMed client with deterministic caching."""

    def __init__(self, http: CachedHttpClient, *, api_key: str | None = None) -> None:
        self.http = http
        self.api_key = api_key or os.getenv("NCBI_API_KEY") or os.getenv("ENTREZ_API_KEY")
        self._last_request_ts = 0.0
        # NCBI E-utilities baseline rates: 3 req/s without key, 10 req/s with key.
        self._min_interval_seconds = 0.11 if self.api_key else 0.34

    def _wait_for_slot(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_ts
        if elapsed < self._min_interval_seconds:
            time.sleep(self._min_interval_seconds - elapsed)
        self._last_request_ts = time.monotonic()

    def _params(self, extra: dict[str, Any]) -> dict[str, Any]:
        params = dict(extra)
        if self.api_key:
            params["api_key"] = self.api_key
        return params

    def search(self, query: str, retmax: int = 20) -> list[str]:
        url = f"{PUBMED_BASE}/esearch.fcgi"
        self._wait_for_slot()
        payload = self.http.get_json(
            url,
            params=self._params(
                {
                "db": "pubmed",
                "term": query,
                "retmode": "json",
                "retmax": str(retmax),
                }
            ),
            namespace="pubmed_esearch",
        )
        idlist = payload.get("esearchresult", {}).get("idlist", []) if isinstance(payload, dict) else []
        return [str(x) for x in idlist]

    def search_by_nct(self, nct_id: str, retmax: int = 20) -> list[str]:
        query = f"{nct_id}[si] OR {nct_id}[tiab] OR {nct_id}[tw]"
        return self.search(query=query, retmax=retmax)

    def fetch_summaries(self, pmids: list[str]) -> dict[str, dict[str, Any]]:
        if not pmids:
            return {}
        url = f"{PUBMED_BASE}/esummary.fcgi"
        self._wait_for_slot()
        payload = self.http.get_json(
            url,
            params=self._params(
                {
                "db": "pubmed",
                "retmode": "json",
                "id": ",".join(pmids),
                }
            ),
            namespace="pubmed_esummary",
        )
        result = payload.get("result", {}) if isinstance(payload, dict) else {}
        out: dict[str, dict[str, Any]] = {}
        for pmid in pmids:
            if pmid in result and isinstance(result[pmid], dict):
                out[pmid] = result[pmid]
        return out

    def search_by_title(self, title: str, retmax: int = 10, fuzzy_threshold: int = 88) -> list[str]:
        if not title.strip():
            return []

        exact_query = f'"{title}"[title]'
        pmids = self.search(exact_query, retmax=retmax)
        if not pmids:
            # Broad fallback when exact title index match fails.
            pmids = self.search(title, retmax=retmax)

        if not pmids:
            return []

        summaries = self.fetch_summaries(pmids)
        target = canonicalize_title(title)
        ranked: list[tuple[int, str]] = []
        for pmid, summary in summaries.items():
            candidate_title = str(summary.get("title", ""))
            score = fuzz.token_sort_ratio(target, canonicalize_title(candidate_title))
            ranked.append((score, pmid))

        ranked.sort(reverse=True)
        kept = [pmid for score, pmid in ranked if score >= fuzzy_threshold]
        return dedupe_list(kept)

    def title_to_pmid_best_effort(self, title: str) -> str | None:
        candidates = self.search_by_title(title=title, retmax=5)
        return candidates[0] if candidates else None
