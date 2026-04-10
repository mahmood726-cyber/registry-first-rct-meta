"""OpenAlex API helper methods for identifier mapping."""

from __future__ import annotations

from typing import Any

from .config import OPENALEX_BASE
from .http import CachedHttpClient
from .normalize import normalize_doi


class OpenAlexClient:
    """OpenAlex lookup client."""

    def __init__(self, http: CachedHttpClient) -> None:
        self.http = http

    def get_work_by_pmid(self, pmid: str) -> dict[str, Any] | None:
        url = f"{OPENALEX_BASE}/works"
        payload = self.http.get_json(
            url,
            params={"filter": f"ids.pmid:https://pubmed.ncbi.nlm.nih.gov/{pmid}", "per-page": "1"},
            namespace="openalex_works",
        )
        results = payload.get("results", []) if isinstance(payload, dict) else []
        if not results:
            return None
        return results[0] if isinstance(results[0], dict) else None

    def get_work_by_doi(self, doi: str) -> dict[str, Any] | None:
        norm = normalize_doi(doi)
        if not norm:
            return None
        url = f"{OPENALEX_BASE}/works/https://doi.org/{norm}"
        payload = self.http.get_json(url, namespace="openalex_work_single")
        return payload if isinstance(payload, dict) else None

    def pmid_to_doi(self, pmid: str) -> str | None:
        work = self.get_work_by_pmid(pmid)
        if not work:
            return None
        doi_url = work.get("doi")
        return normalize_doi(str(doi_url)) if doi_url else None

    def pmid_to_openalex_id(self, pmid: str) -> str | None:
        work = self.get_work_by_pmid(pmid)
        if not work:
            return None
        work_id = work.get("id")
        return str(work_id) if work_id else None
