"""Regulatory-source metadata link-outs (no scraping, API-first only)."""

from __future__ import annotations

from typing import Any

from .http import CachedHttpClient

OPENFDA_BASE = "https://api.fda.gov"


class RegulatoryMetadataClient:
    """Metadata-only regulatory integrations.

    This module intentionally does not scrape non-API portals. It provides
    link-out and metadata retrieval where official API endpoints exist.
    """

    def __init__(self, http: CachedHttpClient) -> None:
        self.http = http

    def openfda_drug_labels(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        payload = self.http.get_json(
            f"{OPENFDA_BASE}/drug/label.json",
            params={"search": query, "limit": str(limit)},
            namespace="openfda_label",
        )
        results = payload.get("results", []) if isinstance(payload, dict) else []
        return [row for row in results if isinstance(row, dict)]

    @staticmethod
    def drugsatfda_search_url(term: str) -> str:
        return f"https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo={term}"

    @staticmethod
    def ema_ctis_link(term: str) -> str:
        return f"https://euclinicaltrials.eu/search-for-clinical-trials/?query={term}"
