"""Unpaywall API helper (optional OA discovery)."""

from __future__ import annotations

import os
from typing import Any

from .config import UNPAYWALL_BASE
from .http import CachedHttpClient
from .normalize import normalize_doi


class UnpaywallClient:
    """Unpaywall metadata lookup by DOI."""

    def __init__(self, http: CachedHttpClient, email: str | None = None) -> None:
        self.http = http
        self.email = email or os.getenv("UNPAYWALL_EMAIL", "registry.first.meta@example.org")

    def lookup_doi(self, doi: str) -> dict[str, Any] | None:
        norm = normalize_doi(doi)
        if not norm:
            return None
        url = f"{UNPAYWALL_BASE}/{norm}"
        payload = self.http.get_json(
            url,
            params={"email": self.email},
            namespace="unpaywall",
        )
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def best_oa_url(payload: dict[str, Any] | None) -> str | None:
        if not payload:
            return None
        best = payload.get("best_oa_location")
        if isinstance(best, dict):
            url = best.get("url_for_pdf") or best.get("url")
            if url:
                return str(url)
        return None
