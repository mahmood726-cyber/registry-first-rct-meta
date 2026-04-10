"""Crossref API helpers for review metadata enrichment."""

from __future__ import annotations

import logging
from typing import Any

from .config import CROSSREF_BASE
from .http import CachedHttpClient
from .normalize import normalize_doi

LOGGER = logging.getLogger(__name__)


class CrossrefClient:
    """Minimal Crossref client with deterministic cache."""

    def __init__(self, http: CachedHttpClient) -> None:
        self.http = http

    def lookup_doi(self, doi: str | None) -> dict[str, Any] | None:
        norm = normalize_doi(doi)
        if not norm:
            return None
        try:
            payload = self.http.get_json(
                f"{CROSSREF_BASE}/works/{norm}",
                namespace="crossref_work",
            )
        except Exception as exc:
            LOGGER.debug("Crossref lookup failed for %s: %s", norm, exc)
            return None

        if not isinstance(payload, dict):
            return None
        message = payload.get("message", {})
        if not isinstance(message, dict):
            return None

        titles = message.get("title") or []
        title = str(titles[0]).strip() if isinstance(titles, list) and titles else None
        subjects = [str(s).strip() for s in (message.get("subject") or []) if str(s).strip()]

        year: int | None = None
        issued = message.get("issued", {})
        if isinstance(issued, dict):
            parts = issued.get("date-parts") or []
            if isinstance(parts, list) and parts and isinstance(parts[0], list) and parts[0]:
                first = parts[0][0]
                if isinstance(first, int):
                    year = first

        return {
            "doi": norm,
            "title": title,
            "subjects": subjects,
            "published_year": year,
        }

