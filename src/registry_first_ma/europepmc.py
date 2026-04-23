"""Europe PMC helper for OA metadata/full-text endpoints."""

from __future__ import annotations

from urllib.parse import urljoin
from typing import Any

from .config import EUROPEPMC_BASE
from .http import CachedHttpClient


class EuropePMCClient:
    """Europe PMC search and OA full-text lookup client."""

    def __init__(self, http: CachedHttpClient) -> None:
        self.http = http

    def search(self, query: str, page_size: int = 25) -> list[dict[str, Any]]:
        url = f"{EUROPEPMC_BASE}/search"
        payload = self.http.get_json(
            url,
            params={"query": query, "format": "json", "pageSize": str(page_size)},
            namespace="europepmc_search",
        )
        result_list = payload.get("resultList", {}).get("result", []) if isinstance(payload, dict) else []
        return [item for item in result_list if isinstance(item, dict)]

    def full_text_xml(self, pmcid: str) -> str | None:
        if not pmcid:
            return None
        url = f"{EUROPEPMC_BASE}/{pmcid}/fullTextXML"
        try:
            return self.http.get_text(url, namespace="europepmc_fulltext")
        except Exception:
            return None

    @staticmethod
    def best_pdf_url(result: dict[str, Any] | None) -> str | None:
        if not result:
            return None
        if isinstance(result.get("fullTextUrlList"), dict):
            full_text_urls = result["fullTextUrlList"].get("fullTextUrl", [])
            if isinstance(full_text_urls, dict):
                full_text_urls = [full_text_urls]
            for item in full_text_urls:
                if not isinstance(item, dict):
                    continue
                url = item.get("url")
                if isinstance(url, str) and ".pdf" in url.lower():
                    return url
        pmcid = result.get("pmcid")
        has_pdf = str(result.get("hasPDF") or "").upper() == "Y"
        if has_pdf and pmcid:
            return urljoin("https://europepmc.org/articles/", f"{pmcid}?pdf=render")
        return None
