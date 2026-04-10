"""Deterministic cached HTTP helpers with retry/backoff."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import requests

from .config import USER_AGENT
from .normalize import stable_json_hash

LOGGER = logging.getLogger(__name__)


class DiskCache:
    """Simple file-based cache keyed by a stable hash."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, namespace: str, key: str, suffix: str) -> Path:
        ns_dir = self.root / namespace
        ns_dir.mkdir(parents=True, exist_ok=True)
        return ns_dir / f"{key}.{suffix}"

    def get_text(self, namespace: str, key: str) -> str | None:
        path = self._path(namespace, key, "txt")
        if not path.exists():
            return None
        return path.read_text(encoding="utf-8")

    def set_text(self, namespace: str, key: str, value: str) -> None:
        path = self._path(namespace, key, "txt")
        path.write_text(value, encoding="utf-8")

    def get_json(self, namespace: str, key: str) -> dict[str, Any] | list[Any] | None:
        path = self._path(namespace, key, "json")
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def set_json(self, namespace: str, key: str, value: dict[str, Any] | list[Any]) -> None:
        path = self._path(namespace, key, "json")
        path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


class CachedHttpClient:
    """HTTP client with deterministic disk cache and retry/backoff."""

    def __init__(
        self,
        cache_dir: Path,
        timeout: int = 30,
        max_retries: int = 4,
        backoff_seconds: float = 1.25,
    ) -> None:
        self.cache = DiskCache(cache_dir)
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    @staticmethod
    def _cache_key(url: str, params: dict[str, Any] | None = None) -> str:
        return stable_json_hash({"url": url, "params": params or {}})

    def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        namespace: str,
        use_cache: bool = True,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any] | list[Any]:
        key = self._cache_key(url, params)
        if use_cache:
            cached = self.cache.get_json(namespace, key)
            if cached is not None:
                return cached

        payload = self._request("GET", url, params=params, headers=headers)
        decoded = payload.json()
        if use_cache:
            self.cache.set_json(namespace, key, decoded)
        return decoded

    def get_text(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        namespace: str,
        use_cache: bool = True,
        headers: dict[str, str] | None = None,
    ) -> str:
        key = self._cache_key(url, params)
        if use_cache:
            cached = self.cache.get_text(namespace, key)
            if cached is not None:
                return cached

        payload = self._request("GET", url, params=params, headers=headers)
        text = payload.text
        if use_cache:
            self.cache.set_text(namespace, key, text)
        return text

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        transient_statuses = {429, 500, 502, 503, 504}
        last_exc: Exception | None = None
        last_response: requests.Response | None = None

        for attempt in range(self.max_retries):
            try:
                response = self.session.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                )
                last_response = response

                if response.status_code in transient_statuses:
                    if attempt >= self.max_retries - 1:
                        break
                    delay = self.backoff_seconds * (2**attempt)
                    LOGGER.warning("HTTP %s for %s; retrying in %.2fs", response.status_code, url, delay)
                    time.sleep(delay)
                    continue

                # Do not retry terminal client errors (e.g., 404 from Unpaywall DOI misses).
                if 400 <= response.status_code < 500:
                    response.raise_for_status()

                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last_exc = exc
                if isinstance(exc, requests.HTTPError):
                    status = exc.response.status_code if exc.response is not None else None
                    if status is not None and 400 <= status < 500 and status != 429:
                        raise
                if attempt >= self.max_retries - 1:
                    break
                delay = self.backoff_seconds * (2**attempt)
                LOGGER.warning("Request error for %s (%s); retrying in %.2fs", url, exc, delay)
                time.sleep(delay)

        if last_response is not None:
            last_response.raise_for_status()
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"Request failed without exception for {url}")
