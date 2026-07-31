"""Serper (Google Search) client. 429 -> RateLimited, and only 429."""

from __future__ import annotations

import httpx

from ..errors import RateLimited
from ..ports import SearchResult

SERPER_URL = "https://google.serper.dev/search"


class SerperSearch:
    def __init__(
        self, api_key: str, client: httpx.Client | None = None, timeout: float = 30.0
    ) -> None:
        self._api_key = api_key
        self._client = client or httpx.Client(timeout=timeout)

    def search(self, query: str, count: int) -> list[SearchResult]:
        resp = self._client.post(
            SERPER_URL,
            json={"q": query, "num": count},
            headers={"X-API-KEY": self._api_key, "Content-Type": "application/json"},
        )
        if resp.status_code == 429:
            raise RateLimited(f"serper 429 for {query!r}")
        resp.raise_for_status()
        payload = resp.json()
        results = []
        for i, item in enumerate(payload.get("organic", [])[:count]):
            results.append(
                SearchResult(
                    rank=int(item.get("position", i + 1)),
                    title=str(item.get("title", "")),
                    snippet=str(item.get("snippet", "")),
                    url=str(item.get("link", "")),
                )
            )
        return results
