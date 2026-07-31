"""FakeSearch: canned results per query, programmable 429s."""

from __future__ import annotations

import threading

from ..errors import RateLimited
from ..ports import SearchResult


class FakeSearch:
    def __init__(self) -> None:
        self.results: dict[str, list[SearchResult]] = {}
        self.default_results: list[SearchResult] = []
        self.rate_limited_queries: set[str] = set()
        self.rate_limit_all = False
        self.error_queries: set[str] = set()  # raise a genuine (non-429) error
        self.calls: list[str] = []
        self._lock = threading.Lock()

    def canned(self, query: str, results: list[SearchResult]) -> None:
        self.results[query] = results

    def search(self, query: str, count: int) -> list[SearchResult]:
        with self._lock:
            self.calls.append(query)
            if self.rate_limit_all or query in self.rate_limited_queries:
                raise RateLimited(f"429 for {query!r}")
            if query in self.error_queries:
                raise RuntimeError(f"canned search error for {query!r}")
            return list(self.results.get(query, self.default_results))[:count]
