"""FakeFetcher: URL -> (status, bytes, content_type) fixtures."""

from __future__ import annotations

import threading

from ..errors import FetchError, UnreachableHost
from ..ports import FetchResponse


class FakeFetcher:
    def __init__(self) -> None:
        self.fixtures: dict[str, FetchResponse] = {}
        self.unreachable: set[str] = set()
        self.transient_failures: dict[str, int] = {}  # url -> remaining failures
        self.calls: list[str] = []
        self._lock = threading.Lock()

    def fixture(
        self, url: str, content: bytes, status: int = 200, content_type: str | None = None
    ) -> None:
        self.fixtures[url] = FetchResponse(
            status=status, content=content, content_type=content_type
        )

    def get(self, url: str, max_bytes: int) -> FetchResponse:
        with self._lock:
            self.calls.append(url)
            if url in self.unreachable:
                raise UnreachableHost(url)
            remaining = self.transient_failures.get(url, 0)
            if remaining > 0:
                self.transient_failures[url] = remaining - 1
                raise FetchError(f"canned transient failure for {url}")
            resp = self.fixtures.get(url)
            if resp is None:
                return FetchResponse(status=404, content=b"", content_type="text/html")
            return FetchResponse(
                status=resp.status,
                content=resp.content[:max_bytes],
                content_type=resp.content_type,
            )
