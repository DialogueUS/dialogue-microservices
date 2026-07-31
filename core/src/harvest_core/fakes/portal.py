"""FakePortalDiscoverer: programmable discovery results per portal URL."""

from __future__ import annotations

import threading
from datetime import datetime

from ..domain import Publisher
from ..errors import PortalError, TransientPortalError
from ..ports import DiscoveryResult, PortalCandidate


class FakePortalDiscoverer:
    def __init__(self) -> None:
        # portal url -> candidates emitted for it
        self.candidates: dict[str, list[PortalCandidate]] = {}
        self.transient_for: set[str] = set()
        self.error_for: set[str] = set()
        self.incomplete_for: set[str] = set()  # simulate deadline truncation
        self.calls: list[tuple[tuple[str, ...], Publisher]] = []
        self._lock = threading.Lock()

    def discover(
        self,
        portal_urls: list[str],
        publisher: Publisher,
        max_pages: int,
        deadline: datetime,
    ) -> DiscoveryResult:
        with self._lock:
            self.calls.append((tuple(portal_urls), publisher))
            found: list[PortalCandidate] = []
            complete = True
            for url in portal_urls:
                if url in self.transient_for:
                    raise TransientPortalError(url)
                found.extend(self.candidates.get(url, []))
                if url in self.incomplete_for:
                    complete = False
                elif url in self.error_for:
                    if not found:
                        raise PortalError(url)
                    complete = False
            return DiscoveryResult(
                candidates=found,
                complete=complete,
                detail="" if complete else "truncated at deadline",
            )
