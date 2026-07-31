"""FakeKeyValue: SETNX / HINCRBY / TTL expiry against the virtual clock."""

from __future__ import annotations

import threading
from collections.abc import Mapping
from datetime import datetime, timedelta

from ..ports import Clock


class FakeKeyValue:
    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._strings: dict[str, str] = {}
        self._hashes: dict[str, dict[str, int]] = {}
        self._expiry: dict[str, datetime] = {}
        self._lock = threading.Lock()

    def _purge_expired(self, now: datetime) -> None:
        for key, deadline in list(self._expiry.items()):
            if deadline <= now:
                self._strings.pop(key, None)
                self._hashes.pop(key, None)
                del self._expiry[key]

    def setnx(self, key: str, value: str) -> bool:
        with self._lock:
            self._purge_expired(self._clock.now())
            if key in self._strings or key in self._hashes:
                return False
            self._strings[key] = value
            return True

    def hincrby(self, key: str, increments: Mapping[str, int]) -> dict[str, int]:
        with self._lock:
            self._purge_expired(self._clock.now())
            h = self._hashes.setdefault(key, {})
            for field, amount in increments.items():
                h[field] = h.get(field, 0) + amount
            return dict(h)

    def expire(self, key: str, ttl_seconds: int) -> None:
        with self._lock:
            if key in self._strings or key in self._hashes:
                self._expiry[key] = self._clock.now() + timedelta(seconds=ttl_seconds)

    def get(self, key: str) -> str | None:
        with self._lock:
            self._purge_expired(self._clock.now())
            return self._strings.get(key)

    def delete_prefix(self, prefix: str) -> int:
        with self._lock:
            doomed = [k for k in [*self._strings, *self._hashes] if k.startswith(prefix)]
            for k in doomed:
                self._strings.pop(k, None)
                self._hashes.pop(k, None)
                self._expiry.pop(k, None)
            return len(doomed)

    # -- test helpers -----------------------------------------------------
    def flush(self) -> None:
        """Simulate an ElastiCache failover losing all keys."""
        with self._lock:
            self._strings.clear()
            self._hashes.clear()
            self._expiry.clear()

    def hash_value(self, key: str) -> dict[str, int] | None:
        with self._lock:
            self._purge_expired(self._clock.now())
            h = self._hashes.get(key)
            return dict(h) if h is not None else None
