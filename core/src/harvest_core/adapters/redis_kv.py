"""redis-py KeyValue adapter."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class RedisKeyValue:
    def __init__(self, client: Any) -> None:
        self._r = client

    @classmethod
    def from_url(cls, url: str) -> RedisKeyValue:
        import redis

        return cls(redis.Redis.from_url(url, decode_responses=True))

    def setnx(self, key: str, value: str) -> bool:
        return bool(self._r.set(key, value, nx=True))

    def hincrby(self, key: str, increments: Mapping[str, int]) -> dict[str, int]:
        pipe = self._r.pipeline(transaction=True)
        for field, amount in increments.items():
            pipe.hincrby(key, field, amount)
        pipe.hgetall(key)
        results = pipe.execute()
        final: dict[str, Any] = results[-1]
        return {str(k): int(v) for k, v in final.items()}

    def expire(self, key: str, ttl_seconds: int) -> None:
        self._r.expire(key, ttl_seconds)

    def get(self, key: str) -> str | None:
        value = self._r.get(key)
        return None if value is None else str(value)

    def delete_prefix(self, prefix: str) -> int:
        count = 0
        for key in self._r.scan_iter(match=f"{prefix}*", count=500):
            self._r.delete(key)
            count += 1
        return count
