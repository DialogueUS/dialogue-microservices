"""FakeObjectStore: dict-backed S3 stand-in."""

from __future__ import annotations

import threading


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.content_types: dict[str, str] = {}
        self._lock = threading.Lock()

    def put(self, key: str, data: bytes, content_type: str) -> None:
        with self._lock:
            self.objects[key] = data
            self.content_types[key] = content_type

    def presign(self, key: str) -> str:
        return f"https://fake-object-store.local/{key}?X-Fake-Signature=1"

    def delete_prefix(self, prefix: str) -> int:
        with self._lock:
            doomed = [k for k in self.objects if k.startswith(prefix)]
            for k in doomed:
                del self.objects[k]
                self.content_types.pop(k, None)
            return len(doomed)
