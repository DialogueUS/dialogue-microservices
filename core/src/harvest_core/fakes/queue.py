"""FakeQueue: SQS semantics against the virtual clock.

Received messages are invisible until their per-message visibility
deadline passes, then redelivered with receive_count+1; a message
already delivered maxReceiveCount times moves to the DLQ list instead
of being delivered again.
"""

from __future__ import annotations

import itertools
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from ..constants import MAX_RECEIVE_COUNT, VISIBILITY_TIMEOUT_S
from ..ports import Clock, QueueMessage


@dataclass
class _StoredMessage:
    id: str
    body: str
    receive_count: int = 0
    invisible_until: datetime | None = None
    deleted: bool = False


@dataclass
class FakeQueue:
    clock: Clock
    visibility_timeout: float = VISIBILITY_TIMEOUT_S
    max_receive_count: int = MAX_RECEIVE_COUNT
    dlq: list[QueueMessage] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._messages: list[_StoredMessage] = []
        self._ids = itertools.count(1)
        self._lock = threading.Lock()
        self.send_hook: object = None  # settable: callable raising to simulate a dead queue

    def send(self, body: str) -> None:
        if callable(self.send_hook):
            self.send_hook(body)
        with self._lock:
            self._messages.append(_StoredMessage(id=f"m{next(self._ids)}", body=body))

    def receive(self, max_messages: int) -> list[QueueMessage]:
        now = self.clock.now()
        out: list[QueueMessage] = []
        with self._lock:
            for msg in self._messages:
                if len(out) >= max_messages:
                    break
                if msg.deleted:
                    continue
                if msg.invisible_until is not None and msg.invisible_until > now:
                    continue
                if msg.receive_count >= self.max_receive_count:
                    msg.deleted = True
                    self.dlq.append(QueueMessage(msg.id, msg.body, msg.receive_count))
                    continue
                msg.receive_count += 1
                msg.invisible_until = now + timedelta(seconds=self.visibility_timeout)
                out.append(QueueMessage(msg.id, msg.body, msg.receive_count))
        return out

    def delete(self, message_id: str) -> None:
        with self._lock:
            for msg in self._messages:
                if msg.id == message_id:
                    msg.deleted = True
                    return

    def change_visibility(self, message_id: str, timeout_seconds: float) -> None:
        now = self.clock.now()
        with self._lock:
            for msg in self._messages:
                if msg.id == message_id and not msg.deleted:
                    msg.invisible_until = now + timedelta(seconds=timeout_seconds)
                    return

    # -- test helpers -----------------------------------------------------
    def pending_count(self) -> int:
        """Messages not yet deleted (in flight or visible)."""
        with self._lock:
            return sum(1 for m in self._messages if not m.deleted)

    def bodies(self) -> list[str]:
        with self._lock:
            return [m.body for m in self._messages if not m.deleted]

    def drop_all(self) -> None:
        """Simulate total message loss."""
        with self._lock:
            for m in self._messages:
                m.deleted = True
