"""Consumer framework (plan 3.1): polling loops, idempotency gates,
visibility heartbeats.

The idempotency contract (spec §6.6): before working, re-read the run
switch and the row; a mismatch means this message is a ghost of an
earlier dispatch — delete it unworked. After working, commit Postgres
then delete the message; a handler that raises leaves its messages
undeleted so SQS redelivers them.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from datetime import datetime, timedelta

from harvest_core.domain import Artifact, ArtifactStatus, RunState
from harvest_core.messages import CodeTask, FetchTask, SweepTask
from harvest_core.ports import Clock, Datastore, QueueMessage, TaskQueue

log = logging.getLogger(__name__)


class Heartbeat:
    """Extend a message's visibility every `interval` seconds, up to a
    hard deadline (code tasks: extend by 900 s every 300 s until 3600 s)."""

    def __init__(
        self,
        queue: TaskQueue,
        message_id: str,
        clock: Clock,
        interval_seconds: float,
        extension_seconds: float,
        deadline_seconds: float,
    ) -> None:
        self._queue = queue
        self._message_id = message_id
        self._clock = clock
        self._interval = interval_seconds
        self._extension = extension_seconds
        started = clock.now()
        self._deadline = started + timedelta(seconds=deadline_seconds)
        self._next_beat = started + timedelta(seconds=interval_seconds)
        self.beats: list[datetime] = []

    @property
    def deadline(self) -> datetime:
        return self._deadline

    def expired(self) -> bool:
        return self._clock.now() >= self._deadline

    def maybe_beat(self) -> None:
        now = self._clock.now()
        if now >= self._next_beat and now < self._deadline:
            self._queue.change_visibility(self._message_id, self._extension)
            self.beats.append(now)
            self._next_beat = now + timedelta(seconds=self._interval)


def gate_sweep(ds: Datastore, task: SweepTask) -> bool:
    """True when the message should be worked; False → delete unworked."""
    if ds.get_run_state(task.corpus) != RunState.RUNNING:
        return False
    target = ds.get_target(task.sweep_target_id)
    if target is None or target.dispatch_id != task.dispatch_id:
        return False  # re-dispatched (or finalized): this message is stale
    if any(
        h.dispatch_id == task.dispatch_id and h.query_seq == task.query_seq
        for h in ds.list_history(task.corpus, task.jurisdiction_id)
    ):
        return False  # already completed by a previous delivery
    return True


def gate_code(ds: Datastore, task: CodeTask) -> bool:
    if ds.get_run_state(task.corpus) != RunState.RUNNING:
        return False
    target = ds.get_target(task.sweep_target_id)
    return target is not None and target.dispatch_id == task.dispatch_id


def gate_fetch(ds: Datastore, task: FetchTask) -> Artifact | None:
    """The artifact to work, or None → delete the message unworked."""
    if ds.get_run_state(task.corpus) != RunState.RUNNING:
        return None
    artifact = ds.get_artifact(task.artifact_id)
    if artifact is None or artifact.status != ArtifactStatus.PENDING:
        return None
    if artifact.dispatch_id != task.dispatch_id:
        return None  # re-published under a fresh dispatch_id; that one wins
    return artifact


class ConsumerLoop:
    """Long-poll one queue and hand batches to a handler.

    The handler owns per-message deletes (commit-then-delete). If it
    raises, nothing is deleted and every message redelivers after its
    visibility timeout.
    """

    def __init__(
        self,
        queue: TaskQueue,
        handler: Callable[[list[QueueMessage]], None],
        batch_size: int,
        clock: Clock,
        idle_sleep_seconds: float = 1.0,
    ) -> None:
        self._queue = queue
        self._handler = handler
        self._batch_size = batch_size
        self._clock = clock
        self._idle_sleep = idle_sleep_seconds

    def run_once(self) -> int:
        messages = self._queue.receive(self._batch_size)
        if not messages:
            return 0
        try:
            self._handler(messages)
        except Exception:
            log.exception("handler failed; %d message(s) left for redelivery", len(messages))
        return len(messages)

    def run_forever(self, stop: threading.Event) -> None:
        while not stop.is_set():
            if self.run_once() == 0:
                self._clock.sleep(self._idle_sleep)
