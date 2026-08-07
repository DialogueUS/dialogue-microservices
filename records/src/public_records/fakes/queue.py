"""FakeQueueWithDlq: harvest_core's FakeQueue plus SQS-style redrive.

The shared FakeQueue collects over-received messages in its `dlq` list;
this subclass forwards them into a linked FakeQueue so the DLQ watcher
can consume them through the ordinary TaskQueue port, exactly like a
real SQS redrive policy.
"""

from __future__ import annotations

from dataclasses import dataclass

from harvest_core.fakes import FakeQueue
from harvest_core.ports import QueueMessage


@dataclass
class FakeQueueWithDlq(FakeQueue):
    dlq_queue: FakeQueue | None = None

    def receive(self, max_messages: int) -> list[QueueMessage]:
        out = super().receive(max_messages)
        if self.dlq_queue is not None:
            while self.dlq:
                dead = self.dlq.pop(0)
                self.dlq_queue.send(dead.body)
        return out
