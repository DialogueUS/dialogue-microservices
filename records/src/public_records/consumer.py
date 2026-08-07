"""Consumer framework: commit-then-delete, redelivery on exception, and
the DLQ watcher that turns persistent failure into an escalation."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable

from harvest_core.ports import TaskQueue

from .constants import BATCH_RECEIVE
from .domain import EscalationReason
from .world import World, escalate

log = logging.getLogger("public_records.consumer")

# A handler returns True when the message's work is committed (delete it)
# and False to leave it for redelivery. An exception also leaves it.
Handler = Callable[[World, str], bool]


def process_queue(
    world: World, queue: TaskQueue, handler: Handler, max_messages: int = BATCH_RECEIVE
) -> int:
    """One receive pass. Returns the number of messages handled
    (successfully or not); deletes only after the handler commits."""
    handled = 0
    for message in queue.receive(max_messages):
        handled += 1
        try:
            done = handler(world, message.body)
        except Exception:
            log.exception("handler failed; message %s left for redelivery", message.id)
            continue
        if done:
            queue.delete(message.id)
    return handled


def drain_dlq(world: World, queue_name: str, dlq: TaskQueue) -> int:
    """Every dead-lettered message raises exactly one `other` escalation
    naming the queue and payload summary; a thread named in the payload
    is parked at needs_human (§4 diagram's DLQ edge)."""
    drained = 0
    for message in dlq.receive(BATCH_RECEIVE):
        campaign_id: int | None = None
        thread_id: int | None = None
        try:
            payload = json.loads(message.body)
            campaign_id = payload.get("campaign_id")
            thread_id = payload.get("thread_id")
            if campaign_id is None and thread_id is not None:
                thread = world.store.get_thread(int(thread_id))
                if thread is not None:
                    campaign_id = thread.campaign_id
        except (ValueError, TypeError):
            pass
        escalate(
            world,
            campaign_id,
            thread_id,
            EscalationReason.OTHER,
            f"message dead-lettered from {queue_name}: {message.body[:500]}",
        )
        dlq.delete(message.id)
        drained += 1
    return drained
