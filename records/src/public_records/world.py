"""The wired world: every port the service needs, plus shared helpers.

Both the orchestrator and the consumers take a World; tests build one
from fakes, `cli.py` builds one from real adapters.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from harvest_core.ports import (
    CensusSource,
    Clock,
    Fetcher,
    KeyValue,
    ObjectStore,
    PubSub,
    SearchProvider,
    TaskQueue,
)

from .constants import ESCALATION_DETAIL_CHARS
from .domain import Campaign, EscalationReason, ThreadStatus
from .errors import IllegalTransition
from .ports import (
    ContactPicker,
    ContactQueryGenerator,
    EmailDrafter,
    EmailTransport,
    InboundClassifier,
    RecordsStore,
)

log = logging.getLogger("public_records")


@dataclass
class World:
    clock: Clock
    store: RecordsStore
    kv: KeyValue
    mail_bucket: ObjectStore  # SES receiving; read-only here
    documents: ObjectStore
    search_queue: TaskQueue  # pr-search-queries
    contacts_queue: TaskQueue  # pr-contacts
    followups_queue: TaskQueue  # pr-followups
    inbound_queue: TaskQueue  # pr-inbound-mail
    search: SearchProvider
    fetcher: Fetcher
    query_generator: ContactQueryGenerator
    picker: ContactPicker
    drafter: EmailDrafter
    classifier: InboundClassifier
    transport: EmailTransport
    from_address: str  # Resend-verified sender (required config)
    census: CensusSource | None = None  # jurisdiction seeding (§3.1)
    # Health check only, and used by nothing a handler touches: `run_service`
    # answers pings on it. None means no responder runs — which is right for
    # tests and for the one-off commands, and would fail the container health
    # check if it ever happened to `run`.
    pubsub: PubSub | None = None


def escalate(
    world: World,
    campaign_id: int | None,
    thread_id: int | None,
    reason: EscalationReason,
    details: str,
) -> None:
    """Create an escalation and park the thread (if any) at needs_human.

    Details are truncated to 2,000 chars (§3.1). Threads already in a
    terminal state keep their status — the escalation still lands.
    """
    world.store.insert_escalation(
        campaign_id,
        thread_id,
        reason,
        details[:ESCALATION_DETAIL_CHARS],
        world.clock.now(),
    )
    if thread_id is not None:
        try:
            world.store.set_thread_status(thread_id, ThreadStatus.NEEDS_HUMAN, None)
        except IllegalTransition:
            log.warning("thread %s not parked: terminal status", thread_id)


def next_action_time(world: World, campaign: Campaign) -> datetime:
    interval_days = campaign.config.limits.followup_interval_days
    return world.clock.now() + timedelta(days=interval_days)
