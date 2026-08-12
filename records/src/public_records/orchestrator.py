"""The orchestrator: the only component that originates work (§5).

Three periodic concerns plus the notification digest, each single-flight
(a concern whose previous run is still in flight is skipped silently).
"""

from __future__ import annotations

import logging
import threading
from hashlib import sha256

from harvest_core.health import Beat

from .census import ensure_states
from .config import ScopeConfig
from .constants import (
    FALLBACK_QUERY_PATTERN,
    FOLLOWUP_SCAN_BATCH,
    MAIL_POLL_CAP,
    ORCHESTRATOR_PERIOD_S,
    QUERIES_PER_TARGET_MAX,
    SES_SETUP_MARKER,
)
from .digest import send_digests
from .domain import (
    Campaign,
    EscalationReason,
    Jurisdiction,
    extract_token,
    fallback_message_id,
    slugify,
    spool_key,
)
from .errors import GenerationError, UniqueViolation
from .mailparse import parse_mime
from .messages import (
    ContactMessage,
    FollowupJobMessage,
    InboundAttachment,
    InboundMailMessage,
    SearchQueryMessage,
)
from .world import World, escalate, next_action_time

log = logging.getLogger("public_records.orchestrator")


# --------------------------------------------------------------------------
# §5.1 Seeding


def resolve_scope(world: World, scope: ScopeConfig) -> list[Jurisdiction]:
    states = None if "ALL" in scope.states else scope.states
    jurisdictions = world.store.list_jurisdictions(states=states, levels=scope.levels)
    # comma-county rows (e.g. "X County, Y County") are excluded from scope
    # resolution but retained in the table — carried over from the old pipeline.
    jurisdictions = [j for j in jurisdictions if "," not in j.name]
    if scope.only is not None:
        allowed = {name.lower() for name in scope.only}
        jurisdictions = [j for j in jurisdictions if j.name.lower() in allowed]
    return jurisdictions


def _queries_for(world: World, campaign: Campaign, jurisdiction: Jurisdiction) -> list[str]:
    """Fallback pattern always first: an LLM failure degrades to one
    usable query, never zero. Hard cap 3 per email goal."""
    fallback = FALLBACK_QUERY_PATTERN.format(
        name=jurisdiction.name, state=jurisdiction.state
    )
    queries = [fallback]
    try:
        generated = world.query_generator.generate_queries(
            jurisdiction, campaign.config.record_type
        )
    except GenerationError as exc:
        log.warning("query generation failed for %s: %s", jurisdiction.name, exc)
        generated = []
    for query in generated:
        if query and query not in queries:
            queries.append(query)
    return queries[:QUERIES_PER_TARGET_MAX]


def _seed_test_contacts(world: World, campaign: Campaign) -> int:
    """A test campaign's targets come from its config, already answered:
    the address is given, so the search stage is skipped outright rather
    than run and discarded. Same target bookkeeping as the searched path
    — `resolve_target` still gates the enqueue — so a crashed pass
    re-runs without double-sending.

    `bypass_cooldown` because the office was never really contacted: the
    mail goes to the test address, and the shared clock on a real
    jurisdiction row belongs to the campaigns that do mail it.
    """
    sent = 0
    for contact in campaign.config.test_contacts:
        jurisdiction = world.store.find_jurisdiction(
            contact.jurisdiction, contact.state, contact.level
        )
        if jurisdiction is None:
            try:
                jurisdiction = world.store.insert_jurisdiction(
                    contact.jurisdiction, contact.state, contact.level
                )
            except UniqueViolation:  # concurrent seeder won; harmless
                jurisdiction = world.store.find_jurisdiction(
                    contact.jurisdiction, contact.state, contact.level
                )
                assert jurisdiction is not None

        target = world.store.find_search_target(campaign.id, jurisdiction.id)
        if target is None:
            try:
                target = world.store.insert_search_target(
                    campaign.id, jurisdiction.id, world.clock.now()
                )
            except UniqueViolation:
                target = world.store.find_search_target(campaign.id, jurisdiction.id)
                assert target is not None
        if target.resolved or target.queries_enqueued > 0:
            continue  # crash-rerun skip marker

        # Resolve first, as the searched seeded-contact branch does. A
        # crash in the window between drops this contact (see that
        # branch); re-sending instead would risk two copies of a real
        # request racing through the sender pool, which is the worse
        # failure of the two.
        if world.store.resolve_target(target.id):
            world.contacts_queue.send(
                ContactMessage(
                    campaign_id=campaign.id,
                    jurisdiction_id=jurisdiction.id,
                    contact_email=contact.email,
                    source="seeded",
                    bypass_cooldown=True,
                ).to_json()
            )
            sent += 1
    return sent


def seed_pass(world: World) -> int:
    """Seed every active, unseeded, consented campaign. Returns the
    number of queue messages sent (queries + seeded contacts)."""
    sent = 0
    for campaign in world.store.list_campaigns():
        if not (campaign.active and not campaign.seeded and campaign.consent_confirmed):
            continue
        if campaign.is_test:
            # No census and no search queue: a test campaign reaches the
            # sender without touching either network dependency.
            sent += _seed_test_contacts(world, campaign)
            world.store.set_campaign_seeded(campaign.id)
            continue
        if world.census is not None:
            ensure_states(world.store, world.census, campaign.config.scope.states)
        for jurisdiction in resolve_scope(world, campaign.config.scope):
            target = world.store.find_search_target(campaign.id, jurisdiction.id)
            if target is None:
                try:
                    target = world.store.insert_search_target(
                        campaign.id, jurisdiction.id, world.clock.now()
                    )
                except UniqueViolation:  # concurrent seeder won; harmless
                    target = world.store.find_search_target(campaign.id, jurisdiction.id)
                    assert target is not None
            if target.resolved or target.queries_enqueued > 0:
                continue  # crash-rerun skip marker

            if jurisdiction.contact_email:
                # seeded contacts are trusted and never flagged for review
                if world.store.resolve_target(target.id):
                    world.contacts_queue.send(
                        ContactMessage(
                            campaign_id=campaign.id,
                            jurisdiction_id=jurisdiction.id,
                            contact_email=jurisdiction.contact_email,
                            source="seeded",
                        ).to_json()
                    )
                    sent += 1
                continue

            queries = _queries_for(world, campaign, jurisdiction)
            for index, query in enumerate(queries):
                world.search_queue.send(
                    SearchQueryMessage(
                        campaign_id=campaign.id,
                        jurisdiction_id=jurisdiction.id,
                        search_target_id=target.id,
                        query=query,
                        query_index=index,
                    ).to_json()
                )
                sent += 1
            world.store.set_target_queries_enqueued(target.id, len(queries))
        # only reached when every in-scope target enqueued without error
        world.store.set_campaign_seeded(campaign.id)
    return sent


# --------------------------------------------------------------------------
# §5.2 Mail polling


def poll_mail(world: World) -> int:
    """List the SES mail bucket into pr-inbound-mail — read-only, only
    when the inbound queue is empty. Returns messages enqueued."""
    if world.inbound_queue.pending_count() > 0:
        return 0
    enqueued = 0
    for key in world.mail_bucket.list_keys(""):
        if enqueued >= MAIL_POLL_CAP:
            break
        if SES_SETUP_MARKER in key:
            continue
        if world.store.has_source_key(key):
            continue
        parsed = parse_mime(world.mail_bucket.get(key))
        token = extract_token(parsed.headers, parsed.subject)

        campaign_slug = "unmatched"
        if token is not None:
            thread = world.store.get_thread_by_token(token)
            if thread is not None:
                campaign = world.store.get_campaign(thread.campaign_id)
                if campaign is not None:
                    campaign_slug = slugify(campaign.name)

        # attachment bytes never ride in SQS (256 KB cap): spool to the
        # documents bucket and pass keys.
        attachments: list[InboundAttachment] = []
        for index, attachment in enumerate(parsed.attachments):
            digest = sha256(attachment.payload).hexdigest()
            tmp_key = spool_key(campaign_slug, digest, index, attachment.filename)
            world.documents.put(tmp_key, attachment.payload, attachment.content_type)
            attachments.append(
                InboundAttachment(
                    filename=attachment.filename,
                    content_type=attachment.content_type,
                    s3_tmp_key=tmp_key,
                )
            )

        message_id = parsed.message_id or fallback_message_id(
            parsed.from_address, parsed.subject, parsed.body
        )
        world.inbound_queue.send(
            InboundMailMessage(
                source_key=key,
                message_id=message_id,
                thread_token=token,
                from_address=parsed.from_address,
                subject=parsed.subject,
                body=parsed.body,
                attachments=attachments,
            ).to_json()
        )
        enqueued += 1
    return enqueued


# --------------------------------------------------------------------------
# §5.3 Silence-based follow-up scheduling


def followup_scan(world: World) -> int:
    """Enqueue follow-ups for silent threads; escalate exhausted ones.
    The reschedule happens at enqueue time, not send time."""
    acted = 0
    for thread in world.store.select_due_followups(world.clock.now(), FOLLOWUP_SCAN_BATCH):
        campaign = world.store.get_campaign(thread.campaign_id)
        if campaign is None:
            continue
        if thread.followups_sent >= campaign.config.limits.max_followups:
            escalate(
                world,
                campaign.id,
                thread.id,
                EscalationReason.NO_RESPONSE,
                f"No response after {thread.followups_sent} follow-ups.",
            )
        else:
            world.followups_queue.send(
                FollowupJobMessage(
                    thread_id=thread.id,
                    kind="followup",
                    followup_index=thread.followups_sent,
                ).to_json()
            )
            world.store.set_thread_status(
                thread.id, thread.status, next_action_time(world, campaign)
            )
        acted += 1
    return acted


# --------------------------------------------------------------------------
# The scheduler


class Orchestrator:
    """Runs the periodic concerns, each single-flight."""

    def __init__(self, world: World, period_s: float = ORCHESTRATOR_PERIOD_S) -> None:
        self.world = world
        self.period_s = period_s
        self._locks = {name: threading.Lock() for name in ("seed", "poll", "scan", "digest")}
        self._stop = threading.Event()
        # Health: the tick's progress marker, read by the responder in cli.py.
        # Marked at construction so a loop that never completes a tick goes
        # stale on schedule rather than looking eternally new.
        self.beat = Beat(world.clock)

    def _run_concern(self, name: str, fn: object) -> None:
        lock = self._locks[name]
        if not lock.acquire(blocking=False):
            return  # previous run still in flight — skip silently
        try:
            assert callable(fn)
            fn(self.world)
        except Exception:
            log.exception("orchestrator concern %s failed", name)
        finally:
            lock.release()

    def tick(self) -> None:
        self._run_concern("seed", seed_pass)
        self._run_concern("poll", poll_mail)
        self._run_concern("scan", followup_scan)
        self._run_concern("digest", send_digests)

    def stop(self) -> None:
        self._stop.set()

    def run_forever(self) -> None:
        while not self._stop.is_set():
            self.tick()
            # `tick` swallows each concern's exceptions, so reaching here
            # means the scheduler itself is turning — which is what a stale
            # beat would disprove.
            self.beat.mark()
            self.world.clock.sleep(self.period_s)
