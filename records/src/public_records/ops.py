"""Operational surface (§12): registration, start/stop, escalation
resolution, and kill-as-purge."""

from __future__ import annotations

import logging

from .config import CampaignConfig
from .domain import Campaign, ThreadStatus, slugify
from .messages import ContactMessage
from .world import World

log = logging.getLogger("public_records.ops")


def register_campaign(world: World, config: CampaignConfig) -> Campaign:
    """Create the campaign row from a validated config. Registration
    never activates: `active` and consent are separate, deliberate acts."""
    return world.store.insert_campaign(config, world.clock.now())


def set_active(world: World, name: str, active: bool) -> Campaign:
    campaign = world.store.get_campaign_by_name(name)
    if campaign is None:
        raise LookupError(f"no campaign named {name!r}")
    world.store.set_campaign_active(campaign.id, active)
    loaded = world.store.get_campaign(campaign.id)
    assert loaded is not None
    return loaded


def resolve_escalation(
    world: World,
    escalation_id: int,
    resolution: str,
    thread_status: ThreadStatus | None = None,
) -> None:
    """Record the resolution; optionally set the thread to any status.
    Resolving to pending_send also enqueues a human-approved contact —
    the common approve-a-reviewed-contact case."""
    escalation = world.store.get_escalation(escalation_id)
    if escalation is None:
        raise LookupError(f"no escalation {escalation_id}")
    world.store.resolve_escalation(escalation_id, resolution, world.clock.now())
    if escalation.thread_id is None or thread_status is None:
        return
    world.store.set_thread_status(
        escalation.thread_id, thread_status, None, by_human=True
    )
    if thread_status is ThreadStatus.PENDING_SEND:
        thread = world.store.get_thread(escalation.thread_id)
        assert thread is not None
        world.contacts_queue.send(
            ContactMessage(
                campaign_id=thread.campaign_id,
                jurisdiction_id=thread.jurisdiction_id,
                contact_email=thread.contact_email,
                source="human_approved",
                bypass_cooldown=True,
            ).to_json()
        )


def kill_campaign(world: World, name: str) -> dict[str, int]:
    """Permanent purge: rows FK-ordered, the Redis dedupe prefix, and the
    campaign's S3 prefix (documents and inbox spool). Jurisdictions,
    their contacts, raw MIME, and in-flight SQS messages survive —
    consumers drop messages whose campaign no longer exists."""
    campaign = world.store.get_campaign_by_name(name)
    if campaign is None:
        raise LookupError(f"no campaign named {name!r}")
    slug = slugify(campaign.name)
    counts = world.store.purge_campaign(campaign.id)
    counts["redis_keys"] = world.kv.delete_prefix(f"dedupe:{slug}")
    counts["s3_objects"] = world.documents.delete_prefix(f"{slug}/")
    log.info("killed campaign %r: %s", name, counts)
    return counts
