"""The email receiver (§8): match -> classify -> log -> react.

Idempotency key: message_id, unique per campaign. The inbound row is
always written before any reaction; a handler exception leaves the SQS
message for redelivery.
"""

from __future__ import annotations

import logging

from .attachments import delete_spooled, store_attachments
from .constants import CLASSIFY_BODY_CHARS, ESCALATION_DETAIL_CHARS
from .domain import (
    Campaign,
    EmailRecord,
    EmailThread,
    EscalationReason,
    InboundCategory,
    ThreadStatus,
)
from .errors import ClassifyError
from .fees import handle_payment_required
from .messages import ContactMessage, FollowupJobMessage, InboundMailMessage
from .world import World, escalate, next_action_time

log = logging.getLogger("public_records.receiver")


def _match_thread(world: World, msg: InboundMailMessage) -> EmailThread | None:
    """Token first (globally unique — tokens cross campaigns safely),
    else sender address against an open thread."""
    if msg.thread_token:
        thread = world.store.get_thread_by_token(msg.thread_token)
        if thread is not None:
            return thread
    return world.store.find_open_thread_by_contact(msg.from_address)


def handle_inbound(world: World, body: str) -> bool:
    msg = InboundMailMessage.from_json(body)
    thread = _match_thread(world, msg)
    campaign: Campaign | None = None
    if thread is not None:
        campaign = world.store.get_campaign(thread.campaign_id)

    campaign_id = campaign.id if campaign is not None else None
    if world.store.has_message_id(campaign_id, msg.message_id):
        return True  # at-least-once dedupe

    if thread is None or campaign is None:
        # unmatched mail: a campaign-less, thread-less row, kept forever;
        # a human can attach it later
        world.store.insert_email(
            thread_id=None, campaign_id=None, direction="inbound",
            from_address=msg.from_address, to_address=world.from_address,
            subject=msg.subject, body=msg.body, kind=None, classification=None,
            message_id=msg.message_id, source_key=msg.source_key, resend_id=None,
            in_reply_to_email_id=None, attachment_refs=[], created_at=world.clock.now(),
        )
        delete_spooled(world, msg.attachments)
        return True

    try:
        classification = world.classifier.classify(msg.subject, msg.body[:CLASSIFY_BODY_CHARS])
    except ClassifyError:
        return False  # transient; the third failed receive redrives to the DLQ

    # log the inbound row — always, before any reaction. Confidence is
    # recorded but never thresholded.
    inbound = world.store.insert_email(
        thread_id=thread.id, campaign_id=campaign.id, direction="inbound",
        from_address=msg.from_address, to_address=world.from_address,
        subject=msg.subject, body=msg.body, kind=None, classification=classification,
        message_id=msg.message_id, source_key=msg.source_key, resend_id=None,
        in_reply_to_email_id=None, attachment_refs=[], created_at=world.clock.now(),
    )

    _react(world, campaign, thread, inbound, msg, classification.category,
           classification.referral_email)
    delete_spooled(world, msg.attachments)
    return True


def _react(
    world: World,
    campaign: Campaign,
    thread: EmailThread,
    inbound: EmailRecord,
    msg: InboundMailMessage,
    category: InboundCategory,
    referral_email: str | None,
) -> None:
    if category in (InboundCategory.DATA_PROVIDED, InboundCategory.ACKNOWLEDGMENT):
        jurisdiction = world.store.get_jurisdiction(thread.jurisdiction_id)
        jurisdiction_name = jurisdiction.name if jurisdiction else "unknown"
        refs, stored = store_attachments(
            world, campaign, jurisdiction_name, thread.id, msg.attachments
        )
        if refs:
            world.store.update_attachment_refs(inbound.id, refs)
        if category is InboundCategory.DATA_PROVIDED or stored > 0:
            # data_provided fulfills even with zero attachments stored — an
            # office pointing at inline links is trusted (known gap).
            world.store.set_thread_status(thread.id, ThreadStatus.FULFILLED, None)
        else:
            world.store.set_thread_status(
                thread.id, ThreadStatus.AWAITING_REPLY, next_action_time(world, campaign)
            )
        return

    if category is InboundCategory.PAYMENT_REQUIRED:
        handle_payment_required(world, campaign, thread, inbound)
        return

    if category is InboundCategory.REFERRAL:
        if referral_email:
            world.contacts_queue.send(
                ContactMessage(
                    campaign_id=campaign.id,
                    jurisdiction_id=thread.jurisdiction_id,
                    contact_email=referral_email,
                    source="referral",
                    bypass_cooldown=True,
                    parent_thread_id=thread.id,
                ).to_json()
            )
            world.store.set_thread_status(thread.id, ThreadStatus.REFERRED, None)
        else:
            escalate(
                world, campaign.id, thread.id, EscalationReason.REFERRAL_NO_ADDRESS,
                f"Office redirected us without an address.\n\n{inbound.body}",
            )
        return

    if category is InboundCategory.DENIAL:
        escalate(
            world, campaign.id, thread.id, EscalationReason.DENIAL,
            f"Request denied.\n\n{inbound.body}",
        )
        return

    if category is InboundCategory.NEEDS_CLARIFICATION:
        world.followups_queue.send(
            FollowupJobMessage(
                thread_id=thread.id,
                kind="clarification_reply",
                inbound_email_id=inbound.id,
            ).to_json()
        )
        world.store.set_thread_status(
            thread.id, ThreadStatus.AWAITING_REPLY, next_action_time(world, campaign)
        )
        return

    # unclear (including everything the classifier degraded)
    escalate(
        world, campaign.id, thread.id, EscalationReason.UNCLEAR_REPLY,
        f"Unclear reply.\n\n{inbound.body[:ESCALATION_DETAIL_CHARS]}",
    )
