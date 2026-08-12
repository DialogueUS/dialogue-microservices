"""The email sender (§7): initial requests and thread jobs.

Every send commits its emails row and thread-status change in one
transaction (the store's composite methods), then deletes the message.
Under dry_run the Resend call is skipped and the payload logged; all DB
writes still happen so the whole flow rehearses.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from .constants import ANON_BLOCKED_STATES, CLARIFICATION_CONTEXT_CHARS, TOKEN_HEADER
from .domain import (
    Campaign,
    EscalationReason,
    OutboundKind,
    ThreadStatus,
    new_thread_token,
)
from .errors import DraftError, SendTransientError
from .messages import ContactMessage, FollowupJobMessage
from .ports import OutboundEmail
from .world import World, escalate, next_action_time

log = logging.getLogger("public_records.sender")


def _utc_midnight(now: datetime) -> datetime:
    return now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def _signature(campaign: Campaign, reply_address: str) -> str:
    requester = campaign.config.requester
    if requester.anonymous:
        return f"Thanks for your time.\n{reply_address}"
    lines = ["Sincerely,", requester.name]
    if requester.organization:
        lines.append(requester.organization)
    lines.append(requester.email)
    if requester.phone:
        lines.append(requester.phone)
    return "\n".join(lines)


def assemble_body(campaign: Campaign, drafted: str, reply_address: str) -> str:
    """LLM text + the mechanical exact-records block + signature (§7.1)."""
    return (
        f"{drafted}\n\n---\nExact records requested:\n"
        f"{campaign.config.record_description}\n\n"
        f"{_signature(campaign, reply_address)}"
    )


def _deliver(world: World, campaign: Campaign, email: OutboundEmail) -> str | None:
    """Returns the resend id, or None under dry_run. Raises
    SendTransientError on transport failure."""
    if campaign.dry_run:
        log.info(
            "dry_run send skipped: to=%s subject=%r body[:200]=%r",
            email.to_address, email.subject, email.body[:200],
        )
        return None
    return world.transport.send(email)


def _reply_subject(office_subject: str, token: str) -> str:
    """Reuse the office's subject, `Re:` prefixed if absent, token
    appended if missing (§7.2)."""
    subject = office_subject.strip() or f"Public Records Request [DLG-{token}]"
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"
    if token.lower() not in subject.lower():
        subject = f"{subject} [DLG-{token}]"
    return subject


# --------------------------------------------------------------------------
# §7.1 Initial requests (pr-contacts)


def handle_contact(world: World, body: str) -> bool:
    msg = ContactMessage.from_json(body)
    campaign = world.store.get_campaign(msg.campaign_id)
    if campaign is None:
        return True  # purged campaign: drop
    jurisdiction = world.store.get_jurisdiction(msg.jurisdiction_id)
    if jurisdiction is None:
        return True

    # idempotency: (campaign, jurisdiction, contact_email). An existing
    # pending_send thread is a human-approved re-entry and DOES send.
    existing_thread_id: int | None = None
    thread_token = new_thread_token()
    existing = world.store.find_thread(campaign.id, jurisdiction.id, msg.contact_email)
    if existing is not None:
        if existing.status is not ThreadStatus.PENDING_SEND:
            return True
        existing_thread_id = existing.id
        thread_token = existing.thread_token

    # gate 1: consent + active — a stopped campaign drains its queues inertly
    if not (campaign.consent_confirmed and campaign.active):
        return True

    # gate 2: the daily cap — counts all kinds, gates only initial requests
    now = world.clock.now()
    limits = campaign.config.limits
    if world.store.count_outbound_since(campaign.id, _utc_midnight(now)) >= limits.daily_send_cap:
        return False  # over cap: let visibility expire and retry later

    # gate 3: the office cooldown, on the shared jurisdiction row
    if not msg.bypass_cooldown and jurisdiction.last_contacted_at is not None:
        cooldown = timedelta(days=limits.per_office_cooldown_days)
        if now < jurisdiction.last_contacted_at + cooldown:
            return False

    # gate 4: the anonymous-state guard — a per-target refusal
    if campaign.config.requester.anonymous and jurisdiction.state in ANON_BLOCKED_STATES:
        escalate(
            world,
            campaign.id,
            existing_thread_id,
            EscalationReason.OTHER,
            f"Anonymous request refused: {jurisdiction.state} requires requester "
            f"identity ({jurisdiction.name}). Set requester.anonymous=false or drop "
            "this jurisdiction.",
        )
        return True

    try:
        drafted = world.drafter.draft_initial(campaign, jurisdiction)
    except DraftError:
        return False  # transient: retry, DLQ after 5 receives

    subject = (
        f"Public Records Request — {campaign.config.record_type} — "
        f"{jurisdiction.name}, {jurisdiction.state} [DLG-{thread_token}]"
    )
    email = OutboundEmail(
        from_address=world.from_address,
        to_address=msg.contact_email,
        subject=subject,
        body=assemble_body(campaign, drafted, world.from_address),
        headers={TOKEN_HEADER: thread_token},
    )
    try:
        resend_id = _deliver(world, campaign, email)
    except SendTransientError:
        return False

    world.store.record_initial_send(
        campaign_id=campaign.id,
        jurisdiction_id=jurisdiction.id,
        thread_token=thread_token,
        contact_email=msg.contact_email,
        parent_thread_id=msg.parent_thread_id,
        existing_thread_id=existing_thread_id,
        from_address=world.from_address,
        to_address=msg.contact_email,
        subject=subject,
        body=email.body,
        resend_id=resend_id,
        next_action_at=next_action_time(world, campaign),
        now=now,
        # a test campaign never actually contacted this office
        stamp_cooldown=not campaign.is_test,
    )
    return True


# --------------------------------------------------------------------------
# §7.2 Thread jobs (pr-followups)


def handle_followup_job(world: World, body: str) -> bool:
    msg = FollowupJobMessage.from_json(body)
    thread = world.store.get_thread(msg.thread_id)
    if thread is None:
        return True  # purged campaign: drop
    campaign = world.store.get_campaign(thread.campaign_id)
    if campaign is None:
        return True
    if thread.status not in (ThreadStatus.REQUEST_SENT, ThreadStatus.AWAITING_REPLY):
        return True  # thread moved on (fulfilled / referred / parked)

    if msg.kind == "followup":
        return _send_followup(world, campaign, thread_id=thread.id, msg=msg)
    return _send_reply(world, campaign, thread_id=thread.id, msg=msg)


def _send_followup(world: World, campaign: Campaign, thread_id: int,
                   msg: FollowupJobMessage) -> bool:
    thread = world.store.get_thread(thread_id)
    assert thread is not None
    # idempotency: a duplicate whose index is behind the counter is dropped
    if msg.followup_index is not None and msg.followup_index < thread.followups_sent:
        return True

    interval = campaign.config.limits.followup_interval_days
    original_subject = (
        world.store.first_outbound_subject(thread.id)
        or f"Public Records Request [DLG-{thread.thread_token}]"
    )
    waited_days = interval * (thread.followups_sent + 1)
    try:
        drafted = world.drafter.draft_followup(campaign, original_subject, waited_days)
    except DraftError:
        return False

    email = OutboundEmail(
        from_address=world.from_address,
        to_address=thread.contact_email,
        subject=_reply_subject(original_subject, thread.thread_token),
        body=drafted,
        headers={TOKEN_HEADER: thread.thread_token},
    )
    try:
        resend_id = _deliver(world, campaign, email)
    except SendTransientError:
        return False

    world.store.record_thread_send(
        thread_id=thread.id,
        kind=OutboundKind.FOLLOWUP,
        from_address=email.from_address,
        to_address=email.to_address,
        subject=email.subject,
        body=email.body,
        resend_id=resend_id,
        in_reply_to_email_id=None,
        new_status=ThreadStatus.AWAITING_REPLY,
        next_action_at=thread.next_action_at,  # the scan already rescheduled
        increment_followups=True,
        now=world.clock.now(),
    )
    return True


def _send_reply(world: World, campaign: Campaign, thread_id: int,
                msg: FollowupJobMessage) -> bool:
    thread = world.store.get_thread(thread_id)
    assert thread is not None
    kind = (
        OutboundKind.CLARIFICATION_REPLY
        if msg.kind == "clarification_reply"
        else OutboundKind.FEE_AGREEMENT
    )
    # idempotency: a second message for the same inbound is dropped once an
    # outbound row references it
    if msg.inbound_email_id is not None and world.store.outbound_reply_exists(
        thread.id, msg.inbound_email_id, kind
    ):
        return True

    inbound_subject = ""
    inbound_body = ""
    if msg.inbound_email_id is not None:
        inbound = world.store.get_email(msg.inbound_email_id)
        if inbound is not None:
            inbound_subject = inbound.subject
            inbound_body = inbound.body

    try:
        if kind is OutboundKind.CLARIFICATION_REPLY:
            drafted = world.drafter.draft_clarification(
                campaign, inbound_body[:CLARIFICATION_CONTEXT_CHARS]
            )
        else:
            if msg.amount_cents is None:
                log.error("fee_agreement job without amount_cents: thread %s", thread.id)
                return True  # malformed: drop (the ledger row already exists)
            drafted = world.drafter.draft_fee_agreement(
                campaign, msg.amount_cents, inbound_body[:CLARIFICATION_CONTEXT_CHARS]
            )
    except DraftError:
        return False

    email = OutboundEmail(
        from_address=world.from_address,
        to_address=thread.contact_email,
        subject=_reply_subject(inbound_subject, thread.thread_token),
        body=drafted,
        headers={TOKEN_HEADER: thread.thread_token},
    )
    try:
        resend_id = _deliver(world, campaign, email)
    except SendTransientError:
        return False

    world.store.record_thread_send(
        thread_id=thread.id,
        kind=kind,
        from_address=email.from_address,
        to_address=email.to_address,
        subject=email.subject,
        body=email.body,
        resend_id=resend_id,
        in_reply_to_email_id=msg.inbound_email_id,
        new_status=ThreadStatus.AWAITING_REPLY,
        next_action_at=next_action_time(world, campaign),
        increment_followups=False,
        now=world.clock.now(),
    )
    return True
