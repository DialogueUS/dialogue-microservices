"""The fee flow (§9), carried over intact.

Auto-agree only when: an amount parsed, the budget is nonzero, and
committed + amount fits. Money is integer cents, booked to the ledger
before the agreement email is enqueued; remittance is always manual.
"""

from __future__ import annotations

import re

from .constants import FEE_NOTE_CHARS
from .domain import Campaign, EmailRecord, EmailThread, EscalationReason, ThreadStatus
from .messages import FollowupJobMessage
from .world import World, escalate, next_action_time

# $ amounts up to 6 digits, commas and cents tolerated
_AMOUNT_RE = re.compile(r"\$\s*([0-9][0-9,]{0,7})(?:\.([0-9]{1,2}))?")


def parse_largest_amount_cents(text: str) -> int | None:
    """The largest dollar amount in the message, as integer cents."""
    best: int | None = None
    for match in _AMOUNT_RE.finditer(text):
        dollars = match.group(1).replace(",", "")
        if len(dollars) > 6:
            continue
        cents_part = (match.group(2) or "").ljust(2, "0")
        amount = int(dollars) * 100 + int(cents_part or "0")
        if best is None or amount > best:
            best = amount
    return best


def handle_payment_required(
    world: World, campaign: Campaign, thread: EmailThread, inbound: EmailRecord
) -> None:
    amount = parse_largest_amount_cents(inbound.body)
    budget = campaign.config.limits.fee_budget_cents

    if amount is None:
        escalate(
            world, campaign.id, thread.id, EscalationReason.PAYMENT_REQUIRED,
            "Payment required but no clear amount to authorize.\n\n" + inbound.body,
        )
        return
    if budget <= 0:
        escalate(
            world, campaign.id, thread.id, EscalationReason.PAYMENT_REQUIRED,
            f"Payment of ${amount / 100:.2f} required but no fee budget is "
            "configured.\n\n" + inbound.body,
        )
        return

    entry = world.store.book_fee(
        campaign.id,
        thread.id,
        amount,
        budget,
        inbound.body[:FEE_NOTE_CHARS],
        world.clock.now(),
    )
    if entry is None:
        committed = world.store.committed_total_cents(campaign.id)
        escalate(
            world, campaign.id, thread.id, EscalationReason.PAYMENT_REQUIRED,
            f"Fee ${amount / 100:.2f} exceeds remaining budget: "
            f"${committed / 100:.2f} of ${budget / 100:.2f} already committed.\n\n"
            + inbound.body,
        )
        return

    # ledger booked; now the agreement email and the thread state
    world.followups_queue.send(
        FollowupJobMessage(
            thread_id=thread.id,
            kind="fee_agreement",
            inbound_email_id=inbound.id,
            amount_cents=amount,
        ).to_json()
    )
    world.store.set_thread_status(
        thread.id, ThreadStatus.AWAITING_REPLY, next_action_time(world, campaign)
    )
