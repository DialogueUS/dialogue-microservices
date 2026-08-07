"""The notification digest (§12): one plain-text email per campaign per
period covering un-notified escalations and spend entries, each row
alerted exactly once."""

from __future__ import annotations

import logging
from collections import Counter

from .constants import DIGEST_KIND_HEADER, DIGEST_MAX_LINE_ITEMS, DIGEST_TOKEN, TOKEN_HEADER
from .domain import Campaign, Escalation, SpendEntry
from .errors import SendTransientError
from .ports import OutboundEmail
from .world import World

log = logging.getLogger("public_records.digest")


def _format_digest(
    campaign: Campaign, escalations: list[Escalation], spend: list[SpendEntry], committed: int
) -> str:
    lines = [f"Dialogue digest for campaign {campaign.name!r}", ""]
    if escalations:
        counts = Counter(e.reason.api_value for e in escalations)
        lines.append(f"Open escalations ({len(escalations)}):")
        for reason, count in sorted(counts.items()):
            lines.append(f"  {reason}: {count}")
        for esc in escalations[:DIGEST_MAX_LINE_ITEMS]:
            summary = esc.details.splitlines()[0][:120] if esc.details else ""
            lines.append(f"  - #{esc.id} [{esc.reason.api_value}] thread={esc.thread_id}"
                         f" {summary}")
        if len(escalations) > DIGEST_MAX_LINE_ITEMS:
            lines.append(f"  … and {len(escalations) - DIGEST_MAX_LINE_ITEMS} more")
        lines.append("")
    if spend:
        lines.append(f"New fees authorized ({len(spend)}):")
        for entry in spend[:DIGEST_MAX_LINE_ITEMS]:
            lines.append(
                f"  - #{entry.id} ${entry.amount_cents / 100:.2f} thread={entry.thread_id}"
            )
        if len(spend) > DIGEST_MAX_LINE_ITEMS:
            lines.append(f"  … and {len(spend) - DIGEST_MAX_LINE_ITEMS} more")
        lines.append("")
    budget_cents = campaign.config.limits.fee_budget_cents
    remaining = budget_cents - committed
    lines.append(
        f"Fee budget: ${committed / 100:.2f} committed of ${budget_cents / 100:.2f}"
        f" (${max(remaining, 0) / 100:.2f} remaining)"
    )
    if budget_cents > 0 and remaining <= 0:
        lines.append("WARNING: budget EXHAUSTED")
    return "\n".join(lines)


def send_digests(world: World) -> int:
    """Returns the number of digests sent. Rows are marked notified only
    after a successful send — a transport failure retries next period."""
    sent = 0
    for campaign in world.store.list_campaigns():
        if not campaign.config.notify_email:
            continue
        escalations = world.store.unnotified_escalations(campaign.id)
        spend = world.store.unnotified_spend(campaign.id)
        if not escalations and not spend:
            continue
        body = _format_digest(
            campaign, escalations, spend, world.store.committed_total_cents(campaign.id)
        )
        email = OutboundEmail(
            from_address=world.from_address,
            to_address=campaign.config.notify_email,
            subject=f"Dialogue digest — {campaign.name} [DLG-{DIGEST_TOKEN}]",
            body=body,
            headers={TOKEN_HEADER: DIGEST_TOKEN, DIGEST_KIND_HEADER: "alert"},
        )
        try:
            world.transport.send(email)
        except SendTransientError as exc:
            log.warning("digest send failed for %s: %s", campaign.name, exc)
            continue
        world.store.mark_escalations_notified([e.id for e in escalations])
        world.store.mark_spend_notified([s.id for s in spend])
        sent += 1
    return sent
