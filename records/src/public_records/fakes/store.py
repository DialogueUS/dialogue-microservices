"""FakeRecordsStore: in-memory Postgres stand-in with honest semantics.

Unique constraints raise the same UniqueViolation the SQL adapter maps;
conditional writes (resolve_target, thread transitions) behave exactly
like their compare-and-set SQL counterparts.
"""

from __future__ import annotations

import itertools
import threading
from collections.abc import Iterable
from dataclasses import replace
from datetime import datetime

from ..config import CampaignConfig
from ..domain import (
    Campaign,
    Classification,
    EmailDirection,
    EmailRecord,
    EmailThread,
    Escalation,
    EscalationReason,
    EscalationStatus,
    Jurisdiction,
    OutboundKind,
    SearchTarget,
    SpendEntry,
    ThreadStatus,
    check_transition,
)
from ..errors import UniqueViolation


class FakeRecordsStore:
    def __init__(self) -> None:
        self._campaigns: dict[int, Campaign] = {}
        self._jurisdictions: dict[int, Jurisdiction] = {}
        self._targets: dict[int, SearchTarget] = {}
        self._threads: dict[int, EmailThread] = {}
        self._emails: dict[int, EmailRecord] = {}
        self._escalations: dict[int, Escalation] = {}
        self._spend: dict[int, SpendEntry] = {}
        self._next = {
            "campaign": itertools.count(1),
            "jurisdiction": itertools.count(1),
            "target": itertools.count(1),
            "thread": itertools.count(1),
            "email": itertools.count(1),
            "escalation": itertools.count(1),
            "spend": itertools.count(1),
        }
        self._lock = threading.RLock()

    # -- campaigns ---------------------------------------------------------
    def insert_campaign(self, config: CampaignConfig, created_at: datetime) -> Campaign:
        with self._lock:
            if any(c.name == config.name for c in self._campaigns.values()):
                raise UniqueViolation(f"campaign name {config.name!r}")
            campaign = Campaign(
                id=next(self._next["campaign"]),
                config=config,
                active=False,
                seeded=False,
                created_at=created_at,
            )
            self._campaigns[campaign.id] = campaign
            return campaign

    def get_campaign(self, campaign_id: int) -> Campaign | None:
        with self._lock:
            return self._campaigns.get(campaign_id)

    def get_campaign_by_name(self, name: str) -> Campaign | None:
        with self._lock:
            for c in self._campaigns.values():
                if c.name == name:
                    return c
            return None

    def list_campaigns(self) -> list[Campaign]:
        with self._lock:
            return sorted(self._campaigns.values(), key=lambda c: c.id)

    def set_campaign_active(self, campaign_id: int, active: bool) -> None:
        with self._lock:
            self._campaigns[campaign_id].active = active

    def set_campaign_seeded(self, campaign_id: int) -> None:
        with self._lock:
            self._campaigns[campaign_id].seeded = True

    def update_campaign_config(self, campaign_id: int, config: CampaignConfig) -> None:
        with self._lock:
            self._campaigns[campaign_id].config = config

    def count_outbound_since(self, campaign_id: int, since: datetime) -> int:
        with self._lock:
            return sum(
                1
                for e in self._emails.values()
                if e.campaign_id == campaign_id
                and e.direction is EmailDirection.OUTBOUND
                and e.created_at is not None
                and e.created_at >= since
            )

    # -- jurisdictions -----------------------------------------------------
    def insert_jurisdiction(
        self, name: str, state: str, level: str, parent_name: str | None = None
    ) -> Jurisdiction:
        with self._lock:
            for j in self._jurisdictions.values():
                if (j.level, j.state, j.name) == (level, state, name):
                    raise UniqueViolation(f"jurisdiction ({level}, {state}, {name})")
            j = Jurisdiction(
                id=next(self._next["jurisdiction"]),
                name=name,
                state=state,
                level=level,
                parent_name=parent_name,
            )
            self._jurisdictions[j.id] = j
            return j

    def get_jurisdiction(self, jurisdiction_id: int) -> Jurisdiction | None:
        with self._lock:
            return self._jurisdictions.get(jurisdiction_id)

    def list_jurisdictions(
        self, states: list[str] | None = None, levels: list[str] | None = None
    ) -> list[Jurisdiction]:
        with self._lock:
            out = [
                j
                for j in self._jurisdictions.values()
                if (states is None or j.state in states)
                and (levels is None or j.level in levels)
            ]
            return sorted(out, key=lambda j: j.id)

    def state_row_exists(self, state: str) -> bool:
        with self._lock:
            return any(
                j.level == "state" and j.state == state for j in self._jurisdictions.values()
            )

    def set_jurisdiction_contact(
        self, jurisdiction_id: int, email: str, name: str | None, url: str | None
    ) -> None:
        with self._lock:
            j = self._jurisdictions[jurisdiction_id]
            j.contact_email = email
            j.contact_name = name
            j.contact_url = url
            j.contact_verified = False

    def stamp_last_contacted(self, jurisdiction_id: int, at: datetime) -> None:
        with self._lock:
            self._jurisdictions[jurisdiction_id].last_contacted_at = at

    # -- search targets ------------------------------------------------------
    def insert_search_target(
        self, campaign_id: int, jurisdiction_id: int, created_at: datetime
    ) -> SearchTarget:
        with self._lock:
            for t in self._targets.values():
                if (t.campaign_id, t.jurisdiction_id) == (campaign_id, jurisdiction_id):
                    raise UniqueViolation(f"search_target ({campaign_id}, {jurisdiction_id})")
            t = SearchTarget(
                id=next(self._next["target"]),
                campaign_id=campaign_id,
                jurisdiction_id=jurisdiction_id,
                created_at=created_at,
            )
            self._targets[t.id] = t
            return t

    def get_search_target(self, target_id: int) -> SearchTarget | None:
        with self._lock:
            t = self._targets.get(target_id)
            return replace(t, consumed_indexes=list(t.consumed_indexes)) if t else None

    def find_search_target(
        self, campaign_id: int, jurisdiction_id: int
    ) -> SearchTarget | None:
        with self._lock:
            for t in self._targets.values():
                if (t.campaign_id, t.jurisdiction_id) == (campaign_id, jurisdiction_id):
                    return replace(t, consumed_indexes=list(t.consumed_indexes))
            return None

    def set_target_queries_enqueued(self, target_id: int, count: int) -> None:
        with self._lock:
            self._targets[target_id].queries_enqueued = count

    def resolve_target(self, target_id: int) -> bool:
        with self._lock:
            t = self._targets[target_id]
            if t.resolved:
                return False
            t.resolved = True
            return True

    def mark_query_consumed(self, target_id: int, query_index: int) -> bool:
        with self._lock:
            t = self._targets[target_id]
            if query_index not in t.consumed_indexes:
                t.consumed_indexes.append(query_index)
            return t.queries_enqueued > 0 and len(t.consumed_indexes) >= t.queries_enqueued

    # -- threads -------------------------------------------------------------
    def get_thread(self, thread_id: int) -> EmailThread | None:
        with self._lock:
            t = self._threads.get(thread_id)
            return replace(t, attachment_keys=list(t.attachment_keys)) if t else None

    def get_thread_by_token(self, token: str) -> EmailThread | None:
        with self._lock:
            for t in self._threads.values():
                if t.thread_token == token:
                    return replace(t, attachment_keys=list(t.attachment_keys))
            return None

    def find_thread(
        self, campaign_id: int, jurisdiction_id: int, contact_email: str
    ) -> EmailThread | None:
        with self._lock:
            for t in self._threads.values():
                if (t.campaign_id, t.jurisdiction_id, t.contact_email) == (
                    campaign_id,
                    jurisdiction_id,
                    contact_email,
                ):
                    return replace(t, attachment_keys=list(t.attachment_keys))
            return None

    def find_open_thread_by_contact(self, contact_email: str) -> EmailThread | None:
        with self._lock:
            for t in self._threads.values():
                if t.contact_email == contact_email and t.status in (
                    ThreadStatus.REQUEST_SENT,
                    ThreadStatus.AWAITING_REPLY,
                ):
                    return replace(t, attachment_keys=list(t.attachment_keys))
            return None

    def set_thread_status(
        self,
        thread_id: int,
        status: ThreadStatus,
        next_action_at: datetime | None,
        *,
        by_human: bool = False,
    ) -> None:
        with self._lock:
            t = self._threads[thread_id]
            check_transition(t.status, status, by_human=by_human)
            t.status = status
            t.next_action_at = next_action_at

    def select_due_followups(self, now: datetime, limit: int) -> list[EmailThread]:
        with self._lock:
            due = [
                t
                for t in self._threads.values()
                if t.status in (ThreadStatus.REQUEST_SENT, ThreadStatus.AWAITING_REPLY)
                and t.next_action_at is not None
                and t.next_action_at <= now
            ]
            due.sort(key=lambda t: (t.next_action_at, t.id))  # type: ignore[arg-type,return-value]
            return [replace(t, attachment_keys=list(t.attachment_keys)) for t in due[:limit]]

    def append_attachment_key(self, thread_id: int, key: str) -> None:
        with self._lock:
            self._threads[thread_id].attachment_keys.append(key)

    # -- emails ----------------------------------------------------------------
    def has_source_key(self, source_key: str) -> bool:
        with self._lock:
            return any(e.source_key == source_key for e in self._emails.values())

    def has_message_id(self, campaign_id: int | None, message_id: str) -> bool:
        with self._lock:
            return any(
                e.message_id == message_id and e.campaign_id == campaign_id
                for e in self._emails.values()
            )

    def insert_email(
        self,
        *,
        thread_id: int | None,
        campaign_id: int | None,
        direction: str,
        from_address: str,
        to_address: str,
        subject: str,
        body: str,
        kind: OutboundKind | None,
        classification: Classification | None,
        message_id: str | None,
        source_key: str | None,
        resend_id: str | None,
        in_reply_to_email_id: int | None,
        attachment_refs: list[dict[str, str]],
        created_at: datetime,
    ) -> EmailRecord:
        with self._lock:
            if message_id is not None and self.has_message_id(campaign_id, message_id):
                raise UniqueViolation(f"message_id {message_id!r} for campaign {campaign_id}")
            e = EmailRecord(
                id=next(self._next["email"]),
                thread_id=thread_id,
                campaign_id=campaign_id,
                direction=EmailDirection(direction.upper()),
                from_address=from_address,
                to_address=to_address,
                subject=subject,
                body=body,
                kind=kind,
                classification=classification,
                message_id=message_id,
                source_key=source_key,
                resend_id=resend_id,
                in_reply_to_email_id=in_reply_to_email_id,
                attachment_refs=list(attachment_refs),
                created_at=created_at,
            )
            self._emails[e.id] = e
            return e

    def get_email(self, email_id: int) -> EmailRecord | None:
        with self._lock:
            return self._emails.get(email_id)

    def update_attachment_refs(self, email_id: int, refs: list[dict[str, str]]) -> None:
        with self._lock:
            self._emails[email_id].attachment_refs = list(refs)

    def first_outbound_subject(self, thread_id: int) -> str | None:
        with self._lock:
            outbound = [
                e
                for e in self._emails.values()
                if e.thread_id == thread_id and e.direction is EmailDirection.OUTBOUND
            ]
            outbound.sort(key=lambda e: e.id)
            return outbound[0].subject if outbound else None

    def outbound_reply_exists(
        self, thread_id: int, inbound_email_id: int, kind: OutboundKind
    ) -> bool:
        with self._lock:
            return any(
                e.thread_id == thread_id
                and e.direction is EmailDirection.OUTBOUND
                and e.kind is kind
                and e.in_reply_to_email_id == inbound_email_id
                for e in self._emails.values()
            )

    def list_emails(
        self, thread_id: int | None = None, campaign_id: int | None = None
    ) -> list[EmailRecord]:
        with self._lock:
            out = [
                e
                for e in self._emails.values()
                if (thread_id is None or e.thread_id == thread_id)
                and (campaign_id is None or e.campaign_id == campaign_id)
            ]
            return sorted(out, key=lambda e: e.id)

    # -- composite send commits -------------------------------------------------
    def record_initial_send(
        self,
        *,
        campaign_id: int,
        jurisdiction_id: int,
        thread_token: str,
        contact_email: str,
        parent_thread_id: int | None,
        existing_thread_id: int | None,
        from_address: str,
        to_address: str,
        subject: str,
        body: str,
        resend_id: str | None,
        next_action_at: datetime,
        now: datetime,
    ) -> tuple[EmailThread, EmailRecord]:
        with self._lock:
            if existing_thread_id is not None:
                thread = self._threads[existing_thread_id]
                check_transition(thread.status, ThreadStatus.REQUEST_SENT)
                thread.status = ThreadStatus.REQUEST_SENT
                thread.contact_email = contact_email
                thread.next_action_at = next_action_at
                thread.updated_at = now
            else:
                if any(t.thread_token == thread_token for t in self._threads.values()):
                    raise UniqueViolation(f"thread_token {thread_token!r}")
                thread = EmailThread(
                    id=next(self._next["thread"]),
                    campaign_id=campaign_id,
                    jurisdiction_id=jurisdiction_id,
                    thread_token=thread_token,
                    contact_email=contact_email,
                    status=ThreadStatus.REQUEST_SENT,
                    parent_thread_id=parent_thread_id,
                    next_action_at=next_action_at,
                    created_at=now,
                    updated_at=now,
                )
                self._threads[thread.id] = thread
            email = self.insert_email(
                thread_id=thread.id,
                campaign_id=campaign_id,
                direction="outbound",
                from_address=from_address,
                to_address=to_address,
                subject=subject,
                body=body,
                kind=OutboundKind.INITIAL_REQUEST,
                classification=None,
                message_id=None,
                source_key=None,
                resend_id=resend_id,
                in_reply_to_email_id=None,
                attachment_refs=[],
                created_at=now,
            )
            self._jurisdictions[jurisdiction_id].last_contacted_at = now
            return (
                replace(thread, attachment_keys=list(thread.attachment_keys)),
                email,
            )

    def record_thread_send(
        self,
        *,
        thread_id: int,
        kind: OutboundKind,
        from_address: str,
        to_address: str,
        subject: str,
        body: str,
        resend_id: str | None,
        in_reply_to_email_id: int | None,
        new_status: ThreadStatus,
        next_action_at: datetime | None,
        increment_followups: bool,
        now: datetime,
    ) -> EmailRecord:
        with self._lock:
            thread = self._threads[thread_id]
            check_transition(thread.status, new_status)
            email = self.insert_email(
                thread_id=thread_id,
                campaign_id=thread.campaign_id,
                direction="outbound",
                from_address=from_address,
                to_address=to_address,
                subject=subject,
                body=body,
                kind=kind,
                classification=None,
                message_id=None,
                source_key=None,
                resend_id=resend_id,
                in_reply_to_email_id=in_reply_to_email_id,
                attachment_refs=[],
                created_at=now,
            )
            thread.status = new_status
            thread.next_action_at = next_action_at
            thread.updated_at = now
            if increment_followups:
                thread.followups_sent += 1
            return email

    # -- escalations -------------------------------------------------------------
    def insert_escalation(
        self,
        campaign_id: int | None,
        thread_id: int | None,
        reason: EscalationReason,
        details: str,
        created_at: datetime,
    ) -> Escalation:
        with self._lock:
            esc = Escalation(
                id=next(self._next["escalation"]),
                campaign_id=campaign_id,
                thread_id=thread_id,
                reason=reason,
                details=details,
                created_at=created_at,
            )
            self._escalations[esc.id] = esc
            return esc

    def get_escalation(self, escalation_id: int) -> Escalation | None:
        with self._lock:
            return self._escalations.get(escalation_id)

    def list_escalations(
        self,
        campaign_id: int | None = None,
        status: EscalationStatus | None = None,
    ) -> list[Escalation]:
        with self._lock:
            out = [
                e
                for e in self._escalations.values()
                if (campaign_id is None or e.campaign_id == campaign_id)
                and (status is None or e.status is status)
            ]
            return sorted(out, key=lambda e: e.id)

    def resolve_escalation(
        self, escalation_id: int, resolution: str, resolved_at: datetime
    ) -> None:
        with self._lock:
            esc = self._escalations[escalation_id]
            esc.status = EscalationStatus.RESOLVED
            esc.resolution = resolution
            esc.resolved_at = resolved_at

    def unnotified_escalations(self, campaign_id: int) -> list[Escalation]:
        with self._lock:
            return [
                e
                for e in self.list_escalations(campaign_id=campaign_id)
                if not e.notified and e.status is EscalationStatus.OPEN
            ]

    def mark_escalations_notified(self, escalation_ids: Iterable[int]) -> None:
        with self._lock:
            for eid in escalation_ids:
                self._escalations[eid].notified = True

    # -- spend ---------------------------------------------------------------------
    def book_fee(
        self,
        campaign_id: int,
        thread_id: int | None,
        amount_cents: int,
        budget_cents: int,
        note: str,
        created_at: datetime,
    ) -> SpendEntry | None:
        with self._lock:
            committed = self.committed_total_cents(campaign_id)
            if committed + amount_cents > budget_cents:
                return None
            entry = SpendEntry(
                id=next(self._next["spend"]),
                campaign_id=campaign_id,
                thread_id=thread_id,
                amount_cents=amount_cents,
                note=note,
                created_at=created_at,
            )
            self._spend[entry.id] = entry
            return entry

    def committed_total_cents(self, campaign_id: int) -> int:
        with self._lock:
            return sum(
                s.amount_cents for s in self._spend.values() if s.campaign_id == campaign_id
            )

    def list_spend(self, campaign_id: int) -> list[SpendEntry]:
        with self._lock:
            return sorted(
                (s for s in self._spend.values() if s.campaign_id == campaign_id),
                key=lambda s: s.id,
            )

    def unnotified_spend(self, campaign_id: int) -> list[SpendEntry]:
        with self._lock:
            return [s for s in self.list_spend(campaign_id) if not s.notified]

    def mark_spend_notified(self, spend_ids: Iterable[int]) -> None:
        with self._lock:
            for sid in spend_ids:
                self._spend[sid].notified = True

    # -- campaign kill -----------------------------------------------------------
    def purge_campaign(self, campaign_id: int) -> dict[str, int]:
        with self._lock:
            thread_ids = {
                t.id for t in self._threads.values() if t.campaign_id == campaign_id
            }
            counts = {"spend_entries": 0, "escalations": 0, "emails": 0, "threads": 0,
                      "search_targets": 0, "campaigns": 0}
            for sid in [s.id for s in self._spend.values() if s.campaign_id == campaign_id]:
                del self._spend[sid]
                counts["spend_entries"] += 1
            for eid in [
                e.id for e in self._escalations.values() if e.campaign_id == campaign_id
            ]:
                del self._escalations[eid]
                counts["escalations"] += 1
            for mid in [
                e.id
                for e in self._emails.values()
                if e.campaign_id == campaign_id or e.thread_id in thread_ids
            ]:
                del self._emails[mid]
                counts["emails"] += 1
            for tid in thread_ids:
                del self._threads[tid]
                counts["threads"] += 1
            for gid in [
                t.id for t in self._targets.values() if t.campaign_id == campaign_id
            ]:
                del self._targets[gid]
                counts["search_targets"] += 1
            if campaign_id in self._campaigns:
                del self._campaigns[campaign_id]
                counts["campaigns"] += 1
            return counts
