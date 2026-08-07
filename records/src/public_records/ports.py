"""Public-records-specific ports.

Generic infrastructure (Clock, TaskQueue, KeyValue, ObjectStore,
SearchProvider, Fetcher) is harvest_core's — reused, not forked. This
module adds the store and the campaign-mode LLM / transport surface.
Fakes live in `public_records.fakes`, real adapters in
`public_records.adapters`; `_protocol_checks.py` verifies conformance.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from .config import CampaignConfig
from .domain import (
    Campaign,
    Classification,
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
)

# --------------------------------------------------------------------------
# Email transport (Resend)


@dataclass
class OutboundEmail:
    from_address: str
    to_address: str
    subject: str
    body: str
    headers: dict[str, str] = field(default_factory=dict)


class EmailTransport(Protocol):
    def send(self, email: OutboundEmail) -> str:
        """Deliver and return the provider message id.

        Raises SendTransientError on timeout / 5xx / 429 — the SQS
        message is left undeleted and retried."""
        ...


# --------------------------------------------------------------------------
# LLMs: luna (queries + contact pick), terra (drafts + classification)


class ContactQueryGenerator(Protocol):
    def generate_queries(self, jurisdiction: Jurisdiction, record_type: str) -> list[str]:
        """Search queries aimed at the office's records-request email.

        Raises GenerationError on API/parse failure; the caller degrades
        to the deterministic fallback query (§5.1)."""
        ...


@dataclass
class EmailCandidate:
    email: str
    context: str
    source_url: str


@dataclass
class ContactPick:
    email: str | None
    confidence: float = 0.0


class ContactPicker(Protocol):
    def pick(self, jurisdiction: Jurisdiction, candidates: list[EmailCandidate]) -> ContactPick:
        """Pick one of the listed addresses or null when in doubt.

        Raises PickError on API failure (transient). A parse failure is
        a null pick, not an error."""
        ...


class EmailDrafter(Protocol):
    """All methods raise DraftError on API/parse failure (transient)."""

    def draft_initial(self, campaign: Campaign, jurisdiction: Jurisdiction) -> str:
        """Body text only: begins with the salutation, no subject, no
        sign-off, no restatement of the records scope (§7.1)."""
        ...

    def draft_followup(
        self, campaign: Campaign, original_subject: str, waited_days: int
    ) -> str: ...

    def draft_clarification(self, campaign: Campaign, inbound_body: str) -> str: ...

    def draft_fee_agreement(
        self, campaign: Campaign, amount_cents: int, inbound_body: str
    ) -> str: ...


class InboundClassifier(Protocol):
    def classify(self, subject: str, body: str) -> Classification:
        """Terra with reasoning enabled. Raises ClassifyError on API
        failure only; parse failures / unknown categories degrade to
        UNCLEAR inside the adapter (§8)."""
        ...


# --------------------------------------------------------------------------
# Datastore (Postgres)


class RecordsStore(Protocol):
    # -- campaigns
    def insert_campaign(self, config: CampaignConfig, created_at: datetime) -> Campaign:
        """Unique on name; raises UniqueViolation."""
        ...

    def get_campaign(self, campaign_id: int) -> Campaign | None: ...

    def get_campaign_by_name(self, name: str) -> Campaign | None: ...

    def list_campaigns(self) -> list[Campaign]: ...

    def set_campaign_active(self, campaign_id: int, active: bool) -> None: ...

    def set_campaign_seeded(self, campaign_id: int) -> None: ...

    def update_campaign_config(self, campaign_id: int, config: CampaignConfig) -> None: ...

    def count_outbound_since(self, campaign_id: int, since: datetime) -> int:
        """Outbound emails rows of any kind since `since` (the daily cap)."""
        ...

    # -- jurisdictions
    def insert_jurisdiction(
        self, name: str, state: str, level: str, parent_name: str | None = None
    ) -> Jurisdiction:
        """Unique on (level, state, name); raises UniqueViolation."""
        ...

    def get_jurisdiction(self, jurisdiction_id: int) -> Jurisdiction | None: ...

    def list_jurisdictions(
        self, states: list[str] | None = None, levels: list[str] | None = None
    ) -> list[Jurisdiction]: ...

    def state_row_exists(self, state: str) -> bool: ...

    def set_jurisdiction_contact(
        self, jurisdiction_id: int, email: str, name: str | None, url: str | None
    ) -> None:
        """Writes contact_verified = false — reserved for humans."""
        ...

    def stamp_last_contacted(self, jurisdiction_id: int, at: datetime) -> None: ...

    # -- search targets
    def insert_search_target(
        self, campaign_id: int, jurisdiction_id: int, created_at: datetime
    ) -> SearchTarget:
        """Unique on (campaign_id, jurisdiction_id); raises UniqueViolation."""
        ...

    def get_search_target(self, target_id: int) -> SearchTarget | None: ...

    def find_search_target(
        self, campaign_id: int, jurisdiction_id: int
    ) -> SearchTarget | None: ...

    def set_target_queries_enqueued(self, target_id: int, count: int) -> None: ...

    def resolve_target(self, target_id: int) -> bool:
        """Compare-and-set resolved=true; False when already resolved —
        the duplicate-message guard for every scraper outcome."""
        ...

    def mark_query_consumed(self, target_id: int, query_index: int) -> bool:
        """Record one consumed query (idempotent per index). True when
        every enqueued query has now been consumed."""
        ...

    # -- threads
    def get_thread(self, thread_id: int) -> EmailThread | None: ...

    def get_thread_by_token(self, token: str) -> EmailThread | None: ...

    def find_thread(
        self, campaign_id: int, jurisdiction_id: int, contact_email: str
    ) -> EmailThread | None: ...

    def find_open_thread_by_contact(self, contact_email: str) -> EmailThread | None:
        """An open (request_sent / awaiting_reply) thread mailing this address."""
        ...

    def set_thread_status(
        self,
        thread_id: int,
        status: ThreadStatus,
        next_action_at: datetime | None,
        *,
        by_human: bool = False,
    ) -> None:
        """Validates the §4 transition; raises IllegalTransition."""
        ...

    def select_due_followups(self, now: datetime, limit: int) -> list[EmailThread]:
        """status in (request_sent, awaiting_reply) and next_action_at <= now."""
        ...

    def append_attachment_key(self, thread_id: int, key: str) -> None: ...

    # -- emails
    def has_source_key(self, source_key: str) -> bool: ...

    def has_message_id(self, campaign_id: int | None, message_id: str) -> bool:
        """Inbound dedupe: unique per campaign (campaign null = unmatched)."""
        ...

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
        """Raises UniqueViolation on a duplicate (campaign, message_id)."""
        ...

    def get_email(self, email_id: int) -> EmailRecord | None: ...

    def update_attachment_refs(self, email_id: int, refs: list[dict[str, str]]) -> None: ...

    def first_outbound_subject(self, thread_id: int) -> str | None: ...

    def outbound_reply_exists(
        self, thread_id: int, inbound_email_id: int, kind: OutboundKind
    ) -> bool:
        """An outbound row of this kind already references the inbound."""
        ...

    def list_emails(
        self, thread_id: int | None = None, campaign_id: int | None = None
    ) -> list[EmailRecord]: ...

    # -- composite send commits (§7: state commits with the action)
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
        """One transaction: create the thread (or promote an existing
        pending_send one), the outbound emails row, and stamp the
        jurisdiction's last_contacted_at."""
        ...

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
        """One transaction: outbound emails row + thread status/counter."""
        ...

    # -- escalations
    def insert_escalation(
        self,
        campaign_id: int | None,
        thread_id: int | None,
        reason: EscalationReason,
        details: str,
        created_at: datetime,
    ) -> Escalation: ...

    def get_escalation(self, escalation_id: int) -> Escalation | None: ...

    def list_escalations(
        self,
        campaign_id: int | None = None,
        status: EscalationStatus | None = None,
    ) -> list[Escalation]: ...

    def resolve_escalation(
        self, escalation_id: int, resolution: str, resolved_at: datetime
    ) -> None: ...

    def unnotified_escalations(self, campaign_id: int) -> list[Escalation]: ...

    def mark_escalations_notified(self, escalation_ids: Iterable[int]) -> None: ...

    # -- spend (the fee ledger)
    def book_fee(
        self,
        campaign_id: int,
        thread_id: int | None,
        amount_cents: int,
        budget_cents: int,
        note: str,
        created_at: datetime,
    ) -> SpendEntry | None:
        """Atomic check-and-book: None when committed + amount > budget."""
        ...

    def committed_total_cents(self, campaign_id: int) -> int: ...

    def list_spend(self, campaign_id: int) -> list[SpendEntry]: ...

    def unnotified_spend(self, campaign_id: int) -> list[SpendEntry]: ...

    def mark_spend_notified(self, spend_ids: Iterable[int]) -> None: ...

    # -- campaign kill (§12): FK-ordered purge
    def purge_campaign(self, campaign_id: int) -> dict[str, int]: ...
