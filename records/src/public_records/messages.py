"""The four SQS message schemas (§2.1). One schema per queue.

Bodies are JSON; unknown fields are rejected so a message landing on
the wrong queue fails loudly at parse time rather than half-working.
"""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict


class _Message(BaseModel):
    model_config = ConfigDict(extra="forbid")

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, body: str) -> Self:
        return cls.model_validate_json(body)


class SearchQueryMessage(_Message):
    """orchestrator -> scraper (pr-search-queries)."""

    campaign_id: int
    jurisdiction_id: int
    search_target_id: int
    query: str
    query_index: int


ContactSource = Literal["scraper", "seeded", "referral", "human_approved"]


class ContactMessage(_Message):
    """scraper (also receiver, on referral) -> email sender (pr-contacts)."""

    campaign_id: int
    jurisdiction_id: int
    contact_email: str
    source: ContactSource
    bypass_cooldown: bool = False
    parent_thread_id: int | None = None  # referral lineage


FollowupKind = Literal["followup", "clarification_reply", "fee_agreement"]


class FollowupJobMessage(_Message):
    """receiver (reactive) / orchestrator (silence) -> sender (pr-followups)."""

    thread_id: int
    kind: FollowupKind
    followup_index: int | None = None  # followups_sent at enqueue time
    inbound_email_id: int | None = None  # the email being answered, if any
    amount_cents: int | None = None  # fee_agreement only


class InboundAttachment(_Message):
    filename: str
    content_type: str
    s3_tmp_key: str


class InboundMailMessage(_Message):
    """orchestrator mail poller -> email receiver (pr-inbound-mail)."""

    source_key: str
    message_id: str
    thread_token: str | None = None
    from_address: str
    subject: str
    body: str
    attachments: list[InboundAttachment] = []
