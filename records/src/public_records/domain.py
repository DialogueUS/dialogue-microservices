"""Table-row models, the §4 thread state machine, and token/id helpers.

Enum values are persisted as uppercase names in the column and exposed
lowercase in APIs (`.api_value`), matching the old pipeline convention.
"""

from __future__ import annotations

import re
import secrets
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from hashlib import sha256

from harvest_core.storage import slugify as _core_slugify

from .config import CampaignConfig
from .constants import (
    GENERIC_LOCAL_KEYWORDS,
    JUNK_LOCAL_PARTS,
    SLUG_LEN,
    TOKEN_BYTES,
    TOKEN_HEADER,
)
from .errors import IllegalTransition


class _ApiEnum(Enum):
    @property
    def api_value(self) -> str:
        return self.name.lower()


class ThreadStatus(_ApiEnum):
    PENDING_SEND = "PENDING_SEND"
    REQUEST_SENT = "REQUEST_SENT"
    AWAITING_REPLY = "AWAITING_REPLY"
    FULFILLED = "FULFILLED"
    REFERRED = "REFERRED"
    NEEDS_HUMAN = "NEEDS_HUMAN"
    FAILED = "FAILED"


# Automatic transitions the pipeline itself may make. `FAILED` is absent
# everywhere: it exists only for a human to resolve an escalation into.
# Leaving NEEDS_HUMAN likewise requires a human (`by_human=True`).
_AUTO_TRANSITIONS: dict[ThreadStatus, frozenset[ThreadStatus]] = {
    ThreadStatus.PENDING_SEND: frozenset({ThreadStatus.REQUEST_SENT, ThreadStatus.NEEDS_HUMAN}),
    ThreadStatus.REQUEST_SENT: frozenset(
        {
            ThreadStatus.AWAITING_REPLY,
            ThreadStatus.FULFILLED,
            ThreadStatus.REFERRED,
            ThreadStatus.NEEDS_HUMAN,
        }
    ),
    ThreadStatus.AWAITING_REPLY: frozenset(
        {
            ThreadStatus.AWAITING_REPLY,
            ThreadStatus.FULFILLED,
            ThreadStatus.REFERRED,
            ThreadStatus.NEEDS_HUMAN,
        }
    ),
    ThreadStatus.FULFILLED: frozenset(),
    ThreadStatus.REFERRED: frozenset(),
    ThreadStatus.NEEDS_HUMAN: frozenset(),
    ThreadStatus.FAILED: frozenset(),
}


def check_transition(old: ThreadStatus, new: ThreadStatus, *, by_human: bool = False) -> None:
    """Raise IllegalTransition unless old -> new is allowed (§4)."""
    if old is new:
        return
    if by_human:
        return  # a human resolution may set any status
    if new not in _AUTO_TRANSITIONS[old]:
        raise IllegalTransition(f"{old.api_value} -> {new.api_value}")


class EmailDirection(_ApiEnum):
    INBOUND = "INBOUND"
    OUTBOUND = "OUTBOUND"


class OutboundKind(_ApiEnum):
    INITIAL_REQUEST = "INITIAL_REQUEST"
    FOLLOWUP = "FOLLOWUP"
    CLARIFICATION_REPLY = "CLARIFICATION_REPLY"
    FEE_AGREEMENT = "FEE_AGREEMENT"


class EscalationReason(_ApiEnum):
    PAYMENT_REQUIRED = "PAYMENT_REQUIRED"
    REFERRAL_NO_ADDRESS = "REFERRAL_NO_ADDRESS"
    DENIAL = "DENIAL"
    UNCLEAR_REPLY = "UNCLEAR_REPLY"
    NO_RESPONSE = "NO_RESPONSE"
    NO_CONTACT_FOUND = "NO_CONTACT_FOUND"
    CONTACT_NEEDS_REVIEW = "CONTACT_NEEDS_REVIEW"
    OTHER = "OTHER"


class EscalationStatus(_ApiEnum):
    OPEN = "OPEN"
    RESOLVED = "RESOLVED"


class InboundCategory(_ApiEnum):
    DATA_PROVIDED = "DATA_PROVIDED"
    PAYMENT_REQUIRED = "PAYMENT_REQUIRED"
    NEEDS_CLARIFICATION = "NEEDS_CLARIFICATION"
    DENIAL = "DENIAL"
    REFERRAL = "REFERRAL"
    ACKNOWLEDGMENT = "ACKNOWLEDGMENT"
    UNCLEAR = "UNCLEAR"


@dataclass
class Classification:
    category: InboundCategory
    summary: str
    confidence: float
    referral_email: str | None = None

    def to_json_dict(self) -> dict[str, object]:
        return {
            "category": self.category.api_value,
            "summary": self.summary,
            "confidence": self.confidence,
            "referral_email": self.referral_email,
        }


# --------------------------------------------------------------------------
# Rows


@dataclass
class Campaign:
    """One row per campaign; created from the config at registration."""

    id: int
    config: CampaignConfig
    active: bool = False  # the run switch
    seeded: bool = False  # set only after ALL search queries are enqueued
    created_at: datetime | None = None

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def consent_confirmed(self) -> bool:
        return self.config.requester.consent_confirmed

    @property
    def dry_run(self) -> bool:
        return self.config.dry_run


@dataclass
class Jurisdiction:
    id: int
    name: str
    state: str
    level: str
    parent_name: str | None = None
    contact_email: str | None = None
    contact_name: str | None = None
    contact_url: str | None = None
    contact_verified: bool = False  # written false; reserved for humans
    last_contacted_at: datetime | None = None  # shared office cooldown clock


@dataclass
class EmailThread:
    id: int
    campaign_id: int
    jurisdiction_id: int
    thread_token: str
    contact_email: str
    status: ThreadStatus
    parent_thread_id: int | None = None
    followups_sent: int = 0
    next_action_at: datetime | None = None
    attachment_keys: list[str] = field(default_factory=list)
    notes: str = ""
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass
class EmailRecord:
    id: int
    thread_id: int | None
    campaign_id: int | None  # null only for unmatched inbound mail
    direction: EmailDirection
    from_address: str
    to_address: str
    subject: str
    body: str
    kind: OutboundKind | None = None
    classification: Classification | None = None
    message_id: str | None = None
    source_key: str | None = None
    resend_id: str | None = None
    in_reply_to_email_id: int | None = None
    attachment_refs: list[dict[str, str]] = field(default_factory=list)
    created_at: datetime | None = None


@dataclass
class Escalation:
    id: int
    campaign_id: int | None
    thread_id: int | None
    reason: EscalationReason
    details: str
    status: EscalationStatus = EscalationStatus.OPEN
    resolution: str = ""
    notified: bool = False
    created_at: datetime | None = None
    resolved_at: datetime | None = None


@dataclass
class SpendEntry:
    id: int
    campaign_id: int
    thread_id: int | None
    amount_cents: int  # integer cents, never floats
    kind: str = "fee_authorized"
    note: str = ""
    remitted: bool = False  # manual checkbox — remittance has no payment rail
    notified: bool = False
    created_at: datetime | None = None


@dataclass
class SearchTarget:
    id: int
    campaign_id: int
    jurisdiction_id: int
    queries_enqueued: int = 0
    consumed_indexes: list[int] = field(default_factory=list)
    resolved: bool = False  # a contact was found or the target escalated
    created_at: datetime | None = None


# --------------------------------------------------------------------------
# Thread token & inbound identity


def new_thread_token() -> str:
    """16 hex chars = 8 random bytes; globally unique (DB-enforced)."""
    return secrets.token_hex(TOKEN_BYTES)


_SUBJECT_TOKEN_RE = re.compile(r"\[(?:DLG|RF)-([0-9a-fA-F]{4,32})\]")


def extract_token(headers: Mapping[str, str], subject: str) -> str | None:
    """Header first, then a subject regex matching [DLG-…] and legacy [RF-…]."""
    for name, value in headers.items():
        if name.lower() == TOKEN_HEADER.lower() and value.strip():
            return value.strip().lower()
    match = _SUBJECT_TOKEN_RE.search(subject or "")
    return match.group(1).lower() if match else None


def fallback_message_id(from_address: str, subject: str, body: str) -> str:
    """`sha:` + first 32 hex of sha256 over `from|subject|body` (§3.1)."""
    digest = sha256(f"{from_address}|{subject}|{body}".encode()).hexdigest()
    return f"sha:{digest[:32]}"


# --------------------------------------------------------------------------
# Address heuristics (§6, carried over verbatim)


def local_part(address: str) -> str:
    return address.rsplit("@", 1)[0].lower() if "@" in address else address.lower()


def is_junk_address(address: str) -> bool:
    return local_part(address) in JUNK_LOCAL_PARTS


def is_generic_address(address: str) -> bool:
    """Generic-keyword local-parts may be auto-mailed; anything else is a
    personal mailbox and goes through contact_needs_review."""
    lp = local_part(address)
    return any(keyword in lp for keyword in GENERIC_LOCAL_KEYWORDS)


# --------------------------------------------------------------------------
# Storage keys (§3.3)


def slugify(text: str) -> str:
    return _core_slugify(text)[:SLUG_LEN]


_FILENAME_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")


def sanitize_filename(filename: str) -> str:
    cleaned = _FILENAME_SAFE_RE.sub("_", filename).strip("._-")
    return cleaned or "attachment"


def document_key(campaign_name: str, jurisdiction_name: str, digest: str, filename: str) -> str:
    from .constants import DIGEST_PREFIX_LEN

    return (
        f"{slugify(campaign_name)}/{slugify(jurisdiction_name)}/"
        f"{digest[:DIGEST_PREFIX_LEN]}_{sanitize_filename(filename)}"
    )


def spool_key(campaign_name: str, digest: str, index: int, filename: str) -> str:
    from .constants import DIGEST_PREFIX_LEN

    return (
        f"{slugify(campaign_name)}/inbox-spool/"
        f"{digest[:DIGEST_PREFIX_LEN]}_{index}_{sanitize_filename(filename)}"
    )
