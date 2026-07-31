"""Domain models: rows of the six tables plus the artifact state machine."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from .constants import DEFAULT_PRIORITY, LEVEL_PRIORITIES
from .errors import IllegalTransition


class Source(StrEnum):
    SERPER = "serper"
    LEGAL_CODES = "legal_codes"


class SweepResult(StrEnum):
    CANDIDATES = "candidates"
    NOT_FOUND = "not_found"
    ERROR = "error"


class ArtifactStatus(StrEnum):
    PENDING = "pending"
    FETCHED = "fetched"
    FAILED = "failed"
    NOT_DOCUMENT = "not_document"
    DUPLICATE = "duplicate"
    # Kept in the schema for the future catalog stage; unused this phase.
    CATALOGED = "cataloged"
    REJECTED = "rejected"


# Truncated state machine for this phase (spec §4.4). 4xx/unreachable deletes
# the row outright, which is not a transition.
_ALLOWED_TRANSITIONS: dict[ArtifactStatus, frozenset[ArtifactStatus]] = {
    ArtifactStatus.PENDING: frozenset(
        {
            ArtifactStatus.FETCHED,
            ArtifactStatus.FAILED,
            ArtifactStatus.NOT_DOCUMENT,
            ArtifactStatus.DUPLICATE,
        }
    ),
    ArtifactStatus.FETCHED: frozenset({ArtifactStatus.CATALOGED, ArtifactStatus.REJECTED}),
    ArtifactStatus.FAILED: frozenset(),
    ArtifactStatus.NOT_DOCUMENT: frozenset(),
    ArtifactStatus.DUPLICATE: frozenset(),
    ArtifactStatus.CATALOGED: frozenset(),
    ArtifactStatus.REJECTED: frozenset(),
}


def check_transition(current: ArtifactStatus, new: ArtifactStatus) -> None:
    """Raise IllegalTransition unless current -> new is a legal move."""
    if new not in _ALLOWED_TRANSITIONS[current]:
        raise IllegalTransition(f"artifact may not move {current.value} -> {new.value}")


def level_priority(level: str) -> int:
    return LEVEL_PRIORITIES.get(level, DEFAULT_PRIORITY)


class Publisher(StrEnum):
    MUNICODE = "municode"
    AMLEGAL = "amlegal"
    ECODE360 = "ecode360"
    OTHER = "other"


class RunState(StrEnum):
    RUNNING = "running"
    STOPPED = "stopped"


@dataclass
class Jurisdiction:
    id: int
    name: str
    state: str
    level: str
    fips: str | None = None
    parent_id: int | None = None
    parent_name: str | None = None


@dataclass
class SweepTarget:
    id: int
    corpus: str
    jurisdiction_id: int
    source: Source
    priority: int
    next_due_at: datetime
    last_result: SweepResult | None = None
    dispatched_at: datetime | None = None
    dispatch_id: str | None = None
    query_count: int | None = None


@dataclass
class Artifact:
    id: int
    corpus: str
    jurisdiction_id: int
    origin: str
    source_url: str
    context: str = ""
    status: ArtifactStatus = ArtifactStatus.PENDING
    attempts: int = 0
    last_error: str | None = None
    filename: str | None = None
    ext: str | None = None
    content_type: str | None = None
    sha256: str | None = None
    path: str | None = None
    size_bytes: int | None = None
    extracted_text: str | None = None
    dispatched_at: datetime | None = None
    dispatch_id: str | None = None
    created_at: datetime | None = None
    # Verdict columns: kept for the future catalog stage, unused this phase.
    responsive: bool | None = None
    title: str | None = None
    topics: list[str] = field(default_factory=list)
    confidence: float | None = None
    verdict_reason: str | None = None
    document_id: int | None = None


@dataclass
class SweepHistory:
    id: int
    corpus: str
    jurisdiction_id: int
    source: Source
    dispatch_id: str
    query_seq: int
    result: SweepResult
    topic: str | None = None
    results_seen: int = 0
    results_triaged_relevant: int = 0
    candidates_staged: int = 0
    detail: str = ""
    swept_at: datetime | None = None


@dataclass
class CodeSource:
    id: int
    jurisdiction_id: int
    url: str
    publisher: Publisher
    enabled: bool = True
    added_by: str | None = None
    added_at: datetime | None = None


@dataclass
class RunSwitch:
    name: str
    state: RunState
