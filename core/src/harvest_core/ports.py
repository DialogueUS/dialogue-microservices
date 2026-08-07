"""Ports: every external system behind exactly one Protocol.

Services import this module (and `domain`/`errors`) only — never an
adapter. Fakes live in `harvest_core.fakes`, real adapters in
`harvest_core.adapters`; both are declared implementations of these
Protocols and mypy checks conformance in `_protocol_checks.py`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from .domain import (
    Artifact,
    CodeSource,
    Jurisdiction,
    Publisher,
    RunState,
    Source,
    SweepHistory,
    SweepResult,
    SweepTarget,
)


class Clock(Protocol):
    def now(self) -> datetime: ...

    def sleep(self, seconds: float) -> None: ...


# --------------------------------------------------------------------------
# Queue


@dataclass
class QueueMessage:
    id: str
    body: str
    receive_count: int


class TaskQueue(Protocol):
    def send(self, body: str) -> None: ...

    def receive(self, max_messages: int) -> list[QueueMessage]: ...

    def delete(self, message_id: str) -> None: ...

    def change_visibility(self, message_id: str, timeout_seconds: float) -> None: ...

    def pending_count(self) -> int:
        """Approximate visible + in-flight messages (SQS attributes)."""
        ...


# --------------------------------------------------------------------------
# Key-value (Redis)


class KeyValue(Protocol):
    def setnx(self, key: str, value: str) -> bool:
        """Set key if absent; True when this call set it."""
        ...

    def hincrby(self, key: str, increments: Mapping[str, int]) -> dict[str, int]:
        """Apply the increments to a hash and return the full resulting hash."""
        ...

    def expire(self, key: str, ttl_seconds: int) -> None: ...

    def get(self, key: str) -> str | None: ...

    def delete_prefix(self, prefix: str) -> int:
        """Delete all keys under a prefix (corpus kill); returns count deleted."""
        ...


# --------------------------------------------------------------------------
# Object store (S3)


class ObjectStore(Protocol):
    def put(self, key: str, data: bytes, content_type: str) -> None: ...

    def get(self, key: str) -> bytes:
        """Raises ObjectNotFound for a missing key."""
        ...

    def list_keys(self, prefix: str) -> list[str]: ...

    def delete(self, key: str) -> None: ...

    def presign(self, key: str) -> str: ...

    def delete_prefix(self, prefix: str) -> int: ...


# --------------------------------------------------------------------------
# Search (Serper)


@dataclass
class SearchResult:
    rank: int
    title: str
    snippet: str
    url: str


class SearchProvider(Protocol):
    def search(self, query: str, count: int) -> list[SearchResult]:
        """Raises RateLimited on HTTP 429 — and only on 429."""
        ...


# --------------------------------------------------------------------------
# LLM: query generation (orchestrator) and relevance triage (harvester)


class QueryGenerator(Protocol):
    def generate(
        self, jurisdiction: Jurisdiction, topics: list[str], queries_per_topic: int
    ) -> dict[str, list[str]]:
        """One call per jurisdiction covering all topics.

        Returns topic -> queries (1..queries_per_topic each, <= 200 chars).
        Raises GenerationError on API or parse failure.
        """
        ...


@dataclass
class TriageRequest:
    jurisdiction_name: str
    state: str
    level: str
    topic: str
    results: list[SearchResult]


@dataclass
class TriageVerdict:
    relevant: bool
    is_document: bool
    confidence: float


class Triage(Protocol):
    def triage(self, requests: list[TriageRequest]) -> list[list[TriageVerdict]]:
        """One batched call judging result *metadata* only.

        Returns verdicts parallel to requests[i].results.
        Raises TriageError on API or parse failure.
        """
        ...


# --------------------------------------------------------------------------
# Fetching


@dataclass
class FetchResponse:
    status: int
    content: bytes
    content_type: str | None = None


class Fetcher(Protocol):
    def get(self, url: str, max_bytes: int) -> FetchResponse:
        """Raises UnreachableHost for dead hosts, FetchError for other
        transport failures. HTTP error statuses are returned, not raised."""
        ...


# --------------------------------------------------------------------------
# Code-portal discovery (Scrapy-Playwright behind this port)


@dataclass
class PortalCandidate:
    url: str
    context: str


@dataclass
class DiscoveryResult:
    candidates: list[PortalCandidate] = field(default_factory=list)
    complete: bool = True
    detail: str = ""


class PortalDiscoverer(Protocol):
    def discover(
        self,
        portal_urls: list[str],
        publisher: Publisher,
        max_pages: int,
        deadline: datetime,
    ) -> DiscoveryResult:
        """Discover native document URLs from a rendered code portal.

        Returns partial results with complete=False when the deadline cut
        discovery short. Raises TransientPortalError (unreachable /
        anti-bot / render hang) or PortalError (crawl errored, nothing
        found).
        """
        ...


# --------------------------------------------------------------------------
# Datastore (Postgres)


class Datastore(Protocol):
    # -- jurisdictions
    def get_jurisdiction(self, jurisdiction_id: int) -> Jurisdiction | None: ...

    def insert_jurisdiction(
        self,
        name: str,
        state: str,
        level: str,
        fips: str | None = None,
        parent_id: int | None = None,
        parent_name: str | None = None,
    ) -> Jurisdiction:
        """Unique on (level, state, name); raises UniqueViolation."""
        ...

    def state_row_exists(self, state: str) -> bool:
        """Whether the state-level row exists — the loaded marker for seeding."""
        ...

    def list_jurisdictions(
        self, states: list[str] | None = None, levels: list[str] | None = None
    ) -> list[Jurisdiction]: ...

    def get_federal_anchor(self) -> Jurisdiction | None: ...

    # -- sweep targets
    def get_target(self, target_id: int) -> SweepTarget | None: ...

    def insert_target(
        self,
        corpus: str,
        jurisdiction_id: int,
        source: Source,
        priority: int,
        next_due_at: datetime,
    ) -> SweepTarget:
        """Unique on (corpus, jurisdiction_id, source); raises UniqueViolation."""
        ...

    def list_targets(self, corpus: str, source: Source | None = None) -> list[SweepTarget]: ...

    def select_due(
        self,
        corpus: str,
        now: datetime,
        dispatch_timeouts: Mapping[Source, float],
        limit: int,
    ) -> list[SweepTarget]:
        """Due rows: next_due_at <= now and stamp null or older than the
        source's dispatch timeout; ordered priority then next_due_at."""
        ...

    def stamp_target(
        self,
        target_id: int,
        dispatched_at: datetime,
        dispatch_id: str,
        query_count: int,
        expected_dispatch_id: str | None,
        expected_dispatched_at: datetime | None,
    ) -> bool:
        """Compare-and-set dispatch stamp: applies only when the row still
        carries the stamp the caller read (None for an unstamped row).
        Returns False when another orchestrator won the race."""
        ...

    def finalize_target(
        self,
        target_id: int,
        dispatch_id: str,
        last_result: SweepResult,
        next_due_at: datetime,
    ) -> bool:
        """Set outcome + clear the stamp, gated on dispatch_id matching.

        Returns False (no write) when the row's dispatch_id differs."""
        ...

    def park_target(
        self, target_id: int, next_due_at: datetime, last_result: SweepResult
    ) -> None:
        """Push a row out (e.g. legal_codes with all seeds disabled)."""
        ...

    # -- artifacts
    def insert_artifact(
        self,
        corpus: str,
        jurisdiction_id: int,
        origin: str,
        source_url: str,
        context: str,
        created_at: datetime,
    ) -> Artifact:
        """Unique on (corpus, source_url); raises UniqueViolation."""
        ...

    def get_artifact(self, artifact_id: int) -> Artifact | None: ...

    def update_artifact(self, artifact: Artifact) -> None: ...

    def delete_artifact(self, artifact_id: int) -> None: ...

    def corpus_has_sha256(
        self, corpus: str, sha256: str, exclude_artifact_id: int
    ) -> bool:
        """Postgres dedupe backstop: another artifact in this corpus
        already carries these bytes."""
        ...

    def select_pending_stale(
        self, corpus: str, now: datetime, timeout_seconds: float, limit: int
    ) -> list[Artifact]:
        """Pending artifacts whose dispatch stamp is null or stale."""
        ...

    def stamp_artifact(
        self, artifact_id: int, dispatched_at: datetime, dispatch_id: str
    ) -> None: ...

    # -- sweep history
    def insert_history(
        self,
        corpus: str,
        jurisdiction_id: int,
        source: Source,
        dispatch_id: str,
        query_seq: int,
        result: SweepResult,
        topic: str | None,
        results_seen: int,
        results_triaged_relevant: int,
        candidates_staged: int,
        detail: str,
        swept_at: datetime,
    ) -> bool:
        """Idempotent on (dispatch_id, query_seq): False when already written."""
        ...

    def list_history(
        self, corpus: str, jurisdiction_id: int | None = None
    ) -> list[SweepHistory]: ...

    # -- code sources
    def insert_code_source(
        self,
        jurisdiction_id: int,
        url: str,
        publisher: Publisher,
        enabled: bool,
        added_by: str | None,
        added_at: datetime,
    ) -> CodeSource:
        """Unique on (jurisdiction_id, url); raises UniqueViolation."""
        ...

    def list_code_sources(
        self, jurisdiction_id: int | None = None, enabled_only: bool = False
    ) -> list[CodeSource]: ...

    def set_code_source_enabled(self, code_source_id: int, enabled: bool) -> None: ...

    # -- run switch
    def get_run_state(self, name: str) -> RunState | None:
        """None means no control row — treated as stopped."""
        ...

    def set_run_state(self, name: str, state: RunState) -> None: ...

    # -- corpus kill (FK-ordered purge of this corpus's rows)
    def purge_corpus(self, corpus: str) -> dict[str, int]: ...
