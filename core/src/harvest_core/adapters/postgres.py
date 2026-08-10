"""SQLAlchemy schema, idempotent boot migration, and the Postgres datastore.

The schema keeps the catalog columns and states (spec ruling 8) so
catalog lands later as a pure consumer of `fetched`.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Engine, Row
from sqlalchemy.exc import IntegrityError

from ..domain import (
    Artifact,
    ArtifactStatus,
    CodeSource,
    Jurisdiction,
    Publisher,
    RunState,
    Source,
    SweepHistory,
    SweepResult,
    SweepTarget,
)
from ..errors import UniqueViolation
from .db import run_migration

metadata = sa.MetaData()

jurisdictions = sa.Table(
    "jurisdictions",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("fips", sa.String(16)),
    sa.Column("name", sa.String(200), nullable=False),
    sa.Column("state", sa.String(2), nullable=False),
    sa.Column("level", sa.String(16), nullable=False),
    sa.Column("parent_id", sa.Integer, sa.ForeignKey("jurisdictions.id")),
    sa.Column("parent_name", sa.String(200)),
    sa.UniqueConstraint("level", "state", "name", name="uq_jurisdiction_level_state_name"),
)

sweep_targets = sa.Table(
    "sweep_targets",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("corpus", sa.String(200), nullable=False),
    sa.Column(
        "jurisdiction_id", sa.Integer, sa.ForeignKey("jurisdictions.id"), nullable=False
    ),
    sa.Column("source", sa.String(32), nullable=False),
    sa.Column("priority", sa.Integer, nullable=False),
    sa.Column("next_due_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("last_result", sa.String(16)),
    sa.Column("dispatched_at", sa.DateTime(timezone=True)),
    sa.Column("dispatch_id", sa.String(64)),
    sa.Column("query_count", sa.Integer),
    sa.UniqueConstraint("corpus", "jurisdiction_id", "source", name="uq_target_triple"),
    sa.Index("ix_targets_corpus_due", "corpus", "next_due_at"),
)

artifacts = sa.Table(
    "artifacts",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("corpus", sa.String(200), nullable=False),
    sa.Column(
        "jurisdiction_id", sa.Integer, sa.ForeignKey("jurisdictions.id"), nullable=False
    ),
    sa.Column("origin", sa.String(32), nullable=False),
    sa.Column("source_url", sa.String(600), nullable=False),
    sa.Column("context", sa.Text, nullable=False, default=""),
    sa.Column("status", sa.String(16), nullable=False, default="pending"),
    sa.Column("attempts", sa.Integer, nullable=False, default=0),
    sa.Column("last_error", sa.String(2000)),
    sa.Column("filename", sa.String(400)),
    sa.Column("ext", sa.String(16)),
    sa.Column("content_type", sa.String(200)),
    sa.Column("sha256", sa.String(64)),
    sa.Column("path", sa.String(800)),
    sa.Column("size_bytes", sa.BigInteger),
    sa.Column("extracted_text", sa.Text),
    sa.Column("dispatched_at", sa.DateTime(timezone=True)),
    sa.Column("dispatch_id", sa.String(64)),
    sa.Column("created_at", sa.DateTime(timezone=True)),
    # Verdict columns for the future catalog stage (unused this phase).
    sa.Column("responsive", sa.Boolean),
    sa.Column("title", sa.String(300)),
    sa.Column("topics", sa.Text),
    sa.Column("confidence", sa.Float),
    sa.Column("verdict_reason", sa.Text),
    sa.Column("document_id", sa.Integer),
    sa.UniqueConstraint("corpus", "source_url", name="uq_artifact_corpus_url"),
    sa.Index("ix_artifacts_sha", "corpus", "sha256"),
    sa.Index("ix_artifacts_corpus_status", "corpus", "status"),
)

harvest_sweeps = sa.Table(
    "harvest_sweeps",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("corpus", sa.String(200), nullable=False),
    sa.Column(
        "jurisdiction_id", sa.Integer, sa.ForeignKey("jurisdictions.id"), nullable=False
    ),
    sa.Column("source", sa.String(32), nullable=False),
    sa.Column("dispatch_id", sa.String(64), nullable=False),
    sa.Column("query_seq", sa.Integer, nullable=False),
    sa.Column("topic", sa.String(200)),
    sa.Column("result", sa.String(16), nullable=False),
    sa.Column("results_seen", sa.Integer, nullable=False, default=0),
    sa.Column("results_triaged_relevant", sa.Integer, nullable=False, default=0),
    sa.Column("candidates_staged", sa.Integer, nullable=False, default=0),
    sa.Column("detail", sa.String(500), nullable=False, default=""),
    sa.Column("swept_at", sa.DateTime(timezone=True), nullable=False),
    sa.UniqueConstraint("dispatch_id", "query_seq", name="uq_sweep_dispatch_seq"),
)

code_sources = sa.Table(
    "code_sources",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column(
        "jurisdiction_id", sa.Integer, sa.ForeignKey("jurisdictions.id"), nullable=False
    ),
    sa.Column("url", sa.String(600), nullable=False),
    sa.Column("publisher", sa.String(16), nullable=False),
    sa.Column("enabled", sa.Boolean, nullable=False, default=True),
    sa.Column("added_by", sa.String(200)),
    sa.Column("added_at", sa.DateTime(timezone=True)),
    sa.UniqueConstraint("jurisdiction_id", "url", name="uq_code_source"),
)

campaign_controls = sa.Table(
    "campaign_controls",
    metadata,
    sa.Column("name", sa.String(200), primary_key=True),
    sa.Column("state", sa.String(16), nullable=False),
)

# Output table; catalog is out of scope but the purge path covers it.
documents = sa.Table(
    "documents",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column(
        "jurisdiction_id", sa.Integer, sa.ForeignKey("jurisdictions.id"), nullable=False
    ),
    sa.Column("corpus", sa.String(200)),
    sa.Column("request_id", sa.Integer),
    sa.Column("filename", sa.String(400)),
    sa.Column("content_type", sa.String(200)),
    sa.Column("path", sa.String(800)),
    sa.Column("sha256", sa.String(64)),
    sa.Column("size_bytes", sa.BigInteger),
    sa.Column("method", sa.String(32)),
    sa.Column("source_url", sa.String(600)),
    sa.Column("title", sa.String(300)),
    sa.Column("topics", sa.Text),
    sa.Column("catalog_confidence", sa.Float),
    sa.Column("received_at", sa.DateTime(timezone=True)),
)


def migrate(engine: Engine) -> None:
    """Idempotent boot migration (old spec §10): safe to run every start,
    and safe when several services start at once (see db.run_migration)."""
    run_migration(engine, metadata)


def _pk(result: sa.CursorResult[Any]) -> int:
    pk = result.inserted_primary_key
    assert pk is not None
    return int(pk[0])


def _aware(dt: datetime | None) -> datetime | None:
    """SQLite round-trips tz-aware datetimes as naive; normalize to UTC."""
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


class PostgresDatastore:
    """Datastore over SQLAlchemy. Also runs against SQLite in unit tests;

    live-Postgres verification is the 4.2 manual smoke."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    # -- row mappers ------------------------------------------------------
    @staticmethod
    def _jurisdiction(row: Row[Any]) -> Jurisdiction:
        return Jurisdiction(
            id=row.id,
            name=row.name,
            state=row.state,
            level=row.level,
            fips=row.fips,
            parent_id=row.parent_id,
            parent_name=row.parent_name,
        )

    @staticmethod
    def _target(row: Row[Any]) -> SweepTarget:
        return SweepTarget(
            id=row.id,
            corpus=row.corpus,
            jurisdiction_id=row.jurisdiction_id,
            source=Source(row.source),
            priority=row.priority,
            next_due_at=_aware(row.next_due_at),  # type: ignore[arg-type]
            last_result=SweepResult(row.last_result) if row.last_result else None,
            dispatched_at=_aware(row.dispatched_at),
            dispatch_id=row.dispatch_id,
            query_count=row.query_count,
        )

    @staticmethod
    def _artifact(row: Row[Any]) -> Artifact:
        return Artifact(
            id=row.id,
            corpus=row.corpus,
            jurisdiction_id=row.jurisdiction_id,
            origin=row.origin,
            source_url=row.source_url,
            context=row.context or "",
            status=ArtifactStatus(row.status),
            attempts=row.attempts,
            last_error=row.last_error,
            filename=row.filename,
            ext=row.ext,
            content_type=row.content_type,
            sha256=row.sha256,
            path=row.path,
            size_bytes=row.size_bytes,
            extracted_text=row.extracted_text,
            dispatched_at=_aware(row.dispatched_at),
            dispatch_id=row.dispatch_id,
            created_at=_aware(row.created_at),
            responsive=row.responsive,
            title=row.title,
            topics=json.loads(row.topics) if row.topics else [],
            confidence=row.confidence,
            verdict_reason=row.verdict_reason,
            document_id=row.document_id,
        )

    @staticmethod
    def _history(row: Row[Any]) -> SweepHistory:
        return SweepHistory(
            id=row.id,
            corpus=row.corpus,
            jurisdiction_id=row.jurisdiction_id,
            source=Source(row.source),
            dispatch_id=row.dispatch_id,
            query_seq=row.query_seq,
            result=SweepResult(row.result),
            topic=row.topic,
            results_seen=row.results_seen,
            results_triaged_relevant=row.results_triaged_relevant,
            candidates_staged=row.candidates_staged,
            detail=row.detail or "",
            swept_at=_aware(row.swept_at),
        )

    @staticmethod
    def _code_source(row: Row[Any]) -> CodeSource:
        return CodeSource(
            id=row.id,
            jurisdiction_id=row.jurisdiction_id,
            url=row.url,
            publisher=Publisher(row.publisher),
            enabled=row.enabled,
            added_by=row.added_by,
            added_at=_aware(row.added_at),
        )

    # -- jurisdictions ----------------------------------------------------
    def get_jurisdiction(self, jurisdiction_id: int) -> Jurisdiction | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(jurisdictions).where(jurisdictions.c.id == jurisdiction_id)
            ).first()
            return self._jurisdiction(row) if row else None

    def insert_jurisdiction(
        self,
        name: str,
        state: str,
        level: str,
        fips: str | None = None,
        parent_id: int | None = None,
        parent_name: str | None = None,
    ) -> Jurisdiction:
        with self._engine.begin() as conn:
            try:
                result = conn.execute(
                    jurisdictions.insert().values(
                        name=name,
                        state=state,
                        level=level,
                        fips=fips,
                        parent_id=parent_id,
                        parent_name=parent_name,
                    )
                )
            except IntegrityError as exc:
                raise UniqueViolation(f"jurisdiction ({level}, {state}, {name})") from exc
            new_id = _pk(result)
        return Jurisdiction(
            id=new_id,
            name=name,
            state=state,
            level=level,
            fips=fips,
            parent_id=parent_id,
            parent_name=parent_name,
        )

    def state_row_exists(self, state: str) -> bool:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(jurisdictions.c.id)
                .where(jurisdictions.c.level == "state")
                .where(jurisdictions.c.state == state)
            ).first()
            return row is not None

    def list_jurisdictions(
        self, states: list[str] | None = None, levels: list[str] | None = None
    ) -> list[Jurisdiction]:
        stmt = sa.select(jurisdictions).order_by(jurisdictions.c.id)
        if states is not None:
            stmt = stmt.where(jurisdictions.c.state.in_(states))
        if levels is not None:
            stmt = stmt.where(jurisdictions.c.level.in_(levels))
        with self._engine.connect() as conn:
            return [self._jurisdiction(r) for r in conn.execute(stmt)]

    def get_federal_anchor(self) -> Jurisdiction | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(jurisdictions).where(jurisdictions.c.level == "federal")
            ).first()
            return self._jurisdiction(row) if row else None

    # -- sweep targets ----------------------------------------------------
    def get_target(self, target_id: int) -> SweepTarget | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(sweep_targets).where(sweep_targets.c.id == target_id)
            ).first()
            return self._target(row) if row else None

    def insert_target(
        self,
        corpus: str,
        jurisdiction_id: int,
        source: Source,
        priority: int,
        next_due_at: datetime,
    ) -> SweepTarget:
        with self._engine.begin() as conn:
            try:
                result = conn.execute(
                    sweep_targets.insert().values(
                        corpus=corpus,
                        jurisdiction_id=jurisdiction_id,
                        source=source.value,
                        priority=priority,
                        next_due_at=next_due_at,
                    )
                )
            except IntegrityError as exc:
                raise UniqueViolation(
                    f"sweep_target ({corpus}, {jurisdiction_id}, {source})"
                ) from exc
            new_id = _pk(result)
        return SweepTarget(
            id=new_id,
            corpus=corpus,
            jurisdiction_id=jurisdiction_id,
            source=source,
            priority=priority,
            next_due_at=next_due_at,
        )

    def list_targets(self, corpus: str, source: Source | None = None) -> list[SweepTarget]:
        stmt = (
            sa.select(sweep_targets)
            .where(sweep_targets.c.corpus == corpus)
            .order_by(sweep_targets.c.id)
        )
        if source is not None:
            stmt = stmt.where(sweep_targets.c.source == source.value)
        with self._engine.connect() as conn:
            return [self._target(r) for r in conn.execute(stmt)]

    def select_due(
        self,
        corpus: str,
        now: datetime,
        dispatch_timeouts: Mapping[Source, float],
        limit: int,
    ) -> list[SweepTarget]:
        stamp_clauses: list[sa.ColumnElement[bool]] = [
            sweep_targets.c.dispatched_at.is_(None)
        ]
        for source, timeout in dispatch_timeouts.items():
            stamp_clauses.append(
                sa.and_(
                    sweep_targets.c.source == source.value,
                    sweep_targets.c.dispatched_at <= now - timedelta(seconds=timeout),
                )
            )
        stmt = (
            sa.select(sweep_targets)
            .where(sweep_targets.c.corpus == corpus)
            .where(sweep_targets.c.next_due_at <= now)
            .where(sa.or_(*stamp_clauses))
            .order_by(sweep_targets.c.priority, sweep_targets.c.next_due_at, sweep_targets.c.id)
            .limit(limit)
        )
        with self._engine.connect() as conn:
            return [self._target(r) for r in conn.execute(stmt)]

    def stamp_target(
        self,
        target_id: int,
        dispatched_at: datetime,
        dispatch_id: str,
        query_count: int,
        expected_dispatch_id: str | None,
        expected_dispatched_at: datetime | None,
    ) -> bool:
        stmt = (
            sweep_targets.update()
            .where(sweep_targets.c.id == target_id)
            .values(
                dispatched_at=dispatched_at,
                dispatch_id=dispatch_id,
                query_count=query_count,
            )
        )
        if expected_dispatch_id is None:
            stmt = stmt.where(sweep_targets.c.dispatch_id.is_(None))
        else:
            stmt = stmt.where(sweep_targets.c.dispatch_id == expected_dispatch_id)
        if expected_dispatched_at is None:
            stmt = stmt.where(sweep_targets.c.dispatched_at.is_(None))
        else:
            stmt = stmt.where(sweep_targets.c.dispatched_at == expected_dispatched_at)
        with self._engine.begin() as conn:
            return conn.execute(stmt).rowcount > 0

    def finalize_target(
        self,
        target_id: int,
        dispatch_id: str,
        last_result: SweepResult,
        next_due_at: datetime,
    ) -> bool:
        with self._engine.begin() as conn:
            result = conn.execute(
                sweep_targets.update()
                .where(sweep_targets.c.id == target_id)
                .where(sweep_targets.c.dispatch_id == dispatch_id)
                .values(
                    last_result=last_result.value,
                    next_due_at=next_due_at,
                    dispatched_at=None,
                    dispatch_id=None,
                    query_count=None,
                )
            )
            return result.rowcount > 0

    def park_target(
        self, target_id: int, next_due_at: datetime, last_result: SweepResult
    ) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                sweep_targets.update()
                .where(sweep_targets.c.id == target_id)
                .values(
                    next_due_at=next_due_at,
                    last_result=last_result.value,
                    dispatched_at=None,
                    dispatch_id=None,
                    query_count=None,
                )
            )

    # -- artifacts --------------------------------------------------------
    def insert_artifact(
        self,
        corpus: str,
        jurisdiction_id: int,
        origin: str,
        source_url: str,
        context: str,
        created_at: datetime,
    ) -> Artifact:
        with self._engine.begin() as conn:
            try:
                result = conn.execute(
                    artifacts.insert().values(
                        corpus=corpus,
                        jurisdiction_id=jurisdiction_id,
                        origin=origin,
                        source_url=source_url,
                        context=context,
                        status=ArtifactStatus.PENDING.value,
                        attempts=0,
                        created_at=created_at,
                        topics=None,
                    )
                )
            except IntegrityError as exc:
                raise UniqueViolation(f"artifact ({corpus}, {source_url})") from exc
            new_id = _pk(result)
        return Artifact(
            id=new_id,
            corpus=corpus,
            jurisdiction_id=jurisdiction_id,
            origin=origin,
            source_url=source_url,
            context=context,
            created_at=created_at,
        )

    def get_artifact(self, artifact_id: int) -> Artifact | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(artifacts).where(artifacts.c.id == artifact_id)
            ).first()
            return self._artifact(row) if row else None

    def update_artifact(self, artifact: Artifact) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                artifacts.update()
                .where(artifacts.c.id == artifact.id)
                .values(
                    status=artifact.status.value,
                    attempts=artifact.attempts,
                    last_error=artifact.last_error,
                    filename=artifact.filename,
                    ext=artifact.ext,
                    content_type=artifact.content_type,
                    sha256=artifact.sha256,
                    path=artifact.path,
                    size_bytes=artifact.size_bytes,
                    extracted_text=artifact.extracted_text,
                    dispatched_at=artifact.dispatched_at,
                    dispatch_id=artifact.dispatch_id,
                    context=artifact.context,
                    responsive=artifact.responsive,
                    title=artifact.title,
                    topics=json.dumps(artifact.topics) if artifact.topics else None,
                    confidence=artifact.confidence,
                    verdict_reason=artifact.verdict_reason,
                    document_id=artifact.document_id,
                )
            )

    def delete_artifact(self, artifact_id: int) -> None:
        with self._engine.begin() as conn:
            conn.execute(artifacts.delete().where(artifacts.c.id == artifact_id))

    def corpus_has_sha256(
        self, corpus: str, sha256: str, exclude_artifact_id: int
    ) -> bool:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(artifacts.c.id)
                .where(artifacts.c.corpus == corpus)
                .where(artifacts.c.sha256 == sha256)
                .where(artifacts.c.id != exclude_artifact_id)
            ).first()
            return row is not None

    def select_pending_stale(
        self, corpus: str, now: datetime, timeout_seconds: float, limit: int
    ) -> list[Artifact]:
        cutoff = now - timedelta(seconds=timeout_seconds)
        stmt = (
            sa.select(artifacts)
            .where(artifacts.c.corpus == corpus)
            .where(artifacts.c.status == ArtifactStatus.PENDING.value)
            .where(
                sa.or_(
                    artifacts.c.dispatched_at.is_(None),
                    artifacts.c.dispatched_at <= cutoff,
                )
            )
            .order_by(artifacts.c.id)
            .limit(limit)
        )
        with self._engine.connect() as conn:
            return [self._artifact(r) for r in conn.execute(stmt)]

    def stamp_artifact(
        self, artifact_id: int, dispatched_at: datetime, dispatch_id: str
    ) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                artifacts.update()
                .where(artifacts.c.id == artifact_id)
                .values(dispatched_at=dispatched_at, dispatch_id=dispatch_id)
            )

    # -- sweep history ----------------------------------------------------
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
        with self._engine.begin() as conn:
            try:
                conn.execute(
                    harvest_sweeps.insert().values(
                        corpus=corpus,
                        jurisdiction_id=jurisdiction_id,
                        source=source.value,
                        dispatch_id=dispatch_id,
                        query_seq=query_seq,
                        result=result.value,
                        topic=topic,
                        results_seen=results_seen,
                        results_triaged_relevant=results_triaged_relevant,
                        candidates_staged=candidates_staged,
                        detail=detail[:500],
                        swept_at=swept_at,
                    )
                )
            except IntegrityError:
                return False
            return True

    def list_history(
        self, corpus: str, jurisdiction_id: int | None = None
    ) -> list[SweepHistory]:
        stmt = (
            sa.select(harvest_sweeps)
            .where(harvest_sweeps.c.corpus == corpus)
            .order_by(harvest_sweeps.c.id)
        )
        if jurisdiction_id is not None:
            stmt = stmt.where(harvest_sweeps.c.jurisdiction_id == jurisdiction_id)
        with self._engine.connect() as conn:
            return [self._history(r) for r in conn.execute(stmt)]

    # -- code sources -----------------------------------------------------
    def insert_code_source(
        self,
        jurisdiction_id: int,
        url: str,
        publisher: Publisher,
        enabled: bool,
        added_by: str | None,
        added_at: datetime,
    ) -> CodeSource:
        with self._engine.begin() as conn:
            try:
                result = conn.execute(
                    code_sources.insert().values(
                        jurisdiction_id=jurisdiction_id,
                        url=url,
                        publisher=publisher.value,
                        enabled=enabled,
                        added_by=added_by,
                        added_at=added_at,
                    )
                )
            except IntegrityError as exc:
                raise UniqueViolation(f"code_source ({jurisdiction_id}, {url})") from exc
            new_id = _pk(result)
        return CodeSource(
            id=new_id,
            jurisdiction_id=jurisdiction_id,
            url=url,
            publisher=publisher,
            enabled=enabled,
            added_by=added_by,
            added_at=added_at,
        )

    def list_code_sources(
        self, jurisdiction_id: int | None = None, enabled_only: bool = False
    ) -> list[CodeSource]:
        stmt = sa.select(code_sources).order_by(code_sources.c.id)
        if jurisdiction_id is not None:
            stmt = stmt.where(code_sources.c.jurisdiction_id == jurisdiction_id)
        if enabled_only:
            stmt = stmt.where(code_sources.c.enabled.is_(True))
        with self._engine.connect() as conn:
            return [self._code_source(r) for r in conn.execute(stmt)]

    def set_code_source_enabled(self, code_source_id: int, enabled: bool) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                code_sources.update()
                .where(code_sources.c.id == code_source_id)
                .values(enabled=enabled)
            )

    # -- run switch -------------------------------------------------------
    def get_run_state(self, name: str) -> RunState | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(campaign_controls.c.state).where(campaign_controls.c.name == name)
            ).first()
            return RunState(row.state) if row else None

    def set_run_state(self, name: str, state: RunState) -> None:
        with self._engine.begin() as conn:
            updated = conn.execute(
                campaign_controls.update()
                .where(campaign_controls.c.name == name)
                .values(state=state.value)
            )
            if updated.rowcount == 0:
                conn.execute(
                    campaign_controls.insert().values(name=name, state=state.value)
                )

    # -- corpus kill ------------------------------------------------------
    def purge_corpus(self, corpus: str) -> dict[str, int]:
        """FK-ordered purge: artifacts reference documents, so artifacts first."""
        counts: dict[str, int] = {}
        with self._engine.begin() as conn:
            counts["artifacts"] = conn.execute(
                artifacts.delete().where(artifacts.c.corpus == corpus)
            ).rowcount
            counts["documents"] = conn.execute(
                documents.delete().where(documents.c.corpus == corpus)
            ).rowcount
            counts["harvest_sweeps"] = conn.execute(
                harvest_sweeps.delete().where(harvest_sweeps.c.corpus == corpus)
            ).rowcount
            counts["sweep_targets"] = conn.execute(
                sweep_targets.delete().where(sweep_targets.c.corpus == corpus)
            ).rowcount
        return counts
