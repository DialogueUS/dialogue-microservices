"""FakeDatastore: in-memory tables with the same constraint surface as Postgres.

Unique violations raise harvest_core.errors.UniqueViolation — the exact
type the real adapter raises — so race-losing code paths are testable.
"""

from __future__ import annotations

import copy
import itertools
import threading
from collections.abc import Mapping
from datetime import datetime, timedelta

from ..domain import (
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
from ..errors import UniqueViolation


class FakeDatastore:
    def __init__(self) -> None:
        self.jurisdictions: dict[int, Jurisdiction] = {}
        self.targets: dict[int, SweepTarget] = {}
        self.artifacts: dict[int, Artifact] = {}
        self.history: dict[int, SweepHistory] = {}
        self.code_sources: dict[int, CodeSource] = {}
        self.run_switches: dict[str, RunState] = {}
        self._ids = itertools.count(1)
        self._lock = threading.RLock()

    # -- jurisdictions ----------------------------------------------------
    def get_jurisdiction(self, jurisdiction_id: int) -> Jurisdiction | None:
        with self._lock:
            j = self.jurisdictions.get(jurisdiction_id)
            return copy.copy(j) if j else None

    def insert_jurisdiction(
        self,
        name: str,
        state: str,
        level: str,
        fips: str | None = None,
        parent_id: int | None = None,
        parent_name: str | None = None,
    ) -> Jurisdiction:
        with self._lock:
            for j in self.jurisdictions.values():
                if (j.level, j.state, j.name) == (level, state, name):
                    raise UniqueViolation(f"jurisdiction ({level}, {state}, {name})")
            row = Jurisdiction(
                id=next(self._ids),
                name=name,
                state=state,
                level=level,
                fips=fips,
                parent_id=parent_id,
                parent_name=parent_name,
            )
            self.jurisdictions[row.id] = row
            return copy.copy(row)

    def state_row_exists(self, state: str) -> bool:
        with self._lock:
            return any(
                j.level == "state" and j.state == state for j in self.jurisdictions.values()
            )

    def list_jurisdictions(
        self, states: list[str] | None = None, levels: list[str] | None = None
    ) -> list[Jurisdiction]:
        with self._lock:
            rows = [
                copy.copy(j)
                for j in self.jurisdictions.values()
                if (states is None or j.state in states)
                and (levels is None or j.level in levels)
            ]
            return sorted(rows, key=lambda j: j.id)

    def get_federal_anchor(self) -> Jurisdiction | None:
        with self._lock:
            for j in self.jurisdictions.values():
                if j.level == "federal":
                    return copy.copy(j)
            return None

    # -- sweep targets ----------------------------------------------------
    def get_target(self, target_id: int) -> SweepTarget | None:
        with self._lock:
            t = self.targets.get(target_id)
            return copy.copy(t) if t else None

    def insert_target(
        self,
        corpus: str,
        jurisdiction_id: int,
        source: Source,
        priority: int,
        next_due_at: datetime,
    ) -> SweepTarget:
        with self._lock:
            for t in self.targets.values():
                if (t.corpus, t.jurisdiction_id, t.source) == (corpus, jurisdiction_id, source):
                    raise UniqueViolation(f"sweep_target ({corpus}, {jurisdiction_id}, {source})")
            row = SweepTarget(
                id=next(self._ids),
                corpus=corpus,
                jurisdiction_id=jurisdiction_id,
                source=source,
                priority=priority,
                next_due_at=next_due_at,
            )
            self.targets[row.id] = row
            return copy.copy(row)

    def list_targets(self, corpus: str, source: Source | None = None) -> list[SweepTarget]:
        with self._lock:
            rows = [
                copy.copy(t)
                for t in self.targets.values()
                if t.corpus == corpus and (source is None or t.source == source)
            ]
            return sorted(rows, key=lambda t: t.id)

    def select_due(
        self,
        corpus: str,
        now: datetime,
        dispatch_timeouts: Mapping[Source, float],
        limit: int,
    ) -> list[SweepTarget]:
        with self._lock:
            due = []
            for t in self.targets.values():
                if t.corpus != corpus or t.next_due_at > now:
                    continue
                if t.dispatched_at is not None:
                    timeout = dispatch_timeouts[t.source]
                    if t.dispatched_at > now - timedelta(seconds=timeout):
                        continue  # stamped inside the window — in flight
                due.append(copy.copy(t))
            due.sort(key=lambda t: (t.priority, t.next_due_at, t.id))
            return due[:limit]

    def stamp_target(
        self,
        target_id: int,
        dispatched_at: datetime,
        dispatch_id: str,
        query_count: int,
        expected_dispatch_id: str | None,
        expected_dispatched_at: datetime | None,
    ) -> bool:
        with self._lock:
            t = self.targets[target_id]
            if (t.dispatch_id, t.dispatched_at) != (expected_dispatch_id, expected_dispatched_at):
                return False
            t.dispatched_at = dispatched_at
            t.dispatch_id = dispatch_id
            t.query_count = query_count
            return True

    def finalize_target(
        self,
        target_id: int,
        dispatch_id: str,
        last_result: SweepResult,
        next_due_at: datetime,
    ) -> bool:
        with self._lock:
            t = self.targets.get(target_id)
            if t is None or t.dispatch_id != dispatch_id:
                return False
            t.last_result = last_result
            t.next_due_at = next_due_at
            t.dispatched_at = None
            t.dispatch_id = None
            t.query_count = None
            return True

    def park_target(
        self, target_id: int, next_due_at: datetime, last_result: SweepResult
    ) -> None:
        with self._lock:
            t = self.targets[target_id]
            t.next_due_at = next_due_at
            t.last_result = last_result
            t.dispatched_at = None
            t.dispatch_id = None
            t.query_count = None

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
        with self._lock:
            for a in self.artifacts.values():
                if (a.corpus, a.source_url) == (corpus, source_url):
                    raise UniqueViolation(f"artifact ({corpus}, {source_url})")
            row = Artifact(
                id=next(self._ids),
                corpus=corpus,
                jurisdiction_id=jurisdiction_id,
                origin=origin,
                source_url=source_url,
                context=context,
                created_at=created_at,
            )
            self.artifacts[row.id] = row
            return copy.copy(row)

    def get_artifact(self, artifact_id: int) -> Artifact | None:
        with self._lock:
            a = self.artifacts.get(artifact_id)
            return copy.copy(a) if a else None

    def update_artifact(self, artifact: Artifact) -> None:
        with self._lock:
            if artifact.id in self.artifacts:
                self.artifacts[artifact.id] = copy.copy(artifact)

    def delete_artifact(self, artifact_id: int) -> None:
        with self._lock:
            self.artifacts.pop(artifact_id, None)

    def corpus_has_sha256(
        self, corpus: str, sha256: str, exclude_artifact_id: int
    ) -> bool:
        with self._lock:
            return any(
                a.corpus == corpus and a.sha256 == sha256 and a.id != exclude_artifact_id
                for a in self.artifacts.values()
            )

    def select_pending_stale(
        self, corpus: str, now: datetime, timeout_seconds: float, limit: int
    ) -> list[Artifact]:
        from ..domain import ArtifactStatus

        with self._lock:
            stale = [
                copy.copy(a)
                for a in self.artifacts.values()
                if a.corpus == corpus
                and a.status == ArtifactStatus.PENDING
                and (
                    a.dispatched_at is None
                    or a.dispatched_at <= now - timedelta(seconds=timeout_seconds)
                )
            ]
            stale.sort(key=lambda a: a.id)
            return stale[:limit]

    def stamp_artifact(
        self, artifact_id: int, dispatched_at: datetime, dispatch_id: str
    ) -> None:
        with self._lock:
            a = self.artifacts.get(artifact_id)
            if a is not None:
                a.dispatched_at = dispatched_at
                a.dispatch_id = dispatch_id

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
        with self._lock:
            for h in self.history.values():
                if (h.dispatch_id, h.query_seq) == (dispatch_id, query_seq):
                    return False
            row = SweepHistory(
                id=next(self._ids),
                corpus=corpus,
                jurisdiction_id=jurisdiction_id,
                source=source,
                dispatch_id=dispatch_id,
                query_seq=query_seq,
                result=result,
                topic=topic,
                results_seen=results_seen,
                results_triaged_relevant=results_triaged_relevant,
                candidates_staged=candidates_staged,
                detail=detail[:500],
                swept_at=swept_at,
            )
            self.history[row.id] = row
            return True

    def list_history(
        self, corpus: str, jurisdiction_id: int | None = None
    ) -> list[SweepHistory]:
        with self._lock:
            rows = [
                copy.copy(h)
                for h in self.history.values()
                if h.corpus == corpus
                and (jurisdiction_id is None or h.jurisdiction_id == jurisdiction_id)
            ]
            return sorted(rows, key=lambda h: h.id)

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
        with self._lock:
            for c in self.code_sources.values():
                if (c.jurisdiction_id, c.url) == (jurisdiction_id, url):
                    raise UniqueViolation(f"code_source ({jurisdiction_id}, {url})")
            row = CodeSource(
                id=next(self._ids),
                jurisdiction_id=jurisdiction_id,
                url=url,
                publisher=publisher,
                enabled=enabled,
                added_by=added_by,
                added_at=added_at,
            )
            self.code_sources[row.id] = row
            return copy.copy(row)

    def list_code_sources(
        self, jurisdiction_id: int | None = None, enabled_only: bool = False
    ) -> list[CodeSource]:
        with self._lock:
            rows = [
                copy.copy(c)
                for c in self.code_sources.values()
                if (jurisdiction_id is None or c.jurisdiction_id == jurisdiction_id)
                and (not enabled_only or c.enabled)
            ]
            return sorted(rows, key=lambda c: c.id)

    def set_code_source_enabled(self, code_source_id: int, enabled: bool) -> None:
        with self._lock:
            self.code_sources[code_source_id].enabled = enabled

    # -- run switch -------------------------------------------------------
    def get_run_state(self, name: str) -> RunState | None:
        with self._lock:
            return self.run_switches.get(name)

    def set_run_state(self, name: str, state: RunState) -> None:
        with self._lock:
            self.run_switches[name] = state

    # -- corpus kill ------------------------------------------------------
    def purge_corpus(self, corpus: str) -> dict[str, int]:
        with self._lock:
            counts = {"artifacts": 0, "harvest_sweeps": 0, "sweep_targets": 0}
            for aid in [a.id for a in self.artifacts.values() if a.corpus == corpus]:
                del self.artifacts[aid]
                counts["artifacts"] += 1
            for hid in [h.id for h in self.history.values() if h.corpus == corpus]:
                del self.history[hid]
                counts["harvest_sweeps"] += 1
            for tid in [t.id for t in self.targets.values() if t.corpus == corpus]:
                del self.targets[tid]
                counts["sweep_targets"] += 1
            return counts
