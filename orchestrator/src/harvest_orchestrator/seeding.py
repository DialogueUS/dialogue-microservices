"""Scope resolution and sweep-target seeding (plan 2.2, spec §5.1).

Every in-scope jurisdiction gets a `serper` row; jurisdictions with an
enabled code_sources seed also get a `legal_codes` row. A `legal_codes`
row whose last enabled seed is disabled is parked, never deleted.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from harvest_core.config import HarvestConfig
from harvest_core.domain import (
    Jurisdiction,
    Source,
    SweepResult,
    level_priority,
)
from harvest_core.errors import UniqueViolation
from harvest_core.ports import Datastore

from .census import ensure_federal_anchor


def resolve_scope(ds: Datastore, config: HarvestConfig) -> list[Jurisdiction]:
    scope = config.scope
    if scope.region_query:
        needle = scope.region_query.lower()
        pool = [
            j
            for j in ds.list_jurisdictions()
            if needle in j.name.lower() or j.name.lower() in needle
        ]
    else:
        states = None if scope.states == ["ALL"] else scope.states
        pool = ds.list_jurisdictions(states=states, levels=scope.levels)
        if "federal" in scope.levels:
            anchor_id = ensure_federal_anchor(ds)
            anchor = ds.get_jurisdiction(anchor_id)
            if anchor is not None and all(j.id != anchor.id for j in pool):
                pool.append(anchor)

    if scope.within:
        wanted = {w.lower() for w in scope.within}
        pool = [
            j
            for j in pool
            if j.name.lower() in wanted
            or (j.parent_name or "").lower() in wanted
            or j.level == "federal"
        ]
    if scope.only:
        exact = set(scope.only)
        pool = [j for j in pool if j.name in exact]

    # Comma-county exclusion: legacy artifacts of an unsplit Census county
    # column; retained in the table, never swept.
    return [j for j in pool if not (j.level == "county" and "," in j.name)]


def _backfill_due(
    ds: Datastore, corpus: str, jurisdiction_id: int, source: Source, now: datetime, config:
    HarvestConfig,
) -> datetime:
    """Due-date backfill from prior history (old spec §4): rebuilding the
    queue never re-purchases work already done."""
    history = [
        h
        for h in ds.list_history(corpus, jurisdiction_id)
        if h.source == source and h.swept_at is not None
    ]
    if not history:
        return now
    non_error = [h for h in history if h.result != SweepResult.ERROR]
    if non_error:
        newest = max(h.swept_at for h in non_error)  # type: ignore[type-var]
        assert newest is not None
        return newest + timedelta(days=config.resweep_interval_days)
    newest_any = max(h.swept_at for h in history)  # type: ignore[type-var]
    assert newest_any is not None
    return newest_any + timedelta(days=1)


def seed_targets(ds: Datastore, config: HarvestConfig, now: datetime) -> int:
    """Insert missing sweep_targets rows; park orphaned legal_codes rows.

    Returns the number of rows inserted."""
    in_scope = resolve_scope(ds, config)
    existing = {
        (t.jurisdiction_id, t.source): t for t in ds.list_targets(config.name)
    }
    with_seeds = {
        c.jurisdiction_id for c in ds.list_code_sources(enabled_only=True)
    }

    inserted = 0
    for jur in in_scope:
        wanted_sources = [Source.SERPER]
        if jur.id in with_seeds:
            wanted_sources.append(Source.LEGAL_CODES)
        for source in wanted_sources:
            if (jur.id, source) in existing:
                continue
            due = _backfill_due(ds, config.name, jur.id, source, now, config)
            try:
                ds.insert_target(
                    config.name, jur.id, source, level_priority(jur.level), due
                )
                inserted += 1
            except UniqueViolation:
                pass  # another orchestrator won the race — harmless

    # Park legal_codes rows whose seeds are all disabled (spec §5.1).
    for (jur_id, source), target in existing.items():
        if source != Source.LEGAL_CODES or jur_id in with_seeds:
            continue
        if target.next_due_at <= now:
            ds.park_target(
                target.id,
                now + timedelta(days=config.resweep_interval_days),
                SweepResult.ERROR,
            )
    return inserted
