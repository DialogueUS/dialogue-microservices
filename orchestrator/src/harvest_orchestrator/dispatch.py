"""Dispatch = generate + stamp + send (plan 2.4, spec §5.2).

Stamp-then-send: a crash between the two leaves a stamped row the
dispatch timeout recovers; send-then-stamp could double-dispatch with
no record. The stamp is a compare-and-set, so a concurrent
orchestrator's loser stamps nothing and sends nothing.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta

from harvest_core.config import HarvestConfig
from harvest_core.constants import CODE_DISPATCH_TIMEOUT_S, DISPATCH_TIMEOUT_S
from harvest_core.domain import Source, SweepResult, SweepTarget
from harvest_core.errors import GenerationError
from harvest_core.messages import CodeTask, SweepTask, to_json
from harvest_core.ports import Datastore, QueryGenerator, TaskQueue

log = logging.getLogger(__name__)

DISPATCH_TIMEOUTS: dict[Source, float] = {
    Source.SERPER: DISPATCH_TIMEOUT_S,
    Source.LEGAL_CODES: CODE_DISPATCH_TIMEOUT_S,
}


def dispatch_cycle(
    ds: Datastore,
    generator: QueryGenerator,
    sweep_queue: TaskQueue,
    code_queue: TaskQueue,
    config: HarvestConfig,
    now: datetime,
) -> int:
    """One dispatch pass; returns the number of targets dispatched."""
    due = ds.select_due(
        config.name, now, DISPATCH_TIMEOUTS, config.max_sweeps_per_dispatch
    )
    dispatched = 0
    for target in due:
        try:
            if target.source == Source.SERPER:
                ok = _dispatch_serper(ds, generator, sweep_queue, config, target, now)
            else:
                ok = _dispatch_code(ds, code_queue, config, target, now)
        except Exception:
            # A dead queue (or any send failure) leaves a stamped row the
            # dispatch timeout recovers; the cycle moves on.
            log.exception("dispatch failed for target %s", target.id)
            continue
        if ok:
            dispatched += 1
    return dispatched


def _dispatch_serper(
    ds: Datastore,
    generator: QueryGenerator,
    queue: TaskQueue,
    config: HarvestConfig,
    target: SweepTarget,
    now: datetime,
) -> bool:
    jur = ds.get_jurisdiction(target.jurisdiction_id)
    if jur is None:
        return False
    from .generate import generate_queries

    try:
        pairs = generate_queries(generator, jur, config)
    except GenerationError:
        # Skip this jurisdiction, leave the row untouched: it stays due
        # and is retried next cycle. One bad generation never blocks the batch.
        log.warning("query generation failed for %s; skipped", jur.name)
        return False

    dispatch_id = str(uuid.uuid4())
    if not ds.stamp_target(
        target.id, now, dispatch_id, len(pairs), target.dispatch_id, target.dispatched_at
    ):
        return False  # another orchestrator won this row
    for seq, (topic, query) in enumerate(pairs):
        queue.send(
            to_json(
                SweepTask(
                    corpus=config.name,
                    sweep_target_id=target.id,
                    jurisdiction_id=jur.id,
                    topic=topic,
                    query_text=query,
                    dispatch_id=dispatch_id,
                    query_seq=seq,
                    query_count=len(pairs),
                    dispatched_at=now,
                )
            )
        )
    return True


def _dispatch_code(
    ds: Datastore,
    queue: TaskQueue,
    config: HarvestConfig,
    target: SweepTarget,
    now: datetime,
) -> bool:
    seeds = ds.list_code_sources(target.jurisdiction_id, enabled_only=True)
    if not seeds:
        ds.park_target(
            target.id,
            now + timedelta(days=config.resweep_interval_days),
            SweepResult.ERROR,
        )
        return False
    dispatch_id = str(uuid.uuid4())
    if not ds.stamp_target(
        target.id, now, dispatch_id, 1, target.dispatch_id, target.dispatched_at
    ):
        return False
    queue.send(
        to_json(
            CodeTask(
                corpus=config.name,
                sweep_target_id=target.id,
                jurisdiction_id=target.jurisdiction_id,
                portal_urls=[s.url for s in seeds],
                dispatch_id=dispatch_id,
                dispatched_at=now,
            )
        )
    )
    return True
