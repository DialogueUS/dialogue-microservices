"""Sweep fan-in (plan 3.3, spec §6.2).

HINCRBY on sweep:{dispatch_id} after each query's history commit; the
worker whose increment reaches query_count finalizes the target row,
gated by dispatch_id. Fan-in is an optimization for promptness — a lost
counter never finalizes and the dispatch timeout re-dispatches.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from harvest_core.constants import ERROR_RETRY_DAYS, FANIN_COUNTER_TTL_S
from harvest_core.domain import SweepResult
from harvest_core.ports import Datastore, KeyValue

log = logging.getLogger(__name__)


def counter_key(dispatch_id: str) -> str:
    return f"sweep:{dispatch_id}"


def record_query_done(
    kv: KeyValue,
    ds: Datastore,
    sweep_target_id: int,
    dispatch_id: str,
    query_count: int,
    candidates_staged: int,
    errored: bool,
    now: datetime,
    resweep_interval_days: int,
) -> bool:
    """Increment the fan-in counter; finalize the target when this
    increment completes the dispatch. Returns True when it finalized."""
    key = counter_key(dispatch_id)
    counts = kv.hincrby(
        key,
        {
            "done": 1,
            "candidates": candidates_staged,
            "errors": 1 if errored else 0,
        },
    )
    kv.expire(key, FANIN_COUNTER_TTL_S)
    if counts.get("done", 0) != query_count:
        return False

    # Aggregate write-back (spec §6.2): a transient failure must never
    # black out a jurisdiction — genuine errors retry in 1 day.
    if counts.get("errors", 0) >= query_count and counts.get("candidates", 0) == 0:
        result = SweepResult.ERROR
        next_due = now + timedelta(days=ERROR_RETRY_DAYS)
    else:
        result = (
            SweepResult.CANDIDATES
            if counts.get("candidates", 0) > 0
            else SweepResult.NOT_FOUND
        )
        next_due = now + timedelta(days=resweep_interval_days)
    finalized = ds.finalize_target(sweep_target_id, dispatch_id, result, next_due)
    if not finalized:
        log.info("fan-in finalize lost the dispatch_id gate for target %s", sweep_target_id)
    return finalized
