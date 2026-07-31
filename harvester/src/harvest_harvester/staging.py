"""Candidate staging (spec §6.1 step 5) — shared by sweep and code workers.

The Postgres insert is the authoritative dedupe; Redis SETNX is written
only after a successful insert (§6.5), so a lost Redis key can only
cost one constraint hit, never a false skip.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from dataclasses import dataclass

from harvest_core.errors import UniqueViolation
from harvest_core.messages import FetchTask, to_json
from harvest_core.ports import Clock, Datastore, KeyValue, TaskQueue

log = logging.getLogger(__name__)


def sanitize_text(text: str) -> str:
    """Strip NULs and lone UTF-16 surrogates — Postgres TEXT holds neither,
    and one poisoned row must never wedge a pass (old spec §6.4)."""
    text = text.replace("\x00", "")
    return "".join(c for c in text if not (0xD800 <= ord(c) <= 0xDFFF))


def url_key(corpus: str, url: str) -> str:
    return f"url:{corpus}:{hashlib.sha256(url.encode()).hexdigest()}"


@dataclass
class Candidate:
    url: str
    context: str


def stage_candidate(
    ds: Datastore,
    kv: KeyValue,
    fetch_queue: TaskQueue,
    clock: Clock,
    corpus: str,
    jurisdiction_id: int,
    origin: str,
    candidate: Candidate,
) -> bool:
    """Stage one URL: dedupe -> insert -> SETNX -> stamp -> publish.

    Returns True when this call staged the artifact (and published its
    fetch task); False when it was already staged. A crash between
    insert and publish is recovered by orchestrator reconciliation.
    """
    key = url_key(corpus, candidate.url)
    if kv.get(key) is not None:
        return False  # fast path: this corpus already staged this URL
    now = clock.now()
    try:
        artifact = ds.insert_artifact(
            corpus,
            jurisdiction_id,
            origin,
            candidate.url[:600],
            sanitize_text(candidate.context),
            now,
        )
    except UniqueViolation:
        kv.setnx(key, "1")  # heal the cache after a Redis false negative
        return False
    kv.setnx(key, "1")

    dispatch_id = str(uuid.uuid4())
    ds.stamp_artifact(artifact.id, now, dispatch_id)
    fetch_queue.send(
        to_json(
            FetchTask(
                corpus=corpus,
                artifact_id=artifact.id,
                dispatch_id=dispatch_id,
                dispatched_at=now,
            )
        )
    )
    return True
