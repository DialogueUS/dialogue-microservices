"""Fetch worker (plan 3.6, spec §6.4): one URL task -> stored document.

Dead links (400/401/403/404/410/unreachable) delete the row outright;
other errors count attempts to 3. Redis answers fast, Postgres answers
right: the (corpus, sha256) backstop catches lost doc-hash keys.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from datetime import datetime, timedelta
from urllib.parse import urlsplit

from harvest_core.config import HarvestConfig
from harvest_core.constants import (
    DEAD_LINK_STATUSES,
    DOCUMENT_BYTE_CAP,
    FETCH_ATTEMPT_LIMIT,
    LAST_ERROR_CHARS,
    PER_HOST_FETCH_SPACING_S,
)
from harvest_core.domain import Artifact, ArtifactStatus, check_transition
from harvest_core.errors import FetchError, UnreachableHost
from harvest_core.messages import FetchTask, parse_task
from harvest_core.ports import (
    Clock,
    Datastore,
    Fetcher,
    KeyValue,
    ObjectStore,
    QueueMessage,
    TaskQueue,
)
from harvest_core.storage import filename_from_url, object_key

from .consumer import gate_fetch
from .extract import extract_text
from .sniff import EXT_CONTENT_TYPES, sniff
from .staging import sanitize_text

log = logging.getLogger(__name__)


class HostThrottle:
    """Per-host token bucket: >= 1 s spacing per domain across the pool."""

    def __init__(self, clock: Clock, spacing_seconds: float = PER_HOST_FETCH_SPACING_S) -> None:
        self._clock = clock
        self._spacing = spacing_seconds
        self._next_allowed: dict[str, datetime] = {}
        self._lock = threading.Lock()

    def wait(self, url: str) -> None:
        host = urlsplit(url).netloc.lower()
        while True:
            with self._lock:
                now = self._clock.now()
                allowed_at = self._next_allowed.get(host)
                if allowed_at is None or allowed_at <= now:
                    self._next_allowed[host] = now + timedelta(seconds=self._spacing)
                    return
                wait_s = (allowed_at - now).total_seconds()
            self._clock.sleep(wait_s)


def doc_key(corpus: str, sha256: str) -> str:
    return f"doc:{corpus}:{sha256}"


class FetchWorker:
    def __init__(
        self,
        ds: Datastore,
        kv: KeyValue,
        objects: ObjectStore,
        fetcher: Fetcher,
        fetch_queue: TaskQueue,
        clock: Clock,
        config: HarvestConfig,
        throttle: HostThrottle | None = None,
    ) -> None:
        self._ds = ds
        self._kv = kv
        self._objects = objects
        self._fetcher = fetcher
        self._queue = fetch_queue
        self._clock = clock
        self._config = config
        self._throttle = throttle or HostThrottle(clock)

    def handle_batch(self, messages: list[QueueMessage]) -> None:
        for msg in messages:
            self.handle_one(msg)

    def handle_one(self, msg: QueueMessage) -> None:
        try:
            task = parse_task(msg.body)
        except Exception:
            log.warning("unparseable fetch message %s; deleting", msg.id)
            self._queue.delete(msg.id)
            return
        if not isinstance(task, FetchTask):
            self._queue.delete(msg.id)
            return
        artifact = gate_fetch(self._ds, task)
        if artifact is None:
            self._queue.delete(msg.id)
            return

        self._throttle.wait(artifact.source_url)
        try:
            resp = self._fetcher.get(artifact.source_url, DOCUMENT_BYTE_CAP)
        except UnreachableHost:
            # A dead link: nothing to retry, no value in a tombstone.
            self._ds.delete_artifact(artifact.id)
            self._queue.delete(msg.id)
            return
        except FetchError as exc:
            self._record_attempt(artifact, str(exc))
            self._queue.delete(msg.id)
            return

        if resp.status in DEAD_LINK_STATUSES:
            self._ds.delete_artifact(artifact.id)
            self._queue.delete(msg.id)
            return
        if resp.status != 200:
            self._record_attempt(artifact, f"http {resp.status}")
            self._queue.delete(msg.id)
            return

        ext = sniff(resp.content, resp.content_type, artifact.source_url)
        if ext is None:
            check_transition(artifact.status, ArtifactStatus.NOT_DOCUMENT)
            artifact.status = ArtifactStatus.NOT_DOCUMENT
            artifact.content_type = resp.content_type
            self._ds.update_artifact(artifact)
            self._queue.delete(msg.id)
            return

        sha = hashlib.sha256(resp.content).hexdigest()
        key = doc_key(task.corpus, sha)
        duplicate = self._kv.get(key) is not None
        if not duplicate and self._ds.corpus_has_sha256(
            task.corpus, sha, exclude_artifact_id=artifact.id
        ):
            duplicate = True  # Postgres backstop caught a lost Redis key
            self._kv.setnx(key, "1")  # heal the cache
        if duplicate:
            check_transition(artifact.status, ArtifactStatus.DUPLICATE)
            artifact.status = ArtifactStatus.DUPLICATE
            artifact.sha256 = sha
            self._ds.update_artifact(artifact)
            self._queue.delete(msg.id)
            return

        jur = self._ds.get_jurisdiction(artifact.jurisdiction_id)
        jur_name = jur.name if jur else str(artifact.jurisdiction_id)
        filename = filename_from_url(artifact.source_url, ext)
        storage_key = object_key(task.corpus, jur_name, sha, filename)
        content_type = resp.content_type or EXT_CONTENT_TYPES.get(
            ext, "application/octet-stream"
        )
        self._objects.put(storage_key, resp.content, content_type)

        check_transition(artifact.status, ArtifactStatus.FETCHED)
        artifact.status = ArtifactStatus.FETCHED
        artifact.filename = filename
        artifact.ext = ext
        artifact.content_type = content_type
        artifact.sha256 = sha
        artifact.path = storage_key
        artifact.size_bytes = len(resp.content)
        artifact.extracted_text = extract_text(ext, resp.content)
        self._ds.update_artifact(artifact)
        # SETNX from actual bytes, only after the authoritative commit.
        self._kv.setnx(key, "1")
        self._queue.delete(msg.id)

    def _record_attempt(self, artifact: Artifact, error: str) -> None:
        artifact.attempts += 1
        artifact.last_error = sanitize_text(error)[:LAST_ERROR_CHARS]
        if artifact.attempts >= FETCH_ATTEMPT_LIMIT:
            check_transition(artifact.status, ArtifactStatus.FAILED)
            artifact.status = ArtifactStatus.FAILED
        else:
            # Still pending: the stale dispatch stamp brings it back via
            # orchestrator reconciliation after the dispatch timeout.
            artifact.dispatched_at = None
            artifact.dispatch_id = None
        self._ds.update_artifact(artifact)
