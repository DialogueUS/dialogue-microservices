"""Code worker (plan 3.4, spec §6.3): one portal crawl per task.

No LLM anywhere in this path — the seed is human-vetted, so there is
nothing to triage. The crawl is its own fan-in (query_count = 1).
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import timedelta

from harvest_core.config import HarvestConfig
from harvest_core.constants import (
    CODE_DEADLINE_S,
    CODE_HEARTBEAT_S,
    CODE_VISIBILITY_TIMEOUT_S,
    ERROR_RETRY_DAYS,
)
from harvest_core.domain import Publisher, Source, SweepResult
from harvest_core.errors import PortalError, TransientPortalError
from harvest_core.messages import CodeTask, parse_task
from harvest_core.ports import (
    Clock,
    Datastore,
    KeyValue,
    PortalCandidate,
    PortalDiscoverer,
    QueueMessage,
    TaskQueue,
)

from .consumer import Heartbeat, gate_code
from .staging import Candidate, stage_candidate

log = logging.getLogger(__name__)


class CodeWorker:
    def __init__(
        self,
        ds: Datastore,
        kv: KeyValue,
        discoverer: PortalDiscoverer,
        code_queue: TaskQueue,
        fetch_queue: TaskQueue,
        clock: Clock,
        config: HarvestConfig,
    ) -> None:
        self._ds = ds
        self._kv = kv
        self._discoverer = discoverer
        self._queue = code_queue
        self._fetch_queue = fetch_queue
        self._clock = clock
        self._config = config

    def handle_batch(self, messages: list[QueueMessage]) -> None:
        for msg in messages:
            self.handle_one(msg)

    def handle_one(self, msg: QueueMessage) -> None:
        try:
            task = parse_task(msg.body)
        except Exception:
            log.warning("unparseable code message %s; deleting", msg.id)
            self._queue.delete(msg.id)
            return
        if not isinstance(task, CodeTask):
            self._queue.delete(msg.id)
            return
        if not gate_code(self._ds, task):
            self._queue.delete(msg.id)
            return

        heartbeat = Heartbeat(
            self._queue,
            msg.id,
            self._clock,
            interval_seconds=CODE_HEARTBEAT_S,
            extension_seconds=CODE_VISIBILITY_TIMEOUT_S,
            deadline_seconds=CODE_DEADLINE_S,
        )

        # Publishers come from code_sources (the message carries URLs only).
        publisher_by_url = {
            s.url: s.publisher
            for s in self._ds.list_code_sources(task.jurisdiction_id)
        }
        groups: dict[Publisher, list[str]] = defaultdict(list)
        for url in task.portal_urls:
            groups[publisher_by_url.get(url, Publisher.OTHER)].append(url)

        staged = 0
        complete = True
        errors: list[str] = []
        details: list[str] = []
        for publisher, urls in groups.items():
            heartbeat.maybe_beat()
            if heartbeat.expired():
                complete = False
                details.append("deadline hit before all portals were crawled")
                break
            try:
                result = self._discoverer.discover(
                    urls, publisher, self._config.code_max_pages, heartbeat.deadline
                )
            except TransientPortalError as exc:
                # Portal unreachable / anti-bot / render hang: nothing is
                # recorded and the message is left undeleted — visibility
                # redelivers. Candidates already staged this run are kept.
                log.info("transient portal failure (%s); deferred via redelivery", exc)
                return
            except PortalError as exc:
                errors.append(str(exc))
                continue
            staged += self._stage_all(task, result.candidates)
            heartbeat.maybe_beat()
            if not result.complete:
                complete = False
                if result.detail:
                    details.append(result.detail)

        now = self._clock.now()
        if errors and staged == 0 and complete:
            result_value = SweepResult.ERROR
            next_due = now + timedelta(days=ERROR_RETRY_DAYS)
            details.extend(errors)
        elif not complete:
            # Deadline hit with candidates staged: resume discovery
            # tomorrow — URL dedupe makes the re-crawl incremental.
            result_value = (
                SweepResult.CANDIDATES if staged > 0 else SweepResult.ERROR
            )
            next_due = now + timedelta(days=ERROR_RETRY_DAYS)
            details.append("truncated; resuming in 1 day")
        else:
            result_value = (
                SweepResult.CANDIDATES if staged > 0 else SweepResult.NOT_FOUND
            )
            next_due = now + timedelta(days=self._config.resweep_interval_days)

        inserted = self._ds.insert_history(
            corpus=task.corpus,
            jurisdiction_id=task.jurisdiction_id,
            source=Source.LEGAL_CODES,
            dispatch_id=task.dispatch_id,
            query_seq=0,
            result=result_value,
            topic=None,
            results_seen=0,
            results_triaged_relevant=0,
            candidates_staged=staged,
            detail=" | ".join(details) if details else "portal crawl",
            swept_at=now,
        )
        if inserted:
            self._ds.finalize_target(
                task.sweep_target_id, task.dispatch_id, result_value, next_due
            )
        self._queue.delete(msg.id)

    def _stage_all(self, task: CodeTask, candidates: list[PortalCandidate]) -> int:
        staged = 0
        for c in candidates:
            if stage_candidate(
                self._ds,
                self._kv,
                self._fetch_queue,
                self._clock,
                task.corpus,
                task.jurisdiction_id,
                "legal_codes",
                Candidate(url=c.url, context=c.context),
            ):
                staged += 1
        return staged
