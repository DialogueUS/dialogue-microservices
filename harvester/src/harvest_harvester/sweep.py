"""Sweep worker (plan 3.2, spec §6.1): a batch of query tasks.

The rate-limit invariant is load-bearing: a 429 (or a triage error)
records nothing at all — no history row, no counter increment, message
left undeleted — so the same query text redelivers later, for free.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from harvest_core.config import HarvestConfig
from harvest_core.constants import PAGE_BYTE_CAP
from harvest_core.domain import Jurisdiction, Source, SweepResult
from harvest_core.errors import RateLimited, TriageError
from harvest_core.messages import SweepTask, parse_task
from harvest_core.ports import (
    Clock,
    Datastore,
    Fetcher,
    KeyValue,
    QueueMessage,
    SearchProvider,
    SearchResult,
    TaskQueue,
    Triage,
    TriageRequest,
    TriageVerdict,
)

from .consumer import gate_sweep
from .fanin import record_query_done
from .links import extract_document_links, is_document_url
from .staging import Candidate, stage_candidate

log = logging.getLogger(__name__)


@dataclass
class _Item:
    message: QueueMessage
    task: SweepTask
    jurisdiction: Jurisdiction
    results: list[SearchResult] = field(default_factory=list)
    verdicts: list[TriageVerdict] = field(default_factory=list)
    errored: bool = False
    error_detail: str = ""


class SweepWorker:
    def __init__(
        self,
        ds: Datastore,
        kv: KeyValue,
        search: SearchProvider,
        triage: Triage,
        fetcher: Fetcher,
        sweep_queue: TaskQueue,
        fetch_queue: TaskQueue,
        clock: Clock,
        config: HarvestConfig,
    ) -> None:
        self._ds = ds
        self._kv = kv
        self._search = search
        self._triage = triage
        self._fetcher = fetcher
        self._queue = sweep_queue
        self._fetch_queue = fetch_queue
        self._clock = clock
        self._config = config

    def handle_batch(self, messages: list[QueueMessage]) -> None:
        # 1. Parse + idempotency gate per message.
        items: list[_Item] = []
        for msg in messages:
            try:
                task = parse_task(msg.body)
            except Exception:
                log.warning("unparseable sweep message %s; deleting", msg.id)
                self._queue.delete(msg.id)
                continue
            if not isinstance(task, SweepTask):
                self._queue.delete(msg.id)
                continue
            if not gate_sweep(self._ds, task):
                self._queue.delete(msg.id)
                continue
            jur = self._ds.get_jurisdiction(task.jurisdiction_id)
            if jur is None:
                self._queue.delete(msg.id)
                continue
            items.append(_Item(message=msg, task=task, jurisdiction=jur))

        # 2. Search, serially within the thread. 429 → that message is
        # left untouched (redelivered with the same query text later).
        searched: list[_Item] = []
        for item in items:
            try:
                item.results = self._search.search(
                    item.task.query_text, self._config.search_count
                )
            except RateLimited:
                log.info("429 for %r; deferred via redelivery", item.task.query_text)
                continue
            except Exception as exc:
                item.errored = True
                item.error_detail = f"search error: {exc}"
            searched.append(item)

        # 3. One batched triage call, splitting past the results cap.
        # An LLM error is transient exactly like a rate limit: the
        # affected messages are left undeleted, nothing is recorded.
        completed: list[_Item] = []
        for chunk in self._triage_chunks([i for i in searched if not i.errored]):
            requests = [
                TriageRequest(
                    jurisdiction_name=i.jurisdiction.name,
                    state=i.jurisdiction.state,
                    level=i.jurisdiction.level,
                    topic=i.task.topic,
                    results=i.results,
                )
                for i in chunk
            ]
            try:
                verdict_lists = self._triage.triage(requests) if requests else []
            except TriageError:
                log.info("triage failed for %d message(s); deferred", len(chunk))
                continue
            for item, verdicts in zip(chunk, verdict_lists, strict=True):
                item.verdicts = verdicts
                completed.append(item)
        completed.extend(i for i in searched if i.errored)

        # 4-6. Extract, stage, write back, delete — per completed message.
        for item in completed:
            self._complete(item)

    def _triage_chunks(self, items: list[_Item]) -> list[list[_Item]]:
        cap = self._config.triage_batch_max_results
        chunks: list[list[_Item]] = []
        current: list[_Item] = []
        count = 0
        for item in items:
            n = len(item.results)
            if current and count + n > cap:
                chunks.append(current)
                current, count = [], 0
            current.append(item)
            count += n
        if current:
            chunks.append(current)
        return chunks

    def _candidates_for(self, item: _Item) -> tuple[list[Candidate], int]:
        """(ordered candidates, relevant_count). Direct hits first with
        PDFs floated, then linked documents (old spec §5.5 ordering)."""
        direct: list[Candidate] = []
        linked: list[Candidate] = []
        relevant = 0
        for result, verdict in zip(item.results, item.verdicts, strict=True):
            if not verdict.relevant:
                continue  # never fetch what the filter rejected
            relevant += 1
            if is_document_url(result.url) or verdict.is_document:
                direct.append(
                    Candidate(url=result.url, context=f"{result.title}\n{result.snippet}")
                )
                continue
            # An HTML page: scrape that single page for document links.
            try:
                resp = self._fetcher.get(result.url, PAGE_BYTE_CAP)
            except Exception:
                continue  # a dead page contributes nothing
            if resp.status != 200:
                continue
            try:
                page_html = resp.content.decode("utf-8", errors="replace")
                links = extract_document_links(page_html, result.url)
            except Exception:
                continue
            for link in links:
                context = f"linked from {result.url}"
                if link.anchor_text:
                    context += f"\n{link.anchor_text}"
                linked.append(Candidate(url=link.url, context=context))

        pdf_direct = [c for c in direct if c.url.lower().split("?")[0].endswith(".pdf")]
        other_direct = [c for c in direct if c not in pdf_direct]
        return pdf_direct + other_direct + linked, relevant

    def _complete(self, item: _Item) -> None:
        task = item.task
        staged = 0
        relevant = 0
        if not item.errored:
            try:
                candidates, relevant = self._candidates_for(item)
                for candidate in candidates:
                    if stage_candidate(
                        self._ds,
                        self._kv,
                        self._fetch_queue,
                        self._clock,
                        task.corpus,
                        task.jurisdiction_id,
                        "serper",
                        candidate,
                    ):
                        staged += 1
            except Exception as exc:
                item.errored = True
                item.error_detail = f"extraction error: {exc}"

        if item.errored:
            result = SweepResult.ERROR
        elif staged > 0:
            result = SweepResult.CANDIDATES
        else:
            result = SweepResult.NOT_FOUND

        detail = f"query: {task.query_text}"
        if item.error_detail:
            detail += f" | {item.error_detail}"

        now = self._clock.now()
        inserted = self._ds.insert_history(
            corpus=task.corpus,
            jurisdiction_id=task.jurisdiction_id,
            source=Source.SERPER,
            dispatch_id=task.dispatch_id,
            query_seq=task.query_seq,
            result=result,
            topic=task.topic,
            results_seen=len(item.results),
            results_triaged_relevant=relevant,
            candidates_staged=staged,
            detail=detail,
            swept_at=now,
        )
        if inserted:
            # Counter increments only follow a first-time history commit:
            # a redelivered completed query writes nothing twice.
            record_query_done(
                self._kv,
                self._ds,
                task.sweep_target_id,
                task.dispatch_id,
                task.query_count,
                staged,
                item.errored,
                now,
                self._config.resweep_interval_days,
            )
        self._queue.delete(item.message.id)
