"""Harvester process: a thread pool partitioned by --role (spec §6).

Threads long-poll their queue; no ticks, no phases. Only the code role
needs the Playwright browser runtime.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

from harvest_core.config import HarvestConfig
from harvest_core.constants import SWEEP_BATCH_RECEIVE
from harvest_core.ports import (
    Clock,
    Datastore,
    Fetcher,
    KeyValue,
    ObjectStore,
    PortalDiscoverer,
    SearchProvider,
    TaskQueue,
    Triage,
)

from .code import CodeWorker
from .consumer import ConsumerLoop
from .fetch import FetchWorker, HostThrottle
from .sweep import SweepWorker

log = logging.getLogger(__name__)

ROLES = ("sweep", "code", "fetch", "all")


@dataclass
class HarvesterDeps:
    ds: Datastore
    kv: KeyValue
    objects: ObjectStore
    search: SearchProvider
    triage: Triage
    fetcher: Fetcher
    discoverer: PortalDiscoverer
    sweep_queue: TaskQueue
    code_queue: TaskQueue
    fetch_queue: TaskQueue
    clock: Clock


class HarvesterService:
    def __init__(self, deps: HarvesterDeps, config: HarvestConfig) -> None:
        self._deps = deps
        self._config = config
        self._stop = threading.Event()
        throttle = HostThrottle(deps.clock)
        self.sweep_worker = SweepWorker(
            deps.ds,
            deps.kv,
            deps.search,
            deps.triage,
            deps.fetcher,
            deps.sweep_queue,
            deps.fetch_queue,
            deps.clock,
            config,
        )
        self.code_worker = CodeWorker(
            deps.ds,
            deps.kv,
            deps.discoverer,
            deps.code_queue,
            deps.fetch_queue,
            deps.clock,
            config,
        )
        self.fetch_worker = FetchWorker(
            deps.ds,
            deps.kv,
            deps.objects,
            deps.fetcher,
            deps.fetch_queue,
            deps.clock,
            config,
            throttle=throttle,
        )

    def stop(self) -> None:
        self._stop.set()

    def _loops_for(self, role: str) -> list[ConsumerLoop]:
        loops = []
        if role in ("sweep", "all"):
            loops.append(
                ConsumerLoop(
                    self._deps.sweep_queue,
                    self.sweep_worker.handle_batch,
                    SWEEP_BATCH_RECEIVE,
                    self._deps.clock,
                )
            )
        if role in ("code", "all"):
            loops.append(
                ConsumerLoop(
                    self._deps.code_queue, self.code_worker.handle_batch, 1, self._deps.clock
                )
            )
        if role in ("fetch", "all"):
            loops.append(
                ConsumerLoop(
                    self._deps.fetch_queue, self.fetch_worker.handle_batch, 1, self._deps.clock
                )
            )
        return loops

    def run(self, role: str, threads: int) -> None:
        """Blocks until stop(). Threads are split evenly across the
        role's loops (role=all -> sweep/code/fetch round-robin)."""
        loops = self._loops_for(role)
        pool: list[threading.Thread] = []
        for i in range(max(threads, len(loops))):
            loop = loops[i % len(loops)]
            t = threading.Thread(
                target=loop.run_forever, args=(self._stop,), name=f"{role}-{i}", daemon=True
            )
            t.start()
            pool.append(t)
        for t in pool:
            t.join()
