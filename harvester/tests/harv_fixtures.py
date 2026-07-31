from __future__ import annotations

import uuid
from dataclasses import dataclass

import pytest
from harvest_core.config import HarvestConfig
from harvest_core.domain import Publisher, RunState, Source, SweepTarget
from harvest_core.fakes import (
    FakeDatastore,
    FakeFetcher,
    FakeKeyValue,
    FakeLLM,
    FakeObjectStore,
    FakePortalDiscoverer,
    FakeQueue,
    FakeSearch,
    VirtualClock,
)
from harvest_core.messages import CodeTask, SweepTask, to_json
from harvest_harvester.code import CodeWorker
from harvest_harvester.fetch import FetchWorker, HostThrottle
from harvest_harvester.sweep import SweepWorker


def make_config(**overrides: object) -> HarvestConfig:
    base: dict[str, object] = {
        "mode": "harvest",
        "name": "test-corpus",
        "scope": {"levels": ["city"], "states": ["CA"]},
        "topics": ["noise"],
    }
    base.update(overrides)
    return HarvestConfig.model_validate(base)


@dataclass
class HWorld:
    clock: VirtualClock
    ds: FakeDatastore
    kv: FakeKeyValue
    objects: FakeObjectStore
    search: FakeSearch
    llm: FakeLLM
    fetcher: FakeFetcher
    discoverer: FakePortalDiscoverer
    sweep_queue: FakeQueue
    code_queue: FakeQueue
    fetch_queue: FakeQueue
    config: HarvestConfig

    def sweep_worker(self) -> SweepWorker:
        return SweepWorker(
            self.ds,
            self.kv,
            self.search,
            self.llm,
            self.fetcher,
            self.sweep_queue,
            self.fetch_queue,
            self.clock,
            self.config,
        )

    def code_worker(self) -> CodeWorker:
        return CodeWorker(
            self.ds,
            self.kv,
            self.discoverer,
            self.code_queue,
            self.fetch_queue,
            self.clock,
            self.config,
        )

    def fetch_worker(self) -> FetchWorker:
        return FetchWorker(
            self.ds,
            self.kv,
            self.objects,
            self.fetcher,
            self.fetch_queue,
            self.clock,
            self.config,
            throttle=HostThrottle(self.clock),
        )

    def add_city(self, name: str = "Pasadena") -> int:
        return self.ds.insert_jurisdiction(name, "CA", "city").id

    def add_target(self, jurisdiction_id: int, source: Source = Source.SERPER) -> SweepTarget:
        return self.ds.insert_target(
            self.config.name, jurisdiction_id, source, 3, self.clock.now()
        )

    def dispatch_sweep(
        self, target: SweepTarget, queries: list[tuple[str, str]]
    ) -> list[SweepTask]:
        """Stamp the target and enqueue one message per (topic, query)."""
        dispatch_id = str(uuid.uuid4())
        now = self.clock.now()
        assert self.ds.stamp_target(
            target.id, now, dispatch_id, len(queries), target.dispatch_id, target.dispatched_at
        )
        tasks = []
        for seq, (topic, query) in enumerate(queries):
            task = SweepTask(
                corpus=self.config.name,
                sweep_target_id=target.id,
                jurisdiction_id=target.jurisdiction_id,
                topic=topic,
                query_text=query,
                dispatch_id=dispatch_id,
                query_seq=seq,
                query_count=len(queries),
                dispatched_at=now,
            )
            self.sweep_queue.send(to_json(task))
            tasks.append(task)
        return tasks

    def dispatch_code(self, target: SweepTarget, portal_urls: list[str]) -> CodeTask:
        dispatch_id = str(uuid.uuid4())
        now = self.clock.now()
        assert self.ds.stamp_target(
            target.id, now, dispatch_id, 1, target.dispatch_id, target.dispatched_at
        )
        task = CodeTask(
            corpus=self.config.name,
            sweep_target_id=target.id,
            jurisdiction_id=target.jurisdiction_id,
            portal_urls=portal_urls,
            dispatch_id=dispatch_id,
            dispatched_at=now,
        )
        self.code_queue.send(to_json(task))
        return task

    def add_code_source(
        self, jurisdiction_id: int, url: str, publisher: Publisher = Publisher.MUNICODE
    ) -> None:
        self.ds.insert_code_source(
            jurisdiction_id, url, publisher, True, None, self.clock.now()
        )


@pytest.fixture()
def hworld() -> HWorld:
    clock = VirtualClock()
    config = make_config()
    world = HWorld(
        clock=clock,
        ds=FakeDatastore(),
        kv=FakeKeyValue(clock),
        objects=FakeObjectStore(),
        search=FakeSearch(),
        llm=FakeLLM(),
        fetcher=FakeFetcher(),
        discoverer=FakePortalDiscoverer(),
        sweep_queue=FakeQueue(clock),
        code_queue=FakeQueue(clock, visibility_timeout=900),
        fetch_queue=FakeQueue(clock),
        config=config,
    )
    world.ds.set_run_state(config.name, RunState.RUNNING)
    return world
