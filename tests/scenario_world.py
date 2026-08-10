"""One shared fake world wiring BOTH services (plan 4.1).

The orchestrator and all three harvester workers run against the same
fakes and the same virtual clock, so every §7 failure-table row can be
scripted and asserted at exact time boundaries.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from harvest_core.config import HarvestConfig
from harvest_core.domain import RunState
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
from harvest_core.ports import CensusPlace, CensusState
from harvest_harvester.code import CodeWorker
from harvest_harvester.fetch import FetchWorker, HostThrottle
from harvest_harvester.sweep import SweepWorker
from harvest_orchestrator.loop import Orchestrator


class ScriptedCensus:
    def __init__(self) -> None:
        self.data: dict[str, CensusState] = {
            "CA": CensusState(
                state_name="California",
                places=[
                    CensusPlace(name="Los Angeles County", level="county"),
                    CensusPlace(
                        name="Pasadena", level="city", parent_name="Los Angeles County"
                    ),
                ],
            )
        }

    def load_state(self, state: str) -> CensusState:
        return self.data.get(state, CensusState(state_name=state, places=[]))


def scenario_config(**overrides: object) -> HarvestConfig:
    base: dict[str, object] = {
        "mode": "harvest",
        "name": "scenario-corpus",
        "scope": {"levels": ["city"], "states": ["CA"]},
        "topics": ["noise"],
        "queries_per_jurisdiction": 3,
    }
    base.update(overrides)
    return HarvestConfig.model_validate(base)


@dataclass
class ScenarioWorld:
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
    orchestrator: Orchestrator
    sweep_worker: SweepWorker
    code_worker: CodeWorker
    fetch_worker: FetchWorker

    def start(self) -> None:
        self.ds.set_run_state(self.config.name, RunState.RUNNING)

    def drain(self, max_rounds: int = 50) -> None:
        """Run harvester workers until every queue is quiet (in-flight
        messages excluded — the virtual clock does not advance here)."""
        for _ in range(max_rounds):
            worked = 0
            batch = self.sweep_queue.receive(10)
            if batch:
                self.sweep_worker.handle_batch(batch)
                worked += len(batch)
            for msg in self.code_queue.receive(1):
                self.code_worker.handle_one(msg)
                worked += 1
            for msg in self.fetch_queue.receive(10):
                self.fetch_worker.handle_one(msg)
                worked += 1
            if worked == 0:
                return
        raise AssertionError("queues never went quiet")


def build_world(config: HarvestConfig | None = None) -> ScenarioWorld:
    clock = VirtualClock()
    config = config or scenario_config()
    ds = FakeDatastore()
    kv = FakeKeyValue(clock)
    objects = FakeObjectStore()
    search = FakeSearch()
    llm = FakeLLM()
    fetcher = FakeFetcher()
    discoverer = FakePortalDiscoverer()
    sweep_queue = FakeQueue(clock)
    code_queue = FakeQueue(clock, visibility_timeout=900)
    fetch_queue = FakeQueue(clock)
    census = ScriptedCensus()

    orchestrator = Orchestrator(
        config=config,
        ds=ds,
        clock=clock,
        generator=llm,
        census=census,
        sweep_queue=sweep_queue,
        code_queue=code_queue,
        fetch_queue=fetch_queue,
    )
    sweep_worker = SweepWorker(
        ds, kv, search, llm, fetcher, sweep_queue, fetch_queue, clock, config
    )
    code_worker = CodeWorker(ds, kv, discoverer, code_queue, fetch_queue, clock, config)
    fetch_worker = FetchWorker(
        ds, kv, objects, fetcher, fetch_queue, clock, config, throttle=HostThrottle(clock)
    )
    return ScenarioWorld(
        clock=clock,
        ds=ds,
        kv=kv,
        objects=objects,
        search=search,
        llm=llm,
        fetcher=fetcher,
        discoverer=discoverer,
        sweep_queue=sweep_queue,
        code_queue=code_queue,
        fetch_queue=fetch_queue,
        config=config,
        orchestrator=orchestrator,
        sweep_worker=sweep_worker,
        code_worker=code_worker,
        fetch_worker=fetch_worker,
    )


@pytest.fixture()
def sworld() -> ScenarioWorld:
    return build_world()
