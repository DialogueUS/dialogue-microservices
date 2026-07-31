from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from harvest_core.config import HarvestConfig
from harvest_core.domain import RunState
from harvest_core.fakes import (
    FakeDatastore,
    FakeKeyValue,
    FakeLLM,
    FakeObjectStore,
    FakeQueue,
    VirtualClock,
)
from harvest_orchestrator.census import CensusState
from harvest_orchestrator.loop import Orchestrator


class FixtureCensus:
    """CensusSource over an in-test dict of state -> CensusState."""

    def __init__(self, data: dict[str, CensusState]) -> None:
        self.data = data
        self.load_calls: list[str] = []

    def load_state(self, state: str) -> CensusState:
        self.load_calls.append(state)
        return self.data.get(state, CensusState(state_name=state, places=[]))


@dataclass
class World:
    clock: VirtualClock
    ds: FakeDatastore
    kv: FakeKeyValue
    objects: FakeObjectStore
    sweep_queue: FakeQueue
    code_queue: FakeQueue
    fetch_queue: FakeQueue
    llm: FakeLLM
    census: FixtureCensus
    configs: dict[str, HarvestConfig] = field(default_factory=dict)

    def orchestrator(self, config: HarvestConfig) -> Orchestrator:
        return Orchestrator(
            config=config,
            ds=self.ds,
            clock=self.clock,
            generator=self.llm,
            census=self.census,
            sweep_queue=self.sweep_queue,
            code_queue=self.code_queue,
            fetch_queue=self.fetch_queue,
        )

    def start(self, config: HarvestConfig) -> None:
        self.ds.set_run_state(config.name, RunState.RUNNING)


@pytest.fixture()
def world() -> World:
    clock = VirtualClock()
    return World(
        clock=clock,
        ds=FakeDatastore(),
        kv=FakeKeyValue(clock),
        objects=FakeObjectStore(),
        sweep_queue=FakeQueue(clock),
        code_queue=FakeQueue(clock, visibility_timeout=900),
        fetch_queue=FakeQueue(clock),
        llm=FakeLLM(),
        census=FixtureCensus({}),
    )


def make_config(**overrides: object) -> HarvestConfig:
    base: dict[str, object] = {
        "mode": "harvest",
        "name": "test-corpus",
        "scope": {"levels": ["city"], "states": ["CA"]},
        "topics": ["noise"],
    }
    base.update(overrides)
    return HarvestConfig.model_validate(base)
