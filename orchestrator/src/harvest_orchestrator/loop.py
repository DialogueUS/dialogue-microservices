"""Orchestrator main loop (plan 2.6, spec §5).

One process, one thread, one loop: run-switch read → seed (throttled)
→ dispatch → reconcile. Everything is idempotent; an accidental second
orchestrator is wasteful but safe.
"""

from __future__ import annotations

import logging
import threading

from harvest_core.config import HarvestConfig
from harvest_core.domain import RunState
from harvest_core.ports import Clock, Datastore, QueryGenerator, TaskQueue

from .census import CensusSeeder, CensusSource
from .dispatch import dispatch_cycle
from .reconcile import reconcile_fetch
from .seeding import seed_targets

log = logging.getLogger(__name__)


class Orchestrator:
    def __init__(
        self,
        config: HarvestConfig,
        ds: Datastore,
        clock: Clock,
        generator: QueryGenerator,
        census: CensusSource,
        sweep_queue: TaskQueue,
        code_queue: TaskQueue,
        fetch_queue: TaskQueue,
    ) -> None:
        self._config = config
        self._ds = ds
        self._clock = clock
        self._generator = generator
        self._seeder = CensusSeeder(ds, census, clock)
        self._sweep_queue = sweep_queue
        self._code_queue = code_queue
        self._fetch_queue = fetch_queue
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run_cycle(self) -> dict[str, int]:
        """One cycle; returns counters for observability/tests."""
        counters = {"dispatched": 0, "republished": 0, "seeded": 0}
        if self._ds.get_run_state(self._config.name) != RunState.RUNNING:
            return counters  # stopped (or no control row): nothing new dispatched

        if self._seeder.ensure_states(self._config.scope.states):
            counters["seeded"] = seed_targets(self._ds, self._config, self._clock.now())

        counters["dispatched"] = dispatch_cycle(
            self._ds,
            self._generator,
            self._sweep_queue,
            self._code_queue,
            self._config,
            self._clock.now(),
        )
        counters["republished"] = reconcile_fetch(
            self._ds, self._fetch_queue, self._config, self._clock.now()
        )
        return counters

    def run_forever(self, interval_seconds: float) -> None:
        while not self._stop.is_set():
            try:
                counters = self.run_cycle()
                if any(counters.values()):
                    log.info("cycle: %s", counters)
            except Exception:
                # Stateless over Postgres: log and try again next interval.
                log.exception("orchestrator cycle failed")
            self._clock.sleep(interval_seconds)
