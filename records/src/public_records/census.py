"""Census jurisdiction seeding for the records store.

The jurisdictions table is unchanged from the old pipeline
(Census-seeded, §3.1); the CensusSource port and the census.gov client
are reused from the harvester orchestrator. The state-level row is the
loaded marker, so a state with zero places is not reloaded every cycle.
"""

from __future__ import annotations

from harvest_orchestrator.census import ALL_STATES, CensusSource

from .errors import UniqueViolation
from .ports import RecordsStore


def ensure_states(store: RecordsStore, census: CensusSource, states: list[str]) -> int:
    """Load any state whose state row is missing. Returns states loaded.
    Idempotent: concurrent inserts lose harmlessly on the unique key."""
    wanted = ALL_STATES if "ALL" in states else states
    loaded = 0
    for state in wanted:
        if store.state_row_exists(state):
            continue
        data = census.load_state(state)
        try:
            store.insert_jurisdiction(data.state_name, state, "state")
        except UniqueViolation:
            pass
        for place in data.places:
            try:
                store.insert_jurisdiction(place.name, state, place.level, place.parent_name)
            except UniqueViolation:
                pass
        loaded += 1
    return loaded
