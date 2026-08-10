"""Census jurisdiction seeding into the harvester's Datastore (plan 2.1).

The port itself (`CensusSource`) and the census.gov client are generic
and live in `harvest_core`, because the public-records pipeline seeds
the same jurisdictions; only this Datastore-writing seeder is the
harvesting system's.

The state-level row is the loaded marker — a state with zero
incorporated places (Hawaii's counties-only file, say) must not be
reloaded every cycle. Comma-county rows are inserted (they hold
already-harvested documents) but excluded from scope resolution.
"""

from __future__ import annotations

from datetime import datetime

from harvest_core.constants import ALL_STATES, SEED_THROTTLE_S
from harvest_core.errors import UniqueViolation
from harvest_core.ports import CensusSource, Clock, Datastore


class CensusSeeder:
    def __init__(self, ds: Datastore, census: CensusSource, clock: Clock) -> None:
        self._ds = ds
        self._census = census
        self._clock = clock
        self._last_run: datetime | None = None

    def ensure_states(self, states: list[str]) -> bool:
        """Load any state whose state row is missing. Throttled to once
        per 600 s; returns False when throttled."""
        now = self._clock.now()
        if self._last_run is not None and (now - self._last_run).total_seconds() < SEED_THROTTLE_S:
            return False
        self._last_run = now
        wanted = ALL_STATES if states == ["ALL"] else states
        for state in wanted:
            if self._ds.state_row_exists(state):
                continue
            self._load_state(state)
        return True

    def _load_state(self, state: str) -> None:
        data = self._census.load_state(state)
        # County name -> id, for parent linkage of places.
        county_ids: dict[str, int] = {}
        for place in data.places:
            if place.level != "county":
                continue
            try:
                row = self._ds.insert_jurisdiction(
                    place.name, state, "county", fips=place.fips
                )
                county_ids[place.name] = row.id
            except UniqueViolation:
                pass
        for place in data.places:
            if place.level == "county":
                continue
            try:
                self._ds.insert_jurisdiction(
                    place.name,
                    state,
                    place.level,
                    fips=place.fips,
                    parent_id=county_ids.get(place.parent_name or ""),
                    parent_name=place.parent_name,
                )
            except UniqueViolation:
                pass
        # The state row last: its existence marks the state as loaded, so
        # it must not appear before the places are in.
        try:
            self._ds.insert_jurisdiction(data.state_name, state, "state")
        except UniqueViolation:
            pass


def ensure_federal_anchor(ds: Datastore) -> int:
    """Create the single federal / US / 'United States' row on demand."""
    anchor = ds.get_federal_anchor()
    if anchor is not None:
        return anchor.id
    try:
        return ds.insert_jurisdiction("United States", "US", "federal").id
    except UniqueViolation:
        existing = ds.get_federal_anchor()
        assert existing is not None
        return existing.id
