"""Plan 2.1: census seeding via the CensusSource port."""

from harvest_orchestrator.census import CensusPlace, CensusSeeder, CensusState
from harvest_orchestrator.seeding import resolve_scope
from orch_fixtures import World, make_config


def test_zero_place_state_not_reloaded_every_cycle(world: World) -> None:
    # Hawaii-like fixture: a state whose file yields no incorporated places.
    world.census.data["HI"] = CensusState(state_name="Hawaii", places=[])
    seeder = CensusSeeder(world.ds, world.census, world.clock)

    assert seeder.ensure_states(["HI"]) is True
    assert world.census.load_calls == ["HI"]
    # The state-level row is the loaded marker even with zero places.
    assert world.ds.state_row_exists("HI")

    world.clock.advance(601)  # past the seed throttle
    assert seeder.ensure_states(["HI"]) is True
    assert world.census.load_calls == ["HI"]  # not reloaded


def test_seed_throttled_to_600s(world: World) -> None:
    world.census.data["HI"] = CensusState(state_name="Hawaii", places=[])
    seeder = CensusSeeder(world.ds, world.census, world.clock)
    assert seeder.ensure_states(["HI"]) is True
    world.clock.advance(599)
    assert seeder.ensure_states(["HI"]) is False  # throttled: no work at all


def test_loader_idempotent_second_run_inserts_nothing(world: World) -> None:
    world.census.data["CA"] = CensusState(
        state_name="California",
        places=[
            CensusPlace(name="Los Angeles County", level="county"),
            CensusPlace(
                name="Pasadena", level="city", fips="55156", parent_name="Los Angeles County"
            ),
        ],
    )
    seeder = CensusSeeder(world.ds, world.census, world.clock)
    assert seeder.ensure_states(["CA"])
    count_after_first = len(world.ds.jurisdictions)

    # Force a second load attempt by wiping the loaded marker check path:
    # advance past throttle; state row exists so nothing loads again.
    world.clock.advance(601)
    assert seeder.ensure_states(["CA"])
    assert len(world.ds.jurisdictions) == count_after_first

    # Even a direct reload (marker missing scenario) inserts nothing new:
    seeder._load_state("CA")
    assert len(world.ds.jurisdictions) == count_after_first


def test_comma_county_rows_retained_but_excluded_from_scope(world: World) -> None:
    world.census.data["MT"] = CensusState(
        state_name="Montana",
        places=[
            CensusPlace(name="Gallatin County", level="county"),
            CensusPlace(name="Gallatin County, Park County", level="county"),
            CensusPlace(
                name="Three Forks", level="city", parent_name="Gallatin County"
            ),
        ],
    )
    CensusSeeder(world.ds, world.census, world.clock).ensure_states(["MT"])

    all_counties = world.ds.list_jurisdictions(states=["MT"], levels=["county"])
    assert {c.name for c in all_counties} == {
        "Gallatin County",
        "Gallatin County, Park County",
    }  # the comma row is retained as a document holder

    config = make_config(scope={"levels": ["county"], "states": ["MT"]})
    scoped = resolve_scope(world.ds, config)
    assert {j.name for j in scoped} == {"Gallatin County"}  # never swept
