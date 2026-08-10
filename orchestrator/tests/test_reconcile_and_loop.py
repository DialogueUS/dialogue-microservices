"""Plan 2.5 (fetch reconciliation) and 2.6 (main loop)."""

from datetime import UTC, datetime, timedelta

from harvest_core.domain import RunState, Source
from harvest_core.messages import FetchTask, parse_task
from harvest_orchestrator.dispatch import _dispatch_serper, dispatch_cycle
from harvest_orchestrator.reconcile import reconcile_fetch
from orch_fixtures import World, make_config

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _city(world: World, name: str = "Pasadena") -> int:
    return world.ds.insert_jurisdiction(name, "CA", "city").id


# -- 2.5 reconciliation -----------------------------------------------------


def test_staged_never_published_artifact_republished_after_timeout(world: World) -> None:
    jur = _city(world)
    config = make_config()
    # Sweep worker crashed between insert and publish: no stamp at all.
    art = world.ds.insert_artifact(config.name, jur, "serper", "https://x.gov/a.pdf", "", NOW)

    assert reconcile_fetch(world.ds, world.fetch_queue, config, NOW) == 1
    (body,) = world.fetch_queue.bodies()
    task = parse_task(body)
    assert isinstance(task, FetchTask) and task.artifact_id == art.id
    stamped = world.ds.get_artifact(art.id)
    assert stamped is not None and stamped.dispatch_id == task.dispatch_id


def test_recently_stamped_artifacts_left_alone_then_recovered(world: World) -> None:
    jur = _city(world)
    config = make_config()
    art = world.ds.insert_artifact(config.name, jur, "serper", "https://x.gov/a.pdf", "", NOW)
    world.ds.stamp_artifact(art.id, NOW, "d-orig")

    # Inside the dispatch timeout: left alone.
    assert reconcile_fetch(world.ds, world.fetch_queue, config, NOW + timedelta(seconds=1799)) == 0
    # Stale (fetch message died somewhere): re-published with a new dispatch_id.
    assert reconcile_fetch(world.ds, world.fetch_queue, config, NOW + timedelta(seconds=1801)) == 1
    refreshed = world.ds.get_artifact(art.id)
    assert refreshed is not None and refreshed.dispatch_id != "d-orig"


def test_reconcile_cap_respected(world: World) -> None:
    jur = _city(world)
    config = make_config(max_fetch_redispatch=3)
    for i in range(5):
        world.ds.insert_artifact(
            config.name, jur, "serper", f"https://x.gov/{i}.pdf", "", NOW
        )
    assert reconcile_fetch(world.ds, world.fetch_queue, config, NOW) == 3
    assert len(world.fetch_queue.bodies()) == 3


# -- 2.6 main loop ----------------------------------------------------------


def test_stopped_config_dispatches_nothing(world: World) -> None:
    jur = _city(world)
    config = make_config()
    world.ds.insert_target(config.name, jur, Source.SERPER, 3, NOW)
    # No run switch row at all -> stopped.
    orch = world.orchestrator(config)
    assert orch.run_cycle() == {"dispatched": 0, "republished": 0, "seeded": 0}
    world.ds.set_run_state(config.name, RunState.STOPPED)
    assert orch.run_cycle() == {"dispatched": 0, "republished": 0, "seeded": 0}
    assert world.sweep_queue.bodies() == []


def test_flipping_switch_stops_new_dispatch_within_one_cycle(world: World) -> None:
    _city(world, "Alameda")
    _city(world, "Berkeley")
    config = make_config(max_sweeps_per_dispatch=1)
    world.start(config)
    orch = world.orchestrator(config)

    first = orch.run_cycle()
    assert first["dispatched"] == 1  # one of the two dispatched this cycle

    world.ds.set_run_state(config.name, RunState.STOPPED)
    world.clock.advance(60)
    second = orch.run_cycle()
    assert second["dispatched"] == 0  # the other one never goes out


def test_loop_seeds_targets_from_census_scope(world: World) -> None:
    from harvest_core.ports import CensusPlace, CensusState

    world.census.data["CA"] = CensusState(
        state_name="California",
        places=[
            CensusPlace(name="Los Angeles County", level="county"),
            CensusPlace(
                name="Pasadena", level="city", parent_name="Los Angeles County"
            ),
        ],
    )
    config = make_config()
    world.start(config)
    counters = world.orchestrator(config).run_cycle()
    assert counters["seeded"] == 1  # Pasadena serper row
    assert counters["dispatched"] == 1


def test_two_orchestrators_same_cycle_loser_hits_stamp_gate(world: World) -> None:
    jur = _city(world)
    config = make_config()
    world.start(config)
    world.ds.insert_target(config.name, jur, Source.SERPER, 3, NOW)

    # Interleave: both orchestrators read the same due snapshot before
    # either stamps (the racy schedule two processes can hit).
    from harvest_orchestrator.dispatch import DISPATCH_TIMEOUTS

    snapshot_a = world.ds.select_due(config.name, NOW, DISPATCH_TIMEOUTS, 25)
    snapshot_b = world.ds.select_due(config.name, NOW, DISPATCH_TIMEOUTS, 25)
    won_a = _dispatch_serper(
        world.ds, world.llm, world.sweep_queue, config, snapshot_a[0], NOW
    )
    won_b = _dispatch_serper(
        world.ds, world.llm, world.sweep_queue, config, snapshot_b[0], NOW
    )
    assert (won_a, won_b) == (True, False)  # loser stamped nothing, sent nothing

    target = world.ds.get_target(snapshot_a[0].id)
    assert target is not None and target.query_count is not None
    assert len(world.sweep_queue.bodies()) == target.query_count

    # Sequential cycles agree: a second full cycle dispatches nothing new.
    assert (
        dispatch_cycle(world.ds, world.llm, world.sweep_queue, world.code_queue, config, NOW)
        == 0
    )
