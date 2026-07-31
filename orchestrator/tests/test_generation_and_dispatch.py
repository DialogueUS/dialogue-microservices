"""Plan 2.3 (query generation) and 2.4 (dispatch)."""

from datetime import UTC, datetime, timedelta

import pytest
from harvest_core.constants import QUERY_MAX_CHARS
from harvest_core.domain import Publisher, Source
from harvest_core.errors import GenerationError
from harvest_core.messages import CodeTask, SweepTask, parse_task
from harvest_orchestrator.dispatch import dispatch_cycle
from harvest_orchestrator.generate import generate_queries
from orch_fixtures import World, make_config

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _city(world: World, name: str) -> int:
    return world.ds.insert_jurisdiction(name, "CA", "city").id


# -- 2.3 generation ---------------------------------------------------------


def test_n_topics_one_llm_call(world: World) -> None:
    jur_id = _city(world, "Pasadena")
    jur = world.ds.get_jurisdiction(jur_id)
    assert jur is not None
    config = make_config(topics=["noise", "zoning", "water"])
    pairs = generate_queries(world.llm, jur, config)
    assert len(world.llm.generate_calls) == 1  # one call covers all topics
    assert {t for t, _ in pairs} == {"noise", "zoning", "water"}


def test_overlong_and_extra_queries_clamped(world: World) -> None:
    jur_id = _city(world, "Pasadena")
    jur = world.ds.get_jurisdiction(jur_id)
    assert jur is not None
    world.llm.generations["Pasadena"] = {
        "noise": ["x" * 500, "q2", "q3", "q4", "q5"],  # over-long + too many
    }
    config = make_config(topics=["noise"], queries_per_jurisdiction=3)
    pairs = generate_queries(world.llm, jur, config)
    assert len(pairs) == 3  # extra queries dropped
    assert all(len(q) <= QUERY_MAX_CHARS for _, q in pairs)


def test_generation_failure_leaves_row_due_others_dispatch(world: World) -> None:
    a = _city(world, "Alameda")
    b = _city(world, "Berkeley")
    config = make_config()
    world.ds.insert_target(config.name, a, Source.SERPER, 3, NOW)
    world.ds.insert_target(config.name, b, Source.SERPER, 3, NOW)
    world.llm.fail_generation_for = {"Alameda"}

    dispatched = dispatch_cycle(
        world.ds, world.llm, world.sweep_queue, world.code_queue, config, NOW
    )
    assert dispatched == 1
    # Berkeley's messages are out; Alameda's row is untouched and still due.
    sent = [parse_task(b_) for b_ in world.sweep_queue.bodies()]
    assert all(isinstance(t, SweepTask) and t.jurisdiction_id == b for t in sent)
    t_a = next(t for t in world.ds.list_targets(config.name) if t.jurisdiction_id == a)
    assert t_a.dispatched_at is None and t_a.dispatch_id is None
    assert t_a.next_due_at <= NOW


# -- 2.4 dispatch -----------------------------------------------------------


def test_federal_precedes_cities(world: World) -> None:
    fed = world.ds.insert_jurisdiction("United States", "US", "federal").id
    city = _city(world, "Pasadena")
    config = make_config(max_sweeps_per_dispatch=1)
    world.ds.insert_target(config.name, city, Source.SERPER, 3, NOW - timedelta(days=2))
    world.ds.insert_target(config.name, fed, Source.SERPER, 0, NOW - timedelta(days=1))

    dispatch_cycle(world.ds, world.llm, world.sweep_queue, world.code_queue, config, NOW)
    sent = [parse_task(b) for b in world.sweep_queue.bodies()]
    assert sent and all(
        isinstance(t, SweepTask) and t.jurisdiction_id == fed for t in sent
    )


def test_stamp_then_send_dead_queue_recovers_via_timeout(world: World) -> None:
    city = _city(world, "Pasadena")
    config = make_config()
    target = world.ds.insert_target(config.name, city, Source.SERPER, 3, NOW)

    def dead_send(body: str) -> None:
        raise ConnectionError("queue is down")

    world.sweep_queue.send_hook = dead_send
    assert (
        dispatch_cycle(world.ds, world.llm, world.sweep_queue, world.code_queue, config, NOW)
        == 0
    )
    stamped = world.ds.get_target(target.id)
    assert stamped is not None and stamped.dispatch_id is not None
    first_dispatch_id = stamped.dispatch_id
    assert world.sweep_queue.bodies() == []

    # Inside the window: never re-selected, stamp untouched.
    world.sweep_queue.send_hook = None
    later = NOW + timedelta(seconds=1799)
    assert (
        dispatch_cycle(world.ds, world.llm, world.sweep_queue, world.code_queue, config, later)
        == 0
    )
    still = world.ds.get_target(target.id)
    assert still is not None and still.dispatch_id == first_dispatch_id

    # Past the dispatch timeout: re-dispatched with a fresh dispatch_id.
    after = NOW + timedelta(seconds=1801)
    assert (
        dispatch_cycle(world.ds, world.llm, world.sweep_queue, world.code_queue, config, after)
        == 1
    )
    recovered = world.ds.get_target(target.id)
    assert recovered is not None
    assert recovered.dispatch_id is not None
    assert recovered.dispatch_id != first_dispatch_id
    sent = [parse_task(b) for b in world.sweep_queue.bodies()]
    assert {t.dispatch_id for t in sent} == {recovered.dispatch_id}


def test_code_path_stamps_query_count_one_and_sends_one_task(world: World) -> None:
    city = _city(world, "Pasadena")
    config = make_config()
    world.ds.insert_code_source(
        city, "https://library.municode.com/ca/pasadena", Publisher.MUNICODE, True, None, NOW
    )
    target = world.ds.insert_target(config.name, city, Source.LEGAL_CODES, 3, NOW)

    assert (
        dispatch_cycle(world.ds, world.llm, world.sweep_queue, world.code_queue, config, NOW)
        == 1
    )
    stamped = world.ds.get_target(target.id)
    assert stamped is not None and stamped.query_count == 1
    (body,) = world.code_queue.bodies()
    task = parse_task(body)
    assert isinstance(task, CodeTask)
    assert task.portal_urls == ["https://library.municode.com/ca/pasadena"]
    assert world.llm.generate_calls == []  # no LLM on the code path


@pytest.mark.parametrize("seconds_in", [0, 1799])
def test_stamped_row_inside_window_never_reselected(world: World, seconds_in: int) -> None:
    city = _city(world, "Pasadena")
    config = make_config()
    world.ds.insert_target(config.name, city, Source.SERPER, 3, NOW)
    dispatch_cycle(world.ds, world.llm, world.sweep_queue, world.code_queue, config, NOW)
    n_before = len(world.sweep_queue.bodies())

    later = NOW + timedelta(seconds=seconds_in)
    assert (
        dispatch_cycle(world.ds, world.llm, world.sweep_queue, world.code_queue, config, later)
        == 0
    )
    assert len(world.sweep_queue.bodies()) == n_before


def test_generation_error_type_is_generation_error() -> None:
    # generate_queries wraps empty topics as GenerationError, not KeyError.
    from harvest_core.domain import Jurisdiction
    from harvest_core.fakes import FakeLLM

    llm = FakeLLM()
    llm.generations["X"] = {}
    with pytest.raises(GenerationError):
        generate_queries(
            llm, Jurisdiction(id=1, name="X", state="CA", level="city"), make_config()
        )
