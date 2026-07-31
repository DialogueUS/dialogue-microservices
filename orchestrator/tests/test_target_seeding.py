"""Plan 2.2: scope resolution + sweep-target seeding."""

from datetime import UTC, datetime, timedelta

from harvest_core.domain import Publisher, Source, SweepResult
from harvest_orchestrator.seeding import seed_targets
from orch_fixtures import World, make_config

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _seed_city(world: World, name: str = "Pasadena") -> int:
    return world.ds.insert_jurisdiction(name, "CA", "city").id


def test_serper_rows_for_all_in_scope_legal_codes_only_with_seed(world: World) -> None:
    a = _seed_city(world, "Pasadena")
    b = _seed_city(world, "Berkeley")
    world.ds.insert_code_source(
        a, "https://library.municode.com/ca/pasadena", Publisher.MUNICODE, True, None, NOW
    )
    config = make_config()
    seed_targets(world.ds, config, NOW)

    targets = {(t.jurisdiction_id, t.source) for t in world.ds.list_targets(config.name)}
    assert targets == {
        (a, Source.SERPER),
        (a, Source.LEGAL_CODES),
        (b, Source.SERPER),
    }


def test_jurisdiction_gaining_code_source_gets_row_next_pass(world: World) -> None:
    a = _seed_city(world)
    config = make_config()
    seed_targets(world.ds, config, NOW)
    assert len(world.ds.list_targets(config.name)) == 1  # serper only

    world.ds.insert_code_source(
        a, "https://library.municode.com/ca/pasadena", Publisher.MUNICODE, True, None, NOW
    )
    seed_targets(world.ds, config, NOW)
    sources = {t.source for t in world.ds.list_targets(config.name)}
    assert sources == {Source.SERPER, Source.LEGAL_CODES}


def test_disabling_seed_parks_legal_codes_row(world: World) -> None:
    a = _seed_city(world)
    cs = world.ds.insert_code_source(
        a, "https://library.municode.com/ca/pasadena", Publisher.MUNICODE, True, None, NOW
    )
    config = make_config()
    seed_targets(world.ds, config, NOW)

    world.ds.set_code_source_enabled(cs.id, False)
    seed_targets(world.ds, config, NOW)

    (lc,) = [
        t for t in world.ds.list_targets(config.name) if t.source == Source.LEGAL_CODES
    ]
    assert lc.last_result == SweepResult.ERROR
    assert lc.next_due_at == NOW + timedelta(days=config.resweep_interval_days)
    # Parked, not deleted; the serper row is untouched.
    (sp,) = [t for t in world.ds.list_targets(config.name) if t.source == Source.SERPER]
    assert sp.next_due_at <= NOW


def test_unique_race_loses_harmlessly_and_pass_continues(world: World) -> None:
    _seed_city(world, "Pasadena")
    _seed_city(world, "Berkeley")
    config = make_config()

    # Simulate a concurrent orchestrator inserting Pasadena's serper row
    # between our existence check and our insert: pre-insert directly.
    pas = next(j for j in world.ds.list_jurisdictions() if j.name == "Pasadena")
    real_insert = world.ds.insert_target
    raced = {"done": False}

    def racing_insert(corpus, jurisdiction_id, source, priority, next_due_at):  # type: ignore[no-untyped-def]
        if jurisdiction_id == pas.id and not raced["done"]:
            raced["done"] = True
            real_insert(corpus, jurisdiction_id, source, priority, next_due_at)
        return real_insert(corpus, jurisdiction_id, source, priority, next_due_at)

    world.ds.insert_target = racing_insert  # type: ignore[method-assign]
    try:
        seed_targets(world.ds, config, NOW)
    finally:
        world.ds.insert_target = real_insert  # type: ignore[method-assign]

    # Both jurisdictions ended with exactly one serper row each.
    targets = world.ds.list_targets(config.name)
    assert sorted(t.jurisdiction_id for t in targets) == sorted(
        [pas.id, next(j.id for j in world.ds.list_jurisdictions() if j.name == "Berkeley")]
    )


def test_due_date_backfill_from_history(world: World) -> None:
    a = _seed_city(world)
    config = make_config()
    # Prior non-error sweep exists: the new row must not be due immediately.
    world.ds.insert_history(
        corpus=config.name,
        jurisdiction_id=a,
        source=Source.SERPER,
        dispatch_id="old",
        query_seq=0,
        result=SweepResult.CANDIDATES,
        topic="noise",
        results_seen=1,
        results_triaged_relevant=1,
        candidates_staged=1,
        detail="",
        swept_at=NOW - timedelta(days=10),
    )
    seed_targets(world.ds, config, NOW)
    (t,) = world.ds.list_targets(config.name)
    assert t.next_due_at == NOW - timedelta(days=10) + timedelta(
        days=config.resweep_interval_days
    )
