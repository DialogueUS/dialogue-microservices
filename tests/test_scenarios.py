"""Plan 4.1: the §7 failure table, scripted end-to-end on fakes.

Each scenario asserts both the end state and the recovery budget in
virtual time.
"""

from datetime import timedelta

from harvest_core.domain import ArtifactStatus, RunState, Source, SweepResult
from harvest_core.ports import SearchResult
from scenario_world import ScenarioWorld

PDF_URL = "https://cityofpasadena.net/ordinances/noise.pdf"
PDF_BYTES = b"%PDF-1.4\nfake regulation body\n%%EOF"


def _arm_happy_path(sworld: ScenarioWorld) -> None:
    """Every generated query surfaces the same PDF; the PDF fetches."""
    sworld.search.default_results = [
        SearchResult(rank=1, title="Noise Ordinance", snippet="Ch. 9", url=PDF_URL)
    ]
    sworld.fetcher.fixture(PDF_URL, PDF_BYTES, content_type="application/pdf")


def _the_target(sworld: ScenarioWorld):  # type: ignore[no-untyped-def]
    (target,) = sworld.ds.list_targets(sworld.config.name, Source.SERPER)
    return target


def test_happy_path_seed_dispatch_sweep_fetch_to_fetched(sworld: ScenarioWorld) -> None:
    _arm_happy_path(sworld)
    sworld.start()

    counters = sworld.orchestrator.run_cycle()
    assert counters["seeded"] == 1 and counters["dispatched"] == 1
    sworld.drain()

    # One artifact, fetched, bytes in the store under the key scheme.
    (artifact,) = sworld.ds.artifacts.values()
    assert artifact.status == ArtifactStatus.FETCHED
    assert artifact.source_url == PDF_URL
    assert artifact.path is not None and artifact.path in sworld.objects.objects
    assert artifact.path.startswith("scenario-corpus/pasadena/")

    # The target finalized promptly (fan-in, not timeout) with candidates.
    target = _the_target(sworld)
    assert target.dispatch_id is None
    assert target.last_result == SweepResult.CANDIDATES
    assert target.next_due_at == sworld.clock.now() + timedelta(
        days=sworld.config.resweep_interval_days
    )
    # One history row per query, query text recorded in detail.
    history = sworld.ds.list_history(sworld.config.name)
    assert len(history) == 3
    assert all("query" in h.detail for h in history)
    # All queues fully drained.
    assert sworld.sweep_queue.pending_count() == 0
    assert sworld.fetch_queue.pending_count() == 0


def test_worker_crash_mid_sweep_redelivery_completes_exactly_once(
    sworld: ScenarioWorld,
) -> None:
    _arm_happy_path(sworld)
    sworld.start()
    sworld.orchestrator.run_cycle()

    # Crash between history commit and message delete on the first query:
    # the deletes stop working for one batch.
    real_delete = sworld.sweep_queue.delete
    crashed = {"n": 0}

    def crashing_delete(message_id: str) -> None:
        crashed["n"] += 1
        raise ConnectionError("process died before delete")

    messages = sworld.sweep_queue.receive(10)
    sworld.sweep_queue.delete = crashing_delete  # type: ignore[method-assign]
    try:
        sworld.sweep_worker.handle_batch(messages[:1])
    except ConnectionError:
        pass
    finally:
        sworld.sweep_queue.delete = real_delete  # type: ignore[method-assign]

    assert crashed["n"] == 1
    assert len(sworld.ds.list_history(sworld.config.name)) == 1  # committed
    assert sworld.sweep_queue.pending_count() == 3  # nothing deleted

    # Visibility timeout passes; redelivery must not double-write.
    sworld.clock.advance(301)
    sworld.drain()

    history = sworld.ds.list_history(sworld.config.name)
    assert len(history) == 3  # the crashed query has exactly one row
    assert len({(h.dispatch_id, h.query_seq) for h in history}) == 3
    target = _the_target(sworld)
    assert target.last_result == SweepResult.CANDIDATES
    (artifact,) = sworld.ds.artifacts.values()
    assert artifact.status == ArtifactStatus.FETCHED


def test_lost_messages_redispatched_with_regenerated_queries(
    sworld: ScenarioWorld,
) -> None:
    _arm_happy_path(sworld)
    sworld.start()
    started = sworld.clock.now()
    sworld.orchestrator.run_cycle()
    first_generation_calls = len(sworld.llm.generate_calls)
    first_dispatch_id = _the_target(sworld).dispatch_id
    assert first_dispatch_id is not None

    sworld.sweep_queue.drop_all()  # the whole message set dies (DLQ'd, say)

    # Cycles inside the dispatch timeout do nothing for this row.
    sworld.clock.advance(60)
    assert sworld.orchestrator.run_cycle()["dispatched"] == 0

    # Past the dispatch timeout: stamp cleared, queries regenerated fresh.
    sworld.clock.advance(1800)
    assert sworld.orchestrator.run_cycle()["dispatched"] == 1
    assert len(sworld.llm.generate_calls) == first_generation_calls + 1
    second_dispatch_id = _the_target(sworld).dispatch_id
    assert second_dispatch_id is not None and second_dispatch_id != first_dispatch_id

    sworld.drain()
    target = _the_target(sworld)
    assert target.last_result == SweepResult.CANDIDATES
    # Recovery budget: within 1800 s + one cycle interval of virtual time.
    assert (sworld.clock.now() - started).total_seconds() <= 1800 + 60


def test_redis_flush_mid_flight_correctness_via_constraints_and_timeout(
    sworld: ScenarioWorld,
) -> None:
    _arm_happy_path(sworld)
    sworld.start()
    started = sworld.clock.now()
    sworld.orchestrator.run_cycle()

    # Work 2 of 3 queries, then ElastiCache loses everything.
    messages = sorted(sworld.sweep_queue.receive(10), key=lambda m: m.body)
    sworld.sweep_worker.handle_batch(messages[:2])
    sworld.kv.flush()
    sworld.sweep_worker.handle_batch(messages[2:])

    # Counter was lost: nobody finalized; the row is still stamped.
    assert _the_target(sworld).dispatch_id is not None

    # The dispatch timeout re-dispatches; URL dedupe (now via the unique
    # constraint, since Redis forgot) keeps the repeat cheap.
    sworld.clock.advance(1801)
    assert sworld.orchestrator.run_cycle()["dispatched"] == 1
    sworld.drain()

    target = _the_target(sworld)
    assert target.dispatch_id is None
    assert target.last_result in (SweepResult.CANDIDATES, SweepResult.NOT_FOUND)
    # The PDF was staged exactly once across both dispatches.
    assert len(sworld.ds.artifacts) == 1
    (artifact,) = sworld.ds.artifacts.values()
    assert artifact.status == ArtifactStatus.FETCHED
    assert len(sworld.objects.objects) == 1
    assert (sworld.clock.now() - started).total_seconds() <= 1800 + 60


def test_double_dispatch_loser_dropped(sworld: ScenarioWorld) -> None:
    _arm_happy_path(sworld)
    sworld.start()
    sworld.orchestrator.run_cycle()
    first_dispatch_id = _the_target(sworld).dispatch_id

    # The first message set is delayed (in flight, invisible) while the
    # dispatch timeout passes and a second set is dispatched.
    stuck = sworld.sweep_queue.receive(10)  # in flight, never worked
    assert len(stuck) == 3
    sworld.clock.advance(1801)
    assert sworld.orchestrator.run_cycle()["dispatched"] == 1
    second_dispatch_id = _the_target(sworld).dispatch_id
    assert second_dispatch_id != first_dispatch_id

    # Both sets are now deliverable (the first re-appears after its
    # visibility lapsed at receive-time +300 s, long past).
    sworld.drain()

    # Single effective execution: only the valid dispatch_id wrote history.
    history = sworld.ds.list_history(sworld.config.name)
    assert {h.dispatch_id for h in history} == {second_dispatch_id}
    assert len(history) == 3
    assert sworld.sweep_queue.pending_count() == 0  # losers deleted unworked
    assert len(sworld.ds.artifacts) == 1


def test_stop_switch_drains_without_work(sworld: ScenarioWorld) -> None:
    _arm_happy_path(sworld)
    sworld.start()
    sworld.orchestrator.run_cycle()
    assert sworld.sweep_queue.pending_count() == 3

    sworld.ds.set_run_state(sworld.config.name, RunState.STOPPED)

    # Nothing new dispatches, and in-flight tasks drain without work.
    sworld.clock.advance(60)
    assert sworld.orchestrator.run_cycle()["dispatched"] == 0
    sworld.drain()
    assert sworld.sweep_queue.pending_count() == 0  # drained
    assert sworld.search.calls == []  # without work
    assert sworld.ds.list_history(sworld.config.name) == []
    assert sworld.ds.artifacts == {}


def test_serper_429_storm_never_blacks_out_jurisdiction(sworld: ScenarioWorld) -> None:
    _arm_happy_path(sworld)
    sworld.start()
    sworld.orchestrator.run_cycle()
    original_due = None

    # A storm: every search answers 429 across several redelivery rounds.
    sworld.search.rate_limit_all = True
    for _ in range(2):
        batch = sworld.sweep_queue.receive(10)
        sworld.sweep_worker.handle_batch(batch)
        # History stays clean and nothing was recorded against the row.
        assert sworld.ds.list_history(sworld.config.name) == []
        target = _the_target(sworld)
        assert target.last_result is None
        original_due = target.next_due_at
        sworld.clock.advance(301)

    # The storm passes; the same messages redeliver and complete.
    sworld.search.rate_limit_all = False
    sworld.drain()

    target = _the_target(sworld)
    assert target.last_result == SweepResult.CANDIDATES  # never ERROR
    assert target.next_due_at != original_due
    history = sworld.ds.list_history(sworld.config.name)
    assert len(history) == 3
    assert all(h.result != SweepResult.ERROR for h in history)
    (artifact,) = sworld.ds.artifacts.values()
    assert artifact.status == ArtifactStatus.FETCHED


def test_sweep_worker_crash_after_staging_before_publish_reconciled(
    sworld: ScenarioWorld,
) -> None:
    """§7 row: artifacts staged, fetch tasks never published →
    orchestrator re-publishes within the dispatch timeout."""
    _arm_happy_path(sworld)
    sworld.start()
    started = sworld.clock.now()
    sworld.orchestrator.run_cycle()

    # The fetch-task publish dies (queue down) but staging commits.
    def dead_send(body: str) -> None:
        raise ConnectionError("fetch queue down")

    sworld.fetch_queue.send_hook = dead_send
    try:
        sworld.sweep_worker.handle_batch(sworld.sweep_queue.receive(10))
    except ConnectionError:
        pass
    sworld.fetch_queue.send_hook = None

    pending = [
        a for a in sworld.ds.artifacts.values() if a.status == ArtifactStatus.PENDING
    ]
    assert pending  # staged but unpublished
    assert sworld.fetch_queue.pending_count() == 0

    # Reconciliation re-publishes once the artifact stamp goes stale.
    sworld.clock.advance(1801)
    counters = sworld.orchestrator.run_cycle()
    assert counters["republished"] >= 1
    sworld.drain()
    (artifact,) = sworld.ds.artifacts.values()
    assert artifact.status == ArtifactStatus.FETCHED
    assert (sworld.clock.now() - started).total_seconds() <= 1800 + 60 + 301
