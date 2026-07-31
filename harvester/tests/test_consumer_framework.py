"""Plan 3.1: consumer framework — gates, redelivery, heartbeat."""

from harv_fixtures import HWorld
from harvest_core.domain import RunState
from harvest_core.fakes import FakeQueue, VirtualClock
from harvest_core.ports import QueueMessage
from harvest_harvester.consumer import ConsumerLoop, Heartbeat, gate_sweep


def test_rotated_dispatch_id_message_deleted_unworked(hworld: HWorld) -> None:
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    tasks = hworld.dispatch_sweep(target, [("noise", "q1")])

    # The orchestrator re-dispatched (timeout): dispatch_id rotates.
    refreshed = hworld.ds.get_target(target.id)
    assert refreshed is not None
    hworld.ds.stamp_target(
        target.id,
        hworld.clock.now(),
        "rotated-id",
        1,
        refreshed.dispatch_id,
        refreshed.dispatched_at,
    )

    worker = hworld.sweep_worker()
    messages = hworld.sweep_queue.receive(10)
    worker.handle_batch(messages)

    assert hworld.sweep_queue.pending_count() == 0  # deleted unworked
    assert hworld.search.calls == []  # no search spent
    assert hworld.ds.list_history(hworld.config.name) == []
    assert tasks[0].dispatch_id != "rotated-id"


def test_stopped_corpus_message_deleted_unworked(hworld: HWorld) -> None:
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    hworld.dispatch_sweep(target, [("noise", "q1")])
    hworld.ds.set_run_state(hworld.config.name, RunState.STOPPED)

    hworld.sweep_worker().handle_batch(hworld.sweep_queue.receive(10))
    assert hworld.sweep_queue.pending_count() == 0
    assert hworld.search.calls == []


def test_handler_raising_leaves_message_for_redelivery() -> None:
    clock = VirtualClock()
    queue = FakeQueue(clock, visibility_timeout=300)
    queue.send("boom")

    def exploding_handler(messages: list[QueueMessage]) -> None:
        raise RuntimeError("worker crashed mid-task")

    loop = ConsumerLoop(queue, exploding_handler, 10, clock)
    assert loop.run_once() == 1  # the exception is contained
    assert queue.pending_count() == 1  # nothing deleted

    clock.advance(301)
    redelivered = queue.receive(10)
    assert [m.body for m in redelivered] == ["boom"]
    assert redelivered[0].receive_count == 2


def test_heartbeat_extends_at_expected_clock_ticks() -> None:
    clock = VirtualClock()
    queue = FakeQueue(clock, visibility_timeout=900)
    queue.send("long-crawl")
    (msg,) = queue.receive(1)

    started = clock.now()
    hb = Heartbeat(
        queue,
        msg.id,
        clock,
        interval_seconds=300,
        extension_seconds=900,
        deadline_seconds=3600,
    )
    hb.maybe_beat()
    assert hb.beats == []  # too early

    clock.advance(300)
    hb.maybe_beat()
    clock.advance(150)
    hb.maybe_beat()  # between ticks: no beat
    clock.advance(150)
    hb.maybe_beat()
    assert [(b - started).total_seconds() for b in hb.beats] == [300.0, 600.0]

    # The message stays invisible through the extensions...
    assert queue.receive(1) == []
    # ...and past the hard deadline no further beats happen.
    clock.advance(3600)
    hb.maybe_beat()
    assert len(hb.beats) == 2
    assert hb.expired()


def test_gate_sweep_rejects_already_written_history(hworld: HWorld) -> None:
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    (task,) = hworld.dispatch_sweep(target, [("noise", "q1")])

    from harvest_core.domain import Source, SweepResult

    hworld.ds.insert_history(
        corpus=task.corpus,
        jurisdiction_id=task.jurisdiction_id,
        source=Source.SERPER,
        dispatch_id=task.dispatch_id,
        query_seq=task.query_seq,
        result=SweepResult.CANDIDATES,
        topic="noise",
        results_seen=1,
        results_triaged_relevant=1,
        candidates_staged=1,
        detail="",
        swept_at=hworld.clock.now(),
    )
    assert gate_sweep(hworld.ds, task) is False
