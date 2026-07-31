"""Plan 3.3: fan-in — out-of-order completion, lost counters, aggregates."""

import random
from datetime import timedelta

from harv_fixtures import HWorld
from harvest_core.domain import SweepResult
from harvest_core.ports import SearchResult
from harvest_harvester.fanin import counter_key


def _canned(hworld: HWorld, query: str, n_results: int = 1) -> None:
    hworld.search.canned(
        query,
        [
            SearchResult(rank=i + 1, title="t", snippet="s", url=f"https://x.gov/{query}-{i}.pdf")
            for i in range(n_results)
        ],
    )


def test_three_query_dispatch_out_of_order_finalizes_exactly_once(hworld: HWorld) -> None:
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    hworld.dispatch_sweep(target, [("noise", "q1"), ("noise", "q2"), ("noise", "q3")])
    for q in ("q1", "q2", "q3"):
        _canned(hworld, q)

    worker = hworld.sweep_worker()
    messages = hworld.sweep_queue.receive(10)
    random.Random(7).shuffle(messages)

    finalized_states = []
    for msg in messages:  # one at a time, shuffled — out-of-order fan-in
        worker.handle_batch([msg])
        t = hworld.ds.get_target(target.id)
        assert t is not None
        finalized_states.append(t.dispatch_id is None)

    # Only the last completion finalized; the earlier two left it in flight.
    assert finalized_states == [False, False, True]
    final = hworld.ds.get_target(target.id)
    assert final is not None
    assert final.last_result == SweepResult.CANDIDATES
    assert final.next_due_at == hworld.clock.now() + timedelta(
        days=hworld.config.resweep_interval_days
    )


def test_partial_failure_one_error_two_candidates_full_resweep(hworld: HWorld) -> None:
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    hworld.dispatch_sweep(target, [("noise", "q1"), ("noise", "q2"), ("noise", "q3")])
    _canned(hworld, "q1")
    _canned(hworld, "q2")
    hworld.search.error_queries = {"q3"}  # a genuine (non-429) error

    hworld.sweep_worker().handle_batch(hworld.sweep_queue.receive(10))

    final = hworld.ds.get_target(target.id)
    assert final is not None
    assert final.last_result == SweepResult.CANDIDATES  # not blacked out
    assert final.next_due_at == hworld.clock.now() + timedelta(
        days=hworld.config.resweep_interval_days
    )
    results = {h.query_seq: h.result for h in hworld.ds.list_history(hworld.config.name)}
    assert list(results.values()).count(SweepResult.ERROR) == 1


def test_all_errored_zero_candidates_error_plus_one_day(hworld: HWorld) -> None:
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    hworld.dispatch_sweep(target, [("noise", "q1"), ("noise", "q2")])
    hworld.search.error_queries = {"q1", "q2"}

    hworld.sweep_worker().handle_batch(hworld.sweep_queue.receive(10))

    final = hworld.ds.get_target(target.id)
    assert final is not None
    assert final.last_result == SweepResult.ERROR
    assert final.next_due_at == hworld.clock.now() + timedelta(days=1)  # never a month


def test_lost_counter_never_finalizes_target_stays_stamped(hworld: HWorld) -> None:
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    tasks = hworld.dispatch_sweep(target, [("noise", "q1"), ("noise", "q2"), ("noise", "q3")])
    for q in ("q1", "q2", "q3"):
        _canned(hworld, q)

    worker = hworld.sweep_worker()
    messages = sorted(hworld.sweep_queue.receive(10), key=lambda m: m.body)
    worker.handle_batch([messages[0]])
    worker.handle_batch([messages[1]])

    hworld.kv.flush()  # ElastiCache failover mid-dispatch

    worker.handle_batch([messages[2]])
    # The fresh counter shows done=1 != 3: nobody finalizes...
    final = hworld.ds.get_target(target.id)
    assert final is not None
    assert final.dispatch_id == tasks[0].dispatch_id  # still stamped
    counts = hworld.kv.hash_value(counter_key(tasks[0].dispatch_id))
    assert counts is not None and counts["done"] == 1
    # ...and recovery belongs to the orchestrator dispatch timeout (2.4),
    # which re-selects this row once the stamp is 1800 s old.


def test_counter_ttl_is_dispatch_timeout(hworld: HWorld) -> None:
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    tasks = hworld.dispatch_sweep(target, [("noise", "q1"), ("noise", "q2")])
    _canned(hworld, "q1")
    _canned(hworld, "q2")

    worker = hworld.sweep_worker()
    messages = sorted(hworld.sweep_queue.receive(10), key=lambda m: m.body)
    worker.handle_batch([messages[0]])
    key = counter_key(tasks[0].dispatch_id)
    assert hworld.kv.hash_value(key) is not None
    hworld.clock.advance(1801)  # dispatch timeout passes
    assert hworld.kv.hash_value(key) is None  # counter expired with it
