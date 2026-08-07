"""Contract tests for the fakes themselves (plan 1.4).

The scenario suite (4.1) proves the spec's failure model against these
semantics, so they must be honest: visibility windows, DLQ after 3
receives, SETNX once-only, unique violations.
"""

from datetime import UTC, datetime

import pytest
from harvest_core.domain import Source
from harvest_core.errors import UniqueViolation
from harvest_core.fakes import FakeDatastore, FakeKeyValue, FakeQueue, VirtualClock

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def test_received_message_invisible_until_deadline_then_redelivered() -> None:
    clock = VirtualClock(NOW)
    q = FakeQueue(clock, visibility_timeout=300)
    q.send("task-1")

    first = q.receive(10)
    assert [m.body for m in first] == ["task-1"]
    assert first[0].receive_count == 1

    # In flight: invisible to further receives before the deadline.
    assert q.receive(10) == []
    clock.advance(299)
    assert q.receive(10) == []

    # Past the deadline: redelivered with receive_count + 1.
    clock.advance(2)
    second = q.receive(10)
    assert [m.body for m in second] == ["task-1"]
    assert second[0].receive_count == 2


def test_message_dlqd_after_three_receives() -> None:
    clock = VirtualClock(NOW)
    q = FakeQueue(clock, visibility_timeout=300)
    q.send("poison")

    for expected_count in (1, 2, 3):
        (msg,) = q.receive(10)
        assert msg.receive_count == expected_count
        clock.advance(301)

    assert q.receive(10) == []
    assert [m.body for m in q.dlq] == ["poison"]
    assert q.pending_count() == 0


def test_deleted_message_never_redelivered() -> None:
    clock = VirtualClock(NOW)
    q = FakeQueue(clock, visibility_timeout=300)
    q.send("done")
    (msg,) = q.receive(10)
    q.delete(msg.id)
    clock.advance(1000)
    assert q.receive(10) == []


def test_change_visibility_extends_the_window() -> None:
    clock = VirtualClock(NOW)
    q = FakeQueue(clock, visibility_timeout=300)
    q.send("long-job")
    (msg,) = q.receive(10)
    clock.advance(250)
    q.change_visibility(msg.id, 900)
    clock.advance(300)  # original deadline long past; extended one holds
    assert q.receive(10) == []
    clock.advance(601)
    assert [m.body for m in q.receive(10)] == ["long-job"]


def test_setnx_returns_false_on_second_call() -> None:
    kv = FakeKeyValue(VirtualClock(NOW))
    assert kv.setnx("url:c:abc", "1") is True
    assert kv.setnx("url:c:abc", "1") is False


def test_kv_ttl_expiry_via_clock() -> None:
    clock = VirtualClock(NOW)
    kv = FakeKeyValue(clock)
    kv.hincrby("sweep:d1", {"done": 1})
    kv.expire("sweep:d1", 1800)
    clock.advance(1799)
    assert kv.hash_value("sweep:d1") == {"done": 1}
    clock.advance(2)
    assert kv.hash_value("sweep:d1") is None
    # a fresh increment starts from zero — the counter was genuinely lost
    assert kv.hincrby("sweep:d1", {"done": 1}) == {"done": 1}


def test_hincrby_returns_full_resulting_hash() -> None:
    kv = FakeKeyValue(VirtualClock(NOW))
    assert kv.hincrby("sweep:d", {"done": 1, "candidates": 3}) == {"done": 1, "candidates": 3}
    assert kv.hincrby("sweep:d", {"done": 1, "errors": 1}) == {
        "done": 2,
        "candidates": 3,
        "errors": 1,
    }


def test_kv_delete_prefix() -> None:
    kv = FakeKeyValue(VirtualClock(NOW))
    kv.setnx("url:corpus-a:h1", "1")
    kv.setnx("url:corpus-a:h2", "1")
    kv.setnx("url:corpus-b:h1", "1")
    assert kv.delete_prefix("url:corpus-a:") == 2
    assert kv.get("url:corpus-a:h1") is None
    assert kv.get("url:corpus-b:h1") == "1"


def test_duplicate_sweep_target_insert_raises() -> None:
    ds = FakeDatastore()
    j = ds.insert_jurisdiction("Pasadena", "CA", "city")
    ds.insert_target("c", j.id, Source.SERPER, 3, NOW)
    with pytest.raises(UniqueViolation):
        ds.insert_target("c", j.id, Source.SERPER, 3, NOW)
    # a different source for the same jurisdiction is fine
    ds.insert_target("c", j.id, Source.LEGAL_CODES, 3, NOW)


def test_duplicate_artifact_url_insert_raises() -> None:
    ds = FakeDatastore()
    j = ds.insert_jurisdiction("Pasadena", "CA", "city")
    ds.insert_artifact("c", j.id, "serper", "https://x.gov/a.pdf", "ctx", NOW)
    with pytest.raises(UniqueViolation):
        ds.insert_artifact("c", j.id, "serper", "https://x.gov/a.pdf", "ctx2", NOW)
    # same URL in another corpus is fine — dedupe is corpus-scoped
    ds.insert_artifact("c2", j.id, "serper", "https://x.gov/a.pdf", "ctx", NOW)


def test_history_insert_idempotent_on_dispatch_and_seq() -> None:
    from harvest_core.domain import SweepResult

    ds = FakeDatastore()
    j = ds.insert_jurisdiction("Pasadena", "CA", "city")
    args = dict(
        corpus="c",
        jurisdiction_id=j.id,
        source=Source.SERPER,
        dispatch_id="d1",
        query_seq=1,
        result=SweepResult.CANDIDATES,
        topic="noise",
        results_seen=10,
        results_triaged_relevant=4,
        candidates_staged=2,
        detail="q",
        swept_at=NOW,
    )
    assert ds.insert_history(**args) is True  # type: ignore[arg-type]
    assert ds.insert_history(**args) is False  # type: ignore[arg-type]
    assert len(ds.list_history("c")) == 1


def test_object_store_get_list_delete() -> None:
    from harvest_core.errors import ObjectNotFound
    from harvest_core.fakes import FakeObjectStore

    store = FakeObjectStore()
    store.put("camp/a/1_x.pdf", b"one", "application/pdf")
    store.put("camp/a/2_y.pdf", b"two", "application/pdf")
    store.put("other/z.pdf", b"three", "application/pdf")

    assert store.get("camp/a/1_x.pdf") == b"one"
    assert store.list_keys("camp/") == ["camp/a/1_x.pdf", "camp/a/2_y.pdf"]

    store.delete("camp/a/1_x.pdf")
    with pytest.raises(ObjectNotFound):
        store.get("camp/a/1_x.pdf")
    assert store.list_keys("camp/") == ["camp/a/2_y.pdf"]
    # deleting a missing key is a no-op, matching S3
    store.delete("camp/a/1_x.pdf")


def test_queue_pending_count_tracks_visible_and_in_flight() -> None:
    clock = VirtualClock()
    q = FakeQueue(clock, visibility_timeout=300)
    assert q.pending_count() == 0
    q.send("a")
    q.send("b")
    assert q.pending_count() == 2
    got = q.receive(1)  # in flight still counts
    assert q.pending_count() == 2
    q.delete(got[0].id)
    assert q.pending_count() == 1
