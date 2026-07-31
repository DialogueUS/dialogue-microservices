"""Plan 3.6: fetch worker — failure rules, dedupe, storage, throttle."""

import uuid

from harv_fixtures import HWorld
from harvest_core.domain import Artifact, ArtifactStatus
from harvest_core.messages import FetchTask, to_json
from harvest_core.ports import QueueMessage
from pdf_fixture import minimal_pdf


def _stage(hworld: HWorld, url: str, jur: int | None = None) -> tuple[Artifact, QueueMessage]:
    if jur is None:
        jur = hworld.add_city()
    artifact = hworld.ds.insert_artifact(
        hworld.config.name, jur, "serper", url, "ctx", hworld.clock.now()
    )
    dispatch_id = str(uuid.uuid4())
    hworld.ds.stamp_artifact(artifact.id, hworld.clock.now(), dispatch_id)
    hworld.fetch_queue.send(
        to_json(
            FetchTask(
                corpus=hworld.config.name,
                artifact_id=artifact.id,
                dispatch_id=dispatch_id,
                dispatched_at=hworld.clock.now(),
            )
        )
    )
    (msg,) = hworld.fetch_queue.receive(1)
    refreshed = hworld.ds.get_artifact(artifact.id)
    assert refreshed is not None
    return refreshed, msg


def test_happy_path_pdf_to_fetched_with_key_scheme(hworld: HWorld) -> None:
    url = "https://cityofpasadena.net/Ordinances/Noise Ordinance.pdf"
    pdf = minimal_pdf("Section 1. No loud noise after 10pm.")
    hworld.fetcher.fixture(url, pdf, content_type="application/pdf")
    artifact, msg = _stage(hworld, url)

    hworld.fetch_worker().handle_one(msg)

    final = hworld.ds.get_artifact(artifact.id)
    assert final is not None
    assert final.status == ArtifactStatus.FETCHED
    assert final.ext == ".pdf"
    assert final.sha256 is not None
    assert final.size_bytes == len(pdf)
    assert "loud noise" in (final.extracted_text or "")
    assert final.path == (
        f"test-corpus/pasadena/{final.sha256[:8]}_Noise_Ordinance.pdf"
    )
    assert hworld.objects.objects[final.path] == pdf
    assert hworld.fetch_queue.pending_count() == 0


def test_dead_link_statuses_delete_the_row(hworld: HWorld) -> None:
    jur = hworld.add_city()
    for status in (400, 401, 403, 404, 410):
        url = f"https://x.gov/dead-{status}.pdf"
        hworld.fetcher.fixture(url, b"", status=status)
        artifact, msg = _stage(hworld, url, jur)
        hworld.fetch_worker().handle_one(msg)
        assert hworld.ds.get_artifact(artifact.id) is None  # deleted outright
        assert hworld.fetch_queue.pending_count() == 0


def test_unreachable_host_deletes_the_row(hworld: HWorld) -> None:
    url = "https://gone.example/a.pdf"
    hworld.fetcher.unreachable.add(url)
    artifact, msg = _stage(hworld, url)
    hworld.fetch_worker().handle_one(msg)
    assert hworld.ds.get_artifact(artifact.id) is None


def test_three_transient_errors_reach_failed(hworld: HWorld) -> None:
    url = "https://flaky.gov/a.pdf"
    hworld.fetcher.transient_failures[url] = 99
    jur = hworld.add_city()
    worker = hworld.fetch_worker()

    artifact, msg = _stage(hworld, url, jur)
    for attempt in (1, 2, 3):
        worker.handle_one(msg)
        current = hworld.ds.get_artifact(artifact.id)
        assert current is not None
        assert current.attempts == attempt
        if attempt < 3:
            assert current.status == ArtifactStatus.PENDING
            assert current.dispatched_at is None  # reconcile picks it up
            # Simulate the orchestrator re-publishing after the timeout.
            dispatch_id = str(uuid.uuid4())
            hworld.ds.stamp_artifact(artifact.id, hworld.clock.now(), dispatch_id)
            hworld.fetch_queue.send(
                to_json(
                    FetchTask(
                        corpus=hworld.config.name,
                        artifact_id=artifact.id,
                        dispatch_id=dispatch_id,
                        dispatched_at=hworld.clock.now(),
                    )
                )
            )
            (msg,) = hworld.fetch_queue.receive(1)
    final = hworld.ds.get_artifact(artifact.id)
    assert final is not None
    assert final.status == ArtifactStatus.FAILED
    assert final.last_error is not None


def test_not_a_document_type_terminal(hworld: HWorld) -> None:
    url = "https://x.gov/page"
    hworld.fetcher.fixture(url, b"<html>a page</html>", content_type="text/html")
    artifact, msg = _stage(hworld, url)
    hworld.fetch_worker().handle_one(msg)
    final = hworld.ds.get_artifact(artifact.id)
    assert final is not None and final.status == ArtifactStatus.NOT_DOCUMENT


def test_same_bytes_two_urls_second_is_duplicate(hworld: HWorld) -> None:
    jur = hworld.add_city()
    pdf = minimal_pdf("identical bytes")
    url1 = "https://x.gov/a.pdf"
    url2 = "https://mirror.x.gov/a-copy.pdf"
    hworld.fetcher.fixture(url1, pdf, content_type="application/pdf")
    hworld.fetcher.fixture(url2, pdf, content_type="application/pdf")
    worker = hworld.fetch_worker()

    a1, m1 = _stage(hworld, url1, jur)
    worker.handle_one(m1)
    a2, m2 = _stage(hworld, url2, jur)
    worker.handle_one(m2)

    final1 = hworld.ds.get_artifact(a1.id)
    final2 = hworld.ds.get_artifact(a2.id)
    assert final1 is not None and final1.status == ArtifactStatus.FETCHED
    assert final2 is not None and final2.status == ArtifactStatus.DUPLICATE
    assert final2.sha256 == final1.sha256
    assert len(hworld.objects.objects) == 1  # bytes stored exactly once


def test_duplicate_caught_by_postgres_backstop_after_redis_flush(hworld: HWorld) -> None:
    jur = hworld.add_city()
    pdf = minimal_pdf("identical bytes again")
    url1 = "https://x.gov/b.pdf"
    url2 = "https://mirror.x.gov/b-copy.pdf"
    hworld.fetcher.fixture(url1, pdf, content_type="application/pdf")
    hworld.fetcher.fixture(url2, pdf, content_type="application/pdf")
    worker = hworld.fetch_worker()

    a1, m1 = _stage(hworld, url1, jur)
    worker.handle_one(m1)
    hworld.kv.flush()  # Redis says "never seen these bytes"
    a2, m2 = _stage(hworld, url2, jur)
    worker.handle_one(m2)

    final2 = hworld.ds.get_artifact(a2.id)
    assert final2 is not None and final2.status == ArtifactStatus.DUPLICATE
    # The backstop healed the cache.
    from harvest_harvester.fetch import doc_key

    assert final2.sha256 is not None
    assert hworld.kv.get(doc_key(hworld.config.name, final2.sha256)) == "1"


def test_token_bucket_five_urls_one_host_take_4s_virtual(hworld: HWorld) -> None:
    jur = hworld.add_city()
    worker = hworld.fetch_worker()
    started = hworld.clock.now()
    messages = []
    for i in range(5):
        url = f"https://smalltown.gov/doc-{i}.pdf"
        hworld.fetcher.fixture(url, minimal_pdf(f"doc {i}"), content_type="application/pdf")
        messages.append(_stage(hworld, url, jur)[1])
    for msg in messages:
        worker.handle_one(msg)

    elapsed = (hworld.clock.now() - started).total_seconds()
    assert elapsed >= 4.0  # >= 1 s spacing per host across the pool


def test_rotated_fetch_dispatch_id_message_dropped(hworld: HWorld) -> None:
    url = "https://x.gov/c.pdf"
    hworld.fetcher.fixture(url, minimal_pdf("doc"), content_type="application/pdf")
    artifact, msg = _stage(hworld, url)
    # Reconcile re-published with a fresh dispatch_id: the old message loses.
    hworld.ds.stamp_artifact(artifact.id, hworld.clock.now(), "fresh-id")
    hworld.fetch_worker().handle_one(msg)
    final = hworld.ds.get_artifact(artifact.id)
    assert final is not None and final.status == ArtifactStatus.PENDING  # unworked
    assert hworld.fetcher.calls == []
    assert hworld.fetch_queue.pending_count() == 0  # dropped
