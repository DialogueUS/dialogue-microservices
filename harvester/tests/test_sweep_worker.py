"""Plan 3.2: sweep worker — rate-limit invariant, triage, staging."""

import threading

from harv_fixtures import HWorld
from harvest_core.domain import ArtifactStatus, SweepResult
from harvest_core.ports import SearchResult, TriageVerdict
from harvest_harvester.fanin import counter_key
from harvest_harvester.staging import Candidate, stage_candidate


def _result(url: str, rank: int = 1) -> SearchResult:
    return SearchResult(rank=rank, title=f"t{rank}", snippet=f"s{rank}", url=url)


def test_rate_limit_invariant_and_same_query_redelivered(hworld: HWorld) -> None:
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    (task,) = hworld.dispatch_sweep(target, [("noise", "pasadena noise ordinance")])
    hworld.search.rate_limited_queries = {"pasadena noise ordinance"}

    worker = hworld.sweep_worker()
    worker.handle_batch(hworld.sweep_queue.receive(10))

    # The invariant: no history row, no counter increment, message kept.
    assert hworld.ds.list_history(hworld.config.name) == []
    assert hworld.kv.hash_value(counter_key(task.dispatch_id)) is None
    assert hworld.sweep_queue.pending_count() == 1

    # After the storm: redelivery carries the *same* query text.
    hworld.search.rate_limited_queries = set()
    hworld.search.canned(
        "pasadena noise ordinance", [_result("https://cityofpasadena.net/noise.pdf")]
    )
    hworld.clock.advance(301)
    redelivered = hworld.sweep_queue.receive(10)
    assert len(redelivered) == 1
    worker.handle_batch(redelivered)

    (history,) = hworld.ds.list_history(hworld.config.name)
    assert "pasadena noise ordinance" in history.detail
    assert history.result == SweepResult.CANDIDATES
    assert hworld.sweep_queue.pending_count() == 0


def test_429_only_stalls_that_message_batch_continues(hworld: HWorld) -> None:
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    hworld.dispatch_sweep(target, [("noise", "limited-q"), ("noise", "fine-q")])
    hworld.search.rate_limited_queries = {"limited-q"}
    hworld.search.canned("fine-q", [_result("https://cityofpasadena.net/a.pdf")])

    hworld.sweep_worker().handle_batch(hworld.sweep_queue.receive(10))

    assert hworld.sweep_queue.pending_count() == 1  # only the limited one
    (history,) = hworld.ds.list_history(hworld.config.name)
    assert history.result == SweepResult.CANDIDATES
    counts = hworld.kv.hash_value(counter_key(history.dispatch_id))
    assert counts is not None and counts["done"] == 1


def test_triage_error_leaves_messages_nothing_recorded(hworld: HWorld) -> None:
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    hworld.dispatch_sweep(target, [("noise", "q1")])
    hworld.search.canned("q1", [_result("https://cityofpasadena.net/a.pdf")])
    hworld.llm.fail_triage = True

    hworld.sweep_worker().handle_batch(hworld.sweep_queue.receive(10))

    assert hworld.sweep_queue.pending_count() == 1  # left for redelivery
    assert hworld.ds.list_history(hworld.config.name) == []
    assert len(hworld.ds.artifacts) == 0  # never fetch unfiltered


def test_triage_rejected_results_counted_but_not_staged(hworld: HWorld) -> None:
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    hworld.dispatch_sweep(target, [("noise", "q1")])
    hworld.search.canned(
        "q1",
        [
            _result("https://cityofpasadena.net/noise.pdf", 1),
            _result("https://spam.example/casino.pdf", 2),
        ],
    )

    def rule(req, i):  # type: ignore[no-untyped-def]
        relevant = "spam" not in req.results[i].url
        return TriageVerdict(relevant=relevant, is_document=True, confidence=0.9)

    hworld.llm.triage_rule = rule
    hworld.sweep_worker().handle_batch(hworld.sweep_queue.receive(10))

    (history,) = hworld.ds.list_history(hworld.config.name)
    assert history.results_seen == 2
    assert history.results_triaged_relevant == 1
    assert history.candidates_staged == 1
    urls = [a.source_url for a in hworld.ds.artifacts.values()]
    assert urls == ["https://cityofpasadena.net/noise.pdf"]


def test_entity_unescape_case_from_old_spec(hworld: HWorld) -> None:
    """The observed `A %20&amp;%20B.pdf` case: the href entity must be
    unescaped or the fetch 404s."""
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    hworld.dispatch_sweep(target, [("noise", "q1")])
    page_url = "https://cityofpasadena.net/ordinances"
    hworld.search.canned("q1", [_result(page_url)])

    def rule(req, i):  # type: ignore[no-untyped-def]
        return TriageVerdict(relevant=True, is_document=False, confidence=0.8)

    hworld.llm.triage_rule = rule
    hworld.fetcher.fixture(
        page_url,
        b'<html><body><a href="A %20&amp;%20B.pdf">Nuisance rules</a></body></html>',
        content_type="text/html",
    )
    hworld.sweep_worker().handle_batch(hworld.sweep_queue.receive(10))

    urls = [a.source_url for a in hworld.ds.artifacts.values()]
    assert urls == ["https://cityofpasadena.net/A %20&%20B.pdf"]
    (artifact,) = hworld.ds.artifacts.values()
    assert "linked from https://cityofpasadena.net/ordinances" in artifact.context
    assert "Nuisance rules" in artifact.context


def test_duplicate_url_staged_once_across_concurrent_workers(hworld: HWorld) -> None:
    jur = hworld.add_city()
    url = "https://cityofpasadena.net/noise.pdf"
    results = []

    def stage() -> None:
        results.append(
            stage_candidate(
                hworld.ds,
                hworld.kv,
                hworld.fetch_queue,
                hworld.clock,
                hworld.config.name,
                jur,
                "serper",
                Candidate(url=url, context="ctx"),
            )
        )

    threads = [threading.Thread(target=stage) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(results) == [False, True]  # exactly one winner
    assert len(hworld.ds.artifacts) == 1
    assert len(hworld.fetch_queue.bodies()) == 1


def test_redis_false_negative_heals_via_constraint(hworld: HWorld) -> None:
    jur = hworld.add_city()
    url = "https://cityofpasadena.net/noise.pdf"
    candidate = Candidate(url=url, context="ctx")
    args = (hworld.ds, hworld.kv, hworld.fetch_queue, hworld.clock)
    assert stage_candidate(*args, hworld.config.name, jur, "serper", candidate) is True

    hworld.kv.flush()  # ElastiCache failover loses the seen-URL key
    assert stage_candidate(*args, hworld.config.name, jur, "serper", candidate) is False
    assert len(hworld.ds.artifacts) == 1  # constraint held
    from harvest_harvester.staging import url_key

    assert hworld.kv.get(url_key(hworld.config.name, url)) == "1"  # cache healed


def test_redelivered_completed_message_writes_nothing_twice(hworld: HWorld) -> None:
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    tasks = hworld.dispatch_sweep(target, [("noise", "q1"), ("noise", "q2")])
    hworld.search.canned("q1", [_result("https://cityofpasadena.net/a.pdf")])

    worker = hworld.sweep_worker()
    # Work q1 only (crash before q2's message is ever received).
    first_batch = [
        m for m in hworld.sweep_queue.receive(10) if '"query_seq":0' in m.body
    ]
    worker.handle_batch(first_batch)
    assert len(hworld.ds.list_history(hworld.config.name)) == 1
    counts = hworld.kv.hash_value(counter_key(tasks[0].dispatch_id))
    assert counts is not None and counts["done"] == 1

    # Simulate a redelivery of the already-completed q1 (crash landed
    # between commit and delete in a parallel worker's history).
    hworld.sweep_queue.send(first_batch[0].body)
    worker.handle_batch(hworld.sweep_queue.receive(10))

    assert len(hworld.ds.list_history(hworld.config.name)) == 1  # no second row
    counts = hworld.kv.hash_value(counter_key(tasks[0].dispatch_id))
    assert counts is not None and counts["done"] == 1  # no re-increment
    assert len(hworld.ds.artifacts) == 1  # URL dedupe held


def test_direct_pdf_hits_precede_linked_documents(hworld: HWorld) -> None:
    jur = hworld.add_city()
    target = hworld.add_target(jur)
    hworld.dispatch_sweep(target, [("noise", "q1")])
    page = "https://cityofpasadena.net/laws"
    hworld.search.canned(
        "q1",
        [
            _result(page, 1),
            _result("https://cityofpasadena.net/direct.docx", 2),
            _result("https://cityofpasadena.net/direct.pdf", 3),
        ],
    )

    def rule(req, i):  # type: ignore[no-untyped-def]
        is_doc = req.results[i].url != page
        return TriageVerdict(relevant=True, is_document=is_doc, confidence=0.9)

    hworld.llm.triage_rule = rule
    hworld.fetcher.fixture(
        page,
        b'<a href="/linked.xlsx">x</a><a href="/linked.pdf">p</a>'
        b'<a href="/feed.xml">feed</a><a href="/api.json">api</a>',
        content_type="text/html",
    )
    hworld.sweep_worker().handle_batch(hworld.sweep_queue.receive(10))

    urls = [a.source_url for a in sorted(hworld.ds.artifacts.values(), key=lambda a: a.id)]
    # Direct hits first with the PDF floated, then linked docs (PDF first);
    # .xml/.json links are never followed off a page.
    assert urls == [
        "https://cityofpasadena.net/direct.pdf",
        "https://cityofpasadena.net/direct.docx",
        "https://cityofpasadena.net/linked.pdf",
        "https://cityofpasadena.net/linked.xlsx",
    ]
    assert all(a.status == ArtifactStatus.PENDING for a in hworld.ds.artifacts.values())
    assert len(hworld.fetch_queue.bodies()) == 4
