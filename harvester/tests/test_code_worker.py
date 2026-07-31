"""Plan 3.4: code worker — write-back table, deadline, transients."""

from datetime import timedelta

from harv_fixtures import HWorld
from harvest_core.domain import Publisher, Source, SweepResult
from harvest_core.ports import PortalCandidate

PORTAL = "https://library.municode.com/ca/pasadena"


def _setup(hworld: HWorld) -> tuple[int, object, object]:
    jur = hworld.add_city()
    hworld.add_code_source(jur, PORTAL, Publisher.MUNICODE)
    serper_target = hworld.add_target(jur, Source.SERPER)
    code_target = hworld.add_target(jur, Source.LEGAL_CODES)
    return jur, serper_target, code_target


def test_completion_finalizes_legal_codes_without_touching_serper(hworld: HWorld) -> None:
    jur, serper_target, code_target = _setup(hworld)
    hworld.discoverer.candidates[PORTAL] = [
        PortalCandidate("https://library.municode.com/ca/pasadena/export/ch14.pdf",
                        "Chapter 14 — Nuisances"),
        PortalCandidate("https://api.municode.com/content/12345", "content API"),
    ]
    hworld.dispatch_code(code_target, [PORTAL])  # type: ignore[arg-type]

    hworld.code_worker().handle_batch(hworld.code_queue.receive(1))

    final = hworld.ds.get_target(code_target.id)  # type: ignore[attr-defined]
    assert final is not None
    assert final.last_result == SweepResult.CANDIDATES
    assert final.dispatch_id is None
    assert final.next_due_at == hworld.clock.now() + timedelta(
        days=hworld.config.resweep_interval_days
    )
    # Sibling serper row untouched.
    sibling = hworld.ds.get_target(serper_target.id)  # type: ignore[attr-defined]
    assert sibling is not None and sibling.last_result is None
    assert sibling.next_due_at == serper_target.next_due_at  # type: ignore[attr-defined]

    assert len(hworld.ds.artifacts) == 2
    assert {a.origin for a in hworld.ds.artifacts.values()} == {"legal_codes"}
    assert hworld.code_queue.pending_count() == 0
    (history,) = hworld.ds.list_history(hworld.config.name)
    assert history.source == Source.LEGAL_CODES and history.topic is None


def test_deadline_abort_keeps_staged_candidates_and_schedules_plus_one_day(
    hworld: HWorld,
) -> None:
    _, _, code_target = _setup(hworld)
    hworld.discoverer.candidates[PORTAL] = [
        PortalCandidate("https://library.municode.com/ca/pasadena/export/ch1.pdf", "Ch 1"),
    ]
    hworld.discoverer.incomplete_for = {PORTAL}  # deadline truncation
    hworld.dispatch_code(code_target, [PORTAL])  # type: ignore[arg-type]

    hworld.code_worker().handle_batch(hworld.code_queue.receive(1))

    assert len(hworld.ds.artifacts) == 1  # partial candidates kept
    final = hworld.ds.get_target(code_target.id)  # type: ignore[attr-defined]
    assert final is not None
    assert final.last_result == SweepResult.CANDIDATES
    assert final.next_due_at == hworld.clock.now() + timedelta(days=1)  # resume tomorrow
    (history,) = hworld.ds.list_history(hworld.config.name)
    assert "truncated" in history.detail or "deadline" in history.detail


def test_discoverer_transient_failure_leaves_message_for_redelivery(hworld: HWorld) -> None:
    _, _, code_target = _setup(hworld)
    hworld.discoverer.transient_for = {PORTAL}  # anti-bot / render hang
    hworld.dispatch_code(code_target, [PORTAL])  # type: ignore[arg-type]

    hworld.code_worker().handle_batch(hworld.code_queue.receive(1))

    assert hworld.code_queue.pending_count() == 1  # left undeleted
    assert hworld.ds.list_history(hworld.config.name) == []  # nothing recorded
    still = hworld.ds.get_target(code_target.id)  # type: ignore[attr-defined]
    assert still is not None and still.dispatch_id is not None  # stays in flight

    # After the block clears, redelivery completes the crawl.
    hworld.discoverer.transient_for = set()
    hworld.discoverer.candidates[PORTAL] = [
        PortalCandidate("https://library.municode.com/ca/pasadena/export/ch1.pdf", "Ch 1")
    ]
    hworld.clock.advance(901)
    hworld.code_worker().handle_batch(hworld.code_queue.receive(1))
    assert hworld.code_queue.pending_count() == 0
    assert len(hworld.ds.artifacts) == 1


def test_crawl_error_zero_candidates_plus_one_day(hworld: HWorld) -> None:
    _, _, code_target = _setup(hworld)
    hworld.discoverer.error_for = {PORTAL}
    hworld.dispatch_code(code_target, [PORTAL])  # type: ignore[arg-type]

    hworld.code_worker().handle_batch(hworld.code_queue.receive(1))

    final = hworld.ds.get_target(code_target.id)  # type: ignore[attr-defined]
    assert final is not None
    assert final.last_result == SweepResult.ERROR
    assert final.next_due_at == hworld.clock.now() + timedelta(days=1)
    (history,) = hworld.ds.list_history(hworld.config.name)
    assert history.result == SweepResult.ERROR
    assert hworld.code_queue.pending_count() == 0


def test_no_llm_involved_in_code_path(hworld: HWorld) -> None:
    _, _, code_target = _setup(hworld)
    hworld.discoverer.candidates[PORTAL] = [
        PortalCandidate("https://library.municode.com/ca/pasadena/export/ch1.pdf", "Ch 1")
    ]
    hworld.dispatch_code(code_target, [PORTAL])  # type: ignore[arg-type]
    hworld.code_worker().handle_batch(hworld.code_queue.receive(1))
    assert hworld.llm.generate_calls == []
    assert hworld.llm.triage_calls == []
