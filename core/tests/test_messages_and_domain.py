from datetime import UTC, datetime

import pytest
from harvest_core.domain import ArtifactStatus, check_transition
from harvest_core.errors import IllegalTransition
from harvest_core.messages import CodeTask, FetchTask, SweepTask, parse_task, to_json

NOW = datetime(2026, 7, 30, 12, 0, 0, tzinfo=UTC)


def test_sweep_task_round_trip() -> None:
    task = SweepTask(
        corpus="c",
        sweep_target_id=45,
        jurisdiction_id=45,
        topic="noise",
        query_text="Pasadena CA noise ordinance filetype:pdf",
        dispatch_id="d-1",
        query_seq=2,
        query_count=6,
        dispatched_at=NOW,
    )
    parsed = parse_task(to_json(task))
    assert isinstance(parsed, SweepTask)
    assert parsed == task


def test_code_task_round_trip() -> None:
    task = CodeTask(
        corpus="c",
        sweep_target_id=46,
        jurisdiction_id=45,
        portal_urls=["https://library.municode.com/ca/pasadena"],
        dispatch_id="d-2",
        dispatched_at=NOW,
    )
    parsed = parse_task(to_json(task))
    assert isinstance(parsed, CodeTask)
    assert parsed == task


def test_fetch_task_round_trip() -> None:
    task = FetchTask(corpus="c", artifact_id=901, dispatch_id="d-3", dispatched_at=NOW)
    parsed = parse_task(to_json(task))
    assert isinstance(parsed, FetchTask)
    assert parsed == task


def test_legal_artifact_transitions() -> None:
    check_transition(ArtifactStatus.PENDING, ArtifactStatus.FETCHED)
    check_transition(ArtifactStatus.PENDING, ArtifactStatus.FAILED)
    check_transition(ArtifactStatus.PENDING, ArtifactStatus.NOT_DOCUMENT)
    check_transition(ArtifactStatus.PENDING, ArtifactStatus.DUPLICATE)


@pytest.mark.parametrize(
    ("current", "new"),
    [
        (ArtifactStatus.FETCHED, ArtifactStatus.PENDING),
        (ArtifactStatus.FAILED, ArtifactStatus.FETCHED),
        (ArtifactStatus.DUPLICATE, ArtifactStatus.PENDING),
        (ArtifactStatus.PENDING, ArtifactStatus.CATALOGED),
        (ArtifactStatus.FETCHED, ArtifactStatus.FETCHED),
    ],
)
def test_illegal_artifact_transitions(current: ArtifactStatus, new: ArtifactStatus) -> None:
    with pytest.raises(IllegalTransition):
        check_transition(current, new)
