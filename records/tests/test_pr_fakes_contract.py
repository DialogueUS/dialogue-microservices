"""Contract tests for the PR-specific fakes — scenario tests prove the
spec, not fake quirks, because these semantics are themselves tested."""

from datetime import UTC, datetime
from typing import Any

import pytest
from harvest_core.fakes import FakeQueue, VirtualClock
from public_records.config import CampaignConfig
from public_records.domain import EscalationReason, OutboundKind, ThreadStatus
from public_records.errors import IllegalTransition, SendTransientError, UniqueViolation
from public_records.fakes import FakeEmailTransport, FakeQueueWithDlq, FakeRecordsStore
from public_records.ports import OutboundEmail

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _config(name: str = "camp") -> CampaignConfig:
    return CampaignConfig.model_validate(
        {
            "name": name,
            "record_type": "noise complaints",
            "record_description": "All noise complaints filed in 2025.",
            "requester": {"name": "Ada", "email": "ada@example.org",
                          "consent_confirmed": True},
        }
    )


def _seed(store: FakeRecordsStore) -> tuple[int, int]:
    campaign = store.insert_campaign(_config(), NOW)
    jur = store.insert_jurisdiction("Pasadena", "CA", "city")
    return campaign.id, jur.id


def test_duplicate_campaign_name_raises() -> None:
    store = FakeRecordsStore()
    store.insert_campaign(_config(), NOW)
    with pytest.raises(UniqueViolation):
        store.insert_campaign(_config(), NOW)


def test_duplicate_search_target_raises_and_resolve_is_cas() -> None:
    store = FakeRecordsStore()
    cid, jid = _seed(store)
    target = store.insert_search_target(cid, jid, NOW)
    with pytest.raises(UniqueViolation):
        store.insert_search_target(cid, jid, NOW)
    assert store.resolve_target(target.id) is True
    assert store.resolve_target(target.id) is False  # second race loses


def test_mark_query_consumed_idempotent_per_index() -> None:
    store = FakeRecordsStore()
    cid, jid = _seed(store)
    target = store.insert_search_target(cid, jid, NOW)
    store.set_target_queries_enqueued(target.id, 3)
    assert store.mark_query_consumed(target.id, 0) is False
    assert store.mark_query_consumed(target.id, 0) is False  # duplicate delivery
    assert store.mark_query_consumed(target.id, 2) is False
    assert store.mark_query_consumed(target.id, 1) is True  # all consumed
    assert store.mark_query_consumed(target.id, 1) is True  # still true, not double


def test_duplicate_message_id_per_campaign_raises() -> None:
    store = FakeRecordsStore()
    cid, jid = _seed(store)
    kwargs: dict[str, Any] = dict(
        thread_id=None, campaign_id=cid, direction="inbound",
        from_address="clerk@x.gov", to_address="req@example.org", subject="s",
        body="b", kind=None, classification=None, message_id="<m1@x>",
        source_key="inbox/k1", resend_id=None, in_reply_to_email_id=None,
        attachment_refs=[], created_at=NOW,
    )
    store.insert_email(**kwargs)  # type: ignore[arg-type]
    with pytest.raises(UniqueViolation):
        store.insert_email(**kwargs)  # type: ignore[arg-type]
    # same message id under another campaign is fine
    other = store.insert_campaign(_config("other"), NOW)
    store.insert_email(**{**kwargs, "campaign_id": other.id})  # type: ignore[arg-type]
    assert store.has_source_key("inbox/k1")
    assert store.has_message_id(cid, "<m1@x>")
    assert not store.has_message_id(cid, "<m2@x>")


def test_record_initial_send_creates_thread_and_stamps_cooldown() -> None:
    store = FakeRecordsStore()
    cid, jid = _seed(store)
    thread, email = store.record_initial_send(
        campaign_id=cid, jurisdiction_id=jid, thread_token="ab" * 8,
        contact_email="records@pasadena.gov", parent_thread_id=None,
        existing_thread_id=None, from_address="req@dialogue.org",
        to_address="records@pasadena.gov", subject="s [DLG-abababababababab]",
        body="b", resend_id="r1", next_action_at=NOW, now=NOW,
    )
    assert thread.status is ThreadStatus.REQUEST_SENT
    assert email.kind is OutboundKind.INITIAL_REQUEST
    jur = store.get_jurisdiction(jid)
    assert jur is not None and jur.last_contacted_at == NOW
    assert store.count_outbound_since(cid, NOW) == 1
    with pytest.raises(UniqueViolation):  # token is globally unique
        store.record_initial_send(
            campaign_id=cid, jurisdiction_id=jid, thread_token="ab" * 8,
            contact_email="x@y.gov", parent_thread_id=None, existing_thread_id=None,
            from_address="a@b", to_address="x@y.gov", subject="s", body="b",
            resend_id=None, next_action_at=NOW, now=NOW,
        )


def test_thread_transitions_validated_in_store() -> None:
    store = FakeRecordsStore()
    cid, jid = _seed(store)
    thread, _ = store.record_initial_send(
        campaign_id=cid, jurisdiction_id=jid, thread_token="cd" * 8,
        contact_email="records@pasadena.gov", parent_thread_id=None,
        existing_thread_id=None, from_address="a@b", to_address="c@d",
        subject="s", body="b", resend_id=None, next_action_at=NOW, now=NOW,
    )
    store.set_thread_status(thread.id, ThreadStatus.FULFILLED, None)
    with pytest.raises(IllegalTransition):
        store.set_thread_status(thread.id, ThreadStatus.AWAITING_REPLY, NOW)
    # human resolution may set anything
    store.set_thread_status(thread.id, ThreadStatus.PENDING_SEND, None, by_human=True)


def test_book_fee_admits_exactly_one_at_the_budget_edge() -> None:
    store = FakeRecordsStore()
    cid, _ = _seed(store)
    budget = 5000
    first = store.book_fee(cid, None, 3000, budget, "note", NOW)
    assert first is not None
    second = store.book_fee(cid, None, 3000, budget, "note", NOW)
    assert second is None  # 3000 + 3000 > 5000
    third = store.book_fee(cid, None, 2000, budget, "note", NOW)
    assert third is not None
    assert store.committed_total_cents(cid) == 5000


def test_purge_campaign_is_scoped() -> None:
    store = FakeRecordsStore()
    cid, jid = _seed(store)
    other = store.insert_campaign(_config("other"), NOW)
    target = store.insert_search_target(cid, jid, NOW)
    thread, _ = store.record_initial_send(
        campaign_id=cid, jurisdiction_id=jid, thread_token="ef" * 8,
        contact_email="records@pasadena.gov", parent_thread_id=None,
        existing_thread_id=None, from_address="a@b", to_address="c@d",
        subject="s", body="b", resend_id=None, next_action_at=NOW, now=NOW,
    )
    store.insert_escalation(cid, thread.id, EscalationReason.OTHER, "d", NOW)
    store.book_fee(cid, thread.id, 100, 1000, "n", NOW)

    counts = store.purge_campaign(cid)
    assert counts == {
        "spend_entries": 1, "escalations": 1, "emails": 1, "threads": 1,
        "search_targets": 1, "campaigns": 1,
    }
    assert store.get_campaign(cid) is None
    assert store.get_search_target(target.id) is None
    # jurisdictions survive — shared across campaigns
    assert store.get_jurisdiction(jid) is not None
    assert store.get_campaign(other.id) is not None


def test_transport_failure_records_nothing() -> None:
    transport = FakeEmailTransport()
    transport.fail_next = 1
    email = OutboundEmail(from_address="a@b", to_address="c@d", subject="s", body="b")
    with pytest.raises(SendTransientError):
        transport.send(email)
    assert transport.sent == []
    assert transport.send(email).startswith("resend-")
    assert len(transport.sent) == 1


def test_fake_queue_redrives_to_linked_dlq() -> None:
    clock = VirtualClock()
    dlq = FakeQueue(clock)
    queue = FakeQueueWithDlq(clock, visibility_timeout=900, max_receive_count=3,
                             dlq_queue=dlq)
    queue.send("doomed")
    for _ in range(3):  # three failed receives
        got = queue.receive(10)
        assert len(got) == 1
        clock.advance(901)
    assert queue.receive(10) == []  # fourth receive redrives instead
    dead = dlq.receive(10)
    assert len(dead) == 1 and dead[0].body == "doomed"
