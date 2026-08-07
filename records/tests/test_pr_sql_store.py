"""SqlRecordsStore on SQLite: same contract surface as the fake."""

from datetime import UTC, datetime
from typing import Any

import pytest
import sqlalchemy as sa
from public_records.adapters.sql_store import SqlRecordsStore, migrate
from public_records.config import CampaignConfig
from public_records.domain import EscalationReason, OutboundKind, ThreadStatus
from public_records.errors import IllegalTransition, UniqueViolation

NOW = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture()
def store() -> SqlRecordsStore:
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    migrate(engine)
    migrate(engine)  # idempotent
    return SqlRecordsStore(engine)


def _config(name: str = "camp") -> CampaignConfig:
    return CampaignConfig.model_validate(
        {
            "name": name,
            "record_type": "noise complaints",
            "record_description": "All noise complaints filed in 2025.",
            "legal_basis": "CPRA",
            "requester": {
                "name": "Ada",
                "email": "ada@example.org",
                "anonymous": False,
                "consent_confirmed": True,
            },
            "scope": {"levels": ["city"], "states": ["CA"]},
            "limits": {"fee_budget_usd": 50.0},
            "notify_email": "ops@example.org",
        }
    )


def test_campaign_round_trip_and_unique_name(store: SqlRecordsStore) -> None:
    created = store.insert_campaign(_config(), NOW)
    loaded = store.get_campaign(created.id)
    assert loaded is not None
    assert loaded.config == _config()
    assert loaded.active is False and loaded.seeded is False
    with pytest.raises(UniqueViolation):
        store.insert_campaign(_config(), NOW)

    store.set_campaign_active(created.id, True)
    store.set_campaign_seeded(created.id)
    reloaded = store.get_campaign_by_name("camp")
    assert reloaded is not None and reloaded.active and reloaded.seeded


def test_target_resolution_and_consumed_indexes(store: SqlRecordsStore) -> None:
    campaign = store.insert_campaign(_config(), NOW)
    jur = store.insert_jurisdiction("Pasadena", "CA", "city")
    target = store.insert_search_target(campaign.id, jur.id, NOW)
    with pytest.raises(UniqueViolation):
        store.insert_search_target(campaign.id, jur.id, NOW)

    store.set_target_queries_enqueued(target.id, 2)
    assert store.mark_query_consumed(target.id, 1) is False
    assert store.mark_query_consumed(target.id, 1) is False  # idempotent
    assert store.mark_query_consumed(target.id, 0) is True
    assert store.resolve_target(target.id) is True
    assert store.resolve_target(target.id) is False


def test_initial_send_thread_email_and_cooldown_commit_together(
    store: SqlRecordsStore,
) -> None:
    campaign = store.insert_campaign(_config(), NOW)
    jur = store.insert_jurisdiction("Pasadena", "CA", "city")
    thread, email = store.record_initial_send(
        campaign_id=campaign.id, jurisdiction_id=jur.id, thread_token="ab" * 8,
        contact_email="records@pasadena.gov", parent_thread_id=None,
        existing_thread_id=None, from_address="req@dialogue.org",
        to_address="records@pasadena.gov", subject="s [DLG-abababababababab]",
        body="b", resend_id="r1", next_action_at=NOW, now=NOW,
    )
    assert thread.status is ThreadStatus.REQUEST_SENT
    assert email.kind is OutboundKind.INITIAL_REQUEST
    jur2 = store.get_jurisdiction(jur.id)
    assert jur2 is not None and jur2.last_contacted_at == NOW
    assert store.count_outbound_since(campaign.id, NOW) == 1
    assert store.first_outbound_subject(thread.id) == "s [DLG-abababababababab]"
    assert store.get_thread_by_token("ab" * 8) is not None
    assert store.find_thread(campaign.id, jur.id, "records@pasadena.gov") is not None
    assert store.find_open_thread_by_contact("records@pasadena.gov") is not None

    with pytest.raises(UniqueViolation):
        store.record_initial_send(
            campaign_id=campaign.id, jurisdiction_id=jur.id, thread_token="ab" * 8,
            contact_email="x@y.gov", parent_thread_id=None, existing_thread_id=None,
            from_address="a@b", to_address="x@y.gov", subject="s", body="b",
            resend_id=None, next_action_at=NOW, now=NOW,
        )


def test_thread_send_and_transition_validation(store: SqlRecordsStore) -> None:
    campaign = store.insert_campaign(_config(), NOW)
    jur = store.insert_jurisdiction("Pasadena", "CA", "city")
    thread, _ = store.record_initial_send(
        campaign_id=campaign.id, jurisdiction_id=jur.id, thread_token="cd" * 8,
        contact_email="records@pasadena.gov", parent_thread_id=None,
        existing_thread_id=None, from_address="a@b", to_address="c@d",
        subject="s", body="b", resend_id=None, next_action_at=NOW, now=NOW,
    )
    email = store.record_thread_send(
        thread_id=thread.id, kind=OutboundKind.FOLLOWUP, from_address="a@b",
        to_address="c@d", subject="Re: s", body="nudge", resend_id="r2",
        in_reply_to_email_id=None, new_status=ThreadStatus.AWAITING_REPLY,
        next_action_at=NOW, increment_followups=True, now=NOW,
    )
    updated = store.get_thread(thread.id)
    assert updated is not None and updated.followups_sent == 1
    assert updated.status is ThreadStatus.AWAITING_REPLY
    assert not store.outbound_reply_exists(thread.id, 999, OutboundKind.FOLLOWUP)

    inbound = store.insert_email(
        thread_id=thread.id, campaign_id=campaign.id, direction="inbound",
        from_address="c@d", to_address="a@b", subject="Re: s", body="q",
        kind=None, classification=None, message_id="<m1@x>", source_key="inbox/1",
        resend_id=None, in_reply_to_email_id=None, attachment_refs=[], created_at=NOW,
    )
    store.record_thread_send(
        thread_id=thread.id, kind=OutboundKind.CLARIFICATION_REPLY, from_address="a@b",
        to_address="c@d", subject="Re: s", body="answer", resend_id=None,
        in_reply_to_email_id=inbound.id, new_status=ThreadStatus.AWAITING_REPLY,
        next_action_at=NOW, increment_followups=False, now=NOW,
    )
    assert store.outbound_reply_exists(thread.id, inbound.id, OutboundKind.CLARIFICATION_REPLY)
    assert email.id != inbound.id

    store.set_thread_status(thread.id, ThreadStatus.FULFILLED, None)
    with pytest.raises(IllegalTransition):
        store.set_thread_status(thread.id, ThreadStatus.AWAITING_REPLY, NOW)
    store.set_thread_status(thread.id, ThreadStatus.PENDING_SEND, None, by_human=True)


def test_message_id_unique_per_campaign(store: SqlRecordsStore) -> None:
    campaign = store.insert_campaign(_config(), NOW)
    other = store.insert_campaign(_config("other"), NOW)
    kwargs: dict[str, Any] = dict(
        thread_id=None, direction="inbound", from_address="a@b", to_address="c@d",
        subject="s", body="b", kind=None, classification=None, message_id="<m@x>",
        source_key="inbox/x", resend_id=None, in_reply_to_email_id=None,
        attachment_refs=[], created_at=NOW,
    )
    store.insert_email(campaign_id=campaign.id, **kwargs)  # type: ignore[arg-type]
    with pytest.raises(UniqueViolation):
        store.insert_email(campaign_id=campaign.id, **kwargs)  # type: ignore[arg-type]
    store.insert_email(campaign_id=other.id, **kwargs)  # type: ignore[arg-type]
    assert store.has_source_key("inbox/x")
    assert store.has_message_id(campaign.id, "<m@x>")


def test_classification_and_attachment_refs_round_trip(store: SqlRecordsStore) -> None:
    from public_records.domain import Classification, InboundCategory

    campaign = store.insert_campaign(_config(), NOW)
    email = store.insert_email(
        thread_id=None, campaign_id=campaign.id, direction="inbound",
        from_address="a@b", to_address="c@d", subject="s", body="b", kind=None,
        classification=Classification(
            category=InboundCategory.REFERRAL, summary="go ask county",
            confidence=0.8, referral_email="county@x.gov",
        ),
        message_id="<m2@x>", source_key=None, resend_id=None,
        in_reply_to_email_id=None,
        attachment_refs=[{"filename": "a.pdf", "status": "stored", "key": "k"}],
        created_at=NOW,
    )
    loaded = store.list_emails(campaign_id=campaign.id)[0]
    assert loaded.classification is not None
    assert loaded.classification.category.api_value == "referral"
    assert loaded.classification.referral_email == "county@x.gov"
    assert loaded.attachment_refs == [{"filename": "a.pdf", "status": "stored", "key": "k"}]
    store.update_attachment_refs(email.id, [])
    assert store.list_emails(campaign_id=campaign.id)[0].attachment_refs == []


def test_escalations_spend_and_purge(store: SqlRecordsStore) -> None:
    campaign = store.insert_campaign(_config(), NOW)
    jur = store.insert_jurisdiction("Pasadena", "CA", "city")
    thread, _ = store.record_initial_send(
        campaign_id=campaign.id, jurisdiction_id=jur.id, thread_token="ef" * 8,
        contact_email="records@pasadena.gov", parent_thread_id=None,
        existing_thread_id=None, from_address="a@b", to_address="c@d",
        subject="s", body="b", resend_id=None, next_action_at=NOW, now=NOW,
    )
    esc = store.insert_escalation(campaign.id, thread.id, EscalationReason.DENIAL, "d", NOW)
    assert [e.id for e in store.unnotified_escalations(campaign.id)] == [esc.id]
    store.mark_escalations_notified([esc.id])
    assert store.unnotified_escalations(campaign.id) == []
    store.resolve_escalation(esc.id, "reviewed", NOW)
    got = store.get_escalation(esc.id)
    assert got is not None and got.status.api_value == "resolved"

    assert store.book_fee(campaign.id, thread.id, 4000, 5000, "note", NOW) is not None
    assert store.book_fee(campaign.id, thread.id, 2000, 5000, "note", NOW) is None
    assert store.committed_total_cents(campaign.id) == 4000
    assert len(store.unnotified_spend(campaign.id)) == 1
    store.mark_spend_notified([store.list_spend(campaign.id)[0].id])
    assert store.unnotified_spend(campaign.id) == []

    counts = store.purge_campaign(campaign.id)
    assert counts == {
        "spend_entries": 1, "escalations": 1, "emails": 1, "threads": 1,
        "search_targets": 0, "campaigns": 1,
    }
    assert store.get_campaign(campaign.id) is None
    assert store.get_jurisdiction(jur.id) is not None  # shared, survives


def test_select_due_followups_boundary(store: SqlRecordsStore) -> None:
    from datetime import timedelta

    campaign = store.insert_campaign(_config(), NOW)
    jur = store.insert_jurisdiction("Pasadena", "CA", "city")
    thread, _ = store.record_initial_send(
        campaign_id=campaign.id, jurisdiction_id=jur.id, thread_token="0a" * 8,
        contact_email="records@pasadena.gov", parent_thread_id=None,
        existing_thread_id=None, from_address="a@b", to_address="c@d",
        subject="s", body="b", resend_id=None,
        next_action_at=NOW + timedelta(days=10), now=NOW,
    )
    assert store.select_due_followups(NOW + timedelta(days=10, seconds=-1), 50) == []
    due = store.select_due_followups(NOW + timedelta(days=10), 50)
    assert [t.id for t in due] == [thread.id]
    # fulfilled threads are never selected
    store.set_thread_status(thread.id, ThreadStatus.FULFILLED, None)
    assert store.select_due_followups(NOW + timedelta(days=30), 50) == []
