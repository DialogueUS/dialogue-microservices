import re

import pytest
from public_records.domain import (
    InboundCategory,
    ThreadStatus,
    check_transition,
    document_key,
    extract_token,
    fallback_message_id,
    is_generic_address,
    is_junk_address,
    new_thread_token,
    spool_key,
)
from public_records.errors import IllegalTransition
from public_records.messages import (
    ContactMessage,
    FollowupJobMessage,
    InboundAttachment,
    InboundMailMessage,
    SearchQueryMessage,
)


def test_all_four_messages_round_trip() -> None:
    msgs = [
        SearchQueryMessage(
            campaign_id=1, jurisdiction_id=123, search_target_id=456,
            query="Pasadena CA public records request CPRA email clerk", query_index=0,
        ),
        ContactMessage(
            campaign_id=1, jurisdiction_id=123, contact_email="clerk@pasadena.gov",
            source="referral", bypass_cooldown=True, parent_thread_id=42,
        ),
        FollowupJobMessage(thread_id=42, kind="fee_agreement", inbound_email_id=7,
                           amount_cents=2500),
        InboundMailMessage(
            source_key="inbox/abc", message_id="<x@y>", thread_token="deadbeefdeadbeef",
            from_address="clerk@pasadena.gov", subject="Re: request", body="hello",
            attachments=[InboundAttachment(
                filename="a.pdf", content_type="application/pdf",
                s3_tmp_key="camp/inbox-spool/12345678_0_a.pdf")],
        ),
    ]
    for msg in msgs:
        assert type(msg).from_json(msg.to_json()) == msg


def test_unknown_fields_rejected() -> None:
    with pytest.raises(ValueError):
        FollowupJobMessage.from_json('{"thread_id": 1, "kind": "followup", "surprise": true}')
    with pytest.raises(ValueError):
        FollowupJobMessage.from_json('{"thread_id": 1, "kind": "resweep"}')


def test_transition_helper_enforces_state_machine() -> None:
    check_transition(ThreadStatus.PENDING_SEND, ThreadStatus.REQUEST_SENT)
    check_transition(ThreadStatus.REQUEST_SENT, ThreadStatus.FULFILLED)
    check_transition(ThreadStatus.AWAITING_REPLY, ThreadStatus.AWAITING_REPLY)
    check_transition(ThreadStatus.AWAITING_REPLY, ThreadStatus.REFERRED)

    with pytest.raises(IllegalTransition):
        check_transition(ThreadStatus.FULFILLED, ThreadStatus.AWAITING_REPLY)
    with pytest.raises(IllegalTransition):
        check_transition(ThreadStatus.PENDING_SEND, ThreadStatus.FULFILLED)
    # needs_human is a parking state: only a human moves a thread out
    with pytest.raises(IllegalTransition):
        check_transition(ThreadStatus.NEEDS_HUMAN, ThreadStatus.PENDING_SEND)
    check_transition(ThreadStatus.NEEDS_HUMAN, ThreadStatus.PENDING_SEND, by_human=True)


def test_failed_is_never_assigned_automatically() -> None:
    for status in ThreadStatus:
        if status is ThreadStatus.FAILED:
            continue
        with pytest.raises(IllegalTransition):
            check_transition(status, ThreadStatus.FAILED)
    check_transition(ThreadStatus.NEEDS_HUMAN, ThreadStatus.FAILED, by_human=True)


def test_thread_token_shape_and_uniqueness() -> None:
    tokens = {new_thread_token() for _ in range(64)}
    assert len(tokens) == 64
    for token in tokens:
        assert re.fullmatch(r"[0-9a-f]{16}", token)


def test_token_extraction_header_first_then_subject() -> None:
    token = "deadbeefcafef00d"
    assert extract_token({"X-Dialogue-Token": token}, "no marker") == token
    assert extract_token({"x-dialogue-token": token.upper()}, "") == token
    assert extract_token({}, f"Re: Public Records Request [DLG-{token}]") == token
    # legacy [RF-…] accepted inbound
    assert extract_token({}, f"RE: old request [RF-{token}]") == token
    # header wins over a different subject token
    assert extract_token({"X-Dialogue-Token": token}, "[DLG-1111222233334444]") == token
    assert extract_token({}, "Public Records Request") is None


def test_fallback_message_id_stable() -> None:
    a = fallback_message_id("a@x.gov", "Re: hi", "body")
    assert a == fallback_message_id("a@x.gov", "Re: hi", "body")
    assert a != fallback_message_id("a@x.gov", "Re: hi", "body2")
    assert re.fullmatch(r"sha:[0-9a-f]{32}", a)


def test_address_heuristics_verbatim_lists() -> None:
    for junk in ("noreply@x.gov", "No-Reply@x.gov", "mailer-daemon@x.gov", "webmaster@x.gov"):
        assert is_junk_address(junk)
    assert not is_junk_address("records@x.gov")

    for generic in (
        "records@x.gov", "cityclerk@x.gov", "foia.officer@x.gov", "CPRA@x.gov",
        "publicrecords@x.gov", "sunshine-requests@x.gov", "info@x.gov", "cityhall@x.gov",
    ):
        assert is_generic_address(generic), generic
    for personal in ("jsmith@x.gov", "maria.lopez@x.gov", "bob@x.gov"):
        assert not is_generic_address(personal), personal


def test_storage_keys() -> None:
    digest = "abcdef0123456789" * 4
    key = document_key("My Campaign!", "Los Angeles County", digest, "fee schedule.pdf")
    assert key == "my-campaign/los-angeles-county/abcdef01_fee_schedule.pdf"
    skey = spool_key("My Campaign!", digest, 2, "b c.xlsx")
    assert skey == "my-campaign/inbox-spool/abcdef01_2_b_c.xlsx"
    # slugs capped at 60 chars
    long = document_key("x" * 100, "y" * 100, digest, "a.pdf")
    campaign_slug, jur_slug, _ = long.split("/")
    assert len(campaign_slug) == 60 and len(jur_slug) == 60


def test_classification_categories_are_the_old_pipelines() -> None:
    assert {c.api_value for c in InboundCategory} == {
        "data_provided", "payment_required", "needs_clarification", "denial",
        "referral", "acknowledgment", "unclear",
    }
