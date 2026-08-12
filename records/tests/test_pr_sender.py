"""Sender (§7): gate order, drafting assembly, idempotency, dry_run."""

from datetime import timedelta

from pr_fixtures import PrWorld
from public_records.domain import (
    Campaign,
    EscalationReason,
    Jurisdiction,
    OutboundKind,
    ThreadStatus,
)
from public_records.messages import ContactMessage, FollowupJobMessage
from public_records.sender import handle_contact, handle_followup_job

DAY = 86_400


def _contact_msg(campaign: Campaign, jur: Jurisdiction, email: str = "records@x.gov",
                 source: str = "scraper", bypass: bool = False,
                 parent: int | None = None) -> str:
    return ContactMessage(
        campaign_id=campaign.id, jurisdiction_id=jur.id, contact_email=email,
        source=source, bypass_cooldown=bypass, parent_thread_id=parent,  # type: ignore[arg-type]
    ).to_json()


# --------------------------------------------------------------------------
# §7.1 initial requests


def test_initial_send_full_assembly(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    assert handle_contact(pr.world, _contact_msg(campaign, jur)) is True

    [sent] = pr.transport.sent
    assert sent.to_address == "records@x.gov"
    assert sent.subject.startswith("Public Records Request — noise complaints — Pasadena, CA")
    token = sent.headers["X-Dialogue-Token"]
    assert f"[DLG-{token}]" in sent.subject
    assert sent.body.startswith("Dear")  # salutation first
    assert "---\nExact records requested:\nAll noise complaints filed in 2025." in sent.body
    assert "Thanks for your time." in sent.body  # anonymous signature

    thread = pr.store.get_thread_by_token(token)
    assert thread is not None and thread.status is ThreadStatus.REQUEST_SENT
    assert thread.next_action_at == pr.clock.now() + timedelta(days=10)
    jur2 = pr.store.get_jurisdiction(jur.id)
    assert jur2 is not None and jur2.last_contacted_at == pr.clock.now()
    [email] = pr.store.list_emails(thread_id=thread.id)
    assert email.kind is OutboundKind.INITIAL_REQUEST and email.resend_id is not None


def test_named_requester_signature(pr: PrWorld) -> None:
    campaign = pr.add_campaign(requester={
        "name": "Ada Requester", "email": "ada@example.org", "anonymous": False,
        "consent_confirmed": True, "organization": "Example Org", "phone": "555-0100",
    })
    jur = pr.add_jurisdiction()
    handle_contact(pr.world, _contact_msg(campaign, jur))
    [sent] = pr.transport.sent
    assert "Sincerely,\nAda Requester\nExample Org\nada@example.org\n555-0100" in sent.body


def test_idempotency_triple_drops_duplicate(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    body = _contact_msg(campaign, jur)
    assert handle_contact(pr.world, body) is True
    assert handle_contact(pr.world, body) is True  # deleted without sending
    assert len(pr.transport.sent) == 1


def test_consent_or_active_off_drains_inertly(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    pr.store.set_campaign_active(campaign.id, False)
    assert handle_contact(pr.world, _contact_msg(campaign, jur)) is True  # dropped
    assert pr.transport.sent == []

    revoked = pr.add_campaign(
        name="revoked",
        requester={"name": "A", "email": "a@b", "consent_confirmed": False},
    )
    assert handle_contact(pr.world, _contact_msg(revoked, jur)) is True
    assert pr.transport.sent == []


def test_daily_cap_gates_initial_only_and_resets_at_utc_midnight(pr: PrWorld) -> None:
    campaign = pr.add_campaign(limits={"daily_send_cap": 1})
    jur_a = pr.add_jurisdiction("Kern County")
    jur_b = pr.add_jurisdiction("Inyo County")
    assert handle_contact(pr.world, _contact_msg(campaign, jur_a, "records@a.gov")) is True
    # cap reached: second initial retries (message left)
    assert handle_contact(pr.world, _contact_msg(campaign, jur_b, "records@b.gov")) is False
    assert len(pr.transport.sent) == 1

    # but a follow-up on the same campaign still sends past the cap
    thread = pr.store.find_thread(campaign.id, jur_a.id, "records@a.gov")
    assert thread is not None
    job = FollowupJobMessage(thread_id=thread.id, kind="followup", followup_index=0)
    assert handle_followup_job(pr.world, job.to_json()) is True
    assert len(pr.transport.sent) == 2

    # VirtualClock starts at midnight UTC: at 23:59:59 still capped, at
    # midnight the counter window resets
    pr.clock.advance(DAY - 1)
    assert handle_contact(pr.world, _contact_msg(campaign, jur_b, "records@b.gov")) is False
    pr.clock.advance(1)
    assert handle_contact(pr.world, _contact_msg(campaign, jur_b, "records@b.gov")) is True


def test_office_cooldown_shared_across_campaigns_and_bypass(pr: PrWorld) -> None:
    campaign_a = pr.add_campaign(name="camp-a")
    campaign_b = pr.add_campaign(name="camp-b")
    jur = pr.add_jurisdiction()
    assert handle_contact(pr.world, _contact_msg(campaign_a, jur, "records@x.gov")) is True

    # a second campaign mailing the same office throttles (shared clock)
    pr.clock.advance(3 * DAY)
    assert handle_contact(pr.world, _contact_msg(campaign_b, jur, "records@x.gov")) is False
    # bypass_cooldown (referral / human_approved) sends anyway — and
    # re-stamps the shared clock
    assert handle_contact(
        pr.world, _contact_msg(campaign_b, jur, "other@x.gov", source="referral", bypass=True)
    ) is True
    # cooldown expires exactly 7 days after the latest contact
    pr.clock.advance(7 * DAY - 1)
    assert handle_contact(pr.world, _contact_msg(campaign_b, jur, "records@x.gov")) is False
    pr.clock.advance(1)
    assert handle_contact(pr.world, _contact_msg(campaign_b, jur, "records@x.gov")) is True


def test_test_campaign_does_not_claim_the_shared_office_clock(pr: PrWorld) -> None:
    test_campaign = pr.add_campaign(
        name="camp-test",
        test_contacts=[
            {"jurisdiction": "Pasadena", "state": "CA", "email": "inbox@example.test"}
        ],
    )
    real = pr.add_campaign(name="camp-real")
    jur = pr.add_jurisdiction()  # Pasadena, CA — the same office row

    msg = _contact_msg(test_campaign, jur, "inbox@example.test", source="seeded", bypass=True)
    assert handle_contact(pr.world, msg) is True
    assert pr.transport.sent[0].to_address == "inbox@example.test"

    # the office was never really written to, so the real campaign is
    # free to mail it immediately — and its send stamps the clock
    assert pr.store.get_jurisdiction(jur.id).last_contacted_at is None  # type: ignore[union-attr]
    assert handle_contact(pr.world, _contact_msg(real, jur, "records@x.gov")) is True
    assert pr.store.get_jurisdiction(jur.id).last_contacted_at is not None  # type: ignore[union-attr]


def test_gate_order_cap_checked_before_cooldown(pr: PrWorld) -> None:
    campaign = pr.add_campaign(limits={"daily_send_cap": 1})
    jur = pr.add_jurisdiction()
    handle_contact(pr.world, _contact_msg(campaign, jur, "records@x.gov"))
    # both over-cap AND cooling down: retries (False), no escalation
    assert handle_contact(pr.world, _contact_msg(campaign, jur, "again@x.gov")) is False
    assert pr.store.list_escalations() == []


def test_anonymous_state_guard_escalates_per_target(pr: PrWorld) -> None:
    campaign = pr.add_campaign(scope={"states": ["ALL"]})  # anonymous default true
    jur = pr.add_jurisdiction("Davidson County", state="TN")
    assert handle_contact(pr.world, _contact_msg(campaign, jur)) is True  # dropped
    assert pr.transport.sent == []
    [esc] = pr.store.list_escalations()
    assert esc.reason is EscalationReason.OTHER
    assert "TN" in esc.details


def test_transport_failure_leaves_message_and_writes_nothing(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    pr.transport.fail_next = 1
    assert handle_contact(pr.world, _contact_msg(campaign, jur)) is False
    assert pr.store.find_thread(campaign.id, jur.id, "records@x.gov") is None
    assert pr.store.count_outbound_since(campaign.id, pr.clock.now()) == 0
    # retry succeeds
    assert handle_contact(pr.world, _contact_msg(campaign, jur)) is True


def test_send_succeeded_commit_crashed_redelivery_is_absorbed(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()

    real_record = pr.store.record_initial_send
    calls = {"n": 0}

    def crashing_record(**kwargs: object) -> object:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("db died after send")
        return real_record(**kwargs)  # type: ignore[arg-type]

    pr.store.record_initial_send = crashing_record  # type: ignore[method-assign]
    body = _contact_msg(campaign, jur)
    try:
        handle_contact(pr.world, body)
    except RuntimeError:
        pass
    assert len(pr.transport.sent) == 1  # the office got one email

    # redelivery: idempotency triple finds no thread (commit failed), so it
    # sends again — duplicate sends are harmless by the thread token rule
    assert handle_contact(pr.world, body) is True
    assert len(pr.transport.sent) == 2
    assert pr.store.find_thread(campaign.id, jur.id, "records@x.gov") is not None
    # a third delivery IS absorbed
    assert handle_contact(pr.world, body) is True
    assert len(pr.transport.sent) == 2


def test_dry_run_writes_rows_with_zero_transport_calls(pr: PrWorld) -> None:
    campaign = pr.add_campaign(dry_run=True)
    jur = pr.add_jurisdiction()
    assert handle_contact(pr.world, _contact_msg(campaign, jur)) is True
    assert pr.transport.sent == []  # Resend call skipped
    thread = pr.store.find_thread(campaign.id, jur.id, "records@x.gov")
    assert thread is not None and thread.status is ThreadStatus.REQUEST_SENT
    [email] = pr.store.list_emails(thread_id=thread.id)
    assert email.resend_id is None


def test_pending_send_thread_is_human_approved_reentry(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    assert handle_contact(pr.world, _contact_msg(campaign, jur)) is True
    thread = pr.store.find_thread(campaign.id, jur.id, "records@x.gov")
    assert thread is not None
    # park, then human resolves to pending_send
    pr.store.set_thread_status(thread.id, ThreadStatus.NEEDS_HUMAN, None)
    pr.store.set_thread_status(thread.id, ThreadStatus.PENDING_SEND, None, by_human=True)

    assert handle_contact(
        pr.world,
        _contact_msg(campaign, jur, source="human_approved", bypass=True),
    ) is True
    reloaded = pr.store.get_thread(thread.id)
    assert reloaded is not None and reloaded.status is ThreadStatus.REQUEST_SENT
    assert len(pr.transport.sent) == 2
    # same thread, same token — not a new row
    assert reloaded.thread_token == thread.thread_token


# --------------------------------------------------------------------------
# §7.2 thread jobs


def _thread(pr: PrWorld, campaign: Campaign, jur: Jurisdiction) -> int:
    handle_contact(pr.world, _contact_msg(campaign, jur, "records@x.gov"))
    thread = pr.store.find_thread(campaign.id, jur.id, "records@x.gov")
    assert thread is not None
    return thread.id


def test_followup_increments_counter_and_stale_index_dropped(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    thread_id = _thread(pr, campaign, jur)

    job = FollowupJobMessage(thread_id=thread_id, kind="followup", followup_index=0)
    assert handle_followup_job(pr.world, job.to_json()) is True
    thread = pr.store.get_thread(thread_id)
    assert thread is not None and thread.followups_sent == 1
    assert thread.status is ThreadStatus.AWAITING_REPLY
    followup = pr.store.list_emails(thread_id=thread_id)[-1]
    assert followup.kind is OutboundKind.FOLLOWUP
    assert followup.subject.startswith("Re: Public Records Request")
    assert thread.thread_token in followup.subject

    # duplicate with a stale index: dropped without sending
    sent_before = len(pr.transport.sent)
    assert handle_followup_job(pr.world, job.to_json()) is True
    assert len(pr.transport.sent) == sent_before


def test_clarification_reply_idempotent_on_inbound(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    thread_id = _thread(pr, campaign, jur)
    inbound = pr.store.insert_email(
        thread_id=thread_id, campaign_id=campaign.id, direction="inbound",
        from_address="records@x.gov", to_address=pr.world.from_address,
        subject="Which year?", body="Which year do you mean?", kind=None,
        classification=None, message_id="<q@x>", source_key="inbox/q", resend_id=None,
        in_reply_to_email_id=None, attachment_refs=[], created_at=pr.clock.now(),
    )
    job = FollowupJobMessage(
        thread_id=thread_id, kind="clarification_reply", inbound_email_id=inbound.id
    )
    assert handle_followup_job(pr.world, job.to_json()) is True
    reply = pr.store.list_emails(thread_id=thread_id)[-1]
    assert reply.kind is OutboundKind.CLARIFICATION_REPLY
    assert reply.in_reply_to_email_id == inbound.id
    assert reply.subject == f"Re: Which year? [DLG-{pr.store.get_thread(thread_id).thread_token}]"  # type: ignore[union-attr]

    # a second message for the same inbound is dropped
    sent_before = len(pr.transport.sent)
    assert handle_followup_job(pr.world, job.to_json()) is True
    assert len(pr.transport.sent) == sent_before


def test_fee_agreement_send_and_reschedule(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    thread_id = _thread(pr, campaign, jur)
    inbound = pr.store.insert_email(
        thread_id=thread_id, campaign_id=campaign.id, direction="inbound",
        from_address="records@x.gov", to_address=pr.world.from_address,
        subject="Fee due", body="The fee is $25.00.", kind=None, classification=None,
        message_id="<fee@x>", source_key="inbox/fee", resend_id=None,
        in_reply_to_email_id=None, attachment_refs=[], created_at=pr.clock.now(),
    )
    job = FollowupJobMessage(
        thread_id=thread_id, kind="fee_agreement", inbound_email_id=inbound.id,
        amount_cents=2500,
    )
    assert handle_followup_job(pr.world, job.to_json()) is True
    reply = pr.store.list_emails(thread_id=thread_id)[-1]
    assert reply.kind is OutboundKind.FEE_AGREEMENT
    assert "$25.00" in reply.body
    thread = pr.store.get_thread(thread_id)
    assert thread is not None and thread.status is ThreadStatus.AWAITING_REPLY
    assert thread.next_action_at == pr.clock.now() + timedelta(days=10)


def test_job_for_moved_on_thread_is_dropped(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    thread_id = _thread(pr, campaign, jur)
    pr.store.set_thread_status(thread_id, ThreadStatus.FULFILLED, None)
    job = FollowupJobMessage(thread_id=thread_id, kind="followup", followup_index=0)
    sent_before = len(pr.transport.sent)
    assert handle_followup_job(pr.world, job.to_json()) is True
    assert len(pr.transport.sent) == sent_before
