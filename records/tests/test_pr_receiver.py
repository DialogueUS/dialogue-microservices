"""Receiver (§8): match, classify, log-before-react, the reaction table,
attachment storage (§8.1), and the fee flow (§9)."""

from datetime import timedelta
from hashlib import sha256

from pr_fixtures import PrWorld
from public_records.attachments import rebuild_dedupe
from public_records.domain import (
    Campaign,
    Classification,
    EmailThread,
    EscalationReason,
    InboundCategory,
    ThreadStatus,
)
from public_records.fees import parse_largest_amount_cents
from public_records.messages import (
    ContactMessage,
    FollowupJobMessage,
    InboundAttachment,
    InboundMailMessage,
)
from public_records.receiver import handle_inbound
from public_records.sender import handle_contact


def _thread(pr: PrWorld, jur_name: str = "Pasadena",
            **campaign_overrides: object) -> tuple[Campaign, EmailThread]:
    campaign = pr.add_campaign(**campaign_overrides)  # type: ignore[arg-type]
    jur = pr.add_jurisdiction(jur_name)
    handle_contact(pr.world, ContactMessage(
        campaign_id=campaign.id, jurisdiction_id=jur.id,
        contact_email="records@x.gov", source="scraper",
    ).to_json())
    thread = pr.store.find_thread(campaign.id, jur.id, "records@x.gov")
    assert thread is not None
    return campaign, thread


def _inbound(pr: PrWorld, thread: EmailThread | None, body: str = "hello",
             subject: str = "Re: request", message_id: str = "<m1@x>",
             source_key: str = "inbox/m1", from_address: str = "records@x.gov",
             attachments: list[InboundAttachment] | None = None,
             use_token: bool = True) -> str:
    return InboundMailMessage(
        source_key=source_key, message_id=message_id,
        thread_token=thread.thread_token if (thread and use_token) else None,
        from_address=from_address, subject=subject, body=body,
        attachments=attachments or [],
    ).to_json()


def _spool(pr: PrWorld, campaign: Campaign, filename: str, content_type: str,
           data: bytes, index: int = 0) -> InboundAttachment:
    digest = sha256(data).hexdigest()[:8]
    key = f"{campaign.name}/inbox-spool/{digest}_{index}_{filename}"
    pr.documents.put(key, data, content_type)
    return InboundAttachment(filename=filename, content_type=content_type, s3_tmp_key=key)


def _classify_as(pr: PrWorld, needle: str, category: InboundCategory,
                 referral_email: str | None = None) -> None:
    pr.classifier.canned(needle, Classification(
        category=category, summary="s", confidence=0.8, referral_email=referral_email,
    ))


# --------------------------------------------------------------------------
# matching & dedupe


def test_duplicate_message_id_deleted_unworked(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    assert handle_inbound(pr.world, _inbound(pr, thread)) is True
    emails_before = len(pr.store.list_emails())
    assert handle_inbound(pr.world, _inbound(pr, thread, source_key="inbox/m1-copy")) is True
    assert len(pr.store.list_emails()) == emails_before


def test_token_match_beats_sender_address(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    other = pr.add_campaign(name="other")
    jur2 = pr.add_jurisdiction("Inyo County")
    handle_contact(pr.world, ContactMessage(
        campaign_id=other.id, jurisdiction_id=jur2.id,
        contact_email="records@x.gov", source="scraper", bypass_cooldown=True,
    ).to_json())
    # mail from the shared address but carrying thread's token
    assert handle_inbound(pr.world, _inbound(pr, thread)) is True
    [email] = [e for e in pr.store.list_emails() if e.message_id == "<m1@x>"]
    assert email.thread_id == thread.id


def test_unmatched_mail_is_a_thread_less_row(pr: PrWorld) -> None:
    pr.add_campaign()
    body = InboundMailMessage(
        source_key="inbox/stray", message_id="<stray@x>", thread_token=None,
        from_address="somebody@nowhere.gov", subject="hi", body="stray mail",
    ).to_json()
    assert handle_inbound(pr.world, body) is True
    [email] = pr.store.list_emails()
    assert email.thread_id is None and email.campaign_id is None
    assert email.source_key == "inbox/stray"
    assert pr.store.list_escalations() == []  # dead end, no reaction


def test_classifier_transient_failure_leaves_message(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    pr.classifier.fail_next = 1
    assert handle_inbound(pr.world, _inbound(pr, thread)) is False
    assert [e for e in pr.store.list_emails() if e.message_id == "<m1@x>"] == []
    assert handle_inbound(pr.world, _inbound(pr, thread)) is True  # retry works


# --------------------------------------------------------------------------
# the reaction table


def test_acknowledgment_reschedules(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    assert handle_inbound(pr.world, _inbound(pr, thread, body="we got it")) is True
    reloaded = pr.store.get_thread(thread.id)
    assert reloaded is not None and reloaded.status is ThreadStatus.AWAITING_REPLY
    assert reloaded.next_action_at == pr.clock.now() + timedelta(days=10)
    [email] = [e for e in pr.store.list_emails() if e.message_id == "<m1@x>"]
    assert email.classification is not None
    assert email.classification.category is InboundCategory.ACKNOWLEDGMENT


def test_data_provided_stores_attachments_and_fulfills(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    _classify_as(pr, "records attached", InboundCategory.DATA_PROVIDED)
    att = _spool(pr, campaign, "report.pdf", "application/pdf", b"%PDF-1.4 body")
    assert handle_inbound(
        pr.world, _inbound(pr, thread, body="records attached", attachments=[att])
    ) is True

    reloaded = pr.store.get_thread(thread.id)
    assert reloaded is not None
    assert reloaded.status is ThreadStatus.FULFILLED and reloaded.next_action_at is None
    [key] = reloaded.attachment_keys
    assert key == f"noise-2026/pasadena/{sha256(b'%PDF-1.4 body').hexdigest()[:8]}_report.pdf"
    assert pr.documents.objects[key] == b"%PDF-1.4 body"
    assert att.s3_tmp_key not in pr.documents.objects  # spool cleaned after commit
    [email] = [e for e in pr.store.list_emails() if e.message_id == "<m1@x>"]
    assert email.attachment_refs[0]["status"] == "stored"


def test_data_provided_fulfills_even_with_zero_stored(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    _classify_as(pr, "see links", InboundCategory.DATA_PROVIDED)
    assert handle_inbound(pr.world, _inbound(pr, thread, body="see links inline")) is True
    reloaded = pr.store.get_thread(thread.id)
    assert reloaded is not None and reloaded.status is ThreadStatus.FULFILLED


def test_acknowledgment_with_stored_attachment_fulfills(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    att = _spool(pr, campaign, "data.csv", "text/csv", b"a,b\n1,2\n")
    assert handle_inbound(pr.world, _inbound(pr, thread, attachments=[att])) is True
    reloaded = pr.store.get_thread(thread.id)
    assert reloaded is not None and reloaded.status is ThreadStatus.FULFILLED


def test_referral_with_address_reenqueues_and_refers(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    _classify_as(pr, "contact the county", InboundCategory.REFERRAL,
                 referral_email="county@elsewhere.gov")
    assert handle_inbound(pr.world, _inbound(pr, thread, body="contact the county")) is True

    reloaded = pr.store.get_thread(thread.id)
    assert reloaded is not None
    assert reloaded.status is ThreadStatus.REFERRED and reloaded.next_action_at is None
    [msg_body] = pr.contacts_queue.bodies()
    contact = ContactMessage.from_json(msg_body)
    assert contact.contact_email == "county@elsewhere.gov"
    assert contact.source == "referral" and contact.bypass_cooldown is True
    assert contact.parent_thread_id == thread.id
    assert contact.jurisdiction_id == thread.jurisdiction_id

    # the sender records the lineage on the new thread
    assert handle_contact(pr.world, msg_body) is True
    child = pr.store.find_thread(campaign.id, thread.jurisdiction_id, "county@elsewhere.gov")
    assert child is not None and child.parent_thread_id == thread.id


def test_referral_without_address_escalates(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    _classify_as(pr, "not us", InboundCategory.REFERRAL)
    assert handle_inbound(pr.world, _inbound(pr, thread, body="not us, ask elsewhere")) is True
    [esc] = pr.store.list_escalations()
    assert esc.reason is EscalationReason.REFERRAL_NO_ADDRESS
    reloaded = pr.store.get_thread(thread.id)
    assert reloaded is not None and reloaded.status is ThreadStatus.NEEDS_HUMAN


def test_denial_escalates(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    _classify_as(pr, "denied", InboundCategory.DENIAL)
    assert handle_inbound(pr.world, _inbound(pr, thread, body="denied per exemption")) is True
    [esc] = pr.store.list_escalations()
    assert esc.reason is EscalationReason.DENIAL
    reloaded = pr.store.get_thread(thread.id)
    assert reloaded is not None
    assert reloaded.status is ThreadStatus.NEEDS_HUMAN and reloaded.next_action_at is None


def test_needs_clarification_enqueues_reply_job(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    _classify_as(pr, "which year", InboundCategory.NEEDS_CLARIFICATION)
    assert handle_inbound(pr.world, _inbound(pr, thread, body="which year exactly?")) is True
    [job_body] = pr.followups_queue.bodies()
    job = FollowupJobMessage.from_json(job_body)
    assert job.kind == "clarification_reply"
    [email] = [e for e in pr.store.list_emails() if e.message_id == "<m1@x>"]
    assert job.inbound_email_id == email.id
    reloaded = pr.store.get_thread(thread.id)
    assert reloaded is not None and reloaded.status is ThreadStatus.AWAITING_REPLY


def test_unclear_escalates_with_truncated_body(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    _classify_as(pr, "gibberish", InboundCategory.UNCLEAR)
    long_body = "gibberish " + "x" * 5000
    assert handle_inbound(pr.world, _inbound(pr, thread, body=long_body)) is True
    [esc] = pr.store.list_escalations()
    assert esc.reason is EscalationReason.UNCLEAR_REPLY
    assert len(esc.details) <= 2000


def test_terminal_threads_never_rescanned(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    _classify_as(pr, "records attached", InboundCategory.DATA_PROVIDED)
    handle_inbound(pr.world, _inbound(pr, thread, body="records attached"))
    from public_records.orchestrator import followup_scan

    pr.clock.advance(365 * 86_400)
    assert followup_scan(pr.world) == 0


# --------------------------------------------------------------------------
# §8.1 attachment storage details


def test_type_gate_rejections_recorded(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    _classify_as(pr, "files", InboundCategory.DATA_PROVIDED)
    good = _spool(pr, campaign, "data.zip", "application/zip", b"PK\x03\x04", 0)
    image = _spool(pr, campaign, "photo.png", "image/png", b"\x89PNG", 1)
    invite = _spool(pr, campaign, "meet.ics", "text/calendar", b"BEGIN:VCALENDAR", 2)
    assert handle_inbound(
        pr.world, _inbound(pr, thread, body="files", attachments=[good, image, invite])
    ) is True
    [email] = [e for e in pr.store.list_emails() if e.message_id == "<m1@x>"]
    statuses = {r["filename"]: r["status"] for r in email.attachment_refs}
    assert statuses == {"data.zip": "stored", "photo.png": "rejected", "meet.ics": "rejected"}
    reasons = {r["filename"]: r.get("reason", "") for r in email.attachment_refs}
    assert "unsupported type" in reasons["photo.png"]


def test_dedupe_same_campaign_skips_upload_cross_campaign_stores(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    _classify_as(pr, "files", InboundCategory.DATA_PROVIDED)
    data = b"%PDF-1.4 identical"
    att1 = _spool(pr, campaign, "a.pdf", "application/pdf", data, 0)
    att2 = _spool(pr, campaign, "b.pdf", "application/pdf", data, 1)
    assert handle_inbound(
        pr.world, _inbound(pr, thread, body="files", attachments=[att1, att2])
    ) is True
    [email] = [e for e in pr.store.list_emails() if e.message_id == "<m1@x>"]
    st = [r["status"] for r in email.attachment_refs]
    assert st == ["stored", "duplicate"]
    # the duplicate ref points at the existing key
    assert email.attachment_refs[1]["key"] == email.attachment_refs[0]["key"]
    stored_keys = [k for k in pr.documents.objects if "/inbox-spool/" not in k]
    assert len(stored_keys) == 1

    # two campaigns receiving the same file each store their own copy
    other_campaign, other_thread = _thread(pr, jur_name="Inyo County", name="other-camp")
    att3 = _spool(pr, other_campaign, "a.pdf", "application/pdf", data, 0)
    assert handle_inbound(
        pr.world,
        _inbound(pr, other_thread, body="files", attachments=[att3],
                 message_id="<m2@x>", source_key="inbox/m2"),
    ) is True
    stored_keys = [k for k in pr.documents.objects if "/inbox-spool/" not in k]
    assert len(stored_keys) == 2


def test_redis_flush_then_rebuild_from_listing(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)
    _classify_as(pr, "files", InboundCategory.DATA_PROVIDED)
    data = b"%PDF-1.4 original"
    att = _spool(pr, campaign, "a.pdf", "application/pdf", data, 0)
    handle_inbound(pr.world, _inbound(pr, thread, body="files", attachments=[att]))

    pr.kv.flush()  # ElastiCache failover
    assert rebuild_dedupe(pr.world, campaign) == 1
    # the same bytes arriving again are recognized as duplicates
    att2 = _spool(pr, campaign, "again.pdf", "application/pdf", data, 0)
    handle_inbound(pr.world, _inbound(pr, thread, body="files", attachments=[att2],
                                      message_id="<m3@x>", source_key="inbox/m3"))
    email = [e for e in pr.store.list_emails() if e.message_id == "<m3@x>"][0]
    assert email.attachment_refs[0]["status"] == "duplicate"


# --------------------------------------------------------------------------
# §9 the fee flow


def test_parse_largest_amount() -> None:
    assert parse_largest_amount_cents("the fee is $25.00 (deposit $5)") == 2500
    assert parse_largest_amount_cents("copying: $1,234.56 total") == 123456
    assert parse_largest_amount_cents("costs $999999") == 99999900
    assert parse_largest_amount_cents("$1234567 is absurd") is None  # > 6 digits
    assert parse_largest_amount_cents("fees may apply") is None
    assert parse_largest_amount_cents("$0.50 per page, $12 minimum") == 1200


def test_fee_within_budget_books_ledger_before_enqueue(pr: PrWorld) -> None:
    campaign, thread = _thread(pr, limits={"fee_budget_usd": 50.0})
    _classify_as(pr, "fee", InboundCategory.PAYMENT_REQUIRED)
    assert handle_inbound(
        pr.world,
        _inbound(pr, thread, body="Please pay the fee of $25.00 to proceed."),
    ) is True

    [entry] = pr.store.list_spend(campaign.id)
    assert entry.amount_cents == 2500 and entry.remitted is False
    assert entry.note.startswith("Please pay the fee")
    [job_body] = pr.followups_queue.bodies()
    job = FollowupJobMessage.from_json(job_body)
    assert job.kind == "fee_agreement" and job.amount_cents == 2500
    reloaded = pr.store.get_thread(thread.id)
    assert reloaded is not None and reloaded.status is ThreadStatus.AWAITING_REPLY
    assert pr.store.list_escalations() == []


def test_fee_over_budget_escalates_stating_both(pr: PrWorld) -> None:
    campaign, thread = _thread(pr, limits={"fee_budget_usd": 10.0})
    _classify_as(pr, "fee", InboundCategory.PAYMENT_REQUIRED)
    handle_inbound(pr.world, _inbound(pr, thread, body="The fee is $25.00."))
    [esc] = pr.store.list_escalations()
    assert esc.reason is EscalationReason.PAYMENT_REQUIRED
    assert "$25.00" in esc.details and "$10.00" in esc.details
    assert pr.store.list_spend(campaign.id) == []
    assert pr.followups_queue.pending_count() == 0
    reloaded = pr.store.get_thread(thread.id)
    assert reloaded is not None and reloaded.status is ThreadStatus.NEEDS_HUMAN


def test_fee_with_zero_budget_escalates(pr: PrWorld) -> None:
    campaign, thread = _thread(pr)  # default budget 0
    _classify_as(pr, "fee", InboundCategory.PAYMENT_REQUIRED)
    handle_inbound(pr.world, _inbound(pr, thread, body="The fee is $5.00."))
    [esc] = pr.store.list_escalations()
    assert "no fee budget" in esc.details


def test_fee_without_amount_escalates(pr: PrWorld) -> None:
    campaign, thread = _thread(pr, limits={"fee_budget_usd": 50.0})
    _classify_as(pr, "payment", InboundCategory.PAYMENT_REQUIRED)
    handle_inbound(pr.world, _inbound(pr, thread, body="payment will be needed"))
    [esc] = pr.store.list_escalations()
    assert "no clear amount" in esc.details
