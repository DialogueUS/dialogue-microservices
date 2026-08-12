"""End-to-end scenarios: both the orchestrator and all three consumers
against one shared fake world, on the virtual clock, asserting end state
AND the timing budget."""

from hashlib import sha256

from harvest_core.ports import SearchResult
from pr_fixtures import PrWorld, build_mime
from public_records.consumer import drain_dlq, process_queue
from public_records.digest import send_digests
from public_records.domain import (
    Classification,
    EscalationReason,
    InboundCategory,
    OutboundKind,
    ThreadStatus,
)
from public_records.orchestrator import followup_scan, poll_mail, seed_pass
from public_records.ports import ContactPick
from public_records.receiver import handle_inbound
from public_records.scraper import handle_search_message
from public_records.sender import handle_contact, handle_followup_job

DAY = 86_400


def drain(pr: PrWorld, rounds: int = 6) -> None:
    """Run every consumer until the queues stop moving."""
    for _ in range(rounds):
        moved = 0
        moved += process_queue(pr.world, pr.search_queue, handle_search_message)
        moved += process_queue(pr.world, pr.contacts_queue, handle_contact)
        moved += process_queue(pr.world, pr.followups_queue, handle_followup_job)
        moved += process_queue(pr.world, pr.inbound_queue, handle_inbound)
        if moved == 0:
            return


def deliver_reply(pr: PrWorld, token: str, body: str, message_id: str,
                  attachments: list[tuple[str, str, bytes]] | None = None) -> None:
    """An office reply lands in the SES mail bucket; the poller picks it
    up and the receiver reacts."""
    key = f"inbox/{message_id.strip('<>')}"
    pr.mail_bucket.put(
        key,
        build_mime(token=token, body=body, message_id=message_id, attachments=attachments),
        "message/rfc822",
    )
    assert poll_mail(pr.world) == 1
    drain(pr)


def scraper_finds(pr: PrWorld, jurisdiction_name: str, email: str) -> None:
    pr.search.default_results = [
        SearchResult(1, "Clerk", "records", "https://county.gov/clerk")
    ]
    pr.fetcher.fixture(
        "https://county.gov/clerk",
        f"<p>Public records: {email}</p>".encode(),
        content_type="text/html",
    )
    pr.picker.picks[jurisdiction_name] = ContactPick(email=email, confidence=0.9)


def test_test_campaign_runs_the_whole_loop_with_no_search_provider(pr: PrWorld) -> None:
    """A test campaign rehearses seed -> send -> reply -> fulfilled
    against a mailbox you own, with Serper (and census) never called."""
    campaign = pr.add_campaign(
        name="rehearsal",
        test_contacts=[
            {"jurisdiction": "Kern County", "state": "CA", "email": "inbox@example.test"}
        ],
    )
    # no scraper_finds(): the search provider has nothing canned, so a
    # single call would come back empty and escalate instead of sending

    seed_pass(pr.world)
    drain(pr)
    jur = pr.store.find_jurisdiction("Kern County", "CA", "county")
    assert jur is not None
    thread = pr.store.find_thread(campaign.id, jur.id, "inbox@example.test")
    assert thread is not None and thread.status is ThreadStatus.REQUEST_SENT
    [initial] = pr.transport.sent
    assert initial.to_address == "inbox@example.test"

    pdf = b"%PDF-1.4 the requested records"
    pr.classifier.canned("attached are the records", Classification(
        category=InboundCategory.DATA_PROVIDED, summary="records", confidence=0.9,
    ))
    deliver_reply(pr, thread.thread_token, "attached are the records", "<data@kern>",
                  attachments=[("records.pdf", "application/pdf", pdf)])

    final = pr.store.get_thread(thread.id)
    assert final is not None and final.status is ThreadStatus.FULFILLED
    assert final.attachment_keys == [
        f"rehearsal/kern-county/{sha256(pdf).hexdigest()[:8]}_records.pdf"
    ]
    assert pr.store.list_escalations() == []
    # the point of the whole feature
    assert pr.search.calls == []
    assert pr.query_generator.calls == []
    # and the shared office row is untouched by the rehearsal
    office = pr.store.get_jurisdiction(jur.id)
    assert office is not None
    assert office.last_contacted_at is None and office.contact_email is None


def test_happy_path_seed_to_fulfilled_with_exact_timing(pr: PrWorld) -> None:
    campaign = pr.add_campaign(notify_email="ops@example.org")
    jur = pr.add_jurisdiction("Kern County")
    scraper_finds(pr, "Kern County", "records@kerncounty.gov")

    # t0: seed -> scrape -> contact -> initial send
    seed_pass(pr.world)
    drain(pr)
    thread = pr.store.find_thread(campaign.id, jur.id, "records@kerncounty.gov")
    assert thread is not None and thread.status is ThreadStatus.REQUEST_SENT
    token = thread.thread_token
    [initial] = pr.transport.sent
    assert f"[DLG-{token}]" in initial.subject

    # the office acknowledges
    deliver_reply(pr, token, "We received your request.", "<ack@kern>")
    thread = pr.store.get_thread(thread.id)
    assert thread is not None and thread.status is ThreadStatus.AWAITING_REPLY

    # silence: exactly 10 days later the scan enqueues one follow-up
    pr.clock.advance(10 * DAY - 1)
    assert followup_scan(pr.world) == 0
    pr.clock.advance(1)
    assert followup_scan(pr.world) == 1
    drain(pr)
    followup = pr.store.list_emails(thread_id=thread.id)[-1]
    assert followup.kind is OutboundKind.FOLLOWUP

    # the office produces records
    pdf = b"%PDF-1.4 the requested records"
    pr.classifier.canned("attached are the records", Classification(
        category=InboundCategory.DATA_PROVIDED, summary="records", confidence=0.9,
    ))
    deliver_reply(pr, token, "attached are the records", "<data@kern>",
                  attachments=[("records.pdf", "application/pdf", pdf)])

    final = pr.store.get_thread(thread.id)
    assert final is not None
    assert final.status is ThreadStatus.FULFILLED and final.next_action_at is None
    digest8 = sha256(pdf).hexdigest()[:8]
    key = f"noise-2026/kern-county/{digest8}_records.pdf"
    assert final.attachment_keys == [key]
    assert pr.documents.objects[key] == pdf
    # audit trail: initial + followup outbound, ack + data inbound
    emails = pr.store.list_emails(thread_id=thread.id)
    kinds = [(e.direction.api_value, e.kind.api_value if e.kind else None) for e in emails]
    assert kinds == [
        ("outbound", "initial_request"), ("inbound", None),
        ("outbound", "followup"), ("inbound", None),
    ]
    # the follow-up scan never touches the fulfilled thread again
    pr.clock.advance(100 * DAY)
    assert followup_scan(pr.world) == 0
    assert pr.store.list_escalations() == []


def test_silence_exhaustion_escalates_at_exactly_the_budget(pr: PrWorld) -> None:
    campaign = pr.add_campaign(notify_email="ops@example.org")
    jur = pr.add_jurisdiction("Kern County", contact_email="records@kerncounty.gov")
    seed_pass(pr.world)
    drain(pr)
    thread = pr.store.find_thread(campaign.id, jur.id, "records@kerncounty.gov")
    assert thread is not None

    # three follow-ups at t0+10d, +20d, +30d — each sent, none answered
    for expected in range(1, 4):
        pr.clock.advance(10 * DAY)
        assert followup_scan(pr.world) == 1
        drain(pr)
        loaded = pr.store.get_thread(thread.id)
        assert loaded is not None and loaded.followups_sent == expected
        assert pr.store.list_escalations() == []

    # the 4th silence, at exactly t0+40d, escalates instead
    pr.clock.advance(10 * DAY)
    assert followup_scan(pr.world) == 1
    [esc] = pr.store.list_escalations()
    assert esc.reason is EscalationReason.NO_RESPONSE
    loaded = pr.store.get_thread(thread.id)
    assert loaded is not None
    assert loaded.status is ThreadStatus.NEEDS_HUMAN and loaded.next_action_at is None

    # the digest alerts exactly once
    assert send_digests(pr.world) == 1
    assert send_digests(pr.world) == 0


def test_referral_chain_child_bypasses_cooldown_and_links_parent(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction("Kern County", contact_email="records@kerncounty.gov")
    seed_pass(pr.world)
    drain(pr)
    parent = pr.store.find_thread(campaign.id, jur.id, "records@kerncounty.gov")
    assert parent is not None

    pr.classifier.canned("that is the assessor", Classification(
        category=InboundCategory.REFERRAL, summary="referral", confidence=0.9,
        referral_email="assessor@kerncounty.gov",
    ))
    # referral seconds after the initial send: the cooldown would block a
    # plain contact, but referrals bypass it
    deliver_reply(pr, parent.thread_token, "that is the assessor's office",
                  "<ref@kern>")

    reloaded = pr.store.get_thread(parent.id)
    assert reloaded is not None
    assert reloaded.status is ThreadStatus.REFERRED and reloaded.next_action_at is None
    child = pr.store.find_thread(campaign.id, jur.id, "assessor@kerncounty.gov")
    assert child is not None
    assert child.parent_thread_id == parent.id
    assert child.status is ThreadStatus.REQUEST_SENT
    assert len(pr.transport.sent) == 2


def test_consent_revoked_mid_flight_drains_inertly(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    pr.add_jurisdiction("Kern County", contact_email="records@kerncounty.gov")
    pr.add_jurisdiction("Inyo County", contact_email="records@inyocounty.gov")
    seed_pass(pr.world)
    assert pr.contacts_queue.pending_count() == 2

    # consent revoked between seeding and sending
    revoked = campaign.config.model_copy(deep=True)
    revoked.requester.consent_confirmed = False
    pr.store.update_campaign_config(campaign.id, revoked)

    drain(pr)
    assert pr.transport.sent == []  # zero sends
    assert pr.contacts_queue.pending_count() == 0  # drained, not stuck


def test_poller_and_receiver_absorb_at_least_once_delivery(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction("Kern County", contact_email="records@kerncounty.gov")
    seed_pass(pr.world)
    drain(pr)
    thread = pr.store.find_thread(campaign.id, jur.id, "records@kerncounty.gov")
    assert thread is not None

    raw = build_mime(token=thread.thread_token, body="got it", message_id="<dup@kern>")
    pr.mail_bucket.put("inbox/dup", raw, "message/rfc822")
    assert poll_mail(pr.world) == 1
    # race the same key into the queue a second time (poller crash between
    # enqueue and the receiver's commit)
    pr.inbound_queue.send(pr.inbound_queue.bodies()[0])
    drain(pr)

    inbound_rows = [e for e in pr.store.list_emails(thread_id=thread.id)
                    if e.direction.api_value == "inbound"]
    assert len(inbound_rows) == 1  # message_id dedupe absorbed the duplicate

    # and the next poll skips the ingested source_key entirely
    assert poll_mail(pr.world) == 0


def test_crashed_receiver_redelivers_and_completes_exactly_once(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction("Kern County", contact_email="records@kerncounty.gov")
    seed_pass(pr.world)
    drain(pr)
    thread = pr.store.find_thread(campaign.id, jur.id, "records@kerncounty.gov")
    assert thread is not None

    pr.classifier.fail_next = 1  # crash on first classify (before any write)
    raw = build_mime(token=thread.thread_token, body="ok", message_id="<crash@kern>")
    pr.mail_bucket.put("inbox/crash", raw, "message/rfc822")
    poll_mail(pr.world)
    process_queue(pr.world, pr.inbound_queue, handle_inbound)
    assert pr.inbound_queue.pending_count() == 1  # left for redelivery

    pr.clock.advance(901)  # visibility timeout (900 s) expires
    process_queue(pr.world, pr.inbound_queue, handle_inbound)
    assert pr.inbound_queue.pending_count() == 0
    inbound_rows = [e for e in pr.store.list_emails(thread_id=thread.id)
                    if e.direction.api_value == "inbound"]
    assert len(inbound_rows) == 1


def test_poison_inbound_message_dead_letters_into_one_escalation(pr: PrWorld) -> None:
    pr.add_campaign()
    pr.add_jurisdiction("Kern County", contact_email="records@kerncounty.gov")
    seed_pass(pr.world)
    drain(pr)

    pr.classifier.fail_next = 99  # classification permanently down
    raw = build_mime(token=None, subject="mystery",
                     message_id="<poison@kern>", from_address="records@kerncounty.gov")
    pr.mail_bucket.put("inbox/poison", raw, "message/rfc822")
    poll_mail(pr.world)

    for _ in range(3):  # maxReceiveCount for pr-inbound-mail
        process_queue(pr.world, pr.inbound_queue, handle_inbound)
        pr.clock.advance(901)
    process_queue(pr.world, pr.inbound_queue, handle_inbound)  # redrives

    assert drain_dlq(pr.world, "pr-inbound-mail", pr.inbound_dlq) == 1
    escalations = pr.store.list_escalations()
    assert len(escalations) == 1
    assert escalations[0].reason is EscalationReason.OTHER
    assert "pr-inbound-mail" in escalations[0].details


def test_fee_flow_end_to_end_inside_then_over_budget(pr: PrWorld) -> None:
    campaign = pr.add_campaign(limits={"fee_budget_usd": 30.0},
                               notify_email="ops@example.org")
    jur = pr.add_jurisdiction("Kern County", contact_email="records@kerncounty.gov")
    seed_pass(pr.world)
    drain(pr)
    thread = pr.store.find_thread(campaign.id, jur.id, "records@kerncounty.gov")
    assert thread is not None
    pr.classifier.canned("fee", Classification(
        category=InboundCategory.PAYMENT_REQUIRED, summary="fee", confidence=0.9,
    ))

    # $25 fits the $30 budget: booked, agreed, thread awaits
    deliver_reply(pr, thread.thread_token, "The copying fee is $25.00.", "<fee1@kern>")
    agreement = pr.store.list_emails(thread_id=thread.id)[-1]
    assert agreement.kind is OutboundKind.FEE_AGREEMENT and "$25.00" in agreement.body
    assert pr.store.committed_total_cents(campaign.id) == 2500

    # a second $25 fee exceeds the remaining $5: escalated, not booked
    deliver_reply(pr, thread.thread_token, "An additional fee of $25.00 applies.",
                  "<fee2@kern>")
    [esc] = pr.store.list_escalations()
    assert esc.reason is EscalationReason.PAYMENT_REQUIRED
    assert pr.store.committed_total_cents(campaign.id) == 2500
    loaded = pr.store.get_thread(thread.id)
    assert loaded is not None and loaded.status is ThreadStatus.NEEDS_HUMAN

    # the digest covers both the booked fee and the escalation, once
    assert send_digests(pr.world) == 1
    [digest] = pr.transport.sent[-1:]
    assert "payment_required: 1" in digest.body
    assert send_digests(pr.world) == 0


def test_dry_run_rehearses_whole_flow_with_zero_transport_calls(pr: PrWorld) -> None:
    campaign = pr.add_campaign(dry_run=True)
    jur = pr.add_jurisdiction("Kern County", contact_email="records@kerncounty.gov")
    seed_pass(pr.world)
    drain(pr)
    thread = pr.store.find_thread(campaign.id, jur.id, "records@kerncounty.gov")
    assert thread is not None and thread.status is ThreadStatus.REQUEST_SENT
    assert pr.transport.sent == []

    pr.clock.advance(10 * DAY)
    followup_scan(pr.world)
    drain(pr)
    loaded = pr.store.get_thread(thread.id)
    assert loaded is not None and loaded.followups_sent == 1
    assert pr.transport.sent == []  # every send skipped, all rows written
