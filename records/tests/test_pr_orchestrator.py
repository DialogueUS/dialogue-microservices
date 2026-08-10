"""Orchestrator concerns: seeding, mail polling, follow-up scan, digest,
single-flight scheduling."""

import threading

from pr_fixtures import PrWorld, build_mime
from public_records.digest import send_digests
from public_records.domain import EscalationReason, OutboundKind, ThreadStatus
from public_records.messages import (
    ContactMessage,
    FollowupJobMessage,
    InboundMailMessage,
    SearchQueryMessage,
)
from public_records.orchestrator import Orchestrator, followup_scan, poll_mail, seed_pass

DAY = 86_400


# --------------------------------------------------------------------------
# §5.1 seeding


def test_no_consent_no_work(pr: PrWorld) -> None:
    pr.add_campaign(requester={"name": "A", "email": "a@b", "consent_confirmed": False})
    pr.add_jurisdiction()
    assert seed_pass(pr.world) == 0
    assert pr.search_queue.pending_count() == 0
    campaign = pr.store.list_campaigns()[0]
    assert campaign.seeded is False  # not seeded either


def test_seeding_enqueues_fallback_first_and_caps_at_three(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction("Kern County")
    pr.query_generator.queries["Kern County"] = ["q1", "q2", "q3", "q4", "q5"]

    seed_pass(pr.world)

    bodies = [SearchQueryMessage.from_json(b) for b in pr.search_queue.bodies()]
    assert len(bodies) == 3  # hard cap
    assert bodies[0].query == "Kern County CA public records request CPRA email clerk"
    assert bodies[0].query_index == 0 and bodies[2].query_index == 2
    target = pr.store.find_search_target(campaign.id, jur.id)
    assert target is not None and target.queries_enqueued == 3
    assert pr.store.list_campaigns()[0].seeded is True


def test_llm_failure_degrades_to_one_fallback_query(pr: PrWorld) -> None:
    pr.add_campaign()
    pr.add_jurisdiction("Kern County")
    pr.query_generator.fail_all = True
    seed_pass(pr.world)
    bodies = [SearchQueryMessage.from_json(b) for b in pr.search_queue.bodies()]
    assert len(bodies) == 1
    assert "Kern County" in bodies[0].query


def test_seeded_contact_shortcuts_to_contacts_queue(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction("Kern County", contact_email="clerk@kerncounty.gov")
    seed_pass(pr.world)
    assert pr.search_queue.pending_count() == 0
    msg = ContactMessage.from_json(pr.contacts_queue.bodies()[0])
    assert msg.contact_email == "clerk@kerncounty.gov"
    assert msg.source == "seeded" and msg.bypass_cooldown is False
    target = pr.store.find_search_target(campaign.id, jur.id)
    assert target is not None and target.resolved is True


def test_seeding_idempotent_after_crash_mid_pass(pr: PrWorld) -> None:
    pr.add_campaign()
    pr.add_jurisdiction("Kern County")
    pr.add_jurisdiction("Inyo County")
    pr.query_generator.fail_all = True  # one fallback query per target

    # crash mid-pass: the queue dies on the 2nd jurisdiction's send
    calls = {"n": 0}

    def hook(body: str) -> None:
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("queue died")

    pr.search_queue.send_hook = hook
    try:
        seed_pass(pr.world)
    except RuntimeError:
        pass
    pr.search_queue.send_hook = None
    assert pr.store.list_campaigns()[0].seeded is False

    seed_pass(pr.world)  # re-run completes, skipping the finished target
    campaign = pr.store.list_campaigns()[0]
    assert campaign.seeded is True
    assert pr.search_queue.pending_count() == 2  # one per target, no dupes
    targets = [
        pr.store.find_search_target(campaign.id, j.id)
        for j in pr.store.list_jurisdictions()
    ]
    assert all(t is not None and t.queries_enqueued == 1 for t in targets)


def test_scope_resolution_excludes_comma_counties_and_honors_only(pr: PrWorld) -> None:
    pr.add_campaign(scope={"levels": ["county"], "states": ["CA"], "only": ["Kern County"]})
    pr.add_jurisdiction("Kern County")
    pr.add_jurisdiction("Inyo County")
    pr.add_jurisdiction("Weird County, Other County")  # comma-county: excluded
    pr.add_jurisdiction("Reno County", state="NV")  # out of state
    seed_pass(pr.world)
    bodies = [SearchQueryMessage.from_json(b) for b in pr.search_queue.bodies()]
    names = {pr.store.get_jurisdiction(b.jurisdiction_id).name for b in bodies}  # type: ignore[union-attr]
    assert names == {"Kern County"}


# --------------------------------------------------------------------------
# §5.2 mail polling


def test_poll_skips_when_queue_not_empty(pr: PrWorld) -> None:
    pr.mail_bucket.put("inbox/m1", build_mime(), "message/rfc822")
    pr.inbound_queue.send("occupied")
    assert poll_mail(pr.world) == 0
    # in-flight (received but undeleted) also blocks
    pr.inbound_queue.receive(1)
    assert poll_mail(pr.world) == 0


def test_poll_enqueues_new_mail_with_token_and_spooled_attachments(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    thread, _ = pr.store.record_initial_send(
        campaign_id=campaign.id, jurisdiction_id=jur.id, thread_token="ab" * 8,
        contact_email="clerk@pasadena.gov", parent_thread_id=None,
        existing_thread_id=None, from_address="req@x", to_address="clerk@pasadena.gov",
        subject="s", body="b", resend_id=None, next_action_at=pr.clock.now(),
        now=pr.clock.now(),
    )
    raw = build_mime(
        token="ab" * 8,
        attachments=[("report.pdf", "application/pdf", b"%PDF-1.4 data")],
    )
    pr.mail_bucket.put("inbox/m1", raw, "message/rfc822")
    pr.mail_bucket.put("inbox/AMAZON_SES_SETUP_NOTIFICATION", b"marker", "text/plain")

    assert poll_mail(pr.world) == 1
    msg = InboundMailMessage.from_json(pr.inbound_queue.bodies()[0])
    assert msg.thread_token == "ab" * 8
    assert msg.source_key == "inbox/m1"
    assert msg.message_id == "<msg-1@pasadena.gov>"
    assert msg.body.startswith("Thank you")
    [att] = msg.attachments
    assert att.s3_tmp_key.startswith("noise-2026/inbox-spool/")
    assert pr.documents.objects[att.s3_tmp_key] == b"%PDF-1.4 data"
    # mail bucket untouched (read-only poller)
    assert "inbox/m1" in pr.mail_bucket.objects


def test_poll_source_key_dedupe_and_cap(pr: PrWorld) -> None:
    pr.add_campaign()
    # an already-ingested key: present as some email's source_key
    pr.store.insert_email(
        thread_id=None, campaign_id=None, direction="inbound", from_address="a@b",
        to_address="c@d", subject="s", body="b", kind=None, classification=None,
        message_id="<old@x>", source_key="inbox/old", resend_id=None,
        in_reply_to_email_id=None, attachment_refs=[], created_at=pr.clock.now(),
    )
    pr.mail_bucket.put("inbox/old", build_mime(message_id="<old@x>"), "message/rfc822")
    for n in range(201):
        pr.mail_bucket.put(
            f"inbox/new-{n:03}", build_mime(message_id=f"<new-{n}@x>"), "message/rfc822"
        )
    assert poll_mail(pr.world) == 200  # cap per poll; the old key skipped
    keys = {InboundMailMessage.from_json(b).source_key for b in pr.inbound_queue.bodies()}
    assert "inbox/old" not in keys


def test_poll_prefers_plain_text_over_html(pr: PrWorld) -> None:
    raw = build_mime(body="plain text wins", html_body="<p>html loses</p>")
    pr.mail_bucket.put("inbox/m2", raw, "message/rfc822")
    poll_mail(pr.world)
    msg = InboundMailMessage.from_json(pr.inbound_queue.bodies()[0])
    assert "plain text wins" in msg.body and "html loses" not in msg.body


# --------------------------------------------------------------------------
# §5.3 follow-up scan


def _sent_thread(pr: PrWorld, followups_sent: int = 0) -> int:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    thread, _ = pr.store.record_initial_send(
        campaign_id=campaign.id, jurisdiction_id=jur.id, thread_token="cd" * 8,
        contact_email="clerk@pasadena.gov", parent_thread_id=None,
        existing_thread_id=None, from_address="req@x", to_address="clerk@pasadena.gov",
        subject="s", body="b", resend_id=None,
        next_action_at=pr.clock.now(), now=pr.clock.now(),
    )
    for _ in range(followups_sent):
        pr.store.record_thread_send(
            thread_id=thread.id, kind=OutboundKind.FOLLOWUP,
            from_address="req@x", to_address="clerk@pasadena.gov", subject="Re: s",
            body="nudge", resend_id=None, in_reply_to_email_id=None,
            new_status=ThreadStatus.AWAITING_REPLY, next_action_at=pr.clock.now(),
            increment_followups=True, now=pr.clock.now(),
        )
    return thread.id


def test_scan_enqueues_once_and_reschedules_at_enqueue_time(pr: PrWorld) -> None:
    thread_id = _sent_thread(pr)
    assert followup_scan(pr.world) == 1
    [body] = pr.followups_queue.bodies()
    job = FollowupJobMessage.from_json(body)
    assert job.kind == "followup" and job.followup_index == 0

    # sender is stalled; the next scan must NOT double-enqueue
    assert followup_scan(pr.world) == 0
    thread = pr.store.get_thread(thread_id)
    assert thread is not None and thread.next_action_at is not None
    # rescheduled exactly one interval (10 days) out
    assert (thread.next_action_at - pr.clock.now()).total_seconds() == 10 * DAY

    # not due at 10 days minus a second; due at 10 days
    pr.clock.advance(10 * DAY - 1)
    assert followup_scan(pr.world) == 0
    pr.clock.advance(1)
    assert followup_scan(pr.world) == 1


def test_scan_escalates_no_response_after_max_followups(pr: PrWorld) -> None:
    thread_id = _sent_thread(pr, followups_sent=3)
    # make it due again
    pr.store.set_thread_status(thread_id, ThreadStatus.AWAITING_REPLY, pr.clock.now())
    assert followup_scan(pr.world) == 1
    assert pr.followups_queue.pending_count() == 0
    [esc] = pr.store.list_escalations()
    assert esc.reason is EscalationReason.NO_RESPONSE
    assert "3 follow-ups" in esc.details
    thread = pr.store.get_thread(thread_id)
    assert thread is not None
    assert thread.status is ThreadStatus.NEEDS_HUMAN and thread.next_action_at is None
    # parked: never selected again
    pr.clock.advance(100 * DAY)
    assert followup_scan(pr.world) == 0


# --------------------------------------------------------------------------
# §12 digest


def test_digest_notifies_each_row_exactly_once(pr: PrWorld) -> None:
    campaign = pr.add_campaign(
        notify_email="ops@example.org", limits={"fee_budget_usd": 10.0}
    )
    pr.store.insert_escalation(
        campaign.id, None, EscalationReason.DENIAL, "denied flatly", pr.clock.now()
    )
    pr.store.book_fee(campaign.id, None, 1000, 1000, "fee note", pr.clock.now())

    assert send_digests(pr.world) == 1
    [email] = pr.transport.sent
    assert email.to_address == "ops@example.org"
    assert email.headers["X-Dialogue-Token"] == "notify"
    assert email.headers["X-Dialogue-Kind"] == "alert"
    assert "denial: 1" in email.body
    assert "$10.00 committed of $10.00" in email.body
    assert "budget EXHAUSTED" in email.body

    # second period: nothing un-notified, no send
    assert send_digests(pr.world) == 0


def test_digest_transport_failure_retries_next_period(pr: PrWorld) -> None:
    campaign = pr.add_campaign(notify_email="ops@example.org")
    pr.store.insert_escalation(
        campaign.id, None, EscalationReason.OTHER, "d", pr.clock.now()
    )
    pr.transport.fail_next = 1
    assert send_digests(pr.world) == 0
    assert len(pr.store.unnotified_escalations(campaign.id)) == 1
    assert send_digests(pr.world) == 1
    assert pr.store.unnotified_escalations(campaign.id) == []


def test_digest_skips_without_notify_email(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    pr.store.insert_escalation(
        campaign.id, None, EscalationReason.OTHER, "d", pr.clock.now()
    )
    assert send_digests(pr.world) == 0


# --------------------------------------------------------------------------
# scheduler


def test_concern_in_flight_is_skipped_silently(pr: PrWorld) -> None:
    pr.add_campaign()
    pr.add_jurisdiction()
    orch = Orchestrator(pr.world)
    # simulate a stuck seeding pass by holding its single-flight lock
    assert orch._locks["seed"].acquire(blocking=False)
    try:
        orch.tick()
        assert pr.search_queue.pending_count() == 0  # seed skipped
    finally:
        orch._locks["seed"].release()
    orch.tick()
    assert pr.search_queue.pending_count() > 0


def test_inactive_campaign_produces_no_new_work(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    pr.add_jurisdiction()
    pr.store.set_campaign_active(campaign.id, False)
    Orchestrator(pr.world).tick()
    assert pr.search_queue.pending_count() == 0


def test_concurrent_ticks_do_not_double_seed(pr: PrWorld) -> None:
    pr.add_campaign()
    pr.add_jurisdiction()
    pr.query_generator.fail_all = True  # one fallback query per target
    orch = Orchestrator(pr.world)
    threads = [threading.Thread(target=orch.tick) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # single-flight + the queries_enqueued skip marker: exactly one query set
    assert pr.search_queue.pending_count() == 1


# --------------------------------------------------------------------------
# §3.1 census-seeded jurisdictions


class _FakeCensus:
    def __init__(self) -> None:
        from harvest_core.ports import CensusPlace, CensusState

        self.loads: list[str] = []
        self._states = {
            "CA": CensusState("California", [
                CensusPlace("Kern County", "county"),
                CensusPlace("Bakersfield", "city", parent_name="Kern County"),
                CensusPlace("Weird County, Other County", "county"),
            ]),
            "HI": CensusState("Hawaii", []),  # zero places
        }

    def load_state(self, state: str):  # type: ignore[no-untyped-def]
        self.loads.append(state)
        return self._states[state]


def test_census_seeding_idempotent_with_state_marker(pr: PrWorld) -> None:
    census = _FakeCensus()
    pr.world.census = census  # type: ignore[assignment]
    pr.add_campaign(scope={"levels": ["county"], "states": ["CA"]})

    seed_pass(pr.world)
    names = {j.name for j in pr.store.list_jurisdictions()}
    assert {"California", "Kern County", "Bakersfield",
            "Weird County, Other County"} <= names
    # comma-county retained in the table but excluded from targets
    bodies = [SearchQueryMessage.from_json(b) for b in pr.search_queue.bodies()]
    target_names = {pr.store.get_jurisdiction(b.jurisdiction_id).name for b in bodies}  # type: ignore[union-attr]
    assert target_names == {"Kern County"}

    # second campaign over the same state: the state-row marker prevents
    # a reload; a zero-place state is loaded once and never again
    pr.add_campaign(name="second", scope={"levels": ["county"], "states": ["CA", "HI"]})
    seed_pass(pr.world)
    seed_pass(pr.world)
    assert census.loads == ["CA", "HI"]
