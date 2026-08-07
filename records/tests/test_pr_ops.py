"""Operational surface (§12): resolve-to-pending_send, kill-as-purge."""

import pytest
from pr_fixtures import PrWorld
from public_records.domain import EscalationReason, EscalationStatus, ThreadStatus
from public_records.messages import ContactMessage
from public_records.ops import kill_campaign, register_campaign, resolve_escalation, set_active
from public_records.sender import handle_contact


def test_register_creates_inactive_row(pr: PrWorld) -> None:
    from pr_fixtures import campaign_config

    campaign = register_campaign(pr.world, campaign_config())
    assert campaign.active is False and campaign.seeded is False
    started = set_active(pr.world, "noise-2026", True)
    assert started.active is True
    with pytest.raises(LookupError):
        set_active(pr.world, "nope", True)


def test_resolve_to_pending_send_enqueues_human_approved(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    handle_contact(pr.world, ContactMessage(
        campaign_id=campaign.id, jurisdiction_id=jur.id,
        contact_email="records@x.gov", source="scraper",
    ).to_json())
    thread = pr.store.find_thread(campaign.id, jur.id, "records@x.gov")
    assert thread is not None
    esc = pr.store.insert_escalation(
        campaign.id, thread.id, EscalationReason.UNCLEAR_REPLY, "d", pr.clock.now()
    )
    pr.store.set_thread_status(thread.id, ThreadStatus.NEEDS_HUMAN, None)

    resolve_escalation(pr.world, esc.id, "approved after review", ThreadStatus.PENDING_SEND)
    loaded = pr.store.get_escalation(esc.id)
    assert loaded is not None and loaded.status is EscalationStatus.RESOLVED
    assert loaded.resolution == "approved after review"
    reloaded = pr.store.get_thread(thread.id)
    assert reloaded is not None and reloaded.status is ThreadStatus.PENDING_SEND
    [body] = pr.contacts_queue.bodies()
    msg = ContactMessage.from_json(body)
    assert msg.source == "human_approved" and msg.bypass_cooldown is True
    assert msg.contact_email == "records@x.gov"

    # the human-approved message re-sends on the SAME thread (pending_send
    # re-entry), skipping the review gate
    assert handle_contact(pr.world, body) is True
    final = pr.store.get_thread(thread.id)
    assert final is not None and final.status is ThreadStatus.REQUEST_SENT


def test_resolve_to_failed_is_the_only_failed_path(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    handle_contact(pr.world, ContactMessage(
        campaign_id=campaign.id, jurisdiction_id=jur.id,
        contact_email="records@x.gov", source="scraper",
    ).to_json())
    thread = pr.store.find_thread(campaign.id, jur.id, "records@x.gov")
    assert thread is not None
    esc = pr.store.insert_escalation(
        campaign.id, thread.id, EscalationReason.DENIAL, "d", pr.clock.now()
    )
    resolve_escalation(pr.world, esc.id, "give up", ThreadStatus.FAILED)
    reloaded = pr.store.get_thread(thread.id)
    assert reloaded is not None and reloaded.status is ThreadStatus.FAILED


def test_kill_campaign_purges_rows_redis_and_s3_prefix(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    handle_contact(pr.world, ContactMessage(
        campaign_id=campaign.id, jurisdiction_id=jur.id,
        contact_email="records@x.gov", source="scraper",
    ).to_json())
    pr.kv.setnx("dedupe:noise-2026:abcd1234", "noise-2026/pasadena/abcd1234_a.pdf")
    pr.documents.put("noise-2026/pasadena/abcd1234_a.pdf", b"x", "application/pdf")
    pr.documents.put("noise-2026/inbox-spool/ffff0000_0_b.pdf", b"y", "application/pdf")
    pr.documents.put("other-camp/pasadena/12345678_c.pdf", b"z", "application/pdf")

    counts = kill_campaign(pr.world, "noise-2026")
    assert counts["campaigns"] == 1 and counts["threads"] == 1
    assert counts["redis_keys"] == 1 and counts["s3_objects"] == 2
    assert pr.store.get_campaign_by_name("noise-2026") is None
    # not deleted: jurisdictions + other campaigns' objects
    assert pr.store.get_jurisdiction(jur.id) is not None
    assert "other-camp/pasadena/12345678_c.pdf" in pr.documents.objects

    # consumers drop in-flight messages whose campaign no longer exists
    stray = ContactMessage(
        campaign_id=campaign.id, jurisdiction_id=jur.id,
        contact_email="records@x.gov", source="scraper",
    ).to_json()
    assert handle_contact(pr.world, stray) is True
    assert len(pr.transport.sent) == 1  # only the original send
