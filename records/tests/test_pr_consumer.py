"""Consumer framework: commit-then-delete, redelivery, DLQ escalation."""

from pr_fixtures import PrWorld
from public_records.consumer import drain_dlq, process_queue
from public_records.domain import EscalationReason, ThreadStatus
from public_records.messages import ContactMessage, FollowupJobMessage
from public_records.sender import handle_contact
from public_records.world import World


def test_raising_handler_leaves_message_for_redelivery(pr: PrWorld) -> None:
    pr.search_queue.send("payload")
    attempts = {"n": 0}

    def handler(world: World, body: str) -> bool:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("boom")
        return True

    assert process_queue(pr.world, pr.search_queue, handler) == 1
    assert pr.search_queue.pending_count() == 1  # left undeleted
    pr.clock.advance(901)  # visibility expires
    assert process_queue(pr.world, pr.search_queue, handler) == 1
    assert pr.search_queue.pending_count() == 0
    assert attempts["n"] == 2


def test_false_handler_retries_until_dlq_and_exactly_one_escalation(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    pr.search_queue.send(f'{{"campaign_id": {campaign.id}, "query": "q"}}')

    def never_done(world: World, body: str) -> bool:
        return False

    for _ in range(3):  # maxReceiveCount for pr-search-queries
        process_queue(pr.world, pr.search_queue, never_done)
        pr.clock.advance(901)
    # 4th receive redrives to the DLQ instead of delivering
    assert process_queue(pr.world, pr.search_queue, never_done) == 0

    assert drain_dlq(pr.world, "pr-search-queries", pr.search_dlq) == 1
    [esc] = pr.store.list_escalations()
    assert esc.reason is EscalationReason.OTHER
    assert esc.campaign_id == campaign.id
    assert "pr-search-queries" in esc.details
    # exactly once: draining again does nothing
    assert drain_dlq(pr.world, "pr-search-queries", pr.search_dlq) == 0
    assert len(pr.store.list_escalations()) == 1


def test_dlq_escalation_parks_named_thread(pr: PrWorld) -> None:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction()
    handle_contact(pr.world, ContactMessage(
        campaign_id=campaign.id, jurisdiction_id=jur.id,
        contact_email="records@x.gov", source="scraper",
    ).to_json())
    thread = pr.store.find_thread(campaign.id, jur.id, "records@x.gov")
    assert thread is not None

    job = FollowupJobMessage(thread_id=thread.id, kind="followup", followup_index=0)
    pr.followups_dlq.send(job.to_json())
    drain_dlq(pr.world, "pr-followups", pr.followups_dlq)
    [esc] = pr.store.list_escalations()
    assert esc.thread_id == thread.id and esc.campaign_id == campaign.id
    reloaded = pr.store.get_thread(thread.id)
    assert reloaded is not None and reloaded.status is ThreadStatus.NEEDS_HUMAN
