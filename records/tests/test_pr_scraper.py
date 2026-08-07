"""Scraper (§6): heuristics, harvest, pick gates, outcome table."""

from harvest_core.ports import SearchResult
from pr_fixtures import PrWorld
from public_records.domain import EscalationReason, SearchTarget
from public_records.messages import ContactMessage, SearchQueryMessage
from public_records.ports import ContactPick
from public_records.scraper import handle_search_message, harvest_candidates, is_official_site


def _setup(pr: PrWorld, queries: int = 1) -> tuple[int, int, SearchTarget]:
    campaign = pr.add_campaign()
    jur = pr.add_jurisdiction("Kern County")
    target = pr.store.insert_search_target(campaign.id, jur.id, pr.clock.now())
    pr.store.set_target_queries_enqueued(target.id, queries)
    return campaign.id, jur.id, target


def _msg(pr: PrWorld, campaign_id: int, jur_id: int, target: SearchTarget,
         query: str = "kern county records email", index: int = 0) -> str:
    return SearchQueryMessage(
        campaign_id=campaign_id, jurisdiction_id=jur_id,
        search_target_id=target.id, query=query, query_index=index,
    ).to_json()


PAGE = b"""
<html><body>
<p>Contact the clerk's office: records@kerncounty.gov for public records.</p>
<p>Ignore noreply@kerncounty.gov entirely.</p>
<a href="/contact-us">Contact page</a>
<a href="https://elsewhere.example.com/records">off-domain records</a>
</body></html>
"""

CONTACT_PAGE = b"""
<html><body>Deputy clerk: jsmith@kerncounty.gov (records requests)</body></html>
"""


def test_official_site_heuristic() -> None:
    assert is_official_site("https://www.kerncounty.gov/clerk")
    assert is_official_site("https://co.inyo.ca.us/records")
    assert is_official_site("https://www.kerncounty.org/foia")
    assert is_official_site("https://cityofpasadena.org/clerk")
    assert not is_official_site("https://www.yelp.com/kern-county")
    assert not is_official_site("https://facebook.com/kerncountyclerk")


def test_harvest_crawls_same_domain_links_junk_dropped(pr: PrWorld) -> None:
    pr.fetcher.fixture("https://kerncounty.gov/clerk", PAGE, content_type="text/html")
    pr.fetcher.fixture("https://kerncounty.gov/contact-us", CONTACT_PAGE,
                       content_type="text/html")
    results = [SearchResult(1, "Clerk", "snippet", "https://kerncounty.gov/clerk")]
    candidates = harvest_candidates(pr.world, results)
    emails = [c.email for c in candidates]
    assert emails == ["records@kerncounty.gov", "jsmith@kerncounty.gov"]
    # junk local-part dropped outright; off-domain link never fetched
    assert "noreply@kerncounty.gov" not in emails
    assert "https://elsewhere.example.com/records" not in pr.fetcher.calls
    # context window: chars before/after, whitespace collapsed
    assert "clerk's office" in candidates[0].context
    assert "for public records" in candidates[0].context


def test_harvest_respects_page_budget_and_candidate_cap(pr: PrWorld) -> None:
    many = "".join(f"<p>contact{i}@x.gov records</p>" for i in range(20)).encode()
    pr.fetcher.fixture("https://a.gov/records", many, content_type="text/html")
    results = [SearchResult(1, "t", "s", "https://a.gov/records")]
    candidates = harvest_candidates(pr.world, results)
    assert len(candidates) == 12  # harvesting stops at 12 per query

    # page budget: 5 distinct result urls, only 4 fetched
    pr2 = pr
    for i in range(5):
        pr2.fetcher.fixture(f"https://b{i}.gov/page", b"<p>none here</p>",
                            content_type="text/html")
    urls = [SearchResult(i, "t", "s", f"https://b{i}.gov/page") for i in range(5)]
    harvest_candidates(pr2.world, urls)
    assert sum(1 for u in pr2.fetcher.calls if u.startswith("https://b")) == 4


def test_generic_hit_writes_contact_and_enqueues(pr: PrWorld) -> None:
    cid, jid, target = _setup(pr)
    pr.search.default_results = [SearchResult(1, "Clerk", "s", "https://kerncounty.gov/clerk")]
    pr.fetcher.fixture("https://kerncounty.gov/clerk", PAGE, content_type="text/html")
    pr.picker.picks["Kern County"] = ContactPick(email="records@kerncounty.gov",
                                                 confidence=0.9)

    assert handle_search_message(pr.world, _msg(pr, cid, jid, target)) is True
    jur = pr.store.get_jurisdiction(jid)
    assert jur is not None
    assert jur.contact_email == "records@kerncounty.gov"
    assert jur.contact_verified is False
    loaded = pr.store.get_search_target(target.id)
    assert loaded is not None and loaded.resolved
    contact = ContactMessage.from_json(pr.contacts_queue.bodies()[0])
    assert contact.source == "scraper" and contact.bypass_cooldown is False
    assert pr.store.list_escalations() == []

    # duplicate delivery after resolve: deleted unworked, nothing written twice
    assert handle_search_message(pr.world, _msg(pr, cid, jid, target)) is True
    assert len(pr.contacts_queue.bodies()) == 1


def test_personal_hit_escalates_never_enqueues(pr: PrWorld) -> None:
    cid, jid, target = _setup(pr)
    pr.search.default_results = [SearchResult(1, "Clerk", "s", "https://kerncounty.gov/c")]
    pr.fetcher.fixture("https://kerncounty.gov/c", CONTACT_PAGE, content_type="text/html")
    pr.picker.picks["Kern County"] = ContactPick(email="jsmith@kerncounty.gov",
                                                 confidence=0.95)

    assert handle_search_message(pr.world, _msg(pr, cid, jid, target)) is True
    assert pr.contacts_queue.pending_count() == 0
    [esc] = pr.store.list_escalations()
    assert esc.reason is EscalationReason.CONTACT_NEEDS_REVIEW
    assert "jsmith@kerncounty.gov" in esc.details
    loaded = pr.store.get_search_target(target.id)
    assert loaded is not None and loaded.resolved
    jur = pr.store.get_jurisdiction(jid)
    assert jur is not None and jur.contact_email is None  # nothing written


def test_pick_must_be_verbatim_and_confident(pr: PrWorld) -> None:
    cid, jid, target = _setup(pr, queries=1)
    pr.search.default_results = [SearchResult(1, "Clerk", "s", "https://kerncounty.gov/c")]
    pr.fetcher.fixture("https://kerncounty.gov/c", PAGE, content_type="text/html")

    # invented address -> rejected -> last query -> no_contact_found
    pr.picker.picks["Kern County"] = ContactPick(email="invented@kerncounty.gov",
                                                 confidence=0.99)
    assert handle_search_message(pr.world, _msg(pr, cid, jid, target)) is True
    [esc] = pr.store.list_escalations()
    assert esc.reason is EscalationReason.NO_CONTACT_FOUND


def test_low_confidence_pick_rejected(pr: PrWorld) -> None:
    cid, jid, target = _setup(pr, queries=1)
    pr.search.default_results = [SearchResult(1, "Clerk", "s", "https://kerncounty.gov/c")]
    pr.fetcher.fixture("https://kerncounty.gov/c", PAGE, content_type="text/html")
    pr.picker.picks["Kern County"] = ContactPick(email="records@kerncounty.gov",
                                                 confidence=0.5)  # < 0.6 floor
    assert handle_search_message(pr.world, _msg(pr, cid, jid, target)) is True
    assert pr.contacts_queue.pending_count() == 0
    [esc] = pr.store.list_escalations()
    assert esc.reason is EscalationReason.NO_CONTACT_FOUND


def test_no_contact_found_only_after_last_outstanding_query(pr: PrWorld) -> None:
    cid, jid, target = _setup(pr, queries=3)
    # all queries miss (no results at all)
    for index in range(2):
        assert handle_search_message(
            pr.world, _msg(pr, cid, jid, target, query=f"q{index}", index=index)
        ) is True
        assert pr.store.list_escalations() == []
    assert handle_search_message(
        pr.world, _msg(pr, cid, jid, target, query="q2", index=2)
    ) is True
    [esc] = pr.store.list_escalations()
    assert esc.reason is EscalationReason.NO_CONTACT_FOUND
    # a redelivered miss after the escalation writes nothing twice
    assert handle_search_message(
        pr.world, _msg(pr, cid, jid, target, query="q1", index=1)
    ) is True
    assert len(pr.store.list_escalations()) == 1


def test_serper_429_records_nothing_and_redelivers(pr: PrWorld) -> None:
    cid, jid, target = _setup(pr)
    pr.search.rate_limit_all = True
    assert handle_search_message(pr.world, _msg(pr, cid, jid, target)) is False
    assert pr.store.list_escalations() == []
    loaded = pr.store.get_search_target(target.id)
    assert loaded is not None
    assert loaded.consumed_indexes == [] and not loaded.resolved

    # the storm passes; the same message succeeds later
    pr.search.rate_limit_all = False
    pr.search.default_results = [SearchResult(1, "Clerk", "s", "https://kerncounty.gov/c")]
    pr.fetcher.fixture("https://kerncounty.gov/c", PAGE, content_type="text/html")
    pr.picker.picks["Kern County"] = ContactPick(email="records@kerncounty.gov",
                                                 confidence=0.9)
    assert handle_search_message(pr.world, _msg(pr, cid, jid, target)) is True
    assert pr.contacts_queue.pending_count() == 1


def test_picker_api_failure_is_transient(pr: PrWorld) -> None:
    cid, jid, target = _setup(pr)
    pr.search.default_results = [SearchResult(1, "Clerk", "s", "https://kerncounty.gov/c")]
    pr.fetcher.fixture("https://kerncounty.gov/c", PAGE, content_type="text/html")
    pr.picker.fail_names.add("Kern County")
    assert handle_search_message(pr.world, _msg(pr, cid, jid, target)) is False
    assert pr.store.list_escalations() == []


def test_purged_campaign_message_dropped(pr: PrWorld) -> None:
    cid, jid, target = _setup(pr)
    pr.store.purge_campaign(cid)
    assert handle_search_message(pr.world, _msg(pr, cid, jid, target)) is True
    assert pr.contacts_queue.pending_count() == 0


def test_non_official_results_filtered_before_crawl(pr: PrWorld) -> None:
    cid, jid, target = _setup(pr, queries=1)
    pr.search.default_results = [
        SearchResult(1, "Yelp", "s", "https://yelp.com/kern"),
        SearchResult(2, "FB", "s", "https://facebook.com/kern"),
    ]
    assert handle_search_message(pr.world, _msg(pr, cid, jid, target)) is True
    assert pr.fetcher.calls == []  # nothing crawled
    [esc] = pr.store.list_escalations()
    assert esc.reason is EscalationReason.NO_CONTACT_FOUND
