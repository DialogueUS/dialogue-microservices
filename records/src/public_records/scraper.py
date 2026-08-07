"""The scraper (§6): search -> crawl -> harvest -> pick -> outcome.

Idempotency key: search_target_id — a resolved target deletes the
message unworked; every outcome write is gated on resolve_target's
compare-and-set.
"""

from __future__ import annotations

import logging
import re
from html import unescape
from urllib.parse import urljoin, urlsplit

from harvest_core.ports import SearchResult

from .constants import (
    CONTEXT_AFTER_CHARS,
    CONTEXT_BEFORE_CHARS,
    MAX_EMAIL_CANDIDATES,
    SCRAPER_PAGE_BUDGET,
    SCRAPER_PAGE_MAX_BYTES,
    SEARCH_PAGE_SIZE,
)
from .domain import EscalationReason, is_generic_address, is_junk_address
from .errors import FetchError, PickError, RateLimited, UnreachableHost
from .messages import ContactMessage, SearchQueryMessage
from .ports import EmailCandidate
from .world import World, escalate

log = logging.getLogger("public_records.scraper")

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_TAG_RE = re.compile(r"<[^>]+>")
_LINK_RE = re.compile(r"""<a\s[^>]*href=["']([^"'#]+)["'][^>]*>(.*?)</a>""",
                      re.IGNORECASE | re.DOTALL)
_RECORDS_HINTS = ("record", "contact", "clerk", "foia", "cpra", "public", "request")


def is_official_site(url: str) -> bool:
    """Government-domain heuristic, carried over from the old pipeline:
    .gov / .us, plus county/city official sites on other TLDs."""
    host = urlsplit(url).hostname or ""
    host = host.lower()
    if host.endswith(".gov") or host.endswith(".us"):
        return True
    bare = host[4:] if host.startswith("www.") else host
    return bare.startswith(("cityof", "townof", "villageof", "countyof")) or "county" in bare


def _page_text(html: str) -> str:
    return " ".join(_TAG_RE.sub(" ", unescape(html)).split())


def _records_links(html: str, base_url: str) -> list[str]:
    base_host = urlsplit(base_url).hostname
    out: list[str] = []
    for href, anchor in _LINK_RE.findall(html):
        blob = f"{href} {_TAG_RE.sub(' ', anchor)}".lower()
        if not any(hint in blob for hint in _RECORDS_HINTS):
            continue
        absolute = urljoin(base_url, unescape(href))
        if urlsplit(absolute).hostname == base_host:
            out.append(absolute)
    return out


def harvest_candidates(world: World, results: list[SearchResult]) -> list[EmailCandidate]:
    """Crawl result pages plus same-domain records/contact links, to the
    per-query page budget; regex-harvest addresses with context."""
    candidates: list[EmailCandidate] = []
    seen: set[str] = set()
    queue = [r.url for r in results]
    visited: set[str] = set()
    pages_fetched = 0

    while queue and pages_fetched < SCRAPER_PAGE_BUDGET:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        try:
            response = world.fetcher.get(url, SCRAPER_PAGE_MAX_BYTES)
        except (FetchError, UnreachableHost):
            continue  # one dead page is not a transient for the whole query
        if response.status != 200:
            continue
        pages_fetched += 1
        html = response.content.decode("utf-8", errors="replace")
        text = _page_text(html)
        for match in _EMAIL_RE.finditer(text):
            email = match.group(0).lower().rstrip(".")
            if email in seen or is_junk_address(email):
                continue
            seen.add(email)
            start, end = match.span()
            context = text[max(0, start - CONTEXT_BEFORE_CHARS): end + CONTEXT_AFTER_CHARS]
            candidates.append(EmailCandidate(email=email, context=context, source_url=url))
            if len(candidates) >= MAX_EMAIL_CANDIDATES:
                return candidates
        queue.extend(_records_links(html, url))
    return candidates


def handle_search_message(world: World, body: str) -> bool:
    msg = SearchQueryMessage.from_json(body)
    campaign = world.store.get_campaign(msg.campaign_id)
    if campaign is None:
        return True  # campaign purged: drop
    target = world.store.get_search_target(msg.search_target_id)
    if target is None or target.resolved:
        return True  # later query for an already-found contact is a no-op
    jurisdiction = world.store.get_jurisdiction(msg.jurisdiction_id)
    if jurisdiction is None:
        return True

    try:
        results = world.search.search(msg.query, SEARCH_PAGE_SIZE)
    except RateLimited:
        return False  # transient: nothing recorded, message redelivers
    except Exception as exc:
        log.warning("search failed for %r: %s", msg.query, exc)
        return False  # never escalate on a hiccup

    official = [r for r in results if is_official_site(r.url)]
    candidates = harvest_candidates(world, official)

    accepted: EmailCandidate | None = None
    if candidates:
        try:
            pick = world.picker.pick(jurisdiction, candidates)
        except PickError:
            return False  # transient
        if pick.email is not None:
            by_email = {c.email: c for c in candidates}
            chosen = by_email.get(pick.email)  # verbatim membership only
            if chosen is not None and pick.confidence >= campaign.config.contacts.min_confidence:
                accepted = chosen

    if accepted is not None:
        if is_generic_address(accepted.email):
            if world.store.resolve_target(target.id):
                world.store.set_jurisdiction_contact(
                    jurisdiction.id, accepted.email, None, accepted.source_url
                )
                world.contacts_queue.send(
                    ContactMessage(
                        campaign_id=campaign.id,
                        jurisdiction_id=jurisdiction.id,
                        contact_email=accepted.email,
                        source="scraper",
                    ).to_json()
                )
        else:
            # personal-looking mailbox: never enqueued for sending
            if world.store.resolve_target(target.id):
                escalate(
                    world,
                    campaign.id,
                    None,
                    EscalationReason.CONTACT_NEEDS_REVIEW,
                    f"Candidate contact for {jurisdiction.name}, {jurisdiction.state}: "
                    f"{accepted.email} (found at {accepted.source_url}). "
                    f"Context: {accepted.context}",
                )
        return True

    # no candidates / pick rejected: nothing written unless this was the
    # target's last outstanding query
    if world.store.mark_query_consumed(target.id, msg.query_index):
        if world.store.resolve_target(target.id):
            escalate(
                world,
                campaign.id,
                None,
                EscalationReason.NO_CONTACT_FOUND,
                f"No records-request contact found for {jurisdiction.name}, "
                f"{jurisdiction.state} after {target.queries_enqueued} search queries.",
            )
    return True
