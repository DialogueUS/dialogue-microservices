"""Scrapy-Playwright PortalDiscoverer (plan 3.5).

Renders JS code viewers headlessly to *discover* native document URLs;
the rendered HTML is never emitted, and downloading stays with the
plain-HTTP fetch workers. Bounds: <= code_max_pages rendered pages,
<= 2 concurrent renders, >= 1 s delay, robots.txt obeyed.

The crawl runs in a child process (Scrapy's reactor is not restartable
in-process); candidates stream back over a queue so a deadline abort
keeps everything discovered so far. This module is exercised by the
@pytest.mark.live crawl only — parsing logic is unit-tested in
`parsing.py` without any network.
"""

from __future__ import annotations

import logging
import multiprocessing
from datetime import datetime
from typing import TYPE_CHECKING, Any

from harvest_core.constants import CODE_RENDER_CONCURRENCY, CODE_RENDER_DELAY_S
from harvest_core.domain import Publisher
from harvest_core.errors import TransientPortalError
from harvest_core.ports import Clock, DiscoveryResult, PortalCandidate

from .parsing import CapturedResponse, parse_portal_page

log = logging.getLogger(__name__)

if TYPE_CHECKING:  # static conformance only (plan 1.3 style); never at runtime
    from harvest_core.ports import PortalDiscoverer

    def _check(d: PlaywrightPortalDiscoverer) -> None:
        _p: PortalDiscoverer = d


_SENTINEL_DONE = "__done__"
_SENTINEL_ERROR = "__error__"


def _run_crawl(
    portal_urls: list[str],
    publisher_value: str,
    max_pages: int,
    out_queue: multiprocessing.Queue[Any],
) -> None:  # pragma: no cover - child process, live-only
    try:
        import scrapy
        from scrapy.crawler import CrawlerProcess

        publisher = Publisher(publisher_value)

        class PortalSpider(scrapy.Spider):  # type: ignore[misc]
            name = "portal"
            custom_settings = {
                "ROBOTSTXT_OBEY": True,
                "DOWNLOAD_DELAY": CODE_RENDER_DELAY_S,
                "CONCURRENT_REQUESTS": CODE_RENDER_CONCURRENCY,
                "CONCURRENT_REQUESTS_PER_DOMAIN": CODE_RENDER_CONCURRENCY,
                "CLOSESPIDER_PAGECOUNT": max_pages,
                "DOWNLOAD_HANDLERS": {
                    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
                    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
                },
                "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
                "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
                "LOG_LEVEL": "WARNING",
            }

            def start_requests(self):  # type: ignore[no-untyped-def]
                for url in portal_urls:
                    yield self._render_request(url)

            def _render_request(self, url: str):  # type: ignore[no-untyped-def]
                captures: list[CapturedResponse] = []

                async def on_response(response) -> None:  # type: ignore[no-untyped-def]
                    captures.append(
                        CapturedResponse(
                            url=response.url,
                            content_type=response.headers.get("content-type", ""),
                        )
                    )

                return scrapy.Request(
                    url,
                    meta={
                        "playwright": True,
                        "playwright_page_event_handlers": {"response": on_response},
                        "captures": captures,
                    },
                    callback=self.parse,
                    errback=self.on_error,
                )

            def parse(self, response):  # type: ignore[no-untyped-def]
                captures = response.meta.get("captures", [])
                parsed = parse_portal_page(publisher, response.text, response.url, captures)
                for candidate in parsed.candidates:
                    out_queue.put(("candidate", candidate.url, candidate.context))
                for link in parsed.toc_links:
                    yield self._render_request(link)

            def on_error(self, failure):  # type: ignore[no-untyped-def]
                out_queue.put((_SENTINEL_ERROR, str(failure.value), ""))

        process = CrawlerProcess(install_root_handler=False)
        process.crawl(PortalSpider)
        process.start()
        out_queue.put((_SENTINEL_DONE, "", ""))
    except Exception as exc:
        out_queue.put((_SENTINEL_ERROR, str(exc), ""))


class PlaywrightPortalDiscoverer:
    """PortalDiscoverer over a child-process Scrapy-Playwright crawl."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock

    def discover(
        self,
        portal_urls: list[str],
        publisher: Publisher,
        max_pages: int,
        deadline: datetime,
    ) -> DiscoveryResult:
        ctx = multiprocessing.get_context("spawn")
        out_queue: multiprocessing.Queue[Any] = ctx.Queue()
        proc = ctx.Process(
            target=_run_crawl,
            args=(portal_urls, publisher.value, max_pages, out_queue),
            daemon=True,
        )
        proc.start()

        candidates: list[PortalCandidate] = []
        errors: list[str] = []
        complete = False
        budget = max((deadline - self._clock.now()).total_seconds(), 1.0)
        proc.join(timeout=budget)
        timed_out = proc.is_alive()
        if timed_out:
            proc.terminate()
            proc.join(timeout=10)

        while not out_queue.empty():
            kind, a, b = out_queue.get_nowait()
            if kind == "candidate":
                candidates.append(PortalCandidate(url=a, context=b))
            elif kind == _SENTINEL_DONE:
                complete = True
            elif kind == _SENTINEL_ERROR:
                errors.append(a)

        if timed_out:
            return DiscoveryResult(
                candidates=candidates, complete=False, detail="deadline hit"
            )
        if not complete and not candidates:
            raise TransientPortalError(
                f"crawl of {portal_urls} died without completing: {errors or 'unknown'}"
            )
        return DiscoveryResult(
            candidates=candidates,
            complete=complete,
            detail="; ".join(errors) if errors else "",
        )
