"""Per-publisher parsing/capture logic (plan 3.5) — pure functions.

Each strategy turns one rendered portal page (HTML + the JSON responses
Playwright observed while rendering it) into: native document
candidates (PDF exports, JSON content endpoints — never the rendered
HTML itself) and TOC links to render next.
"""

from __future__ import annotations

import html as html_module
from dataclasses import dataclass, field
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

from harvest_core.domain import Publisher
from harvest_core.ports import PortalCandidate

from ..links import url_extension


@dataclass
class CapturedResponse:
    """A network response observed during render (HAR-style)."""

    url: str
    content_type: str


@dataclass
class PortalParse:
    candidates: list[PortalCandidate] = field(default_factory=list)
    toc_links: list[str] = field(default_factory=list)


class _AnchorScan(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.anchors: list[tuple[str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            self._flush()
            self._href = dict(attrs).get("href")

    def handle_endtag(self, tag: str) -> None:
        if tag == "a":
            self._flush()

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def _flush(self) -> None:
        if self._href is not None:
            text = " ".join(" ".join(self._text).split())
            self.anchors.append((self._href, text))
        self._href = None
        self._text = []

    def close(self) -> None:
        super().close()
        self._flush()


def _anchors(page_html: str, base_url: str) -> list[tuple[str, str]]:
    scan = _AnchorScan()
    scan.feed(page_html)
    scan.close()
    out = []
    for href, text in scan.anchors:
        if not href:
            continue
        absolute = urljoin(base_url, html_module.unescape(href.strip()))
        if absolute.startswith(("http://", "https://")):
            out.append((absolute, text))
    return out


def _context(portal_url: str, heading: str) -> str:
    return f"{portal_url}\n{heading}" if heading else portal_url


def _json_captures(
    captures: list[CapturedResponse], host_fragment: str, path_fragment: str = ""
) -> list[str]:
    urls = []
    for cap in captures:
        if "json" not in cap.content_type.lower():
            continue
        parts = urlsplit(cap.url)
        if host_fragment not in parts.netloc.lower():
            continue
        if path_fragment and path_fragment not in parts.path.lower():
            continue
        urls.append(cap.url)
    return urls


def _dedupe(items: list[PortalCandidate]) -> list[PortalCandidate]:
    seen: set[str] = set()
    out = []
    for c in items:
        if c.url not in seen:
            seen.add(c.url)
            out.append(c)
    return out


def parse_portal_page(
    publisher: Publisher,
    page_html: str,
    page_url: str,
    captures: list[CapturedResponse],
) -> PortalParse:
    if publisher == Publisher.MUNICODE:
        return _parse_municode(page_html, page_url, captures)
    if publisher == Publisher.AMLEGAL:
        return _parse_amlegal(page_html, page_url, captures)
    if publisher == Publisher.ECODE360:
        return _parse_ecode360(page_html, page_url, captures)
    return _parse_other(page_html, page_url, captures)


def _parse_municode(
    page_html: str, page_url: str, captures: list[CapturedResponse]
) -> PortalParse:
    parse = PortalParse()
    for url, text in _anchors(page_html, page_url):
        lower = url.lower()
        if url_extension(url) == ".pdf" or "exportpdf" in lower or "export/pdf" in lower:
            parse.candidates.append(PortalCandidate(url, _context(page_url, text)))
        elif "library.municode.com" in lower and "/codes/" in lower and url != page_url:
            parse.toc_links.append(url)
    for cap_url in _json_captures(captures, "api.municode.com", "content"):
        parse.candidates.append(PortalCandidate(cap_url, _context(page_url, "content API")))
    parse.candidates = _dedupe(parse.candidates)
    return parse


def _parse_amlegal(
    page_html: str, page_url: str, captures: list[CapturedResponse]
) -> PortalParse:
    parse = PortalParse()
    for url, text in _anchors(page_html, page_url):
        lower = url.lower()
        if url_extension(url) == ".pdf" or "/pdf/" in lower or "requestpdf" in lower:
            parse.candidates.append(PortalCandidate(url, _context(page_url, text)))
        elif "codelibrary.amlegal.com" in lower and "/codes/" in lower and url != page_url:
            parse.toc_links.append(url)
    for cap_url in _json_captures(captures, "amlegal.com", "/api/"):
        parse.candidates.append(PortalCandidate(cap_url, _context(page_url, "content API")))
    parse.candidates = _dedupe(parse.candidates)
    return parse


def _parse_ecode360(
    page_html: str, page_url: str, captures: list[CapturedResponse]
) -> PortalParse:
    parse = PortalParse()
    portal_host = urlsplit(page_url).netloc.lower()
    for url, text in _anchors(page_html, page_url):
        lower = url.lower()
        if url_extension(url) == ".pdf" or "/attachment/" in lower or "/print/" in lower:
            parse.candidates.append(PortalCandidate(url, _context(page_url, text)))
        elif urlsplit(url).netloc.lower() == portal_host and url != page_url:
            parse.toc_links.append(url)
    for cap_url in _json_captures(captures, "ecode360.com"):
        parse.candidates.append(PortalCandidate(cap_url, _context(page_url, "content API")))
    parse.candidates = _dedupe(parse.candidates)
    return parse


def _parse_other(
    page_html: str, page_url: str, captures: list[CapturedResponse]
) -> PortalParse:
    """Generic strategy: any followable document-extension link plus
    captured JSON content endpoints from the rendered page. Page-linked
    .xml/.json stay excluded — structured content enters only through
    the captured endpoints, which we asked for by rendering."""
    from harvest_core.constants import FOLLOWABLE_EXTENSIONS

    parse = PortalParse()
    portal_host = urlsplit(page_url).netloc.lower()
    for url, text in _anchors(page_html, page_url):
        if url_extension(url) in FOLLOWABLE_EXTENSIONS:
            parse.candidates.append(PortalCandidate(url, _context(page_url, text)))
        elif urlsplit(url).netloc.lower() == portal_host and url != page_url:
            parse.toc_links.append(url)
    for cap in captures:
        if "json" in cap.content_type.lower():
            parse.candidates.append(
                PortalCandidate(cap.url, _context(page_url, "content API"))
            )
    parse.candidates = _dedupe(parse.candidates)
    return parse
