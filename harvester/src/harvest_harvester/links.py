"""Document-link extraction from a single HTML page (spec §6.1 step 4).

Old link rules carry forward exactly: hrefs HTML-entity-unescaped
(an href is an attribute value — `A %20&amp;%20B.pdf` 404s verbatim),
absolutized against the page URL, filtered to followable extensions
(the stored set minus .xml/.json), deduplicated preserving order with
PDFs floated to the front.
"""

from __future__ import annotations

import html as html_module
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

from harvest_core.constants import FOLLOWABLE_EXTENSIONS


@dataclass
class Link:
    url: str
    anchor_text: str


class _AnchorCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.anchors: list[tuple[str, str]] = []  # (href, text)
        self._current_href: str | None = None
        self._current_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            href = dict(attrs).get("href")
            if self._current_href is not None:
                self._flush()
            self._current_href = href

    def handle_endtag(self, tag: str) -> None:
        if tag == "a":
            self._flush()

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            self._current_text.append(data)

    def _flush(self) -> None:
        if self._current_href is not None:
            self.anchors.append((self._current_href, " ".join(self._current_text).strip()))
        self._current_href = None
        self._current_text = []

    def close(self) -> None:
        super().close()
        self._flush()


def url_extension(url: str) -> str:
    path = urlsplit(url).path.lower()
    dot = path.rfind(".")
    return path[dot:] if dot != -1 else ""


def is_document_url(url: str, extensions: tuple[str, ...] = FOLLOWABLE_EXTENSIONS) -> bool:
    return url_extension(url) in extensions


def extract_document_links(page_html: str, base_url: str) -> list[Link]:
    parser = _AnchorCollector()
    parser.feed(page_html)
    parser.close()

    seen: set[str] = set()
    links: list[Link] = []
    for href, text in parser.anchors:
        if not href:
            continue
        # html.parser unescapes entities in attribute values already, but
        # unescape defensively for feeds that double-encode.
        href = html_module.unescape(href.strip())
        absolute = urljoin(base_url, href)
        if not absolute.startswith(("http://", "https://")):
            continue
        if not is_document_url(absolute):
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append(Link(url=absolute, anchor_text=text))

    # PDFs float to the front, otherwise document order is preserved.
    pdfs = [link for link in links if url_extension(link.url) == ".pdf"]
    rest = [link for link in links if url_extension(link.url) != ".pdf"]
    return pdfs + rest
