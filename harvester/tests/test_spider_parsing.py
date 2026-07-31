"""Plan 3.5: per-publisher parsing/capture logic against fixtures. No network."""

import pytest
from harvest_core.domain import Publisher
from harvest_harvester.spiders.parsing import CapturedResponse, parse_portal_page

MUNICODE_URL = "https://library.municode.com/ca/pasadena/codes/code_of_ordinances"
MUNICODE_HTML = """
<html><body>
  <nav>
    <a href="/ca/pasadena/codes/code_of_ordinances?nodeId=TIT14">Title 14 - Nuisances</a>
    <a href="https://library.municode.com/ca/pasadena/codes/code_of_ordinances?nodeId=TIT9">
      Title 9 - Public Peace</a>
  </nav>
  <a href="https://export.municode.com/exportpdf?clientId=1234&amp;nodeId=TIT14">
    Download Title 14 (PDF)</a>
  <a href="/ca/pasadena/codes/code_of_ordinances">self link</a>
</body></html>
"""
MUNICODE_CAPTURES = [
    CapturedResponse(
        url="https://api.municode.com/CodesContent?jobId=1&nodeId=TIT14",
        content_type="application/json; charset=utf-8",
    ),
    CapturedResponse(
        url="https://cdn.municode.com/app.js", content_type="text/javascript"
    ),
    CapturedResponse(
        url="https://analytics.example.com/track", content_type="application/json"
    ),
]


def test_municode_strategy() -> None:
    parse = parse_portal_page(
        Publisher.MUNICODE, MUNICODE_HTML, MUNICODE_URL, MUNICODE_CAPTURES
    )
    urls = [c.url for c in parse.candidates]
    # PDF export link harvested, entity-unescaped.
    assert (
        "https://export.municode.com/exportpdf?clientId=1234&nodeId=TIT14" in urls
    )
    # JSON content endpoint captured from the municode API only.
    assert "https://api.municode.com/CodesContent?jobId=1&nodeId=TIT14" in urls
    assert all("analytics" not in u and "app.js" not in u for u in urls)
    # TOC navigation: both title links, absolutized; self link excluded.
    assert set(parse.toc_links) == {
        f"{MUNICODE_URL}?nodeId=TIT14",
        f"{MUNICODE_URL}?nodeId=TIT9",
    }
    # Context carries the portal URL + the code hierarchy heading.
    export = next(c for c in parse.candidates if "exportpdf" in c.url)
    assert MUNICODE_URL in export.context
    assert "Title 14" in export.context


AMLEGAL_URL = "https://codelibrary.amlegal.com/codes/pasadena/latest/overview"
AMLEGAL_HTML = """
<html><body>
  <a href="/codes/pasadena/latest/pasadena_ca/0-0-0-1">CHAPTER 1: GENERAL</a>
  <a href="https://codelibrary.amlegal.com/pdf/pasadena_ch1.pdf">Chapter 1 PDF</a>
</body></html>
"""


def test_amlegal_strategy() -> None:
    captures = [
        CapturedResponse(
            url="https://codelibrary.amlegal.com/api/client-content/pasadena?part=ch1",
            content_type="application/json",
        )
    ]
    parse = parse_portal_page(Publisher.AMLEGAL, AMLEGAL_HTML, AMLEGAL_URL, captures)
    urls = [c.url for c in parse.candidates]
    assert "https://codelibrary.amlegal.com/pdf/pasadena_ch1.pdf" in urls
    assert (
        "https://codelibrary.amlegal.com/api/client-content/pasadena?part=ch1" in urls
    )
    assert parse.toc_links == [
        "https://codelibrary.amlegal.com/codes/pasadena/latest/pasadena_ca/0-0-0-1"
    ]


ECODE_URL = "https://ecode360.com/PA2001"
ECODE_HTML = """
<html><body>
  <a href="/PA2001/laws/list">Chapter list</a>
  <a href="https://ecode360.com/attachment/PA2001/PA2001-014a.pdf">Ch 14 attachment</a>
  <a href="https://other-site.example/x">external</a>
</body></html>
"""


def test_ecode360_strategy() -> None:
    captures = [
        CapturedResponse(
            url="https://ecode360.com/api/content/PA2001/CH14",
            content_type="application/json",
        )
    ]
    parse = parse_portal_page(Publisher.ECODE360, ECODE_HTML, ECODE_URL, captures)
    urls = [c.url for c in parse.candidates]
    assert "https://ecode360.com/attachment/PA2001/PA2001-014a.pdf" in urls
    assert "https://ecode360.com/api/content/PA2001/CH14" in urls
    assert parse.toc_links == ["https://ecode360.com/PA2001/laws/list"]
    assert all("other-site" not in u for u in urls)


OTHER_URL = "https://www.smalltown.gov/code"
OTHER_HTML = """
<html><body>
  <a href="/code/chapter1.pdf">Chapter 1</a>
  <a href="/code/chapter2.docx">Chapter 2</a>
  <a href="/code/page2">More</a>
  <a href="/feeds/code.xml">RSS</a>
</body></html>
"""


def test_other_strategy_generic_documents_plus_captured_json() -> None:
    captures = [
        CapturedResponse(
            url="https://www.smalltown.gov/api/code/ch1", content_type="application/json"
        )
    ]
    parse = parse_portal_page(Publisher.OTHER, OTHER_HTML, OTHER_URL, captures)
    urls = [c.url for c in parse.candidates]
    assert "https://www.smalltown.gov/code/chapter1.pdf" in urls
    assert "https://www.smalltown.gov/code/chapter2.docx" in urls
    assert "https://www.smalltown.gov/api/code/ch1" in urls
    # .xml links are followable-set-excluded even for the generic strategy;
    # structured content enters only via the captured endpoints.
    assert all(not u.endswith("code.xml") for u in urls)
    assert "https://www.smalltown.gov/code/page2" in parse.toc_links


def test_rendered_html_page_itself_is_never_a_candidate() -> None:
    for publisher in Publisher:
        parse = parse_portal_page(publisher, MUNICODE_HTML, MUNICODE_URL, [])
        assert MUNICODE_URL not in [c.url for c in parse.candidates]


@pytest.mark.live
def test_live_municode_crawl() -> None:
    """One real Municode crawl — excluded from default runs (-m 'not live')."""
    from datetime import timedelta

    from harvest_core.adapters.system_clock import SystemClock
    from harvest_harvester.spiders.discoverer import PlaywrightPortalDiscoverer

    clock = SystemClock()
    discoverer = PlaywrightPortalDiscoverer(clock)
    result = discoverer.discover(
        ["https://library.municode.com/ca/pasadena/codes/code_of_ordinances"],
        Publisher.MUNICODE,
        max_pages=3,
        deadline=clock.now() + timedelta(seconds=300),
    )
    assert result.candidates, "expected at least one discovered document URL"
