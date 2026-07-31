"""Plan 3.6 (part): type sniffing, text extraction, link extraction."""

import pytest
from harvest_harvester.extract import extract_text
from harvest_harvester.links import extract_document_links
from harvest_harvester.sniff import sniff
from pdf_fixture import minimal_pdf

OLE = bytes.fromhex("D0CF11E0A1B11AE1")


@pytest.mark.parametrize(
    ("content", "content_type", "url", "expected"),
    [
        # 1. magic bytes win
        (b"%PDF-1.7 ...", "text/plain", "https://x.gov/a", ".pdf"),
        # 2. zip + URL extension
        (b"PK\x03\x04rest", None, "https://x.gov/a.docx", ".docx"),
        (b"PK\x03\x04rest", None, "https://x.gov/a.xlsx", ".xlsx"),
        (b"PK\x03\x04rest", None, "https://x.gov/a.zip", None),
        # 3. OLE + URL extension
        (OLE + b"rest", None, "https://x.gov/a.doc", ".doc"),
        (OLE + b"rest", None, "https://x.gov/a.xls", ".xls"),
        # 4. RTF
        (b"{\\rtf1\\ansi hello}", None, "https://x.gov/a", ".rtf"),
        # 5. THE TRAP: XHTML served as text/html from a claimed PDF link.
        #    HTML must be checked before XML or this stores as .xml.
        (
            b'<?xml version="1.0"?><!DOCTYPE html><html>stub</html>',
            "text/html",
            "https://legislature.example/doc.pdf",
            None,
        ),
        (
            b"<html><body>page</body></html>",
            "text/html; charset=utf-8",
            "https://x.gov/a.pdf",
            None,
        ),
        # 6. XML (not served as html)
        (b'<?xml version="1.0"?><rule/>', "application/octet-stream", "https://x.gov/a", ".xml"),
        # 7. JSON
        (b'{"a": 1}', "application/json", "https://x.gov/api", ".json"),
        (b"[1, 2]", None, "https://x.gov/data.json", ".json"),
        (b"not json", None, "https://x.gov/data.json", ".json"),  # 9. falls to URL ext
        # 8. exact content-type match
        (b"whatever", "application/msword", "https://x.gov/download", ".doc"),
        # 9. URL extension fallback
        (b"whatever", "application/octet-stream", "https://x.gov/a.odt", ".odt"),
        # nothing → not a document
        (b"whatever", "application/octet-stream", "https://x.gov/a", None),
    ],
)
def test_sniff_table(
    content: bytes, content_type: str | None, url: str, expected: str | None
) -> None:
    assert sniff(content, content_type, url) == expected


def test_nul_poisoned_pdf_stores_sanitized_text() -> None:
    poisoned = minimal_pdf("Nuisance\x00Ordinance Section\x001")
    assert sniff(poisoned, "application/pdf", "https://x.gov/a.pdf") == ".pdf"
    text = extract_text(".pdf", poisoned)
    assert "Nuisance" in text and "Ordinance" in text
    assert "\x00" not in text  # Postgres TEXT can hold no NULs


def test_extract_docx_strips_tags_preserving_run_whitespace() -> None:
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "word/document.xml",
            "<w:document><w:p><w:r><w:t>No person</w:t></w:r>"
            "<w:r><w:t>shall emit</w:t></w:r></w:p></w:document>",
        )
    text = extract_text(".docx", buf.getvalue())
    assert "No person shall emit" == text


def test_extract_rtf_and_xml_and_json() -> None:
    assert "Quiet hours" in extract_text(
        ".rtf", b"{\\rtf1\\ansi\\deff0 Quiet hours apply}"
    )
    assert extract_text(".xml", b'<?xml version="1.0"?><rule>No noise</rule>') == "No noise"
    assert extract_text(".json", b'{"section": "14"}') == '{"section": "14"}'
    assert extract_text(".json", b"not-json") == ""  # verbatim only if it parses


def test_extraction_failure_yields_empty_never_raises() -> None:
    assert extract_text(".pdf", b"%PDF-corrupt garbage") == ""
    assert extract_text(".docx", b"PK\x03\x04 not a zip") == ""


def test_extract_truncates_to_20000_chars() -> None:
    text = extract_text(".json", b'"' + b"x" * 50_000 + b'"')
    assert len(text) == 20_000


def test_link_extraction_rules() -> None:
    html = """
    <html><body>
      <a href="docs/a.docx">Word doc</a>
      <a href="/files/b.pdf">PDF two</a>
      <a href="https://x.gov/c.pdf">PDF three</a>
      <a href="feed.xml">Feed</a>
      <a href="api.json">API</a>
      <a href="page.html">Page</a>
      <a href="/files/b.pdf">duplicate</a>
      <a href="mailto:clerk@x.gov">mail</a>
    </body></html>
    """
    links = extract_document_links(html, "https://x.gov/laws/")
    urls = [link.url for link in links]
    # PDFs float to the front; .xml/.json never followed; dedupe holds.
    assert urls == [
        "https://x.gov/files/b.pdf",
        "https://x.gov/c.pdf",
        "https://x.gov/laws/docs/a.docx",
    ]
    assert links[0].anchor_text == "PDF two"
