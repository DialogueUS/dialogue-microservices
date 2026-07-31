"""Per-format text extraction (old spec §6.4), best-effort by design.

Any extraction failure yields empty text rather than an exception. NUL
and lone-surrogate stripping is a durability requirement: Postgres TEXT
holds neither, and one poisoned artifact must never wedge a corpus.
"""

from __future__ import annotations

import io
import json
import logging
import re
import zipfile

from harvest_core.constants import EXTRACTED_TEXT_CHARS, PDF_MAX_PAGES

from .staging import sanitize_text

log = logging.getLogger(__name__)


def _strip_tags(xml_text: str) -> str:
    # Preserve whitespace between runs — Word splits text across elements.
    text = re.sub(r"<[^>]+>", " ", xml_text)
    return re.sub(r"[ \t]+", " ", text).strip()


def _extract_pdf(data: bytes) -> str:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data))
    pages = reader.pages[:PDF_MAX_PAGES]
    return "\n".join(filter(None, (page.extract_text() or "" for page in pages)))


def _extract_zip_xml(data: bytes, member: str) -> str:
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        return _strip_tags(zf.read(member).decode("utf-8", errors="replace"))


def _extract_rtf(data: bytes) -> str:
    text = data.decode("latin-1", errors="replace")
    text = re.sub(r"\\'[0-9a-fA-F]{2}", " ", text)  # hex escapes
    text = re.sub(r"\\[a-zA-Z]+-?\d*\s?", " ", text)  # control words
    text = text.replace("{", " ").replace("}", " ")
    return re.sub(r"\s+", " ", text).strip()


def extract_text(ext: str, data: bytes) -> str:
    """Extract and sanitize text; truncated to the stored cap."""
    try:
        if ext == ".pdf":
            text = _extract_pdf(data)
        elif ext == ".docx":
            text = _extract_zip_xml(data, "word/document.xml")
        elif ext == ".odt":
            text = _extract_zip_xml(data, "content.xml")
        elif ext == ".rtf":
            text = _extract_rtf(data)
        elif ext == ".xml":
            text = _strip_tags(data.decode("utf-8", errors="replace"))
        elif ext == ".json":
            text = data.decode("utf-8", errors="replace")
            json.loads(text)  # verbatim, but only if it parses
        else:
            text = ""
    except Exception:
        log.debug("extraction failed for %s document", ext, exc_info=True)
        text = ""
    return sanitize_text(text)[:EXTRACTED_TEXT_CHARS]
