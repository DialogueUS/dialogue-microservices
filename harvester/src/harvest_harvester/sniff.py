"""Type sniffing in the exact old order (old spec §6.3).

HTML must be checked before XML: XHTML begins with `<?xml`, and a state
legislature really does serve 4 KB XHTML stubs from what claim to be
PDF links. Get the order wrong and the corpus fills with error pages
stored as XML.
"""

from __future__ import annotations

from harvest_core.constants import STORED_EXTENSIONS

from .links import url_extension

_OLE_MAGIC = bytes.fromhex("D0CF11E0A1B11AE1")

_CONTENT_TYPE_MAP = {
    "application/pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/msword": ".doc",
    "application/vnd.oasis.opendocument.text": ".odt",
    "application/rtf": ".rtf",
    "text/rtf": ".rtf",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-excel": ".xls",
    "application/xml": ".xml",
    "text/xml": ".xml",
    "application/json": ".json",
}

EXT_CONTENT_TYPES = {
    ".pdf": "application/pdf",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".doc": "application/msword",
    ".odt": "application/vnd.oasis.opendocument.text",
    ".rtf": "application/rtf",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xls": "application/vnd.ms-excel",
    ".xml": "application/xml",
    ".json": "application/json",
}


def sniff(content: bytes, content_type: str | None, url: str) -> str | None:
    """Extension for a stored document type, or None (not a document)."""
    ct = (content_type or "").lower().split(";")[0].strip()
    ext = url_extension(url)

    # 1. PDF magic.
    if content.startswith(b"%PDF-"):
        return ".pdf"
    # 2. Zip container + a matching URL extension.
    if content.startswith(b"PK") and ext in (".docx", ".odt", ".xlsx"):
        return ext
    # 3. OLE compound document + a matching URL extension.
    if content.startswith(_OLE_MAGIC) and ext in (".doc", ".xls"):
        return ext
    # 4. RTF.
    if content.startswith(b"{\\rtf"):
        return ".rtf"
    # 5. HTML is not a document — and this check must precede XML.
    if ct == "text/html":
        return None
    # 6. XML.
    if content.lstrip().startswith(b"<?xml"):
        return ".xml"
    # 7. JSON.
    body = content.lstrip()
    if ct == "application/json" or (
        ext == ".json" and body[:1] in (b"{", b"[")
    ):
        return ".json"
    # 8. Exact content-type match.
    if ct in _CONTENT_TYPE_MAP:
        return _CONTENT_TYPE_MAP[ct]
    # 9. Fall back to the URL extension.
    if ext in STORED_EXTENSIONS:
        return ext
    return None
