"""Object-storage key scheme (old spec §6.4, carried forward).

Key: `<corpus-slug>/<jurisdiction-slug>/<first 8 hex of sha256>_<filename>`.
Slugs are lowercased, non-alphanumerics collapsed to hyphens, truncated
to 60 chars. Filename derives from the URL's last path segment with the
query stripped and the extension normalized, falling back to `record`.
"""

from __future__ import annotations

import re
from urllib.parse import unquote, urlsplit

from .constants import STORAGE_HASH_PREFIX_LEN, STORAGE_SLUG_LEN


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug[:STORAGE_SLUG_LEN]


def filename_from_url(url: str, ext: str) -> str:
    path = urlsplit(url).path
    segment = unquote(path.rsplit("/", 1)[-1]) if path else ""
    segment = re.sub(r"[^A-Za-z0-9._-]+", "_", segment).strip("._-")
    stem = segment
    for known in (ext, *_KNOWN_EXTS):
        if stem.lower().endswith(known):
            stem = stem[: -len(known)]
            break
    if not stem:
        stem = "record"
    return f"{stem}{ext}"


_KNOWN_EXTS = (
    ".pdf",
    ".docx",
    ".doc",
    ".odt",
    ".rtf",
    ".xlsx",
    ".xls",
    ".xml",
    ".json",
)


def object_key(corpus: str, jurisdiction_name: str, sha256: str, filename: str) -> str:
    return (
        f"{slugify(corpus)}/{slugify(jurisdiction_name)}/"
        f"{sha256[:STORAGE_HASH_PREFIX_LEN]}_{filename}"
    )
