"""Attachment storage (§8.1): type gate -> Redis dedupe -> upload.

Dedupe scope is per campaign (independently killable); the Redis entry
is keyed on the first 8 sha256 hex chars — the same digest embedded in
object keys, so a lost cache rebuilds from an S3 listing without
re-hashing.
"""

from __future__ import annotations

import logging
from hashlib import sha256
from pathlib import PurePosixPath

from .constants import (
    ACCEPTED_ATTACHMENT_CONTENT_TYPES,
    ACCEPTED_ATTACHMENT_EXTS,
    DIGEST_PREFIX_LEN,
)
from .domain import Campaign, document_key, slugify
from .errors import ObjectNotFound
from .messages import InboundAttachment
from .world import World

log = logging.getLogger("public_records.attachments")


def _dedupe_key(campaign: Campaign, digest8: str) -> str:
    return f"dedupe:{slugify(campaign.name)}:{digest8}"


def _accepted(attachment: InboundAttachment) -> bool:
    ext = PurePosixPath(attachment.filename.lower()).suffix
    if ext in ACCEPTED_ATTACHMENT_EXTS:
        return True
    content_type = attachment.content_type.partition(";")[0].strip().lower()
    return content_type in ACCEPTED_ATTACHMENT_CONTENT_TYPES


def store_attachments(
    world: World,
    campaign: Campaign,
    jurisdiction_name: str,
    thread_id: int,
    attachments: list[InboundAttachment],
) -> tuple[list[dict[str, str]], int]:
    """Returns (attachment_refs, stored_count). Refs record a stored key,
    a duplicate pointer, or a rejection reason per attachment."""
    refs: list[dict[str, str]] = []
    stored = 0
    for attachment in attachments:
        if not _accepted(attachment):
            refs.append(
                {
                    "filename": attachment.filename,
                    "content_type": attachment.content_type,
                    "status": "rejected",
                    "reason": f"unsupported type {attachment.content_type!r}",
                }
            )
            continue
        try:
            data = world.documents.get(attachment.s3_tmp_key)
        except ObjectNotFound:
            refs.append(
                {
                    "filename": attachment.filename,
                    "content_type": attachment.content_type,
                    "status": "rejected",
                    "reason": "inbox-spool object missing",
                }
            )
            continue

        digest = sha256(data).hexdigest()
        key = document_key(campaign.name, jurisdiction_name, digest, attachment.filename)
        if not world.kv.setnx(_dedupe_key(campaign, digest[:DIGEST_PREFIX_LEN]), key):
            existing = world.kv.get(_dedupe_key(campaign, digest[:DIGEST_PREFIX_LEN])) or key
            # heal a crash between SETNX and upload: idempotent overwrite
            try:
                world.documents.get(existing)
            except ObjectNotFound:
                world.documents.put(existing, data, attachment.content_type)
            refs.append(
                {
                    "filename": attachment.filename,
                    "content_type": attachment.content_type,
                    "status": "duplicate",
                    "key": existing,
                }
            )
            continue

        world.documents.put(key, data, attachment.content_type)
        world.store.append_attachment_key(thread_id, key)
        refs.append(
            {
                "filename": attachment.filename,
                "content_type": attachment.content_type,
                "status": "stored",
                "key": key,
            }
        )
        stored += 1
    return refs, stored


def delete_spooled(world: World, attachments: list[InboundAttachment]) -> None:
    """The inbox-spool object is deleted after the message commits."""
    for attachment in attachments:
        world.documents.delete(attachment.s3_tmp_key)


def rebuild_dedupe(world: World, campaign: Campaign) -> int:
    """Cold-start rebuild from S3 key listings (digest8 embedded in
    keys; re-hashing not required). Returns entries written."""
    written = 0
    prefix = f"{slugify(campaign.name)}/"
    for key in world.documents.list_keys(prefix):
        parts = key.split("/")
        if len(parts) < 3 or parts[1] == "inbox-spool":
            continue
        digest8 = parts[-1].split("_", 1)[0]
        if len(digest8) == DIGEST_PREFIX_LEN and world.kv.setnx(
            _dedupe_key(campaign, digest8), key
        ):
            written += 1
    return written
