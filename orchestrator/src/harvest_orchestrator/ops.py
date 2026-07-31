"""Operational commands: corpus kill-as-purge (spec §9 operational surface).

Kill is a permanent purge, not a stop: Postgres rows (FK-ordered), the
corpus's S3 prefix, and the corpus's Redis prefixes all go.
"""

from __future__ import annotations

from harvest_core.ports import Datastore, KeyValue, ObjectStore
from harvest_core.storage import slugify


def purge_corpus(
    ds: Datastore, kv: KeyValue, objects: ObjectStore, corpus: str
) -> dict[str, int]:
    counts = ds.purge_corpus(corpus)
    counts["s3_objects"] = objects.delete_prefix(f"{slugify(corpus)}/")
    counts["redis_keys"] = kv.delete_prefix(f"url:{corpus}:")
    counts["redis_keys"] += kv.delete_prefix(f"doc:{corpus}:")
    return counts
