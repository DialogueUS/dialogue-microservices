"""Plan 4.2 (unit part): kill-as-purge leaves zero rows/keys/objects."""

from datetime import UTC, datetime

from harvest_core.domain import Source
from harvest_core.fakes import FakeDatastore, FakeKeyValue, FakeObjectStore, VirtualClock
from harvest_core.storage import slugify
from harvest_orchestrator.ops import purge_corpus

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def test_purge_leaves_zero_rows_keys_objects_for_the_corpus() -> None:
    ds = FakeDatastore()
    kv = FakeKeyValue(VirtualClock(NOW))
    objects = FakeObjectStore()
    corpus, other = "doomed corpus", "survivor"

    jur = ds.insert_jurisdiction("Pasadena", "CA", "city").id
    for name in (corpus, other):
        ds.insert_target(name, jur, Source.SERPER, 3, NOW)
        ds.insert_artifact(name, jur, "serper", f"https://x.gov/{name}.pdf", "", NOW)
        kv.setnx(f"url:{name}:h1", "1")
        kv.setnx(f"doc:{name}:h2", "1")
        objects.put(f"{slugify(name)}/pasadena/aa_doc.pdf", b"bytes", "application/pdf")

    counts = purge_corpus(ds, kv, objects, corpus)
    assert counts["artifacts"] == 1
    assert counts["sweep_targets"] == 1
    assert counts["s3_objects"] == 1
    assert counts["redis_keys"] == 2

    # Zero anything left for the corpus...
    assert ds.list_targets(corpus) == []
    assert all(a.corpus != corpus for a in ds.artifacts.values())
    assert kv.get(f"url:{corpus}:h1") is None
    assert kv.get(f"doc:{corpus}:h2") is None
    assert all(not k.startswith(slugify(corpus)) for k in objects.objects)
    # ...and the other corpus untouched (independently killable).
    assert len(ds.list_targets(other)) == 1
    assert kv.get(f"url:{other}:h1") == "1"
