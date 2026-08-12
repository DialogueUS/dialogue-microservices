"""Adapter unit tests with stubbed transports (plan 1.5) — no live backends."""

from datetime import UTC, datetime, timedelta

import fakeredis
import httpx
import pytest
import respx
import sqlalchemy as sa
from botocore.session import get_session
from botocore.stub import Stubber
from harvest_core.adapters.fetcher import HttpxFetcher, encode_url
from harvest_core.adapters.llm import LangChainLLM, QueryGenerationOutput, TriageOutput
from harvest_core.adapters.postgres import PostgresDatastore, migrate
from harvest_core.adapters.redis_kv import RedisKeyValue
from harvest_core.adapters.redis_pubsub import RedisPubSub
from harvest_core.adapters.serper import SERPER_URL, SerperSearch
from harvest_core.adapters.sqs import SqsQueue
from harvest_core.domain import Jurisdiction, Source, SweepResult
from harvest_core.errors import GenerationError, RateLimited, TriageError, UniqueViolation
from harvest_core.ports import SearchResult, TriageRequest
from harvest_core.storage import filename_from_url, object_key, slugify

NOW = datetime(2026, 7, 30, 12, 0, 0, tzinfo=UTC)


# -- Serper -----------------------------------------------------------------


@respx.mock
def test_serper_429_maps_to_rate_limited() -> None:
    respx.post(SERPER_URL).mock(return_value=httpx.Response(429, text="slow down"))
    search = SerperSearch(api_key="k", client=httpx.Client())
    with pytest.raises(RateLimited):
        search.search("pasadena noise ordinance", 20)


@respx.mock
def test_serper_request_shape_and_parsing() -> None:
    route = respx.post(SERPER_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "organic": [
                    {
                        "position": 1,
                        "title": "Noise Ordinance",
                        "snippet": "Chapter 9",
                        "link": "https://cityofpasadena.net/noise.pdf",
                    }
                ]
            },
        )
    )
    results = SerperSearch(api_key="secret", client=httpx.Client()).search("q", 20)
    request = route.calls.last.request
    assert request.headers["X-API-KEY"] == "secret"
    import json

    assert json.loads(request.content) == {"q": "q", "num": 20}
    assert results == [
        SearchResult(
            rank=1,
            title="Noise Ordinance",
            snippet="Chapter 9",
            url="https://cityofpasadena.net/noise.pdf",
        )
    ]


# -- SQS --------------------------------------------------------------------


def _sqs_client():  # type: ignore[no-untyped-def]
    return get_session().create_client(
        "sqs",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def test_sqs_change_visibility_parameters() -> None:
    client = _sqs_client()
    stubber = Stubber(client)
    stubber.add_response(
        "change_message_visibility",
        {},
        {
            "QueueUrl": "https://sqs.example/q",
            "ReceiptHandle": "rh-1",
            "VisibilityTimeout": 900,
        },
    )
    with stubber:
        SqsQueue(client, "https://sqs.example/q").change_visibility("rh-1", 900)
    stubber.assert_no_pending_responses()


def test_sqs_receive_long_polls_and_maps_receive_count() -> None:
    client = _sqs_client()
    stubber = Stubber(client)
    stubber.add_response(
        "receive_message",
        {
            "Messages": [
                {
                    "MessageId": "id-1",
                    "ReceiptHandle": "rh-1",
                    "Body": "{}",
                    "Attributes": {"ApproximateReceiveCount": "2"},
                }
            ]
        },
        {
            "QueueUrl": "https://sqs.example/q",
            "MaxNumberOfMessages": 10,
            "WaitTimeSeconds": 20,
            "AttributeNames": ["ApproximateReceiveCount"],
        },
    )
    with stubber:
        messages = SqsQueue(client, "https://sqs.example/q").receive(10)
    assert len(messages) == 1
    assert messages[0].id == "rh-1"
    assert messages[0].receive_count == 2


# -- storage key scheme -----------------------------------------------------


def test_object_key_scheme() -> None:
    key = object_key(
        "Nuisance Regs!",
        "Pasadena city",
        "abcdef0123456789" * 4,
        "noise.pdf",
    )
    assert key == "nuisance-regs/pasadena-city/abcdef01_noise.pdf"


def test_slugify_truncates_to_60() -> None:
    assert len(slugify("x" * 100)) == 60
    assert slugify("A  B--C") == "a-b-c"


def test_filename_from_url() -> None:
    url = "https://x.gov/Ordinances/2026-002%20Nuisance%20Ordinance.pdf?v=2"
    assert filename_from_url(url, ".pdf") == "2026-002_Nuisance_Ordinance.pdf"
    assert filename_from_url("https://x.gov/", ".pdf") == "record.pdf"
    # extension normalized to the sniffed one
    assert filename_from_url("https://x.gov/download.aspx", ".pdf") == "download.aspx.pdf"


# -- percent-encoding (old spec §6.1 cases) ---------------------------------


def test_encode_url_encodes_spaces() -> None:
    assert (
        encode_url("https://x.gov/Ordinances/2026-002 Nuisance Ordinance.pdf")
        == "https://x.gov/Ordinances/2026-002%20Nuisance%20Ordinance.pdf"
    )


def test_encode_url_preserves_existing_escapes() -> None:
    already = "https://x.gov/files/A%20%26%20B.pdf"
    assert encode_url(already) == already


def test_encode_url_mixed_spaces_and_escapes() -> None:
    # A partially escaped URL: the %20 stays, the raw space is encoded.
    assert encode_url("https://x.gov/a%20b c.pdf") == "https://x.gov/a%20b%20c.pdf"


@respx.mock
def test_fetcher_caps_bytes_and_reports_content_type() -> None:
    respx.get("https://x.gov/big.pdf").mock(
        return_value=httpx.Response(
            200, content=b"x" * 100, headers={"content-type": "application/pdf"}
        )
    )
    fetcher = HttpxFetcher(client=httpx.Client())
    resp = fetcher.get("https://x.gov/big.pdf", max_bytes=10)
    assert resp.status == 200
    assert resp.content == b"x" * 10
    assert resp.content_type is not None and "pdf" in resp.content_type


# -- Redis ------------------------------------------------------------------


def test_redis_adapter_semantics_with_fakeredis() -> None:
    kv = RedisKeyValue(fakeredis.FakeRedis(decode_responses=True))
    assert kv.setnx("url:c:h", "1") is True
    assert kv.setnx("url:c:h", "1") is False
    assert kv.hincrby("sweep:d", {"done": 1, "candidates": 2}) == {"done": 1, "candidates": 2}
    assert kv.hincrby("sweep:d", {"done": 1}) == {"done": 2, "candidates": 2}
    kv.expire("sweep:d", 1800)
    assert kv.get("url:c:h") == "1"
    assert kv.delete_prefix("url:c:") == 1
    assert kv.get("url:c:h") is None


def test_redis_pubsub_adapter_never_returns_the_subscribe_confirmation() -> None:
    """redis-py hands back a {'type': 'subscribe'} frame before any real
    message. An adapter that passed it through would have the health
    probe's first poll read `1` as the service's reply."""
    bus = RedisPubSub(fakeredis.FakeRedis(decode_responses=True))
    subscription = bus.subscribe("health:svc:pong:n1")

    assert bus.publish("health:svc:pong:n1", '{"healthy": true}') == 1
    assert subscription.poll(1.0) == '{"healthy": true}'


def test_redis_pubsub_adapter_times_out_and_closes() -> None:
    bus = RedisPubSub(fakeredis.FakeRedis(decode_responses=True))
    subscription = bus.subscribe("health:svc:ping")

    assert subscription.poll(0.05) is None  # nothing published: None, not a frame

    subscription.close()
    subscription.close()  # idempotent, so `finally: close()` is always safe
    assert bus.publish("health:svc:ping", "n1") == 0


# -- Postgres datastore (SQLite engine; live Postgres deferred to 4.2) ------


@pytest.fixture()
def ds() -> PostgresDatastore:
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    migrate(engine)
    migrate(engine)  # boot migration must be idempotent
    return PostgresDatastore(engine)


def test_pg_unique_violations(ds: PostgresDatastore) -> None:
    j = ds.insert_jurisdiction("Pasadena", "CA", "city")
    with pytest.raises(UniqueViolation):
        ds.insert_jurisdiction("Pasadena", "CA", "city")
    ds.insert_target("c", j.id, Source.SERPER, 3, NOW)
    with pytest.raises(UniqueViolation):
        ds.insert_target("c", j.id, Source.SERPER, 3, NOW)
    ds.insert_artifact("c", j.id, "serper", "https://x.gov/a.pdf", "ctx", NOW)
    with pytest.raises(UniqueViolation):
        ds.insert_artifact("c", j.id, "serper", "https://x.gov/a.pdf", "", NOW)


def test_pg_select_due_ordering_and_stamp_window(ds: PostgresDatastore) -> None:
    fed = ds.insert_jurisdiction("United States", "US", "federal")
    city = ds.insert_jurisdiction("Pasadena", "CA", "city")
    t_city = ds.insert_target("c", city.id, Source.SERPER, 3, NOW - timedelta(days=2))
    t_fed = ds.insert_target("c", fed.id, Source.SERPER, 0, NOW - timedelta(days=1))
    timeouts = {Source.SERPER: 1800.0, Source.LEGAL_CODES: 7200.0}

    due = ds.select_due("c", NOW, timeouts, 10)
    assert [t.id for t in due] == [t_fed.id, t_city.id]  # federal precedes cities

    # A stamped row inside the window is never re-selected...
    assert ds.stamp_target(t_fed.id, NOW - timedelta(seconds=1799), "d1", 3, None, None)
    assert [t.id for t in ds.select_due("c", NOW, timeouts, 10)] == [t_city.id]
    # ...and reappears once the stamp is older than the dispatch timeout.
    assert ds.stamp_target(
        t_fed.id, NOW - timedelta(seconds=1801), "d1", 3, "d1", NOW - timedelta(seconds=1799)
    )
    assert [t.id for t in ds.select_due("c", NOW, timeouts, 10)] == [t_fed.id, t_city.id]
    # CAS: a stamp gated on a stale expectation loses harmlessly.
    assert ds.stamp_target(t_fed.id, NOW, "d2", 3, None, None) is False


def test_pg_finalize_gated_by_dispatch_id(ds: PostgresDatastore) -> None:
    j = ds.insert_jurisdiction("Pasadena", "CA", "city")
    t = ds.insert_target("c", j.id, Source.SERPER, 3, NOW)
    assert ds.stamp_target(t.id, NOW, "d1", 3, None, None)
    assert ds.finalize_target(t.id, "stale-id", SweepResult.CANDIDATES, NOW) is False
    assert ds.finalize_target(t.id, "d1", SweepResult.CANDIDATES, NOW) is True
    row = ds.get_target(t.id)
    assert row is not None
    assert row.dispatch_id is None and row.dispatched_at is None
    assert row.last_result == SweepResult.CANDIDATES


def test_pg_history_idempotent(ds: PostgresDatastore) -> None:
    j = ds.insert_jurisdiction("Pasadena", "CA", "city")
    args = dict(
        corpus="c",
        jurisdiction_id=j.id,
        source=Source.SERPER,
        dispatch_id="d1",
        query_seq=0,
        result=SweepResult.CANDIDATES,
        topic="noise",
        results_seen=5,
        results_triaged_relevant=2,
        candidates_staged=1,
        detail="query text here",
        swept_at=NOW,
    )
    assert ds.insert_history(**args) is True  # type: ignore[arg-type]
    assert ds.insert_history(**args) is False  # type: ignore[arg-type]


def test_pg_sha_backstop_and_pending_stale(ds: PostgresDatastore) -> None:
    j = ds.insert_jurisdiction("Pasadena", "CA", "city")
    a1 = ds.insert_artifact("c", j.id, "serper", "https://x.gov/a.pdf", "", NOW)
    a2 = ds.insert_artifact("c", j.id, "serper", "https://x.gov/b.pdf", "", NOW)
    art = ds.get_artifact(a1.id)
    assert art is not None
    art.sha256 = "deadbeef"
    ds.update_artifact(art)
    assert ds.corpus_has_sha256("c", "deadbeef", exclude_artifact_id=a2.id) is True
    assert ds.corpus_has_sha256("c", "deadbeef", exclude_artifact_id=a1.id) is False

    stale = ds.select_pending_stale("c", NOW, 1800, 10)
    assert {a.id for a in stale} == {a1.id, a2.id}
    ds.stamp_artifact(a1.id, NOW, "d9")
    stale = ds.select_pending_stale("c", NOW, 1800, 10)
    assert {a.id for a in stale} == {a2.id}


def test_pg_purge_corpus_counts(ds: PostgresDatastore) -> None:
    j = ds.insert_jurisdiction("Pasadena", "CA", "city")
    ds.insert_target("c", j.id, Source.SERPER, 3, NOW)
    ds.insert_artifact("c", j.id, "serper", "https://x.gov/a.pdf", "", NOW)
    ds.insert_history(
        corpus="c",
        jurisdiction_id=j.id,
        source=Source.SERPER,
        dispatch_id="d",
        query_seq=0,
        result=SweepResult.CANDIDATES,
        topic=None,
        results_seen=0,
        results_triaged_relevant=0,
        candidates_staged=0,
        detail="",
        swept_at=NOW,
    )
    counts = ds.purge_corpus("c")
    assert counts["artifacts"] == 1
    assert counts["sweep_targets"] == 1
    assert counts["harvest_sweeps"] == 1
    assert ds.list_targets("c") == []


# -- LangChain adapter (stub model, no network) -----------------------------


class _StubStructured:
    def __init__(self, output: object) -> None:
        self._output = output

    def invoke(self, prompt: str) -> object:
        if isinstance(self._output, Exception):
            raise self._output
        return self._output


class _StubModel:
    def __init__(self, output: object) -> None:
        self.output = output
        self.schemas: list[type] = []

    def with_structured_output(self, schema: type) -> _StubStructured:
        self.schemas.append(schema)
        return _StubStructured(self.output)


JUR = Jurisdiction(id=1, name="Pasadena", state="CA", level="city")


def test_llm_generation_happy_path() -> None:
    output = QueryGenerationOutput.model_validate(
        {"topics": [{"topic": "noise", "queries": ["pasadena noise ordinance pdf"]}]}
    )
    llm = LangChainLLM(_StubModel(output))
    assert llm.generate(JUR, ["noise"], 3) == {"noise": ["pasadena noise ordinance pdf"]}


def test_llm_generation_parse_failure_is_typed_error() -> None:
    llm = LangChainLLM(_StubModel(ValueError("bad json from model")))
    with pytest.raises(GenerationError):
        llm.generate(JUR, ["noise"], 3)
    # missing topic → typed error too, never a KeyError escaping
    output = QueryGenerationOutput.model_validate({"topics": []})
    with pytest.raises(GenerationError):
        LangChainLLM(_StubModel(output)).generate(JUR, ["noise"], 3)


def test_llm_triage_maps_verdicts_back_per_request() -> None:
    reqs = [
        TriageRequest(
            "Pasadena",
            "CA",
            "city",
            "noise",
            [SearchResult(1, "t1", "s1", "https://a.gov/x.pdf")],
        ),
        TriageRequest(
            "Berkeley",
            "CA",
            "city",
            "noise",
            [
                SearchResult(1, "t2", "s2", "https://b.gov/y"),
                SearchResult(2, "t3", "s3", "https://b.gov/z.pdf"),
            ],
        ),
    ]
    output = TriageOutput.model_validate(
        {
            "verdicts": [
                {"index": 0, "relevant": True, "is_document": True, "confidence": 0.9},
                {"index": 1, "relevant": False, "is_document": False, "confidence": 0.2},
                {"index": 2, "relevant": True, "is_document": True, "confidence": 0.8},
            ]
        }
    )
    verdicts = LangChainLLM(_StubModel(output)).triage(reqs)
    assert len(verdicts) == 2
    assert len(verdicts[0]) == 1 and len(verdicts[1]) == 2
    assert verdicts[0][0].relevant is True
    assert verdicts[1][0].relevant is False


def test_llm_triage_incomplete_indices_is_typed_error() -> None:
    reqs = [
        TriageRequest(
            "Pasadena",
            "CA",
            "city",
            "noise",
            [SearchResult(1, "t", "s", "https://a.gov/x.pdf")],
        )
    ]
    output = TriageOutput.model_validate({"verdicts": []})
    with pytest.raises(TriageError):
        LangChainLLM(_StubModel(output)).triage(reqs)
