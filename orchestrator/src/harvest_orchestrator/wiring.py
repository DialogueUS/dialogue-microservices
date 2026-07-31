"""Process wiring: real vs fake adapters, chosen here and nowhere else.

Env for the real backend:
  DATABASE_URL, SWEEP_QUEUE_URL, CODE_QUEUE_URL, FETCH_QUEUE_URL,
  S3_BUCKET, AWS_ENDPOINT_URL (LocalStack), plus the key env vars named
  by the config (serper_api_key_env, llm.api_key_env, redis_url_env).
Offline smoke: HARVEST_FAKE_SEARCH=1 / HARVEST_FAKE_LLM=1 swap those
two for fakes while everything else stays real (plan 4.2).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from harvest_core.config import HarvestConfig
from harvest_core.ports import (
    Clock,
    Datastore,
    KeyValue,
    ObjectStore,
    QueryGenerator,
    TaskQueue,
)

from .census import CensusSource


@dataclass
class Backends:
    ds: Datastore
    clock: Clock
    kv: KeyValue
    objects: ObjectStore
    sweep_queue: TaskQueue
    code_queue: TaskQueue
    fetch_queue: TaskQueue
    generator: QueryGenerator
    census: CensusSource


def _sqs_client() -> Any:
    import boto3

    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    return boto3.client("sqs", endpoint_url=endpoint) if endpoint else boto3.client("sqs")


def _s3_client() -> Any:
    import boto3

    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    return boto3.client("s3", endpoint_url=endpoint) if endpoint else boto3.client("s3")


def wire_real(config: HarvestConfig) -> Backends:
    import sqlalchemy as sa
    from harvest_core.adapters.postgres import PostgresDatastore, migrate
    from harvest_core.adapters.redis_kv import RedisKeyValue
    from harvest_core.adapters.s3 import S3ObjectStore
    from harvest_core.adapters.sqs import SqsQueue
    from harvest_core.adapters.system_clock import SystemClock

    engine = sa.create_engine(os.environ["DATABASE_URL"])
    migrate(engine)
    sqs = _sqs_client()

    generator: QueryGenerator
    if os.environ.get("HARVEST_FAKE_LLM"):
        from harvest_core.fakes import FakeLLM

        generator = FakeLLM()
    else:
        from harvest_core.adapters.llm import LangChainLLM

        generator = LangChainLLM.from_config(config.llm.model, config.llm.api_key_env)

    from .census_gov import CensusGovSource

    return Backends(
        ds=PostgresDatastore(engine),
        clock=SystemClock(),
        kv=RedisKeyValue.from_url(os.environ[config.redis_url_env]),
        objects=S3ObjectStore(_s3_client(), os.environ["S3_BUCKET"]),
        sweep_queue=SqsQueue(sqs, os.environ["SWEEP_QUEUE_URL"]),
        code_queue=SqsQueue(sqs, os.environ["CODE_QUEUE_URL"]),
        fetch_queue=SqsQueue(sqs, os.environ["FETCH_QUEUE_URL"]),
        generator=generator,
        census=CensusGovSource(),
    )


def wire_fake(config: HarvestConfig) -> Backends:
    """All fakes — a fully offline orchestrator for local dry runs."""
    from harvest_core.fakes import (
        FakeDatastore,
        FakeKeyValue,
        FakeLLM,
        FakeObjectStore,
        FakeQueue,
        VirtualClock,
    )

    from .census import CensusPlace, CensusState

    class _TinyCensus:
        def load_state(self, state: str) -> CensusState:
            return CensusState(
                state_name=state,
                places=[
                    CensusPlace(name=f"{state} County", level="county"),
                    CensusPlace(
                        name=f"{state}ville", level="city", parent_name=f"{state} County"
                    ),
                ],
            )

    clock = VirtualClock()
    return Backends(
        ds=FakeDatastore(),
        clock=clock,
        kv=FakeKeyValue(clock),
        objects=FakeObjectStore(),
        sweep_queue=FakeQueue(clock),
        code_queue=FakeQueue(clock),
        fetch_queue=FakeQueue(clock),
        generator=FakeLLM(),
        census=_TinyCensus(),
    )


def wire(config: HarvestConfig, backend: str) -> Backends:
    if backend == "real":
        return wire_real(config)
    if backend == "fake":
        return wire_fake(config)
    raise ValueError(f"unknown backend {backend!r}")
