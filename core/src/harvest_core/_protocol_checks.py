"""Static conformance: fakes (and adapters, once built) satisfy the ports.

This module is never imported at runtime by services; mypy checks the
assignments below, which is the 1.3 verification that every fake and
adapter is a declared implementation of its port.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from . import ports
    from .adapters.fetcher import HttpxFetcher
    from .adapters.llm import LangChainLLM
    from .adapters.postgres import PostgresDatastore
    from .adapters.redis_kv import RedisKeyValue
    from .adapters.s3 import S3ObjectStore
    from .adapters.serper import SerperSearch
    from .adapters.sqs import SqsQueue
    from .adapters.system_clock import SystemClock
    from .fakes import (
        FakeDatastore,
        FakeFetcher,
        FakeKeyValue,
        FakeLLM,
        FakeObjectStore,
        FakePortalDiscoverer,
        FakeQueue,
        FakeSearch,
        VirtualClock,
    )

    def _check_fakes(
        clock: VirtualClock,
        queue: FakeQueue,
        ds: FakeDatastore,
        kv: FakeKeyValue,
        objects: FakeObjectStore,
        search: FakeSearch,
        llm: FakeLLM,
        fetcher: FakeFetcher,
        portal: FakePortalDiscoverer,
    ) -> None:
        _c: ports.Clock = clock
        _q: ports.TaskQueue = queue
        _d: ports.Datastore = ds
        _k: ports.KeyValue = kv
        _o: ports.ObjectStore = objects
        _s: ports.SearchProvider = search
        _g: ports.QueryGenerator = llm
        _t: ports.Triage = llm
        _f: ports.Fetcher = fetcher
        _p: ports.PortalDiscoverer = portal

    def _check_adapters(
        clock: SystemClock,
        queue: SqsQueue,
        ds: PostgresDatastore,
        kv: RedisKeyValue,
        objects: S3ObjectStore,
        search: SerperSearch,
        llm: LangChainLLM,
        fetcher: HttpxFetcher,
    ) -> None:
        _c: ports.Clock = clock
        _q: ports.TaskQueue = queue
        _d: ports.Datastore = ds
        _k: ports.KeyValue = kv
        _o: ports.ObjectStore = objects
        _s: ports.SearchProvider = search
        _g: ports.QueryGenerator = llm
        _t: ports.Triage = llm
        _f: ports.Fetcher = fetcher
