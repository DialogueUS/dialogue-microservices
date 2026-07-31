"""Harvester CLI: --role sweep|code|fetch|all, --threads N."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
from types import FrameType

from harvest_core.config import HarvestConfig, load_config
from harvest_core.ports import Fetcher, PortalDiscoverer, SearchProvider, Triage

from .service import ROLES, HarvesterDeps, HarvesterService


def wire_real(config: HarvestConfig) -> HarvesterDeps:
    import sqlalchemy as sa
    from harvest_core.adapters.fetcher import HttpxFetcher
    from harvest_core.adapters.postgres import PostgresDatastore, migrate
    from harvest_core.adapters.redis_kv import RedisKeyValue
    from harvest_core.adapters.s3 import S3ObjectStore
    from harvest_core.adapters.sqs import SqsQueue
    from harvest_core.adapters.system_clock import SystemClock

    engine = sa.create_engine(os.environ["DATABASE_URL"])
    migrate(engine)
    clock = SystemClock()

    import boto3

    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    sqs = boto3.client("sqs", endpoint_url=endpoint) if endpoint else boto3.client("sqs")
    s3 = boto3.client("s3", endpoint_url=endpoint) if endpoint else boto3.client("s3")

    search: SearchProvider
    triage: Triage
    if os.environ.get("HARVEST_FAKE_SEARCH"):
        from harvest_core.fakes import FakeSearch

        search = FakeSearch()
    else:
        from harvest_core.adapters.serper import SerperSearch

        search = SerperSearch(api_key=os.environ[config.serper_api_key_env])
    if os.environ.get("HARVEST_FAKE_LLM"):
        from harvest_core.fakes import FakeLLM

        triage = FakeLLM()
    else:
        from harvest_core.adapters.llm import LangChainLLM

        triage = LangChainLLM.from_config(config.llm.model, config.llm.api_key_env)

    discoverer: PortalDiscoverer
    if os.environ.get("HARVEST_FAKE_PORTAL"):
        from harvest_core.fakes import FakePortalDiscoverer

        discoverer = FakePortalDiscoverer()
    else:
        from .spiders.discoverer import PlaywrightPortalDiscoverer

        discoverer = PlaywrightPortalDiscoverer(clock)

    fetcher: Fetcher = HttpxFetcher()
    return HarvesterDeps(
        ds=PostgresDatastore(engine),
        kv=RedisKeyValue.from_url(os.environ[config.redis_url_env]),
        objects=S3ObjectStore(s3, os.environ["S3_BUCKET"]),
        search=search,
        triage=triage,
        fetcher=fetcher,
        discoverer=discoverer,
        sweep_queue=SqsQueue(sqs, os.environ["SWEEP_QUEUE_URL"]),
        code_queue=SqsQueue(sqs, os.environ["CODE_QUEUE_URL"]),
        fetch_queue=SqsQueue(sqs, os.environ["FETCH_QUEUE_URL"]),
        clock=clock,
    )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(prog="harvest-harvester")
    parser.add_argument("--config", required=True, help="path to harvest config YAML")
    parser.add_argument("--role", choices=ROLES, default="all")
    parser.add_argument("--threads", type=int, default=8)
    args = parser.parse_args(argv)

    config = load_config(args.config)
    service = HarvesterService(wire_real(config), config)

    def _shutdown(signum: int, frame: FrameType | None) -> None:
        logging.getLogger(__name__).info("signal %s: draining", signum)
        service.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    service.run(args.role, args.threads)
    return 0


if __name__ == "__main__":
    sys.exit(main())
