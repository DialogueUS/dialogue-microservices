"""Process wiring and CLI. Real adapters are chosen here and only here.

Commands:
    pr-records migrate                      boot-migrate the database
    pr-records register <campaign.yaml>     create the campaign row
    pr-records start|stop <name>            flip campaigns.active
    pr-records kill <name>                  permanent purge
    pr-records run                          orchestrator + all consumers
"""

from __future__ import annotations

import argparse
import logging
import os
import threading

from .config import load_campaign_config
from .constants import (
    ORCHESTRATOR_PERIOD_S,
    QUEUE_CONTACTS,
    QUEUE_FOLLOWUPS,
    QUEUE_INBOUND,
    QUEUE_SEARCH,
    SCRAPER_HTTP_TIMEOUT_S,
)
from .consumer import drain_dlq, process_queue
from .ops import kill_campaign, register_campaign, set_active
from .orchestrator import Orchestrator
from .receiver import handle_inbound
from .scraper import handle_search_message
from .sender import handle_contact, handle_followup_job
from .world import World

log = logging.getLogger("public_records.cli")


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"missing required environment variable {name}")
    return value


def build_world() -> World:
    """Wire the real adapters from the §11 service-level environment."""
    import boto3
    import sqlalchemy as sa
    from harvest_core.adapters.fetcher import HttpxFetcher
    from harvest_core.adapters.redis_kv import RedisKeyValue
    from harvest_core.adapters.s3 import S3ObjectStore
    from harvest_core.adapters.serper import SerperSearch
    from harvest_core.adapters.sqs import SqsQueue
    from harvest_core.adapters.system_clock import SystemClock
    from harvest_orchestrator.census_gov import CensusGovSource

    from .adapters.llm import LunaContacts, TerraCorrespondence
    from .adapters.resend import ResendTransport
    from .adapters.sql_store import SqlRecordsStore, migrate

    engine = sa.create_engine(_require_env("DATABASE_URL"))
    migrate(engine)
    s3 = boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
    sqs = boto3.client("sqs", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
    luna = LunaContacts.from_env()
    terra = TerraCorrespondence.from_env()
    return World(
        clock=SystemClock(),
        store=SqlRecordsStore(engine),
        kv=RedisKeyValue.from_url(_require_env("REDIS_URL")),
        mail_bucket=S3ObjectStore(s3, _require_env("PR_MAIL_BUCKET")),
        documents=S3ObjectStore(s3, _require_env("PR_DOCUMENTS_BUCKET")),
        search_queue=SqsQueue(sqs, _require_env("PR_SEARCH_QUEUE_URL")),
        contacts_queue=SqsQueue(sqs, _require_env("PR_CONTACTS_QUEUE_URL")),
        followups_queue=SqsQueue(sqs, _require_env("PR_FOLLOWUPS_QUEUE_URL")),
        inbound_queue=SqsQueue(sqs, _require_env("PR_INBOUND_QUEUE_URL")),
        search=SerperSearch(_require_env("SERPER_API_KEY")),
        fetcher=HttpxFetcher(timeout=SCRAPER_HTTP_TIMEOUT_S),
        query_generator=luna,
        picker=luna,
        drafter=terra,
        classifier=terra,
        transport=ResendTransport(_require_env("RESEND_API_KEY")),
        from_address=_require_env("PR_FROM_ADDRESS"),
        census=CensusGovSource(),
    )


def run_service(world: World) -> None:
    """One process: the orchestrator plus the three consumer loops (and
    DLQ watchers when the *_DLQ_URL variables are set)."""
    import boto3
    from harvest_core.adapters.sqs import SqsQueue

    stop = threading.Event()
    orchestrator = Orchestrator(world, period_s=ORCHESTRATOR_PERIOD_S)

    def consumer_loop(queue: object, handler: object) -> None:
        while not stop.is_set():
            try:
                process_queue(world, queue, handler)  # type: ignore[arg-type]
            except Exception:
                log.exception("consumer loop error")
                world.clock.sleep(5)

    loops = [
        threading.Thread(
            target=consumer_loop, args=(world.search_queue, handle_search_message),
            name="scraper", daemon=True,
        ),
        threading.Thread(
            target=consumer_loop, args=(world.contacts_queue, handle_contact),
            name="sender-contacts", daemon=True,
        ),
        threading.Thread(
            target=consumer_loop, args=(world.followups_queue, handle_followup_job),
            name="sender-followups", daemon=True,
        ),
        threading.Thread(
            target=consumer_loop, args=(world.inbound_queue, handle_inbound),
            name="receiver", daemon=True,
        ),
    ]

    dlq_env = {
        QUEUE_SEARCH: "PR_SEARCH_DLQ_URL",
        QUEUE_CONTACTS: "PR_CONTACTS_DLQ_URL",
        QUEUE_FOLLOWUPS: "PR_FOLLOWUPS_DLQ_URL",
        QUEUE_INBOUND: "PR_INBOUND_DLQ_URL",
    }
    sqs = boto3.client("sqs", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
    dlqs = {
        name: SqsQueue(sqs, os.environ[env])
        for name, env in dlq_env.items()
        if os.environ.get(env)
    }

    def dlq_loop() -> None:
        while not stop.is_set():
            for name, dlq in dlqs.items():
                try:
                    drain_dlq(world, name, dlq)
                except Exception:
                    log.exception("dlq watcher error for %s", name)
            world.clock.sleep(60)

    if dlqs:
        loops.append(threading.Thread(target=dlq_loop, name="dlq-watcher", daemon=True))

    for loop in loops:
        loop.start()
    try:
        orchestrator.run_forever()
    except KeyboardInterrupt:
        log.info("shutting down")
    finally:
        orchestrator.stop()
        stop.set()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(prog="pr-records")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate")
    register = sub.add_parser("register")
    register.add_argument("config_path")
    for name in ("start", "stop", "kill"):
        cmd = sub.add_parser(name)
        cmd.add_argument("name")
    sub.add_parser("run")
    args = parser.parse_args()

    world = build_world()  # migrates as a side effect
    if args.command == "migrate":
        print("migrated")
    elif args.command == "register":
        campaign = register_campaign(world, load_campaign_config(args.config_path))
        print(f"registered campaign {campaign.name!r} (id {campaign.id}); "
              "it is inactive until `start`")
    elif args.command in ("start", "stop"):
        campaign = set_active(world, args.name, args.command == "start")
        print(f"campaign {campaign.name!r} active={campaign.active}")
    elif args.command == "kill":
        counts = kill_campaign(world, args.name)
        print(f"purged: {counts}")
    elif args.command == "run":
        run_service(world)


if __name__ == "__main__":
    main()
