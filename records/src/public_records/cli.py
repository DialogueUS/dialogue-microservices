"""Process wiring and CLI. Real adapters are chosen here and only here.

Commands:
    pr-records migrate                      boot-migrate the database
    pr-records register <campaign.yaml>     create the campaign row
    pr-records start|stop <name>            flip campaigns.active
    pr-records kill <name>                  permanent purge
    pr-records run                          orchestrator + all consumers
    pr-records healthcheck                  ping a running `run` over Redis
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from types import FrameType

from harvest_core.constants import HEALTH_PROBE_TIMEOUT_S
from harvest_core.health import HealthResponder, probe

from .config import load_campaign_config
from .constants import (
    ORCHESTRATOR_PERIOD_S,
    QUEUE_CONTACTS,
    QUEUE_FOLLOWUPS,
    QUEUE_INBOUND,
    QUEUE_SEARCH,
    SCRAPER_HTTP_TIMEOUT_S,
    SHUTDOWN_DRAIN_S,
)
from .consumer import drain_dlq, process_queue
from .health import SERVICE_NAME, build_status
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
    from harvest_core.adapters.census_gov import CensusGovSource
    from harvest_core.adapters.db import create_engine
    from harvest_core.adapters.fetcher import HttpxFetcher
    from harvest_core.adapters.redis_kv import RedisKeyValue
    from harvest_core.adapters.redis_pubsub import RedisPubSub
    from harvest_core.adapters.s3 import S3ObjectStore
    from harvest_core.adapters.serper import SerperSearch
    from harvest_core.adapters.sqs import SqsQueue
    from harvest_core.adapters.system_clock import SystemClock

    from .adapters.llm import LunaContacts, TerraCorrespondence
    from .adapters.resend import ResendTransport
    from .adapters.sql_store import SqlRecordsStore, migrate

    engine = create_engine(_require_env("DATABASE_URL"))
    migrate(engine)
    s3 = boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
    sqs = boto3.client("sqs", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
    luna = LunaContacts.from_env()
    terra = TerraCorrespondence.from_env()
    redis_url = _require_env("REDIS_URL")
    return World(
        clock=SystemClock(),
        store=SqlRecordsStore(engine),
        kv=RedisKeyValue.from_url(redis_url),
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
        # Its own client: a subscribed connection cannot serve KeyValue.
        pubsub=RedisPubSub.from_url(redis_url),
    )


def run_service(world: World) -> None:
    """One process: the orchestrator plus the three consumer loops (and
    DLQ watchers when the *_DLQ_URL variables are set)."""
    import boto3
    from harvest_core.adapters.sqs import SqsQueue

    stop = threading.Event()
    orchestrator = Orchestrator(world, period_s=ORCHESTRATOR_PERIOD_S)

    def sleep_or_stop(seconds: float) -> None:
        """Sleep in slices so a shutdown isn't waited out to the end."""
        remaining = seconds
        while remaining > 0 and not stop.is_set():
            world.clock.sleep(min(1.0, remaining))
            remaining -= 1.0

    def consumer_loop(queue: object, handler: object) -> None:
        while not stop.is_set():
            try:
                process_queue(world, queue, handler, stop=stop)  # type: ignore[arg-type]
            except Exception:
                log.exception("consumer loop error")
                sleep_or_stop(5)

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
            sleep_or_stop(60)

    if dlqs:
        loops.append(threading.Thread(target=dlq_loop, name="dlq-watcher", daemon=True))

    # Health: answer pings on Redis pub/sub, reporting on the threads above
    # and the orchestrator's tick. Kept out of `loops` on purpose — it is not
    # part of the send drain, and it must not report on itself.
    health_stop = threading.Event()
    health_thread: threading.Thread | None = None
    if world.pubsub is not None:
        responder = HealthResponder(
            world.pubsub,
            SERVICE_NAME,
            lambda: build_status(loops, orchestrator.beat),
        )
        health_thread = threading.Thread(
            target=responder.serve_forever,
            args=(health_stop,),
            name="health",
            daemon=True,
        )

    # A send is a real legal request, and `_deliver` runs before the emails
    # row is written: a task killed in that window redelivers and mails the
    # office twice. So the container's stop signal has to drain rather than
    # kill. Setting a flag is not enough — under PEP 475 a handler that
    # returns leaves the interrupted clock.sleep to resume for its full
    # remaining time (up to ORCHESTRATOR_PERIOD_S), which outlives any
    # supervisor's grace period. Raising is what actually unwinds the sleep;
    # KeyboardInterrupt reuses the shutdown path already below.
    def _shutdown(signum: int, frame: FrameType | None) -> None:
        log.info("signal %s: draining consumers", signum)
        orchestrator.stop()
        stop.set()
        health_stop.set()
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    for loop in loops:
        loop.start()
    # Strictly after the consumers: an unstarted thread reads as dead, so a
    # responder that answered first would report four dead consumers, and
    # three of those at 30 s intervals kills the task inside its startPeriod.
    if health_thread is not None:
        health_thread.start()
    try:
        orchestrator.run_forever()
    except KeyboardInterrupt:
        log.info("shutting down")
    finally:
        orchestrator.stop()
        stop.set()
        # Stop answering before the drain, not after: a container health
        # check that keeps passing through shutdown just delays the
        # replacement task.
        health_stop.set()
        # Consumers are daemons: without the join the interpreter exits and
        # takes any in-flight handler with it, mid-send. The budget is one
        # shared wall-clock window — the threads drain concurrently, so each
        # gets the whole of it to finish the handler it is already inside.
        deadline = float(SHUTDOWN_DRAIN_S)
        for loop in loops:
            start = time.monotonic()
            loop.join(timeout=max(0.0, deadline))
            deadline -= time.monotonic() - start
            if loop.is_alive():
                log.warning("consumer %s did not finish its pass in time", loop.name)


def healthcheck(timeout_s: float) -> int:
    """Ping the running service; exit 0 healthy, 1 not.

    Never goes through `build_world`. That opens Postgres and runs the boot
    migration on the way past, which a check running every thirty seconds
    must not do — REDIS_URL is the whole of what a probe needs.
    """
    from harvest_core.adapters.redis_pubsub import RedisPubSub

    status = probe(
        RedisPubSub.from_url(_require_env("REDIS_URL")),
        SERVICE_NAME,
        timeout_s=timeout_s,
    )
    if status.healthy:
        print("healthy")
        return 0
    print(f"unhealthy: {status.failures()}", file=sys.stderr)
    return 1


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
    health = sub.add_parser("healthcheck")
    health.add_argument("--timeout", type=float, default=HEALTH_PROBE_TIMEOUT_S)
    args = parser.parse_args()

    # Before build_world, deliberately: see the docstring above.
    if args.command == "healthcheck":
        raise SystemExit(healthcheck(args.timeout))

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
