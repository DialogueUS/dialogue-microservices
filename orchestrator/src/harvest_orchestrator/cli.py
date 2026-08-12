"""Orchestrator CLI: run loop, migrate, run switch, code-source import, purge."""

from __future__ import annotations

import argparse
import csv
import logging
import os
import signal
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType

from harvest_core.config import load_config
from harvest_core.constants import HEALTH_PROBE_TIMEOUT_S
from harvest_core.domain import Publisher, RunState
from harvest_core.errors import UniqueViolation
from harvest_core.health import HealthResponder, probe

from .loop import SERVICE_NAME, Orchestrator
from .ops import purge_corpus
from .wiring import wire

# ---- TEMPORARY: harvester orchestration is disabled -----------------------
# `harvest-orchestrator run` wires up and stays alive, but seeds, dispatches,
# and reconciles nothing, so the harvester workers see empty queues. Set
# HARVEST_ORCHESTRATOR_ENABLED=1 to turn it back on without a code change.
#
# To restore permanently: delete this block and the `enabled=` argument below,
# plus the matching branch in loop.py:run_forever.
#
# Scoped to the harvesting system only. The public-records pipeline is a
# separate process (`pr-records run`) that imports nothing from this package,
# so it is unaffected.
ORCHESTRATION_ENABLE_ENV_VAR = "HARVEST_ORCHESTRATOR_ENABLED"
# ---------------------------------------------------------------------------


def _cmd_run(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    backends = wire(config, args.backend)
    orchestrator = Orchestrator(
        config=config,
        ds=backends.ds,
        clock=backends.clock,
        generator=backends.generator,
        census=backends.census,
        sweep_queue=backends.sweep_queue,
        code_queue=backends.code_queue,
        fetch_queue=backends.fetch_queue,
    )

    # Health: answer pings on Redis pub/sub for as long as the loop lives.
    # Its own thread, because the loop spends nearly all its time asleep
    # between intervals and a probe cannot wait that long for an answer.
    health_stop = threading.Event()
    responder = HealthResponder(backends.pubsub, SERVICE_NAME, orchestrator.health)
    health_thread = threading.Thread(
        target=responder.serve_forever, args=(health_stop,), name="health", daemon=True
    )

    def _shutdown(signum: int, frame: FrameType | None) -> None:
        logging.getLogger(__name__).info("signal %s: shutting down", signum)
        orchestrator.stop()
        health_stop.set()
        # Setting the flag alone leaves the interrupted clock.sleep to resume
        # for its full remaining interval (PEP 475), so the process outlives
        # its supervisor's grace period and gets killed instead. Raise.
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    health_thread.start()
    try:
        orchestrator.run_forever(
            args.interval,
            enabled=os.environ.get(ORCHESTRATION_ENABLE_ENV_VAR) == "1",
        )
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("stopped")
    finally:
        health_stop.set()
    return 0


def _cmd_healthcheck(args: argparse.Namespace) -> int:
    """Ping the running orchestrator; exit 0 healthy, 1 not.

    Deliberately does not call `wire()`: that opens Postgres and runs the
    boot migration, which a check running every thirty seconds must never
    do. The config is read only for the name of the Redis URL variable.
    """
    from harvest_core.adapters.redis_pubsub import RedisPubSub

    config = load_config(args.config)
    status = probe(
        RedisPubSub.from_url(os.environ[config.redis_url_env]),
        SERVICE_NAME,
        timeout_s=args.timeout,
    )
    if status.healthy:
        print(f"healthy{f' ({status.detail})' if status.detail else ''}")
        return 0
    print(f"unhealthy: {status.failures()}", file=sys.stderr)
    return 1


def _cmd_migrate(args: argparse.Namespace) -> int:
    from harvest_core.adapters.db import create_engine
    from harvest_core.adapters.postgres import migrate

    migrate(create_engine(os.environ["DATABASE_URL"]))
    print("migration complete")
    return 0


def _cmd_switch(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    backends = wire(config, args.backend)
    backends.ds.set_run_state(config.name, RunState(args.state))
    print(f"{config.name}: {args.state}")
    return 0


def _cmd_import_code_sources(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    backends = wire(config, args.backend)
    ds = backends.ds
    inserted = skipped = 0
    with Path(args.csv).open() as fh:
        for row in csv.DictReader(fh):
            matches = [
                j
                for j in ds.list_jurisdictions(states=[row["state"].strip()])
                if j.name == row["jurisdiction"].strip()
            ]
            if not matches:
                print(f"no jurisdiction match: {row['jurisdiction']}, {row['state']}")
                skipped += 1
                continue
            try:
                ds.insert_code_source(
                    matches[0].id,
                    row["url"].strip(),
                    Publisher(row["publisher"].strip()),
                    enabled=True,
                    added_by=row.get("added_by", "csv-import"),
                    added_at=datetime.now(UTC),
                )
                inserted += 1
            except UniqueViolation:
                skipped += 1
    print(f"inserted {inserted}, skipped {skipped}")
    return 0


def _cmd_purge(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    if not args.yes:
        print("purge is a permanent kill; re-run with --yes to confirm", file=sys.stderr)
        return 2
    backends = wire(config, args.backend)
    counts = purge_corpus(backends.ds, backends.kv, backends.objects, config.name)
    print(f"purged {config.name}: {counts}")
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(prog="harvest-orchestrator")
    sub = parser.add_subparsers(dest="command", required=True)

    def _common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--config", required=True, help="path to harvest config YAML")
        p.add_argument("--backend", choices=["real", "fake"], default="real")

    p_run = sub.add_parser("run", help="run the orchestrator loop")
    _common(p_run)
    p_run.add_argument("--interval", type=float, default=60.0)
    p_run.set_defaults(func=_cmd_run)

    p_migrate = sub.add_parser("migrate", help="apply the idempotent boot migration")
    p_migrate.set_defaults(func=_cmd_migrate)

    p_health = sub.add_parser(
        "healthcheck", help="ping the running loop over Redis pub/sub (exit 0 = healthy)"
    )
    # No --backend: this talks to whatever process is already running, and
    # reads the config only for the name of the Redis URL variable.
    p_health.add_argument("--config", required=True, help="path to harvest config YAML")
    p_health.add_argument("--timeout", type=float, default=HEALTH_PROBE_TIMEOUT_S)
    p_health.set_defaults(func=_cmd_healthcheck)

    p_switch = sub.add_parser("switch", help="flip the run switch")
    _common(p_switch)
    p_switch.add_argument("state", choices=["running", "stopped"])
    p_switch.set_defaults(func=_cmd_switch)

    p_import = sub.add_parser(
        "import-code-sources",
        help="CSV import (columns: jurisdiction,state,url,publisher)",
    )
    _common(p_import)
    p_import.add_argument("csv")
    p_import.set_defaults(func=_cmd_import_code_sources)

    p_purge = sub.add_parser("purge", help="kill-as-purge: rows + S3 + Redis prefixes")
    _common(p_purge)
    p_purge.add_argument("--yes", action="store_true")
    p_purge.set_defaults(func=_cmd_purge)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
