"""Orchestrator CLI: run loop, migrate, run switch, code-source import, purge."""

from __future__ import annotations

import argparse
import csv
import logging
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType

from harvest_core.config import load_config
from harvest_core.domain import Publisher, RunState
from harvest_core.errors import UniqueViolation

from .loop import Orchestrator
from .ops import purge_corpus
from .wiring import wire


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

    def _shutdown(signum: int, frame: FrameType | None) -> None:
        logging.getLogger(__name__).info("signal %s: shutting down", signum)
        orchestrator.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    orchestrator.run_forever(args.interval)
    return 0


def _cmd_migrate(args: argparse.Namespace) -> int:
    import os

    import sqlalchemy as sa
    from harvest_core.adapters.postgres import migrate

    migrate(sa.create_engine(os.environ["DATABASE_URL"]))
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
