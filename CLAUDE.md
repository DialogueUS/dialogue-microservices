# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
uv sync                                   # set up the Python 3.12 workspace (all three packages)
uv run pytest                             # full suite; excludes @pytest.mark.live by default
uv run pytest harvester/tests/test_sweep_worker.py            # one file
uv run pytest harvester/tests/test_fanin.py -k out_of_order   # one test
uv run pytest -m live                     # live network tests only (real Municode crawl; needs `uv run playwright install chromium`)
uv run ruff check .                       # lint (--fix for autofixes)
uv run mypy .                             # typecheck
```

A change is done when the full `pytest`, `ruff check .`, and `mypy .` all stay green. The default suite runs entirely on in-memory fakes with a virtual clock — it must never need a live backend, network, or real sleep.

`NEW_HARVESTER.md` is the spec of record (v0.4). `OLD_HARVESTER.md` and `PLAN.md` are git-ignored working documents; the old spec documents rules carried forward (link extraction, sniff order, percent-encoding, storage keys, char caps) and explains *why* they exist.

## Architecture

Three uv-workspace packages; two deployable services bridged by SQS, one shared library:

- **`core/`** (`harvest_core`) — never deployed alone. Contains, in dependency order:
  - `constants.py`, `config.py` — every hardcoded number and the §9 config surface. Timeouts, caps, and extension sets live here only.
  - `domain.py`, `messages.py`, `errors.py` — table-row models + artifact state machine (`check_transition`), the three SQS message schemas (discriminated on `kind`), and the typed error surface shared by fakes and adapters (`UniqueViolation`, `RateLimited`, `TriageError`…).
  - `ports.py` — one `typing.Protocol` per external system (Datastore, TaskQueue, KeyValue, ObjectStore, SearchProvider, QueryGenerator, Triage, Fetcher, PortalDiscoverer, Clock).
  - `fakes/` — exactly one fake per port, with honest semantics (FakeQueue models per-message visibility + DLQ against the virtual clock; FakeDatastore raises the same `UniqueViolation` as Postgres). The fakes are themselves contract-tested (`core/tests/test_fakes_contract.py`) so scenario tests prove the spec, not fake quirks.
  - `adapters/` — the real implementations (SQLAlchemy/Postgres, boto3 SQS/S3, redis-py, Serper, LangChain, httpx). **Service code never imports an adapter**; adapters are chosen only in wiring/CLI modules (`orchestrator/.../wiring.py`, `harvester/.../cli.py`). `_protocol_checks.py` makes mypy verify that fakes and adapters satisfy the ports.
- **`orchestrator/`** — single-threaded planner: census seed → scope/target seed → dispatch (LLM query generation at dispatch time) → fetch reconciliation, one cycle per `--interval`. Queries live only in SQS messages, never Postgres.
- **`harvester/`** — thread-pool consumer with three roles: sweep (Serper → one batched LLM triage → single-page link extraction), code (Scrapy-Playwright portal discovery, heartbeat-extended visibility), fetch (download → sniff → dedupe → S3 + text extraction). Staging (`staging.py`) and fan-in (`fanin.py`) are shared by sweep and code workers.

Cross-service scenario tests live in the root `tests/` directory (both services against one shared fake world, scripting the spec's §7 failure table with exact virtual-time budgets).

## Load-bearing invariants (violating these breaks the failure model)

- **Postgres is the source of truth; SQS messages and Redis keys are disposable.** Every Redis answer is backstopped: URL/content dedupe by unique constraints and the (corpus, sha256) index; fan-in by the dispatch timeout. SETNX is written only *after* the authoritative insert/commit.
- **A rate limit (Serper 429) or LLM triage error records nothing at all** — no history row, no counter increment, message left undeleted so the same query text redelivers. Genuine errors retry in 1 day, never a full resweep interval.
- **Stamp-then-send, and the stamp is a compare-and-set** (`stamp_target` is gated on the previously read stamp) so concurrent orchestrators can't double-dispatch. Workers are gated on `dispatch_id` match and idempotent history insert on (dispatch_id, query_seq); handlers commit Postgres, then delete the message.
- **Two clocks cover each other:** SQS visibility (300 s; 900 s + heartbeat for code) recovers dead consumers; the dispatch timeout (1800 s serper / 7200 s code) recovers dead messages and lost counters. Code dispatch timeout must exceed the 3600 s crawl deadline.
- **Documents only, never web pages.** Two distinct extension sets: stored types vs followable-off-a-page (stored minus `.xml`/`.json`) — do not collapse them. Sniff order is fixed and HTML must be checked before XML (XHTML-stub trap). Hrefs are HTML-entity-unescaped; fetch URLs are percent-encoded preserving existing escapes.
- **Dedupe is corpus-scoped, not global** — overlapping corpora pay separately by design; corpora must stay independently killable (`purge` deletes rows FK-ordered + S3 prefix + Redis prefixes).

## Test-layout quirks

- Shared fixtures are in uniquely named modules (`orch_fixtures.py`, `harv_fixtures.py`, `scenario_world.py`, `pdf_fixture.py`), imported by tests directly; each `conftest.py` only re-imports the fixture function for registration. Don't add same-named helper modules across the three test dirs (pytest sys.path collisions), and add any new helper module name to the mypy override list in the root `pyproject.toml`.
- Time-dependent behavior is always tested through `VirtualClock` (`clock.advance(...)`) at exact boundaries (e.g. 1799 vs 1801 s) — never `time.sleep`.
- The Postgres adapter's unit tests run on SQLite; live-Postgres verification belongs to the manual smoke (README runbook, docker-compose + LocalStack). SQLite doesn't enforce FK ordering, so purge-order bugs only show against real Postgres.
