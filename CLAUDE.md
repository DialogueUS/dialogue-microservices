# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
uv sync                                   # set up the Python 3.12 workspace (all four packages)
uv run pytest                             # full suite; excludes @pytest.mark.live by default
uv run pytest harvester/tests/test_sweep_worker.py            # one file
uv run pytest harvester/tests/test_fanin.py -k out_of_order   # one test
uv run pytest records/tests/test_pr_scenarios.py              # public-records end-to-end
uv run pytest -m live                     # live network tests only (real Municode crawl; needs `uv run playwright install chromium`)
uv run ruff check .                       # lint (--fix for autofixes)
uv run mypy .                             # typecheck
```

A change is done when the full `pytest`, `ruff check .`, and `mypy .` all stay green. The default suite runs entirely on in-memory fakes with a virtual clock — it must never need a live backend, network, or real sleep.

Two specs of record, both tracked: `NEW_HARVESTER.md` (v0.4) for the harvesting system and `NEW_PUBLIC_RECORDS.md` for the public-records pipeline. The `§n.n` references throughout the code point into them — `core/`, `orchestrator/`, and `harvester/` cite `NEW_HARVESTER.md`; `records/` cites `NEW_PUBLIC_RECORDS.md`. `HARVESTER_PLAN.md` / `PUBLIC_RECORDS_PLAN.md` are the tracked build plans. `OLD_HARVESTER.md` is a git-ignored working document that records rules carried forward into *both* systems (link extraction, sniff order, percent-encoding, storage keys, char caps, address heuristics) and explains *why* they exist.

## Architecture

Four uv-workspace packages; three deployable services, one shared library. Two independent systems live here — the harvesting system (`orchestrator/` + `harvester/`) and the public-records pipeline (`records/`). **`core/` is the only thing they share**: no package depends on another service's package, so `records/` builds and runs with the harvesting packages absent entirely. Anything both systems need (the `CensusSource` port and its census.gov client, §3.1) belongs in `core/`, never in a service — put it there rather than reaching across.

### Shared library

- **`core/`** (`harvest_core`) — never deployed alone. Contains, in dependency order:
  - `constants.py`, `config.py` — every hardcoded number and the §9 config surface. Timeouts, caps, and extension sets live here only.
  - `domain.py`, `messages.py`, `errors.py` — table-row models + artifact state machine (`check_transition`), the three SQS message schemas (discriminated on `kind`), and the typed error surface shared by fakes and adapters (`UniqueViolation`, `RateLimited`, `TriageError`…).
  - `ports.py` — one `typing.Protocol` per external system (Datastore, TaskQueue, KeyValue, ObjectStore, SearchProvider, QueryGenerator, Triage, Fetcher, PortalDiscoverer, CensusSource, Clock).
  - `fakes/` — exactly one fake per port, with honest semantics (FakeQueue models per-message visibility + DLQ against the virtual clock; FakeDatastore raises the same `UniqueViolation` as Postgres). The fakes are themselves contract-tested (`core/tests/test_fakes_contract.py`) so scenario tests prove the spec, not fake quirks.
  - `adapters/` — the real implementations (SQLAlchemy/Postgres, boto3 SQS/S3, redis-py, Serper, LangChain, httpx, census.gov). **Service code never imports an adapter**; adapters are chosen only in wiring/CLI modules (`orchestrator/.../wiring.py`, `harvester/.../cli.py`, `records/.../cli.py:build_world`). `_protocol_checks.py` makes mypy verify that fakes and adapters satisfy the ports.

### Harvesting system (two services bridged by SQS)

- **`orchestrator/`** — single-threaded planner: census seed → scope/target seed → dispatch (LLM query generation at dispatch time) → fetch reconciliation, one cycle per `--interval`. Queries live only in SQS messages, never Postgres.
- **`harvester/`** — thread-pool consumer with three roles: sweep (Serper → one batched LLM triage → single-page link extraction), code (Scrapy-Playwright portal discovery, heartbeat-extended visibility), fetch (download → sniff → dedupe → S3 + text extraction). Staging (`staging.py`) and fan-in (`fanin.py`) are shared by sweep and code workers.

Cross-service scenario tests live in the root `tests/` directory (both services against one shared fake world, scripting the spec's §7 failure table with exact virtual-time budgets).

> **Temporarily disabled:** `harvest-orchestrator run` is currently inert — `cli.py` passes `enabled=False` to `run_forever` unless `HARVEST_ORCHESTRATOR_ENABLED=1` is set, so the loop idles and the harvester workers see empty queues. `run_cycle` is untouched, so the tests still exercise the real planner. Delete the marked block in `cli.py` and the matching branch in `loop.py` to restore.

### Public-records pipeline (one service, four queues)

**`records/`** (`public_records`, entry point `pr-records`) mails public-records requests to jurisdictions, then reads and reacts to the replies. `pr-records run` is a single process: the orchestrator loop plus four consumer threads plus a DLQ watcher, bridged by `pr-search-queries` / `pr-contacts` / `pr-followups` / `pr-inbound-mail`.

- It **reuses `harvest_core`'s generic ports** (Clock, TaskQueue, KeyValue, ObjectStore, SearchProvider, Fetcher, CensusSource) with their fakes and adapters, and defines only its own domain ports in `records/.../ports.py` (`RecordsStore`, `EmailTransport`, `ContactQueryGenerator`, `ContactPicker`, `EmailDrafter`, `InboundClassifier`). Never fork a generic port into `records/`; extend the core one.
- `world.py` is the records analogue of `wiring.py`: a single `World` dataclass holding every port. Orchestrator and consumers both take a `World`; tests build it from fakes, `cli.py` from real adapters. `escalate()` and `next_action_time()` live there too.
- **The orchestrator is the only component that originates work** (`seed_pass`, `poll_mail`, `followup_scan`, `send_digests`), each concern single-flight per tick — a run still in flight is skipped, not queued. Consumers only react: `scraper.py` (search → crawl → pick a contact), `sender.py` (initial requests + follow-up/clarification/fee-agreement jobs), `receiver.py` (match → classify → log → react), with `attachments.py`, `fees.py`, `digest.py` shared between them.
- The rest mirrors `core/`'s layout one-for-one: `constants.py` (every hardcoded number), `config.py` (the §11 per-campaign YAML, pydantic, validated at registration), `domain.py` (rows + the §4 thread state machine `check_transition`), `messages.py`, `errors.py`, `fakes/`, `adapters/`, `_protocol_checks.py`.

## Load-bearing invariants (violating these breaks the failure model)

### Both systems

- **Postgres is the source of truth; SQS messages and Redis keys are disposable.** Every Redis answer is backstopped by a database constraint or a timeout — harvester URL/content dedupe by unique constraints and the (corpus, sha256) index, fan-in by the dispatch timeout, records attachment dedupe by the digest embedded in the object key. SETNX is written only *after* the authoritative insert/commit.
- **Delivery is at-least-once and the handler owns the ack:** commit to Postgres first, delete the message second. A duplicate must be cheap and harmless, never a second side effect.
- **Engines and boot migrations go through `harvest_core.adapters.db`**, never `sa.create_engine` / `metadata.create_all` directly. `create_engine` adds the pre-ping and recycle a managed Postgres needs (failover and idle reaping close pooled connections silently, and these services idle for whole intervals between queries); `run_migration` wraps `create_all` in a transaction-scoped advisory lock on *the same connection as the DDL*, so simultaneous task starts serialize instead of racing its check-then-create. Note `create_all` never alters an existing table — a column change is manual DDL, not a redeploy.
- **A shutdown signal must raise, not set a flag.** Both `run` entry points sleep between passes, and PEP 475 resumes an interrupted `sleep` for its full remaining time once the handler returns — so a flag-setting handler leaves the process alive for a whole interval and it dies by SIGKILL instead. The handlers raise `KeyboardInterrupt`; keep it that way, and keep any container `stopTimeout` above the drain budget.

### Harvesting system

- **A rate limit (Serper 429) or LLM triage error records nothing at all** — no history row, no counter increment, message left undeleted so the same query text redelivers. Genuine errors retry in 1 day, never a full resweep interval.
- **Stamp-then-send, and the stamp is a compare-and-set** (`stamp_target` is gated on the previously read stamp) so concurrent orchestrators can't double-dispatch. Workers are gated on `dispatch_id` match and idempotent history insert on (dispatch_id, query_seq); handlers commit Postgres, then delete the message.
- **Two clocks cover each other:** SQS visibility (300 s; 900 s + heartbeat for code) recovers dead consumers; the dispatch timeout (1800 s serper / 7200 s code) recovers dead messages and lost counters. Code dispatch timeout must exceed the 3600 s crawl deadline.
- **Documents only, never web pages.** Two distinct extension sets: stored types vs followable-off-a-page (stored minus `.xml`/`.json`) — do not collapse them. Sniff order is fixed and HTML must be checked before XML (XHTML-stub trap). Hrefs are HTML-entity-unescaped; fetch URLs are percent-encoded preserving existing escapes.
- **Dedupe is corpus-scoped, not global** — overlapping corpora pay separately by design; corpora must stay independently killable (`purge` deletes rows FK-ordered + S3 prefix + Redis prefixes).

### Public-records pipeline

- **A send is a real legal request to a real government office.** Four gates in `sender.handle_contact`, in order: consent + active, the daily cap, the per-office cooldown (on the shared `jurisdictions` row, so overlapping campaigns throttle each other), and the anonymous-state refusal. `dry_run` (config default `true`) skips *only* the Resend call — every DB write still happens, so the whole flow rehearses. Registering a campaign never activates it; `consent_confirmed` and `start` are separate deliberate acts.
- **Handlers return `True` to delete the message, `False` to redeliver, and commit before deleting.** Transients (draft/classify/pick errors, Serper 429, transport 5xx, over the daily cap) return `False` and record *nothing*. Permanent conditions (purged campaign, already-resolved target, thread moved on) return `True` and drop the message. Raising leaves the message too, but loses the distinction — prefer the explicit return.
- **Every write is gated on a compare-and-set or an idempotency key**, because at-least-once delivery is assumed everywhere: `resolve_target` for every write that resolves a target (the three scraper outcomes and the seeded-contact path), `(campaign, message_id)` for inbound mail, `find_thread` / `outbound_reply_exists` / `followup_index` for sends, and `book_fee`'s atomic check-and-book for money. Fees are integer cents, booked to the ledger *before* the agreement email is enqueued; remittance has no payment rail and is always manual.
- **Run exactly one `pr-records` process.** `followup_scan` selects due threads without a lock and reschedules them non-atomically, so two schedulers enqueue the same follow-up twice and their sender threads both read `followups_sent` before either increments it — the sequential guard can't see a concurrent twin. Scale by threads inside the process, never by replicas (`desiredCount: 1`, no rolling overlap).
- **Shutdown drains rather than kills, because a send is not idempotent in the small.** `sender._deliver` calls Resend *before* `record_initial_send` writes the emails row, so a process killed in that window redelivers the message and mails the office a second time. `run_service` joins the consumer threads for `SHUTDOWN_DRAIN_S` (25 s — one 20 s long poll plus the handler); they drain concurrently, so every consumer gets that whole window to finish the handler it is already inside, and `process_queue` stops pulling further messages out of an already-received batch. Anything still running at the deadline is abandoned and redelivers. The window is only closed for signalled stops; SIGKILL or a hard crash can still double-send.
- **`pr-records kill` is deliberately narrower than the harvester's `purge`** — do not assume symmetry. It deletes campaign rows FK-ordered, the `dedupe:<slug>:*` Redis keys, and the `<slug>/` S3 prefix; `jurisdictions` rows with their contacts, the raw SES MIME, and in-flight SQS messages all survive (consumers drop messages whose campaign is gone).
- **Attachment bytes never ride in SQS** (256 KB cap): the mail poller spools them to `<slug>/inbox-spool/…` and passes keys. Dedupe is `SETNX` on the first 8 sha256 hex chars, which is also embedded in the object key — so a lost Redis rebuilds from an S3 listing without re-hashing (`rebuild_dedupe`).
- **Escalations are the human interface, and only a human leaves one.** Anything ambiguous (unclear reply, denial, personal-looking contact address, fee over budget, referral with no address) parks the thread at `needs_human`; `needs_human` / `failed` / terminal states have no automatic exit — `check_transition` only permits it with `by_human=True`.

## Test-layout quirks

- Shared fixtures are in uniquely named modules (`orch_fixtures.py`, `harv_fixtures.py`, `pr_fixtures.py`, `scenario_world.py`, `pdf_fixture.py`), imported by tests directly; each `conftest.py` only re-imports the fixture function for registration. Don't add same-named helper modules across the five test dirs (pytest sys.path collisions), and add any new helper module name to the mypy override list in the root `pyproject.toml`.
- Records tests all carry the `test_pr_` prefix for the same reason; the pipeline's end-to-end scenarios are in `records/tests/test_pr_scenarios.py`, not the root `tests/` (which is harvester-only).
- Time-dependent behavior is always tested through `VirtualClock` (`clock.advance(...)`) at exact boundaries (e.g. 1799 vs 1801 s) — never `time.sleep`.
- The Postgres adapter's unit tests run on SQLite; live-Postgres verification belongs to the manual smoke (README runbook, docker-compose + LocalStack). SQLite doesn't enforce FK ordering, so purge-order bugs only show against real Postgres.
