# Implementation plan — NEW_HARVESTER.md (v0.4)

Two deployable services as separate pyprojects, plus one shared library
package that both depend on. All tests run against **in-memory fakes**
(Postgres, SQS, Redis, S3, Serper, LLM, HTTP, portal spider) behind a
ports-and-adapters data layer; no test ever needs a live backend.
Integration against real backends is a manual, optional smoke step at
the end.

## Layout & stack

```
dialogue-microservices/
├── core/            # harvest-core: domain models, config, ports, fakes, real adapters
├── orchestrator/    # harvest-orchestrator: seed, generate, dispatch, reconcile
├── harvester/       # harvest-harvester: sweep / code / fetch workers
├── docker-compose.yml   (Postgres + LocalStack + Redis, manual smoke only)
├── NEW_HARVESTER.md
└── PLAN.md
```

- Python 3.12, **uv workspace** with three members; pytest, ruff, mypy.
- pydantic v2 (config + message schemas), SQLAlchemy 2 (Postgres
  adapter), boto3 (SQS/S3), redis-py, langchain + langchain-openai
  (GPT-5.6-luna), Scrapy + scrapy-playwright, httpx (fetching), pypdf
  (extraction).
- Ports are `typing.Protocol`s in `core.ports`; every external system
  has exactly one fake in `core.fakes` and one real adapter in
  `core.adapters`. Services depend on ports only; adapters are chosen
  at process wiring time (CLI flag / env).
- A **`Clock` port** (now(), sleep()) is injected everywhere times
  matter — visibility timeouts, dispatch timeouts, throttles, and
  fan-in TTLs are all tested with a virtual clock, never `time.sleep`.

Conventions for every step below: code + tests land together; a step is
done when its **Verify** line passes plus `ruff check`, `mypy`, and the
full `pytest` suite stay green.

---

## Phase 0 — scaffolding

- [x] **0.1 Workspace and packages.** Create the uv workspace with the
  three pyprojects (`harvest-core`, `harvest-orchestrator`,
  `harvest-harvester`), the two services depending on `harvest-core`
  (workspace source). Wire pytest/ruff/mypy config at the root; add a
  placeholder test per package.
  - **Verify:** `uv sync && uv run pytest` collects and passes 3
    placeholder tests; `uv run ruff check .` and `uv run mypy .` pass.

## Phase 1 — core: contracts before behavior

- [x] **1.1 Config model.** Pydantic model of the §9 config surface
  (mode, name, scope.*, topics, resweep_interval_days, search_count,
  queries_per_jurisdiction, llm.*, serper/redis env keys, caps) with
  spec defaults, plus the hardcoded-constants module (visibility 300 s,
  dispatch timeout 1800/7200 s, deadline 3600 s, batch sizes, byte/char
  caps, level priorities...). YAML loader.
  - **Verify:** unit tests — a minimal YAML yields every §9 default; a
    full YAML round-trips; `mode != harvest` and bad scope values are
    rejected.

- [x] **1.2 Domain + message schemas.** Dataclasses/pydantic for
  Jurisdiction, SweepTarget (corpus × jurisdiction × source), Artifact
  (with the truncated state machine enum), SweepHistory, CodeSource,
  RunSwitch; the three SQS message types (`sweep` with
  query_text/dispatch_id/query_seq/query_count, `code` with
  portal_urls, `fetch` with artifact_id) with JSON serialization.
  - **Verify:** round-trip tests (model → JSON → model) for all three
    messages; artifact state-transition helper rejects illegal
    transitions (e.g. `fetched` → `pending`).

- [x] **1.3 Ports.** Protocols: `Datastore` (repo methods both services
  need: jurisdictions, sweep targets incl. `select_due(corpus, limit)`
  and `finalize(dispatch_id, ...)`, artifacts incl. unique-violation
  signaling, history idempotent-insert on (dispatch_id, query_seq),
  code sources, run switch), `TaskQueue` (send, receive(max),
  delete, change_visibility), `KeyValue` (setnx, hincrby-with-get,
  expire, delete_prefix), `ObjectStore` (put, presign), `SearchProvider`
  (search → results | RateLimited), `QueryGenerator` + `Triage` (LLM),
  `Fetcher` (get with byte cap → bytes/status), `PortalDiscoverer`
  (discover(portal_urls, publisher) → candidates), `Clock`.
  - **Verify:** mypy passes with both fakes (1.4) and adapters (1.5)
    declared as implementations; no service code imports an adapter.

- [x] **1.4 Fakes.** In-memory implementations with honest semantics:
  FakeQueue (per-message visibility deadline against the virtual clock,
  receive hides in-flight messages, redelivery after timeout,
  receive-count → DLQ list), FakeDatastore (unique constraints raise
  the same violation type as the real adapter), FakeKeyValue (SETNX,
  HINCRBY, TTL expiry via clock), FakeObjectStore, FakeSearch (canned
  results, programmable 429), FakeLLM (canned generations/triage,
  programmable failure), FakeFetcher (URL → (status, bytes,
  content_type) fixtures), FakePortalDiscoverer, VirtualClock.
  - **Verify:** contract tests for the fakes themselves — a received
    message is invisible until the clock passes its deadline, then
    redelivered with receive_count+1 and DLQ'd after 3; SETNX returns
    False on the second call; duplicate sweep-target insert raises.

- [x] **1.5 Real adapters.** SQLAlchemy schema + idempotent
  boot-migration (per old §10); Postgres datastore; boto3 SQS queue
  (long poll 20 s, ChangeMessageVisibility); redis-py KV; boto3 S3
  store (bucket posture per spec); Serper client (429 → RateLimited);
  LangChain GPT-5.6-luna generator/triage with structured output
  (parse failure → typed error, never an exception escaping); httpx
  fetcher (byte caps, percent-encoding preserving existing escapes).
  - **Verify:** unit tests with stubbed transports (respx / botocore
    stubber / fakeredis) asserting request shapes: 429 maps to
    RateLimited, SQS visibility call parameters, S3 key scheme
    `<corpus-slug>/<jur-slug>/<hash8>_<filename>`, percent-encoding
    cases from the old spec (spaces, pre-escaped, `&amp;` unescape
    lives in extraction not here). Live-backend checks deferred to 4.2.

## Phase 2 — orchestrator

- [x] **2.1 Census seeding.** Jurisdiction loader behind a
  `CensusSource` port (fixture file in tests): per-state load with the
  state-row-exists marker, comma-county exclusion, federal anchor row
  on demand, 600 s throttle.
  - **Verify:** tests — zero-place state (Hawaii fixture) is not
    reloaded every cycle; comma-county rows excluded from scope
    resolution but retained; loader is idempotent (second run inserts
    nothing).

- [x] **2.2 Scope resolution + target seeding.** Resolve scope.*
  (levels/states/within/only/region_query) to jurisdictions; insert
  missing sweep_targets: `serper` for all in-scope, `legal_codes` only
  where an enabled code_source exists; park `legal_codes` rows whose
  seeds are all disabled; level priorities.
  - **Verify:** tests — a jurisdiction gaining a code_source gets a
    `legal_codes` row on the next seed pass; disabling the seed parks
    it one resweep interval out; unique (corpus, jurisdiction, source)
    race loses harmlessly (fake raises, pass continues).

- [x] **2.3 Query generation.** FakeLLM-driven: one call per
  jurisdiction covering all topics, structured JSON out,
  1–`queries_per_jurisdiction` per topic, ≤ 200 chars each; failure
  isolation (skip jurisdiction, row untouched).
  - **Verify:** tests — N topics → one LLM call; parse failure leaves
    the row due and the other jurisdictions in the batch dispatched;
    over-long/extra queries are clamped.

- [x] **2.4 Dispatch.** Due selection (`next_due_at <= now`, stamp null
  or older than dispatch timeout — 1800 s serper / 7200 s code),
  ordering priority → next_due_at, cap `max_sweeps_per_dispatch`;
  serper path: generate → stamp (dispatched_at, dispatch_id,
  query_count) → commit → send N messages; code path: read enabled
  seeds → stamp query_count=1 → send one code task.
  - **Verify:** tests with FakeQueue + VirtualClock — federal precedes
    cities; stamp-then-send (kill the queue between stamp and send:
    row recovers via timeout, no duplicate stamp); advancing the clock
    past the timeout re-dispatches with a fresh dispatch_id; a stamped
    row inside the window is never re-selected.

- [x] **2.5 Fetch reconciliation.** Re-publish `pending` artifacts with
  stamp null or stale, cap `max_fetch_redispatch`.
  - **Verify:** tests — artifact staged but never published (simulated
    sweep-worker crash) is re-published after timeout; recently
    stamped artifacts are left alone; cap respected.

- [x] **2.6 Main loop + CLI.** Single-threaded cycle: run-switch read →
  seed (throttled) → dispatch → reconcile, per running config;
  `--interval`; graceful shutdown; wiring of real vs fake adapters.
  - **Verify:** loop test on fakes — a `stopped` config dispatches
    nothing; flipping the switch mid-test stops new dispatch within one
    cycle; two orchestrators running the same cycle produce no
    duplicate effective dispatch (loser hits stamp gate).

## Phase 3 — harvester

- [x] **3.1 Consumer framework.** Thread pool with `--role
  sweep|code|fetch|all`; per-message idempotency gate (run switch off →
  delete; dispatch_id mismatch / state already past → delete);
  commit-then-delete helper; heartbeat helper (extend visibility every
  300 s up to deadline).
  - **Verify:** tests — a message whose row was re-dispatched
    (dispatch_id rotated) is deleted unworked; a handler that raises
    leaves the message undeleted and FakeQueue redelivers it; heartbeat
    calls recorded at expected clock ticks.

- [x] **3.2 Sweep worker.** Batch receive (≤ 10), per-message gate;
  Serper search (429 → that message left undeleted, nothing recorded,
  batch continues); one batched triage call (≤ 200 results per call,
  split beyond; LLM error → affected messages left undeleted);
  candidate extraction — direct document URLs, else single-page scrape
  (FakeFetcher HTML fixtures): entity-unescape, absolutize, followable
  extensions (stored set minus .xml/.json), PDFs floated; staging
  (SETNX **after** authoritative insert, unique-violation → skip,
  stamp + publish fetch task); history row idempotent on (dispatch_id,
  query_seq) with triage counters and query text in detail.
  - **Verify:** tests — the rate-limit invariant (429: no history row,
    no counter increment, message redelivered later carries the *same*
    query text); triage-rejected results produce no artifacts but are
    counted; the `A %20&amp;%20B.pdf` unescape case; duplicate URL
    staged once across two concurrent workers (fake constraint);
    redelivered completed message writes nothing twice.

- [x] **3.3 Fan-in.** HINCRBY on `sweep:{dispatch_id}` after each
  query's history commit; the worker reaching query_count finalizes the
  target (aggregate table from §6.2: all-errored-zero-candidates →
  error +1 day, else candidates/not_found + resweep interval), clears
  the stamp; counter TTL = dispatch timeout.
  - **Verify:** tests — 3-query dispatch completing out of order
    finalizes exactly once with the right aggregate; a lost counter
    (fake flush mid-dispatch) never finalizes but the orchestrator
    timeout test from 2.4 re-dispatches; partial failure (1 error, 2
    candidates) → candidates + full resweep interval.

- [x] **3.4 Code worker.** Consume code tasks via FakePortalDiscoverer;
  heartbeat; stage discovered candidates identically to 3.2; write-back
  table from §6.3 (transient/anti-bot → message left undeleted;
  deadline-with-candidates → +1 day resume; error → +1 day; complete →
  resweep interval); self-finalizing (query_count = 1).
  - **Verify:** tests — deadline abort keeps staged candidates and
    schedules +1 day; discoverer raising leaves the message for
    redelivery; completion finalizes the `legal_codes` row without
    touching the sibling `serper` row.

- [x] **3.5 Real portal spiders.** Scrapy-Playwright `PortalDiscoverer`
  with per-publisher strategies (municode / amlegal / ecode360 /
  other): TOC navigation, PDF-export link harvest, JSON content
  endpoints via network capture; bounds (≤ code_max_pages, ≤ 2
  renders, ≥ 1 s delay, robots.txt).
  - **Verify:** parsing/capture logic unit-tested against saved HTML +
    HAR-style fixtures per publisher (no network); a `@pytest.mark.live`
    crawl of one real Municode page exists but is excluded from default
    runs.

- [x] **3.6 Fetch worker.** Per §6.4/old §6: percent-encoded GET
  (FakeFetcher), 20 MB cap; 400/401/403/404/410/unreachable → row
  deleted; other errors → attempts++, 3 → failed; type sniff in exact
  order (**HTML before XML**); sha256 → SETNX → Postgres (corpus,
  sha256) backstop → duplicate; S3 put under key scheme; per-format
  text extraction (pypdf 40 pages, DOCX/ODT unzip+strip, RTF, XML,
  JSON) with NUL/surrogate sanitation, 20,000-char truncation; →
  `fetched`; per-host token bucket (≥ 1 s spacing).
  - **Verify:** table-driven sniff tests including the XHTML-served-
    as-PDF trap; a NUL-poisoned PDF fixture stores sanitized text;
    dedupe: same bytes at two URLs → second becomes `duplicate` (and
    again with Redis flushed, via the Postgres backstop); token bucket
    test with VirtualClock — 5 URLs on one host take ≥ 4 s of virtual
    time.

## Phase 4 — end-to-end and operations

- [x] **4.1 Scenario suite (all fakes, virtual clock).** Wire both
  services against one shared fake world and script the §7 failure
  table: happy path seed→dispatch→sweep→fetch→`fetched`; worker crash
  mid-sweep (exception) → redelivery completes it exactly once; lost
  message → dispatch-timeout re-dispatch with regenerated queries;
  Redis flush mid-flight → correctness preserved via constraints +
  timeout; double dispatch → loser dropped; stop switch → drain
  without work; Serper 429 storm → jurisdiction never blacked out
  (history stays clean, work completes after the storm).
  - **Verify:** each scenario is one test asserting both the end state
    and the recovery budget in virtual time (e.g. lost message
    recovered within 1800 s + one cycle).

- [ ] **4.2 Manual smoke against real backends.** docker-compose with
  Postgres, Redis, LocalStack (SQS+S3); a tiny config (one state, few
  jurisdictions, FakeSearch/FakeLLM still injectable via env for
  offline smoke); README runbook: boot migration, start orchestrator,
  start harvester roles, watch a document reach `fetched` + S3;
  kill-as-purge command (rows + S3 prefix + Redis prefix, FK-ordered);
  DLQ alarm note.
  - **Verify:** manual checklist in the README executed once; purge
    leaves zero rows/keys/objects for the corpus.

---

## Decisions embedded in this plan

- **Three packages, not two**: the ports + fakes must be importable by
  both services' tests; `harvest-core` is a library, never deployed
  alone.
- **Fakes are contract-tested** (1.4) so scenario tests in 4.1 prove
  the spec's failure model, not the fakes' quirks.
- **No live network in the default test run** — Serper, OpenAI,
  Playwright, and census.gov are all behind ports with fixture-driven
  fakes; live paths are `@pytest.mark.live` only.
- **Virtual clock everywhere** — every timeout in the spec (§7 table)
  is asserted in tests at exact boundaries, which real sleeps cannot
  do.
