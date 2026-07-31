# Specification: the harvesting system (re-architecture)

A specification for rebuilding the harvester described in
`OLD_HARVESTER.md` as two microservices — an **orchestrator agent** and a
**harvester agent** — bridged by Amazon SQS. This is a clean
restructure: no backwards compatibility with the old schema or queue
rows is required. Rules carried forward from the old system are carried
forward because they were correct, not because anything depends on the
old shape.

Status: draft, v0.4.

---

## 1. Goals and scope

- **Orchestrator** — single-threaded, LLM-assisted planner. Owns durable
  **per-jurisdiction** state in Postgres: jurisdiction seeding, the
  sweep-target cursor, scheduling, dispatch into SQS, and reconciliation
  of lost work. At dispatch time it **generates search queries with
  GPT-5.6-luna** (via LangChain / the OpenAI API); the query text lives
  **only in the SQS messages**, never in Postgres.
- **Harvester** — multi-threaded consumer. Two worker roles in one
  process: **sweep workers** consume query tasks in batches (Serper
  search → one batched LLM metadata triage → link extraction) and
  publish candidate URLs as fetch tasks; **fetch workers** consume those
  URLs and download, dedupe, and store the documents.
- **The LLM is GPT-5.6-luna via the OpenAI API, called through
  LangChain**, in exactly two roles this phase:
  1. **Query generation** (orchestrator, at dispatch time): write the
     search queries for a due jurisdiction across all config topics.
  2. **Relevance triage** (harvester, batched): judge search *results* —
     title, snippet, URL, nothing fetched — to decide which are worth
     touching. This is what makes crawling negligible: most scraping is
     replaced by a metadata judgment.
- **Search provider is Serper (Google Search).** Scrapy remains, but
  demoted: it fetches individual result pages that triage approved and
  extracts their document links (depth ≤ 1). No site-wide crawling, no
  frontier, no frontier persistence — a lost scrape is re-bought for
  pennies.
- **A second source: municipal code portals via Scrapy-Playwright,
  seeded manually.** Municode/American Legal/eCode360-style code
  viewers are JS-rendered HTML that search-based sweeping cannot
  ingest. For jurisdictions with a manually registered portal URL
  (`code_sources`, §4.6), a dedicated crawl task renders the viewer
  with Scrapy-Playwright and **discovers native document URLs** — PDF
  exports and the publisher's underlying JSON endpoints — which the
  ordinary fetch workers then download. Playwright is for discovery
  only; no rendered page is ever stored as a document.
- **Dedupe and sweep fan-in live in ElastiCache (Redis)**: seen-URL and
  seen-content-hash sets (corpus-scoped) plus a per-dispatch completion
  counter. Postgres unique constraints remain the dedupe correctness
  backstop, and the dispatch timeout remains the fan-in backstop (§6.5,
  §7).
- **Documents land in S3** under the old key scheme.
- **Catalog is out of scope.** Artifacts terminate at `fetched`; the
  schema keeps the catalog columns so catalog lands later as a pure
  consumer.
- **Robust to harvester failure**: a crash, poison message, lost
  message, or lost Redis key never loses work permanently or wedges a
  jurisdiction, and recovery is automatic (§7).

Non-goals carried forward verbatim: no email, no record-request
lifecycle, **documents only — never web pages** (an HTML page only
contributes the document links on it).

---

## 2. Topology

```
┌────────────────────┐   SQS: sweep-tasks (1 msg = 1 query, text inline)
│    ORCHESTRATOR     │ ─────────────────────────────────────────┐
│   (single thread)   │   SQS: code-tasks (1 msg = 1 portal)     │
│                     │ ────────────────────────────┐            ▼
│ seed jurisdictions  │                             ▼┌───────────────────────┐
│ generate queries    │      SQS: fetch-tasks        │       HARVESTER        │
│  (GPT-5.6-luna,     │      (1 msg = 1 URL)         │     (thread pool)      │
│   at dispatch time) │            ┌─────────────────│ sweep workers (batch): │
│ dispatch + reconcile│            │                  │  Serper → LLM triage   │
└─────────┬──────────┘            └────────────────▶│  → link extraction ────┼─▶ publishes
          │                                          │ code workers:          │   fetch-tasks
          │                                          │  Playwright render →   │
          │                                          │  discover doc URLs ────┼─▶ publishes
          │                                          │ fetch workers:         │   fetch-tasks
          │                                          │  GET → sniff → dedupe  │
          │                                          │  → S3 + extract text   │
          │                                          └───────┬───────┬───────┘
          │                                                  │       │
          ▼                                                  ▼       ▼
   Postgres (source of truth, per-jurisdiction) ◀────────────┘   ElastiCache
   jurisdictions, sweep_targets, artifacts,                      (Redis: dedupe +
   harvest_sweeps, documents, campaign_controls                   sweep fan-in)
                                                                      │
                                                                 S3 (bytes)
```

**Division of authority:**

| concern | owner |
|---|---|
| what work exists and when it is due | orchestrator (Postgres `sweep_targets`, per jurisdiction × source) |
| the query text | the SQS message, and nowhere else |
| which jurisdictions have a code portal | `code_sources`, seeded manually (§4.6) |
| what work is in flight right now | SQS messages |
| candidate → fetch handoff | harvester sweep workers publish fetch tasks directly |
| results, artifacts, history | harvester writes Postgres directly |
| fast-path dedupe, sweep fan-in count | ElastiCache Redis |
| dedupe correctness, fan-in backstop | Postgres constraints + dispatch timeout |
| document bytes | S3 |
| crash recovery | SQS visibility timeout + orchestrator reconciliation (§7) |

**Postgres remains the source of truth for scheduling and results.** SQS
messages and Redis keys are both disposable: a message may be lost or
duplicated, and an ElastiCache node may fail over and lose keys, without
corrupting state. The one thing that lives only in a message — the query
text — is deliberately cheap to lose: the orchestrator regenerates
queries for the jurisdiction on re-dispatch.

---

## 3. Queues

Two **standard** SQS queues plus a DLQ each. Standard, not FIFO:
ordering is applied at dispatch time by the orchestrator, and
at-least-once delivery is absorbed by idempotent handlers.

| queue | 1 message = | producer | visibility timeout | maxReceiveCount → DLQ |
|---|---|---|---|---|
| `harvest-sweep-tasks` | one search query, text inline | orchestrator | 300 s | 3 |
| `harvest-code-tasks` | one jurisdiction's code-portal crawl | orchestrator | 900 s, heartbeat-extended | 3 |
| `harvest-fetch-tasks` | one candidate URL (an `artifacts` row) | harvester sweep + code workers | 300 s | 3 |
| `…-dlq` ×3 | poison messages | — | — | — |

- Visibility timeout is the fast-path crash recovery **and the
  transient-failure retry**: a worker that dies — or deliberately
  declines to delete a message after a Serper 429 or an OpenAI error —
  lets SQS redeliver the same query text later, for free.
- DLQ is observability, not durability: underlying jurisdictions are
  recovered by orchestrator reconciliation (§7). Alarm on DLQ depth > 0.
- Long polling (20 s); sweep workers receive up to **10 messages per
  poll** to enable batched triage (§6.1).
- **Code tasks are long jobs**: a JS-rendered portal crawl can
  legitimately exceed 900 s, so code workers extend visibility via
  `ChangeMessageVisibility` (heartbeat every 300 s) up to a hard task
  deadline of **3600 s**, after which the crawl is aborted and whatever
  candidates were already discovered are kept (§6.3).

```jsonc
// sweep task — the query lives here and only here
{ "kind": "sweep", "corpus": "...", "sweep_target_id": 45,
  "jurisdiction_id": 45, "topic": "...",
  "query_text": "…generated by GPT-5.6-luna…",
  "dispatch_id": "uuid", "query_seq": 2, "query_count": 6,
  "dispatched_at": "2026-07-30T12:00:00Z" }

// code task — one jurisdiction's portal crawl
{ "kind": "code", "corpus": "...", "sweep_target_id": 46,
  "jurisdiction_id": 45,
  "portal_urls": ["https://library.municode.com/…"],
  "dispatch_id": "uuid", "dispatched_at": "2026-07-30T12:00:00Z" }

// fetch task
{ "kind": "fetch", "corpus": "...", "artifact_id": 901,
  "dispatch_id": "uuid", "dispatched_at": "2026-07-30T12:00:00Z" }
```

`query_seq`/`query_count` support fan-in: all messages of one
jurisdiction dispatch share a `dispatch_id`, and the worker that
completes the last of `query_count` finalizes the target row (§6.2).

---

## 4. Data model

### 4.1 Carried forward unchanged

`jurisdictions` (including Census self-seeding, the state-row-exists
loaded marker, the comma-county exclusion, and the federal anchor row),
`documents`, `campaign_controls`. See `OLD_HARVESTER.md` §2.

### 4.2 `sweep_targets` — the durable cursor (per jurisdiction × source)

One row per (corpus, jurisdiction, source), unique on that triple,
indexed on (corpus, next_due_at). **No query column, no topic column** —
topics belong to the config, queries to the messages.

| column | meaning |
|---|---|
| `corpus` | owning harvest config |
| `jurisdiction_id` | jurisdiction (or the federal anchor) |
| `source` | `serper` \| `legal_codes` |
| `priority` | 0 federal, 1 state, 2 county, 3 city, 9 other — dispatch order |
| `next_due_at` | when this pair may next be swept |
| `last_result` | `candidates` \| `not_found` \| `error` \| null |
| `dispatched_at`, `dispatch_id`, `query_count` | current dispatch stamp (`query_count` = 1 for `legal_codes`) |

A `serper` row exists for every in-scope jurisdiction; a `legal_codes`
row exists only for jurisdictions with an enabled `code_sources` seed
(§4.6). The two sources schedule independently — a portal crawl failing
never delays the search sweep, and vice versa.

A durable cursor, not a consumable task: rewritten in place after each
sweep, re-dispatched every resweep interval, never deleted in normal
operation.

### 4.3 `harvest_sweeps` — history

Append-only, one row per **query run or portal crawl**: `corpus`,
`jurisdiction_id`, `source`, `dispatch_id`, `topic` (null for
`legal_codes`), `result` (`candidates` | `not_found` | `error`),
`results_seen`, `results_triaged_relevant`, `candidates_staged`,
`detail` (500 chars — the query text is recorded here, truncated, as
audit; it is not queryable state), `swept_at`. The triage counters
measure the LLM filter's precision.

### 4.4 `artifacts`

As in `OLD_HARVESTER.md` §2.4 with claim columns replaced by
`dispatched_at`/`dispatch_id`, and unique on (corpus, source_url) — the
constraint backstopping Redis URL dedupe. State machine truncated for
this phase:

```
        (created by sweep worker)
               |
           [pending] --- 4xx / unreachable host --> ROW DELETED
               |-- 3 fetch errors ---------------> [failed]        (terminal)
               |-- not a document type ----------> [not_document]  (terminal)
               |-- sha256 already in corpus -----> [duplicate]     (terminal)
               v
           [fetched]   <-- terminal FOR NOW; catalog resumes here later
```

Verdict columns and the `cataloged`/`rejected` states stay in the
schema, unused.

### 4.5 Redis keyspaces (ElastiCache)

| key | type | written by | meaning |
|---|---|---|---|
| `url:{corpus}:{sha256(url)}` | SETNX flag | sweep worker | this corpus has staged this URL |
| `doc:{corpus}:{sha256(bytes)}` | SETNX flag | fetch worker | this corpus holds these bytes |
| `sweep:{dispatch_id}` | hash `{done, candidates, errors}` | sweep workers | fan-in counter for one jurisdiction dispatch; TTL = dispatch timeout |

The dedupe keys have no TTL — that state should live as long as the
corpus (corpus kill deletes the prefix). All three keyspaces are
**fast-path only**: §6.5 and §7 define behavior when Redis is wrong or
down. Both dedupe gates remain **corpus-scoped, not global** —
overlapping corpora deliberately pay separately; that old ruling stands.

### 4.6 `code_sources` — manually seeded portal registry

The only manually maintained table. One row per (jurisdiction_id, url):

| column | meaning |
|---|---|
| `jurisdiction_id` | the jurisdiction whose code this portal publishes |
| `url` | portal entry point (e.g. a Municode library page) |
| `publisher` | `municode` \| `amlegal` \| `ecode360` \| `other` — selects the spider's per-publisher discovery strategy |
| `enabled` | disable without deleting; a wrong seed is disabled, never removed |
| `added_by`, `added_at` | provenance |

Seeding is **manual by design**: rows enter via a CLI import command
(CSV of jurisdiction, url, publisher) or direct insert — there is no
automatic portal discovery, no LLM resolution, no search. The table is
corpus-independent: any corpus whose scope includes the jurisdiction
gets a `legal_codes` sweep target for it.

---

## 5. The orchestrator

One process, one thread, one loop, every `--interval` seconds (default
60), per config marked `running`. Everything it does is idempotent; an
accidental second orchestrator is wasteful but safe.

### 5.1 Seed (unchanged in substance)

Ensure Census jurisdictions are loaded for the config's scope, resolve
the scope to concrete jurisdictions, and insert missing `sweep_targets`
rows (`next_due_at` = now, level priority) — all rules from
`OLD_HARVESTER.md` §4 carry forward. Per-source targeting: every
in-scope jurisdiction gets a `serper` row; jurisdictions with an
enabled `code_sources` seed also get a `legal_codes` row. A
`legal_codes` row whose last enabled seed is disabled is parked
(`next_due_at` pushed one resweep interval out, `last_result` =
`error` detail `no_source`) rather than deleted. Throttled to once per
600 s.

### 5.2 Dispatch = generate + send

Select due targets: `next_due_at <= now`, `dispatched_at` null or older
than the dispatch timeout (§7), ordered by `priority` then
`next_due_at` (**federal → state → county → city**, still load-bearing),
limited to `max_sweeps_per_dispatch` (default 25 jurisdictions).

Per selected jurisdiction:

1. **Generate queries with GPT-5.6-luna** (one LangChain call per
   jurisdiction, covering all config topics). Input: jurisdiction name,
   state, level, and `topics`; instructions to write Google queries that
   surface the jurisdiction's own published regulatory documents
   (ordinances, codes, rules), preferring official domains and document
   filetypes. Output: structured JSON, 1–`queries_per_jurisdiction`
   (default 3) strings per topic, each ≤ 200 chars.
   - **LLM failure (API error, parse failure) → skip this jurisdiction,
     leave the row untouched, move on.** It stays due and is retried
     next cycle; one bad generation never blocks the batch.
   - Queries are **regenerated fresh on every dispatch** — this is the
     cost of keeping them out of Postgres, and it is deliberate. There
     is **no regeneration feedback for fruitless queries**: a
     jurisdiction that keeps returning `not_found` simply resweeps on
     schedule with whatever the model writes next time.
2. **Stamp** the row: `dispatched_at = now`, `dispatch_id = <uuid>`,
   `query_count = N`. Commit.
3. **Send** N sweep messages, each carrying one query plus the shared
   `dispatch_id` and `query_count`. Stamp-then-send: a crash between
   the two leaves a stamped row the dispatch timeout recovers;
   send-then-stamp could double-dispatch with no record.

Due `legal_codes` targets take a simpler path in the same selection
(same priority ordering, counted against the same
`max_sweeps_per_dispatch` cap): **no LLM call** — the orchestrator
reads the jurisdiction's enabled `code_sources` URLs, stamps the row
(`query_count` = 1), and sends a single code task carrying the portal
URLs. Fan-in is trivial: the one crawl finalizes the row itself.

### 5.3 Reconcile fetch tasks (backstop only)

Fetch tasks are normally published by sweep and code workers (§6.1,
§6.3). The
orchestrator re-publishes any `pending` artifact whose `dispatched_at`
is null or older than the dispatch timeout — covering a sweep worker
that crashed after inserting artifacts but before publishing, and fetch
messages that died in the DLQ. Capped at `max_fetch_redispatch`
(default 500) per cycle.

### 5.4 Stop semantics

The run switch is read at the top of each cycle; a stopped config gets
nothing new dispatched. In-flight messages drain: the harvester
re-checks the run switch per task and drops tasks for stopped corpora
without working them. Stop takes effect within one task duration.

---

## 6. The harvester

One process (horizontally replicable) with a thread pool of `--threads`
(default 8), partitioned by `--role sweep|code|fetch|all` (default all,
split evenly). Threads long-poll their queue; no ticks, no phases. Only
the code role needs the Playwright browser runtime — a fleet can run
lightweight sweep/fetch replicas and a separate, smaller pool of
Playwright-capable code replicas.

### 6.1 Sweep worker: a batch of query tasks

A single sweep thread consumes **up to 10 sweep messages per poll** —
possibly spanning several jurisdictions and dispatches — and works them
as one batch:

1. **Idempotency gate per message** (§6.6): run switch off, or the
   target row's `dispatch_id` no longer matches → delete that message,
   drop it from the batch.
2. **Search**: run each query against **Serper** (`search_count`
   results, default 20), serially within the thread.
   - HTTP 429 on a query → the rate-limit invariant applies to *that
     message*: **no history row, no counter increment, message not
     deleted** — visibility timeout redelivers the same query text
     later, for free. The rest of the batch proceeds. A rate limit says
     nothing about the jurisdiction.
3. **Batched LLM triage — one GPT-5.6-luna call for the whole batch.**
   The call carries, per query: the jurisdiction (name, state, level),
   the topic, and the result list as metadata only — rank, title,
   snippet, URL. It returns, per result, `relevant` (bool),
   `is_document` (does the URL itself look like a document file), and
   `confidence`. The framing is the old official-site question made
   explicit: *is this plausibly a regulatory document, or a page on the
   jurisdiction's own site that would link to one?* Nothing is fetched
   to answer it. Batching across queries is why triage cost stays small;
   a batch is capped at `triage_batch_max_results` (default 200)
   results, splitting into multiple calls beyond that.
   - **LLM error → transient, exactly like a rate limit**: the affected
     messages are left undeleted for redelivery; nothing is recorded.
     Never fall back to fetching unfiltered — the filter *is* the cost
     model.
   - Not-relevant results are dropped and counted (`results_seen` vs
     `results_triaged_relevant`).
4. **Candidate extraction**, per relevant result:
   - URL is itself a document (by extension or triage's `is_document`)
     → emit directly, with title + snippet as context.
   - Otherwise it is an HTML page → **Scrapy scrapes that single page**
     (depth ≤ 1, 400,000-byte page cap, robots.txt obeyed, ≥ 1 s
     download delay, ≤ 2 concurrent requests per domain) and extracts
     linked document URLs. Old link rules carry forward exactly:
     followable extensions are the stored set minus `.xml`/`.json`;
     hrefs HTML-entity-unescaped and absolutized; PDFs float to the
     front; context is "linked from <page URL>" + anchor text.
5. **Stage each candidate** (per URL): `SETNX url:{corpus}:{hash}` —
   already set → skip. Else insert the `pending` artifact (NUL-stripped
   context, origin `serper`); unique-constraint violation → another
   worker won, skip. **The insert is the authoritative dedupe**; SETNX
   is written only after a successful insert (§6.5). Then stamp the
   artifact and publish its fetch task (crash between insert and
   publish is recovered by §5.3).
6. **Write back, per completed query message**, in one transaction:
   write the `harvest_sweeps` row (result, counters, query text in
   `detail`), then `HINCRBY sweep:{dispatch_id}` (`done` +1, plus
   `candidates`/`errors`), then delete the message.
   - Commit-then-delete: a crash in between causes a redelivery that
     the gate in step 1 must catch — the history insert is made
     idempotent on (dispatch_id, query_seq) so a redelivered completed
     query writes nothing twice and only re-increments after checking.

### 6.2 Fan-in: finalizing the jurisdiction

The worker whose increment brings `done` to the message's `query_count`
finalizes the `sweep_targets` row, gated by `dispatch_id`:

| aggregate condition | `last_result` | `next_due_at` |
|---|---|---|
| every query errored and zero candidates staged | `error` | now + **1 day** |
| otherwise | `candidates` if any staged, else `not_found` | now + `resweep_interval_days` |

then clears the dispatch stamp. A genuine error retries in **1 day**,
never a full resweep interval: **a transient failure must never black
out a jurisdiction.**

If the counter is lost (ElastiCache failover) or never fills (a query
message DLQ'd, or stuck on repeated 429s), no worker finalizes — and
that is fine: the **dispatch timeout** (§7) clears the stamp and the
orchestrator re-dispatches the jurisdiction with freshly generated
queries. URL dedupe makes the repeat cheap. Fan-in is an optimization
for promptness; the timeout is the correctness mechanism.

### 6.3 Code worker: one portal crawl (Scrapy-Playwright)

Consumes `harvest-code-tasks`. One task = one jurisdiction's manually
seeded portal(s), crawled with **Scrapy-Playwright** (Chromium,
headless) because these viewers render their tables of contents and
section bodies with JavaScript.

1. **Idempotency gate** (§6.6), then start a visibility heartbeat
   (extend by 900 s every 300 s, hard deadline 3600 s).
2. **Discover, per portal URL, using the per-publisher strategy from
   `code_sources.publisher`**:
   - Navigate the rendered table of contents to enumerate the code's
     parts/chapters (bounded: ≤ `code_max_pages` rendered pages,
     default 200, ≤ 2 concurrent renders, ≥ 1 s delay, robots.txt
     obeyed).
   - Emit **native document URLs only**: the publisher's PDF
     export/download links where offered, and the underlying JSON
     content endpoints observed via Playwright network capture (the
     viewers hydrate from JSON APIs; those responses are structured
     documents in the stored-types sense, entering through a source
     adapter that knows what it is asking for — the old `.json`
     carve-out). The rendered HTML itself is never emitted.
   - Context per candidate: portal URL plus the code hierarchy heading
     (e.g. "Chapter 14 — Nuisances"). `origin = legal_codes`.
   - The `other` publisher strategy is generic: harvest any
     document-extension links plus captured JSON content endpoints from
     the rendered pages.
3. **Stage** each candidate exactly as sweep workers do (§6.1 step 5:
   Redis SETNX → authoritative insert → publish fetch task).
4. **Write back**, gated by `dispatch_id` (this crawl is its own
   fan-in, `query_count` = 1):

| condition | history row | target row |
|---|---|---|
| portal unreachable / render never completed / anti-bot block | **none** — message left undeleted, visibility redelivers | untouched, stays in flight then due |
| deadline hit with candidates already staged | `candidates`, detail notes truncation | `next_due_at = now + 1 day` — resume discovery tomorrow; URL dedupe makes the re-crawl incremental |
| crawl errored, zero candidates | `error` | `next_due_at = now + 1 day`, `last_result = error` |
| completed | `candidates` / `not_found` | `next_due_at = now + resweep_interval_days` |

   Commit, then delete the message. No LLM is involved anywhere in this
   path — the seed is human-vetted, so there is nothing to triage.

### 6.4 Fetch worker: one URL task

Idempotency gate, then per `OLD_HARVESTER.md` §6 unchanged:
percent-encode (preserving existing escapes), GET with the fleet user
agent, 20,000,000-byte document cap; **delete the row outright on
400/401/403/404/410/unreachable**; other network errors increment
`attempts`, 3 → `failed`. Type-sniff in the exact old order (**HTML
before XML** — the XHTML-stub trap is still real).

Then the content gate:

- sha256 the bytes; `SETNX doc:{corpus}:{hash}` already set → mark
  `duplicate` (terminal), done.
- Postgres backstop: if another artifact in this corpus already carries
  the hash (partial index on (corpus, sha256)), mark `duplicate` even
  though Redis said new.
- Store bytes to **S3** under the old key scheme
  (`<corpus-slug>/<jurisdiction-slug>/<hash8>_<filename>`; same bucket
  posture: public access blocked, versioning, lifecycle rules,
  presigned reads at 120 s).
- Extract text per format (40 PDF pages, tag-stripping rules, 20,000
  stored chars) with NUL/surrogate sanitation — still a durability
  requirement, not hygiene.
- Artifact → `fetched`. Commit, then delete the message.

Fetch workers rate-limit per host (token bucket, ≥ 1 s spacing per
domain across the pool) so a sweep that yields 50 PDFs from one town
does not hit it with 50 concurrent GETs.

### 6.5 Dedupe authority (Redis is fast, Postgres is right)

- **Redis false negative** (lost key): the URL insert hits the unique
  constraint, or the hash hits the (corpus, sha256) backstop — caught
  one step later at Postgres cost. Re-SETNX to heal the cache.
- **Redis false positive** cannot occur: SETNX is written only after
  the authoritative insert / from actual bytes; the race two workers
  lose to each other lands on the unique constraint.
- **Redis down**: skip the fast path, rely on Postgres alone — degraded
  but correct. (Fan-in also degrades: no finalization, so targets
  recover via dispatch timeout; slower, still correct.)

### 6.6 Idempotency contract (what makes at-least-once safe)

Every handler, before working: (1) re-read the run switch — stopped →
delete message, done; (2) re-read the row — `dispatch_id` mismatch, or
state already past expected (`pending` no longer pending, history row
for this (dispatch_id, query_seq) already written) → delete message,
done. Every handler, after working: commit Postgres **then** delete the
message. All side effects (inserts, S3 puts keyed by content hash,
Redis SETNX/HINCRBY-after-check) are idempotent or constraint-absorbed.

---

## 7. Failure model

| failure | detection | recovery | budget |
|---|---|---|---|
| harvester thread/process dies mid-task | visibility timeout (300 s; 900 s + heartbeat for code tasks) | redelivery; idempotency gate ensures single effective execution | minutes |
| code crawl blocked (anti-bot) or render hangs | in-task / deadline (3600 s) | message left undeleted → redelivery; repeated blocks → DLQ + dispatch-timeout recovery; partial candidates always kept | ≤ deadline |
| Serper 429 / OpenAI triage error | in-task | message left undeleted → same query redelivered; nothing recorded | ≤ visibility timeout |
| OpenAI generation error | in-cycle | jurisdiction skipped, still due next cycle | ≤ 60 s |
| sweep/code worker dies after staging artifacts, before publishing fetch tasks | artifact `dispatched_at` stale | orchestrator re-publishes (§5.3) | ≤ dispatch timeout |
| SQS message lost / DLQ'd / stamped-but-never-sent | target/artifact `dispatched_at` older than **dispatch timeout = 1800 s** | orchestrator clears stamp, re-dispatches (queries regenerated) | ≤ 30 min, automatic |
| fan-in counter lost (ElastiCache failover) | no finalization occurs | dispatch timeout re-dispatches the jurisdiction; URL dedupe makes it cheap | ≤ 30 min |
| poison message | 3 receives → DLQ; row recovered as above | operator inspects DLQ; alarm on depth > 0 | observability only |
| ElastiCache down | connection errors | dedupe fast path skipped (Postgres-only); fan-in degrades to timeout recovery | degraded, correct |
| orchestrator down | no new dispatch; harvesters drain and idle | restart — stateless over Postgres, zero recovery procedure | availability only |
| Postgres down | transactions fail | messages **not** deleted → redelivery; orchestrator retries | ≤ outage + one timeout |
| double dispatch | two message sets, one valid `dispatch_id` | gate drops the losers | none |

Two independent clocks cover each other: SQS visibility (fast,
message-level) recovers dead consumers and retries transients; the
orchestrator's dispatch timeout (slow, row-level) recovers dead
messages and dead counters. Dispatch timeout (1800 s) must exceed
visibility timeout × maxReceiveCount (900 s) so a message still
bouncing toward the DLQ is not concurrently re-dispatched. **Code
tasks use a longer dispatch timeout (7200 s)** for the same reason:
it must exceed the 3600 s heartbeat-extended deadline, or a
slow-but-alive crawl would be double-dispatched.

---

## 8. Cross-cutting invariants

Carried forward from the old system: documents only, never web pages ·
dedupe before spend, corpus-scoped · **a rate limit is never an error**
(nothing recorded, work deferred) · a transient failure never blacks
out a jurisdiction (errors retry in 1 day) · sweep order federal →
state → county → city · never fetch what the filter rejected.

Rulings for this architecture:

1. **Postgres is the source of truth for scheduling and results; SQS
   messages and Redis keys are disposable.** The only message-resident
   state — query text — is regenerable by construction.
2. **Postgres holds per-jurisdiction rows only.** No query table, no
   topic column; queries exist in flight and (truncated) in the audit
   history's `detail`.
3. **Every handler is idempotent against row state**, gated by
   `dispatch_id`, commit-then-delete.
4. **Redis is fast, Postgres is right**: every Redis answer is
   backstopped — dedupe by constraints, fan-in by the dispatch timeout.
5. **LLM failures are transient, never errors**: generation skips and
   retries next cycle; triage leaves messages for redelivery. No
   fallback to unfiltered fetching — the triage filter *is* the cost
   model.
6. **Queries are regenerated on every dispatch, never tuned**: no
   feedback loop for fruitless queries, no query persistence.
7. **The orchestrator is single-threaded and stateless between cycles**
   — restartable at any instant.
8. **The artifact schema keeps the catalog stage's columns and states**
   so catalog lands later as a pure consumer of `fetched`.
9. **Code portals are seeded manually, never discovered** — no LLM, no
   search, no auto-registration; a wrong seed is disabled, not deleted.
10. **Playwright is discovery-only.** It renders pages to find native
    document URLs (PDF exports, JSON content endpoints); rendered HTML
    is never stored, and downloading stays with the plain-HTTP fetch
    workers.

---

## 9. Configuration

Carried forward with the same meaning: `mode`, `name`, `scope.*`,
`topics` (input to query generation), `resweep_interval_days` (30),
`search_count` (per-query result count, default 20), `dry_run` /
`min_confidence` (dormant until catalog).

| key | default | controls / costs |
|---|---|---|
| `llm.provider` | `openai` | via LangChain |
| `llm.model` | `gpt-5.6-luna` | query generation + triage |
| `llm.api_key_env` | `OPENAI_API_KEY` | |
| `serper_api_key_env` | `SERPER_API_KEY` | search |
| `queries_per_jurisdiction` | 3 | per topic — **the #1 search- and LLM-spend multiplier** |
| `max_sweeps_per_dispatch` | 25 | jurisdictions dispatched (and query-generation calls made) per cycle |
| `triage_batch_max_results` | 200 | results per triage call before splitting |
| `max_fetch_redispatch` | 500 | reconciliation cap per cycle |
| `code_max_pages` | 200 | rendered pages per portal crawl |
| orchestrator `--interval` | 60 s | dispatch cycle |
| harvester `--threads` / `--role` | 8 / all | pool size / sweep\|code\|fetch\|all |
| `redis_url_env` | `REDIS_URL` | ElastiCache endpoint |

Hardcoded: seed throttle 600 s · visibility 300 s (code: 900 s,
heartbeat 300 s, deadline 3600 s) · dispatch timeout 1800 s (code:
7200 s) · fan-in counter TTL 1800 s · maxReceiveCount 3 · code renders
≤ 2 concurrent, ≥ 1 s delay · sweep batch
receive 10 messages · error retry 1 day · per-domain fetch spacing
≥ 1 s · page/document byte caps 400 K / 20 M · 40 PDF pages · 20,000
stored chars · query text ≤ 200 chars · all other char caps per the old
table.

### Cost model

- **LLM spend** = (a) generation: one call per jurisdiction per
  **resweep** (queries are ephemeral and regenerated at dispatch — this
  is the price of keeping Postgres per-jurisdiction only); (b) triage:
  batched across queries, roughly one call per 10 queries. Both scale
  with `queries_per_jurisdiction × topics × scope / resweep_interval`.
- **Search spend** = queries × resweep rate; same levers.
- **Fetch** is plain HTTP; **S3/SQS/Redis** are negligible. The triage
  filter keeps fetch (and future catalog) volume down — its precision
  is measurable from the history counters.
- **Code-portal crawls** cost no LLM and no search — only bandwidth,
  Playwright compute, and time (bounded by `code_max_pages` × seeded
  jurisdictions per resweep). Their spend scales with how many seeds
  are entered manually, which is the intended throttle.

**Operational surface** carries forward from `OLD_HARVESTER.md` §10:
run switch semantics, kill-as-purge (now also deleting the corpus's
Redis prefix), idempotent boot migrations. The re-judge shortcut is
moot until catalog exists.

---

## 10. Open questions

1. **Dispatch-time generation latency** — 25 jurisdictions per cycle =
   25 sequential LLM calls in the single-threaded orchestrator. At ~2 s
   per call that fits a 60 s cycle; if generation proves slower, the
   options are a smaller dispatch batch or generating for cycle N+1
   while dispatching cycle N. Measure first.
2. **Triage batch shape** — batching is per SQS receive (≤ 10 messages,
   ≤ 200 results per call). Whether one big prompt or parallel smaller
   calls wins on cost/quality is an implementation-time measurement.
3. **History `detail` as the only query record** — with queries
   unpersisted, debugging "why did this jurisdiction find nothing" rests
   on the truncated query text in `harvest_sweeps.detail`. If that
   proves too thin in practice, widen the column before reaching for a
   query table.
4. **Publishers with neither PDF exports nor capturable JSON** — a
   portal that renders pure server-side HTML with no export offers
   nothing this pipeline may store ("documents only" stands). Such
   seeds will chronically report `not_found`; decide per publisher
   whether to disable the seed or extend the spider with a
   section-to-JSON serializer, which would be a deliberate loosening of
   the invariant.
5. **Anti-bot posture** — Municode-class publishers may rate-limit or
   block headless browsers. The spec's answer is politeness plus
   treat-as-transient (retry via redelivery); if a publisher hard-blocks
   the fleet's IPs, that is an operator problem (respect it or negotiate
   access), not something the harvester should engineer around.
