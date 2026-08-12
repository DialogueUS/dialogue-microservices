# Implementation plan — NEW_PUBLIC_RECORDS.md

One deployable service as a new uv-workspace package, reusing
`harvest_core`'s generic infrastructure ports and contract-tested fakes
(Clock, TaskQueue, KeyValue, ObjectStore, SearchProvider, Fetcher). All
tests run against **in-memory fakes** (Postgres, SQS, Redis, S3, Serper,
LLMs, Resend, HTTP) behind the same ports-and-adapters data layer; no
test ever needs a live backend. Integration against real backends is a
manual, optional smoke step at the end.

## Layout & stack

```
dialogue-microservices/
├── core/            # harvest-core: shared infra ports, fakes, adapters (extended, not forked)
├── orchestrator/    # harvest-orchestrator (unchanged)
├── harvester/       # harvest-harvester (unchanged)
├── records/         # pr-records: the public-records microservice
│   └── src/public_records/
│       ├── constants.py, config.py      # §11 config surface + every hardcoded number
│       ├── domain.py, messages.py       # table-row models, thread state machine, 4 queue schemas
│       ├── errors.py, ports.py          # PR-specific ports (RecordsStore, EmailTransport, LLM trio)
│       ├── fakes/, adapters/            # one fake + one real adapter per PR-specific port
│       ├── orchestrator.py              # seeding, mail poll, follow-up scan, digest
│       ├── scraper.py, sender.py, receiver.py, fees.py, attachments.py
│       └── cli.py                       # wiring (real vs fake), scheduler + thread pools
└── NEW_PUBLIC_RECORDS.md
```

- Python 3.12, fourth **uv workspace** member `pr-records` depending on
  `harvest-core`; pytest, ruff, mypy at the existing root config.
- pydantic v2 (config + message schemas), SQLAlchemy 2 (Postgres
  adapter), boto3 (SQS/S3), redis-py, langchain + langchain-openai
  (GPT 5.6-luna and 5.6-terra), httpx (Resend client + page crawling),
  stdlib `email` for MIME parsing.
- Generic infra ports come from `harvest_core.ports`; PR-specific ports
  (`RecordsStore`, `EmailTransport`, the three LLM roles) live in
  `public_records.ports` with exactly one fake and one real adapter
  each. Service code depends on ports only; adapters are chosen in
  `cli.py`.
- The **`Clock` port** is injected everywhere times matter — visibility
  timeouts, the office cooldown, UTC-midnight daily cap, follow-up
  intervals, and `next_action_at` scheduling are all tested with
  `VirtualClock`, never `time.sleep`.

Conventions for every step below: code + tests land together; a step is
done when its **Verify** line passes plus `ruff check`, `mypy`, and the
full `pytest` suite stay green.

---

## Phase 0 — scaffolding

- [x] **0.1 Workspace member.** Add `records/` (`pr-records`) to the uv
  workspace depending on `harvest-core`; register its test dir in root
  pytest/mypy config (respecting the unique-helper-module rule from
  CLAUDE.md); placeholder test.
  - **Verify:** `uv sync && uv run pytest` collects and passes the new
    placeholder alongside the existing suite; `ruff` and `mypy` green.

- [x] **0.2 harvest_core ObjectStore extension.** The mail poller and
  attachment flow need `get(key)`, `list_keys(prefix)`, and
  `delete(key)` on `ObjectStore`. Add them to the port, `FakeObjectStore`,
  and the S3 adapter; extend the fake contract tests.
  - **Verify:** contract tests — `list_keys` returns exactly the put
    keys under a prefix; `get` of a deleted key errors; existing
    harvester suite untouched and green.

## Phase 1 — core: contracts before behavior

- [x] **1.1 Config + constants.** Pydantic model of the §11 per-campaign
  surface (record_type/description, legal_basis default, requester.*
  with `anonymous=true` / `consent_confirmed=false` defaults, scope.*,
  `dry_run=true`, all `limits.*` and `contacts.min_confidence`
  defaults, notify_email, `test_contacts[]`) plus registration-time
  validation rejecting an anonymous requester whose scope — or whose
  test contacts — can include AL/AR/TN/VA/DE/NJ/KY.
  Constants module: visibility 900 s, DLQ maxReceiveCounts 3/5/3,
  orchestrator period 300 s, 3 queries per target, page size 8, page
  budget 4 / 12 s, 12 candidates / 120+60 context, junk local-parts and
  generic-keyword lists verbatim, blocked-state list, token format +
  `[DLG-…]`/`[RF-…]` regexes, poll cap 200, scan batch 50, truncations
  6,000/4,000/2,000/500, Resend 30 s, LLM 120 s / 2 retries, presign
  120 s, slug cap 60, accepted attachment types, document key scheme.
  - **Verify:** unit tests — a minimal config yields every §11 default;
    a full config round-trips; missing requester/consent fields and the
    anonymous × blocked-state scope combination are rejected.

- [x] **1.2 Domain + message schemas.** Models for Campaign,
  Jurisdiction (with the shared contact columns), EmailThread (status
  enum uppercase-persisted / lowercase-API + `check_transition` helper
  encoding §4, including "`failed` never assigned automatically"),
  Email (direction/kind/classification/message_id/source_key rules),
  Escalation (reason enum; creating one parks the thread), SpendEntry
  (integer cents only), SearchTarget. The four SQS message schemas
  (`search_query`, `contact`, `followup_job` with
  kind/`followup_index`/`inbound_email_id`/`amount_cents`,
  `inbound_mail`) discriminated and JSON round-tripping. Helpers:
  thread-token generation (16 hex from 8 random bytes), token
  extraction (header first, then subject regex matching DLG and legacy
  RF), inbound `message_id` fallback (`sha:` + first 32 hex of sha256
  over `from|subject|body`).
  - **Verify:** round-trip tests for all four messages; transition
    helper rejects e.g. `fulfilled → awaiting_reply` and any automatic
    `→ failed`; token round-trips through subject and header, RF legacy
    matches; fallback message_id is stable and header-preferred.

- [x] **1.3 PR ports.** Protocols in `public_records.ports`:
  `RecordsStore` (campaigns incl. conditional `seeded`/counters;
  jurisdictions incl. `last_contacted_at` stamp; threads incl. unique
  token, per-triple existence check, conditional status updates,
  `select_due_followups(now, batch)`; emails incl. unique
  (campaign, message_id) and unique source_key signaling; escalations +
  un-notified selection; spend entries + committed-total; search
  targets incl. conditional resolve and outstanding-query countdown;
  FK-ordered `purge_campaign`), `EmailTransport` (send → resend_id,
  typed transient failure), `ContactQueryGenerator`, `ContactPicker`,
  `EmailDrafter`, `InboundClassifier` (typed parse-failure results,
  never escaping exceptions). Reuse `TaskQueue`, `KeyValue`,
  `ObjectStore`, `SearchProvider`, `Fetcher`, `Clock` from
  harvest_core. `_protocol_checks.py` for mypy conformance.
  - **Verify:** mypy passes with fakes (1.4) and adapters (1.5)
    declared as implementations; no service module imports an adapter.

- [x] **1.4 PR fakes.** `FakeRecordsStore` (unique violations on
  thread_token, (campaign, message_id), source_key raise the same typed
  error as Postgres; conditional writes honest — resolving an
  already-resolved target returns False), `FakeEmailTransport` (records
  every payload, programmable transient/permanent failure),
  `FakeContactQueryGenerator` / `FakeContactPicker` / `FakeEmailDrafter`
  / `FakeInboundClassifier` (canned outputs keyed by input,
  programmable parse failure). Contract tests per CLAUDE.md.
  - **Verify:** contract tests — duplicate message_id insert raises;
    conditional resolve loses the second race; transport failure mode
    raises the typed transient error and records nothing.

- [x] **1.5 Real adapters.** SQLAlchemy schema + idempotent
  boot-migration for the seven tables (§3.1, exact column names/enums);
  Postgres RecordsStore (unit-tested on SQLite per repo convention);
  Resend client over httpx (30 s timeout, dry-run handled in sender
  logic not here); luna/terra LangChain adapters (structured JSON out,
  reasoning enabled for classification only, 120 s / 2 retries, parse
  failure → typed result); SES-mail-bucket access is just the extended
  S3 ObjectStore. Serper adapter reused as-is.
  - **Verify:** stubbed-transport unit tests (respx/botocore stubber)
    asserting request shapes: Resend payload fields incl.
    `X-Dialogue-Token` header; classifier request carries reasoning
    flag and 6,000-char truncation; unknown category and JSON parse
    failure both degrade to `unclear`; unique-violation mapping on the
    SQLite-backed store.

## Phase 2 — orchestrator

- [x] **2.1 Jurisdictions + scope resolution.** Census-seeded
  jurisdictions table with the shared contact columns (port the
  harvester orchestrator's loader semantics: state-row marker,
  comma-county exclusion); scope resolution (`levels` default
  `[county]`, `states` default `[ALL]`, `only` allow-list).
  - **Verify:** tests — comma-county rows excluded from scope but
    retained; `only` narrows; loader idempotent (second run inserts
    nothing).

- [x] **2.2 Seeding pass.** For campaigns `active ∧ ¬seeded ∧
  consent_confirmed`: create missing `search_targets`; luna query
  generation with the deterministic fallback pattern always first;
  hard-cap 3 with truncation recorded in `queries_enqueued`; enqueue to
  `pr-search-queries`; seeded-contact shortcut (target `resolved`,
  direct `pr-contacts` message, `source: "seeded"`, no review flag);
  test campaigns (`test_contacts` non-empty) seed
  from the config instead — jurisdiction rows created on demand, no
  census, no query generation, no search message; `seeded = true` only
  after **every** in-scope target is enqueued.
  - **Verify:** tests — `consent_confirmed = false` produces zero queue
    work even with `active = true`; LLM failure still enqueues exactly
    the one fallback query; crash mid-pass (kill between targets)
    re-runs without duplicate `search_targets` rows and only then sets
    `seeded`; 5-query model output truncates to 3; a test campaign
    reaches `fulfilled` with the search provider never called.

- [x] **2.3 Mail poller.** Gate on `pr-inbound-mail` empty (visible +
  in-flight both 0 on the fake); list mail bucket, skip
  `AMAZON_SES_SETUP_NOTIFICATION`; skip keys already present as an
  `emails.source_key`; parse MIME preferring plain-text part; extract
  token (header, then subject); spool attachments to
  `<campaign-slug>/inbox-spool/<digest8>_<n>_<filename>` in the
  documents bucket; enqueue the §5.2 message; cap 200 per poll; strictly
  read-only on the mail bucket.
  - **Verify:** tests — a non-empty queue skips the poll entirely; an
    already-ingested source_key is not re-enqueued but a brand-new key
    in the same listing is; multipart fixture with HTML+plain picks
    plain; attachment bytes land in the spool and the SQS body carries
    only keys; 201 fresh objects enqueue exactly 200.

- [x] **2.4 Follow-up scan.** Threads in `request_sent`/`awaiting_reply`
  with `next_action_at <= now`, batch 50: at/over `max_followups` →
  `no_response` escalation, thread `needs_human`, `next_action_at`
  cleared; otherwise enqueue `pr-followups` carrying `followup_index` =
  current `followups_sent` and reschedule `next_action_at` **at enqueue
  time**.
  - **Verify:** VirtualClock tests — a thread crossing the interval is
    enqueued once and, with the sender stalled, is *not* enqueued again
    next scan (reschedule-at-enqueue); the 4th silence on
    `max_followups=3` escalates instead of enqueueing; `fulfilled` /
    `needs_human` threads are never selected.

- [x] **2.5 Notification digest.** Per campaign with `notify_email`:
  collect un-notified open escalations + un-notified spend entries; one
  plain-text Resend send (counts by reason, ≤ 25 line items per kind,
  budget position, EXHAUSTED warning at zero remaining); mark rows
  `notified` in the same transaction; token literally `notify`, header
  `X-Dialogue-Kind: alert`.
  - **Verify:** tests — each row is included in exactly one digest
    across repeated periods; no un-notified rows → no send; transport
    failure leaves rows un-notified for the next period.

- [x] **2.6 Scheduler + wiring.** The three periodic concerns (+ digest)
  on a 300 s period, each single-flight (skip silently if the previous
  run is in flight); `cli.py` wiring of real vs fake adapters; graceful
  shutdown.
  - **Verify:** loop test on fakes — a concern whose previous run is
    artificially still running is skipped, others proceed; `active =
    false` mid-test stops new seeding within one period.

## Phase 3 — task consumers

- [x] **3.1 Consumer framework.** Thread pools per role (sender pool
  size `limits.max_concurrent_sends`), commit-then-delete helper
  (delete only after the transaction commits; handler exception leaves
  the message for redelivery); campaign-existence gate (messages for a
  purged campaign are dropped); DLQ watcher that turns any DLQ arrival
  into an `other` escalation naming queue + payload summary, exactly
  once per message.
  - **Verify:** tests — a raising handler leaves the message and
    FakeQueue redelivers with receive_count+1; the 3rd/5th receive
    lands in the DLQ and produces exactly one `other` escalation; a
    dead-campaign message is deleted unworked.

- [x] **3.2 Scraper.** Consume `pr-search-queries`; resolved-target
  short-circuit (delete unworked). Serper search (page size 8) →
  official-site heuristic filter → crawl via the Fetcher port (12 s,
  same-domain records/contact-suggesting links, 4-page budget) →
  email-regex harvest with junk local-part drop, 120/60-char context,
  stop at 12 candidates → luna pick (verbatim-membership check,
  confidence ≥ `contacts.min_confidence`). Outcome table in one
  conditional transaction: generic hit → jurisdiction contact written
  (`contact_verified = false`) + target resolved + `pr-contacts`;
  personal-looking hit (no generic keyword in local-part) → resolved +
  `contact_needs_review`, never enqueued; miss on the target's **last**
  outstanding query → `no_contact_found`; transient (429/timeout/5xx) →
  message left undeleted, nothing recorded.
  - **Verify:** tests — picker returning an address not in the
    candidate list is rejected; `records@…` enqueues while
    `jsmith@…` escalates for review; three queries all missing → one
    `no_contact_found` after the third only; Serper 429 records nothing
    and the redelivered message succeeds later; duplicate delivery
    after resolve writes nothing twice.

- [x] **3.3 Sender — initial requests.** Idempotency on (campaign,
  jurisdiction, contact_email) — existing thread → delete unsent. Gates
  in order: consent/active (delete + drop), daily cap (UTC midnight,
  counts all kinds, gates only initial requests; over → leave for
  retry), office cooldown on the shared jurisdiction row unless
  `bypass_cooldown` (leave for retry), anonymous-state guard (escalate
  `other`, drop). Terra draft under the §7.1 contract; mechanical
  assembly (subject with `[DLG-<token>]`, exact-records block,
  anonymous vs named signature, `X-Dialogue-Token`, verified From).
  Resend send (skipped under `dry_run`, payload logged, DB writes still
  happen); then one transaction: thread row (`request_sent`,
  `next_action_at` = now + interval), `emails` row
  (kind `initial_request`, `resend_id`), `last_contacted_at` stamp;
  then delete the message.
  - **Verify:** VirtualClock tests — gate order (a message that is both
    over-cap and cooling-down retries rather than escalating); 200
    sends today → 201st initial retries while a follow-up on the same
    campaign still sends; cooldown throttles a second campaign mailing
    the same office but `bypass_cooldown` does not; send-succeeded /
    commit-failed redelivery produces no second thread (idempotency
    triple); `dry_run` writes all rows with zero transport calls;
    anonymous + TN escalates `other` without sending.

- [x] **3.4 Sender — thread jobs.** `pr-followups` handling: `followup`
  (idempotent on `followup_index` vs the thread counter; 2–3 sentence
  nudge referencing first outbound subject and approximate wait;
  increments `followups_sent`), `clarification_reply` (answers only
  from the record description, 4,000-char inbound context; idempotent
  on (thread, inbound_email_id, kind)), `fee_agreement` (confirms exact
  `amount_cents` only). Replies reuse the office subject with `Re:` and
  token appended if missing; every send commits its `emails` row +
  status (`awaiting_reply`, `next_action_at` rescheduled) in one
  transaction, then deletes.
  - **Verify:** tests — a duplicate followup message with a stale
    `followup_index` is dropped without sending; a second
    clarification job for the same inbound is dropped once an outbound
    references it; subject `Re:`/token rules table-driven; counter and
    status commit atomically with the email row.

- [x] **3.5 Receiver — match, classify, react.** Consume
  `pr-inbound-mail`; message_id-per-campaign short-circuit. Match by
  token, else by sender address against an open thread; no match →
  thread-less `emails` row, stop. Classify (terra + reasoning, 6,000
  chars, degrade to `unclear`); **log the inbound row before any
  reaction**; then the §8 reaction table in one transaction:
  `data_provided`/ack-with-attachments → store attachments →
  `fulfilled` + clear `next_action_at` (fulfills even with zero stored);
  `payment_required` → fee flow (3.7); `referral` with address →
  `pr-contacts` (`source: "referral"`, `bypass_cooldown: true`) +
  `referred` (new thread later records `parent_thread_id`); referral
  without address / denial / unclear → the matching escalations;
  `needs_clarification` → enqueue reply job + `awaiting_reply`;
  bare acknowledgment → `awaiting_reply` rescheduled.
  - **Verify:** tests — duplicate message_id deleted unworked; token
    match beats a colliding sender-address match; unmatched mail
    persists thread-less and touches nothing else; every terminal
    reaction leaves `next_action_at` null so the 2.4 scan never selects
    it; handler crash after the inbound row commits →
    redelivery skips re-logging and completes the reaction.

- [x] **3.6 Attachment storage.** Per reference: type gate
  (accepted document/data types; rejection reason recorded in
  `attachment_refs`), sha256 → `SADD dedupe:{campaign}` (0 → record
  `duplicate` ref pointing at the existing key, skip upload), upload to
  `<campaign-slug>/<jurisdiction-slug>/<digest8>_<filename>` (sanitized,
  60-char slugs), append to `attachment_keys` + `attachment_refs`;
  delete the inbox-spool object only after the message commits; cold
  start rebuilds the Redis set from S3 key listings (digest8 embedded
  in keys, no re-hashing).
  - **Verify:** tests — `.png` and `.ics` rejected with reasons, `.pdf`
    and `.zip` stored; same bytes twice in one campaign → one object +
    one `duplicate` ref; same bytes across two campaigns → two objects;
    Redis flush then rebuild-from-listing suppresses the re-upload
    (idempotent overwrite otherwise, not data loss); spool object
    survives a crashed handler and is gone after the successful retry.

- [x] **3.7 Fee flow.** On `payment_required`: parse the largest dollar
  amount (≤ 6 digits, commas/cents tolerated) into integer cents;
  auto-agree only when amount parsed ∧ budget nonzero ∧
  committed-total + amount ≤ budget — then atomically book
  `spend_entries` (500-char note), enqueue `fee_agreement` with
  `amount_cents`, thread → `awaiting_reply`. Any failed leg →
  `payment_required` escalation whose detail names the leg.
  - **Verify:** table-driven tests — "$1,234.56" parses to 123456;
    "fees may apply" with no amount escalates "no clear amount"; over
    budget escalates stating both figures; zero budget escalates "no
    budget configured"; two concurrent fees against the last budget
    dollar admit exactly one (ledger check inside the transaction);
    ledger row exists before the agreement message.

## Phase 4 — end-to-end and operations

- [x] **4.1 Scenario suite (all fakes, virtual clock).** One shared fake
  world scripting whole lifecycles: happy path (register → seed →
  scrape → contact → initial send → ack → 10 virtual days → follow-up →
  `data_provided` with attachments → `fulfilled`, S3 + ledger + audit
  rows asserted); silence exhaustion (3 follow-ups over 30 virtual days
  → `no_response`, digest sent once); referral chain (parent
  `referred`, child thread bypasses cooldown, `parent_thread_id` set);
  fee inside then over budget; crashed sender mid-commit → no duplicate
  email to the office; consent revoked mid-flight → queues drain
  inertly with zero sends; poller/receiver at-least-once (same MIME key
  raced into the queue twice → one `emails` row); DLQ storm → exactly
  one `other` escalation per dead message.
  - **Verify:** each scenario is one test asserting end state *and* the
    timing budget in virtual time (e.g. silence escalation at exactly
    3 × interval + scan period, never before).

- [x] **4.2 Operational surface.** Admin API/CLI: campaign registration
  (config → row, validation from 1.1), start/stop via `active`,
  escalation list/resolve (resolution may set any status;
  `pending_send` resolution also enqueues `pr-contacts` with
  `source: "human_approved"`, `bypass_cooldown: true`); kill-as-purge
  (rows FK-ordered: spend → escalations → emails → threads → targets →
  campaign; `dedupe:{campaign}` key; `<campaign-slug>/` S3 prefix;
  jurisdictions and mail bucket untouched).
  - **Verify:** tests — resolving to `pending_send` produces a
    human_approved contact message that skips the review gate; purge
    leaves zero campaign rows/keys/objects while jurisdiction contacts
    and raw MIME survive; post-purge in-flight messages are dropped by
    the 3.1 gate.

- [ ] **4.3 Manual smoke against real backends.** Extend docker-compose
  (Postgres, Redis, LocalStack SQS+S3 with the four queues + DLQs);
  README runbook: boot migration, register a one-county `dry_run`
  campaign with a seeded contact, run the service, watch the logged
  payload, drop a fixture MIME into the LocalStack mail bucket, watch
  it classify and store; purge checklist.
  - **Verify:** manual checklist in the README executed once; purge
    leaves zero rows/keys/objects for the campaign.

---

## Decisions embedded in this plan

- **One package, not three**: the spec is a single microservice; the
  orchestrator and the three consumers are modules wired into one
  process by `cli.py`. Shared infra (queue, KV, object store, search,
  fetcher, clock — fakes included) is imported from `harvest_core`
  rather than forked, so the FakeQueue visibility/DLQ semantics and
  VirtualClock are already contract-tested.
- **`harvest_core.ObjectStore` grows `get`/`list_keys`/`delete`**
  (step 0.2) instead of a parallel blob port — the mail bucket and the
  inbox spool are plain object storage.
- **Crawling goes through the existing `Fetcher` port**, with the
  link-selection/harvest logic in `scraper.py` where fakes can drive
  it; whether the real adapter is Scrapy or httpx is a wiring detail
  invisible to every test.
- **MIME parsing is stdlib and in-process** (no port) — it's pure
  computation on bytes the ObjectStore already returns.
- **Registration-time validation rejects anonymous × blocked-state
  scopes** (the spec's "better still"), while the per-target sender
  guard stays as the runtime backstop for hand-edited rows.
- **No live network in the default test run** — Serper, OpenAI, Resend,
  and SES are all behind ports with fixture-driven fakes.
- **Virtual clock everywhere** — cooldowns, the UTC daily cap, and
  follow-up intervals are asserted at exact boundaries (e.g. enqueue at
  day 10, not day 9.99), which real sleeps cannot do.
