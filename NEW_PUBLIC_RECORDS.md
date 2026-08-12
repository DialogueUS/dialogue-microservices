# Specification: the public-records pipeline (queue-based redesign)

The target architecture for the campaign mode — the system that sends
statutory public-records requests (CPRA/FOIA) by email and manages each
resulting conversation. This document describes the *new* design: a
single microservice running three concurrent tasks (**scraper**, **email
sender**, **email receiver**) plus an **orchestrator**, communicating
through Amazon SQS queues, with Postgres as the system of record, S3 for
mail and documents, and ElastiCache (Redis) for content dedupe.

Where the general design is silent on a specific — status vocabularies,
prompt contracts, classification categories, fee handling, numeric
limits — the value is inherited from the old pipeline and stated exactly
(see `OLD_PUBLIC_RECORDS.md` for the as-built system this replaces).
Config keys, queue message schemas, persisted status values, table and
column names, and numeric constants below are all part of the contract.

---

## 1. Purpose and non-goals

A **campaign** fans one records request out over a set of US
jurisdictions: discover each office's records email, draft and send a
formal request, read every reply, follow up on silence, agree to small
fees inside a budget, store what the office produces, and escalate to a
human whenever the next step requires judgment or money beyond the
system's authority.

**Live sends are real legal requests made in a real person's name.** The
consent gate is unchanged: a campaign whose `requester.consent_confirmed`
is false is never seeded by the orchestrator and never produces queue
work — there is no dry-run exception on consent; rehearsals use a
sandbox Resend key / dry-run send flag instead.

Dropped relative to the old pipeline (deliberately out of scope here):

- **The research channel** (sweeping the web/government APIs for
  already-published copies of requested records). The scraper in this
  design discovers *contacts*, not records.
- **The LLM catalog/judging stage.** Attachments an office produces are
  stored directly (after type and dedupe checks); there is no
  responsiveness verdict or promotion step.
- **The dry-run email transport.** Sending is the Resend API; rehearsal
  is a per-campaign `dry_run` flag that short-circuits the API call and
  logs the payload instead.

---

## 2. Topology

One microservice process. Inside it:

- The **orchestrator**: the only component that originates work. It
  seeds campaigns (generates search queries), polls the SES mail bucket,
  and schedules time-based follow-ups. It is single-flight per concern
  (one seeding pass, one mail poll, one follow-up scan at a time).
- Three **task consumers**, each a thread pool draining one or two SQS
  queues: the scraper, the email sender, the email receiver. Any number
  of threads per pool is safe; all idempotency lives in the message
  contracts and conditional DB writes, not in the consumers.

### 2.1 Queues (Amazon SQS, standard queues)

| queue | producer | consumer | payload |
|---|---|---|---|
| `pr-search-queries` | orchestrator | scraper | one search query + campaign/jurisdiction context |
| `pr-contacts` | scraper (also receiver, on referral) | email sender | one candidate contact email + campaign context |
| `pr-followups` | receiver (reactive), orchestrator (silence-based) | email sender | one follow-up/reply job for an existing thread |
| `pr-inbound-mail` | orchestrator | email receiver | one inbound email (text + attachment references) |

SQS delivery is at-least-once; every consumer must be idempotent per
message (each workflow below states its idempotency key). Settings for
all four queues:

- **Visibility timeout: 900 s** (the old claim-staleness interval — a
  crashed worker's message reappears after at most this long).
- **Redrive to a per-queue DLQ** after `maxReceiveCount` receives:
  **3** for `pr-inbound-mail` (old classify retry budget), **5** for the
  sender queues (old send retry budget), **3** for `pr-search-queries`.
  A message landing in a DLQ raises an `other` escalation naming the
  queue and payload summary.
- A message is deleted from the queue only after its transaction
  commits; failures let visibility expire (that *is* the retry).

### 2.2 Models

- **GPT 5.6-luna** — search-query generation (orchestrator) and contact
  extraction/picking (scraper).
- **GPT 5.6-terra** — email drafting (sender) and inbound classification
  (receiver; classification runs **with reasoning enabled**).
- LLM request timeout **120 s**, **2** retries (carried over).

---

## 3. Data model

### 3.1 Postgres

**`campaigns`** — one row per campaign (replaces config-file-as-truth;
the row is created from the campaign config at registration).
`id`, `name` (unique slug), `config_yaml` (the whole §11 document as
YAML — every config key is read back by parsing this column, so the
document and the row cannot disagree; the file the operator registers
is only the input), `active` (bool — the run switch), `seeded` (bool,
default false — set true by the orchestrator only after **all** search
queries for the campaign are enqueued), `created_at`.

`name` is the one §11 key that is *also* a column, because it is
identity: the lookup key, the unique constraint that refuses a duplicate
registration, and the slug behind the S3 and Redis prefixes. The column
wins — a config update that changes `name` is rewritten to the stored
one rather than renaming a live campaign out from under its objects.
`active` and `seeded` are runtime state and appear in no document.

**`jurisdictions`** — unchanged from the old pipeline (Census-seeded;
`name`, `state`, `level`, `parent_name`), plus the shared contact
columns: `contact_email`, `contact_name`, `contact_url`,
`contact_verified` (written false, reserved for humans),
`last_contacted_at` — the office cooldown clock, still **shared across
campaigns**.

**`email_threads`** — one row per (campaign, jurisdiction)
correspondence; the agent. Columns: `id`, `campaign_id`,
`jurisdiction_id`, `thread_token` (unique; 16 hex chars = 8 random
bytes), `contact_email` (the address this thread actually mails —
per-thread, so referrals never mutate the shared jurisdiction row),
`status` (§4), `parent_thread_id` (referral lineage), `followups_sent`
(int, default 0), `next_action_at` (when the silence-based follow-up
scan should look at this thread; null = nothing scheduled),
`attachment_keys` (JSON array of S3 document keys produced on this
thread), `notes`, `created_at`, `updated_at`.

**`emails`** — one row per message in either direction, the complete
audit trail (child of `email_threads`). `id`, `thread_id`, `direction`
(`inbound` | `outbound`), `from_address`, `to_address`, `subject`,
`body`, `kind` (outbound: `initial_request` | `followup` |
`clarification_reply` | `fee_agreement`; null inbound), `classification`
(inbound: category + summary + confidence JSON; null outbound),
`message_id` (inbound dedupe key, unique per campaign: the Message-ID
header when present, else `sha:` + first 32 hex of sha256 over
`from|subject|body`), `source_key` (S3 key of the raw MIME for inbound;
also the mail-poller dedupe key), `resend_id` (Resend API message id for
outbound), `attachment_refs` (JSON: filename, content type, S3 key or
rejection reason per attachment), `created_at`.

**`escalations`** — `id`, `campaign_id`, `thread_id` (nullable: a
contact-review escalation can precede any thread), `reason` (enum,
stored by name: `payment_required`, `referral_no_address`, `denial`,
`unclear_reply`, `no_response`, `no_contact_found`,
`contact_needs_review`, `other`), `details` (message bodies truncated to
2,000 chars), `status` (`open` | `resolved`), `resolution`, `notified`
(each escalation is alerted exactly once), `created_at`, `resolved_at`.
Creating one always also parks the thread (if any) at `needs_human`.

**`spend_entries`** — the fee ledger, unchanged in contract:
`campaign_id`, `thread_id`, `amount_cents` (**integer cents, never
floats**), `kind` (`fee_authorized`), `note` (first 500 chars of the
office's message), `remitted` (manual checkbox — remittance has no
payment rail), `notified`, `created_at`. The campaign's committed total
is the sum over this table; the budget cap is checked against it before
any new fee is agreed.

**`search_targets`** — seeding bookkeeping: `campaign_id`,
`jurisdiction_id`, `queries_enqueued` (int), `resolved` (bool — a
contact was found or the target escalated), `created_at`. One row per
email goal; enforces the per-target query hard limit and makes seeding
idempotent.

### 3.2 Redis (ElastiCache)

- `dedupe:{campaign}` — a Redis **set** of sha256 hex digests of every
  document stored for that campaign. Membership check + insert is
  `SADD` (returns 0 → duplicate, skip upload; 1 → new, proceed). No TTL;
  the set is deleted when the campaign is killed.
- Redis is a *cache of truth derivable from S3*: on cold start or cache
  loss, the set is rebuilt by listing the campaign's S3 prefix and
  re-hashing is not required (digests are embedded in object keys, §3.3).

### 3.3 S3

- **Mail bucket** (SES receiving): MX → SES receipt rule → raw MIME
  objects. **Never the documents bucket.** The orchestrator's poller is
  read-only here — nothing is marked, moved, or deleted.
- **Documents bucket**: per-campaign folders. An accepted attachment is
  stored at `<campaign-slug>/<jurisdiction-slug>/<digest8>_<filename>`
  (digest8 = first 8 hex of the sha256; filename sanitized; slugs capped
  at 60 chars). Presigned download URLs are 120 s.

---

## 4. The thread lifecycle

`email_threads.status`, persisted as uppercase enum names in the column,
lowercase values in the API (same convention as the old pipeline):

```
 (scraper finds contact) ──> pending_send ──(sender delivers)──> request_sent
                                  │                                   │
                                  │                          (acknowledged / replied /
                                  │                           fee agreed / clarified)
                                  v                                   v
                             needs_human <──(denial, fees over   awaiting_reply
                                  ^          budget, unclear          │
                                  │          reply, silence           │
                                  │          exhausted, DLQ)          │
                                  ├──(office produced records)──> fulfilled
                                  └──(office redirected us; new
                                      contact re-enqueued)──────> referred
```

Values: `pending_send`, `request_sent`, `awaiting_reply`, `fulfilled`,
`referred`, `needs_human`, `failed`. Two carried-over rulings:

- **`failed` is never assigned automatically.** Every give-up path is an
  escalation to `needs_human`; `failed` exists only for a human to
  resolve an escalation into.
- `needs_human` is a parking state, not terminal: resolving the
  escalation sets any status the operator chooses (commonly
  `pending_send` to approve a reviewed contact and re-enter the send
  path).

There is no `discovering`/`contact_found` state: pre-contact work lives
in the queues and `search_targets`, and a thread row is created only
when a contact is accepted (status `pending_send`).

**The thread token** is unchanged: 16 hex characters, globally unique,
stamped into every outbound subject as `[DLG-<token>]` and into an
`X-Dialogue-Token` mail header; inbound matching accepts the header
first, then a subject regex matching both `[DLG-…]` and legacy
`[RF-…]`. The token is what makes at-least-once delivery safe end to
end: a duplicate send is harmless, a lost one is not.

---

## 5. The orchestrator

Runs three periodic concerns on its own scheduler (default period
**300 s** each, the old tick interval). Each concern skips silently if
its previous run is still in flight.

### 5.1 Seeding

For each campaign in Postgres with `active = true`, `seeded = false`,
and `consent_confirmed = true`:

1. Resolve the campaign scope to concrete jurisdictions (same semantics
   as the old pipeline: `levels` default `[county]`, `states` default
   `[ALL]`, optional `only` allow-list; comma-county exclusion applies).
2. For each in-scope jurisdiction without a `search_targets` row,
   create one, then generate search queries with **GPT 5.6-luna**. The
   model receives the jurisdiction name/state/level and the campaign's
   `record_type`, and returns JSON
   `{"queries": ["...", ...]}` — queries aimed at finding the office's
   public-records / FOIA / CPRA request contact email. The first query
   is always the deterministic fallback pattern
   `"<name> <state> public records request CPRA email clerk"` (carried
   over), so an LLM failure degrades to one usable query, never zero.
3. **Hard limit: at most 3 queries per email goal** (per
   search-target). Excess model output is truncated; `queries_enqueued`
   records the count.
4. Enqueue each query to `pr-search-queries`. Message body:

   ```json
   {"campaign_id": 1, "jurisdiction_id": 123, "search_target_id": 456,
    "query": "…", "query_index": 0}
   ```

5. Only after **every** in-scope target has its queries enqueued, set
   `campaigns.seeded = true`. Seeding is idempotent: a crash mid-pass
   re-runs and skips targets that already have rows; duplicate queue
   messages are absorbed downstream (the scraper's writes are
   conditional).

If the jurisdiction already carries a seeded `contact_email`, no queries
are generated: the target is marked `resolved` and a `pr-contacts`
message is enqueued directly (seeded contacts are trusted and never
flagged for review — carried over). The resolve precedes the enqueue
deliberately: a crash in that window drops the contact, and re-sending
instead would risk two copies of a real request racing through the
sender pool, which is the worse failure. Recovering the dropped contact
is a human act (re-register, or resolve it from the campaign's
escalations).

**Test campaigns.** A campaign whose config carries `test_contacts`
(§11) takes its targets from that list instead of `scope`, and the whole
search stage is skipped structurally: no census load, no query
generation, no `pr-search-queries` message, so Serper is never reached.
Each entry names a jurisdiction (`jurisdiction`, `state`, `level`) and
the address that stands in for its office; the row is created if it does
not exist, the target is resolved by the same compare-and-set, and a
`pr-contacts` message is enqueued with `source: seeded`.

The address stays *in the campaign config* and is never written to
`jurisdictions.contact_email`: that row is shared, so a test address
there would silently redirect every real campaign in the same scope.
For the same reason a test campaign neither observes nor advances the
shared office clock — its contact messages carry `bypass_cooldown`, and
its sends leave `last_contacted_at` alone. Everything downstream is the
real thing: consent, the daily cap, the anonymous-state refusal,
drafting, threading, follow-ups and inbound classification all run
unchanged, and `dry_run` remains the independent switch for whether
Resend is actually called.

### 5.2 Mail polling

**Only when `pr-inbound-mail` is empty** (approximate-visible and
approximate-in-flight both 0), list the SES mail bucket. Skip the
`AMAZON_SES_SETUP_NOTIFICATION` marker object. For each key not already
present as some `emails.source_key`, download and parse the MIME
(prefer the plain-text body part over HTML), extract the token
(header first, subject second), and enqueue to `pr-inbound-mail`:

```json
{"source_key": "…", "message_id": "…", "thread_token": "…" | null,
 "from_address": "…", "subject": "…", "body": "…",
 "attachments": [{"filename": "…", "content_type": "…",
                  "s3_tmp_key": "…"}]}
```

Attachment bytes are not carried in SQS (256 KB message cap): the poller
spools each attachment to the documents bucket under
`<campaign-slug>/inbox-spool/<digest8>_<n>_<filename>` and passes the
key. At most **200** new messages per poll. Reading is non-destructive;
the `source_key` check is the only thing preventing re-enqueue, so the
receiver must still dedupe on `message_id` (at-least-once).

The empty-queue precondition means the poller never races its own
backlog, but it also means a steady drip of inbound can delay a poll —
acceptable at current volume (same tradeoff class as the old full-prefix
listing, which this design retains).

### 5.3 Silence-based follow-up scheduling

Scan `email_threads` where `status IN (request_sent, awaiting_reply)`
and `next_action_at <= now()`, batch **50**:

- If `followups_sent >= limits.max_followups` (default **3**): escalate
  `no_response` ("No response after N follow-ups."), thread →
  `needs_human`, `next_action_at` cleared.
- Otherwise enqueue to `pr-followups`
  (`{"thread_id": …, "kind": "followup"}`), and *immediately* set
  `next_action_at` one interval out (`limits.followup_interval_days`,
  default **10**) — the reschedule happens at enqueue time, not send
  time, so a slow sender never causes double-enqueue.

---

## 6. The scraper

Consumes `pr-search-queries`. Idempotency key: `search_target_id` — if
the target's row is already `resolved`, the message is deleted without
work (a later query for an already-found contact is a no-op).

Per message:

1. **Search** via **Serper** (Google results; API key
   `SERPER_API_KEY`). Page size **8** results. Keep only results passing
   the official-site heuristic (government domains: `.gov`, `.us`,
   state/county/city official sites; carried over from the old
   pipeline's shared heuristic).
2. **Crawl** each surviving result with **Scrapy**: fetch the page
   (HTTP timeout **12 s**) plus same-domain links whose anchor or URL
   suggests records/contact pages, to a per-query page budget of **4**
   pages. Regex-harvest email addresses from page text; each candidate
   keeps **120 chars of context before / 60 after** the match
   (whitespace collapsed). Junk local-parts are dropped outright:
   `noreply`, `no-reply`, `donotreply`, `mailer-daemon`, `postmaster`,
   `abuse`, `webmaster`. Harvesting stops at **12 candidates** per
   query.
3. **Pick** with **GPT 5.6-luna**: the model is shown the office name
   and the candidate list (each with source URL and up to 200 chars of
   context) and must return JSON
   `{"email": <one of the listed addresses, or null>,
   "confidence": <0..1>}` — instructed to prefer
   clerk/records/foia/cpra/pra addresses on the office's own domain, to
   **never invent an address not in the list**, and to return null when
   in doubt. Accepted only if the address appears verbatim among the
   candidates and confidence ≥ `contacts.min_confidence` (default
   **0.6**).
4. **Outcome** (all writes in one transaction, conditional on the target
   not already being `resolved` — the duplicate-message guard):

   | result | effect |
   |---|---|
   | generic-looking hit | contact written to the jurisdiction row (`contact_email`/`name`/`url`, `contact_verified = false`); target `resolved`; enqueue `pr-contacts` |
   | personal-looking hit | target `resolved`; escalate `contact_needs_review` with the address and context — **never enqueued for sending** |
   | no candidates / pick rejected | nothing written; if this was the target's **last** outstanding query (all `queries_enqueued` messages consumed without a hit), escalate `no_contact_found` |
   | transient failure (429, timeout, 5xx) | do not delete the message — let visibility expire and SQS retry. **Never escalate on a hiccup.** |

**The personal-mailbox gate** is carried over verbatim: an accepted
address whose local-part contains none of the generic keywords
(`record`, `clerk`, `foia`, `cpra`, `pra`, `prr`, `public`, `request`,
`sunshine`, `info`, `contact`, `cityhall`) is treated as a personal
mailbox and goes to a human, never to the sender queue.

`pr-contacts` message body:

```json
{"campaign_id": 1, "jurisdiction_id": 123, "contact_email": "…",
 "source": "scraper" | "seeded" | "referral" | "human_approved",
 "bypass_cooldown": false}
```

(`referral` and `human_approved` sources set `bypass_cooldown: true`.)

---

## 7. The email sender

One thread pool (size `limits.max_concurrent_sends`, default **8**)
consuming two queues: `pr-contacts` (initial requests) and
`pr-followups` (everything on an existing thread).

### 7.1 Initial requests (`pr-contacts`)

Idempotency key: (campaign_id, jurisdiction_id, contact_email) — if a
thread already exists for the triple, delete the message without
sending.

Pre-send gates, checked in order:

- **Consent**: the campaign row must still have
  `consent_confirmed = true` and `active = true`; otherwise the message
  is deleted and dropped (a stopped campaign drains its queues inertly).
- **The daily cap**: at most `limits.daily_send_cap` (default **200**)
  outbound `emails` rows per campaign since **UTC midnight** — all
  kinds count against the cap, but **only initial requests are gated by
  it** (replies, fee agreements, and follow-ups send regardless; carried
  over). Over cap → do not delete; let visibility expire and retry
  later.
- **The office cooldown**: if the jurisdiction's `last_contacted_at` is
  within `limits.per_office_cooldown_days` (default **7**) and
  `bypass_cooldown` is false, requeue the same way. The cooldown clock
  stays on the shared jurisdiction row — two campaigns mailing the same
  office still throttle each other.
- **The anonymous-state guard**: if `requester.anonymous` is true and
  the jurisdiction's state is one of **AL, AR, TN, VA, DE, NJ, KY**
  (states requiring requester identity), escalate
  `other` for this target and drop — the old pipeline's whole-stage
  failure is replaced by a per-target refusal, which is the behavior the
  old spec said a rebuild should adopt. Better still, registration-time
  validation should reject the scope/anonymous combination outright.

**Drafting (GPT 5.6-terra).** The contract is carried over verbatim:
the model's output is pasted as the body, so it must begin with the
salutation, write no subject line, no sign-off, and no restatement of
the records scope — an "Exact records requested" block and the signature
are appended mechanically. It must never fabricate legal citations
beyond the given `legal_basis` and never promise payment. The prompt
carries the jurisdiction, legal basis, `record_description`, requester
identity (or, when anonymous, an instruction to include no name plus the
electronic-delivery preference), and the standing asks: fee estimates
before costs are incurred; exemption citations and segregable portions
if partially denied.

Assembled email:

- Subject: `Public Records Request — <record_type> — <name>, <ST>
  [DLG-<token>]`
- Body: LLM text + `---\nExact records requested:\n` +
  `record_description` + signature (anonymous: "Thanks for your time."
  plus the reply address; named: "Sincerely," name, organization, email,
  phone — whichever are set).
- From: `email.from_address` (a Resend-verified domain address —
  required config), `X-Dialogue-Token` header set.
- To: the message's `contact_email`.

**Send via the Resend API** (`RESEND_API_KEY`; HTTP timeout **30 s**).
Then in **one transaction**: create the `email_threads` row
(status `request_sent`, `next_action_at` = now +
`followup_interval_days`), the outbound `emails` row (kind
`initial_request`, `resend_id` recorded), and stamp the jurisdiction's
`last_contacted_at`; then delete the SQS message. If the send succeeded
but the commit fails, the retry is absorbed by the idempotency check
plus the thread token (at-least-once, duplicate-harmless — invariant
carried over). Under `dry_run`, the Resend call is skipped and the
payload logged; all DB writes still happen so the whole flow rehearses.

A Resend failure does not delete the message; after **5** receives the
DLQ redrive escalates `other` naming the address and kind.

### 7.2 Thread jobs (`pr-followups`)

Message body:

```json
{"thread_id": 42, "kind": "followup" | "clarification_reply"
                          | "fee_agreement",
 "inbound_email_id": 7,          // the email being answered, if any
 "amount_cents": 2500}           // fee_agreement only
```

Idempotency: for `followup`, the pair (thread_id, `followups_sent`
value at enqueue time) — carried in the message as `followup_index`; a
duplicate whose index is behind the thread's counter is dropped. For
replies, (thread_id, inbound_email_id, kind) — a second message for the
same inbound is dropped if an outbound row already references it.

Drafting contracts (all GPT 5.6-terra, carried over):

- **`followup`**: a 2–3 sentence, courteous, never-threatening status
  nudge referencing the original subject (the thread's first outbound
  subject, else `Public Records Request [DLG-<token>]`) and the
  approximate wait (`followup_interval_days × (followups_sent + 1)`
  days). On send: increment `followups_sent`.
- **`clarification_reply`**: answers **only from the record
  description** — if unanswerable from it, politely restate and offer to
  narrow; never agree to fees; never invent requester facts. Inbound
  body context truncated to 4,000 chars.
- **`fee_agreement`**: confirm the exact `amount_cents` and nothing
  else; ask for accepted payment methods and an invoice/remittance
  address *unless the office already stated one*; never payment details
  or card numbers.

Replies reuse the office's subject, prefixed `Re:` if absent, with the
token appended if missing. Every send writes its `emails` row and any
thread-status change in one transaction, then deletes the SQS message.
Thread status after a reply/agreement send: `awaiting_reply`, with
`next_action_at` rescheduled one interval out.

---

## 8. The email receiver

Consumes `pr-inbound-mail`. Idempotency key: `message_id` unique per
campaign — an `emails` row already holding it means the message is
deleted without work.

Per message:

1. **Match** to a thread: by `thread_token` first (globally unique —
   tokens cross campaigns safely); else by sender address equal to a
   thread's `contact_email` where the thread is open (`request_sent` or
   `awaiting_reply`). No match → record the mail in a campaign-less
   `emails` row with `thread_id` null and stop (kept forever; a human
   can attach it later — same dead-end as the old pipeline, accepted).
2. **Classify** with **GPT 5.6-terra, reasoning enabled**. The model
   returns JSON `{"category", "summary", "confidence"}`; body truncated
   to **6,000 chars**. Categories are the old pipeline's, verbatim:
   `data_provided`, `payment_required`, `needs_clarification`, `denial`,
   `referral`, `acknowledgment`, `unclear`. Prompt rulings carried over:
   payment is `payment_required` only when payment is *necessary to
   proceed* — boilerplate "fees may apply" in an auto-reply is not; a
   referral should carry `"referral_email"` when one is given; a request
   for information the system doesn't have or shouldn't give is
   `unclear` (escalate rather than improvise). Unknown category or parse
   failure degrades to `unclear`. **Confidence is recorded but never
   thresholded** — the conservative prompt is the only gate.
3. **Log** the inbound `emails` row (classification in
   `classification`) — always, before any reaction.
4. **React**, one transaction per message:

| category | reaction |
|---|---|
| `data_provided` — or `acknowledgment` *with* stored attachments | store attachments (§8.1); thread → `fulfilled`, `next_action_at` cleared. `data_provided` fulfills even with zero attachments stored — an office pointing at inline links is trusted; the links are not fetched (carried over, still a known gap). |
| `payment_required` | the fee flow (§9) |
| `referral` with an address | enqueue `pr-contacts` (`source: "referral"`, `bypass_cooldown: true`, same jurisdiction); this thread → `referred`, `next_action_at` cleared; the new thread created by the sender records `parent_thread_id` |
| `referral` without an address | escalate `referral_no_address` |
| `denial` | escalate `denial` |
| `needs_clarification` | enqueue `pr-followups` kind `clarification_reply`; thread → `awaiting_reply`, `next_action_at` rescheduled |
| `acknowledgment` (no attachments) | thread → `awaiting_reply`, `next_action_at` rescheduled |
| `unclear` | escalate `unclear_reply` |

Once no further action is pending on a thread — `fulfilled`, `referred`,
or parked at `needs_human` — its status reflects that and
`next_action_at` is null, so the orchestrator's follow-up scan never
touches it again.

A handler exception leaves the SQS message for redelivery; the third
failed receive redrives to the DLQ and escalates `other`.

### 8.1 Attachment storage

For each attachment reference on the message (bytes fetched from the
inbox-spool key):

1. **Type gate**: must be a document or data file — accepted content
   types/extensions: PDF, Word (`.doc`/`.docx`), Excel
   (`.xls`/`.xlsx`), CSV, XML, JSON, plain text, ZIP. Images, HTML
   bodies, calendar invites, and signature cruft are rejected (the
   rejection reason is recorded in the email row's `attachment_refs`).
2. **Dedupe via Redis**: compute sha256; `SADD dedupe:{campaign} <hex>`.
   Returns 0 → this campaign already stores identical bytes: skip the
   upload, record the ref as `duplicate` pointing at the existing key.
   (This is a deliberate change from the old pipeline, which never
   deduped attachments.)
3. **Upload** to
   `<campaign-slug>/<jurisdiction-slug>/<digest8>_<filename>` in the
   documents bucket; append the key to the thread's `attachment_keys`
   and the email row's `attachment_refs`.

Dedupe scope is **per campaign** (the Redis set is campaign-keyed): two
campaigns receiving the same file each store their own copy, keeping
campaigns independently killable — carried over from the old
owner-scoped rule. The inbox-spool object is deleted after the message
commits (unlike the raw MIME in the mail bucket, which is never
touched).

---

## 9. The fee flow

Carried over intact. On `payment_required`: parse the **largest dollar
amount** in the message (`$` amounts up to 6 digits, commas and cents
tolerated), as integer cents. Auto-agree only when all three hold: an
amount parsed, `limits.fee_budget_usd` is nonzero, and
committed-total + amount ≤ budget. Then atomically: book the
`spend_entries` row (note = first 500 chars of their message), enqueue
`pr-followups` kind `fee_agreement` with `amount_cents`, thread →
`awaiting_reply`.

Anything else escalates `payment_required` with a detail naming the
failed leg: amount exceeds remaining budget (stating both), no budget
configured, or no clear amount to authorize. **Remittance is always
manual** — `spend_entries.remitted` is a human's checkbox and the table
is the "what do we owe" worklist.

---

## 10. Cross-cutting invariants

Carried forward as settled rulings:

1. **No consent, no work.** `consent_confirmed` gates seeding and is
   re-checked at every send.
2. **State commits with the action it records.** Every send writes its
   email row and thread-status change in one transaction; every
   reaction commits with its inbound row.
3. **At-least-once everywhere, idempotent per message.** SQS may
   redeliver; every consumer states its idempotency key; the thread
   token makes duplicate *sends* harmless.
4. **Persist before interpreting.** The inbound email row is written
   before its reaction runs; raw MIME stays in the mail bucket
   untouched.
5. **Never auto-mail a personal mailbox.** Generic-keyword local-parts
   only; everything else goes through `contact_needs_review`.
6. **Never escalate on a transient.** Provider hiccups (429, timeout,
   5xx) leave the message in the queue for redelivery; escalations are
   for states only a human can advance — the DLQ redrive is the
   backstop that converts *persistent* failure into an escalation.
7. **Money is bounded and audited.** Integer cents, budget-capped,
   booked to the ledger before the agreement email is enqueued;
   remittance manual.
8. **Every give-up is an escalation**, every escalation notified exactly
   once, and `failed` is never set automatically.
9. **Dedupe before storage spend** — sha256 via Redis per campaign; and
   never re-download what a key check can skip (mail poller
   `source_key`).

---

## 11. Configuration reference

Per-campaign (registered from a YAML file, stored verbatim in
`campaigns.config_yaml`, and read back by parsing that column):

| key | default | notes |
|---|---|---|
| `record_type` / `record_description` | *(required)* | |
| `legal_basis` | state-law + FOIA boilerplate | see §3.1 |
| `requester.name` / `email` | *(required)* | |
| `requester.organization` / `phone` / `mailing_address` | *(none)* | signature enrichment only |
| `requester.consent_confirmed` | `false` | the master gate |
| `requester.anonymous` | `true` | no name in drafts; refused per-target in AL/AR/TN/VA/DE/NJ/KY |
| `scope.levels` / `states` / `only` | `[county]` / `[ALL]` / — | |
| `dry_run` | `true` | Resend call skipped, payload logged; **start here** |
| `limits.max_concurrent_sends` | `8` | sender thread-pool size |
| `limits.per_office_cooldown_days` | `7` | shared jurisdiction clock |
| `limits.max_followups` | `3` | then `no_response` |
| `limits.followup_interval_days` | `10` | |
| `limits.daily_send_cap` | `200` | outbound emails/UTC day; gates initial requests only |
| `limits.fee_budget_usd` | `0.0` | 0 = every fee escalates |
| `contacts.min_confidence` | `0.6` | luna contact-pick floor |
| `test_contacts[]` | *(empty)* | `jurisdiction` / `state` / `level` (default `county`) / `email`. Non-empty ⇒ a **test campaign**: targets come from this list, `scope` is ignored, and no search or census call is made (§5.1) |
| `notify_email` | *(none)* | escalation/fee digest recipient |

Service-level environment: `DATABASE_URL`, `REDIS_URL`,
`SERPER_API_KEY`, `RESEND_API_KEY`, `OPENAI_API_KEY` (luna + terra),
`AWS_*` credentials, mail bucket name, documents bucket name, the four
queue URLs, `email.from_address` (Resend-verified sender — required for
any live campaign).

### Hardcoded constants

| constant | value |
|---|---|
| SQS visibility timeout (all queues) | 900 s |
| DLQ maxReceiveCount: inbound / sender / search | 3 / 5 / 3 |
| orchestrator period (seed, poll, follow-up scan) | 300 s |
| queries per email goal (hard limit) | 3 |
| search page size | 8 results |
| scraper page budget / HTTP timeout | 4 pages / 12 s |
| contact candidates / context window | 12 / 120 + 60 chars |
| junk local-parts, generic keywords | old-pipeline lists, verbatim (§6) |
| thread token | 16 hex chars, `[DLG-…]` (legacy `[RF-…]` accepted inbound) |
| anonymous-blocked states | AL, AR, TN, VA, DE, NJ, KY |
| mail poll cap | 200 messages |
| follow-up scan batch | 50 threads |
| body truncations: classify / clarification / escalation / fee note | 6,000 / 4,000 / 2,000 / 500 chars |
| Resend HTTP timeout | 30 s |
| LLM timeout / retries | 120 s / 2 |
| presigned download URLs | 120 s |
| slug length cap | 60 chars |
| document key | `<campaign>/<jurisdiction>/<digest8>_<filename>` |

### Cost model

- **Serper**: at most 3 searches per jurisdiction lacking a seeded
  contact, once ever (retried only on transients). Bounded by scope
  size; no resweep exists.
- **LLM**: one luna query-generation call per jurisdiction + one luna
  pick per query with candidates; one terra draft per outbound email;
  one terra classification (with reasoning) per inbound.
- **Real money**: `fee_budget_usd` remains the only autonomous-spend
  dial and defaults to zero.

---

## 12. Operational surface

- **Start/stop**: `campaigns.active`. Stopping stops the orchestrator
  from producing new work; in-flight queue messages drain inertly (the
  sender's consent/active gate drops them without sending).
- **Escalations** are listed and resolved through the admin API; a
  resolution records free text and may set the thread to any status
  (`pending_send` to approve a reviewed contact is the common case —
  which also enqueues a `pr-contacts` message with
  `source: "human_approved"`).
- **Kill is a permanent purge**: delete the campaign's rows FK-ordered
  (spend entries, escalations, emails, threads, search targets, then
  the campaign row), the Redis `dedupe:{campaign}` key, and the
  campaign's S3 prefix `<campaign-slug>/` (documents and inbox spool).
  **Not** deleted: jurisdiction rows and their discovered contacts
  (shared across campaigns), raw MIME in the mail bucket, and any
  in-flight SQS messages — consumers drop messages whose campaign no
  longer exists.
- **Notification digest**: on the orchestrator's period, per campaign
  with `notify_email` set: collect un-notified open escalations and
  un-notified spend entries; if any, send one plain-text digest via
  Resend (counts by reason, up to 25 line items per kind, fee-budget
  position, "budget EXHAUSTED" warning at zero remaining), then mark
  every included row `notified` — exactly once per row. The digest
  bypasses the thread machinery (token literally `notify`, header
  `X-Dialogue-Kind: alert`).

---

## 13. Known limitations and accepted tradeoffs

- **The daily cap still gates only initial requests** — replies, fee
  agreements, and follow-ups send past it.
- **`data_provided` fulfills without verifying storage**; inline links
  in reply bodies are not fetched.
- **Unmatched mail is a dead end** (thread-less email row, no automatic
  retry) — offices replying from a new address with a rewritten subject
  land there.
- **The mail poller lists the whole bucket prefix each poll** and only
  runs when the inbound queue is empty — a steady inbound drip delays
  polling, and an ever-growing mail bucket slows every poll (no
  archival or pagination cursor).
- **The office cooldown is campaign-global** (shared jurisdiction row);
  two campaigns mailing the same office throttle each other.
- **Classification confidence is recorded, never enforced** — the
  conservative prompts and the `unclear` → escalation path are the only
  guard.
- **Redis loss degrades dedupe, not correctness**: until the set is
  rebuilt from S3 key listings, a duplicate attachment may be stored
  twice under the same key (an idempotent overwrite, not data loss).
- **No research channel and no responsiveness judging** — an off-topic
  attachment that passes the type gate is stored and the thread
  fulfilled; quality control of productions is a human concern in this
  design.
- **`failed` thread status and `contact_verified` remain write-never
  fields** — reserved surface for humans, no automatic transitions.
