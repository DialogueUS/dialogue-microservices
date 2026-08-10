# dialogue-microservices

Two systems share this workspace: the **harvesting system**
(`NEW_HARVESTER.md`, v0.4) and the **public-records pipeline**
(`NEW_PUBLIC_RECORDS.md`) — see its section near the end of this file.

## The harvesting system

Two deployable services bridged by SQS, per `NEW_HARVESTER.md` (v0.4):

- **`orchestrator/`** (`harvest-orchestrator`) — single-threaded planner:
  Census seeding, sweep-target cursor, GPT-5.6-luna query generation at
  dispatch time, SQS dispatch, reconciliation of lost work.
- **`harvester/`** (`harvest-harvester`) — multi-threaded consumer:
  sweep workers (Serper → batched LLM triage → link extraction), code
  workers (Scrapy-Playwright portal discovery), fetch workers
  (download → sniff → dedupe → S3 + text extraction).
- **`core/`** (`harvest-core`) — shared library: domain models, config,
  ports (`typing.Protocol`s), in-memory fakes, and real adapters
  (Postgres/SQLAlchemy, SQS, Redis, S3, Serper, LangChain, httpx).

Postgres is the source of truth; SQS messages and Redis keys are
disposable. The full test suite runs on fakes with a virtual clock —
no live backend, no network, no sleeps.

## Development

```bash
uv sync                  # Python 3.12 workspace, all three packages
uv run pytest            # default: excludes @pytest.mark.live
uv run ruff check .
uv run mypy .
uv run pytest -m live    # optional: real Municode crawl (needs playwright install chromium)
```

## Manual smoke against real backends (plan 4.2)

The smoke step is optional and manual; nothing in the default suite
needs it. It uses Postgres + Redis + LocalStack (SQS + S3) from
`docker-compose.yml`; `ops/localstack-init.sh` creates the three queues
(visibility 300 s / 900 s for code, DLQs at maxReceiveCount 3) and the
bucket automatically.

### Runbook / checklist

Status: not yet executed on a live stack (running Docker requires root
on this workstation); every step below is covered by equivalent
fake-backed tests (`tests/test_scenarios.py`, `test_ops_purge.py`).

1. **Boot the backends**

   ```bash
   docker compose up -d
   export DATABASE_URL=postgresql+psycopg2://harvest:harvest@localhost:5432/harvest
   export REDIS_URL=redis://localhost:6379/0
   export AWS_ENDPOINT_URL=http://localhost:4566
   export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
   export SWEEP_QUEUE_URL=http://localhost:4566/000000000000/harvest-sweep-tasks
   export CODE_QUEUE_URL=http://localhost:4566/000000000000/harvest-code-tasks
   export FETCH_QUEUE_URL=http://localhost:4566/000000000000/harvest-fetch-tasks
   export S3_BUCKET=harvest-documents
   # offline smoke: swap the paid providers for fakes
   export HARVEST_FAKE_SEARCH=1 HARVEST_FAKE_LLM=1 HARVEST_FAKE_PORTAL=1
   ```

2. **Boot migration** (idempotent; also runs automatically at service start)

   ```bash
   uv run harvest-orchestrator migrate
   ```

3. **Flip the run switch and start the orchestrator**

   > **Temporarily disabled:** harvester orchestration is currently gated off
   > in code — without `HARVEST_ORCHESTRATOR_ENABLED=1` the `run` command wires
   > up, logs one warning, and idles without dispatching anything, so the
   > harvester workers below sit on empty queues. The run switch alone is not
   > enough. See the marked block in `orchestrator/.../cli.py` to revert.
   > The public-records pipeline is unaffected.

   ```bash
   uv run harvest-orchestrator switch --config configs/smoke.yaml running
   HARVEST_ORCHESTRATOR_ENABLED=1 \
     uv run harvest-orchestrator run --config configs/smoke.yaml --interval 60
   ```

4. **Start harvester roles** (separate terminals; only `code` needs
   Playwright — `uv run playwright install chromium`)

   ```bash
   uv run harvest-harvester --config configs/smoke.yaml --role sweep --threads 2
   uv run harvest-harvester --config configs/smoke.yaml --role fetch --threads 2
   uv run harvest-harvester --config configs/smoke.yaml --role code  --threads 1
   ```

5. **Watch a document reach `fetched` + S3**

   ```bash
   docker compose exec postgres psql -U harvest -c \
     "select id, status, source_url, path from artifacts where corpus='smoke-corpus';"
   AWS_ENDPOINT_URL=http://localhost:4566 aws s3 ls s3://harvest-documents/smoke-corpus/ --recursive
   ```

6. **Kill-as-purge** (permanent: rows FK-ordered + S3 prefix + Redis prefixes)

   ```bash
   uv run harvest-orchestrator purge --config configs/smoke.yaml --yes
   # verify zero rows / keys / objects remain for the corpus:
   docker compose exec postgres psql -U harvest -c \
     "select count(*) from artifacts where corpus='smoke-corpus';"
   ```

### DLQ alarm note

DLQ depth is observability, not durability — jurisdictions behind
DLQ'd messages recover automatically via the 1800 s / 7200 s dispatch
timeout. In production, alarm on `ApproximateNumberOfMessagesVisible > 0`
for each `harvest-*-dlq` (CloudWatch) and inspect poison messages by
hand; deleting them is always safe.

## Operational commands

```bash
uv run harvest-orchestrator switch --config <cfg> running|stopped  # run switch
uv run harvest-orchestrator import-code-sources --config <cfg> seeds.csv
# CSV columns: jurisdiction,state,url,publisher   (publisher: municode|amlegal|ecode360|other)
uv run harvest-orchestrator purge --config <cfg> --yes             # kill-as-purge
```

---

## The public-records pipeline (`records/`, `pr-records`)

One microservice per `NEW_PUBLIC_RECORDS.md`: an orchestrator (campaign
seeding, SES mail polling, silence-based follow-ups, notification
digests) plus three task consumers (scraper, email sender, email
receiver) bridged by four SQS queues. Postgres is the system of record,
S3 holds raw mail and produced documents, Redis dedupes attachment
bytes per campaign. **Live sends are real legal requests**: a campaign
never produces work until `requester.consent_confirmed` is true, and
`dry_run: true` (the default) skips the Resend call while rehearsing
every database write.

The whole pipeline is tested on the shared in-memory fakes with a
virtual clock (`records/tests/`, including the end-to-end scenarios in
`test_pr_scenarios.py`); no test needs a network or backend.

### Manual smoke runbook (plan 4.3)

Status: not yet executed on a live stack; every step is covered by
equivalent fake-backed tests.

1. **Boot the backends** — same `docker compose up -d`;
   `ops/localstack-init.sh` also creates the four `pr-*` queues
   (visibility 900 s; DLQ maxReceiveCount 3/5/5/3) and the
   `pr-mail` / `pr-documents` buckets.

   ```bash
   export DATABASE_URL=postgresql+psycopg2://harvest:harvest@localhost:5432/harvest
   export REDIS_URL=redis://localhost:6379/0
   export AWS_ENDPOINT_URL=http://localhost:4566
   export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
   export PR_MAIL_BUCKET=pr-mail PR_DOCUMENTS_BUCKET=pr-documents
   base=http://localhost:4566/000000000000
   export PR_SEARCH_QUEUE_URL=$base/pr-search-queries
   export PR_CONTACTS_QUEUE_URL=$base/pr-contacts
   export PR_FOLLOWUPS_QUEUE_URL=$base/pr-followups
   export PR_INBOUND_QUEUE_URL=$base/pr-inbound-mail
   export PR_SEARCH_DLQ_URL=$base/pr-search-queries-dlq
   export PR_CONTACTS_DLQ_URL=$base/pr-contacts-dlq
   export PR_FOLLOWUPS_DLQ_URL=$base/pr-followups-dlq
   export PR_INBOUND_DLQ_URL=$base/pr-inbound-mail-dlq
   export PR_FROM_ADDRESS=requests@your-verified-domain.example
   export SERPER_API_KEY=... RESEND_API_KEY=... OPENAI_API_KEY=...
   ```

2. **Register a one-county dry-run campaign** (a YAML matching §11 with
   a seeded `contact_email` on the jurisdiction skips Serper entirely):

   ```bash
   uv run pr-records migrate
   uv run pr-records register configs/pr-smoke.yaml   # dry_run: true
   uv run pr-records start <campaign-name>
   uv run pr-records run
   ```

3. **Watch the dry-run payload in the logs**, then drop a fixture MIME
   reply (with the thread's `[DLG-…]` subject token) into the mail
   bucket and watch the poller classify it and store any attachment
   under `<campaign-slug>/<jurisdiction-slug>/<digest8>_<filename>`.

4. **Kill-as-purge** and verify zero campaign rows, zero
   `dedupe:<slug>:*` Redis keys, and zero objects under
   `<campaign-slug>/` — while `jurisdictions` rows and the raw MIME in
   the mail bucket survive:

   ```bash
   uv run pr-records kill <campaign-name>
   ```

DLQ note: any message that dead-letters raises exactly one `other`
escalation naming the queue and payload (the in-process DLQ watcher);
alarm on DLQ depth in production all the same.

### Running it under a supervisor (systemd, ECS/Fargate, …)

- **One instance only.** `followup_scan` has no cross-process lock, so a
  second scheduler double-sends follow-ups. Scale with threads inside
  the process; on ECS that means `desiredCount: 1` and a deployment
  policy that stops the old task before starting the new one
  (`maximumPercent: 100`).
- **Allow at least 60 s to stop.** `SIGTERM` stops the orchestrator and
  gives the consumers `SHUTDOWN_DRAIN_S` (25 s) to finish the handlers
  they are already inside, because an initial request is mailed a moment
  before its `emails` row is written — killing that window mails the
  office twice. Anything still running at the deadline is abandoned and
  redelivers. Set ECS `stopTimeout: 60`; the default 30 s is too tight.
- **Database URL.** `postgresql+psycopg2://…` works out of the box
  (`psycopg2-binary` is a `harvest-core` dependency); append
  `?sslmode=require` for RDS. Every service boot-migrates under a
  Postgres advisory lock, so concurrent starts are safe — but
  `create_all` only ever *adds* tables, so a schema change to an
  existing table is manual DDL.

---

## Container images (`ops/Dockerfile.*`)

```bash
docker build -f ops/Dockerfile.records      -t pr-records .           # context = repo root
docker build -f ops/Dockerfile.orchestrator -t harvest-orchestrator .
```

The context is the **repo root** either way, because `uv.lock` pins the
whole workspace. Each image still carries only the two packages its
service is built from — the dependency layer is installed from
`uv export --package <name>` (53 requirements) rather than
`uv sync`, which would resolve the workspace's full 84 and drag Scrapy,
scrapy-playwright and pypdf into both images. Neither image can import
the other service.

Both run as a non-root user, ship a venv at `/opt/venv` (built at the
same path, since console scripts hardcode their interpreter), and use
`ENTRYPOINT`'s exec form so `SIGTERM` reaches Python as PID 1 — which is
what the drain above depends on. Neither declares a `HEALTHCHECK`: these
are queue workers with no HTTP listener, so liveness is the task staying
up, and the real signals are queue and DLQ depth.

The one-off commands are the same image with a different `command`:
`["migrate"]`, `["register", "/app/configs/<campaign>.yaml"]`. Run
`migrate` to completion before rolling the services.

Status: not yet built (Docker needs root on this workstation). What is
verified is the builder stage — its `COPY`/`RUN` instructions executed
locally against the real workspace, producing the expected 56-package
environment and a working entry point for each service.

## Deploying (`ops/terraform/`)

A Terraform module brings up both services on ECS/Fargate with RDS,
ElastiCache, the seven queues and their DLQs, three buckets, Secrets
Manager slots, IAM, and DLQ alarms. It assumes an existing VPC with NAT
egress and creates nothing public.

**`ops/terraform/README.md` is the runbook** — apply order, where the
API keys go, how to run the one-off `migrate` / `register` / `start`
tasks, and why `pr-records` is pinned to a single instance. Read it
before applying; the steps are ordered for a reason.
