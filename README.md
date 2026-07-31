# dialogue-microservices — the harvesting system

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

   ```bash
   uv run harvest-orchestrator switch --config configs/smoke.yaml running
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
