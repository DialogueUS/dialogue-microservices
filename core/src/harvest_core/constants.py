"""Hardcoded constants of the harvesting system (spec §9, "Hardcoded").

A few entries are shared with the public-records pipeline, which reuses
this library's generic ports; those are marked where they appear.
"""

from __future__ import annotations

# Scheduling / recovery clocks (seconds unless noted)
SEED_THROTTLE_S = 600
VISIBILITY_TIMEOUT_S = 300
CODE_VISIBILITY_TIMEOUT_S = 900
CODE_HEARTBEAT_S = 300
CODE_DEADLINE_S = 3600
DISPATCH_TIMEOUT_S = 1800
CODE_DISPATCH_TIMEOUT_S = 7200
FANIN_COUNTER_TTL_S = 1800
MAX_RECEIVE_COUNT = 3
ERROR_RETRY_DAYS = 1
LONG_POLL_WAIT_S = 20

# Health check over Redis pub/sub (shared: both deployables answer on it).
# The probe runs as a separate process in the same container, so these are
# a wall-clock budget the container's healthCheck timeout must exceed.
HEALTH_CHANNEL_PREFIX = "health"
HEALTH_PROBE_TIMEOUT_S = 5.0
# How long the responder blocks on one poll. Also the worst case for the
# responder thread noticing a shutdown, so keep it short.
HEALTH_RESPONDER_POLL_S = 1.0
# A loop is stale once it has missed this many of its own intervals. Three
# rather than one so a single slow cycle is not a restart.
HEALTH_STALE_CYCLES = 3
# Floor under the above, so a short interval does not make the check flap.
HEALTH_MIN_STALE_S = 60.0

# Database (managed Postgres/RDS: failover and idle timeouts close pooled
# connections without telling the client, so pre-ping before handing one out
# and recycle well before the server's own idle limit).
DB_POOL_RECYCLE_S = 1800
# One lock id for every boot migration in the database: concurrent task
# starts serialize instead of racing create_all's check-then-create.
DB_MIGRATION_LOCK_ID = 0x48415256  # "HARV"

# Batching
SWEEP_BATCH_RECEIVE = 10

# Politeness
PER_HOST_FETCH_SPACING_S = 1.0
CODE_RENDER_CONCURRENCY = 2
CODE_RENDER_DELAY_S = 1.0

# Byte / char caps
PAGE_BYTE_CAP = 400_000
DOCUMENT_BYTE_CAP = 20_000_000
PDF_MAX_PAGES = 40
EXTRACTED_TEXT_CHARS = 20_000
QUERY_MAX_CHARS = 200
DETAIL_CHARS = 500
LAST_ERROR_CHARS = 2_000
SOURCE_URL_CHARS = 600
FILENAME_CHARS = 400
TITLE_CHARS = 300

# Fetch failure handling
FETCH_ATTEMPT_LIMIT = 3
DEAD_LINK_STATUSES = frozenset({400, 401, 403, 404, 410})

# Storage
STORAGE_SLUG_LEN = 60
STORAGE_HASH_PREFIX_LEN = 8
PRESIGNED_URL_TTL_S = 120

# Census seeding (shared: both systems expand a scope of ["ALL"] with this)
ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA",
    "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX",
    "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]  # 50 states + DC

# Level priorities (dispatch order: federal -> state -> county -> city)
LEVEL_PRIORITIES: dict[str, int] = {"federal": 0, "state": 1, "county": 2, "city": 3}
DEFAULT_PRIORITY = 9

# Two distinct extension sets (old spec §6.3) — collapsing them is a known mistake.
# Order within STORED_EXTENSIONS is meaningful: earlier entries rank first (PDF floats).
STORED_EXTENSIONS: tuple[str, ...] = (
    ".pdf",
    ".docx",
    ".doc",
    ".odt",
    ".rtf",
    ".xlsx",
    ".xls",
    ".xml",
    ".json",
)
# Links followable off an HTML page: stored set minus .xml/.json (feeds and API
# endpoints are not records; structured content enters only via source adapters).
FOLLOWABLE_EXTENSIONS: tuple[str, ...] = tuple(
    e for e in STORED_EXTENSIONS if e not in (".xml", ".json")
)
