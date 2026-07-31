"""Hardcoded constants of the harvesting system (spec §9, "Hardcoded")."""

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
