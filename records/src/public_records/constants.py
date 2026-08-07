"""Every hardcoded number and list in NEW_PUBLIC_RECORDS.md, in one place."""

from __future__ import annotations

# -- queues (§2.1)
QUEUE_SEARCH = "pr-search-queries"
QUEUE_CONTACTS = "pr-contacts"
QUEUE_FOLLOWUPS = "pr-followups"
QUEUE_INBOUND = "pr-inbound-mail"

VISIBILITY_TIMEOUT_S = 900
MAX_RECEIVE_INBOUND = 3
MAX_RECEIVE_SENDER = 5
MAX_RECEIVE_SEARCH = 3
BATCH_RECEIVE = 10

# -- models (§2.2)
MODEL_LUNA = "gpt-5.6-luna"
MODEL_TERRA = "gpt-5.6-terra"
LLM_TIMEOUT_S = 120
LLM_RETRIES = 2

# -- orchestrator (§5)
ORCHESTRATOR_PERIOD_S = 300
QUERIES_PER_TARGET_MAX = 3
FALLBACK_QUERY_PATTERN = "{name} {state} public records request CPRA email clerk"
MAIL_POLL_CAP = 200
SES_SETUP_MARKER = "AMAZON_SES_SETUP_NOTIFICATION"
FOLLOWUP_SCAN_BATCH = 50

# -- scraper (§6)
SEARCH_PAGE_SIZE = 8
SCRAPER_PAGE_BUDGET = 4
SCRAPER_HTTP_TIMEOUT_S = 12
SCRAPER_PAGE_MAX_BYTES = 1_000_000
MAX_EMAIL_CANDIDATES = 12
CONTEXT_BEFORE_CHARS = 120
CONTEXT_AFTER_CHARS = 60
PICK_CONTEXT_CHARS = 200

JUNK_LOCAL_PARTS = frozenset(
    {"noreply", "no-reply", "donotreply", "mailer-daemon", "postmaster", "abuse", "webmaster"}
)
GENERIC_LOCAL_KEYWORDS = (
    "record",
    "clerk",
    "foia",
    "cpra",
    "pra",
    "prr",
    "public",
    "request",
    "sunshine",
    "info",
    "contact",
    "cityhall",
)

# -- thread token (§4)
TOKEN_BYTES = 8  # 16 hex chars
TOKEN_HEADER = "X-Dialogue-Token"
SUBJECT_TOKEN_PREFIX = "DLG"  # legacy "RF" accepted inbound only

# -- sender (§7)
ANON_BLOCKED_STATES = frozenset({"AL", "AR", "TN", "VA", "DE", "NJ", "KY"})
RESEND_TIMEOUT_S = 30

# -- receiver / storage (§8, §3.3)
CLASSIFY_BODY_CHARS = 6_000
CLARIFICATION_CONTEXT_CHARS = 4_000
ESCALATION_DETAIL_CHARS = 2_000
FEE_NOTE_CHARS = 500
PRESIGNED_URL_TTL_S = 120
SLUG_LEN = 60
DIGEST_PREFIX_LEN = 8

# Stored document/data types (§8.1): extension -> canonical content type.
ACCEPTED_ATTACHMENT_EXTS: dict[str, str] = {
    ".pdf": "application/pdf",
    ".doc": "application/msword",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xls": "application/vnd.ms-excel",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".csv": "text/csv",
    ".xml": "application/xml",
    ".json": "application/json",
    ".txt": "text/plain",
    ".zip": "application/zip",
}
ACCEPTED_ATTACHMENT_CONTENT_TYPES = frozenset(
    {
        *ACCEPTED_ATTACHMENT_EXTS.values(),
        "text/xml",
        "application/x-zip-compressed",
        "application/csv",
    }
)

# -- fee flow (§9)
FEE_AMOUNT_MAX_DIGITS = 6

# -- operational surface (§12)
DIGEST_MAX_LINE_ITEMS = 25
DIGEST_TOKEN = "notify"
DIGEST_KIND_HEADER = "X-Dialogue-Kind"
