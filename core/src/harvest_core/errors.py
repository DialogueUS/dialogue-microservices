"""Typed errors shared by fakes and real adapters.

Fakes raise exactly these types so service code tested against fakes
handles the same failure surface the real adapters present.
"""

from __future__ import annotations


class UniqueViolation(Exception):
    """A unique constraint rejected an insert (Postgres or fake)."""


class RateLimited(Exception):
    """The search provider answered HTTP 429. Never an error (spec §8)."""


class GenerationError(Exception):
    """Query generation failed (API error or parse failure). Transient."""


class TriageError(Exception):
    """Relevance triage failed (API error or parse failure). Transient."""


class UnreachableHost(Exception):
    """DNS failure / connection refused / TLS failure — a dead link."""


class FetchError(Exception):
    """A non-dead-link network error during fetch (timeout, 5xx transport...)."""


class TransientPortalError(Exception):
    """Portal unreachable / render never completed / anti-bot block.

    The code task is left undeleted so SQS redelivers it (spec §6.3).
    """


class PortalError(Exception):
    """The portal crawl errored with zero candidates (spec §6.3)."""


class IllegalTransition(Exception):
    """An artifact state transition the state machine forbids."""


class ObjectNotFound(Exception):
    """The object store has no object at the requested key."""
