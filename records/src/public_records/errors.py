"""Typed errors shared by fakes and real adapters.

Infra errors (UniqueViolation, RateLimited, ObjectNotFound, FetchError,
UnreachableHost, GenerationError) are harvest_core's — the same types
the shared fakes raise. This module adds the public-records-specific
failure surface.
"""

from __future__ import annotations

from harvest_core.errors import (
    FetchError,
    GenerationError,
    ObjectNotFound,
    RateLimited,
    UniqueViolation,
    UnreachableHost,
)

__all__ = [
    "FetchError",
    "GenerationError",
    "ObjectNotFound",
    "RateLimited",
    "UniqueViolation",
    "UnreachableHost",
    "SendTransientError",
    "PickError",
    "DraftError",
    "ClassifyError",
    "IllegalTransition",
]


class SendTransientError(Exception):
    """The Resend API call failed (timeout, 5xx, 429). Message retried."""


class PickError(Exception):
    """Contact-pick API failure. Transient: the search message is retried."""


class DraftError(Exception):
    """Email drafting failed (API or parse). Transient: the job is retried."""


class ClassifyError(Exception):
    """Classification API failure. Transient — parse failures are NOT this;
    they degrade to the `unclear` category instead (spec §8)."""


class IllegalTransition(Exception):
    """A thread status transition the §4 state machine forbids."""
