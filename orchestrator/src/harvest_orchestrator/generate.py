"""Dispatch-time query generation (plan 2.3, spec §5.2).

One LLM call per jurisdiction covering all topics; the output is
clamped, never trusted: at most queries_per_jurisdiction per topic,
each truncated to 200 chars, empties dropped. GenerationError
propagates — the caller skips the jurisdiction, leaving the row due.
"""

from __future__ import annotations

from harvest_core.config import HarvestConfig
from harvest_core.constants import QUERY_MAX_CHARS
from harvest_core.domain import Jurisdiction
from harvest_core.errors import GenerationError
from harvest_core.ports import QueryGenerator


def generate_queries(
    generator: QueryGenerator, jurisdiction: Jurisdiction, config: HarvestConfig
) -> list[tuple[str, str]]:
    """Returns ordered (topic, query_text) pairs. Raises GenerationError."""
    raw = generator.generate(
        jurisdiction, list(config.topics), config.queries_per_jurisdiction
    )
    pairs: list[tuple[str, str]] = []
    for topic in config.topics:
        queries = [q.strip()[:QUERY_MAX_CHARS] for q in raw.get(topic, [])]
        queries = [q for q in queries if q]
        if not queries:
            raise GenerationError(
                f"no usable queries for topic {topic!r} of {jurisdiction.name}"
            )
        for query in queries[: config.queries_per_jurisdiction]:
            pairs.append((topic, query))
    return pairs
