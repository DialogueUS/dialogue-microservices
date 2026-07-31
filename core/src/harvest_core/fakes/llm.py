"""FakeLLM: canned query generations and triage verdicts, programmable failure.

One fake implements both LLM roles (QueryGenerator and Triage) — the
real system uses one model for both.
"""

from __future__ import annotations

import threading
from collections.abc import Callable

from ..domain import Jurisdiction
from ..errors import GenerationError, TriageError
from ..ports import TriageRequest, TriageVerdict


class FakeLLM:
    def __init__(self) -> None:
        # jurisdiction name -> topic -> queries; unset jurisdictions get defaults
        self.generations: dict[str, dict[str, list[str]]] = {}
        self.fail_generation_for: set[str] = set()
        self.fail_triage = False
        self.fail_triage_once = False
        # verdict function applied per result when no canned verdicts exist
        self.triage_rule: Callable[[TriageRequest, int], TriageVerdict] | None = None
        self.generate_calls: list[tuple[str, tuple[str, ...], int]] = []
        self.triage_calls: list[list[TriageRequest]] = []
        self._lock = threading.Lock()

    def generate(
        self, jurisdiction: Jurisdiction, topics: list[str], queries_per_topic: int
    ) -> dict[str, list[str]]:
        with self._lock:
            self.generate_calls.append((jurisdiction.name, tuple(topics), queries_per_topic))
            if jurisdiction.name in self.fail_generation_for:
                raise GenerationError(f"canned generation failure for {jurisdiction.name}")
            canned = self.generations.get(jurisdiction.name)
            if canned is not None:
                return {t: list(qs) for t, qs in canned.items() if t in topics}
            return {
                t: [
                    f"{jurisdiction.name} {jurisdiction.state} {t} query {i + 1}"
                    for i in range(queries_per_topic)
                ]
                for t in topics
            }

    def triage(self, requests: list[TriageRequest]) -> list[list[TriageVerdict]]:
        with self._lock:
            self.triage_calls.append(requests)
            if self.fail_triage or self.fail_triage_once:
                self.fail_triage_once = False
                raise TriageError("canned triage failure")
            out: list[list[TriageVerdict]] = []
            for req in requests:
                verdicts = []
                for i, result in enumerate(req.results):
                    if self.triage_rule is not None:
                        verdicts.append(self.triage_rule(req, i))
                    else:
                        looks_doc = result.url.lower().split("?")[0].endswith(
                            (".pdf", ".docx", ".doc", ".odt", ".rtf", ".xlsx", ".xls")
                        )
                        verdicts.append(
                            TriageVerdict(relevant=True, is_document=looks_doc, confidence=0.9)
                        )
                out.append(verdicts)
            return out
