"""LangChain GPT-5.6-luna adapter: query generation and batched triage.

Structured output throughout; a parse failure becomes a typed error
(GenerationError / TriageError) — never an exception escaping to the
caller as anything else.
"""

from __future__ import annotations

import os
from typing import Any

from pydantic import BaseModel, Field

from ..constants import QUERY_MAX_CHARS
from ..domain import Jurisdiction
from ..errors import GenerationError, TriageError
from ..ports import TriageRequest, TriageVerdict


class TopicQueries(BaseModel):
    topic: str
    queries: list[str] = Field(default_factory=list)


class QueryGenerationOutput(BaseModel):
    topics: list[TopicQueries] = Field(default_factory=list)


class ResultVerdict(BaseModel):
    index: int
    relevant: bool
    is_document: bool = False
    confidence: float = 0.0


class TriageOutput(BaseModel):
    verdicts: list[ResultVerdict] = Field(default_factory=list)


GENERATION_PROMPT = """\
You write Google search queries that surface a US jurisdiction's own
published regulatory documents (ordinances, codes, rules), preferring
official domains and document filetypes.

Jurisdiction: {name}, {state} (level: {level})

For each topic below, write between 1 and {n} Google queries, each at
most {max_chars} characters.

Topics:
{topics}
"""

TRIAGE_PROMPT = """\
You judge web search results by metadata only — rank, title, snippet,
URL. Nothing has been fetched. For each numbered result decide:
- relevant: is this plausibly a regulatory document, or a page on the
  jurisdiction's own site that would link to one?
- is_document: does the URL itself look like a document file?
- confidence: 0 to 1.

Return a verdict for every result index listed.

{blocks}
"""


class LangChainLLM:
    """Implements both QueryGenerator and Triage over one chat model."""

    def __init__(self, model: Any) -> None:
        self._model = model

    @classmethod
    def from_config(cls, model_name: str, api_key_env: str) -> LangChainLLM:
        from langchain_openai import ChatOpenAI
        from pydantic import SecretStr

        return cls(ChatOpenAI(model=model_name, api_key=SecretStr(os.environ[api_key_env])))

    def generate(
        self, jurisdiction: Jurisdiction, topics: list[str], queries_per_topic: int
    ) -> dict[str, list[str]]:
        prompt = GENERATION_PROMPT.format(
            name=jurisdiction.name,
            state=jurisdiction.state,
            level=jurisdiction.level,
            n=queries_per_topic,
            max_chars=QUERY_MAX_CHARS,
            topics="\n".join(f"- {t}" for t in topics),
        )
        try:
            structured = self._model.with_structured_output(QueryGenerationOutput)
            output = structured.invoke(prompt)
        except Exception as exc:
            raise GenerationError(str(exc)) from exc
        if not isinstance(output, QueryGenerationOutput):
            raise GenerationError(f"unparseable generation output: {output!r}")
        by_topic = {tq.topic: list(tq.queries) for tq in output.topics}
        missing = [t for t in topics if not by_topic.get(t)]
        if missing:
            raise GenerationError(f"generation returned no queries for topics {missing}")
        return {t: by_topic[t] for t in topics}

    def triage(self, requests: list[TriageRequest]) -> list[list[TriageVerdict]]:
        blocks: list[str] = []
        index = 0
        counts: list[int] = []
        for req in requests:
            lines = [
                f"Jurisdiction: {req.jurisdiction_name}, {req.state} "
                f"(level: {req.level}); topic: {req.topic}"
            ]
            for r in req.results:
                lines.append(
                    f"[{index}] rank={r.rank} title={r.title!r} "
                    f"snippet={r.snippet!r} url={r.url}"
                )
                index += 1
            counts.append(len(req.results))
            blocks.append("\n".join(lines))
        try:
            structured = self._model.with_structured_output(TriageOutput)
            output = structured.invoke(TRIAGE_PROMPT.format(blocks="\n\n".join(blocks)))
        except Exception as exc:
            raise TriageError(str(exc)) from exc
        if not isinstance(output, TriageOutput):
            raise TriageError(f"unparseable triage output: {output!r}")
        by_index = {v.index: v for v in output.verdicts}
        if set(by_index) != set(range(index)):
            raise TriageError(
                f"triage returned indices {sorted(by_index)}; expected 0..{index - 1}"
            )
        out: list[list[TriageVerdict]] = []
        cursor = 0
        for count in counts:
            out.append(
                [
                    TriageVerdict(
                        relevant=by_index[i].relevant,
                        is_document=by_index[i].is_document,
                        confidence=by_index[i].confidence,
                    )
                    for i in range(cursor, cursor + count)
                ]
            )
            cursor += count
        return out
