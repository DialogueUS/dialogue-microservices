"""LangChain adapters for the two model roles (§2.2).

- GPT 5.6-luna (`LunaContacts`): search-query generation + contact pick.
- GPT 5.6-terra (`TerraCorrespondence`): drafting + inbound
  classification (classification runs with reasoning enabled).

Structured output throughout. API failures raise the typed transient
errors; classification parse failures / unknown categories degrade to
`unclear` (§8) instead of raising.
"""

from __future__ import annotations

import os
from typing import Any

from pydantic import BaseModel, Field

from ..constants import CLASSIFY_BODY_CHARS, LLM_RETRIES, LLM_TIMEOUT_S, MODEL_LUNA, MODEL_TERRA
from ..domain import Campaign, Classification, InboundCategory, Jurisdiction
from ..errors import ClassifyError, DraftError, GenerationError, PickError
from ..ports import ContactPick, EmailCandidate


class QueriesOutput(BaseModel):
    queries: list[str] = Field(default_factory=list)


class PickOutput(BaseModel):
    email: str | None = None
    confidence: float = 0.0


class ClassificationOutput(BaseModel):
    category: str = "unclear"
    summary: str = ""
    confidence: float = 0.0
    referral_email: str | None = None


QUERY_PROMPT = """\
You write Google search queries that find a US government office's
public-records / FOIA / CPRA request contact email address.

Office: {name}, {state} (level: {level})
Records sought: {record_type}

Return JSON {{"queries": ["...", ...]}} — each query aimed at finding
the office's records-request email address.
"""

PICK_PROMPT = """\
You are choosing the best email address for sending a formal public
records request to: {name}, {state} (level: {level}).

Candidates (with where each was found and surrounding page text):
{candidates}

Return JSON {{"email": <one of the listed addresses, or null>,
"confidence": <0..1>}}.
Rules:
- Prefer clerk / records / foia / cpra / pra addresses on the office's
  own domain.
- NEVER invent an address that is not in the list.
- When in doubt, return null.
"""

DRAFT_INITIAL_PROMPT = """\
Draft the body of a formal public records request email.

Jurisdiction: {jurisdiction}, {state}
Legal basis: {legal_basis}
Records requested (do NOT restate this in the body — an "Exact records
requested" block is appended mechanically): {record_description}
Requester: {requester}

Hard rules:
- Begin with the salutation. No subject line, no sign-off, no signature.
- Do not restate the records scope.
- Never fabricate legal citations beyond the given legal basis.
- Never promise payment.
- Ask for a fee estimate before costs are incurred.
- Ask for exemption citations and segregable portions if partially denied.
{anonymous_clause}
"""

DRAFT_FOLLOWUP_PROMPT = """\
Draft a 2-3 sentence, courteous, never-threatening status nudge for a
public records request. Reference the original subject "{subject}" and
the approximate wait of {waited_days} days. Body text only: begin with
the salutation, no subject line, no sign-off.
"""

DRAFT_CLARIFICATION_PROMPT = """\
An office asked a clarifying question about a public records request.
Draft a reply body (salutation first, no subject, no sign-off).

Their message (truncated):
{inbound}

The records requested: {record_description}

Hard rules:
- Answer ONLY from the record description above. If the question cannot
  be answered from it, politely restate the request and offer to narrow
  its scope.
- Never agree to fees. Never invent requester facts.
"""

DRAFT_FEE_PROMPT = """\
An office quoted a fee for a public records request and we are agreeing
to it. Draft a reply body (salutation first, no subject, no sign-off).

Their message (truncated):
{inbound}

Hard rules:
- Confirm the exact amount ${amount} and nothing else.
- Ask for accepted payment methods and an invoice / remittance address,
  unless their message already stated one.
- Never include payment details or card numbers.
"""

CLASSIFY_PROMPT = """\
Classify this reply to a formal public records request. Return JSON
{{"category": ..., "summary": ..., "confidence": ..., "referral_email": ...}}.

Categories (exactly one): data_provided, payment_required,
needs_clarification, denial, referral, acknowledgment, unclear.

Rulings:
- payment_required ONLY when payment is necessary to proceed;
  boilerplate "fees may apply" in an auto-reply is not.
- referral should carry "referral_email" when one is given.
- A request for information we don't have or shouldn't give is unclear
  (escalate rather than improvise).

Subject: {subject}
Body (truncated):
{body}
"""


def _requester_block(campaign: Campaign) -> tuple[str, str]:
    requester = campaign.config.requester
    if requester.anonymous:
        return (
            "anonymous",
            "- The requester is anonymous: include no name; state a preference "
            "for electronic delivery to this email address.",
        )
    parts = [requester.name]
    if requester.organization:
        parts.append(requester.organization)
    return (", ".join(parts), "")


class LunaContacts:
    """ContactQueryGenerator + ContactPicker over GPT 5.6-luna."""

    def __init__(self, model: Any) -> None:
        self._model = model

    @classmethod
    def from_env(cls, api_key_env: str = "OPENAI_API_KEY") -> LunaContacts:
        from langchain_openai import ChatOpenAI
        from pydantic import SecretStr

        return cls(
            ChatOpenAI(
                model=MODEL_LUNA,
                api_key=SecretStr(os.environ[api_key_env]),
                timeout=LLM_TIMEOUT_S,
                max_retries=LLM_RETRIES,
            )
        )

    def generate_queries(self, jurisdiction: Jurisdiction, record_type: str) -> list[str]:
        prompt = QUERY_PROMPT.format(
            name=jurisdiction.name,
            state=jurisdiction.state,
            level=jurisdiction.level,
            record_type=record_type,
        )
        try:
            output = self._model.with_structured_output(QueriesOutput).invoke(prompt)
        except Exception as exc:
            raise GenerationError(str(exc)) from exc
        if not isinstance(output, QueriesOutput) or not output.queries:
            raise GenerationError(f"unparseable query output: {output!r}")
        return [q.strip() for q in output.queries if q.strip()]

    def pick(self, jurisdiction: Jurisdiction, candidates: list[EmailCandidate]) -> ContactPick:
        listed = "\n".join(
            f"- {c.email} (from {c.source_url}): {c.context}" for c in candidates
        )
        prompt = PICK_PROMPT.format(
            name=jurisdiction.name,
            state=jurisdiction.state,
            level=jurisdiction.level,
            candidates=listed,
        )
        try:
            output = self._model.with_structured_output(PickOutput).invoke(prompt)
        except Exception as exc:
            raise PickError(str(exc)) from exc
        if not isinstance(output, PickOutput):
            return ContactPick(email=None)  # parse failure = null pick, not an error
        return ContactPick(email=output.email, confidence=output.confidence)


class TerraCorrespondence:
    """EmailDrafter + InboundClassifier over GPT 5.6-terra."""

    def __init__(self, model: Any, reasoning_model: Any | None = None) -> None:
        self._model = model
        self._reasoning_model = reasoning_model or model

    @classmethod
    def from_env(cls, api_key_env: str = "OPENAI_API_KEY") -> TerraCorrespondence:
        from langchain_openai import ChatOpenAI
        from pydantic import SecretStr

        key = SecretStr(os.environ[api_key_env])
        drafting = ChatOpenAI(
            model=MODEL_TERRA, api_key=key, timeout=LLM_TIMEOUT_S, max_retries=LLM_RETRIES
        )
        # classification runs with reasoning enabled (§2.2)
        classifying = ChatOpenAI(
            model=MODEL_TERRA,
            api_key=key,
            timeout=LLM_TIMEOUT_S,
            max_retries=LLM_RETRIES,
            reasoning={"effort": "medium"},
        )
        return cls(drafting, classifying)

    def _draft(self, prompt: str) -> str:
        try:
            result = self._model.invoke(prompt)
        except Exception as exc:
            raise DraftError(str(exc)) from exc
        text = getattr(result, "content", result)
        if not isinstance(text, str) or not text.strip():
            raise DraftError(f"empty draft: {result!r}")
        return text.strip()

    def draft_initial(self, campaign: Campaign, jurisdiction: Jurisdiction) -> str:
        requester, anonymous_clause = _requester_block(campaign)
        return self._draft(
            DRAFT_INITIAL_PROMPT.format(
                jurisdiction=jurisdiction.name,
                state=jurisdiction.state,
                legal_basis=campaign.config.legal_basis,
                record_description=campaign.config.record_description,
                requester=requester,
                anonymous_clause=anonymous_clause,
            )
        )

    def draft_followup(self, campaign: Campaign, original_subject: str, waited_days: int) -> str:
        return self._draft(
            DRAFT_FOLLOWUP_PROMPT.format(subject=original_subject, waited_days=waited_days)
        )

    def draft_clarification(self, campaign: Campaign, inbound_body: str) -> str:
        return self._draft(
            DRAFT_CLARIFICATION_PROMPT.format(
                inbound=inbound_body,
                record_description=campaign.config.record_description,
            )
        )

    def draft_fee_agreement(self, campaign: Campaign, amount_cents: int, inbound_body: str) -> str:
        return self._draft(
            DRAFT_FEE_PROMPT.format(amount=f"{amount_cents / 100:.2f}", inbound=inbound_body)
        )

    def classify(self, subject: str, body: str) -> Classification:
        prompt = CLASSIFY_PROMPT.format(subject=subject, body=body[:CLASSIFY_BODY_CHARS])
        try:
            output = self._reasoning_model.with_structured_output(ClassificationOutput).invoke(
                prompt
            )
        except Exception as exc:
            raise ClassifyError(str(exc)) from exc
        if not isinstance(output, ClassificationOutput):
            return Classification(
                category=InboundCategory.UNCLEAR, summary="unparseable output", confidence=0.0
            )
        try:
            category = InboundCategory(output.category.upper())
        except ValueError:
            category = InboundCategory.UNCLEAR
        return Classification(
            category=category,
            summary=output.summary,
            confidence=output.confidence,
            referral_email=output.referral_email,
        )
