"""Resend + LLM adapters with stubbed transports (no network)."""

from typing import Any

import httpx
import pytest
import respx
from public_records.adapters.llm import (
    ClassificationOutput,
    LunaContacts,
    PickOutput,
    QueriesOutput,
    TerraCorrespondence,
)
from public_records.adapters.resend import RESEND_API_URL, ResendTransport
from public_records.config import CampaignConfig
from public_records.domain import Campaign, InboundCategory, Jurisdiction
from public_records.errors import (
    ClassifyError,
    DraftError,
    GenerationError,
    PickError,
    SendTransientError,
)
from public_records.ports import EmailCandidate, OutboundEmail

MINIMAL: dict[str, Any] = {
    "name": "noise-2026",
    "record_type": "noise complaints",
    "record_description": "All noise complaints filed in 2025.",
    "requester": {"name": "Ada Requester", "email": "ada@example.org"},
}

JUR = Jurisdiction(id=1, name="Pasadena", state="CA", level="city")
CAMPAIGN = Campaign(id=1, config=CampaignConfig.model_validate(MINIMAL))

EMAIL = OutboundEmail(
    from_address="requests@dialogue.org",
    to_address="records@pasadena.gov",
    subject="Public Records Request [DLG-abababababababab]",
    body="Dear Records Officer,",
    headers={"X-Dialogue-Token": "abababababababab"},
)


@respx.mock
def test_resend_payload_shape_and_id() -> None:
    route = respx.post(RESEND_API_URL).mock(
        return_value=httpx.Response(200, json={"id": "re_123"})
    )
    transport = ResendTransport("key", client=httpx.Client())
    assert transport.send(EMAIL) == "re_123"
    request = route.calls.last.request
    assert request.headers["Authorization"] == "Bearer key"
    import json

    payload = json.loads(request.content)
    assert payload["from"] == EMAIL.from_address
    assert payload["to"] == [EMAIL.to_address]
    assert payload["subject"] == EMAIL.subject
    assert payload["text"] == EMAIL.body
    assert payload["headers"]["X-Dialogue-Token"] == "abababababababab"


@respx.mock
@pytest.mark.parametrize("status", [429, 500, 503, 422])
def test_resend_failures_are_typed_transient(status: int) -> None:
    respx.post(RESEND_API_URL).mock(return_value=httpx.Response(status, text="nope"))
    transport = ResendTransport("key", client=httpx.Client())
    with pytest.raises(SendTransientError):
        transport.send(EMAIL)


@respx.mock
def test_resend_network_error_is_transient() -> None:
    respx.post(RESEND_API_URL).mock(side_effect=httpx.ConnectTimeout("t"))
    transport = ResendTransport("key", client=httpx.Client())
    with pytest.raises(SendTransientError):
        transport.send(EMAIL)


class _StubModel:
    """Stands in for ChatOpenAI: returns canned structured/plain outputs."""

    def __init__(self, structured: Any = None, content: Any = None,
                 raise_exc: Exception | None = None) -> None:
        self._structured = structured
        self._content = content
        self._raise = raise_exc
        self.prompts: list[str] = []

    def with_structured_output(self, schema: Any) -> "_StubModel":
        return self

    def invoke(self, prompt: str) -> Any:
        self.prompts.append(prompt)
        if self._raise is not None:
            raise self._raise
        if self._structured is not None:
            return self._structured

        class _Msg:
            content = self._content

        return _Msg()


def test_luna_generate_queries() -> None:
    stub = _StubModel(structured=QueriesOutput(queries=[" q1 ", "q2", ""]))
    luna = LunaContacts(stub)
    assert luna.generate_queries(JUR, "noise complaints") == ["q1", "q2"]
    assert "Pasadena" in stub.prompts[0] and "noise complaints" in stub.prompts[0]

    with pytest.raises(GenerationError):
        LunaContacts(_StubModel(raise_exc=RuntimeError("api down"))).generate_queries(
            JUR, "x"
        )
    with pytest.raises(GenerationError):
        LunaContacts(_StubModel(structured=QueriesOutput(queries=[]))).generate_queries(
            JUR, "x"
        )


def test_luna_pick_and_null_on_parse_failure() -> None:
    candidates = [EmailCandidate(email="records@pasadena.gov", context="ctx", source_url="u")]
    luna = LunaContacts(_StubModel(structured=PickOutput(email="records@pasadena.gov",
                                                         confidence=0.9)))
    pick = luna.pick(JUR, candidates)
    assert pick.email == "records@pasadena.gov" and pick.confidence == 0.9

    # parse failure (wrong type back) is a null pick, not an error
    assert LunaContacts(_StubModel(structured="garbage")).pick(JUR, candidates).email is None
    with pytest.raises(PickError):
        LunaContacts(_StubModel(raise_exc=RuntimeError("api"))).pick(JUR, candidates)


def test_terra_draft_contract_prompts() -> None:
    stub = _StubModel(content="Dear Records Officer,\n\nPlease...")
    terra = TerraCorrespondence(stub)
    body = terra.draft_initial(CAMPAIGN, JUR)
    assert body.startswith("Dear Records Officer,")
    prompt = stub.prompts[0]
    # anonymous campaign: no-name instruction present
    assert "anonymous" in prompt
    assert "Never fabricate legal citations" in prompt
    assert "Never promise payment" in prompt

    terra.draft_followup(CAMPAIGN, "Public Records Request", 20)
    assert "20 days" in stub.prompts[1]
    terra.draft_clarification(CAMPAIGN, "which year?")
    assert "Answer ONLY from the record description" in stub.prompts[2]
    terra.draft_fee_agreement(CAMPAIGN, 2550, "fee is $25.50")
    assert "$25.50" in stub.prompts[3]

    with pytest.raises(DraftError):
        TerraCorrespondence(_StubModel(content="")).draft_initial(CAMPAIGN, JUR)
    with pytest.raises(DraftError):
        TerraCorrespondence(_StubModel(raise_exc=RuntimeError("api"))).draft_initial(
            CAMPAIGN, JUR
        )


def test_terra_classify_truncates_and_degrades() -> None:
    stub = _StubModel(
        structured=ClassificationOutput(
            category="referral", summary="ask county", confidence=0.7,
            referral_email="county@x.gov",
        )
    )
    terra = TerraCorrespondence(_StubModel(content="x"), reasoning_model=stub)
    result = terra.classify("Re: request", "please contact the county" + "x" * 10_000)
    assert result.category is InboundCategory.REFERRAL
    assert result.referral_email == "county@x.gov"
    # 6,000-char truncation applied to the body inside the prompt
    assert len(stub.prompts[0]) < 7_000

    # unknown category and parse failure both degrade to unclear
    bad_cat = _StubModel(structured=ClassificationOutput(category="weird"))
    assert (
        TerraCorrespondence(_StubModel(), reasoning_model=bad_cat)
        .classify("s", "b").category is InboundCategory.UNCLEAR
    )
    garbage = _StubModel(structured="garbage")
    assert (
        TerraCorrespondence(_StubModel(), reasoning_model=garbage)
        .classify("s", "b").category is InboundCategory.UNCLEAR
    )
    with pytest.raises(ClassifyError):
        TerraCorrespondence(
            _StubModel(), reasoning_model=_StubModel(raise_exc=RuntimeError("api"))
        ).classify("s", "b")
