"""Fake LLM roles: canned outputs keyed by input, programmable failure."""

from __future__ import annotations

import threading

from ..domain import Campaign, Classification, InboundCategory, Jurisdiction
from ..errors import ClassifyError, DraftError, GenerationError, PickError
from ..ports import ContactPick, EmailCandidate


class FakeContactQueryGenerator:
    def __init__(self) -> None:
        self.queries: dict[str, list[str]] = {}  # jurisdiction name -> queries
        self.fail_names: set[str] = set()
        self.fail_all = False
        self.calls: list[str] = []
        self._lock = threading.Lock()

    def generate_queries(self, jurisdiction: Jurisdiction, record_type: str) -> list[str]:
        with self._lock:
            self.calls.append(jurisdiction.name)
            if self.fail_all or jurisdiction.name in self.fail_names:
                raise GenerationError(f"canned failure for {jurisdiction.name}")
            return list(
                self.queries.get(
                    jurisdiction.name,
                    [f"{jurisdiction.name} {jurisdiction.state} records email"],
                )
            )


class FakeContactPicker:
    def __init__(self) -> None:
        self.picks: dict[str, ContactPick] = {}  # jurisdiction name -> pick
        self.default_pick: ContactPick | None = None
        self.fail_names: set[str] = set()
        self.calls: list[tuple[str, list[EmailCandidate]]] = []
        self._lock = threading.Lock()

    def pick(self, jurisdiction: Jurisdiction, candidates: list[EmailCandidate]) -> ContactPick:
        with self._lock:
            self.calls.append((jurisdiction.name, list(candidates)))
            if jurisdiction.name in self.fail_names:
                raise PickError(f"canned failure for {jurisdiction.name}")
            pick = self.picks.get(jurisdiction.name, self.default_pick)
            if pick is None:
                # default: highest-confidence pick of the first candidate
                if candidates:
                    return ContactPick(email=candidates[0].email, confidence=0.9)
                return ContactPick(email=None)
            return pick


class FakeDrafter:
    def __init__(self) -> None:
        self.fail_next = 0
        self.calls: list[tuple[str, object]] = []
        self._lock = threading.Lock()

    def _guard(self, kind: str, detail: object) -> None:
        self.calls.append((kind, detail))
        if self.fail_next > 0:
            self.fail_next -= 1
            raise DraftError("canned draft failure")

    def draft_initial(self, campaign: Campaign, jurisdiction: Jurisdiction) -> str:
        with self._lock:
            self._guard("initial", jurisdiction.name)
            return (
                f"Dear {jurisdiction.name} Records Officer,\n\n"
                f"Under {campaign.config.legal_basis}, I request the records "
                "described below. Please provide a fee estimate before "
                "incurring costs."
            )

    def draft_followup(self, campaign: Campaign, original_subject: str, waited_days: int) -> str:
        with self._lock:
            self._guard("followup", original_subject)
            return (
                f"I am following up on my request \"{original_subject}\" from "
                f"about {waited_days} days ago. Could you share its status? "
                "Thank you for your time."
            )

    def draft_clarification(self, campaign: Campaign, inbound_body: str) -> str:
        with self._lock:
            self._guard("clarification", inbound_body[:40])
            return (
                "Thank you for the question. The request covers: "
                f"{campaign.config.record_description}"
            )

    def draft_fee_agreement(self, campaign: Campaign, amount_cents: int, inbound_body: str) -> str:
        with self._lock:
            self._guard("fee_agreement", amount_cents)
            return (
                f"We agree to the quoted fee of ${amount_cents / 100:.2f}. "
                "Please share accepted payment methods and a remittance address."
            )


class FakeClassifier:
    def __init__(self) -> None:
        self.classifications: dict[str, Classification] = {}  # body substring -> result
        self.default = Classification(
            category=InboundCategory.ACKNOWLEDGMENT, summary="ack", confidence=0.9
        )
        self.fail_next = 0
        self.calls: list[tuple[str, str]] = []
        self._lock = threading.Lock()

    def canned(self, body_contains: str, classification: Classification) -> None:
        self.classifications[body_contains] = classification

    def classify(self, subject: str, body: str) -> Classification:
        with self._lock:
            self.calls.append((subject, body))
            if self.fail_next > 0:
                self.fail_next -= 1
                raise ClassifyError("canned classify failure")
            for needle, result in self.classifications.items():
                if needle in body or needle in subject:
                    return result
            return self.default
