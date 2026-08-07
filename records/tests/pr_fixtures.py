"""Shared fake-world fixture for public_records tests.

Imported by tests directly (unique module name per repo convention);
conftest.py re-imports the fixture function for registration.
"""

from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any

import pytest
from harvest_core.fakes import (
    FakeFetcher,
    FakeKeyValue,
    FakeObjectStore,
    FakeQueue,
    FakeSearch,
    VirtualClock,
)
from public_records.config import CampaignConfig
from public_records.constants import (
    MAX_RECEIVE_INBOUND,
    MAX_RECEIVE_SEARCH,
    MAX_RECEIVE_SENDER,
    VISIBILITY_TIMEOUT_S,
)
from public_records.domain import Campaign, Jurisdiction
from public_records.fakes import (
    FakeClassifier,
    FakeContactPicker,
    FakeContactQueryGenerator,
    FakeDrafter,
    FakeEmailTransport,
    FakeQueueWithDlq,
    FakeRecordsStore,
)
from public_records.world import World

FROM_ADDRESS = "requests@dialogue-tests.org"


@dataclass
class PrWorld:
    """The World plus concretely-typed handles to every fake."""

    world: World
    clock: VirtualClock
    store: FakeRecordsStore
    kv: FakeKeyValue
    mail_bucket: FakeObjectStore
    documents: FakeObjectStore
    search_queue: FakeQueueWithDlq
    contacts_queue: FakeQueueWithDlq
    followups_queue: FakeQueueWithDlq
    inbound_queue: FakeQueueWithDlq
    search_dlq: FakeQueue
    contacts_dlq: FakeQueue
    followups_dlq: FakeQueue
    inbound_dlq: FakeQueue
    search: FakeSearch
    fetcher: FakeFetcher
    query_generator: FakeContactQueryGenerator
    picker: FakeContactPicker
    drafter: FakeDrafter
    classifier: FakeClassifier
    transport: FakeEmailTransport

    # -- convenience -------------------------------------------------------
    def add_campaign(self, active: bool = True, **overrides: Any) -> Campaign:
        config = campaign_config(**overrides)
        campaign = self.store.insert_campaign(config, self.clock.now())
        if active:
            self.store.set_campaign_active(campaign.id, True)
        return self.store.get_campaign(campaign.id) or campaign

    def add_jurisdiction(
        self, name: str = "Pasadena", state: str = "CA", level: str = "county",
        contact_email: str | None = None,
    ) -> Jurisdiction:
        jur = self.store.insert_jurisdiction(name, state, level)
        if contact_email:
            self.store.set_jurisdiction_contact(jur.id, contact_email, None, None)
        loaded = self.store.get_jurisdiction(jur.id)
        assert loaded is not None
        return loaded


def campaign_config(**overrides: Any) -> CampaignConfig:
    base: dict[str, Any] = {
        "name": "noise-2026",
        "record_type": "noise complaints",
        "record_description": "All noise complaints filed in 2025.",
        "requester": {
            "name": "Ada Requester",
            "email": "ada@example.org",
            "consent_confirmed": True,
        },
        "scope": {"levels": ["county"], "states": ["CA"]},
        "dry_run": False,
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = {**base[key], **value}
        else:
            base[key] = value
    return CampaignConfig.model_validate(base)


def make_world() -> PrWorld:
    clock = VirtualClock()
    store = FakeRecordsStore()
    kv = FakeKeyValue(clock)
    mail_bucket = FakeObjectStore()
    documents = FakeObjectStore()

    search_dlq = FakeQueue(clock)
    contacts_dlq = FakeQueue(clock)
    followups_dlq = FakeQueue(clock)
    inbound_dlq = FakeQueue(clock)
    search_queue = FakeQueueWithDlq(
        clock, VISIBILITY_TIMEOUT_S, MAX_RECEIVE_SEARCH, dlq_queue=search_dlq
    )
    contacts_queue = FakeQueueWithDlq(
        clock, VISIBILITY_TIMEOUT_S, MAX_RECEIVE_SENDER, dlq_queue=contacts_dlq
    )
    followups_queue = FakeQueueWithDlq(
        clock, VISIBILITY_TIMEOUT_S, MAX_RECEIVE_SENDER, dlq_queue=followups_dlq
    )
    inbound_queue = FakeQueueWithDlq(
        clock, VISIBILITY_TIMEOUT_S, MAX_RECEIVE_INBOUND, dlq_queue=inbound_dlq
    )

    search = FakeSearch()
    fetcher = FakeFetcher()
    query_generator = FakeContactQueryGenerator()
    picker = FakeContactPicker()
    drafter = FakeDrafter()
    classifier = FakeClassifier()
    transport = FakeEmailTransport()

    world = World(
        clock=clock,
        store=store,
        kv=kv,
        mail_bucket=mail_bucket,
        documents=documents,
        search_queue=search_queue,
        contacts_queue=contacts_queue,
        followups_queue=followups_queue,
        inbound_queue=inbound_queue,
        search=search,
        fetcher=fetcher,
        query_generator=query_generator,
        picker=picker,
        drafter=drafter,
        classifier=classifier,
        transport=transport,
        from_address=FROM_ADDRESS,
    )
    return PrWorld(
        world=world, clock=clock, store=store, kv=kv, mail_bucket=mail_bucket,
        documents=documents, search_queue=search_queue, contacts_queue=contacts_queue,
        followups_queue=followups_queue, inbound_queue=inbound_queue,
        search_dlq=search_dlq, contacts_dlq=contacts_dlq,
        followups_dlq=followups_dlq, inbound_dlq=inbound_dlq,
        search=search, fetcher=fetcher, query_generator=query_generator,
        picker=picker, drafter=drafter, classifier=classifier, transport=transport,
    )


@pytest.fixture()
def pr() -> PrWorld:
    return make_world()


def build_mime(
    from_address: str = "clerk@pasadena.gov",
    to_address: str = FROM_ADDRESS,
    subject: str = "Re: Public Records Request",
    body: str = "Thank you for your request.",
    token: str | None = None,
    token_in_header: bool = False,
    message_id: str | None = "<msg-1@pasadena.gov>",
    html_body: str | None = None,
    attachments: list[tuple[str, str, bytes]] | None = None,
) -> bytes:
    msg = EmailMessage()
    if token and not token_in_header:
        subject = f"{subject} [DLG-{token}]"
    msg["From"] = from_address
    msg["To"] = to_address
    msg["Subject"] = subject
    if message_id:
        msg["Message-ID"] = message_id
    if token and token_in_header:
        msg["X-Dialogue-Token"] = token
    msg.set_content(body)
    if html_body:
        msg.add_alternative(html_body, subtype="html")
    for filename, content_type, payload in attachments or []:
        maintype, _, subtype = content_type.partition("/")
        msg.add_attachment(payload, maintype=maintype, subtype=subtype, filename=filename)
    return msg.as_bytes()
