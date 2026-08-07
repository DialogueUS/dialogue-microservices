"""Static conformance: fakes and adapters satisfy the ports (mypy-only)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from harvest_core import ports as core_ports
    from harvest_core.fakes import FakeObjectStore, FakeQueue

    from . import ports
    from .adapters.llm import LunaContacts, TerraCorrespondence
    from .adapters.resend import ResendTransport
    from .adapters.sql_store import SqlRecordsStore
    from .fakes import (
        FakeClassifier,
        FakeContactPicker,
        FakeContactQueryGenerator,
        FakeDrafter,
        FakeEmailTransport,
        FakeQueueWithDlq,
        FakeRecordsStore,
    )

    def _check_fakes(
        store: FakeRecordsStore,
        transport: FakeEmailTransport,
        generator: FakeContactQueryGenerator,
        picker: FakeContactPicker,
        drafter: FakeDrafter,
        classifier: FakeClassifier,
        queue: FakeQueueWithDlq,
        plain_queue: FakeQueue,
        objects: FakeObjectStore,
    ) -> None:
        _s: ports.RecordsStore = store
        _t: ports.EmailTransport = transport
        _g: ports.ContactQueryGenerator = generator
        _p: ports.ContactPicker = picker
        _d: ports.EmailDrafter = drafter
        _c: ports.InboundClassifier = classifier
        _q: core_ports.TaskQueue = queue
        _q2: core_ports.TaskQueue = plain_queue
        _o: core_ports.ObjectStore = objects

    def _check_adapters(
        store: SqlRecordsStore,
        transport: ResendTransport,
        luna: LunaContacts,
        terra: TerraCorrespondence,
    ) -> None:
        _s: ports.RecordsStore = store
        _t: ports.EmailTransport = transport
        _g: ports.ContactQueryGenerator = luna
        _p: ports.ContactPicker = luna
        _d: ports.EmailDrafter = terra
        _c: ports.InboundClassifier = terra
