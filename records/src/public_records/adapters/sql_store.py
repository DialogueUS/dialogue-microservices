"""SQLAlchemy schema, idempotent boot migration, and the SQL RecordsStore.

Unit-tested on SQLite; live-Postgres verification belongs to the manual
smoke (SQLite doesn't enforce FK ordering, so purge-order bugs only show
against real Postgres).
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
from harvest_core.adapters.db import run_migration
from sqlalchemy.engine import Engine, Row
from sqlalchemy.exc import IntegrityError

from ..config import CampaignConfig, ContactsConfig, LimitsConfig, RequesterConfig, ScopeConfig
from ..domain import (
    Campaign,
    Classification,
    EmailDirection,
    EmailRecord,
    EmailThread,
    Escalation,
    EscalationReason,
    EscalationStatus,
    InboundCategory,
    Jurisdiction,
    OutboundKind,
    SearchTarget,
    SpendEntry,
    ThreadStatus,
    check_transition,
)
from ..errors import UniqueViolation

metadata = sa.MetaData()

campaigns = sa.Table(
    "campaigns",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String(200), nullable=False, unique=True),
    sa.Column("record_type", sa.String(400), nullable=False),
    sa.Column("record_description", sa.Text, nullable=False),
    sa.Column("legal_basis", sa.Text, nullable=False),
    sa.Column("requester_name", sa.String(200), nullable=False),
    sa.Column("requester_email", sa.String(320), nullable=False),
    sa.Column("requester_organization", sa.String(200)),
    sa.Column("requester_phone", sa.String(64)),
    sa.Column("requester_mailing_address", sa.String(500)),
    sa.Column("anonymous", sa.Boolean, nullable=False, default=True),
    sa.Column("consent_confirmed", sa.Boolean, nullable=False, default=False),
    sa.Column("scope", sa.Text, nullable=False),  # JSON: levels, states, only
    sa.Column("limits", sa.Text, nullable=False),  # JSON, §11
    sa.Column("contacts", sa.Text, nullable=False),  # JSON: min_confidence
    sa.Column("active", sa.Boolean, nullable=False, default=False),
    sa.Column("seeded", sa.Boolean, nullable=False, default=False),
    sa.Column("dry_run", sa.Boolean, nullable=False, default=True),
    sa.Column("notify_email", sa.String(320)),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
)

jurisdictions = sa.Table(
    "jurisdictions",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String(200), nullable=False),
    sa.Column("state", sa.String(2), nullable=False),
    sa.Column("level", sa.String(16), nullable=False),
    sa.Column("parent_name", sa.String(200)),
    sa.Column("contact_email", sa.String(320)),
    sa.Column("contact_name", sa.String(200)),
    sa.Column("contact_url", sa.String(600)),
    sa.Column("contact_verified", sa.Boolean, nullable=False, default=False),
    sa.Column("last_contacted_at", sa.DateTime(timezone=True)),
    sa.UniqueConstraint("level", "state", "name", name="uq_pr_jurisdiction"),
)

email_threads = sa.Table(
    "email_threads",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("campaign_id", sa.Integer, sa.ForeignKey("campaigns.id"), nullable=False),
    sa.Column(
        "jurisdiction_id", sa.Integer, sa.ForeignKey("jurisdictions.id"), nullable=False
    ),
    sa.Column("thread_token", sa.String(32), nullable=False, unique=True),
    sa.Column("contact_email", sa.String(320), nullable=False),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("parent_thread_id", sa.Integer, sa.ForeignKey("email_threads.id")),
    sa.Column("followups_sent", sa.Integer, nullable=False, default=0),
    sa.Column("next_action_at", sa.DateTime(timezone=True)),
    sa.Column("attachment_keys", sa.Text, nullable=False, default="[]"),
    sa.Column("notes", sa.Text, nullable=False, default=""),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.Index("ix_threads_due", "status", "next_action_at"),
)

emails = sa.Table(
    "emails",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("thread_id", sa.Integer, sa.ForeignKey("email_threads.id")),
    sa.Column("campaign_id", sa.Integer, sa.ForeignKey("campaigns.id")),
    sa.Column("direction", sa.String(8), nullable=False),
    sa.Column("from_address", sa.String(320), nullable=False),
    sa.Column("to_address", sa.String(320), nullable=False),
    sa.Column("subject", sa.Text, nullable=False, default=""),
    sa.Column("body", sa.Text, nullable=False, default=""),
    sa.Column("kind", sa.String(32)),
    sa.Column("classification", sa.Text),  # JSON: category + summary + confidence
    sa.Column("message_id", sa.String(400)),
    sa.Column("source_key", sa.String(600)),
    sa.Column("resend_id", sa.String(200)),
    sa.Column("in_reply_to_email_id", sa.Integer, sa.ForeignKey("emails.id")),
    sa.Column("attachment_refs", sa.Text, nullable=False, default="[]"),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.UniqueConstraint("campaign_id", "message_id", name="uq_email_campaign_message"),
    sa.Index("ix_emails_source_key", "source_key"),
)

escalations = sa.Table(
    "escalations",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("campaign_id", sa.Integer, sa.ForeignKey("campaigns.id")),
    sa.Column("thread_id", sa.Integer, sa.ForeignKey("email_threads.id")),
    sa.Column("reason", sa.String(32), nullable=False),
    sa.Column("details", sa.Text, nullable=False, default=""),
    sa.Column("status", sa.String(16), nullable=False, default="OPEN"),
    sa.Column("resolution", sa.Text, nullable=False, default=""),
    sa.Column("notified", sa.Boolean, nullable=False, default=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("resolved_at", sa.DateTime(timezone=True)),
)

spend_entries = sa.Table(
    "spend_entries",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("campaign_id", sa.Integer, sa.ForeignKey("campaigns.id"), nullable=False),
    sa.Column("thread_id", sa.Integer, sa.ForeignKey("email_threads.id")),
    sa.Column("amount_cents", sa.Integer, nullable=False),  # never floats
    sa.Column("kind", sa.String(32), nullable=False, default="fee_authorized"),
    sa.Column("note", sa.String(500), nullable=False, default=""),
    sa.Column("remitted", sa.Boolean, nullable=False, default=False),
    sa.Column("notified", sa.Boolean, nullable=False, default=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
)

search_targets = sa.Table(
    "search_targets",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("campaign_id", sa.Integer, sa.ForeignKey("campaigns.id"), nullable=False),
    sa.Column(
        "jurisdiction_id", sa.Integer, sa.ForeignKey("jurisdictions.id"), nullable=False
    ),
    sa.Column("queries_enqueued", sa.Integer, nullable=False, default=0),
    sa.Column("consumed_indexes", sa.Text, nullable=False, default="[]"),
    sa.Column("resolved", sa.Boolean, nullable=False, default=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.UniqueConstraint("campaign_id", "jurisdiction_id", name="uq_search_target"),
)


def migrate(engine: Engine) -> None:
    """Idempotent boot migration, safe under concurrent service starts."""
    run_migration(engine, metadata)


def _aware(dt: datetime | None) -> datetime | None:
    """SQLite drops tzinfo; everything in this system is UTC."""
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def _pk(result: sa.CursorResult[Any]) -> int:
    pk = result.inserted_primary_key
    assert pk is not None
    return int(pk[0])


class SqlRecordsStore:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    # -- row mappers -------------------------------------------------------
    @staticmethod
    def _campaign(row: Row[Any]) -> Campaign:
        config = CampaignConfig(
            name=row.name,
            record_type=row.record_type,
            record_description=row.record_description,
            legal_basis=row.legal_basis,
            requester=RequesterConfig(
                name=row.requester_name,
                email=row.requester_email,
                organization=row.requester_organization,
                phone=row.requester_phone,
                mailing_address=row.requester_mailing_address,
                anonymous=row.anonymous,
                consent_confirmed=row.consent_confirmed,
            ),
            scope=ScopeConfig.model_validate(json.loads(row.scope)),
            limits=LimitsConfig.model_validate(json.loads(row.limits)),
            contacts=ContactsConfig.model_validate(json.loads(row.contacts)),
            dry_run=row.dry_run,
            notify_email=row.notify_email,
        )
        return Campaign(
            id=row.id,
            config=config,
            active=row.active,
            seeded=row.seeded,
            created_at=_aware(row.created_at),
        )

    @staticmethod
    def _jurisdiction(row: Row[Any]) -> Jurisdiction:
        return Jurisdiction(
            id=row.id,
            name=row.name,
            state=row.state,
            level=row.level,
            parent_name=row.parent_name,
            contact_email=row.contact_email,
            contact_name=row.contact_name,
            contact_url=row.contact_url,
            contact_verified=row.contact_verified,
            last_contacted_at=_aware(row.last_contacted_at),
        )

    @staticmethod
    def _thread(row: Row[Any]) -> EmailThread:
        return EmailThread(
            id=row.id,
            campaign_id=row.campaign_id,
            jurisdiction_id=row.jurisdiction_id,
            thread_token=row.thread_token,
            contact_email=row.contact_email,
            status=ThreadStatus(row.status),
            parent_thread_id=row.parent_thread_id,
            followups_sent=row.followups_sent,
            next_action_at=_aware(row.next_action_at),
            attachment_keys=list(json.loads(row.attachment_keys)),
            notes=row.notes,
            created_at=_aware(row.created_at),
            updated_at=_aware(row.updated_at),
        )

    @staticmethod
    def _email(row: Row[Any]) -> EmailRecord:
        classification = None
        if row.classification:
            raw = json.loads(row.classification)
            classification = Classification(
                category=InboundCategory(raw["category"].upper()),
                summary=raw.get("summary", ""),
                confidence=raw.get("confidence", 0.0),
                referral_email=raw.get("referral_email"),
            )
        return EmailRecord(
            id=row.id,
            thread_id=row.thread_id,
            campaign_id=row.campaign_id,
            direction=EmailDirection(row.direction),
            from_address=row.from_address,
            to_address=row.to_address,
            subject=row.subject,
            body=row.body,
            kind=OutboundKind(row.kind) if row.kind else None,
            classification=classification,
            message_id=row.message_id,
            source_key=row.source_key,
            resend_id=row.resend_id,
            in_reply_to_email_id=row.in_reply_to_email_id,
            attachment_refs=list(json.loads(row.attachment_refs)),
            created_at=_aware(row.created_at),
        )

    @staticmethod
    def _escalation(row: Row[Any]) -> Escalation:
        return Escalation(
            id=row.id,
            campaign_id=row.campaign_id,
            thread_id=row.thread_id,
            reason=EscalationReason(row.reason),
            details=row.details,
            status=EscalationStatus(row.status),
            resolution=row.resolution,
            notified=row.notified,
            created_at=_aware(row.created_at),
            resolved_at=_aware(row.resolved_at),
        )

    @staticmethod
    def _spend(row: Row[Any]) -> SpendEntry:
        return SpendEntry(
            id=row.id,
            campaign_id=row.campaign_id,
            thread_id=row.thread_id,
            amount_cents=row.amount_cents,
            kind=row.kind,
            note=row.note,
            remitted=row.remitted,
            notified=row.notified,
            created_at=_aware(row.created_at),
        )

    @staticmethod
    def _target(row: Row[Any]) -> SearchTarget:
        return SearchTarget(
            id=row.id,
            campaign_id=row.campaign_id,
            jurisdiction_id=row.jurisdiction_id,
            queries_enqueued=row.queries_enqueued,
            consumed_indexes=list(json.loads(row.consumed_indexes)),
            resolved=row.resolved,
            created_at=_aware(row.created_at),
        )

    # -- campaigns -----------------------------------------------------------
    def insert_campaign(self, config: CampaignConfig, created_at: datetime) -> Campaign:
        values = dict(
            name=config.name,
            record_type=config.record_type,
            record_description=config.record_description,
            legal_basis=config.legal_basis,
            requester_name=config.requester.name,
            requester_email=config.requester.email,
            requester_organization=config.requester.organization,
            requester_phone=config.requester.phone,
            requester_mailing_address=config.requester.mailing_address,
            anonymous=config.requester.anonymous,
            consent_confirmed=config.requester.consent_confirmed,
            scope=config.scope.model_dump_json(),
            limits=config.limits.model_dump_json(),
            contacts=config.contacts.model_dump_json(),
            active=False,
            seeded=False,
            dry_run=config.dry_run,
            notify_email=config.notify_email,
            created_at=created_at,
        )
        try:
            with self._engine.begin() as conn:
                result = conn.execute(sa.insert(campaigns).values(**values))
                new_id = _pk(result)
        except IntegrityError as exc:
            raise UniqueViolation(f"campaign name {config.name!r}") from exc
        return Campaign(id=new_id, config=config, created_at=created_at)

    def get_campaign(self, campaign_id: int) -> Campaign | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(campaigns).where(campaigns.c.id == campaign_id)
            ).first()
        return self._campaign(row) if row else None

    def get_campaign_by_name(self, name: str) -> Campaign | None:
        with self._engine.connect() as conn:
            row = conn.execute(sa.select(campaigns).where(campaigns.c.name == name)).first()
        return self._campaign(row) if row else None

    def list_campaigns(self) -> list[Campaign]:
        with self._engine.connect() as conn:
            rows = conn.execute(sa.select(campaigns).order_by(campaigns.c.id)).all()
        return [self._campaign(r) for r in rows]

    def set_campaign_active(self, campaign_id: int, active: bool) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                sa.update(campaigns).where(campaigns.c.id == campaign_id).values(active=active)
            )

    def set_campaign_seeded(self, campaign_id: int) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                sa.update(campaigns).where(campaigns.c.id == campaign_id).values(seeded=True)
            )

    def update_campaign_config(self, campaign_id: int, config: CampaignConfig) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                sa.update(campaigns)
                .where(campaigns.c.id == campaign_id)
                .values(
                    record_type=config.record_type,
                    record_description=config.record_description,
                    legal_basis=config.legal_basis,
                    requester_name=config.requester.name,
                    requester_email=config.requester.email,
                    requester_organization=config.requester.organization,
                    requester_phone=config.requester.phone,
                    requester_mailing_address=config.requester.mailing_address,
                    anonymous=config.requester.anonymous,
                    consent_confirmed=config.requester.consent_confirmed,
                    scope=config.scope.model_dump_json(),
                    limits=config.limits.model_dump_json(),
                    contacts=config.contacts.model_dump_json(),
                    dry_run=config.dry_run,
                    notify_email=config.notify_email,
                )
            )

    def count_outbound_since(self, campaign_id: int, since: datetime) -> int:
        with self._engine.connect() as conn:
            count = conn.execute(
                sa.select(sa.func.count())
                .select_from(emails)
                .where(
                    emails.c.campaign_id == campaign_id,
                    emails.c.direction == "OUTBOUND",
                    emails.c.created_at >= since,
                )
            ).scalar_one()
        return int(count)

    # -- jurisdictions ---------------------------------------------------------
    def insert_jurisdiction(
        self, name: str, state: str, level: str, parent_name: str | None = None
    ) -> Jurisdiction:
        try:
            with self._engine.begin() as conn:
                result = conn.execute(
                    sa.insert(jurisdictions).values(
                        name=name,
                        state=state,
                        level=level,
                        parent_name=parent_name,
                        contact_verified=False,
                    )
                )
                new_id = _pk(result)
        except IntegrityError as exc:
            raise UniqueViolation(f"jurisdiction ({level}, {state}, {name})") from exc
        return Jurisdiction(id=new_id, name=name, state=state, level=level,
                            parent_name=parent_name)

    def get_jurisdiction(self, jurisdiction_id: int) -> Jurisdiction | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(jurisdictions).where(jurisdictions.c.id == jurisdiction_id)
            ).first()
        return self._jurisdiction(row) if row else None

    def list_jurisdictions(
        self, states: list[str] | None = None, levels: list[str] | None = None
    ) -> list[Jurisdiction]:
        stmt = sa.select(jurisdictions).order_by(jurisdictions.c.id)
        if states is not None:
            stmt = stmt.where(jurisdictions.c.state.in_(states))
        if levels is not None:
            stmt = stmt.where(jurisdictions.c.level.in_(levels))
        with self._engine.connect() as conn:
            rows = conn.execute(stmt).all()
        return [self._jurisdiction(r) for r in rows]

    def state_row_exists(self, state: str) -> bool:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(jurisdictions.c.id).where(
                    jurisdictions.c.state == state, jurisdictions.c.level == "state"
                )
            ).first()
        return row is not None

    def set_jurisdiction_contact(
        self, jurisdiction_id: int, email: str, name: str | None, url: str | None
    ) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                sa.update(jurisdictions)
                .where(jurisdictions.c.id == jurisdiction_id)
                .values(
                    contact_email=email,
                    contact_name=name,
                    contact_url=url,
                    contact_verified=False,
                )
            )

    def stamp_last_contacted(self, jurisdiction_id: int, at: datetime) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                sa.update(jurisdictions)
                .where(jurisdictions.c.id == jurisdiction_id)
                .values(last_contacted_at=at)
            )

    # -- search targets ----------------------------------------------------------
    def insert_search_target(
        self, campaign_id: int, jurisdiction_id: int, created_at: datetime
    ) -> SearchTarget:
        try:
            with self._engine.begin() as conn:
                result = conn.execute(
                    sa.insert(search_targets).values(
                        campaign_id=campaign_id,
                        jurisdiction_id=jurisdiction_id,
                        queries_enqueued=0,
                        consumed_indexes="[]",
                        resolved=False,
                        created_at=created_at,
                    )
                )
                new_id = _pk(result)
        except IntegrityError as exc:
            raise UniqueViolation(
                f"search_target ({campaign_id}, {jurisdiction_id})"
            ) from exc
        return SearchTarget(
            id=new_id,
            campaign_id=campaign_id,
            jurisdiction_id=jurisdiction_id,
            created_at=created_at,
        )

    def get_search_target(self, target_id: int) -> SearchTarget | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(search_targets).where(search_targets.c.id == target_id)
            ).first()
        return self._target(row) if row else None

    def find_search_target(
        self, campaign_id: int, jurisdiction_id: int
    ) -> SearchTarget | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(search_targets).where(
                    search_targets.c.campaign_id == campaign_id,
                    search_targets.c.jurisdiction_id == jurisdiction_id,
                )
            ).first()
        return self._target(row) if row else None

    def set_target_queries_enqueued(self, target_id: int, count: int) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                sa.update(search_targets)
                .where(search_targets.c.id == target_id)
                .values(queries_enqueued=count)
            )

    def resolve_target(self, target_id: int) -> bool:
        with self._engine.begin() as conn:
            result = conn.execute(
                sa.update(search_targets)
                .where(search_targets.c.id == target_id, search_targets.c.resolved == False)  # noqa: E712
                .values(resolved=True)
            )
        return result.rowcount == 1

    def mark_query_consumed(self, target_id: int, query_index: int) -> bool:
        with self._engine.begin() as conn:
            row = conn.execute(
                sa.select(search_targets)
                .where(search_targets.c.id == target_id)
                .with_for_update()
            ).first()
            assert row is not None
            consumed = list(json.loads(row.consumed_indexes))
            if query_index not in consumed:
                consumed.append(query_index)
                conn.execute(
                    sa.update(search_targets)
                    .where(search_targets.c.id == target_id)
                    .values(consumed_indexes=json.dumps(consumed))
                )
            return row.queries_enqueued > 0 and len(consumed) >= row.queries_enqueued

    # -- threads ---------------------------------------------------------------
    def get_thread(self, thread_id: int) -> EmailThread | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(email_threads).where(email_threads.c.id == thread_id)
            ).first()
        return self._thread(row) if row else None

    def get_thread_by_token(self, token: str) -> EmailThread | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(email_threads).where(email_threads.c.thread_token == token)
            ).first()
        return self._thread(row) if row else None

    def find_thread(
        self, campaign_id: int, jurisdiction_id: int, contact_email: str
    ) -> EmailThread | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(email_threads).where(
                    email_threads.c.campaign_id == campaign_id,
                    email_threads.c.jurisdiction_id == jurisdiction_id,
                    email_threads.c.contact_email == contact_email,
                )
            ).first()
        return self._thread(row) if row else None

    def find_open_thread_by_contact(self, contact_email: str) -> EmailThread | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(email_threads).where(
                    email_threads.c.contact_email == contact_email,
                    email_threads.c.status.in_(["REQUEST_SENT", "AWAITING_REPLY"]),
                )
            ).first()
        return self._thread(row) if row else None

    def set_thread_status(
        self,
        thread_id: int,
        status: ThreadStatus,
        next_action_at: datetime | None,
        *,
        by_human: bool = False,
    ) -> None:
        with self._engine.begin() as conn:
            row = conn.execute(
                sa.select(email_threads.c.status).where(email_threads.c.id == thread_id)
            ).first()
            assert row is not None
            check_transition(ThreadStatus(row.status), status, by_human=by_human)
            conn.execute(
                sa.update(email_threads)
                .where(email_threads.c.id == thread_id)
                .values(status=status.value, next_action_at=next_action_at)
            )

    def select_due_followups(self, now: datetime, limit: int) -> list[EmailThread]:
        with self._engine.connect() as conn:
            rows = conn.execute(
                sa.select(email_threads)
                .where(
                    email_threads.c.status.in_(["REQUEST_SENT", "AWAITING_REPLY"]),
                    email_threads.c.next_action_at.is_not(None),
                    email_threads.c.next_action_at <= now,
                )
                .order_by(email_threads.c.next_action_at, email_threads.c.id)
                .limit(limit)
            ).all()
        return [self._thread(r) for r in rows]

    def append_attachment_key(self, thread_id: int, key: str) -> None:
        with self._engine.begin() as conn:
            row = conn.execute(
                sa.select(email_threads.c.attachment_keys)
                .where(email_threads.c.id == thread_id)
                .with_for_update()
            ).first()
            assert row is not None
            keys = list(json.loads(row.attachment_keys))
            keys.append(key)
            conn.execute(
                sa.update(email_threads)
                .where(email_threads.c.id == thread_id)
                .values(attachment_keys=json.dumps(keys))
            )

    # -- emails ------------------------------------------------------------------
    def has_source_key(self, source_key: str) -> bool:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(emails.c.id).where(emails.c.source_key == source_key)
            ).first()
        return row is not None

    def has_message_id(self, campaign_id: int | None, message_id: str) -> bool:
        stmt = sa.select(emails.c.id).where(emails.c.message_id == message_id)
        if campaign_id is None:
            stmt = stmt.where(emails.c.campaign_id.is_(None))
        else:
            stmt = stmt.where(emails.c.campaign_id == campaign_id)
        with self._engine.connect() as conn:
            row = conn.execute(stmt).first()
        return row is not None

    def insert_email(
        self,
        *,
        thread_id: int | None,
        campaign_id: int | None,
        direction: str,
        from_address: str,
        to_address: str,
        subject: str,
        body: str,
        kind: OutboundKind | None,
        classification: Classification | None,
        message_id: str | None,
        source_key: str | None,
        resend_id: str | None,
        in_reply_to_email_id: int | None,
        attachment_refs: list[dict[str, str]],
        created_at: datetime,
    ) -> EmailRecord:
        with self._engine.begin() as conn:
            return self._insert_email_conn(
                conn,
                thread_id=thread_id,
                campaign_id=campaign_id,
                direction=direction,
                from_address=from_address,
                to_address=to_address,
                subject=subject,
                body=body,
                kind=kind,
                classification=classification,
                message_id=message_id,
                source_key=source_key,
                resend_id=resend_id,
                in_reply_to_email_id=in_reply_to_email_id,
                attachment_refs=attachment_refs,
                created_at=created_at,
            )

    def _insert_email_conn(
        self,
        conn: sa.Connection,
        *,
        thread_id: int | None,
        campaign_id: int | None,
        direction: str,
        from_address: str,
        to_address: str,
        subject: str,
        body: str,
        kind: OutboundKind | None,
        classification: Classification | None,
        message_id: str | None,
        source_key: str | None,
        resend_id: str | None,
        in_reply_to_email_id: int | None,
        attachment_refs: list[dict[str, str]],
        created_at: datetime,
    ) -> EmailRecord:
        if message_id is not None:
            dup = conn.execute(
                sa.select(emails.c.id).where(
                    emails.c.message_id == message_id,
                    emails.c.campaign_id.is_(None)
                    if campaign_id is None
                    else emails.c.campaign_id == campaign_id,
                )
            ).first()
            if dup is not None:
                raise UniqueViolation(f"message_id {message_id!r} for campaign {campaign_id}")
        try:
            result = conn.execute(
                sa.insert(emails).values(
                    thread_id=thread_id,
                    campaign_id=campaign_id,
                    direction=direction.upper(),
                    from_address=from_address,
                    to_address=to_address,
                    subject=subject,
                    body=body,
                    kind=kind.value if kind else None,
                    classification=(
                        json.dumps(classification.to_json_dict()) if classification else None
                    ),
                    message_id=message_id,
                    source_key=source_key,
                    resend_id=resend_id,
                    in_reply_to_email_id=in_reply_to_email_id,
                    attachment_refs=json.dumps(attachment_refs),
                    created_at=created_at,
                )
            )
        except IntegrityError as exc:
            raise UniqueViolation(f"message_id {message_id!r}") from exc
        return EmailRecord(
            id=_pk(result),
            thread_id=thread_id,
            campaign_id=campaign_id,
            direction=EmailDirection(direction.upper()),
            from_address=from_address,
            to_address=to_address,
            subject=subject,
            body=body,
            kind=kind,
            classification=classification,
            message_id=message_id,
            source_key=source_key,
            resend_id=resend_id,
            in_reply_to_email_id=in_reply_to_email_id,
            attachment_refs=list(attachment_refs),
            created_at=created_at,
        )

    def get_email(self, email_id: int) -> EmailRecord | None:
        with self._engine.connect() as conn:
            row = conn.execute(sa.select(emails).where(emails.c.id == email_id)).first()
        return self._email(row) if row else None

    def update_attachment_refs(self, email_id: int, refs: list[dict[str, str]]) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                sa.update(emails)
                .where(emails.c.id == email_id)
                .values(attachment_refs=json.dumps(refs))
            )

    def first_outbound_subject(self, thread_id: int) -> str | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(emails.c.subject)
                .where(emails.c.thread_id == thread_id, emails.c.direction == "OUTBOUND")
                .order_by(emails.c.id)
                .limit(1)
            ).first()
        return row.subject if row else None

    def outbound_reply_exists(
        self, thread_id: int, inbound_email_id: int, kind: OutboundKind
    ) -> bool:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(emails.c.id).where(
                    emails.c.thread_id == thread_id,
                    emails.c.direction == "OUTBOUND",
                    emails.c.kind == kind.value,
                    emails.c.in_reply_to_email_id == inbound_email_id,
                )
            ).first()
        return row is not None

    def list_emails(
        self, thread_id: int | None = None, campaign_id: int | None = None
    ) -> list[EmailRecord]:
        stmt = sa.select(emails).order_by(emails.c.id)
        if thread_id is not None:
            stmt = stmt.where(emails.c.thread_id == thread_id)
        if campaign_id is not None:
            stmt = stmt.where(emails.c.campaign_id == campaign_id)
        with self._engine.connect() as conn:
            rows = conn.execute(stmt).all()
        return [self._email(r) for r in rows]

    # -- composite send commits ---------------------------------------------------
    def record_initial_send(
        self,
        *,
        campaign_id: int,
        jurisdiction_id: int,
        thread_token: str,
        contact_email: str,
        parent_thread_id: int | None,
        existing_thread_id: int | None,
        from_address: str,
        to_address: str,
        subject: str,
        body: str,
        resend_id: str | None,
        next_action_at: datetime,
        now: datetime,
    ) -> tuple[EmailThread, EmailRecord]:
        try:
            with self._engine.begin() as conn:
                if existing_thread_id is not None:
                    row = conn.execute(
                        sa.select(email_threads.c.status).where(
                            email_threads.c.id == existing_thread_id
                        )
                    ).first()
                    assert row is not None
                    check_transition(ThreadStatus(row.status), ThreadStatus.REQUEST_SENT)
                    conn.execute(
                        sa.update(email_threads)
                        .where(email_threads.c.id == existing_thread_id)
                        .values(
                            status="REQUEST_SENT",
                            contact_email=contact_email,
                            next_action_at=next_action_at,
                            updated_at=now,
                        )
                    )
                    thread_id = existing_thread_id
                else:
                    result = conn.execute(
                        sa.insert(email_threads).values(
                            campaign_id=campaign_id,
                            jurisdiction_id=jurisdiction_id,
                            thread_token=thread_token,
                            contact_email=contact_email,
                            status="REQUEST_SENT",
                            parent_thread_id=parent_thread_id,
                            followups_sent=0,
                            next_action_at=next_action_at,
                            attachment_keys="[]",
                            notes="",
                            created_at=now,
                            updated_at=now,
                        )
                    )
                    thread_id = _pk(result)
                email = self._insert_email_conn(
                    conn,
                    thread_id=thread_id,
                    campaign_id=campaign_id,
                    direction="outbound",
                    from_address=from_address,
                    to_address=to_address,
                    subject=subject,
                    body=body,
                    kind=OutboundKind.INITIAL_REQUEST,
                    classification=None,
                    message_id=None,
                    source_key=None,
                    resend_id=resend_id,
                    in_reply_to_email_id=None,
                    attachment_refs=[],
                    created_at=now,
                )
                conn.execute(
                    sa.update(jurisdictions)
                    .where(jurisdictions.c.id == jurisdiction_id)
                    .values(last_contacted_at=now)
                )
        except IntegrityError as exc:
            raise UniqueViolation(f"thread_token {thread_token!r}") from exc
        thread = self.get_thread(thread_id)
        assert thread is not None
        return thread, email

    def record_thread_send(
        self,
        *,
        thread_id: int,
        kind: OutboundKind,
        from_address: str,
        to_address: str,
        subject: str,
        body: str,
        resend_id: str | None,
        in_reply_to_email_id: int | None,
        new_status: ThreadStatus,
        next_action_at: datetime | None,
        increment_followups: bool,
        now: datetime,
    ) -> EmailRecord:
        with self._engine.begin() as conn:
            row = conn.execute(
                sa.select(
                    email_threads.c.status,
                    email_threads.c.campaign_id,
                    email_threads.c.followups_sent,
                ).where(email_threads.c.id == thread_id)
            ).first()
            assert row is not None
            check_transition(ThreadStatus(row.status), new_status)
            email = self._insert_email_conn(
                conn,
                thread_id=thread_id,
                campaign_id=row.campaign_id,
                direction="outbound",
                from_address=from_address,
                to_address=to_address,
                subject=subject,
                body=body,
                kind=kind,
                classification=None,
                message_id=None,
                source_key=None,
                resend_id=resend_id,
                in_reply_to_email_id=in_reply_to_email_id,
                attachment_refs=[],
                created_at=now,
            )
            conn.execute(
                sa.update(email_threads)
                .where(email_threads.c.id == thread_id)
                .values(
                    status=new_status.value,
                    next_action_at=next_action_at,
                    followups_sent=row.followups_sent + (1 if increment_followups else 0),
                    updated_at=now,
                )
            )
        return email

    # -- escalations ------------------------------------------------------------
    def insert_escalation(
        self,
        campaign_id: int | None,
        thread_id: int | None,
        reason: EscalationReason,
        details: str,
        created_at: datetime,
    ) -> Escalation:
        with self._engine.begin() as conn:
            result = conn.execute(
                sa.insert(escalations).values(
                    campaign_id=campaign_id,
                    thread_id=thread_id,
                    reason=reason.value,
                    details=details,
                    status="OPEN",
                    resolution="",
                    notified=False,
                    created_at=created_at,
                )
            )
            new_id = _pk(result)
        return Escalation(
            id=new_id,
            campaign_id=campaign_id,
            thread_id=thread_id,
            reason=reason,
            details=details,
            created_at=created_at,
        )

    def get_escalation(self, escalation_id: int) -> Escalation | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(escalations).where(escalations.c.id == escalation_id)
            ).first()
        return self._escalation(row) if row else None

    def list_escalations(
        self,
        campaign_id: int | None = None,
        status: EscalationStatus | None = None,
    ) -> list[Escalation]:
        stmt = sa.select(escalations).order_by(escalations.c.id)
        if campaign_id is not None:
            stmt = stmt.where(escalations.c.campaign_id == campaign_id)
        if status is not None:
            stmt = stmt.where(escalations.c.status == status.value)
        with self._engine.connect() as conn:
            rows = conn.execute(stmt).all()
        return [self._escalation(r) for r in rows]

    def resolve_escalation(
        self, escalation_id: int, resolution: str, resolved_at: datetime
    ) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                sa.update(escalations)
                .where(escalations.c.id == escalation_id)
                .values(status="RESOLVED", resolution=resolution, resolved_at=resolved_at)
            )

    def unnotified_escalations(self, campaign_id: int) -> list[Escalation]:
        with self._engine.connect() as conn:
            rows = conn.execute(
                sa.select(escalations)
                .where(
                    escalations.c.campaign_id == campaign_id,
                    escalations.c.notified == False,  # noqa: E712
                    escalations.c.status == "OPEN",
                )
                .order_by(escalations.c.id)
            ).all()
        return [self._escalation(r) for r in rows]

    def mark_escalations_notified(self, escalation_ids: Iterable[int]) -> None:
        ids = list(escalation_ids)
        if not ids:
            return
        with self._engine.begin() as conn:
            conn.execute(
                sa.update(escalations).where(escalations.c.id.in_(ids)).values(notified=True)
            )

    # -- spend ---------------------------------------------------------------------
    def book_fee(
        self,
        campaign_id: int,
        thread_id: int | None,
        amount_cents: int,
        budget_cents: int,
        note: str,
        created_at: datetime,
    ) -> SpendEntry | None:
        with self._engine.begin() as conn:
            # serialize concurrent bookings for one campaign on the campaign row
            conn.execute(
                sa.select(campaigns.c.id)
                .where(campaigns.c.id == campaign_id)
                .with_for_update()
            ).first()
            committed = conn.execute(
                sa.select(
                    sa.func.coalesce(sa.func.sum(spend_entries.c.amount_cents), 0)
                ).where(spend_entries.c.campaign_id == campaign_id)
            ).scalar_one()
            if int(committed) + amount_cents > budget_cents:
                return None
            result = conn.execute(
                sa.insert(spend_entries).values(
                    campaign_id=campaign_id,
                    thread_id=thread_id,
                    amount_cents=amount_cents,
                    kind="fee_authorized",
                    note=note,
                    remitted=False,
                    notified=False,
                    created_at=created_at,
                )
            )
            new_id = _pk(result)
        return SpendEntry(
            id=new_id,
            campaign_id=campaign_id,
            thread_id=thread_id,
            amount_cents=amount_cents,
            note=note,
            created_at=created_at,
        )

    def committed_total_cents(self, campaign_id: int) -> int:
        with self._engine.connect() as conn:
            total = conn.execute(
                sa.select(sa.func.coalesce(sa.func.sum(spend_entries.c.amount_cents), 0)).where(
                    spend_entries.c.campaign_id == campaign_id
                )
            ).scalar_one()
        return int(total)

    def list_spend(self, campaign_id: int) -> list[SpendEntry]:
        with self._engine.connect() as conn:
            rows = conn.execute(
                sa.select(spend_entries)
                .where(spend_entries.c.campaign_id == campaign_id)
                .order_by(spend_entries.c.id)
            ).all()
        return [self._spend(r) for r in rows]

    def unnotified_spend(self, campaign_id: int) -> list[SpendEntry]:
        with self._engine.connect() as conn:
            rows = conn.execute(
                sa.select(spend_entries)
                .where(
                    spend_entries.c.campaign_id == campaign_id,
                    spend_entries.c.notified == False,  # noqa: E712
                )
                .order_by(spend_entries.c.id)
            ).all()
        return [self._spend(r) for r in rows]

    def mark_spend_notified(self, spend_ids: Iterable[int]) -> None:
        ids = list(spend_ids)
        if not ids:
            return
        with self._engine.begin() as conn:
            conn.execute(
                sa.update(spend_entries).where(spend_entries.c.id.in_(ids)).values(notified=True)
            )

    # -- campaign kill (FK-ordered) ---------------------------------------------
    def purge_campaign(self, campaign_id: int) -> dict[str, int]:
        counts: dict[str, int] = {}
        with self._engine.begin() as conn:
            thread_ids = [
                r.id
                for r in conn.execute(
                    sa.select(email_threads.c.id).where(
                        email_threads.c.campaign_id == campaign_id
                    )
                ).all()
            ]
            counts["spend_entries"] = conn.execute(
                sa.delete(spend_entries).where(spend_entries.c.campaign_id == campaign_id)
            ).rowcount
            counts["escalations"] = conn.execute(
                sa.delete(escalations).where(escalations.c.campaign_id == campaign_id)
            ).rowcount
            email_filter = emails.c.campaign_id == campaign_id
            if thread_ids:
                email_filter = sa.or_(email_filter, emails.c.thread_id.in_(thread_ids))
            counts["emails"] = conn.execute(sa.delete(emails).where(email_filter)).rowcount
            counts["threads"] = conn.execute(
                sa.delete(email_threads).where(email_threads.c.campaign_id == campaign_id)
            ).rowcount
            counts["search_targets"] = conn.execute(
                sa.delete(search_targets).where(search_targets.c.campaign_id == campaign_id)
            ).rowcount
            counts["campaigns"] = conn.execute(
                sa.delete(campaigns).where(campaigns.c.id == campaign_id)
            ).rowcount
        return counts
