"""The §11 per-campaign config surface, with registration-time validation.

A campaign row is created from this model at registration; the row is
the system of record afterwards.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .constants import ANON_BLOCKED_STATES

DEFAULT_LEGAL_BASIS = (
    "state public records law and, where applicable, "
    "the federal Freedom of Information Act"
)

_US_STATES = frozenset(
    "AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO "
    "MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC".split()
)
_LEVELS = frozenset({"state", "county", "city"})


class RequesterConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    email: str
    organization: str | None = None
    phone: str | None = None
    mailing_address: str | None = None
    anonymous: bool = True
    consent_confirmed: bool = False


class ScopeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    levels: list[str] = Field(default_factory=lambda: ["county"])
    states: list[str] = Field(default_factory=lambda: ["ALL"])
    only: list[str] | None = None  # allow-list of jurisdiction names

    @model_validator(mode="after")
    def _validate(self) -> ScopeConfig:
        for level in self.levels:
            if level not in _LEVELS:
                raise ValueError(f"unknown scope level {level!r}")
        for state in self.states:
            if state != "ALL" and state not in _US_STATES:
                raise ValueError(f"unknown state {state!r}")
        return self


class LimitsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_concurrent_sends: int = 8
    per_office_cooldown_days: int = 7
    max_followups: int = 3
    followup_interval_days: int = 10
    daily_send_cap: int = 200
    fee_budget_usd: float = 0.0  # 0 = every fee escalates

    @property
    def fee_budget_cents(self) -> int:
        return round(self.fee_budget_usd * 100)


class ContactsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min_confidence: float = 0.6


class CampaignConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str  # unique slug
    record_type: str
    record_description: str
    legal_basis: str = DEFAULT_LEGAL_BASIS
    requester: RequesterConfig
    scope: ScopeConfig = Field(default_factory=ScopeConfig)
    limits: LimitsConfig = Field(default_factory=LimitsConfig)
    contacts: ContactsConfig = Field(default_factory=ContactsConfig)
    dry_run: bool = True  # start here
    notify_email: str | None = None

    @model_validator(mode="after")
    def _validate(self) -> CampaignConfig:
        if not self.name.strip():
            raise ValueError("campaign name is required")
        if not self.record_type.strip() or not self.record_description.strip():
            raise ValueError("record_type and record_description are required")
        # Registration-time refusal of the anonymous x identity-state combination
        # (spec §7.1): reject scopes that *explicitly* name a blocked state.
        # scope.states == [ALL] passes and relies on the per-target sender guard.
        if self.requester.anonymous:
            named = {s for s in self.scope.states if s != "ALL"}
            blocked = sorted(named & ANON_BLOCKED_STATES)
            if blocked:
                raise ValueError(
                    "anonymous requests are refused in "
                    f"{', '.join(blocked)} (requester identity required); "
                    "set requester.anonymous=false or drop these states"
                )
        return self


def load_campaign_config(path: str | Path) -> CampaignConfig:
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    return CampaignConfig.model_validate(raw)
