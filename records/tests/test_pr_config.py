from pathlib import Path
from typing import Any

import pytest
from public_records.config import DEFAULT_LEGAL_BASIS, CampaignConfig, load_campaign_config
from pydantic import ValidationError

MINIMAL: dict[str, Any] = {
    "name": "noise-2026",
    "record_type": "noise complaints",
    "record_description": "All noise complaints filed in 2025.",
    "requester": {"name": "Ada Requester", "email": "ada@example.org"},
}


def test_minimal_config_yields_every_default() -> None:
    cfg = CampaignConfig.model_validate(MINIMAL)
    assert cfg.legal_basis == DEFAULT_LEGAL_BASIS
    assert cfg.requester.anonymous is True
    assert cfg.requester.consent_confirmed is False  # the master gate
    assert cfg.scope.levels == ["county"]
    assert cfg.scope.states == ["ALL"]
    assert cfg.scope.only is None
    assert cfg.dry_run is True
    assert cfg.limits.max_concurrent_sends == 8
    assert cfg.limits.per_office_cooldown_days == 7
    assert cfg.limits.max_followups == 3
    assert cfg.limits.followup_interval_days == 10
    assert cfg.limits.daily_send_cap == 200
    assert cfg.limits.fee_budget_usd == 0.0
    assert cfg.limits.fee_budget_cents == 0
    assert cfg.contacts.min_confidence == 0.6
    assert cfg.notify_email is None


def test_full_config_round_trips(tmp_path: Path) -> None:
    full = {
        **MINIMAL,
        "legal_basis": "CPRA only",
        "requester": {
            "name": "Ada",
            "email": "ada@example.org",
            "organization": "Example Org",
            "phone": "555-0100",
            "mailing_address": "1 Main St",
            "anonymous": False,
            "consent_confirmed": True,
        },
        "scope": {"levels": ["city", "county"], "states": ["CA", "OR"], "only": ["Pasadena"]},
        "limits": {
            "max_concurrent_sends": 2,
            "per_office_cooldown_days": 3,
            "max_followups": 1,
            "followup_interval_days": 5,
            "daily_send_cap": 10,
            "fee_budget_usd": 25.5,
        },
        "contacts": {"min_confidence": 0.8},
        "dry_run": False,
        "notify_email": "ops@example.org",
    }
    cfg = CampaignConfig.model_validate(full)
    assert CampaignConfig.model_validate(cfg.model_dump()) == cfg
    assert cfg.limits.fee_budget_cents == 2550

    path = tmp_path / "campaign.yaml"
    import yaml

    path.write_text(yaml.safe_dump(full))
    assert load_campaign_config(path) == cfg


def test_missing_required_fields_rejected() -> None:
    with pytest.raises(ValidationError):
        CampaignConfig.model_validate({**MINIMAL, "record_type": "  "})
    with pytest.raises(ValidationError):
        CampaignConfig.model_validate({"name": "x", "record_type": "t", "record_description": "d"})


def test_bad_scope_values_rejected() -> None:
    with pytest.raises(ValidationError):
        CampaignConfig.model_validate({**MINIMAL, "scope": {"levels": ["galaxy"]}})
    with pytest.raises(ValidationError):
        CampaignConfig.model_validate({**MINIMAL, "scope": {"states": ["ZZ"]}})


def test_test_contacts_shape_and_defaults() -> None:
    assert CampaignConfig.model_validate(MINIMAL).is_test is False
    cfg = CampaignConfig.model_validate({
        **MINIMAL,
        "test_contacts": [{"jurisdiction": "Kern County", "state": "CA",
                           "email": "inbox@example.test"}],
    })
    assert cfg.is_test is True
    assert cfg.test_contacts[0].level == "county"

    for bad in (
        {"jurisdiction": "Kern County", "state": "ZZ", "email": "a@b"},
        {"jurisdiction": "Kern County", "state": "CA", "level": "galaxy", "email": "a@b"},
        {"jurisdiction": " ", "state": "CA", "email": "a@b"},
        {"jurisdiction": "Kern County", "state": "CA"},  # no email
    ):
        with pytest.raises(ValidationError):
            CampaignConfig.model_validate({**MINIMAL, "test_contacts": [bad]})

    with pytest.raises(ValidationError, match="same jurisdiction"):
        CampaignConfig.model_validate({**MINIMAL, "test_contacts": [
            {"jurisdiction": "Kern County", "state": "CA", "email": "a@b"},
            {"jurisdiction": "kern county", "state": "CA", "email": "c@d"},
        ]})


def test_anonymous_blocked_state_covers_test_contacts() -> None:
    # scope defaults to [ALL], so only the test contact names the state
    with pytest.raises(ValidationError, match="TN"):
        CampaignConfig.model_validate({**MINIMAL, "test_contacts": [
            {"jurisdiction": "Davidson County", "state": "TN", "email": "a@b"},
        ]})


def test_anonymous_with_blocked_state_scope_rejected() -> None:
    with pytest.raises(ValidationError, match="TN"):
        CampaignConfig.model_validate({**MINIMAL, "scope": {"states": ["CA", "TN"]}})
    # naming a blocked state is fine for an identified requester
    identified = {
        **MINIMAL,
        "requester": {**MINIMAL["requester"], "anonymous": False},
        "scope": {"states": ["TN"]},
    }
    CampaignConfig.model_validate(identified)
    # states=[ALL] passes; the per-target sender guard is the backstop
    CampaignConfig.model_validate(MINIMAL)
