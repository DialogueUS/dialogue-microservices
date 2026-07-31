"""Harvest config surface (spec §9), with spec defaults and a YAML loader."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

VALID_LEVELS = ("federal", "state", "county", "city")


class ScopeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    levels: list[str] = Field(default_factory=lambda: ["county"])
    states: list[str] = Field(default_factory=lambda: ["ALL"])
    within: list[str] = Field(default_factory=list)
    only: list[str] = Field(default_factory=list)
    region_query: str | None = None

    @field_validator("levels")
    @classmethod
    def _check_levels(cls, v: list[str]) -> list[str]:
        for level in v:
            if level not in VALID_LEVELS:
                raise ValueError(f"invalid scope level {level!r}; must be one of {VALID_LEVELS}")
        return v

    @field_validator("states")
    @classmethod
    def _check_states(cls, v: list[str]) -> list[str]:
        for s in v:
            if s != "ALL" and not (len(s) == 2 and s.isalpha() and s.isupper()):
                raise ValueError(
                    f"invalid scope state {s!r}; must be a two-letter code or 'ALL'"
                )
        return v


class LLMConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str = "openai"
    model: str = "gpt-5.6-luna"
    api_key_env: str = "OPENAI_API_KEY"


class HarvestConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["harvest"]
    name: str
    scope: ScopeConfig = Field(default_factory=ScopeConfig)
    topics: list[str] = Field(
        default_factory=lambda: ["regulations ordinances municipal code"]
    )
    resweep_interval_days: int = 30
    search_count: int = 20
    queries_per_jurisdiction: int = 3
    max_sweeps_per_dispatch: int = 25
    triage_batch_max_results: int = 200
    max_fetch_redispatch: int = 500
    code_max_pages: int = 200
    llm: LLMConfig = Field(default_factory=LLMConfig)
    serper_api_key_env: str = "SERPER_API_KEY"
    redis_url_env: str = "REDIS_URL"
    # Dormant until catalog lands (carried forward with the same meaning).
    dry_run: bool = True
    min_confidence: float = 0.7

    @field_validator("name")
    @classmethod
    def _check_name(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("name must be non-empty")
        return v


def load_config(path: str | Path) -> HarvestConfig:
    """Load and validate a harvest config from a YAML file."""
    raw = yaml.safe_load(Path(path).read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"config file {path} did not parse to a mapping")
    return HarvestConfig.model_validate(raw)
