from pathlib import Path

import pytest
from harvest_core.config import HarvestConfig, load_config
from pydantic import ValidationError


def _write(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(text)
    return p


def test_minimal_yaml_yields_every_spec_default(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, "mode: harvest\nname: test-corpus\n"))
    assert cfg.mode == "harvest"
    assert cfg.name == "test-corpus"
    assert cfg.scope.levels == ["county"]
    assert cfg.scope.states == ["ALL"]
    assert cfg.scope.within == []
    assert cfg.scope.only == []
    assert cfg.scope.region_query is None
    assert cfg.topics == ["regulations ordinances municipal code"]
    assert cfg.resweep_interval_days == 30
    assert cfg.search_count == 20
    assert cfg.queries_per_jurisdiction == 3
    assert cfg.max_sweeps_per_dispatch == 25
    assert cfg.triage_batch_max_results == 200
    assert cfg.max_fetch_redispatch == 500
    assert cfg.code_max_pages == 200
    assert cfg.llm.provider == "openai"
    assert cfg.llm.model == "gpt-5.6-luna"
    assert cfg.llm.api_key_env == "OPENAI_API_KEY"
    assert cfg.serper_api_key_env == "SERPER_API_KEY"
    assert cfg.redis_url_env == "REDIS_URL"
    assert cfg.dry_run is True
    assert cfg.min_confidence == 0.7


def test_full_yaml_round_trips(tmp_path: Path) -> None:
    full = """
mode: harvest
name: nuisance-regs
scope:
  levels: [state, county, city]
  states: [CA, NY]
  within: [Los Angeles County]
  only: [Pasadena]
  region_query: southern california
topics: [nuisance ordinances, noise regulations]
resweep_interval_days: 14
search_count: 10
queries_per_jurisdiction: 2
max_sweeps_per_dispatch: 5
triage_batch_max_results: 50
max_fetch_redispatch: 100
code_max_pages: 20
llm:
  provider: openai
  model: gpt-5.6-luna
  api_key_env: MY_OPENAI_KEY
serper_api_key_env: MY_SERPER_KEY
redis_url_env: MY_REDIS_URL
dry_run: false
min_confidence: 0.9
"""
    cfg = load_config(_write(tmp_path, full))
    rehydrated = HarvestConfig.model_validate(cfg.model_dump())
    assert rehydrated == cfg
    assert rehydrated.scope.states == ["CA", "NY"]
    assert rehydrated.llm.api_key_env == "MY_OPENAI_KEY"


def test_non_harvest_mode_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, "mode: campaign\nname: x\n"))


@pytest.mark.parametrize(
    "scope_yaml",
    [
        "scope:\n  levels: [township]\n",
        "scope:\n  states: [California]\n",
        "scope:\n  states: [ca]\n",
    ],
)
def test_bad_scope_values_rejected(tmp_path: Path, scope_yaml: str) -> None:
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, f"mode: harvest\nname: x\n{scope_yaml}"))
