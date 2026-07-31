"""The three SQS message types (spec §3), with JSON round-tripping.

The query text lives in the sweep message and only there — losing a
message loses nothing that cannot be regenerated.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter


class SweepTask(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["sweep"] = "sweep"
    corpus: str
    sweep_target_id: int
    jurisdiction_id: int
    topic: str
    query_text: str
    dispatch_id: str
    query_seq: int
    query_count: int
    dispatched_at: datetime


class CodeTask(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["code"] = "code"
    corpus: str
    sweep_target_id: int
    jurisdiction_id: int
    portal_urls: list[str]
    dispatch_id: str
    dispatched_at: datetime


class FetchTask(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["fetch"] = "fetch"
    corpus: str
    artifact_id: int
    dispatch_id: str
    dispatched_at: datetime


Task = SweepTask | CodeTask | FetchTask

_task_adapter: TypeAdapter[Task] = TypeAdapter(
    Annotated[Task, Field(discriminator="kind")]
)


def to_json(task: Task) -> str:
    return task.model_dump_json()


def parse_task(body: str) -> Task:
    return _task_adapter.validate_json(body)
