"""Fetch-task reconciliation, backstop only (plan 2.5, spec §5.3).

Re-publishes any pending artifact whose dispatch stamp is null or older
than the dispatch timeout — a sweep worker that crashed after staging
but before publishing, or a fetch message that died in the DLQ.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime

from harvest_core.config import HarvestConfig
from harvest_core.constants import DISPATCH_TIMEOUT_S
from harvest_core.messages import FetchTask, to_json
from harvest_core.ports import Datastore, TaskQueue

log = logging.getLogger(__name__)


def reconcile_fetch(
    ds: Datastore, fetch_queue: TaskQueue, config: HarvestConfig, now: datetime
) -> int:
    stale = ds.select_pending_stale(
        config.name, now, DISPATCH_TIMEOUT_S, config.max_fetch_redispatch
    )
    republished = 0
    for artifact in stale:
        dispatch_id = str(uuid.uuid4())
        ds.stamp_artifact(artifact.id, now, dispatch_id)
        try:
            fetch_queue.send(
                to_json(
                    FetchTask(
                        corpus=config.name,
                        artifact_id=artifact.id,
                        dispatch_id=dispatch_id,
                        dispatched_at=now,
                    )
                )
            )
        except Exception:
            log.exception("fetch re-publish failed for artifact %s", artifact.id)
            continue  # stamped row goes stale again and is retried
        republished += 1
    return republished
