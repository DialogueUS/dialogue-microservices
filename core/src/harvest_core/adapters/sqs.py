"""boto3 SQS adapter: long poll 20 s, ChangeMessageVisibility heartbeats."""

from __future__ import annotations

from typing import Any

from ..constants import LONG_POLL_WAIT_S
from ..ports import QueueMessage


class SqsQueue:
    def __init__(self, client: Any, queue_url: str, wait_seconds: int = LONG_POLL_WAIT_S) -> None:
        self._client = client
        self._queue_url = queue_url
        self._wait_seconds = wait_seconds

    def send(self, body: str) -> None:
        self._client.send_message(QueueUrl=self._queue_url, MessageBody=body)

    def receive(self, max_messages: int) -> list[QueueMessage]:
        resp = self._client.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=min(max_messages, 10),
            WaitTimeSeconds=self._wait_seconds,
            AttributeNames=["ApproximateReceiveCount"],
        )
        out = []
        for m in resp.get("Messages", []):
            count = int(m.get("Attributes", {}).get("ApproximateReceiveCount", "1"))
            out.append(QueueMessage(id=m["ReceiptHandle"], body=m["Body"], receive_count=count))
        return out

    def delete(self, message_id: str) -> None:
        self._client.delete_message(QueueUrl=self._queue_url, ReceiptHandle=message_id)

    def change_visibility(self, message_id: str, timeout_seconds: float) -> None:
        self._client.change_message_visibility(
            QueueUrl=self._queue_url,
            ReceiptHandle=message_id,
            VisibilityTimeout=int(timeout_seconds),
        )
