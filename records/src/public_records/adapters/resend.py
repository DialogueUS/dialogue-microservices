"""Resend API transport over httpx (30 s timeout).

The dry-run short-circuit lives in the sender, not here: under dry_run
this adapter is never called.
"""

from __future__ import annotations

import httpx

from ..constants import RESEND_TIMEOUT_S
from ..errors import SendTransientError
from ..ports import OutboundEmail

RESEND_API_URL = "https://api.resend.com/emails"


class ResendTransport:
    def __init__(self, api_key: str, client: httpx.Client | None = None) -> None:
        self._api_key = api_key
        self._client = client or httpx.Client(timeout=RESEND_TIMEOUT_S)

    def send(self, email: OutboundEmail) -> str:
        payload = {
            "from": email.from_address,
            "to": [email.to_address],
            "subject": email.subject,
            "text": email.body,
            "headers": dict(email.headers),
        }
        try:
            response = self._client.post(
                RESEND_API_URL,
                json=payload,
                headers={"Authorization": f"Bearer {self._api_key}"},
                timeout=RESEND_TIMEOUT_S,
            )
        except httpx.HTTPError as exc:
            raise SendTransientError(str(exc)) from exc
        if response.status_code == 429 or response.status_code >= 500:
            raise SendTransientError(f"resend {response.status_code}: {response.text[:200]}")
        if response.status_code >= 400:
            # 4xx other than 429 is a permanent payload problem; retrying the
            # same message can't fix it — raise the same transient type so the
            # DLQ redrive converts persistent failure into an escalation.
            raise SendTransientError(f"resend {response.status_code}: {response.text[:200]}")
        data = response.json()
        message_id = str(data.get("id", ""))
        if not message_id:
            raise SendTransientError(f"resend response missing id: {data!r}")
        return message_id
