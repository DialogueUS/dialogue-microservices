"""FakeEmailTransport: records every payload, programmable failure."""

from __future__ import annotations

import itertools
import threading

from ..errors import SendTransientError
from ..ports import OutboundEmail


class FakeEmailTransport:
    def __init__(self) -> None:
        self.sent: list[OutboundEmail] = []
        self.fail_next: int = 0  # raise SendTransientError for the next N sends
        self.fail_to_addresses: set[str] = set()
        self._ids = itertools.count(1)
        self._lock = threading.Lock()

    def send(self, email: OutboundEmail) -> str:
        with self._lock:
            if self.fail_next > 0:
                self.fail_next -= 1
                raise SendTransientError("canned transport failure")
            if email.to_address in self.fail_to_addresses:
                raise SendTransientError(f"canned failure for {email.to_address}")
            self.sent.append(email)
            return f"resend-{next(self._ids)}"
