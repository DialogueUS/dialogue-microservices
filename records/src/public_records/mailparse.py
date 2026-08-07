"""MIME parsing for the mail poller (stdlib email, pure computation)."""

from __future__ import annotations

from dataclasses import dataclass, field
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser


@dataclass
class ParsedAttachment:
    filename: str
    content_type: str
    payload: bytes


@dataclass
class ParsedMail:
    message_id: str | None
    from_address: str
    subject: str
    body: str
    headers: dict[str, str]
    attachments: list[ParsedAttachment] = field(default_factory=list)


def _address_only(value: str) -> str:
    """`Clerk <clerk@x.gov>` -> `clerk@x.gov`."""
    if "<" in value and ">" in value:
        return value.split("<", 1)[1].split(">", 1)[0].strip()
    return value.strip()


def parse_mime(raw: bytes) -> ParsedMail:
    msg = BytesParser(policy=policy.default).parsebytes(raw)
    assert isinstance(msg, EmailMessage)

    body = ""
    body_part = msg.get_body(preferencelist=("plain", "html"))
    if body_part is not None:
        content = body_part.get_content()
        body = content if isinstance(content, str) else ""

    attachments: list[ParsedAttachment] = []
    for part in msg.iter_attachments():
        payload = part.get_payload(decode=True)
        if not isinstance(payload, bytes):
            continue
        attachments.append(
            ParsedAttachment(
                filename=part.get_filename() or "attachment",
                content_type=part.get_content_type(),
                payload=payload,
            )
        )

    message_id = msg.get("Message-ID")
    return ParsedMail(
        message_id=message_id.strip() if message_id else None,
        from_address=_address_only(str(msg.get("From", ""))),
        subject=str(msg.get("Subject", "")),
        body=body,
        headers={name: str(value) for name, value in msg.items()},
        attachments=attachments,
    )
