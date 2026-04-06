"""Parse EML files into structural elements."""

from __future__ import annotations

import email
from email import policy
from pathlib import Path

from .elements import Element, KVPairElement, TextElement


def parse_eml(path: Path) -> list[Element]:
    raw = path.read_text(encoding="utf-8", errors="replace")
    msg = email.message_from_string(raw, policy=policy.default)
    elements: list[Element] = []

    header_fields = ["From", "To", "Cc", "Subject", "Date"]
    for field_name in header_fields:
        value = msg.get(field_name)
        if value:
            elements.append(
                KVPairElement(key=field_name, value=str(value), is_email_header=True)
            )

    body = msg.get_body(preferencelist=("plain", "html"))
    if body:
        content = body.get_content()
        if isinstance(content, str):
            text = content.strip()
            if text:
                elements.append(TextElement(text=text))

    return elements
