"""Mask anything that looks like a secret before it reaches the LLM or Slack."""
from __future__ import annotations

import re
from typing import Any

MASK = "***"

_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"xox[abposr]-[A-Za-z0-9-]{10,}"), MASK),           # Slack bot/user tokens
    (re.compile(r"xapp-[A-Za-z0-9-]{10,}"), MASK),                  # Slack app tokens
    (re.compile(r"gsk_[A-Za-z0-9]{20,}"), MASK),                    # Groq
    (re.compile(r"sk-[A-Za-z0-9_-]{20,}"), MASK),                   # OpenAI-style
    (re.compile(r"ps_[A-Za-z0-9_-]{20,}"), MASK),                   # Pulse
    (re.compile(r"AIza[0-9A-Za-z_-]{35}"), MASK),                   # Google
    (re.compile(r"AKIA[0-9A-Z]{16}"), MASK),                        # AWS access key id
    (re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]{8,}"), r"\1" + MASK),
    # user:password@host in connection URIs
    (re.compile(r"([a-z][a-z0-9+.-]*://[^:/@\s]+:)[^@\s]+(@)", re.I), r"\1" + MASK + r"\2"),
    # key=value / key: value for secret-looking keys
    (re.compile(r"(?i)\b((?:password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key)\b[\"']?\s*[=:]\s*[\"']?)"
                r"[^\s\"',;]+"), r"\1" + MASK),
]


def mask_secrets(text: str, extra_values: list[str] | None = None) -> str:
    if not text:
        return text
    for value in extra_values or []:
        if value:
            text = text.replace(value, MASK)
    for pattern, repl in _PATTERNS:
        text = pattern.sub(repl, text)
    return text


def mask_obj(obj: Any, extra_values: list[str] | None = None) -> Any:
    """Recursively mask strings inside dicts/lists."""
    if isinstance(obj, str):
        return mask_secrets(obj, extra_values)
    if isinstance(obj, dict):
        return {k: mask_obj(v, extra_values) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [mask_obj(v, extra_values) for v in obj]
    return obj
