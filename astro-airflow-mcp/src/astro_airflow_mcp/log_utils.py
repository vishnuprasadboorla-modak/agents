"""Helpers for turning raw Airflow task logs into compact, agent-friendly text.

Airflow returns logs in two very different shapes:

* 3.x: a list of structured JSON events (``timestamp``/``level``/``event`` and,
  for exceptions, ``error_detail`` with every stack frame).
* 2.x: one string, sometimes the ``repr`` of ``[(host, text)]`` tuples.

Both are flattened to plain text lines here so callers can tail, cap and scan
them without flooding the model context.
"""

from __future__ import annotations

import ast
import re
from typing import Any

DEFAULT_TAIL_LINES = 200
MAX_LOG_CHARS = 20_000
# How many innermost stack frames to keep when rendering a structured exception.
_FRAMES_KEPT = 3

_ERROR_LINE = re.compile(r"^\s*(?:[\w.]+\.)?\w*(?:Error|Exception|Exit|Interrupt)\b.*")


def _render_error_detail(detail: Any) -> list[str]:
    lines: list[str] = []
    if not isinstance(detail, list):
        return lines
    for exc in detail:
        if not isinstance(exc, dict):
            continue
        frames = exc.get("frames") or []
        for frame in frames[-_FRAMES_KEPT:]:
            lines.append(
                f'  File "{frame.get("filename")}", line {frame.get("lineno")}, '
                f"in {frame.get('name')}"
            )
        exc_type = exc.get("exc_type") or "Exception"
        value = exc.get("exc_value", "")
        lines.append(f"{exc_type}: {value}" if value else str(exc_type))
    return lines


def _event_to_lines(event: Any) -> list[str]:
    if not isinstance(event, dict):
        return str(event).splitlines()
    text = str(event.get("event", ""))
    if text.startswith(("::group::", "::endgroup::")):
        return []
    prefix = " ".join(p for p in (event.get("timestamp"), (event.get("level") or "").upper()) if p)
    head = f"{prefix} {text}".strip()
    return [head, *_render_error_detail(event.get("error_detail"))]


def _unwrap_v2_string(content: str) -> str:
    """Airflow 2.x may return ``repr([(host, text), ...])``; unwrap it to text."""
    stripped = content.lstrip()
    if stripped.startswith("[(") and stripped.rstrip().endswith(")]"):
        try:
            parsed = ast.literal_eval(stripped)
        except (ValueError, SyntaxError):
            return content
        if isinstance(parsed, list) and all(isinstance(p, tuple) and len(p) == 2 for p in parsed):
            return "\n".join(str(text) for _, text in parsed)
    return content


def log_lines(content: Any) -> list[str]:
    """Flatten any Airflow log ``content`` value into a list of text lines."""
    if content is None:
        return []
    if isinstance(content, list):
        lines: list[str] = []
        for event in content:
            lines.extend(_event_to_lines(event))
        return lines
    return _unwrap_v2_string(str(content)).splitlines()


def last_error_line(lines: list[str]) -> str | None:
    """Return the most informative error line (last exception line, else last ERROR line)."""
    for line in reversed(lines):
        if _ERROR_LINE.match(line) and ":" in line and not line.lstrip().startswith("File "):
            return line.strip()
    for line in reversed(lines):
        if " ERROR " in f" {line} " or "CRITICAL" in line:
            return line.strip()
    return None


def shape_task_log(
    data: dict[str, Any],
    *,
    tail_lines: int | None = DEFAULT_TAIL_LINES,
    max_chars: int = MAX_LOG_CHARS,
) -> dict[str, Any]:
    """Convert an adapter log response into bounded plain text plus metadata.

    ``tail_lines`` keeps only the last N lines (``None``/``0`` keeps all);
    ``max_chars`` is a final hard cap that also keeps the end of the log.
    Responses without a ``content`` key (e.g. "not available" notes) pass through.
    """
    if "content" not in data:
        return data
    lines = log_lines(data.get("content"))
    total = len(lines)
    truncated = False
    if tail_lines and tail_lines > 0 and total > tail_lines:
        lines = lines[-tail_lines:]
        truncated = True
    text = "\n".join(lines)
    if max_chars and len(text) > max_chars:
        text = text[-max_chars:]
        truncated = True
    result: dict[str, Any] = {
        "content": text,
        "total_lines": total,
        "returned_lines": len(text.splitlines()),
        "truncated": truncated,
    }
    if truncated:
        result["note"] = (
            "Log truncated to the most recent lines. Increase tail_lines "
            "(0 = everything) to see more."
        )
    if data.get("continuation_token"):
        result["continuation_token"] = data["continuation_token"]
    return result
