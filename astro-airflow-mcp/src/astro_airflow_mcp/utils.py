"""Shared utility functions for CLI and MCP server."""

import re
from collections import Counter
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from astro_airflow_mcp.constants import DEFAULT_LIMIT, FAILED_TASK_STATES


def normalize_airflow_url(url: str) -> str:
    """Strip query string, fragment, and trailing slash from an Airflow base URL.

    API URLs are built by concatenation (eg ``f"{airflow_url}/api/v2/version"``),
    so a stored URL like ``https://host/dep?orgId=foo`` produces the malformed
    ``https://host/dep?orgId=foo/api/v2/version`` — the path stays at ``/dep``
    and ``/api/...`` ends up inside the query string. Normalizing once at the
    boundary keeps every downstream call safe.

    Any userinfo (``user:password@``) is also dropped: credentials are supplied
    separately via the token/basic-auth getters, and the normalized URL can end
    up in logs and in structured tool errors surfaced to the model.
    """
    if not url:
        return url
    parts = urlsplit(url)
    path = parts.path.rstrip("/")
    # Strip any ``user:password@`` userinfo while preserving host[:port] (and
    # IPv6 brackets) verbatim.
    netloc = parts.netloc.rsplit("@", 1)[-1]
    return urlunsplit((parts.scheme, netloc, path, "", ""))


def filter_connection_passwords(connections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter sensitive fields from connections.

    Returns connection metadata with passwords excluded for security.

    Args:
        connections: List of connection dicts from the Airflow API

    Returns:
        List of connections with only safe fields included
    """
    return [
        {
            "connection_id": conn.get("connection_id"),
            "conn_type": conn.get("conn_type"),
            "description": conn.get("description"),
            "host": conn.get("host"),
            "port": conn.get("port"),
            "schema": conn.get("schema"),
            "login": conn.get("login"),
            "extra": conn.get("extra"),
        }
        for conn in connections
    ]


def extract_failed_tasks(task_instances: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract failed task info from task instances.

    Args:
        task_instances: List of task instance dicts from the Airflow API

    Returns:
        List of failed task details with relevant fields
    """
    return [
        {
            "task_id": task.get("task_id"),
            "map_index": task.get("map_index"),
            "state": task.get("state"),
            "try_number": task.get("try_number"),
            "start_date": task.get("start_date"),
            "end_date": task.get("end_date"),
        }
        for task in task_instances
        if task.get("state") in FAILED_TASK_STATES
    ]


def summarize_task_instances(task_instances: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize every instance, retaining only a bounded sample of full details.

    Counts and compact failed-task details cover the complete listing. The
    sample prioritizes failed, then upstream_failed instances before other
    states. MCP and CLI diagnostics share this format.

    ``failed_tasks`` holds only instances that failed on their own (root
    causes). Instances skipped because an upstream task failed are reported
    separately under ``upstream_failed_tasks`` so they are not mistaken for
    independent failures.
    """
    state_order = {"failed": 0, "upstream_failed": 1}
    sample = sorted(task_instances, key=lambda task: state_order.get(task.get("state"), 2))[
        :DEFAULT_LIMIT
    ]
    all_failed = extract_failed_tasks(task_instances)
    return {
        "task_instances": sample,
        "task_instances_returned": len(sample),
        "task_instances_truncated": len(sample) < len(task_instances),
        "summary": {
            "total_tasks": len(task_instances),
            "state_counts": dict(
                Counter(task.get("state") or "unknown" for task in task_instances)
            ),
            "failed_tasks": [t for t in all_failed if t.get("state") == "failed"],
            "upstream_failed_tasks": [t for t in all_failed if t.get("state") == "upstream_failed"],
        },
    }


# Substrings that mark a config option as sensitive (matched case-insensitively
# against the option key).
_SENSITIVE_KEY_MARKERS = (
    "secret",
    "password",
    "passwd",
    "token",
    "fernet",
    "api_key",
    "apikey",
    "private_key",
    "credential",
    "sql_alchemy_conn",
    "result_backend",
    "broker_url",
)
_SECRET_KEY_NAMES = {"secret_key", "jwt_secret", "fernet_key"}
REDACTED = "***REDACTED***"


def _is_sensitive_key(key: str) -> bool:
    lowered = key.lower()
    return lowered in _SECRET_KEY_NAMES or any(m in lowered for m in _SENSITIVE_KEY_MARKERS)


def _redact_url_credentials(value: str) -> str:
    """Mask ``user:password@`` userinfo in a URL-like value."""
    return re.sub(r"(://[^/:@\s]*:)[^@/\s]+(@)", rf"\1{REDACTED}\2", value)


def redact_config(data: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of an Airflow config payload with sensitive values masked.

    Handles both shapes the API can return: ``sections -> options -> key/value``
    and nested mappings. Values of sensitive keys are replaced with a fixed
    marker and URL passwords embedded in any value are masked. The server does
    not rely on Airflow's own ``expose_config`` setting to hide secrets.
    """

    def _redact(node: Any) -> Any:
        if isinstance(node, list):
            return [_redact(item) for item in node]
        if isinstance(node, dict):
            out = {k: _redact(v) for k, v in node.items()}
            key = node.get("key")
            if isinstance(key, str) and "value" in node and _is_sensitive_key(key):
                if node["value"] not in (None, ""):
                    out["value"] = REDACTED
            elif isinstance(out.get("value"), str):
                out["value"] = _redact_url_credentials(out["value"])
            return out
        return node

    return _redact(data)


def wrap_list_response(
    items: list[dict[str, Any]], key_name: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Wrap API list response with pagination metadata.

    Args:
        items: List of items from the API
        key_name: Name for the items key in response (e.g., 'dags', 'dag_runs')
        data: Original API response data (for total_entries)

    Returns:
        Dict with pagination metadata
    """
    total_entries = data.get("total_entries", len(items))
    return {
        f"total_{key_name}": total_entries,
        "returned_count": len(items),
        key_name: items,
    }
