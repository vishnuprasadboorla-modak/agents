"""Diagnostic tools - warnings, errors, explore, diagnose, system health."""

import json
from typing import Any

from astro_airflow_mcp.constants import DEFAULT_LIMIT, DEFAULT_OFFSET
from astro_airflow_mcp.log_utils import last_error_line, log_lines
from astro_airflow_mcp.server import (
    _get_adapter,
    _wrap_list_response,
    mcp,
)
from astro_airflow_mcp.tool_annotations import read_only
from astro_airflow_mcp.tool_errors import error_payload, tool_error
from astro_airflow_mcp.utils import summarize_task_instances


def _list_dag_warnings_impl(
    limit: int = DEFAULT_LIMIT,
    offset: int = DEFAULT_OFFSET,
) -> str:
    """Internal implementation for listing DAG warnings from Airflow.

    Args:
        limit: Maximum number of warnings to return (default: 100)
        offset: Offset for pagination (default: 0)

    Returns:
        JSON string containing the list of DAG warnings
    """
    try:
        adapter = _get_adapter()
        data = adapter.list_dag_warnings(limit=limit, offset=offset)

        if "dag_warnings" in data:
            return _wrap_list_response(data["dag_warnings"], "dag_warnings", data)
        return f"No DAG warnings found. Response: {data}"
    except Exception as e:
        return tool_error(e)


def _list_import_errors_impl(
    limit: int = DEFAULT_LIMIT,
    offset: int = DEFAULT_OFFSET,
) -> str:
    """Internal implementation for listing import errors from Airflow.

    Args:
        limit: Maximum number of import errors to return (default: 100)
        offset: Offset for pagination (default: 0)

    Returns:
        JSON string containing the list of import errors
    """
    try:
        adapter = _get_adapter()
        data = adapter.list_import_errors(limit=limit, offset=offset)

        if "import_errors" in data:
            return _wrap_list_response(data["import_errors"], "import_errors", data)
        return f"No import errors found. Response: {data}"
    except Exception as e:
        return tool_error(e)


@mcp.tool(annotations=read_only())
def list_dag_warnings() -> str:
    """Get warnings and issues detected in DAG definitions.

    Use this tool when the user asks about:
    - "Are there any DAG warnings?" or "Show me DAG issues"
    - "What problems exist with my DAGs?" or "Any DAG errors?"
    - "Check DAG health" or "Show me DAG validation warnings"
    - "What's wrong with my workflows?"

    Returns warnings about DAG configuration issues including:
    - dag_id: Which DAG has the warning
    - warning_type: Type of warning (e.g., deprecation, configuration issue)
    - message: Description of the warning
    - timestamp: When the warning was detected

    Returns:
        JSON with list of DAG warnings and their details
    """
    return _list_dag_warnings_impl()


@mcp.tool(annotations=read_only())
def list_import_errors() -> str:
    """Get import errors from DAG files that failed to parse or load.

    Use this tool when the user asks about:
    - "Are there any import errors?" or "Show me import errors"
    - "Why isn't my DAG showing up?" or "DAG not appearing in Airflow"
    - "What DAG files have errors?" or "Show me broken DAGs"
    - "Check for syntax errors" or "Are there any parsing errors?"
    - "Why is my DAG file failing to load?"

    Import errors occur when DAG files have problems that prevent Airflow
    from parsing them, such as:
    - Python syntax errors
    - Missing imports or dependencies
    - Module not found errors
    - Invalid DAG definitions
    - Runtime errors during file parsing

    Returns import error details including:
    - import_error_id: Unique identifier for the error
    - timestamp: When the error was detected
    - filename: Path to the DAG file with the error
    - stack_trace: Complete error message and traceback

    Returns:
        JSON with list of import errors and their stack traces
    """
    return _list_import_errors_impl()


# =============================================================================
# CONSOLIDATED TOOLS (Agent-optimized for complex investigations)
# =============================================================================


@mcp.tool(annotations=read_only())
def explore_dag(dag_id: str) -> str:
    """Comprehensive investigation of a DAG - get all relevant info in one call.

    USE THIS TOOL WHEN you need to understand a DAG completely. Instead of making
    multiple calls, this returns everything about a DAG in a single response.

    This is the preferred first tool when:
    - User asks "Tell me about DAG X" or "What is this DAG?"
    - You need to understand a DAG's structure before diagnosing issues
    - You want to know the schedule, tasks, and source code together

    Returns combined data:
    - DAG metadata (schedule, owners, tags, paused status)
    - All tasks with their operators and dependencies
    - DAG source code
    - Any import errors or warnings for this DAG

    Args:
        dag_id: The ID of the DAG to explore

    Returns:
        JSON with comprehensive DAG information
    """
    result: dict[str, Any] = {"dag_id": dag_id}
    adapter = _get_adapter()

    # Get DAG details
    try:
        result["dag_info"] = adapter.get_dag(dag_id)
    except Exception as e:
        result["dag_info"] = error_payload(e, dag_id=dag_id)

    # Get tasks
    try:
        tasks_data = adapter.list_tasks(dag_id)
        result["tasks"] = tasks_data.get("tasks", [])
    except Exception as e:
        result["tasks"] = error_payload(e, dag_id=dag_id)

    # Get DAG source
    try:
        result["source"] = adapter.get_dag_source(dag_id)
    except Exception as e:
        result["source"] = error_payload(e, dag_id=dag_id)

    return json.dumps(result, indent=2)


_MAX_ERROR_LOOKUPS = 10


def _error_snippet(adapter: Any, dag_id: str, dag_run_id: str, task: dict[str, Any]) -> str | None:
    """Best-effort: the last error line from a failed task's latest attempt log."""
    tries = task.get("try_number")
    if not isinstance(tries, int) or tries < 1:
        return None
    map_index = task.get("map_index")
    for attempt in (tries, tries - 1):
        if attempt < 1:
            break
        try:
            data = adapter.get_task_logs(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task["task_id"],
                try_number=attempt,
                map_index=map_index if isinstance(map_index, int) else -1,
                full_content=True,
            )
        except Exception:
            return None
        if "content" in data:
            return last_error_line(log_lines(data["content"]))
    return None


@mcp.tool(annotations=read_only())
def diagnose_dag_run(dag_id: str, dag_run_id: str) -> str:
    """Diagnose issues with a specific DAG run - get run details and failed tasks.

    USE THIS TOOL WHEN troubleshooting a failed or problematic DAG run. Returns
    all the information you need to understand what went wrong.

    This is the preferred tool when:
    - User asks "Why did this DAG run fail?"
    - User asks "What's wrong with run X?"
    - You need to investigate task failures in a specific run

    Returns combined data:
    - DAG run metadata (state, start/end times, trigger type)
    - Up to 100 task instances with full details and explicit sample metadata
    - summary.failed_tasks: tasks that failed on their own (root causes), across
      every page, each with an `error` line taken from its latest attempt's log
      (up to 10 tasks are inspected; use get_task_logs for the full traceback)
    - summary.upstream_failed_tasks: tasks that never ran because an upstream
      task failed (symptoms, not causes)
    - Complete counts of task states (not limited to the sample)

    If task retrieval fails or exceeds the pagination safety limit, returns a
    task_instances error instead of a potentially incomplete summary.

    Args:
        dag_id: The ID of the DAG
        dag_run_id: The ID of the DAG run (e.g., "manual__2024-01-01T00:00:00+00:00")

    Returns:
        JSON with diagnostic information about the DAG run
    """
    result: dict[str, Any] = {"dag_id": dag_id, "dag_run_id": dag_run_id}
    adapter = _get_adapter()

    # Get DAG run details
    try:
        result["run_info"] = adapter.get_dag_run(dag_id, dag_run_id)
    except Exception as e:
        result["run_info"] = error_payload(e, dag_id=dag_id, dag_run_id=dag_run_id)
        return json.dumps(result, indent=2)

    # Get task instances for this run
    try:
        tasks_data = adapter.get_all_task_instances(dag_id, dag_run_id)
        result.update(summarize_task_instances(tasks_data["task_instances"]))
        for task in result["summary"]["failed_tasks"][:_MAX_ERROR_LOOKUPS]:
            error = _error_snippet(adapter, dag_id, dag_run_id, task)
            if error:
                task["error"] = error
    except Exception as e:
        result["task_instances"] = error_payload(e, dag_id=dag_id, dag_run_id=dag_run_id)

    return json.dumps(result, indent=2)


@mcp.tool(annotations=read_only())
def get_system_health() -> str:
    """Get overall Airflow system health - components, import errors, warnings, DAG stats.

    USE THIS TOOL WHEN you need a quick health check of the Airflow system.
    Returns a consolidated view of potential issues across the entire system.

    This is the preferred tool when:
    - User asks "Are there any problems with Airflow?"
    - User asks "Show me the system health" or "Any errors?"
    - You want to do a morning health check
    - You're starting an investigation and want to see the big picture

    Returns combined data:
    - Import errors (DAG files that failed to parse)
    - DAG warnings (deprecations, configuration issues)
    - Component health: metadatabase, scheduler, triggerer and DAG processor
      status with last heartbeat (a dead scheduler makes the status unhealthy)
    - DAG statistics: run totals by state plus DAGs with failed/running/queued runs
    - Version information

    Returns:
        JSON with system health overview
    """
    result: dict[str, Any] = {}
    adapter = _get_adapter()

    # Get version info
    try:
        result["version"] = adapter.get_version()
    except Exception as e:
        result["version"] = error_payload(e)

    # Get import errors
    try:
        errors_data = adapter.list_import_errors(limit=100)
        import_errors = errors_data.get("import_errors", [])
        result["import_errors"] = {
            "count": len(import_errors),
            "errors": import_errors,
        }
    except Exception as e:
        result["import_errors"] = error_payload(e)

    # Get DAG warnings
    try:
        warnings_data = adapter.list_dag_warnings(limit=100)
        dag_warnings = warnings_data.get("dag_warnings", [])
        result["dag_warnings"] = {
            "count": len(dag_warnings),
            "warnings": dag_warnings,
        }
    except Exception as e:
        result["dag_warnings"] = error_payload(e)

    # Component health: scheduler / triggerer / dag processor / metadatabase
    components = _component_health(adapter, result)

    # Get DAG stats
    try:
        result["dag_stats"] = _summarize_dag_stats(adapter.get_dag_stats())
    except Exception:
        result["dag_stats"] = {"available": False, "note": "dagStats endpoint not available"}

    # Calculate overall health status. Problems are listed worst-first.
    import_error_count = result.get("import_errors", {}).get("count", 0)
    warning_count = result.get("dag_warnings", {}).get("count", 0)

    critical: list[str] = []
    warnings: list[str] = []
    for name in ("metadatabase", "scheduler", "dag_processor"):
        if components.get(name) == "unhealthy":
            critical.append(f"{name} is unhealthy")
    if components.get("triggerer") == "unhealthy":
        warnings.append("triggerer is unhealthy (matters only if you use deferrable operators)")
    if import_error_count > 0:
        critical.append(f"{import_error_count} import error(s) detected")
    if warning_count > 0:
        warnings.append(f"{warning_count} DAG warning(s) detected")

    if critical:
        result["overall_status"] = "unhealthy"
        result["status_reason"] = "; ".join(critical + warnings)
    elif warnings:
        result["overall_status"] = "warning"
        result["status_reason"] = "; ".join(warnings)
    elif not components:
        result["overall_status"] = "unknown"
        result["status_reason"] = (
            "No import errors or warnings, but component (scheduler) health could not be checked"
        )
    else:
        result["overall_status"] = "healthy"
        result["status_reason"] = "All components healthy; no import errors or warnings"

    return json.dumps(result, indent=2)


def _component_health(adapter: Any, result: dict[str, Any]) -> dict[str, str]:
    """Fetch component health into ``result['components']``; return name -> status."""
    try:
        raw = adapter.get_health()
    except Exception as e:
        result["components"] = error_payload(e)
        return {}
    components: dict[str, Any] = {}
    statuses: dict[str, str] = {}
    for name, info in raw.items():
        if not isinstance(info, dict):
            continue
        entry = {"status": info.get("status")}
        for key, value in info.items():
            if key.endswith("heartbeat"):
                entry["last_heartbeat"] = value
        components[name] = entry
        if info.get("status"):
            statuses[name] = info["status"]
    result["components"] = components
    return statuses


def _summarize_dag_stats(stats: dict[str, Any]) -> dict[str, Any]:
    """Collapse per-DAG stats into totals, listing only DAGs with failed/running runs."""
    dags = stats.get("dags")
    if not isinstance(dags, list):
        return stats
    totals: dict[str, int] = {}
    attention: list[dict[str, Any]] = []
    for dag in dags:
        counts = {s.get("state"): s.get("count", 0) for s in dag.get("stats", [])}
        for state, count in counts.items():
            totals[state] = totals.get(state, 0) + count
        if counts.get("failed") or counts.get("running") or counts.get("queued"):
            attention.append(
                {"dag_id": dag.get("dag_id"), **{k: v for k, v in counts.items() if v}}
            )
    summary: dict[str, Any] = {
        "dags_with_runs": len(dags),
        "totals_by_state": totals,
        "dags_needing_attention": attention[:50],
    }
    if len(attention) > 50:
        summary["dags_needing_attention_truncated"] = True
    if stats.get("errors"):
        summary["errors"] = stats["errors"]
    return summary
