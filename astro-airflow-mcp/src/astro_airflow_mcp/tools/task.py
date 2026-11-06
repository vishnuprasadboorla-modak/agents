"""Task management tools - get, list, instance, logs, clear."""

import json
from typing import Any

from astro_airflow_mcp.constants import FAILED_TASK_STATES
from astro_airflow_mcp.log_utils import DEFAULT_TAIL_LINES, shape_task_log
from astro_airflow_mcp.server import _get_adapter, _wrap_list_response, mcp
from astro_airflow_mcp.tool_annotations import read_only, write
from astro_airflow_mcp.tool_errors import tool_error


def _get_task_impl(dag_id: str, task_id: str) -> str:
    """Internal implementation for getting task details from Airflow.

    Args:
        dag_id: The ID of the DAG
        task_id: The ID of the task

    Returns:
        JSON string containing the task details
    """
    try:
        adapter = _get_adapter()
        data = adapter.get_task(dag_id, task_id)
        return json.dumps(data, indent=2)
    except Exception as e:
        return tool_error(e, dag_id=dag_id, task_id=task_id)


def _list_tasks_impl(dag_id: str) -> str:
    """Internal implementation for listing tasks in a DAG from Airflow.

    Args:
        dag_id: The ID of the DAG to list tasks for

    Returns:
        JSON string containing the list of tasks with their metadata
    """
    try:
        adapter = _get_adapter()
        data = adapter.list_tasks(dag_id)

        if "tasks" in data:
            return _wrap_list_response(data["tasks"], "tasks", data)
        return f"No tasks found. Response: {data}"
    except Exception as e:
        return tool_error(e, dag_id=dag_id)


def _get_task_instance_impl(dag_id: str, dag_run_id: str, task_id: str) -> str:
    """Internal implementation for getting task instance details from Airflow.

    Args:
        dag_id: The ID of the DAG
        dag_run_id: The ID of the DAG run
        task_id: The ID of the task

    Returns:
        JSON string containing the task instance details
    """
    try:
        adapter = _get_adapter()
        data = adapter.get_task_instance(dag_id, dag_run_id, task_id)
        return json.dumps(data, indent=2)
    except Exception as e:
        return tool_error(e, dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id)


def _resolve_latest_try(
    adapter: Any, dag_id: str, dag_run_id: str, task_id: str
) -> tuple[int | None, dict[str, Any]]:
    """Return ``(try_number, task_instance)`` for the most recent attempt.

    ``try_number`` is ``None`` when the task instance has not started yet.
    """
    ti = adapter.get_task_instance(dag_id, dag_run_id, task_id)
    tries = ti.get("try_number")
    if not isinstance(tries, int) or tries < 1:
        return None, ti
    return tries, ti


def _get_task_logs_impl(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int | None = None,
    map_index: int = -1,
    tail_lines: int | None = DEFAULT_TAIL_LINES,
) -> str:
    """Internal implementation for getting task instance logs from Airflow.

    Args:
        dag_id: The ID of the DAG
        dag_run_id: The ID of the DAG run
        task_id: The ID of the task
        try_number: The task try number (1-indexed). ``None`` = latest attempt.
        map_index: For mapped tasks, which map index (-1 for unmapped, default: -1)
        tail_lines: Keep only the last N lines (0/None = whole log)

    Returns:
        JSON string containing the task logs
    """
    try:
        adapter = _get_adapter()
        ti_state = None
        if try_number is None:
            try:
                try_number, ti = _resolve_latest_try(adapter, dag_id, dag_run_id, task_id)
            except Exception:
                # e.g. mapped tasks, where the un-indexed lookup is ambiguous:
                # fall back to the first attempt rather than failing outright.
                try_number, ti = 1, {}
            ti_state = ti.get("state")
            if try_number is None:
                return json.dumps(
                    {
                        "logs_found": False,
                        "dag_id": dag_id,
                        "dag_run_id": dag_run_id,
                        "task_id": task_id,
                        "state": ti_state,
                        "note": (
                            "This task has not started yet (no attempts), so there are "
                            "no logs. Check the upstream tasks or the DAG run state."
                        ),
                    },
                    indent=2,
                )

        def _fetch(n: int) -> dict[str, Any]:
            return adapter.get_task_logs(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_id,
                try_number=n,
                map_index=map_index,
                full_content=True,
            )

        data = _fetch(try_number)
        # Airflow 2 can report the *next* try number for finished tasks; fall
        # back one attempt if the reported one has no logs.
        if data.get("logs_found") is False and ti_state is not None and try_number > 1:
            fallback = _fetch(try_number - 1)
            if fallback.get("logs_found") is not False:
                try_number, data = try_number - 1, fallback

        result = shape_task_log(data, tail_lines=tail_lines)
        result = {"try_number": try_number, **result} if "content" in result else result
        return json.dumps(result, indent=2)
    except Exception as e:
        return tool_error(e, dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id)


def _clear_task_instances_impl(
    dag_id: str,
    dag_run_id: str,
    task_ids: list[str] | None = None,
    dry_run: bool = True,
    only_failed: bool = False,
    include_downstream: bool = False,
) -> str:
    """Internal implementation for clearing task instances.

    Note: The adapter also supports ``include_upstream`` and ``reset_dag_runs``
    parameters, but they are intentionally omitted from the tool surface.
    ``include_upstream`` is rarely desired for retries (and risks re-processing
    already-succeeded tasks), and ``reset_dag_runs=True`` is the correct default
    for virtually all retry scenarios.  The adapter sends sensible defaults for
    both.

    Args:
        dag_id: The ID of the DAG
        dag_run_id: The ID of the DAG run
        task_ids: List of task IDs to clear. May be omitted when ``only_failed``
            is True, in which case every failed task in the run is targeted.
        dry_run: If True, return what would be cleared without clearing
        only_failed: Only clear failed task instances
        include_downstream: Also clear downstream tasks

    Returns:
        JSON string with the cleared task instances
    """
    try:
        adapter = _get_adapter()
        if not task_ids:
            if not only_failed:
                return json.dumps(
                    {
                        "error": "task_ids is required unless only_failed=true.",
                        "error_type": "ValueError",
                        "hint": (
                            "Pass task_ids=[...] to clear specific tasks, or set "
                            "only_failed=true to retry every failed task in the run."
                        ),
                        "retryable": True,
                        "dag_id": dag_id,
                        "dag_run_id": dag_run_id,
                    },
                    indent=2,
                )
            instances = adapter.get_all_task_instances(dag_id, dag_run_id)["task_instances"]
            task_ids = sorted(
                {t["task_id"] for t in instances if t.get("state") in FAILED_TASK_STATES}
            )
            if not task_ids:
                return json.dumps(
                    {
                        "dag_id": dag_id,
                        "dag_run_id": dag_run_id,
                        "task_instances": [],
                        "message": "No failed tasks in this run; nothing to clear.",
                    },
                    indent=2,
                )
        data = adapter.clear_task_instances(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_ids=task_ids,
            dry_run=dry_run,
            only_failed=only_failed,
            include_downstream=include_downstream,
        )
        return json.dumps(data, indent=2)
    except Exception as e:
        return tool_error(e, dag_id=dag_id, dag_run_id=dag_run_id, task_ids=task_ids)


@mcp.tool(annotations=read_only())
def get_task(dag_id: str, task_id: str) -> str:
    """Get detailed information about a specific task definition in a DAG.

    Use this tool when the user asks about:
    - "Show me details for task X in DAG Y" or "What does task Z do?"
    - "What operator does task A use?" or "What's the configuration of task B?"
    - "Tell me about task C" or "Get task definition for D"
    - "What are the dependencies of task E?" or "Which tasks does F depend on?"

    Returns task definition information including:
    - task_id: Unique identifier for the task
    - task_display_name: Human-readable display name
    - owner: Who owns this task
    - start_date: When this task becomes active
    - end_date: When this task becomes inactive (if set)
    - trigger_rule: When this task should run (all_success, one_failed, etc.)
    - depends_on_past: Whether task depends on previous run's success
    - wait_for_downstream: Whether to wait for downstream tasks
    - retries: Number of retry attempts
    - retry_delay: Time between retries
    - execution_timeout: Maximum execution time
    - operator_name: Type of operator (PythonOperator, BashOperator, etc.)
    - pool: Resource pool assignment
    - queue: Queue for executor
    - downstream_task_ids: List of tasks that depend on this task
    - upstream_task_ids: List of tasks this task depends on

    Args:
        dag_id: The ID of the DAG containing the task
        task_id: The ID of the task to get details for

    Returns:
        JSON with complete task definition details
    """
    return _get_task_impl(dag_id=dag_id, task_id=task_id)


@mcp.tool(annotations=read_only())
def list_tasks(dag_id: str) -> str:
    """Get all tasks defined in a specific DAG.

    Use this tool when the user asks about:
    - "What tasks are in DAG X?" or "List all tasks for DAG Y"
    - "Show me the tasks in this workflow" or "What's in the DAG?"
    - "What are the steps in DAG Z?" or "Show me the task structure"
    - "What does this DAG do?" or "Explain the workflow steps"

    Returns information about all tasks in the DAG including:
    - task_id: Unique identifier for the task
    - task_display_name: Human-readable display name
    - owner: Who owns this task
    - operator_name: Type of operator (PythonOperator, BashOperator, etc.)
    - start_date: When this task becomes active
    - end_date: When this task becomes inactive (if set)
    - trigger_rule: When this task should run
    - retries: Number of retry attempts
    - pool: Resource pool assignment
    - downstream_task_ids: List of tasks that depend on this task
    - upstream_task_ids: List of tasks this task depends on

    Args:
        dag_id: The ID of the DAG to list tasks for

    Returns:
        JSON with list of all tasks in the DAG and their configurations
    """
    return _list_tasks_impl(dag_id=dag_id)


@mcp.tool(annotations=read_only())
def get_task_instance(dag_id: str, dag_run_id: str, task_id: str) -> str:
    """Get detailed information about a specific task instance execution.

    Use this tool when the user asks about:
    - "Show me details for task X in DAG run Y" or "What's the status of task Z?"
    - "Why did task A fail?" or "When did task B start/finish?"
    - "What's the duration of task C?" or "Show me task execution details"
    - "Get logs for task D" or "What operator does task E use?"

    Returns detailed task instance information including:
    - task_id: Name of the task
    - state: Current state (success, failed, running, queued, etc.)
    - start_date: When the task started
    - end_date: When the task finished
    - duration: How long the task ran
    - try_number: Which attempt this is
    - max_tries: Maximum retry attempts
    - operator: What operator type (PythonOperator, BashOperator, etc.)
    - executor_config: Executor configuration
    - pool: Resource pool assignment

    Args:
        dag_id: The ID of the DAG
        dag_run_id: The ID of the DAG run (e.g., "manual__2024-01-01T00:00:00+00:00")
        task_id: The ID of the task within the DAG

    Returns:
        JSON with complete task instance details
    """
    return _get_task_instance_impl(dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id)


@mcp.tool(annotations=read_only())
def get_task_logs(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int | None = None,
    map_index: int = -1,
    tail_lines: int = DEFAULT_TAIL_LINES,
) -> str:
    """Get logs for a specific task instance execution.

    Use this tool when the user asks about:
    - "Show me the logs for task X" or "Get logs for task Y"
    - "What did task Z output?" or "Show me task execution logs"
    - "Why did task A fail?" (to see error messages in logs)
    - "What happened during task B execution?"
    - "Show me the stdout/stderr for task C"
    - "Debug task D" or "Troubleshoot task E"

    Returns the actual log output from the task execution, which includes:
    - Task execution output (stdout/stderr)
    - Error messages and stack traces (if task failed)
    - Timing information
    - Any logged messages from the task code

    By default this returns the LATEST attempt (so after retries you see the
    final failure, not the first try), flattened to plain text, limited to the
    last `tail_lines` lines. The response includes `try_number`, `total_lines`
    and `truncated`. If the task has not started, `logs_found` is false.

    This is essential for debugging failed tasks or understanding what
    happened during task execution.

    Args:
        dag_id: The ID of the DAG (e.g., "example_dag")
        dag_run_id: The ID of the DAG run (e.g., "manual__2024-01-01T00:00:00+00:00")
        task_id: The ID of the task within the DAG (e.g., "extract_data")
        try_number: The task try/attempt number, 1-indexed. Omit (default) for
                    the latest attempt; set it to inspect an earlier retry.
        map_index: For mapped tasks, which map index to get logs for.
                   Use -1 for non-mapped tasks (default: -1).
        tail_lines: Return only the last N log lines (default: 200). Use 0 to
                    return the whole log (can be very large).

    Returns:
        JSON with the task logs content
    """
    return _get_task_logs_impl(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        try_number=try_number,
        map_index=map_index,
        tail_lines=tail_lines,
    )


@mcp.tool(annotations=write(destructive=True, idempotent=True))
def clear_task_instances(
    dag_id: str,
    dag_run_id: str,
    task_ids: list[str] | None = None,
    dry_run: bool = True,
    only_failed: bool = False,
    include_downstream: bool = False,
) -> str:
    """Clear task instances to retry their execution.

    Use this tool when the user asks about:
    - "Retry task X" or "Clear task Y and run again"
    - "Rerun the failed tasks" or "Reset task Z"
    - "Clear and retry from task A onwards"
    - "Re-execute task B" or "Run task C again"
    - "Fix the failed task and retry"

    This clears the state of task instances, allowing them to be re-executed.
    By default, it runs in dry_run mode to show what would be cleared.
    Set dry_run=False to actually clear the tasks.

    Returns information about cleared task instances including:
    - task_id: The ID of the cleared task
    - dag_id: The DAG containing the task
    - dag_run_id: The DAG run containing the task
    - state: The previous state of the task (before clearing)

    Args:
        dag_id: The ID of the DAG (e.g., "example_dag")
        dag_run_id: The ID of the DAG run (e.g., "manual__2024-01-01T00:00:00+00:00")
        task_ids: List of task IDs to clear (e.g., ["extract_data", "transform_data"]).
                  Optional when only_failed=True: then all failed tasks in the
                  run are cleared ("retry all failed tasks").
        dry_run: If True (default), only show what would be cleared without actually
                 clearing. Set to False to actually clear the tasks.
        only_failed: If True, only clear task instances that are in a failed state.
                     Useful for retrying just the failures.
        include_downstream: If True, also clear all downstream tasks that depend
                           on the specified tasks. Useful for "retry from here onwards".

    Returns:
        JSON with list of task instances that were (or would be) cleared
    """
    return _clear_task_instances_impl(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_ids=task_ids,
        dry_run=dry_run,
        only_failed=only_failed,
        include_downstream=include_downstream,
    )
