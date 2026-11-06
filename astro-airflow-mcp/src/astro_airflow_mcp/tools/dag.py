"""DAG management tools - get, list, source, stats, pause, unpause."""

import json

from astro_airflow_mcp.constants import DEFAULT_LIMIT, DEFAULT_OFFSET
from astro_airflow_mcp.server import (
    _get_adapter,
    _wrap_list_response,
    mcp,
)
from astro_airflow_mcp.tool_annotations import read_only, write
from astro_airflow_mcp.tool_errors import tool_error


def _get_dag_details_impl(dag_id: str) -> str:
    """Internal implementation for getting details about a specific DAG.

    Args:
        dag_id: The ID of the DAG to get details for

    Returns:
        JSON string containing the DAG details
    """
    try:
        adapter = _get_adapter()
        data = adapter.get_dag(dag_id)
        return json.dumps(data, indent=2)
    except Exception as e:
        return tool_error(e, dag_id=dag_id)


@mcp.tool(annotations=read_only())
def get_dag_details(dag_id: str) -> str:
    """Get detailed information about a specific Apache Airflow DAG.

    Use this tool when the user asks about:
    - "Show me details for DAG X" or "What are the details of DAG Y?"
    - "Tell me about DAG Z" or "Get information for this specific DAG"
    - "What's the schedule for DAG X?" or "When does this DAG run?"
    - "Is DAG Y paused?" or "Show me the configuration of DAG Z"
    - "Who owns this DAG?" or "What are the tags for this workflow?"

    Returns complete DAG information including:
    - dag_id: Unique identifier for the DAG
    - is_paused: Whether the DAG is currently paused
    - is_active: Whether the DAG is active
    - is_subdag: Whether this is a SubDAG
    - fileloc: File path where the DAG is defined
    - file_token: Unique token for the DAG file
    - owners: List of DAG owners
    - description: Human-readable description of what the DAG does
    - schedule_interval: Cron expression or timedelta for scheduling
    - tags: List of tags/labels for categorization
    - max_active_runs: Maximum number of concurrent runs
    - max_active_tasks: Maximum number of concurrent tasks
    - has_task_concurrency_limits: Whether task concurrency limits are set
    - has_import_errors: Whether the DAG has import errors
    - next_dagrun: When the next DAG run is scheduled
    - next_dagrun_create_after: Earliest time for next DAG run creation

    Args:
        dag_id: The ID of the DAG to get details for

    Returns:
        JSON with complete details about the specified DAG
    """
    return _get_dag_details_impl(dag_id=dag_id)


# Fields kept per DAG in compact mode. Everything else (file tokens, parse
# timings, timetable descriptions, ...) is dropped to keep responses small.
_COMPACT_DAG_FIELDS = (
    "dag_id",
    "is_paused",
    "is_active",
    "is_stale",
    "schedule_interval",
    "timetable_summary",
    "description",
    "owners",
    "has_import_errors",
    "next_dagrun",
    "next_dagrun_logical_date",
    "next_dagrun_run_after",
)


def _compact_dag(dag: dict) -> dict:
    """Reduce a DAG record to the fields an agent normally needs (nulls omitted)."""
    # Null fields carry no information, so they are omitted entirely.
    out = {k: dag[k] for k in _COMPACT_DAG_FIELDS if dag.get(k) is not None}
    # Airflow 2 returns schedule_interval as {"__type": ..., "value": ...}
    sched = out.get("schedule_interval")
    if isinstance(sched, dict):
        out["schedule_interval"] = sched.get("value", sched)
    # Tags come back as [{"name": "x"}]; flatten to ["x"].
    out["tags"] = [t.get("name") if isinstance(t, dict) else t for t in dag.get("tags") or []]
    return out


def _list_dags_impl(
    limit: int = DEFAULT_LIMIT,
    offset: int = DEFAULT_OFFSET,
    tags: list[str] | None = None,
    dag_id_pattern: str | None = None,
    paused: bool | None = None,
    only_active: bool | None = None,
    compact: bool = True,
) -> str:
    """Internal implementation for listing DAGs from Airflow.

    Args:
        limit: Maximum number of DAGs to return (default: 100)
        offset: Offset for pagination (default: 0)
        tags: Only DAGs having these tags
        dag_id_pattern: Case-insensitive substring match on dag_id
        paused: True = only paused DAGs, False = only unpaused, None = both
        only_active: Only DAGs that are active (not stale/removed)
        compact: Return a reduced set of fields per DAG

    Returns:
        JSON string containing the list of DAGs with their metadata
    """
    try:
        adapter = _get_adapter()
        filters = {
            "tags": tags or None,
            "dag_id_pattern": dag_id_pattern or None,
            "paused": paused,
            "only_active": only_active,
        }
        data = adapter.list_dags(
            limit=limit,
            offset=offset,
            **{k: v for k, v in filters.items() if v is not None},
        )

        if "dags" in data:
            dags = [_compact_dag(d) for d in data["dags"]] if compact else data["dags"]
            payload = json.loads(_wrap_list_response(dags, "dags", data))
            total = payload["total_dags"]
            if offset + len(dags) < total:
                payload["has_more"] = True
                payload["next_offset"] = offset + len(dags)
            return json.dumps(payload, indent=2)
        return f"No DAGs found. Response: {data}"
    except Exception as e:
        return tool_error(e)


@mcp.tool(annotations=read_only())
def list_dags(
    limit: int = DEFAULT_LIMIT,
    offset: int = DEFAULT_OFFSET,
    tags: list[str] | None = None,
    dag_id_pattern: str | None = None,
    paused: bool | None = None,
    only_active: bool | None = None,
    compact: bool = True,
) -> str:
    """List and search Apache Airflow DAGs (Directed Acyclic Graphs).

    Use this tool when the user asks about:
    - "What DAGs are available?" or "List all DAGs"
    - "Show me the workflows" or "What pipelines exist?"
    - "Which DAGs are paused/active?" (use `paused`)
    - "Find DAGs with 'etl' in the name" (use `dag_id_pattern`)
    - "Show DAGs tagged X" (use `tags`)
    - DAG schedules, descriptions, or tags

    Results are paginated. The response has `total_dags` (all matches) and
    `returned_count`; when more remain it also has `has_more=true` and
    `next_offset` - call again with `offset=next_offset` to page through them.

    Each DAG (compact mode, the default) contains: dag_id, is_paused,
    is_active/is_stale, schedule, description, owners, tags, next run info and
    has_import_errors. Use `get_dag_details` for the full record of one DAG.

    Args:
        limit: Maximum number of DAGs to return per page (default: 100).
        offset: Number of DAGs to skip, for pagination (default: 0).
        tags: Only return DAGs that have these tags, e.g. ["finance"].
        dag_id_pattern: Case-insensitive substring match on the dag_id,
                        e.g. "etl" matches "daily_etl_orders".
        paused: true = only paused DAGs, false = only unpaused DAGs,
                omitted = both.
        only_active: true = skip stale/removed DAGs.
        compact: true (default) returns a reduced set of fields per DAG to save
                 context; false returns the full Airflow record for each DAG.

    Returns:
        JSON with a page of DAGs and pagination metadata
    """
    return _list_dags_impl(
        limit=limit,
        offset=offset,
        tags=tags,
        dag_id_pattern=dag_id_pattern,
        paused=paused,
        only_active=only_active,
        compact=compact,
    )


def _get_dag_source_impl(dag_id: str) -> str:
    """Internal implementation for getting DAG source code from Airflow.

    Args:
        dag_id: The ID of the DAG to get source code for

    Returns:
        JSON string containing the DAG source code and metadata
    """
    try:
        adapter = _get_adapter()
        source_data = adapter.get_dag_source(dag_id)
        return json.dumps(source_data, indent=2)
    except Exception as e:
        return tool_error(e, dag_id=dag_id)


@mcp.tool(annotations=read_only())
def get_dag_source(dag_id: str) -> str:
    """Get the source code for a specific Apache Airflow DAG.

    Use this tool when the user asks about:
    - "Show me the code for DAG X" or "What's the source of DAG Y?"
    - "How is DAG Z implemented?" or "What does the DAG file look like?"
    - "Can I see the Python code for this workflow?"
    - "What tasks are defined in the DAG code?"

    Returns the DAG source file contents including:
    - content: The actual Python source code of the DAG file
    - file_token: Unique identifier for the source file

    Args:
        dag_id: The ID of the DAG to get source code for

    Returns:
        JSON with DAG source code and metadata
    """
    return _get_dag_source_impl(dag_id=dag_id)


def _get_dag_stats_impl(dag_ids: list[str] | None = None) -> str:
    """Internal implementation for getting DAG statistics from Airflow.

    Args:
        dag_ids: Optional list of DAG IDs to get stats for. Pass a list even for a
            single DAG, for example ["example_dag"]. If None, gets stats for all DAGs.

    Returns:
        JSON string containing DAG run statistics by state
    """
    try:
        adapter = _get_adapter()
        stats_data = adapter.get_dag_stats(dag_ids=dag_ids)
        return json.dumps(stats_data, indent=2)
    except Exception as e:
        return tool_error(e, dag_ids=dag_ids)


@mcp.tool(annotations=read_only())
def get_dag_stats(dag_ids: list[str] | None = None) -> str:
    """Get statistics about DAG runs (success/failure counts by state).

    Use this tool when the user asks about:
    - "What's the overall health of my DAGs?" or "Show me DAG statistics"
    - "How many DAG runs succeeded/failed?" or "What's the success rate?"
    - "Give me a summary of DAG run states"
    - "How many runs are currently running/queued?"
    - "Show me stats for specific DAGs"

    Returns statistics showing counts of DAG runs grouped by state:
    - success: Number of successful runs
    - failed: Number of failed runs
    - running: Number of currently running runs
    - queued: Number of queued runs
    - And other possible states

    Args:
        dag_ids: Optional list of DAG IDs to filter by. Pass a list even for a
            single DAG, for example ["example_dag"]. If not provided, returns
            stats for all DAGs.

    Returns:
        JSON with DAG run statistics organized by DAG and state
    """
    return _get_dag_stats_impl(dag_ids=dag_ids)


def _pause_dag_impl(dag_id: str) -> str:
    """Internal implementation for pausing a DAG.

    Args:
        dag_id: The ID of the DAG to pause

    Returns:
        JSON string containing the updated DAG details
    """
    try:
        adapter = _get_adapter()
        data = adapter.pause_dag(dag_id)
        return json.dumps(data, indent=2)
    except Exception as e:
        return tool_error(e, dag_id=dag_id)


@mcp.tool(annotations=write(destructive=False, idempotent=True))
def pause_dag(dag_id: str) -> str:
    """Pause a DAG to prevent new scheduled runs from starting.

    Use this tool when the user asks to:
    - "Pause DAG X" or "Stop DAG Y from running"
    - "Disable DAG Z" or "Prevent new runs of DAG X"
    - "Turn off DAG scheduling" or "Suspend DAG execution"

    When a DAG is paused:
    - No new scheduled runs will be created
    - Currently running tasks will complete
    - Manual triggers are still possible
    - The DAG remains visible in the UI with a paused indicator

    IMPORTANT: This is a write operation that modifies Airflow state.
    The DAG will remain paused until explicitly unpaused.

    Args:
        dag_id: The ID of the DAG to pause (e.g., "example_dag")

    Returns:
        JSON with updated DAG details showing is_paused=True
    """
    return _pause_dag_impl(dag_id=dag_id)


def _unpause_dag_impl(dag_id: str) -> str:
    """Internal implementation for unpausing a DAG.

    Args:
        dag_id: The ID of the DAG to unpause

    Returns:
        JSON string containing the updated DAG details
    """
    try:
        adapter = _get_adapter()
        data = adapter.unpause_dag(dag_id)
        return json.dumps(data, indent=2)
    except Exception as e:
        return tool_error(e, dag_id=dag_id)


@mcp.tool(annotations=write(destructive=False, idempotent=True))
def unpause_dag(dag_id: str) -> str:
    """Unpause a DAG to allow scheduled runs to resume.

    Use this tool when the user asks to:
    - "Unpause DAG X" or "Resume DAG Y"
    - "Enable DAG Z" or "Start DAG scheduling again"
    - "Turn on DAG X" or "Activate DAG Y"

    When a DAG is unpaused:
    - The scheduler will create new runs based on the schedule
    - Any missed runs (depending on catchup setting) may be created
    - The DAG will appear active in the UI

    IMPORTANT: This is a write operation that modifies Airflow state.
    New DAG runs will be scheduled according to the DAG's schedule_interval.

    Args:
        dag_id: The ID of the DAG to unpause (e.g., "example_dag")

    Returns:
        JSON with updated DAG details showing is_paused=False
    """
    return _unpause_dag_impl(dag_id=dag_id)
