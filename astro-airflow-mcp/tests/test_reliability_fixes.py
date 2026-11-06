"""Tests for the production-readiness fixes (list filters, logs, health, guards, ...)."""

import json
from unittest.mock import MagicMock

import httpx
import pytest

import astro_airflow_mcp.tools.admin as admin_module
import astro_airflow_mcp.tools.dag as dag_module
import astro_airflow_mcp.tools.dag_run as dag_run_module
import astro_airflow_mcp.tools.diagnostic as diag_module
import astro_airflow_mcp.tools.task as task_module
from astro_airflow_mcp.adapters.airflow_v3 import AirflowV3Adapter
from astro_airflow_mcp.log_utils import last_error_line, log_lines, shape_task_log
from astro_airflow_mcp.utils import REDACTED, redact_config


def fn(module, name):
    tool = getattr(module, name)
    return getattr(tool, "fn", tool)


def patch_adapter(mocker, module, adapter=None):
    adapter = adapter or MagicMock()
    mocker.patch(f"{module.__name__}._get_adapter", return_value=adapter)
    return adapter


# --------------------------------------------------------------------------- #
# list_dags
# --------------------------------------------------------------------------- #
class TestListDags:
    RAW = {
        "dag_id": "etl",
        "is_paused": False,
        "is_stale": False,
        "file_token": "x" * 100,
        "fileloc": "/a/b.py",
        "timetable_description": "long text",
        "tags": [{"name": "finance"}],
        "owners": ["me"],
        "timetable_summary": "@daily",
    }

    def test_filters_forwarded_and_none_dropped(self, mocker):
        adapter = patch_adapter(mocker, dag_module)
        adapter.list_dags.return_value = {"dags": [], "total_entries": 0}

        fn(dag_module, "list_dags")(
            limit=10, offset=20, tags=["finance"], dag_id_pattern="etl", paused=False
        )

        adapter.list_dags.assert_called_once_with(
            limit=10, offset=20, tags=["finance"], dag_id_pattern="etl", paused=False
        )

    def test_no_filters_sends_only_pagination(self, mocker):
        adapter = patch_adapter(mocker, dag_module)
        adapter.list_dags.return_value = {"dags": [], "total_entries": 0}
        fn(dag_module, "list_dags")()
        adapter.list_dags.assert_called_once_with(limit=100, offset=0)

    def test_compact_is_default_and_much_smaller(self, mocker):
        adapter = patch_adapter(mocker, dag_module)
        adapter.list_dags.return_value = {"dags": [dict(self.RAW)] * 3, "total_entries": 3}

        compact = fn(dag_module, "list_dags")()
        full = fn(dag_module, "list_dags")(compact=False)

        d = json.loads(compact)["dags"][0]
        assert d["tags"] == ["finance"]
        assert "file_token" not in d
        assert "fileloc" not in d
        assert "timetable_description" not in d
        assert d["timetable_summary"] == "@daily"
        assert len(compact) < len(full)
        assert "file_token" in json.loads(full)["dags"][0]

    def test_pagination_hint_when_more_remain(self, mocker):
        adapter = patch_adapter(mocker, dag_module)
        adapter.list_dags.return_value = {"dags": [dict(self.RAW)] * 2, "total_entries": 157}
        data = json.loads(fn(dag_module, "list_dags")(limit=2, offset=100))
        assert data["total_dags"] == 157
        assert data["has_more"] is True
        assert data["next_offset"] == 102

    def test_no_pagination_hint_on_last_page(self, mocker):
        adapter = patch_adapter(mocker, dag_module)
        adapter.list_dags.return_value = {"dags": [dict(self.RAW)] * 2, "total_entries": 102}
        data = json.loads(fn(dag_module, "list_dags")(limit=2, offset=100))
        assert "has_more" not in data

    def test_airflow2_schedule_and_string_tags(self, mocker):
        adapter = patch_adapter(mocker, dag_module)
        adapter.list_dags.return_value = {
            "dags": [
                {
                    "dag_id": "a",
                    "schedule_interval": {"__type": "CronExpression", "value": "0 * * * *"},
                    "tags": [{"name": "t"}],
                }
            ],
            "total_entries": 1,
        }
        d = json.loads(fn(dag_module, "list_dags")())["dags"][0]
        assert d["schedule_interval"] == "0 * * * *"


# --------------------------------------------------------------------------- #
# list_dag_runs
# --------------------------------------------------------------------------- #
def test_list_dag_runs_forwards_state_and_date_filters(mocker):
    adapter = patch_adapter(mocker, dag_run_module)
    adapter.list_dag_runs.return_value = {"dag_runs": [], "total_entries": 0}

    fn(dag_run_module, "list_dag_runs")(state=["failed"], start_date_gte="2026-11-05T00:00:00Z")

    kwargs = adapter.list_dag_runs.call_args.kwargs
    assert kwargs["state"] == ["failed"]
    assert kwargs["start_date_gte"] == "2026-11-05T00:00:00Z"
    assert "end_date_lte" not in kwargs


def test_v3_adapter_sends_repeated_state_params(mocker):
    adapter = AirflowV3Adapter("http://localhost:8080", "3.1.0", token_getter=lambda: "t")
    resp = MagicMock(status_code=200)
    resp.json.return_value = {"dag_runs": []}
    client = MagicMock()
    client.get.return_value = resp
    client.__enter__.return_value = client
    mocker.patch("httpx.Client", return_value=client)

    adapter.list_dag_runs(state=["failed", "running"], start_date_gte="2026-01-01T00:00:00Z")

    params = client.get.call_args.kwargs["params"]
    assert params["state"] == ["failed", "running"]
    assert params["start_date_gte"] == "2026-01-01T00:00:00Z"


# --------------------------------------------------------------------------- #
# log helpers + get_task_logs
# --------------------------------------------------------------------------- #
V3_LOG = [
    {"event": "::group::Log message source details", "sources": ["/x.log"]},
    {"timestamp": "2026-01-01T00:00:00Z", "event": "Starting", "level": "info"},
    {
        "timestamp": "2026-01-01T00:00:01Z",
        "event": "Task failed with exception",
        "level": "error",
        "error_detail": [
            {
                "exc_type": "ValueError",
                "exc_value": "bad amount",
                "frames": [
                    {"filename": f"f{i}.py", "lineno": i, "name": f"fn{i}"} for i in range(8)
                ],
            }
        ],
    },
]


class TestLogUtils:
    def test_v3_structured_events_flattened_and_frames_trimmed(self):
        lines = log_lines(V3_LOG)
        text = "\n".join(lines)
        assert "Starting" in text
        assert "::group::" not in text
        assert "ValueError: bad amount" in text
        assert text.count('File "') == 3  # only the innermost frames

    def test_v2_tuple_repr_unwrapped(self):
        raw = repr([("host1", "line one\nTraceback...\nKeyError: 'x'\n")])
        lines = log_lines(raw)
        assert lines[0] == "line one"
        assert last_error_line(lines) == "KeyError: 'x'"

    def test_plain_string(self):
        assert log_lines("a\nb") == ["a", "b"]

    def test_last_error_line_prefers_exception_over_frames(self):
        lines = ['  File "a.py", line 3, in f', "ValueError: bad amount", "INFO done"]
        assert last_error_line(lines) == "ValueError: bad amount"

    def test_last_error_line_falls_back_to_error_level(self):
        assert last_error_line(["x", "2026 ERROR something broke"]) == "2026 ERROR something broke"

    def test_last_error_line_none(self):
        assert last_error_line(["all fine"]) is None

    def test_tail_and_truncation_flags(self):
        data = {"content": "\n".join(f"l{i}" for i in range(500))}
        out = shape_task_log(data, tail_lines=10)
        assert out["truncated"] is True
        assert out["total_lines"] == 500
        assert out["content"].splitlines()[-1] == "l499"
        assert len(out["content"].splitlines()) == 10

    def test_tail_zero_returns_everything(self):
        data = {"content": "\n".join(f"l{i}" for i in range(500))}
        out = shape_task_log(data, tail_lines=0)
        assert out["truncated"] is False
        assert out["returned_lines"] == 500

    def test_max_chars_cap_keeps_end(self):
        out = shape_task_log({"content": "x" * 100 + "END"}, tail_lines=0, max_chars=10)
        assert out["content"].endswith("END")
        assert len(out["content"]) == 10
        assert out["truncated"] is True

    def test_not_available_note_passes_through(self):
        note = {"logs_found": False, "note": "nope"}
        assert shape_task_log(note) == note


class TestGetTaskLogs:
    def _call(self, adapter, **kw):
        return json.loads(
            fn(task_module, "get_task_logs")(dag_id="d", dag_run_id="r", task_id="t", **kw)
        )

    def test_defaults_to_latest_try(self, mocker):
        adapter = patch_adapter(mocker, task_module)
        adapter.get_task_instance.return_value = {"state": "failed", "try_number": 4}
        adapter.get_task_logs.return_value = {"content": V3_LOG}

        out = self._call(adapter)

        assert adapter.get_task_logs.call_args.kwargs["try_number"] == 4
        assert out["try_number"] == 4
        assert "ValueError: bad amount" in out["content"]

    def test_explicit_try_number_skips_lookup(self, mocker):
        adapter = patch_adapter(mocker, task_module)
        adapter.get_task_logs.return_value = {"content": "hello"}
        out = self._call(adapter, try_number=1)
        adapter.get_task_instance.assert_not_called()
        assert out["try_number"] == 1

    def test_not_started_task_says_so(self, mocker):
        adapter = patch_adapter(mocker, task_module)
        adapter.get_task_instance.return_value = {"state": "upstream_failed", "try_number": 0}
        out = self._call(adapter)
        assert out["logs_found"] is False
        assert "not started" in out["note"]
        adapter.get_task_logs.assert_not_called()

    def test_airflow2_next_try_off_by_one_falls_back(self, mocker):
        adapter = patch_adapter(mocker, task_module)
        adapter.get_task_instance.return_value = {"state": "success", "try_number": 2}
        adapter.get_task_logs.side_effect = [
            {"logs_found": False, "note": "none"},
            {"content": "real log"},
        ]
        out = self._call(adapter)
        assert out["try_number"] == 1
        assert out["content"] == "real log"

    def test_lookup_failure_falls_back_to_first_try(self, mocker):
        adapter = patch_adapter(mocker, task_module)
        adapter.get_task_instance.side_effect = RuntimeError("mapped")
        adapter.get_task_logs.return_value = {"content": "x"}
        assert self._call(adapter)["try_number"] == 1

    def test_tail_lines_applied(self, mocker):
        adapter = patch_adapter(mocker, task_module)
        adapter.get_task_logs.return_value = {"content": "\n".join(str(i) for i in range(300))}
        out = self._call(adapter, try_number=1, tail_lines=5)
        assert out["returned_lines"] == 5
        assert out["truncated"] is True

    def test_v3_adapter_missing_logs_message_is_not_misleading(self, mocker):
        adapter = AirflowV3Adapter("http://localhost:8080", "3.1.0", token_getter=lambda: "t")
        resp = MagicMock(status_code=404)
        client = MagicMock()
        client.get.return_value = resp
        client.__enter__.return_value = client
        mocker.patch("httpx.Client", return_value=client)

        out = adapter.get_task_logs("d", "r", "t", try_number=1)

        assert out["logs_found"] is False
        assert "not available in Airflow" not in json.dumps(out)


# --------------------------------------------------------------------------- #
# diagnose_dag_run
# --------------------------------------------------------------------------- #
class TestDiagnose:
    def test_error_line_added_and_upstream_separated(self, mocker):
        adapter = patch_adapter(mocker, diag_module)
        adapter.get_dag_run.return_value = {"state": "failed"}
        adapter.get_all_task_instances.return_value = {
            "task_instances": [
                {"task_id": "ok", "state": "success", "try_number": 1},
                {"task_id": "boom", "state": "failed", "try_number": 4, "map_index": -1},
                {"task_id": "down", "state": "upstream_failed", "try_number": 0},
            ]
        }
        adapter.get_task_logs.return_value = {"content": V3_LOG}

        data = json.loads(fn(diag_module, "diagnose_dag_run")("d", "r"))

        failed = data["summary"]["failed_tasks"]
        assert [t["task_id"] for t in failed] == ["boom"]
        assert failed[0]["error"] == "ValueError: bad amount"
        assert [t["task_id"] for t in data["summary"]["upstream_failed_tasks"]] == ["down"]
        assert adapter.get_task_logs.call_args.kwargs["try_number"] == 4

    def test_log_lookup_failure_does_not_break_diagnosis(self, mocker):
        adapter = patch_adapter(mocker, diag_module)
        adapter.get_dag_run.return_value = {"state": "failed"}
        adapter.get_all_task_instances.return_value = {
            "task_instances": [{"task_id": "boom", "state": "failed", "try_number": 1}]
        }
        adapter.get_task_logs.side_effect = RuntimeError("log server down")

        data = json.loads(fn(diag_module, "diagnose_dag_run")("d", "r"))

        assert data["summary"]["failed_tasks"][0]["task_id"] == "boom"
        assert "error" not in data["summary"]["failed_tasks"][0]

    def test_only_first_ten_failures_are_inspected(self, mocker):
        adapter = patch_adapter(mocker, diag_module)
        adapter.get_dag_run.return_value = {"state": "failed"}
        adapter.get_all_task_instances.return_value = {
            "task_instances": [
                {"task_id": f"t{i}", "state": "failed", "try_number": 1} for i in range(30)
            ]
        }
        adapter.get_task_logs.return_value = {"content": "ValueError: x"}
        fn(diag_module, "diagnose_dag_run")("d", "r")
        assert adapter.get_task_logs.call_count == 10


# --------------------------------------------------------------------------- #
# get_system_health
# --------------------------------------------------------------------------- #
def _health_adapter(mocker, health, *, import_errors=0, warnings=0):
    adapter = patch_adapter(mocker, diag_module)
    adapter.get_version.return_value = {"version": "3.1.0"}
    adapter.list_import_errors.return_value = {"import_errors": [{}] * import_errors}
    adapter.list_dag_warnings.return_value = {"dag_warnings": [{}] * warnings}
    adapter.get_dag_stats.return_value = {"dags": []}
    if isinstance(health, Exception):
        adapter.get_health.side_effect = health
    else:
        adapter.get_health.return_value = health
    return adapter


def _h(**overrides):
    base = {
        "metadatabase": {"status": "healthy"},
        "scheduler": {"status": "healthy", "latest_scheduler_heartbeat": "t1"},
        "triggerer": {"status": "healthy", "latest_triggerer_heartbeat": "t2"},
        "dag_processor": {"status": "healthy", "latest_dag_processor_heartbeat": "t3"},
    }
    for k, v in overrides.items():
        base[k] = {"status": v}
    return base


class TestSystemHealth:
    def run(self):
        return json.loads(fn(diag_module, "get_system_health")())

    def test_all_good(self, mocker):
        _health_adapter(mocker, _h())
        data = self.run()
        assert data["overall_status"] == "healthy"
        assert data["components"]["scheduler"]["last_heartbeat"] == "t1"

    def test_dead_scheduler_is_unhealthy_even_without_import_errors(self, mocker):
        _health_adapter(mocker, _h(scheduler="unhealthy"))
        data = self.run()
        assert data["overall_status"] == "unhealthy"
        assert "scheduler" in data["status_reason"]

    @pytest.mark.parametrize("component", ["metadatabase", "dag_processor"])
    def test_other_critical_components(self, mocker, component):
        _health_adapter(mocker, _h(**{component: "unhealthy"}))
        assert self.run()["overall_status"] == "unhealthy"

    def test_triggerer_down_is_only_a_warning(self, mocker):
        _health_adapter(mocker, _h(triggerer="unhealthy"))
        assert self.run()["overall_status"] == "warning"

    def test_unused_triggerer_with_null_status_ignored(self, mocker):
        health = _h()
        health["triggerer"] = {"status": None, "latest_triggerer_heartbeat": None}
        _health_adapter(mocker, health)
        assert self.run()["overall_status"] == "healthy"

    def test_health_endpoint_failure_is_unknown_not_healthy(self, mocker):
        _health_adapter(mocker, RuntimeError("503"))
        data = self.run()
        assert data["overall_status"] == "unknown"
        assert data["components"]["error"]

    def test_import_errors_still_reported_when_health_unavailable(self, mocker):
        _health_adapter(mocker, RuntimeError("503"), import_errors=2)
        assert self.run()["overall_status"] == "unhealthy"

    def test_airflow2_health_without_dag_processor(self, mocker):
        _health_adapter(
            mocker,
            {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}},
        )
        assert self.run()["overall_status"] == "healthy"

    def test_dag_stats_are_aggregated_not_dumped(self, mocker):
        adapter = _health_adapter(mocker, _h())
        adapter.get_dag_stats.return_value = {
            "dags": [
                {
                    "dag_id": "a",
                    "stats": [{"state": "failed", "count": 2}, {"state": "success", "count": 1}],
                },
                {"dag_id": "b", "stats": [{"state": "success", "count": 5}]},
            ]
        }
        stats = self.run()["dag_stats"]
        assert stats["totals_by_state"] == {"failed": 2, "success": 6}
        assert [d["dag_id"] for d in stats["dags_needing_attention"]] == ["a"]


# --------------------------------------------------------------------------- #
# config redaction
# --------------------------------------------------------------------------- #
CONFIG = {
    "sections": [
        {
            "name": "core",
            "options": [
                {"key": "fernet_key", "value": "abc"},
                {"key": "executor", "value": "LocalExecutor"},
                {"key": "sql_alchemy_conn", "value": "postgresql://u:pw@h/db"},
                {"key": "empty_secret", "value": ""},
            ],
        },
        {
            "name": "api_auth",
            "options": [
                {"key": "jwt_secret", "value": "s3"},
                {"key": "secret_key", "value": "s4"},
                {"key": "some_url", "value": "redis://user:hunter2@host:6379/0"},
            ],
        },
    ]
}


class TestRedaction:
    def test_sensitive_values_masked(self):
        out = redact_config(CONFIG)
        opts = {o["key"]: o["value"] for s in out["sections"] for o in s["options"]}
        assert opts["fernet_key"] == REDACTED
        assert opts["jwt_secret"] == REDACTED
        assert opts["secret_key"] == REDACTED
        assert opts["sql_alchemy_conn"] == REDACTED
        assert opts["executor"] == "LocalExecutor"
        assert opts["empty_secret"] == ""
        assert "hunter2" not in opts["some_url"]
        assert "user:" in opts["some_url"]

    def test_input_not_mutated(self):
        redact_config(CONFIG)
        assert CONFIG["sections"][0]["options"][0]["value"] == "abc"

    def test_tool_and_resource_both_redacted(self, mocker):
        adapter = patch_adapter(mocker, admin_module)
        adapter.get_config.return_value = CONFIG
        for text in (
            fn(admin_module, "get_airflow_config")(),
            admin_module._get_config_impl(),
        ):
            assert "abc" not in text
            assert "s3" not in text
            assert "hunter2" not in text
            assert REDACTED in text

    def test_resource_uses_redacted_impl(self, mocker):
        import astro_airflow_mcp.resources as resources

        patch_adapter(mocker, admin_module).get_config.return_value = CONFIG
        assert "hunter2" not in fn(resources, "resource_config")()


# --------------------------------------------------------------------------- #
# delete_dag_run guard
# --------------------------------------------------------------------------- #
class TestDeleteGuard:
    @pytest.mark.parametrize("state", ["running", "queued"])
    def test_active_run_refused(self, mocker, state):
        adapter = patch_adapter(mocker, dag_run_module)
        adapter.get_dag_run.return_value = {"state": state}
        data = json.loads(fn(dag_run_module, "delete_dag_run")("d", "r"))
        assert data["error_type"] == "ActiveRunError"
        assert data["retryable"] is False
        assert "force" in data["hint"]
        adapter.delete_dag_run.assert_not_called()

    def test_force_deletes_active_run(self, mocker):
        adapter = patch_adapter(mocker, dag_run_module)
        adapter.get_dag_run.return_value = {"state": "running"}
        data = json.loads(fn(dag_run_module, "delete_dag_run")("d", "r", force=True))
        assert "deleted" in data["message"]
        adapter.delete_dag_run.assert_called_once()

    @pytest.mark.parametrize("state", ["success", "failed"])
    def test_finished_run_deleted_without_force(self, mocker, state):
        adapter = patch_adapter(mocker, dag_run_module)
        adapter.get_dag_run.return_value = {"state": state}
        data = json.loads(fn(dag_run_module, "delete_dag_run")("d", "r"))
        assert "deleted" in data["message"]

    def test_unknown_state_still_deletable(self, mocker):
        adapter = patch_adapter(mocker, dag_run_module)
        adapter.get_dag_run.side_effect = RuntimeError("cannot fetch")
        data = json.loads(fn(dag_run_module, "delete_dag_run")("d", "r"))
        assert "deleted" in data["message"]


# --------------------------------------------------------------------------- #
# retry only failed
# --------------------------------------------------------------------------- #
class TestRetryFailed:
    TIS = {
        "task_instances": [
            {"task_id": "ok", "state": "success"},
            {"task_id": "boom", "state": "failed"},
            {"task_id": "down", "state": "upstream_failed"},
        ]
    }

    def test_clear_tasks_without_ids_and_without_only_failed_is_rejected(self, mocker):
        adapter = patch_adapter(mocker, task_module)
        data = json.loads(fn(task_module, "clear_task_instances")("d", "r"))
        assert "only_failed" in data["hint"]
        adapter.clear_task_instances.assert_not_called()

    def test_only_failed_resolves_failed_ids(self, mocker):
        adapter = patch_adapter(mocker, task_module)
        adapter.get_all_task_instances.return_value = self.TIS
        adapter.clear_task_instances.return_value = {"task_instances": []}

        fn(task_module, "clear_task_instances")("d", "r", only_failed=True, dry_run=False)

        kwargs = adapter.clear_task_instances.call_args.kwargs
        assert kwargs["task_ids"] == ["boom", "down"]
        assert kwargs["only_failed"] is True
        assert kwargs["dry_run"] is False

    def test_only_failed_with_nothing_failed(self, mocker):
        adapter = patch_adapter(mocker, task_module)
        adapter.get_all_task_instances.return_value = {
            "task_instances": [{"task_id": "ok", "state": "success"}]
        }
        data = json.loads(fn(task_module, "clear_task_instances")("d", "r", only_failed=True))
        assert "nothing to clear" in data["message"]
        adapter.clear_task_instances.assert_not_called()

    def test_explicit_ids_unchanged(self, mocker):
        adapter = patch_adapter(mocker, task_module)
        adapter.clear_task_instances.return_value = {}
        fn(task_module, "clear_task_instances")("d", "r", task_ids=["a"])
        adapter.get_all_task_instances.assert_not_called()
        assert adapter.clear_task_instances.call_args.kwargs["task_ids"] == ["a"]

    def test_clear_dag_run_only_failed_keeps_successes(self, mocker):
        adapter = patch_adapter(mocker, task_module)
        adapter.get_all_task_instances.return_value = self.TIS
        adapter.clear_task_instances.return_value = {"task_instances": []}
        mocker.patch.object(dag_run_module, "_get_adapter", return_value=adapter)

        fn(dag_run_module, "clear_dag_run")("d", "r", dry_run=False, only_failed=True)

        adapter.clear_dag_run.assert_not_called()
        assert adapter.clear_task_instances.call_args.kwargs["include_downstream"] is True
        assert "ok" not in adapter.clear_task_instances.call_args.kwargs["task_ids"]

    def test_clear_dag_run_default_still_clears_everything(self, mocker):
        adapter = patch_adapter(mocker, dag_run_module)
        adapter.clear_dag_run.return_value = {}
        fn(dag_run_module, "clear_dag_run")("d", "r")
        adapter.clear_dag_run.assert_called_once()


# --------------------------------------------------------------------------- #
# trigger on paused DAG
# --------------------------------------------------------------------------- #
class TestTriggerPaused:
    def test_warning_when_dag_was_paused(self, mocker):
        adapter = patch_adapter(mocker, dag_run_module)
        adapter.get_dag.return_value = {"is_paused": True}
        adapter.trigger_dag_run.return_value = {"dag_run_id": "r1", "state": "queued"}
        data = json.loads(fn(dag_run_module, "trigger_dag")("d"))
        adapter.unpause_dag.assert_called_once_with("d")
        assert data["dag_was_paused"] is True
        assert "unpaused" in data["warning"]

    def test_no_warning_when_not_paused(self, mocker):
        adapter = patch_adapter(mocker, dag_run_module)
        adapter.get_dag.return_value = {"is_paused": False}
        adapter.trigger_dag_run.return_value = {"dag_run_id": "r1", "state": "queued"}
        data = json.loads(fn(dag_run_module, "trigger_dag")("d"))
        assert "warning" not in data
        adapter.unpause_dag.assert_not_called()

    def test_trigger_and_wait_surfaces_warning(self, mocker):
        adapter = patch_adapter(mocker, dag_run_module)
        adapter.get_dag.return_value = {"is_paused": True}
        adapter.trigger_dag_run.return_value = {"dag_run_id": "r1", "state": "queued"}
        adapter.get_dag_run.return_value = {"state": "success"}
        mocker.patch("time.sleep")
        data = json.loads(dag_run_module._trigger_dag_and_wait_impl("d", poll_interval=0))
        assert data["dag_was_paused"] is True


# --------------------------------------------------------------------------- #
# read-only dry-run
# --------------------------------------------------------------------------- #
class TestReadOnlyDryRun:
    @pytest.fixture
    def adapter(self, mocker, monkeypatch):
        monkeypatch.setenv("AF_READ_ONLY", "true")
        adapter = AirflowV3Adapter("http://localhost:8080", "3.1.0", token_getter=lambda: "t")
        resp = MagicMock(status_code=200)
        resp.json.return_value = {"task_instances": []}
        client = MagicMock()
        client.post.return_value = resp
        client.__enter__.return_value = client
        mocker.patch("httpx.Client", return_value=client)
        adapter.client = client
        return adapter

    def test_dry_run_clear_dag_run_allowed(self, adapter):
        assert adapter.clear_dag_run("d", "r", dry_run=True) == {"task_instances": []}
        assert adapter.client.post.call_args.kwargs["json"]["dry_run"] is True

    def test_real_clear_dag_run_blocked(self, adapter):
        from astro_airflow_mcp.adapters.base import ReadOnlyError

        with pytest.raises(ReadOnlyError):
            adapter.clear_dag_run("d", "r", dry_run=False)
        adapter.client.post.assert_not_called()

    def test_dry_run_clear_tasks_allowed_real_blocked(self, adapter):
        from astro_airflow_mcp.adapters.base import ReadOnlyError

        adapter.clear_task_instances("d", "r", ["t"], dry_run=True)
        with pytest.raises(ReadOnlyError):
            adapter.clear_task_instances("d", "r", ["t"], dry_run=False)

    def test_other_posts_still_blocked(self, adapter):
        from astro_airflow_mcp.adapters.base import ReadOnlyError

        with pytest.raises(ReadOnlyError):
            adapter.trigger_dag_run("d")


# --------------------------------------------------------------------------- #
# health endpoints
# --------------------------------------------------------------------------- #
def test_get_health_endpoint_paths(mocker):
    from astro_airflow_mcp.adapters.airflow_v2 import AirflowV2Adapter

    v3 = AirflowV3Adapter("http://x", "3.1.0", token_getter=lambda: "t")
    v2 = AirflowV2Adapter("http://x", "2.11.0")
    for adapter, expected in ((v3, "monitor/health"), (v2, "health")):
        call = mocker.patch.object(adapter, "_call", return_value={})
        adapter.get_health()
        call.assert_called_once_with(expected)


def test_404_not_retryable_end_to_end(mocker):
    adapter = patch_adapter(mocker, dag_module)
    resp = httpx.Response(404, request=httpx.Request("GET", "http://x"))
    adapter.get_dag.side_effect = httpx.HTTPStatusError("nf", request=resp.request, response=resp)
    data = json.loads(fn(dag_module, "get_dag_details")("nope"))
    assert data["retryable"] is False
