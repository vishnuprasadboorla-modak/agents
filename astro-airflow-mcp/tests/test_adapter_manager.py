"""Tests for AdapterManager wiring of auth flows."""

from __future__ import annotations

from astro_airflow_mcp.adapter_manager import AdapterManager, _extract_bearer_token
from astro_airflow_mcp.astro_pat import AstroPATAuth


class TestAdapterManagerAuthRouting:
    def test_astro_pat_constructs_pat_auth(self):
        m = AdapterManager()
        m.configure(
            url="https://example.com",
            auth_kind="astro_pat",
            astro_context="astronomer.io",
        )
        assert isinstance(m._auth_handler, AstroPATAuth)
        # The token_manager should NOT be created in this mode.
        assert m._token_manager is None
        assert m._auth_token is None

    def test_static_token_does_not_construct_pat_auth(self):
        m = AdapterManager()
        m.configure(url="https://example.com", auth_token="static-bearer")
        assert m._auth_handler is None
        assert m._auth_token == "static-bearer"

    def test_basic_auth_does_not_construct_pat_auth(self):
        m = AdapterManager()
        m.configure(url="https://example.com", username="u", password="p")
        assert m._auth_handler is None
        assert m._auth_token is None
        assert m._token_manager is not None

    def test_reconfigure_clears_prior_auth(self):
        # First configure for PAT, then for static token: prior PAT handler
        # must be cleared so we don't double-attach.
        m = AdapterManager()
        m.configure(url="https://x", auth_kind="astro_pat", astro_context="astronomer.io")
        assert isinstance(m._auth_handler, AstroPATAuth)
        m.configure(url="https://x", auth_token="t")
        assert m._auth_handler is None
        assert m._auth_token == "t"

    def test_pat_with_no_context_uses_active(self):
        # No astro_context → resolver uses active context from disk at
        # request time. Manager should still construct the handler.
        m = AdapterManager()
        m.configure(url="https://example.com", auth_kind="astro_pat")
        assert isinstance(m._auth_handler, AstroPATAuth)
        assert m._auth_handler._resolver.domain is None

    def test_get_adapter_passes_handler_through(self, mocker):
        # AdapterManager.get_adapter() must thread auth_handler into
        # create_adapter so the adapter uses it on every HTTP call.
        m = AdapterManager()
        m.configure(
            url="https://example.com",
            auth_kind="astro_pat",
            astro_context="astronomer.io",
        )

        captured = {}

        def fake_create_adapter(**kwargs):
            captured.update(kwargs)
            adapter = mocker.Mock()
            adapter.version = "3.2.0"
            return adapter

        mocker.patch(
            "astro_airflow_mcp.adapter_manager.create_adapter",
            side_effect=fake_create_adapter,
        )
        m.get_adapter()
        assert isinstance(captured["auth_handler"], AstroPATAuth)


class TestExtractBearerToken:
    def test_no_active_request_returns_none(self, mocker):
        # get_http_headers() never raises; it returns {} with no HTTP
        # request (stdio transport, or no active request in this context).
        mocker.patch("astro_airflow_mcp.adapter_manager.get_http_headers", return_value={})
        assert _extract_bearer_token() is None

    def test_bearer_header_is_extracted(self, mocker):
        mocker.patch(
            "astro_airflow_mcp.adapter_manager.get_http_headers",
            return_value={"authorization": "Bearer user-jwt-123"},
        )
        assert _extract_bearer_token() == "user-jwt-123"

    def test_non_bearer_scheme_is_ignored(self, mocker):
        mocker.patch(
            "astro_airflow_mcp.adapter_manager.get_http_headers",
            return_value={"authorization": "Basic dXNlcjpwYXNz"},
        )
        assert _extract_bearer_token() is None

    def test_requests_authorization_explicitly(self, mocker):
        # authorization is in FastMCP's default header-exclusion list, so it
        # must be requested via `include`, not returned by default.
        mock_headers = mocker.patch(
            "astro_airflow_mcp.adapter_manager.get_http_headers", return_value={}
        )
        _extract_bearer_token()
        mock_headers.assert_called_once_with(include={"authorization"})


class TestGetAuthTokenPerRequestForwarding:
    def test_forwarded_token_takes_precedence_over_static_token(self, mocker):
        m = AdapterManager()
        m.configure(url="https://example.com", auth_token="server-configured-token")
        mocker.patch(
            "astro_airflow_mcp.adapter_manager._extract_bearer_token",
            return_value="caller-own-token",
        )
        assert m._get_auth_token() == "caller-own-token"

    def test_forwarded_token_takes_precedence_over_token_manager(self, mocker):
        m = AdapterManager()
        m.configure(url="https://example.com", username="u", password="p")
        mocker.patch(
            "astro_airflow_mcp.adapter_manager._extract_bearer_token",
            return_value="caller-own-token",
        )
        get_token = mocker.patch.object(m._token_manager, "get_token")
        assert m._get_auth_token() == "caller-own-token"
        get_token.assert_not_called()

    def test_falls_back_to_static_token_when_nothing_forwarded(self, mocker):
        m = AdapterManager()
        m.configure(url="https://example.com", auth_token="server-configured-token")
        mocker.patch("astro_airflow_mcp.adapter_manager._extract_bearer_token", return_value=None)
        assert m._get_auth_token() == "server-configured-token"

    def test_falls_back_to_token_manager_when_nothing_forwarded(self, mocker):
        m = AdapterManager()
        m.configure(url="https://example.com")
        mocker.patch("astro_airflow_mcp.adapter_manager._extract_bearer_token", return_value=None)
        mocker.patch.object(m._token_manager, "get_token", return_value="admin-fallback-token")
        assert m._get_auth_token() == "admin-fallback-token"

    def test_returns_none_when_fully_unconfigured_and_nothing_forwarded(self, mocker):
        m = AdapterManager()
        mocker.patch("astro_airflow_mcp.adapter_manager._extract_bearer_token", return_value=None)
        assert m._get_auth_token() is None
