from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from server.app import ExactPathSlashMiddleware, create_app, mount_frontend, lifespan
from server.core.config import settings


@pytest.mark.asyncio
async def test_exact_path_slash_middleware() -> None:
    called_scopes = []
    async def mock_app(scope, receive, send):
        called_scopes.append(scope)
        return

    middleware = ExactPathSlashMiddleware(mock_app, "/mcp")

    # Case 1: matches exact path without slash
    scope1 = {"type": "http", "path": "/mcp"}
    await middleware(scope1, None, None)
    assert len(called_scopes) == 1
    assert called_scopes[0]["path"] == "/mcp/"
    assert called_scopes[0]["raw_path"] == b"/mcp/"

    # Case 2: does not match because type is not http
    scope2 = {"type": "websocket", "path": "/mcp"}
    await middleware(scope2, None, None)
    assert called_scopes[1]["path"] == "/mcp"

    # Case 3: does not match because path is different
    scope3 = {"type": "http", "path": "/api"}
    await middleware(scope3, None, None)
    assert called_scopes[2]["path"] == "/api"


def test_create_app() -> None:
    app = create_app()
    assert isinstance(app, FastAPI)
    assert app.title == "ASFES Multiplex"


@pytest.mark.asyncio
async def test_serve_frontend_blocked_paths() -> None:
    app = FastAPI()
    test_settings = settings.model_copy(deep=True)
    test_settings.app.api_prefix = "/api"
    test_settings.app.mcp_path = "/mcp"
    test_settings.app.frontend_dist = Path("C:/fake/dist")

    mount_frontend(app, test_settings)

    # Get the route handler registered
    route = next(r for r in app.routes if getattr(r, "name", None) == "serve_frontend")
    handler = route.endpoint

    # Case 1: blocked prefix /api
    with pytest.raises(HTTPException) as exc:
        await handler(frontend_path="api/users")
    assert exc.value.status_code == 404

    # Case 2: blocked prefix /mcp
    with pytest.raises(HTTPException) as exc:
        await handler(frontend_path="mcp/tools")
    assert exc.value.status_code == 404

    # Case 3: blocked prefix .well-known
    with pytest.raises(HTTPException) as exc:
        await handler(frontend_path=".well-known/jwks.json")
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_serve_frontend_not_built_and_built(tmp_path) -> None:
    app = FastAPI()
    test_settings = settings.model_copy(deep=True)
    test_settings.app.frontend_dist = tmp_path

    mount_frontend(app, test_settings)
    route = next(r for r in app.routes if getattr(r, "name", None) == "serve_frontend")
    handler = route.endpoint

    # Case 1: index.html does not exist
    with pytest.raises(HTTPException) as exc:
        await handler(frontend_path="dashboard")
    assert exc.value.status_code == 404
    assert "Frontend bundle is not built" in exc.value.detail

    # Case 2: index.html exists
    index_file = tmp_path / "index.html"
    index_file.write_text("<html></html>", encoding="utf-8")
    res = await handler(frontend_path="dashboard")
    assert isinstance(res, FileResponse)
    assert Path(res.path) == index_file


@pytest.mark.asyncio
async def test_lifespan_context() -> None:
    app = FastAPI()
    app.state.mcp_gateway = MagicMock()
    app.state.mcp_gateway.refresh_tools = AsyncMock()
    app.state.mcp_gateway.lifespan = MagicMock()
    app.state.mcp_gateway.lifespan.return_value.__aenter__ = AsyncMock()
    app.state.mcp_gateway.lifespan.return_value.__aexit__ = AsyncMock()

    mock_services = MagicMock()
    mock_services.verifier_task = None

    with patch("server.app.IntegrityLogManager") as mock_log_mgr_cls, \
         patch("server.app.build_application_services", return_value=mock_services) as mock_build_svcs, \
         patch("server.app.periodic_integrity_verifier") as mock_verifier, \
         patch("server.app.shutdown_application_services") as mock_shutdown_svcs:

        # Temporarily change startup_progress to False for easier testing
        with patch("server.app.settings") as mock_settings:
            mock_settings.app.startup_progress = False
            mock_settings.smtp.enabled = False

            async with lifespan(app):
                pass

        mock_build_svcs.assert_called_once()
        mock_shutdown_svcs.assert_called_once_with(mock_services)
        mock_log_mgr_cls.return_value.initialize.assert_called_once()
        mock_log_mgr_cls.return_value.finalize.assert_called_once()
