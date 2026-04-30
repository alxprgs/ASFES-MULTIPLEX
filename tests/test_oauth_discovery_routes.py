from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI, Response
from fastapi.testclient import TestClient

from server.app import ExactPathSlashMiddleware
from server.core.config import Settings
from server.routes import root_router
from server.services import OAuthService


def test_exact_mcp_path_is_rewritten_before_routing() -> None:
    app = FastAPI()
    app.add_middleware(ExactPathSlashMiddleware, path="/mcp")

    @app.api_route("/mcp/", methods=["GET", "POST"])
    async def mcp_probe() -> Response:
        return Response(
            status_code=401,
            headers={"WWW-Authenticate": 'Bearer resource_metadata="http://testserver/.well-known/oauth-protected-resource/mcp"'},
        )

    client = TestClient(app)

    bare_get = client.get("/mcp")
    assert bare_get.status_code == 401
    assert "resource_metadata=" in bare_get.headers["www-authenticate"]

    bare_post = client.post("/mcp", json={})
    assert bare_post.status_code == 401
    assert "resource_metadata=" in bare_post.headers["www-authenticate"]

    slash_get = client.get("/mcp/")
    assert slash_get.status_code == 401
    assert slash_get.headers["www-authenticate"] == bare_get.headers["www-authenticate"]


def test_oauth_well_known_metadata_routes_without_runtime_services() -> None:
    cfg = Settings(_env_file=None)
    app = FastAPI()
    app.state.services = SimpleNamespace(
        settings=cfg,
        oauth=OAuthService(db=None, settings=cfg, users=None, audit=None),
    )
    app.include_router(root_router)
    client = TestClient(app)

    protected = client.get("/.well-known/oauth-protected-resource/mcp")
    assert protected.status_code == 200
    assert protected.json()["scopes_supported"] == cfg.oauth.supported_scopes

    issuer_metadata = client.get("/.well-known/oauth-authorization-server/api/oauth")
    assert issuer_metadata.status_code == 200
    assert issuer_metadata.json()["issuer"] == cfg.oauth_issuer

    missing = client.get("/.well-known/oauth-authorization-server/other")
    assert missing.status_code == 404
