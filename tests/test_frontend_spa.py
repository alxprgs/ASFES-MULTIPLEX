import shutil
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.app import mount_frontend
from server.core.config import settings as base_settings


def test_spa_fallback_serves_frontend_without_capturing_api_paths() -> None:
    workspace = Path.cwd() / ".test_runtime" / f"spa-{uuid4().hex}"
    frontend_dist = workspace / "dist"
    try:
        assets_dir = frontend_dist / "assets"
        assets_dir.mkdir(parents=True)
        (frontend_dist / "index.html").write_text("<main>ASFES Multiplex UI</main>", encoding="utf-8")

        cfg = base_settings.model_copy(deep=True)
        cfg.app.frontend_dist = frontend_dist

        app = FastAPI()
        mount_frontend(app, cfg)
        client = TestClient(app)

        assert client.get("/").status_code == 200
        assert "ASFES Multiplex UI" in client.get("/dashboard").text
        assert client.get("/api/missing").status_code == 404
        assert client.get("/mcp/missing").status_code == 404
        assert client.get("/.well-known/missing").status_code == 404
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
