from __future__ import annotations

import shutil
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from pydantic import SecretStr

from server.core.config import settings as base_settings
from server.core.logging import IntegrityLogManager, Mailer
from server.mcp import create_mcp_gateway
from server.routes import api_router, root_router
from server.services import build_application_services, shutdown_application_services


def make_test_settings():
    unique = uuid4().hex
    workspace = Path.cwd() / ".test_runtime" / unique
    cfg = base_settings.model_copy(deep=True)
    cfg.app.env = "test"
    cfg.app.dev = True
    cfg.app.startup_progress = False
    cfg.mongo.database = f"{base_settings.mongo.database}_test_{unique}"
    cfg.redis.mode = "disabled"
    cfg.redis.url = None
    cfg.redis.enabled_on_startup = False
    cfg.smtp.enabled = False
    cfg.root.username = "root"
    cfg.root.password = SecretStr("IntegrationRootPass123!")
    cfg.root.email = "root.integration@example.com"
    cfg.logging.directory = workspace / "logs"
    cfg.logging.sqlite_path = workspace / "logs.db"
    cfg.host_ops.managed_file_roots = [workspace / "managed"]
    cfg.host_ops.managed_log_roots = [workspace / "managed" / "logs"]
    cfg.host_ops.backup_directory = workspace / "backups"
    cfg.host_ops.database_profiles_directory = workspace / "profiles" / "databases"
    cfg.host_ops.vpn_profiles_directory = workspace / "profiles" / "vpn"
    cfg.host_ops.ssl_profiles_directory = workspace / "profiles" / "ssl"
    cfg.host_ops.nginx_config_paths = [workspace / "managed" / "nginx"]
    return cfg, workspace


@pytest_asyncio.fixture
async def integration_env():
    cfg, workspace = make_test_settings()
    workspace.mkdir(parents=True, exist_ok=True)
    mailer = Mailer(cfg.smtp)
    logger_manager = IntegrityLogManager(cfg.logging, mailer, str(cfg.root.email))
    logger_manager.initialize()
    services = None
    try:
        services = await build_application_services(cfg, logger_manager, mailer)
    except Exception as exc:
        logger_manager.finalize()
        shutil.rmtree(workspace, ignore_errors=True)
        pytest.skip(f"Mongo-backed integration environment is unavailable: {exc}")

    app = FastAPI()
    app.state.services = services
    mcp_gateway = create_mcp_gateway(cfg, lambda: app.state.services)
    app.state.mcp_gateway = mcp_gateway
    app.include_router(root_router)
    app.include_router(api_router, prefix=cfg.api_prefix)
    app.mount(cfg.mcp_path, mcp_gateway.http_app)

    transport = httpx.ASGITransport(app=app)
    await mcp_gateway.refresh_tools()
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        try:
            yield {
                "app": app,
                "settings": cfg,
                "workspace": workspace,
                "services": services,
                "client": client,
                "mcp_gateway": mcp_gateway,
            }
        finally:
            if services.db.client is not None:
                await services.db.client.drop_database(cfg.mongo.database)
            await shutdown_application_services(services)
            logger_manager.finalize()
            shutil.rmtree(workspace, ignore_errors=True)
