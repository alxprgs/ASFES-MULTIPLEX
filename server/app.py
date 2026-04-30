from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from starlette.types import ASGIApp, Receive, Scope, Send

from server.core.config import Settings, settings
from server.core.logging import IntegrityLogManager, Mailer, get_logger
from server.mcp import create_mcp_gateway
from server.routes import api_router, root_router
from server.services import build_application_services, periodic_integrity_verifier, shutdown_application_services


LOGGER = get_logger("multiplex.startup")


class ExactPathSlashMiddleware:
    def __init__(self, app: ASGIApp, path: str) -> None:
        self.app = app
        self.path = path.rstrip("/") or "/"

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope.get("path") == self.path:
            scope = dict(scope)
            scope["path"] = f"{self.path}/"
            scope["raw_path"] = scope["path"].encode("ascii")
        await self.app(scope, receive, send)


@asynccontextmanager
async def lifespan(app: FastAPI):
    mailer = Mailer(settings.smtp)
    logger_manager = IntegrityLogManager(settings.logging, mailer, str(settings.root.email))
    logger_manager.initialize()
    services = None
    try:
        if settings.app.startup_progress:
            console = Console()
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                transient=True,
                console=console,
            ) as progress:
                task_id = progress.add_task("Preparing Multiplex logger", total=4)
                progress.update(task_id, completed=1, description="Connecting MongoDB and creating indexes")
                services = await build_application_services(settings, logger_manager, mailer)
                progress.update(task_id, completed=2, description="Loading plugins and permissions")
                progress.update(task_id, completed=3, description="Starting integrity verifier")
                services.verifier_task = asyncio.create_task(periodic_integrity_verifier(services))
                progress.update(task_id, completed=4, description="Startup complete")
        else:
            services = await build_application_services(settings, logger_manager, mailer)
            services.verifier_task = asyncio.create_task(periodic_integrity_verifier(services))

        app.state.services = services
        await app.state.mcp_gateway.refresh_tools()
        async with app.state.mcp_gateway.lifespan():
            LOGGER.info(
                "Multiplex application startup completed",
                extra={
                    "event_type": "app.startup",
                    "payload": {
                        "api_prefix": settings.api_prefix,
                        "mcp_path": settings.mcp_path,
                        "redis_mode": settings.redis.mode,
                    },
                },
            )
            yield
    finally:
        if services is not None:
            await shutdown_application_services(services)
        logger_manager.finalize()


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app.name,
        version=settings.app.version,
        docs_url=f"{settings.api_prefix}/docs",
        openapi_url=f"{settings.api_prefix}/openapi.json",
        redoc_url=f"{settings.api_prefix}/redoc",
        lifespan=lifespan,
    )
    app.add_middleware(ExactPathSlashMiddleware, path=settings.mcp_path)
    mcp_gateway = create_mcp_gateway(settings, lambda: app.state.services)
    app.state.mcp_gateway = mcp_gateway
    app.include_router(root_router)
    app.include_router(api_router, prefix=settings.api_prefix)
    app.mount(settings.mcp_path, mcp_gateway.http_app)
    mount_frontend(app, settings)
    return app


def mount_frontend(app: FastAPI, app_settings: Settings) -> None:
    frontend_dist = app_settings.app.frontend_dist
    assets_dir = frontend_dist / "assets"
    app.mount("/assets", StaticFiles(directory=str(assets_dir), check_dir=False), name="frontend-assets")

    @app.get("/", include_in_schema=False)
    @app.get("/{frontend_path:path}", include_in_schema=False)
    async def serve_frontend(frontend_path: str = "") -> FileResponse:
        blocked_prefixes = (
            app_settings.api_prefix.strip("/"),
            app_settings.mcp_path.strip("/"),
            ".well-known",
        )
        first_segment = frontend_path.split("/", 1)[0] if frontend_path else ""
        if first_segment in blocked_prefixes:
            raise HTTPException(status_code=404, detail="Not found")

        index_path = frontend_dist / "index.html"
        if not index_path.exists():
            raise HTTPException(status_code=404, detail="Frontend bundle is not built")
        return FileResponse(index_path)


app = create_app()
