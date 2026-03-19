from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from server.core.config import settings
from server.core.logging import IntegrityLogManager, Mailer, get_logger
from server.mcp import create_mcp_gateway
from server.routes import api_router, root_router
from server.services import build_application_services, periodic_integrity_verifier, shutdown_application_services


LOGGER = get_logger("multiplex.startup")


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
    mcp_gateway = create_mcp_gateway(settings, lambda: app.state.services)
    app.state.mcp_gateway = mcp_gateway
    app.include_router(root_router)
    app.include_router(api_router, prefix=settings.api_prefix)
    app.mount(settings.mcp_path, mcp_gateway.http_app)
    return app


app = create_app()
