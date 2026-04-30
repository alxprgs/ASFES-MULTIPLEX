from __future__ import annotations

from fastapi import APIRouter, Depends

from server.core.deps import get_services, require_permission
from server.models import HealthDetailsResponse, HealthResponse, UserPrincipal
from server.services import ApplicationServices


router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def healthcheck(services: ApplicationServices = Depends(get_services)) -> HealthResponse:
    mongodb = await _mongodb_status(services)
    return HealthResponse(status="ok" if mongodb == "ok" else "degraded")


@router.get("/health/details", response_model=HealthDetailsResponse)
async def healthcheck_details(
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("system.health.read")),
) -> HealthDetailsResponse:
    mongodb = await _mongodb_status(services)
    runtime = await services.settings_service.get_runtime_settings()
    redis = "enabled" if services.rate_limiter.should_use_redis() else "disabled"
    return HealthDetailsResponse(status="ok" if mongodb == "ok" else "degraded", mongodb=mongodb, redis=redis, mcp_enabled=bool(runtime.get("mcp_enabled", True)))


async def _mongodb_status(services: ApplicationServices) -> str:
    mongodb = "ok"
    try:
        if services.db.client is not None:
            await services.db.client.admin.command("ping")
    except Exception:
        mongodb = "error"
    return mongodb
