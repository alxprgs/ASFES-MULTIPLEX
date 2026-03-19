from __future__ import annotations

from fastapi import APIRouter, Depends

from server.core.deps import get_services
from server.models import HealthResponse
from server.services import ApplicationServices


router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def healthcheck(services: ApplicationServices = Depends(get_services)) -> HealthResponse:
    mongodb = "ok"
    try:
        if services.db.client is not None:
            await services.db.client.admin.command("ping")
    except Exception:
        mongodb = "error"
    runtime = await services.settings_service.get_runtime_settings()
    redis = "enabled" if services.rate_limiter.should_use_redis() else "disabled"
    return HealthResponse(status="ok" if mongodb == "ok" else "degraded", mongodb=mongodb, redis=redis, mcp_enabled=bool(runtime.get("mcp_enabled", True)))
