"""Observability routes for ASFES Multiplex.

Exposes the Prometheus metrics endpoint at GET /api/metrics.
Access is controlled by ``settings.observability.metrics_public``:
- If true, the endpoint is accessible without any authentication.
- If false (default), the caller must have the ``system.metrics.read`` permission.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import Response

from server.core.deps import get_optional_api_user, get_services
from server.models import UserPrincipal
from server.services import ApplicationServices

router = APIRouter(tags=["observability"])


@router.get(
    "/metrics",
    include_in_schema=False,
    summary="Prometheus metrics",
)
async def prometheus_metrics(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    user: UserPrincipal | None = Depends(get_optional_api_user),
) -> Response:
    """Expose Prometheus metrics in text exposition format.

    The endpoint requires the ``system.metrics.read`` permission unless
    ``OBSERVABILITY__METRICS_PUBLIC=true``.
    """
    if not services.observability.prometheus_enabled:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Prometheus metrics are not enabled. "
            "Set OBSERVABILITY__PROMETHEUS_ENABLED=true to enable.",
        )

    if not services.settings.observability.metrics_public:
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required to access metrics. "
                "Set OBSERVABILITY__METRICS_PUBLIC=true for unauthenticated scraping.",
            )
        if not user.is_root and "system.metrics.read" not in user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Permission 'system.metrics.read' is required.",
            )

    try:
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

        from server.observability.metrics import REGISTRY

        content = generate_latest(REGISTRY)
        return Response(content=content, media_type=CONTENT_TYPE_LATEST)
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="prometheus-client package is not installed.",
        )
