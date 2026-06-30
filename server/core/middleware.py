import time
import uuid

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from server.core.logging import get_logger

LOGGER = get_logger("multiplex.access")


class AuditMetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start_time = time.time()
        
        # Generate the root correlation ID for the entire request lifecycle
        correlation_id = str(uuid.uuid4())
        request.state.correlation_id = correlation_id
        
        response = await call_next(request)
        
        process_time = time.time() - start_time
        
        # HTTP boundary logging (access log style) - this is TRACING, not AUDIT
        # We don't save this to the DB, just standard output for APM/observability
        client_ip = request.client.host if request.client else "unknown"
        LOGGER.info(
            f"{client_ip} - \"{request.method} {request.url.path}\" {response.status_code} {process_time:.3f}s",
            extra={
                "correlation_id": correlation_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_s": process_time,
                "client_ip": client_ip,
            }
        )
        
        # Optionally inject correlation ID into response headers
        response.headers["X-Correlation-ID"] = correlation_id
        
        return response
