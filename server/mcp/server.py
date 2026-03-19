from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, Request, Response, status
from fastapi.responses import JSONResponse

from server.core.deps import get_current_mcp_user, get_services
from server.models import UserPrincipal
from server.services import ApplicationServices, request_meta_from_request


router = APIRouter(tags=["mcp"])


def _success(request_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _error(request_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def _wrap_tool_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False, indent=2)}],
        "structuredContent": result,
        "isError": False,
    }


async def _handle_single_message(message: dict[str, Any], request: Request, services: ApplicationServices, user: UserPrincipal) -> dict[str, Any] | None:
    request_id = message.get("id")
    method = message.get("method")
    params = message.get("params") or {}
    request_meta = request_meta_from_request(request)

    try:
        if method == "initialize":
            return _success(
                request_id,
                {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": services.settings.app.name, "version": services.settings.app.version},
                    "instructions": "Multiplex MCP server. Authenticate with OAuth, then call tools that are enabled for your account.",
                },
            )
        if method in {"notifications/initialized", "notifications/cancelled"}:
            return None
        if method == "ping":
            return _success(request_id, {})
        if method == "tools/list":
            return _success(request_id, {"tools": await services.plugins.describe_tools_for_user(user)})
        if method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments") or {}
            if not tool_name:
                return _error(request_id, -32602, "Missing tool name")
            runtime = await services.settings_service.get_runtime_settings()
            if not runtime.get("mcp_enabled", True):
                return _error(request_id, -32000, "MCP is globally disabled")
            result = await services.plugins.call_tool(user, str(tool_name), arguments, request_meta)
            return _success(request_id, _wrap_tool_result(result))
        return _error(request_id, -32601, f"Method '{method}' is not supported")
    except PermissionError as exc:
        return _error(request_id, -32001, str(exc))
    except LookupError as exc:
        return _error(request_id, -32602, str(exc))
    except Exception as exc:
        return _error(request_id, -32000, str(exc))


@router.post("")
@router.post("/")
async def handle_mcp(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_mcp_user),
) -> Response:
    payload = await request.json()
    if isinstance(payload, list):
        results = []
        for item in payload:
            result = await _handle_single_message(item, request, services, current_user)
            if result is not None:
                results.append(result)
        if not results:
            return Response(status_code=status.HTTP_202_ACCEPTED)
        return JSONResponse(results)
    result = await _handle_single_message(payload, request, services, current_user)
    if result is None:
        return Response(status_code=status.HTTP_202_ACCEPTED)
    return JSONResponse(result)


@router.get("")
@router.get("/")
async def mcp_info(
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_mcp_user),
) -> dict[str, Any]:
    tools = await services.plugins.describe_tools_for_user(current_user)
    return {
        "name": services.settings.app.name,
        "version": services.settings.app.version,
        "transport": "streamable-http",
        "tool_count": len(tools),
    }
