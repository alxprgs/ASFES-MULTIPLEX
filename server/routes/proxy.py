from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status, BackgroundTasks

from server.core.deps import get_current_api_user, get_services, enforce_api_rate_limit
from server.audit import audit_context_from_request
from server.models import (
    ProxyResponse,
    ProxyCreateRequest,
    ProxyCreateFromUrlRequest,
    ProxyImportProxifierRequest,
    ProxyExportProxifierRequest,
    ProxyExportProxifierResponse,
    ProxyExportUrlRequest,
    ProxyExportLinesRequest,
    ProxyExportTgRequest,
    ProxyBulkImportResult,
    ProxyCheckResult,
    ProxyCheckDetail,
    ProxyTgExportResponse,
    UserPrincipal,
)
from server.services import ApplicationServices

router = APIRouter(prefix="/proxy", tags=["proxy"])


def _to_response(doc: dict) -> ProxyResponse:
    # Build ProxyCheckResult if last_check exists
    last_check_val = None
    lc = doc.get("last_check")
    if lc:
        details_dict = {}
        for name, detail in lc.get("details", {}).items():
            details_dict[name] = ProxyCheckDetail(
                ok=detail.get("ok", False),
                latency_ms=detail.get("latency_ms"),
                external_ip=detail.get("external_ip"),
            )
        last_check_val = ProxyCheckResult(
            checked_at=lc.get("checked_at", ""),
            ok=lc.get("ok", False),
            avg_latency_ms=lc.get("avg_latency_ms"),
            details=details_dict,
        )

    return ProxyResponse(
        proxy_id=doc["_id"],
        user_id=doc["user_id"],
        protocol=doc["protocol"],
        host=doc["host"],
        port=doc["port"],
        username=doc.get("username"),
        label=doc.get("label"),
        last_check=last_check_val,
        created_at=doc["created_at"].isoformat() if hasattr(doc["created_at"], "isoformat") else str(doc["created_at"]),
    )


@router.get("/proxies", response_model=list[ProxyResponse])
async def list_proxies(
    request: Request,
    current_user: UserPrincipal = Depends(get_current_api_user),
    services: ApplicationServices = Depends(get_services),
):
    await enforce_api_rate_limit(request, services, user=current_user, suffix="proxy_read")
    docs = await services.proxy_service.list_proxies(current_user.user_id)
    return [_to_response(doc) for doc in docs]


@router.post("/proxies", response_model=ProxyResponse, status_code=status.HTTP_201_CREATED)
async def create_proxy(
    request: Request,
    payload: ProxyCreateRequest,
    current_user: UserPrincipal = Depends(get_current_api_user),
    services: ApplicationServices = Depends(get_services),
):
    await enforce_api_rate_limit(request, services, user=current_user, suffix="proxy_write")
    try:
        doc = await services.proxy_service.create_proxy(
            current_user.user_id,
            protocol=payload.protocol,
            host=payload.host,
            port=payload.port,
            username=payload.username,
            password=payload.password,
            label=payload.label,
        )
        return _to_response(doc)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/proxies/from-url", response_model=ProxyResponse, status_code=status.HTTP_201_CREATED)
async def create_proxy_from_url(
    request: Request,
    payload: ProxyCreateFromUrlRequest,
    current_user: UserPrincipal = Depends(get_current_api_user),
    services: ApplicationServices = Depends(get_services),
):
    await enforce_api_rate_limit(request, services, user=current_user, suffix="proxy_write")
    try:
        parsed = services.proxy_service.parse_proxy_url(payload.url, payload.protocol)
        doc = await services.proxy_service.create_proxy(
            current_user.user_id,
            protocol=parsed["protocol"],
            host=parsed["host"],
            port=parsed["port"],
            username=parsed["username"],
            password=parsed["password"],
            label=payload.label or parsed["host"],
        )
        return _to_response(doc)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/proxies/import/proxifier", response_model=ProxyBulkImportResult)
async def import_proxifier(
    request: Request,
    payload: ProxyImportProxifierRequest,
    current_user: UserPrincipal = Depends(get_current_api_user),
    services: ApplicationServices = Depends(get_services),
):
    await enforce_api_rate_limit(request, services, user=current_user, suffix="proxy_write")
    try:
        parsed_list = services.proxy_service.parse_proxifier_xml(payload.xml_content)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    imported = 0
    skipped = 0
    errors = []

    for item in parsed_list:
        try:
            await services.proxy_service.create_proxy(
                current_user.user_id,
                protocol=item["protocol"],
                host=item["host"],
                port=item["port"],
                username=item["username"],
                password=item["password"],
                label=f"Proxifier {item['host']}",
            )
            imported += 1
        except ValueError as exc:
            if "already exists" in str(exc):
                skipped += 1
            else:
                errors.append(f"{item['host']}:{item['port']} - {exc}")
        except Exception as exc:
            errors.append(f"{item['host']}:{item['port']} - {exc}")

    return ProxyBulkImportResult(imported=imported, skipped=skipped, errors=errors)


@router.post("/proxies/export/proxifier", response_model=ProxyExportProxifierResponse)
async def export_proxifier(
    request: Request,
    payload: ProxyExportProxifierRequest,
    current_user: UserPrincipal = Depends(get_current_api_user),
    services: ApplicationServices = Depends(get_services),
):
    await enforce_api_rate_limit(request, services, user=current_user, suffix="proxy_export")
    if not await services.users.verify_password_for_user(current_user, payload.current_password):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Password verification failed")
    proxies_to_export = []
    for pid in payload.proxy_ids:
        p = await services.proxy_service.get_proxy(current_user.user_id, pid)
        if p:
            # Decrypt password for exporting
            plain_pass = None
            if p.get("password_encrypted"):
                plain_pass = services.proxy_service.decrypt_password(p["password_encrypted"])
            proxies_to_export.append({
                "protocol": p["protocol"],
                "host": p["host"],
                "port": p["port"],
                "username": p.get("username"),
                "password": plain_pass,
            })
            
    xml = services.proxy_service.export_as_proxifier_xml(proxies_to_export)
    
    await services.audit.record(
        "proxy.export",
        actor=current_user,
        audit_ctx=audit_context_from_request(request),
        target={"user_id": current_user.user_id},
        metadata={"format": "proxifier", "count": len(proxies_to_export)},
    )
    
    return ProxyExportProxifierResponse(xml_content=xml)


@router.delete("/proxies/{proxy_id}")
async def delete_proxy(
    request: Request,
    proxy_id: str,
    current_user: UserPrincipal = Depends(get_current_api_user),
    services: ApplicationServices = Depends(get_services),
):
    await enforce_api_rate_limit(request, services, user=current_user, suffix="proxy_write")
    deleted = await services.proxy_service.delete_proxy(current_user.user_id, proxy_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Proxy not found")
    return {"status": "deleted"}


@router.post("/proxies/{proxy_id}/check", response_model=ProxyCheckResult)
async def check_proxy(
    request: Request,
    proxy_id: str,
    current_user: UserPrincipal = Depends(get_current_api_user),
    services: ApplicationServices = Depends(get_services),
):
    await enforce_api_rate_limit(request, services, user=current_user, suffix="proxy_write")
    p = await services.proxy_service.get_proxy(current_user.user_id, proxy_id)
    if not p:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Proxy not found")
    
    res = await services.proxy_service.check_proxy_single(p)
    await services.proxy_service.update_proxy_check(proxy_id, res)
    
    details_dict = {}
    for name, detail in res.get("details", {}).items():
        details_dict[name] = ProxyCheckDetail(
            ok=detail.get("ok", False),
            latency_ms=detail.get("latency_ms"),
            external_ip=detail.get("external_ip"),
        )

    return ProxyCheckResult(
        checked_at=res["checked_at"],
        ok=res["ok"],
        avg_latency_ms=res["avg_latency_ms"],
        details=details_dict,
    )


@router.post("/proxies/check-all")
async def check_all_proxies(
    request: Request,
    background_tasks: BackgroundTasks,
    current_user: UserPrincipal = Depends(get_current_api_user),
    services: ApplicationServices = Depends(get_services),
):
    await enforce_api_rate_limit(request, services, user=current_user, suffix="proxy_write")
    background_tasks.add_task(services.proxy_service.check_all_background, current_user.user_id)
    return {"status": "started"}


@router.post("/proxies/{proxy_id}/export/url")
async def export_url(
    request: Request,
    proxy_id: str,
    payload: ProxyExportUrlRequest,
    current_user: UserPrincipal = Depends(get_current_api_user),
    services: ApplicationServices = Depends(get_services),
):
    await enforce_api_rate_limit(request, services, user=current_user, suffix="proxy_export")
    if not await services.users.verify_password_for_user(current_user, payload.current_password):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Password verification failed")

    p = await services.proxy_service.get_proxy(current_user.user_id, proxy_id)
    if not p:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Proxy not found")
        
    plain_pass = None
    if p.get("password_encrypted"):
        plain_pass = services.proxy_service.decrypt_password(p["password_encrypted"])
        
    formatted = services.proxy_service.export_as_url(p, plain_pass)
    
    await services.audit.record(
        "proxy.export",
        actor=current_user,
        audit_ctx=audit_context_from_request(request),
        target={"proxy_id": proxy_id},
        metadata={"format": "url"},
    )
    return {"url": formatted}


@router.post("/proxies/{proxy_id}/export/lines")
async def export_lines(
    request: Request,
    proxy_id: str,
    payload: ProxyExportLinesRequest,
    current_user: UserPrincipal = Depends(get_current_api_user),
    services: ApplicationServices = Depends(get_services),
):
    await enforce_api_rate_limit(request, services, user=current_user, suffix="proxy_export")
    if not await services.users.verify_password_for_user(current_user, payload.current_password):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Password verification failed")

    p = await services.proxy_service.get_proxy(current_user.user_id, proxy_id)
    if not p:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Proxy not found")
        
    plain_pass = None
    if p.get("password_encrypted"):
        plain_pass = services.proxy_service.decrypt_password(p["password_encrypted"])
        
    formatted = services.proxy_service.export_as_lines(p, plain_pass)

    await services.audit.record(
        "proxy.export",
        actor=current_user,
        audit_ctx=audit_context_from_request(request),
        target={"proxy_id": proxy_id},
        metadata={"format": "lines"},
    )
    return {"lines": formatted}


@router.post("/proxies/{proxy_id}/export/tg", response_model=ProxyTgExportResponse)
async def export_tg(
    request: Request,
    proxy_id: str,
    payload: ProxyExportTgRequest,
    current_user: UserPrincipal = Depends(get_current_api_user),
    services: ApplicationServices = Depends(get_services),
):
    await enforce_api_rate_limit(request, services, user=current_user, suffix="proxy_export")
    if not await services.users.verify_password_for_user(current_user, payload.current_password):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Password verification failed")

    p = await services.proxy_service.get_proxy(current_user.user_id, proxy_id)
    if not p:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Proxy not found")
        
    if p["protocol"] != "socks5":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="TG proxy export is only supported for SOCKS5 proxies",
        )
        
    res = services.proxy_service.export_as_tg_proxy(p, payload.secret)

    await services.audit.record(
        "proxy.export",
        actor=current_user,
        audit_ctx=audit_context_from_request(request),
        target={"proxy_id": proxy_id},
        metadata={"format": "tg"},
    )
    return ProxyTgExportResponse(
        deep_link=res["deep_link"],
        web_url=res["web_url"],
    )
