"""
PyPI Mirror routes.

Two separate routers:
- management_router: REST API for managing the mirror (/api/pypi/…)
- simple_router: pip-compatible Simple Repository API (/pypi/simple/…)
"""

from __future__ import annotations
from server.audit import audit_context_from_request

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Request,
    status,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import FileResponse, HTMLResponse

from server.core.deps import enforce_api_rate_limit, get_services, require_permission
from server.models import (
    PyPIBlocklistResponse,
    PyPIBlockRequest,
    PyPIBulkInstallRequest,
    PyPIInstallRequest,
    PyPIJobStatus,
    PyPIPackage,
    PyPIPackageListResponse,
    PyPIStatsResponse,
    UserPrincipal,
)
from server.services import ApplicationServices


# ---------------------------------------------------------------------------
# Management API — mounted inside api_router (/api/pypi/…)
# ---------------------------------------------------------------------------

management_router = APIRouter(prefix="/pypi", tags=["pypi"])


@management_router.get("/stats", response_model=PyPIStatsResponse)
async def pypi_stats(
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.read")),
    services: ApplicationServices = Depends(get_services),
) -> PyPIStatsResponse:
    """Return storage statistics for the PyPI mirror."""
    _check_enabled(services)
    return await services.pypi_mirror.get_stats()


@management_router.get("/packages", response_model=PyPIPackageListResponse)
async def pypi_packages(
    request: Request,
    search: str = "",
    page: int = 1,
    per_page: int = 25,
    current_user: UserPrincipal = Depends(require_permission("pypi.read")),
    services: ApplicationServices = Depends(get_services),
) -> PyPIPackageListResponse:
    """List packages in the mirror with optional search and pagination."""
    _check_enabled(services)
    return await services.pypi_mirror.list_packages(
        search=search, page=page, per_page=per_page
    )


@management_router.get("/packages/{name}", response_model=PyPIPackage)
async def pypi_package_detail(
    name: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.read")),
    services: ApplicationServices = Depends(get_services),
) -> PyPIPackage:
    """Return detailed information and version list for a single package."""
    _check_enabled(services)
    pkg = await services.pypi_mirror.get_package(name)
    if pkg is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Package not found"
        )
    return pkg


@management_router.post(
    "/packages/install",
    response_model=PyPIJobStatus,
    status_code=status.HTTP_202_ACCEPTED,
)
async def pypi_install(
    payload: PyPIInstallRequest,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.manage")),
    services: ApplicationServices = Depends(get_services),
) -> PyPIJobStatus:
    """
    Install a package into the mirror.
    If version is omitted — all available versions are downloaded.
    """
    _check_enabled(services)
    if payload.version:
        job = services.pypi_mirror.install_version(
            payload.name, payload.version, payload.with_dependencies
        )
    else:
        job = services.pypi_mirror.install_all_versions(
            payload.name, payload.with_dependencies
        )
    return job.to_status()


@management_router.post(
    "/packages/bulk-install",
    response_model=PyPIJobStatus,
    status_code=status.HTTP_202_ACCEPTED,
)
async def pypi_bulk_install(
    payload: PyPIBulkInstallRequest,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.manage")),
    services: ApplicationServices = Depends(get_services),
) -> PyPIJobStatus:
    """
    Bulk-install a list of packages.
    Accepts specs like 'flask==2.0.0', 'requests', 'django>=4.0'.
    """
    _check_enabled(services)
    job = services.pypi_mirror.bulk_install(payload.packages, payload.with_dependencies)
    return job.to_status()


@management_router.post(
    "/packages/sync-all",
    response_model=PyPIJobStatus,
    status_code=status.HTTP_202_ACCEPTED,
)
async def pypi_sync_all_packages(
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.manage")),
    services: ApplicationServices = Depends(get_services),
) -> PyPIJobStatus:
    """Sync all versions for all locally cached packages."""
    _check_enabled(services)

    packages = await services.pypi_mirror.list_packages("", 1, 999999)
    names = [pkg.name for pkg in packages.items]

    if not names:
        raise HTTPException(status_code=400, detail="No local packages to sync")

    job = services.pypi_mirror.bulk_install(names, with_dependencies=False)
    return job.to_status()


@management_router.delete("/packages/{name}", status_code=status.HTTP_200_OK)
async def pypi_delete_package(
    name: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.manage")),
    services: ApplicationServices = Depends(get_services),
) -> dict:
    """Delete an entire package and all its versions from the mirror."""
    _check_enabled(services)
    request_meta = audit_context_from_request(request)
    deleted = await services.pypi_mirror.delete_package(
        name, actor=current_user, request_meta=request_meta
    )
    return {"ok": deleted}


@management_router.delete(
    "/packages/{name}/versions/{version}",
    status_code=status.HTTP_200_OK,
)
async def pypi_delete_version(
    name: str,
    version: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.manage")),
    services: ApplicationServices = Depends(get_services),
) -> dict:
    """Delete a specific version of a package from the mirror."""
    _check_enabled(services)
    request_meta = audit_context_from_request(request)
    deleted = await services.pypi_mirror.delete_version(
        name, version, actor=current_user, request_meta=request_meta
    )
    return {"ok": deleted}


@management_router.get("/blocklist", response_model=PyPIBlocklistResponse)
async def pypi_get_blocklist(
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.read")),
    services: ApplicationServices = Depends(get_services),
) -> PyPIBlocklistResponse:
    """Return the current package/version blocklist."""
    _check_enabled(services)
    return await services.pypi_mirror.get_blocklist()


@management_router.post("/blocklist", status_code=status.HTTP_201_CREATED)
async def pypi_block(
    payload: PyPIBlockRequest,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.manage")),
    services: ApplicationServices = Depends(get_services),
) -> dict:
    """Block a package or a specific version from being served by the mirror."""
    _check_enabled(services)
    request_meta = audit_context_from_request(request)
    await services.pypi_mirror.block(
        payload.name, payload.version, actor=current_user, request_meta=request_meta
    )
    return {"ok": True}


@management_router.delete("/blocklist/{name}", status_code=status.HTTP_200_OK)
async def pypi_unblock_package(
    name: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.manage")),
    services: ApplicationServices = Depends(get_services),
) -> dict:
    """Remove a package (and all its versions) from the blocklist."""
    _check_enabled(services)
    request_meta = audit_context_from_request(request)
    await services.pypi_mirror.unblock(
        name, actor=current_user, request_meta=request_meta
    )
    return {"ok": True}


@management_router.delete(
    "/blocklist/{name}/versions/{version}",
    status_code=status.HTTP_200_OK,
)
async def pypi_unblock_version(
    name: str,
    version: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.manage")),
    services: ApplicationServices = Depends(get_services),
) -> dict:
    """Remove a single version of a package from the blocklist."""
    _check_enabled(services)
    request_meta = audit_context_from_request(request)
    await services.pypi_mirror.unblock(
        name, version, actor=current_user, request_meta=request_meta
    )
    return {"ok": True}


@management_router.post(
    "/verify",
    response_model=PyPIJobStatus,
    status_code=status.HTTP_202_ACCEPTED,
)
async def pypi_verify_all(
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.manage")),
    services: ApplicationServices = Depends(get_services),
) -> PyPIJobStatus:
    """Start background integrity verification of all cached packages."""
    _check_enabled(services)
    job = services.pypi_mirror.verify_all()
    return job.to_status()


@management_router.post(
    "/packages/{name}/verify",
    response_model=PyPIJobStatus,
    status_code=status.HTTP_202_ACCEPTED,
)
async def pypi_verify_package(
    name: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.manage")),
    services: ApplicationServices = Depends(get_services),
) -> PyPIJobStatus:
    """Start background integrity verification for a single package."""
    _check_enabled(services)
    job = services.pypi_mirror.verify_package(name)
    return job.to_status()


@management_router.websocket("/jobs/ws")
async def pypi_jobs_ws(
    websocket: WebSocket,
    services: ApplicationServices = Depends(get_services),
) -> None:
    """Stream active job statuses for a requested list of job IDs via WebSocket."""
    import asyncio

    await websocket.accept()

    # Manual auth using cookie for WebSocket
    token = websocket.cookies.get(services.settings.access_cookie_name)
    if not token:
        await websocket.close(code=1008)
        return

    try:
        if token.startswith("asfes_"):
            user = await services.api_key_service.verify_token(token)
            if user is None:
                raise ValueError("Invalid API key")
            principal = user
        else:
            payload = await services.auth.verify_api_access_token(token)
            user = await services.users.get_user_by_id(payload["sub"])
            if not user:
                raise ValueError("User not found")
            principal = services.users.to_principal(user)

        if not principal.is_root and "pypi.read" not in principal.permissions:
            raise ValueError("No permission")
    except Exception:
        await websocket.close(code=1008)
        return

    job_ids: list[str] = []

    async def receive_loop() -> None:
        nonlocal job_ids
        try:
            while True:
                data = await websocket.receive_json()
                if isinstance(data, dict) and data.get("action") == "subscribe":
                    ids = data.get("job_ids")
                    if isinstance(ids, list):
                        job_ids = ids
        except WebSocketDisconnect:
            pass

    receiver_task = asyncio.create_task(receive_loop())

    try:
        while True:
            if job_ids:
                updates = []
                for jid in job_ids:
                    job = services.pypi_mirror.get_job(jid)
                    if job:
                        updates.append(job.to_status().model_dump())

                if updates:
                    await websocket.send_json({"type": "jobs_update", "jobs": updates})

            await asyncio.sleep(1.5)
            if receiver_task.done():
                break
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if not receiver_task.done():
            receiver_task.cancel()


@management_router.get("/jobs/{job_id}", response_model=PyPIJobStatus)
async def pypi_job_status(
    job_id: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.read")),
    services: ApplicationServices = Depends(get_services),
) -> PyPIJobStatus:
    """Poll the status of a background job (install, verify, bulk-download)."""
    _check_enabled(services)
    job = services.pypi_mirror.get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )
    return job.to_status()


@management_router.delete("/jobs/{job_id}", status_code=status.HTTP_200_OK)
async def pypi_cancel_job(
    job_id: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.manage")),
    services: ApplicationServices = Depends(get_services),
) -> dict:
    """Cancel a running background job. Returns remaining_packages for resume support."""
    _check_enabled(services)
    cancelled = services.pypi_mirror.cancel_job(job_id)
    if not cancelled:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found or not cancellable",
        )
    job = services.pypi_mirror.get_job(job_id)
    return {
        "ok": True,
        "remaining_packages": job.remaining_packages if job else [],
    }


@management_router.post(
    "/bulk-download",
    response_model=PyPIJobStatus,
    status_code=status.HTTP_202_ACCEPTED,
)
async def pypi_bulk_download(
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("pypi.manage")),
    services: ApplicationServices = Depends(get_services),
) -> PyPIJobStatus:
    """
    Re-download/refresh all packages already registered in the mirror storage.
    This does NOT mirror all of PyPI — only packages already present locally.
    """
    _check_enabled(services)
    job = services.pypi_mirror.bulk_download_refresh()
    return job.to_status()


# ---------------------------------------------------------------------------
# pip-compatible Simple Repository API (PEP 503)
# Mounted directly in app.py — NOT through api_router — to keep /pypi/ prefix.
# ---------------------------------------------------------------------------

simple_router = APIRouter(tags=["pypi-simple"])


@simple_router.get(
    "/pypi/simple/",
    response_class=HTMLResponse,
    include_in_schema=False,
)
async def simple_index(
    request: Request,
    services: ApplicationServices = Depends(get_services),
) -> HTMLResponse:
    """PEP 503 root index: lists all packages available on this mirror."""
    _check_enabled(services)
    await enforce_api_rate_limit(request, services, policy_name="pypi_simple")
    html = await services.pypi_mirror.simple_api_root_html()
    return HTMLResponse(content=html)


@simple_router.get(
    "/pypi/simple/{name}/",
    response_class=HTMLResponse,
    include_in_schema=False,
)
async def simple_package(
    name: str,
    request: Request,
    services: ApplicationServices = Depends(get_services),
) -> HTMLResponse:
    """
    PEP 503 package index with rewritten download links.
    Blocked packages return 404 (pip interprets this as 'not found').
    """
    _check_enabled(services)
    await enforce_api_rate_limit(request, services, policy_name="pypi_simple")
    try:
        html = await services.pypi_mirror.simple_api_package_html(name)
    except ValueError:
        # Package is blocked — return 404 so pip reports "no matching distribution"
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Package not found or blocked",
        )
    if html is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Package not found",
        )
    return HTMLResponse(content=html)


@simple_router.get("/pypi/files/{name}/{version}/{filename}", include_in_schema=False)
async def simple_file(
    name: str,
    version: str,
    filename: str,
    request: Request,
    services: ApplicationServices = Depends(get_services),
) -> FileResponse:
    """
    Serve a wheel/sdist file from local storage.
    Downloads on-demand from PyPI if the file is not cached and on_demand_proxy is enabled.
    Returns 403 if the version is blocked, 404 if not found.
    """
    _check_enabled(services)
    await enforce_api_rate_limit(request, services, policy_name="pypi_simple")
    try:
        file_path = await services.pypi_mirror.get_file_path(name, version, filename)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    if file_path is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="File not found"
        )
    return FileResponse(str(file_path), filename=filename)


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


def _check_enabled(services: ApplicationServices) -> None:
    if not services.settings.pypi.enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="PyPI mirror is disabled",
        )
