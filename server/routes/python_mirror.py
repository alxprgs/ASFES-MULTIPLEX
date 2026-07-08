"""
Python Mirror routes — management API.
Mounted inside api_router at /api/python-mirror/…
"""

from __future__ import annotations

import asyncio

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Request,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.responses import FileResponse

from server.audit import audit_context_from_request
from server.core.deps import get_services, require_permission
from server.models import (
    PythonMirrorJobStatus,
    PythonMirrorListResponse,
    PythonMirrorStatsResponse,
    PythonMirrorSuggestRequest,
    PythonMirrorSuggestResponse,
    PythonMirrorVersion,
    UserPrincipal,
)
from server.services import ApplicationServices


router = APIRouter(prefix="/python-mirror", tags=["python-mirror"])


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


@router.get("/stats", response_model=PythonMirrorStatsResponse)
async def pm_stats(
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("python_mirror.read")),
    services: ApplicationServices = Depends(get_services),
) -> PythonMirrorStatsResponse:
    """Storage statistics for the Python distribution mirror."""
    _check_enabled(services)
    return await services.python_mirror.get_stats()


# ---------------------------------------------------------------------------
# Remote versions — MUST be before /versions/{version} to avoid routing clash
# ---------------------------------------------------------------------------


@router.get("/versions/remote")
async def pm_remote_versions(
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("python_mirror.read")),
    services: ApplicationServices = Depends(get_services),
) -> dict:
    """List all Python versions available for download from python.org."""
    _check_enabled(services)
    versions = await services.python_mirror.get_remote_versions()
    return {"versions": versions}


# ---------------------------------------------------------------------------
# Installed versions
# ---------------------------------------------------------------------------


@router.get("/versions", response_model=PythonMirrorListResponse)
async def pm_list_versions(
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("python_mirror.read")),
    services: ApplicationServices = Depends(get_services),
) -> PythonMirrorListResponse:
    """List locally stored Python versions."""
    _check_enabled(services)
    return await services.python_mirror.list_versions()


@router.get("/versions/{version}", response_model=PythonMirrorVersion)
async def pm_version_detail(
    version: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("python_mirror.read")),
    services: ApplicationServices = Depends(get_services),
) -> PythonMirrorVersion:
    """Detailed file listing for a single installed Python version."""
    _check_enabled(services)
    info = await services.python_mirror.get_version_info(version)
    if info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Version {version} is not installed",
        )
    return info


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


@router.post(
    "/versions/{version}/install",
    response_model=PythonMirrorJobStatus,
    status_code=status.HTTP_202_ACCEPTED,
)
async def pm_install_version(
    version: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("python_mirror.manage")),
    services: ApplicationServices = Depends(get_services),
) -> PythonMirrorJobStatus:
    """Download all distribution files for a Python version from python.org."""
    _check_enabled(services)
    return services.python_mirror.install_version(version, actor=current_user)


@router.delete("/versions/{version}", status_code=status.HTTP_200_OK)
async def pm_delete_version(
    version: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("python_mirror.manage")),
    services: ApplicationServices = Depends(get_services),
) -> dict:
    """Delete all locally stored files for a Python version."""
    _check_enabled(services)
    audit_ctx = audit_context_from_request(request)
    ok = await services.python_mirror.delete_version(
        version, actor=current_user, request_meta=audit_ctx
    )
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Version {version} not found",
        )
    return {"ok": True, "version": version}


@router.post(
    "/versions/{version}/verify",
    response_model=PythonMirrorJobStatus,
    status_code=status.HTTP_202_ACCEPTED,
)
async def pm_verify_version(
    version: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("python_mirror.manage")),
    services: ApplicationServices = Depends(get_services),
) -> PythonMirrorJobStatus:
    """Verify integrity of all files for a specific Python version."""
    _check_enabled(services)
    # Check that version exists first
    info = await services.python_mirror.get_version_info(version)
    if info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Version {version} is not installed",
        )
    return services.python_mirror.verify_version(version)


@router.post(
    "/verify",
    response_model=PythonMirrorJobStatus,
    status_code=status.HTTP_202_ACCEPTED,
)
async def pm_verify_all(
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("python_mirror.manage")),
    services: ApplicationServices = Depends(get_services),
) -> PythonMirrorJobStatus:
    """Verify integrity of all locally stored Python distributions."""
    _check_enabled(services)
    return services.python_mirror.verify_all()


@router.post(
    "/versions/{version}/repair",
    response_model=PythonMirrorJobStatus,
    status_code=status.HTTP_202_ACCEPTED,
)
async def pm_repair_version(
    version: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("python_mirror.manage")),
    services: ApplicationServices = Depends(get_services),
) -> PythonMirrorJobStatus:
    """Re-download any corrupted or missing files for a Python version."""
    _check_enabled(services)
    return services.python_mirror.repair_version(version)


# ---------------------------------------------------------------------------
# Suggest
# ---------------------------------------------------------------------------


@router.post("/suggest", response_model=PythonMirrorSuggestResponse)
async def pm_suggest(
    body: PythonMirrorSuggestRequest,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("python_mirror.read")),
    services: ApplicationServices = Depends(get_services),
) -> PythonMirrorSuggestResponse:
    """
    Find the best matching Python distribution files based on filters.
    Supports: version_query (\"3.12\", \"latest\", \"3.12.7\"), os_type, arch, file_type.
    """
    _check_enabled(services)
    return await services.python_mirror.suggest(body)


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


@router.websocket("/jobs/ws")
async def pm_jobs_ws(
    websocket: WebSocket,
    services: ApplicationServices = Depends(get_services),
) -> None:
    """
    WebSocket stream for job progress updates.
    Client sends: {\"action\": \"subscribe\", \"job_ids\": [\"id1\", \"id2\"]}
    Server sends: {\"type\": \"jobs_update\", \"jobs\": [...]}
    """
    await websocket.accept()
    if not services.settings.python_mirror.enabled:
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
                    job = services.python_mirror.get_job(jid)
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


@router.get("/jobs/{job_id}", response_model=PythonMirrorJobStatus)
async def pm_job_status(
    job_id: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("python_mirror.read")),
    services: ApplicationServices = Depends(get_services),
) -> PythonMirrorJobStatus:
    """Poll status of a background job by ID."""
    _check_enabled(services)
    job = services.python_mirror.get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )
    return job.to_status()


@router.delete("/jobs/{job_id}", status_code=status.HTTP_200_OK)
async def pm_cancel_job(
    job_id: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("python_mirror.manage")),
    services: ApplicationServices = Depends(get_services),
) -> dict:
    """Cancel a running background job."""
    _check_enabled(services)
    cancelled = services.python_mirror.cancel_job(job_id)
    if not cancelled:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found or not cancellable",
        )
    return {"ok": True}


# ---------------------------------------------------------------------------
# File serving
# ---------------------------------------------------------------------------


@router.get("/files/{version}/{filename}", include_in_schema=False)
async def pm_download_file(
    version: str,
    filename: str,
    request: Request,
    current_user: UserPrincipal = Depends(require_permission("python_mirror.read")),
    services: ApplicationServices = Depends(get_services),
) -> FileResponse:
    """
    Serve a Python distribution file from local storage.
    Returns 404 if the file is not in the local cache.
    """
    _check_enabled(services)
    try:
        file_path = services.python_mirror.get_file_path(version, filename)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc
    if file_path is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File {filename} for Python {version} not found locally. Download it first.",
        )
    return FileResponse(str(file_path), filename=filename)


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


def _check_enabled(services: ApplicationServices) -> None:
    if not services.settings.python_mirror.enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Python mirror is disabled",
        )
