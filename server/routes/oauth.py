from __future__ import annotations

import html
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from server.core.deps import enforce_api_rate_limit, get_services, require_permission
from server.core.ratelimit import RateLimitError
from server.models import OAuthClientCreateRequest, OAuthClientResponse, OAuthDynamicClientRegistrationRequest, UserPrincipal
from server.services import ApplicationServices, request_meta_from_request


oauth_router = APIRouter(prefix="/oauth", tags=["oauth"])
well_known_router = APIRouter(tags=["oauth"])


def _append_query(url: str, params: dict[str, str]) -> str:
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}{urlencode(params)}"


def _render_authorize_form(client_name: str, values: dict[str, str], error: str | None = None) -> HTMLResponse:
    def hidden(name: str) -> str:
        return f'<input type="hidden" name="{html.escape(name)}" value="{html.escape(values.get(name, ""))}">'

    error_block = f'<p style="color:#b91c1c;">{html.escape(error)}</p>' if error else ""
    page = f"""
    <html lang="en">
      <head>
        <meta charset="utf-8">
        <title>Multiplex OAuth Authorization</title>
      </head>
      <body style="font-family:Segoe UI,Arial,sans-serif;background:#f6f7fb;padding:2rem;">
        <div style="max-width:480px;margin:0 auto;background:white;padding:2rem;border-radius:16px;box-shadow:0 8px 30px rgba(0,0,0,0.08);">
          <h1 style="margin-top:0;">Authorize {html.escape(client_name)}</h1>
          <p>Sign in with an existing Multiplex account to continue.</p>
          {error_block}
          <form method="post">
            {hidden("response_type")}
            {hidden("client_id")}
            {hidden("redirect_uri")}
            {hidden("scope")}
            {hidden("state")}
            {hidden("code_challenge")}
            {hidden("code_challenge_method")}
            <label>Username</label><br />
            <input name="username" type="text" required style="width:100%;padding:.75rem;margin:.4rem 0 1rem;" />
            <label>Password</label><br />
            <input name="password" type="password" required style="width:100%;padding:.75rem;margin:.4rem 0 1rem;" />
            <button type="submit" name="approve" value="true" style="padding:.8rem 1.2rem;">Approve</button>
            <button type="submit" name="approve" value="false" style="padding:.8rem 1.2rem;margin-left:.5rem;">Deny</button>
          </form>
        </div>
      </body>
    </html>
    """
    return HTMLResponse(page)


def _validate_scope(requested_scope: str | None, client: dict, services: ApplicationServices) -> list[str]:
    scopes = [scope for scope in (requested_scope or "mcp").split(" ") if scope]
    allowed = set(client.get("allowed_scopes", []))
    supported = set(services.settings.oauth.supported_scopes)
    if not set(scopes).issubset(allowed) or not set(scopes).issubset(supported):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Requested scopes are not allowed")
    return scopes


@oauth_router.get("/authorize", response_class=HTMLResponse)
async def oauth_authorize_get(
    request: Request,
    response_type: str,
    client_id: str,
    redirect_uri: str,
    scope: str | None = None,
    state: str | None = None,
    code_challenge: str | None = None,
    code_challenge_method: str = "S256",
    services: ApplicationServices = Depends(get_services),
) -> HTMLResponse:
    if response_type != "code":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only response_type=code is supported")
    if services.settings.oauth.require_pkce and not code_challenge:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="PKCE code_challenge is required")
    try:
        client = await services.oauth.validate_client(client_id, redirect_uri)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    _validate_scope(scope, client, services)
    return _render_authorize_form(
        client["name"],
        {
            "response_type": response_type,
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": scope or "mcp",
            "state": state or "",
            "code_challenge": code_challenge or "",
            "code_challenge_method": code_challenge_method,
        },
    )


@oauth_router.post("/authorize", response_class=HTMLResponse, response_model=None)
async def oauth_authorize_post(request: Request, services: ApplicationServices = Depends(get_services)) -> Response:
    form = await request.form()
    response_type = str(form.get("response_type", ""))
    client_id = str(form.get("client_id", ""))
    redirect_uri = str(form.get("redirect_uri", ""))
    scope = str(form.get("scope", "mcp"))
    state = str(form.get("state", ""))
    code_challenge = str(form.get("code_challenge", ""))
    code_challenge_method = str(form.get("code_challenge_method", "S256"))
    username = str(form.get("username", ""))
    password = str(form.get("password", ""))
    approve = str(form.get("approve", "false")).lower() == "true"

    try:
        client = await services.oauth.validate_client(client_id, redirect_uri)
        scopes = _validate_scope(scope, client, services)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    if response_type != "code":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only response_type=code is supported")
    if services.settings.oauth.require_pkce and not code_challenge:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="PKCE code_challenge is required")
    if not approve:
        return RedirectResponse(_append_query(redirect_uri, {"error": "access_denied", "state": state}), status_code=status.HTTP_302_FOUND)

    user = await services.users.authenticate(username, password)
    if user is None:
        return _render_authorize_form(
            client["name"],
            {
                "response_type": response_type,
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "scope": scope,
                "state": state,
                "code_challenge": code_challenge,
                "code_challenge_method": code_challenge_method,
            },
            error="Invalid credentials",
        )

    code = await services.oauth.create_authorization_code(
        client_id=client_id,
        redirect_uri=redirect_uri,
        user=user,
        scopes=scopes,
        code_challenge=code_challenge,
        code_challenge_method=code_challenge_method,
        request_meta=request_meta_from_request(request),
    )
    return RedirectResponse(_append_query(redirect_uri, {"code": code, "state": state}), status_code=status.HTTP_302_FOUND)


@oauth_router.post("/token")
async def oauth_token(request: Request, services: ApplicationServices = Depends(get_services)) -> JSONResponse:
    form = await request.form()
    grant_type = str(form.get("grant_type", ""))
    client_id = str(form.get("client_id", ""))
    request_meta = request_meta_from_request(request)
    try:
        await services.rate_limiter.enforce("oauth_token", f"{client_id}:{request_meta['ip']}")
    except RateLimitError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Too many token requests", headers={"Retry-After": str(exc.retry_after)}) from exc

    if grant_type == "authorization_code":
        code = str(form.get("code", ""))
        redirect_uri = str(form.get("redirect_uri", ""))
        code_verifier = str(form.get("code_verifier", ""))
        try:
            payload = await services.oauth.exchange_code(
                code=code,
                client_id=client_id,
                redirect_uri=redirect_uri,
                code_verifier=code_verifier,
                request_meta=request_meta,
            )
        except LookupError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse(payload)

    if grant_type == "refresh_token":
        refresh_token = str(form.get("refresh_token", ""))
        try:
            payload = await services.oauth.refresh_token(refresh_token=refresh_token, client_id=client_id, request_meta=request_meta)
        except LookupError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return JSONResponse(payload)

    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported grant_type")


@oauth_router.post("/revoke", status_code=status.HTTP_200_OK)
async def oauth_revoke(token: str = Form(...), services: ApplicationServices = Depends(get_services)) -> dict[str, bool]:
    await services.oauth.revoke_token(token)
    return {"revoked": True}


@oauth_router.get("/clients", response_model=list[OAuthClientResponse])
async def oauth_clients(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("oauth.clients.manage")),
) -> list[OAuthClientResponse]:
    await enforce_api_rate_limit(request, services, user=current_user)
    return [OAuthClientResponse.model_validate(item) for item in await services.oauth.list_clients()]


@oauth_router.post("/clients", response_model=OAuthClientResponse, status_code=status.HTTP_201_CREATED)
async def create_oauth_client(
    payload: OAuthClientCreateRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("oauth.clients.manage")),
) -> OAuthClientResponse:
    await enforce_api_rate_limit(request, services, user=current_user, policy_name="rest_write")
    client = await services.oauth.create_client(payload.name, payload.redirect_uris, payload.allowed_scopes, payload.client_id, payload.confidential)
    await services.audit.record(
        "oauth.client.create",
        actor=current_user,
        request_meta=request_meta_from_request(request),
        target={"client_id": client["client_id"]},
        metadata={"redirect_uris": payload.redirect_uris, "allowed_scopes": payload.allowed_scopes},
    )
    return OAuthClientResponse.model_validate(client)


@oauth_router.post("/register", status_code=status.HTTP_201_CREATED)
async def register_oauth_client(payload: OAuthDynamicClientRegistrationRequest, request: Request, services: ApplicationServices = Depends(get_services)) -> JSONResponse:
    if payload.token_endpoint_auth_method != "none":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only public OAuth clients are supported")
    if "authorization_code" not in payload.grant_types:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="authorization_code grant is required")
    if "code" not in payload.response_types:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="code response type is required")

    requested_scopes = [scope for scope in (payload.scope or "mcp").split(" ") if scope]
    client = await services.oauth.create_client(
        payload.client_name,
        payload.redirect_uris,
        requested_scopes,
        client_id=None,
        confidential=False,
    )
    root_actor = UserPrincipal(
        user_id="system",
        username="system",
        is_root=True,
        permissions=[],
        email=None,
        tg_id=None,
        vk_id=None,
    )
    await services.audit.record(
        "oauth.client.dynamic_register",
        actor=root_actor,
        request_meta=request_meta_from_request(request),
        target={"client_id": client["client_id"]},
        metadata={"redirect_uris": payload.redirect_uris, "allowed_scopes": client["allowed_scopes"]},
    )
    return JSONResponse(
        {
            "client_id": client["client_id"],
            "client_name": client["name"],
            "redirect_uris": client["redirect_uris"],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
            "scope": " ".join(client["allowed_scopes"]),
        },
        status_code=status.HTTP_201_CREATED,
    )


@oauth_router.get("/jwks")
async def oauth_jwks() -> dict[str, list]:
    return {"keys": []}


@well_known_router.get("/.well-known/oauth-authorization-server")
async def well_known_authorization_server(services: ApplicationServices = Depends(get_services)) -> dict[str, object]:
    return services.oauth.authorization_server_metadata()


@well_known_router.get("/.well-known/oauth-authorization-server{issuer_path:path}")
async def well_known_authorization_server_for_issuer(
    issuer_path: str,
    services: ApplicationServices = Depends(get_services),
) -> dict[str, object]:
    if issuer_path.rstrip("/") != services.settings.oauth.issuer_path.rstrip("/"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Authorization server metadata not found")
    return services.oauth.authorization_server_metadata()


@well_known_router.get("/.well-known/oauth-protected-resource")
async def well_known_protected_resource_root(services: ApplicationServices = Depends(get_services)) -> dict[str, object]:
    return services.oauth.protected_resource_metadata()


@well_known_router.get("/.well-known/oauth-protected-resource{resource_path:path}")
async def well_known_protected_resource(resource_path: str, services: ApplicationServices = Depends(get_services)) -> dict[str, object]:
    if resource_path.rstrip("/") != services.settings.mcp_path:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Protected resource metadata not found")
    return services.oauth.protected_resource_metadata()
