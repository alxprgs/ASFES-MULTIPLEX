from __future__ import annotations

from server.audit import audit_context_from_request
import base64
import html
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from server.core.deps import enforce_api_rate_limit, get_services, require_permission, get_current_api_user
from server.core.ratelimit import RateLimitError
from server.models import (
    OAuthClientCreateRequest,
    OAuthClientResponse,
    OAuthClientSecretRotateResponse,
    OAuthDynamicClientRegistrationRequest,
    UserPrincipal,
)
from server.services import ApplicationServices


oauth_router = APIRouter(prefix="/oauth", tags=["oauth"])
well_known_router = APIRouter(tags=["oauth"])


def _append_query(url: str, params: dict[str, str]) -> str:
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}{urlencode(params)}"


_CSS = """
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Inter', 'Segoe UI', system-ui, sans-serif;
    background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%);
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 1.5rem;
  }
  .card {
    width: 100%;
    max-width: 440px;
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.1);
    backdrop-filter: blur(24px);
    border-radius: 20px;
    padding: 2.5rem;
    box-shadow: 0 25px 60px rgba(0,0,0,0.5);
  }
  .logo {
    width: 48px;
    height: 48px;
    background: linear-gradient(135deg, #7c3aed, #4f46e5);
    border-radius: 12px;
    display: flex;
    align-items: center;
    justify-content: center;
    margin-bottom: 1.5rem;
    font-size: 1.4rem;
  }
  .subtitle {
    font-size: 0.85rem;
    color: rgba(255,255,255,0.45);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 0.5rem;
  }
  h1 {
    font-size: 1.5rem;
    font-weight: 700;
    color: #fff;
    margin-bottom: 0.4rem;
    line-height: 1.3;
  }
  .client-name {
    color: #a78bfa;
  }
  .desc {
    font-size: 0.9rem;
    color: rgba(255,255,255,0.55);
    margin-bottom: 2rem;
    line-height: 1.6;
  }
  .user-badge {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    background: rgba(124,58,237,0.15);
    border: 1px solid rgba(124,58,237,0.3);
    border-radius: 12px;
    padding: 0.85rem 1rem;
    margin-bottom: 1.5rem;
  }
  .user-avatar {
    width: 36px;
    height: 36px;
    border-radius: 50%;
    background: linear-gradient(135deg, #7c3aed, #4f46e5);
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.9rem;
    color: #fff;
    font-weight: 600;
    flex-shrink: 0;
  }
  .user-info small {
    display: block;
    font-size: 0.75rem;
    color: rgba(255,255,255,0.45);
    margin-bottom: 2px;
  }
  .user-info strong {
    font-size: 0.95rem;
    color: #fff;
  }
  label {
    display: block;
    font-size: 0.8rem;
    font-weight: 600;
    color: rgba(255,255,255,0.6);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 0.4rem;
  }
  input[type=text], input[type=password] {
    width: 100%;
    background: rgba(255,255,255,0.07);
    border: 1px solid rgba(255,255,255,0.12);
    border-radius: 10px;
    color: #fff;
    font-size: 0.95rem;
    padding: 0.75rem 1rem;
    outline: none;
    transition: border-color 0.2s;
    margin-bottom: 1.1rem;
  }
  input[type=text]:focus, input[type=password]:focus {
    border-color: #7c3aed;
    box-shadow: 0 0 0 3px rgba(124,58,237,0.2);
  }
  input::placeholder { color: rgba(255,255,255,0.25); }
  .btn-row {
    display: flex;
    gap: 0.75rem;
    margin-top: 0.5rem;
  }
  .btn-approve {
    flex: 1;
    background: linear-gradient(135deg, #7c3aed, #4f46e5);
    color: #fff;
    border: none;
    border-radius: 10px;
    padding: 0.85rem;
    font-size: 0.95rem;
    font-weight: 600;
    cursor: pointer;
    transition: opacity 0.2s, transform 0.1s;
  }
  .btn-approve:hover { opacity: 0.9; transform: translateY(-1px); }
  .btn-approve:active { transform: translateY(0); }
  .btn-deny {
    flex: 0 0 auto;
    background: rgba(255,255,255,0.06);
    color: rgba(255,255,255,0.6);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 10px;
    padding: 0.85rem 1.25rem;
    font-size: 0.95rem;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.2s;
  }
  .btn-deny:hover { background: rgba(255,255,255,0.1); color: #fff; }
  .error {
    background: rgba(239,68,68,0.15);
    border: 1px solid rgba(239,68,68,0.3);
    color: #fca5a5;
    border-radius: 10px;
    padding: 0.75rem 1rem;
    font-size: 0.88rem;
    margin-bottom: 1.25rem;
  }
  .divider {
    border: none;
    border-top: 1px solid rgba(255,255,255,0.08);
    margin: 1.25rem 0;
  }
  .scopes {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
    margin-bottom: 1.5rem;
  }
  .scope-tag {
    background: rgba(79,70,229,0.2);
    border: 1px solid rgba(79,70,229,0.35);
    color: #a5b4fc;
    border-radius: 6px;
    font-size: 0.78rem;
    padding: 0.2rem 0.6rem;
    font-family: monospace;
  }
"""


def _base_page(title: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{html.escape(title)} — Multiplex</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>{_CSS}</style>
</head>
<body>
<div class="card">
  <div class="logo">✦</div>
  {body}
</div>
</body>
</html>"""


def _render_confirm_page(
    client_name: str,
    username: str,
    values: dict[str, str],
    scopes: list[str],
    need_totp: bool = False,
    error: str | None = None,
) -> HTMLResponse:
    """Page shown when the user is already authenticated — just confirm the OAuth grant."""

    def hidden(name: str) -> str:
        return f'<input type="hidden" name="{html.escape(name)}" value="{html.escape(values.get(name, ""))}">'

    initials = username[0].upper() if username else "?"
    scope_tags = (
        " ".join(f'<span class="scope-tag">{html.escape(s)}</span>' for s in scopes)
        or ""
    )
    error_block = f'<div class="error">{html.escape(error)}</div>' if error else ""
    totp_field = (
        """
        <label>Код аутентификатора</label>
        <input name="totp_code" type="text" inputmode="numeric" autocomplete="one-time-code" placeholder="000000">
    """
        if need_totp
        else ""
    )

    body = f"""
      <div class="subtitle">Авторизация</div>
      <h1>Подключить <span class="client-name">{html.escape(client_name)}</span>?</h1>
      <p class="desc">Это приложение запрашивает доступ к вашему Multiplex-аккаунту.</p>
      <div class="user-badge">
        <div class="user-avatar">{html.escape(initials)}</div>
        <div class="user-info">
          <small>Вы вошли как</small>
          <strong>{html.escape(username)}</strong>
        </div>
      </div>
      {f'<div class="scopes">{scope_tags}</div>' if scope_tags else ""}
      {error_block}
      <form method="post">
        {hidden("response_type")}
        {hidden("client_id")}
        {hidden("redirect_uri")}
        {hidden("scope")}
        {hidden("state")}
        {hidden("code_challenge")}
        {hidden("code_challenge_method")}
        <input type="hidden" name="username" value="{html.escape(username)}">
        <input type="hidden" name="session_auth" value="1">
        {totp_field}
        <div class="btn-row">
          <button class="btn-approve" type="submit" name="approve" value="true">Разрешить доступ</button>
          <button class="btn-deny" type="submit" name="approve" value="false">Отклонить</button>
        </div>
      </form>
    """
    return HTMLResponse(_base_page(f"Подключить {client_name}", body))


def _render_login_page(
    client_name: str,
    values: dict[str, str],
    error: str | None = None,
    need_totp: bool = False,
) -> HTMLResponse:
    """Page shown when the user is not authenticated — requires login."""

    def hidden(name: str) -> str:
        return f'<input type="hidden" name="{html.escape(name)}" value="{html.escape(values.get(name, ""))}">'

    error_block = f'<div class="error">{html.escape(error)}</div>' if error else ""
    totp_field = (
        """
        <label>Код аутентификатора</label>
        <input name="totp_code" type="text" inputmode="numeric" autocomplete="one-time-code" placeholder="000000">
    """
        if need_totp
        else ""
    )

    body = f"""
      <div class="subtitle">Авторизация</div>
      <h1>Войти для <span class="client-name">{html.escape(client_name)}</span></h1>
      <p class="desc">Войдите в аккаунт Multiplex, чтобы предоставить доступ.</p>
      {error_block}
      <form method="post">
        {hidden("response_type")}
        {hidden("client_id")}
        {hidden("redirect_uri")}
        {hidden("scope")}
        {hidden("state")}
        {hidden("code_challenge")}
        {hidden("code_challenge_method")}
        <label>Имя пользователя</label>
        <input name="username" type="text" required autocomplete="username" placeholder="root">
        <label>Пароль</label>
        <input name="password" type="password" required autocomplete="current-password" placeholder="••••••••••">
        {totp_field}
        <div class="btn-row">
          <button class="btn-approve" type="submit" name="approve" value="true">Войти и разрешить</button>
          <button class="btn-deny" type="submit" name="approve" value="false">Отмена</button>
        </div>
      </form>
    """
    return HTMLResponse(_base_page(f"Войти для {client_name}", body))


def _validate_scope(
    requested_scope: str | None, client: dict, services: ApplicationServices
) -> list[str]:
    scopes = [scope for scope in (requested_scope or "mcp").split(" ") if scope]
    allowed = set(client.get("allowed_scopes", []))
    supported = set(services.settings.oauth.supported_scopes)
    if not set(scopes).issubset(allowed) or not set(scopes).issubset(supported):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Requested scopes are not allowed",
        )
    return scopes


def _basic_client_credentials(request: Request) -> tuple[str | None, str | None]:
    authorization = request.headers.get("authorization", "")
    if not authorization.lower().startswith("basic "):
        return None, None
    try:
        decoded = base64.b64decode(authorization.split(" ", 1)[1]).decode("utf-8")
    except Exception:
        return None, None
    client_id, _, secret = decoded.partition(":")
    return client_id or None, secret or None


def _client_secret_from_request(request: Request, form) -> str | None:
    if form.get("client_secret"):
        return str(form.get("client_secret"))
    return _basic_client_credentials(request)[1]


def _validate_pkce_method(method: str, services: ApplicationServices) -> str:
    normalized = method.upper()
    if normalized == "PLAIN" and not services.settings.oauth.allow_plain_pkce:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="plain PKCE is not allowed"
        )
    if normalized not in {"S256", "PLAIN"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported PKCE code_challenge_method",
        )
    return normalized


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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only response_type=code is supported",
        )
    if services.settings.oauth.require_pkce and not code_challenge:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="PKCE code_challenge is required",
        )
    code_challenge_method = _validate_pkce_method(code_challenge_method, services)
    try:
        client = await services.oauth.validate_client(client_id, redirect_uri)
    except LookupError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc
    scopes = _validate_scope(scope, client, services)

    values = {
        "response_type": response_type,
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scope or "mcp",
        "state": state or "",
        "code_challenge": code_challenge or "",
        "code_challenge_method": code_challenge_method,
    }

    # Check if user already has a valid session cookie — if so, show confirm page.
    session_token = request.cookies.get(services.settings.access_cookie_name)
    if session_token:
        try:
            payload = await services.auth.verify_api_access_token(session_token)
            user_doc = await services.users.get_user_by_id(payload["sub"])
            if user_doc:
                username = (
                    user_doc.get("username")
                    or user_doc.get("display_name")
                    or payload["sub"]
                )
                return _render_confirm_page(client["name"], username, values, scopes)
        except Exception:
            pass  # Invalid / expired session cookie → fall through to login form

    return _render_login_page(client["name"], values)


@oauth_router.post("/authorize", response_class=HTMLResponse, response_model=None)
async def oauth_authorize_post(
    request: Request, services: ApplicationServices = Depends(get_services)
) -> Response:
    form = await request.form()
    response_type = str(form.get("response_type", ""))
    basic_client_id, _ = _basic_client_credentials(request)
    client_id = str(form.get("client_id", "") or basic_client_id or "")
    redirect_uri = str(form.get("redirect_uri", ""))
    scope = str(form.get("scope", "mcp"))
    state = str(form.get("state", ""))
    code_challenge = str(form.get("code_challenge", ""))
    code_challenge_method = str(form.get("code_challenge_method", "S256"))
    username = str(form.get("username", ""))
    password = str(form.get("password", ""))
    totp_code = str(form.get("totp_code", ""))
    approve = str(form.get("approve", "false")).lower() == "true"
    session_auth = str(form.get("session_auth", "")) == "1"  # came from confirm page

    try:
        client = await services.oauth.validate_client(client_id, redirect_uri)
        scopes = _validate_scope(scope, client, services)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc

    if response_type != "code":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only response_type=code is supported",
        )
    if services.settings.oauth.require_pkce and not code_challenge:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="PKCE code_challenge is required",
        )
    code_challenge_method = _validate_pkce_method(code_challenge_method, services)
    if not approve:
        return RedirectResponse(
            _append_query(redirect_uri, {"error": "access_denied", "state": state}),
            status_code=status.HTTP_302_FOUND,
        )

    values = {
        "response_type": response_type,
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scope,
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": code_challenge_method,
    }

    # --- Session-based flow (confirm page) ---
    if session_auth and username:
        session_token = request.cookies.get(services.settings.access_cookie_name)
        user_doc = None
        if session_token:
            try:
                payload = await services.auth.verify_api_access_token(session_token)
                user_doc = await services.users.get_user_by_id(payload["sub"])
                if user_doc and (user_doc.get("username") or "") != username:
                    user_doc = None  # Mismatch — someone tampered with the hidden field
            except Exception:
                user_doc = None
        if user_doc is None:
            return _render_login_page(
                client["name"],
                values,
                error="Сессия истекла. Пожалуйста, войдите снова.",
            )
        user = services.users.to_principal(user_doc)
        # TOTP check for session-auth path too
        if services.users.two_factor_enabled(
            user_doc
        ) and not await services.users.verify_second_factor(user_doc, totp_code):
            return _render_confirm_page(
                client["name"],
                username,
                values,
                scopes,
                need_totp=True,
                error="Неверный код аутентификатора." if totp_code else None,
            )
    else:
        # --- Password-based login flow ---
        user = await services.users.authenticate(username, password)
        if user is None:
            return _render_login_page(
                client["name"], values, error="Неверное имя пользователя или пароль."
            )
        user_doc = await services.users.get_user_by_id(user.user_id)
        if not user_doc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="User does not exist"
            )
        if services.users.two_factor_enabled(
            user_doc
        ) and not await services.users.verify_second_factor(user_doc, totp_code):
            return _render_login_page(
                client["name"],
                values,
                error="Неверный код аутентификатора.",
                need_totp=True,
            )

    code = await services.oauth.create_authorization_code(
        client_id=client_id,
        redirect_uri=redirect_uri,
        user=user,
        scopes=scopes,
        code_challenge=code_challenge,
        code_challenge_method=code_challenge_method,
        audit_ctx=audit_context_from_request(request),
    )
    return RedirectResponse(
        _append_query(redirect_uri, {"code": code, "state": state}),
        status_code=status.HTTP_302_FOUND,
    )


@oauth_router.post("/token")
async def oauth_token(
    request: Request, services: ApplicationServices = Depends(get_services)
) -> JSONResponse:
    form = await request.form()
    grant_type = str(form.get("grant_type", ""))
    basic_client_id, _ = _basic_client_credentials(request)
    client_id = str(form.get("client_id", "") or basic_client_id or "")
    client_secret = _client_secret_from_request(request, form)
    request_meta = audit_context_from_request(request)
    try:
        await services.rate_limiter.enforce(
            "oauth_token", f"{client_id}:{request_meta.actor.ip or 'anonymous'}"
        )
    except RateLimitError as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many token requests",
            headers={"Retry-After": str(exc.retry_after)},
        ) from exc

    if grant_type == "authorization_code":
        code = str(form.get("code", ""))
        redirect_uri = str(form.get("redirect_uri", ""))
        code_verifier = str(form.get("code_verifier", ""))
        try:
            payload = await services.oauth.exchange_code(
                code=code,
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
                code_verifier=code_verifier,
                audit_ctx=request_meta,
            )
        except LookupError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
            ) from exc
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
            ) from exc
        return JSONResponse(payload)

    if grant_type == "refresh_token":
        refresh_token = str(form.get("refresh_token", ""))
        try:
            payload = await services.oauth.refresh_token(
                refresh_token=refresh_token,
                client_id=client_id,
                client_secret=client_secret,
                audit_ctx=request_meta,
            )
        except LookupError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
            ) from exc
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
            ) from exc
        return JSONResponse(payload)

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported grant_type"
    )


@oauth_router.post("/revoke", status_code=status.HTTP_200_OK)
async def oauth_revoke(
    request: Request,
    token: str = Form(...),
    client_id: str | None = Form(default=None),
    services: ApplicationServices = Depends(get_services),
) -> dict[str, bool]:
    form = await request.form()
    basic_client_id, _ = _basic_client_credentials(request)
    await services.oauth.revoke_token(
        token, client_id or basic_client_id, _client_secret_from_request(request, form)
    )
    return {"revoked": True}


@oauth_router.get("/clients", response_model=list[OAuthClientResponse])
async def oauth_clients(
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("oauth.clients.manage")),
) -> list[OAuthClientResponse]:
    await enforce_api_rate_limit(request, services, user=current_user)
    return [
        OAuthClientResponse.model_validate(item)
        for item in await services.oauth.list_clients()
    ]


@oauth_router.post(
    "/clients", response_model=OAuthClientResponse, status_code=status.HTTP_201_CREATED
)
async def create_oauth_client(
    payload: OAuthClientCreateRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("oauth.clients.manage")),
) -> OAuthClientResponse:
    await enforce_api_rate_limit(
        request, services, user=current_user, policy_name="rest_write"
    )
    try:
        client = await services.oauth.create_client(
            payload.name,
            payload.redirect_uris,
            payload.allowed_scopes,
            payload.client_id,
            payload.confidential,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc
    await services.audit.record(
        "oauth.client.create",
        actor=current_user,
        audit_ctx=audit_context_from_request(request),
        target={"client_id": client["client_id"]},
        metadata={
            "redirect_uris": payload.redirect_uris,
            "allowed_scopes": payload.allowed_scopes,
        },
    )
    return OAuthClientResponse.model_validate(client)


@oauth_router.post(
    "/clients/{client_id}/secret/rotate", response_model=OAuthClientSecretRotateResponse
)
async def rotate_oauth_client_secret(
    client_id: str,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(require_permission("oauth.clients.manage")),
) -> OAuthClientSecretRotateResponse:
    await enforce_api_rate_limit(
        request, services, user=current_user, policy_name="rest_write"
    )
    try:
        rotated = await services.oauth.rotate_client_secret(client_id)
    except LookupError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc
    await services.audit.record(
        "oauth.client.secret.rotate",
        actor=current_user,
        audit_ctx=audit_context_from_request(request),
        target={"client_id": client_id},
    )
    return OAuthClientSecretRotateResponse.model_validate(rotated)


@oauth_router.post("/register", status_code=status.HTTP_201_CREATED)
async def register_oauth_client(
    payload: OAuthDynamicClientRegistrationRequest,
    request: Request,
    services: ApplicationServices = Depends(get_services),
    current_user: UserPrincipal = Depends(get_current_api_user),
) -> JSONResponse:
    await enforce_api_rate_limit(request, services, policy_name="rest_write")
    if payload.token_endpoint_auth_method != "none":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only public OAuth clients are supported",
        )
    if "authorization_code" not in payload.grant_types:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="authorization_code grant is required",
        )
    if "code" not in payload.response_types:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="code response type is required",
        )

    requested_scopes = [scope for scope in (payload.scope or "mcp").split(" ") if scope]
    try:
        client = await services.oauth.create_client(
            payload.client_name,
            payload.redirect_uris,
            requested_scopes,
            client_id=None,
            confidential=False,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc
    await services.audit.record(
        "oauth.client.dynamic_register",
        actor=None,
        audit_ctx=audit_context_from_request(request),
        target={"client_id": client["client_id"]},
        metadata={
            "redirect_uris": payload.redirect_uris,
            "allowed_scopes": client["allowed_scopes"],
        },
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
async def well_known_authorization_server(
    request: Request, services: ApplicationServices = Depends(get_services)
) -> dict[str, object]:
    return services.oauth.authorization_server_metadata(base_url=str(request.base_url))


@well_known_router.get("/.well-known/oauth-authorization-server{issuer_path:path}")
async def well_known_authorization_server_path(
    request: Request,
    issuer_path: str,
    services: ApplicationServices = Depends(get_services),
) -> dict[str, object]:
    metadata_paths = {
        services.settings.oauth.issuer_path.rstrip("/"),
        services.settings.mcp_path.rstrip("/"),
    }
    if issuer_path.rstrip("/") not in metadata_paths:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Authorization server metadata not found",
        )
    return services.oauth.authorization_server_metadata(base_url=str(request.base_url))


@well_known_router.get("{resource_path:path}/.well-known/oauth-authorization-server")
async def well_known_authorization_server_resource(
    request: Request,
    resource_path: str,
    services: ApplicationServices = Depends(get_services),
) -> dict[str, object]:
    if resource_path.rstrip("/") != services.settings.mcp_path.rstrip("/"):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Authorization server metadata not found",
        )
    return services.oauth.authorization_server_metadata(base_url=str(request.base_url))


@well_known_router.get("/.well-known/oauth-protected-resource")
async def well_known_protected_resource_root(
    request: Request, services: ApplicationServices = Depends(get_services)
) -> dict[str, object]:
    return services.oauth.protected_resource_metadata(base_url=str(request.base_url))


@well_known_router.get("/.well-known/oauth-protected-resource{resource_path:path}")
async def well_known_protected_resource(
    request: Request,
    resource_path: str,
    services: ApplicationServices = Depends(get_services),
) -> dict[str, object]:
    if resource_path.rstrip("/") != services.settings.mcp_path:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Protected resource metadata not found",
        )
    return services.oauth.protected_resource_metadata(base_url=str(request.base_url))
