from __future__ import annotations

import asyncio
import contextlib
import importlib
import pkgutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from server.alerting import AlertingService
from server.core.config import Settings
from server.core.database import AUDIT_EVENTS, OAUTH_CLIENTS, OAUTH_CODES, PLUGINS, REFRESH_TOKENS, SETTINGS, TOOL_POLICIES, USERS, DatabaseManager
from server.core.logging import IntegrityLogManager, Mailer, get_logger
from server.core.ratelimit import RateLimitPolicy, RateLimiter
from server.core.security import TokenBundle, create_jwt, decode_jwt, hash_password, now_utc, random_token, sha256_text, verify_password, verify_pkce
from server.host_ops import HostOpsService
from server.models import MCPTool, PermissionDefinition, PluginDefinition, RuntimeAvailability, ToolExecutionContext, UserPrincipal


LOGGER = get_logger("multiplex.services")
CORE_PERMISSIONS = {
    "mcp.enable": "Enable or disable MCP globally for all users.",
    "mcp.plugin.manage": "Reload and manage local MCP plugins.",
    "mcp.tool.toggle": "Enable or disable MCP tools globally or per user.",
    "users.permission.grant": "Grant and revoke explicit permissions for users.",
    "settings.registration.update": "Toggle self-registration availability.",
    "settings.redis.update": "Toggle Redis runtime usage.",
    "audit.read": "Read audit and sensitive operation records.",
    "oauth.clients.manage": "Create and inspect OAuth clients.",
}


def validate_runtime_security(settings: Settings) -> None:
    if settings.is_production and settings._uses_default_secret_values():
        raise RuntimeError("Production mode requires custom SECURITY secrets and ROOT password")


def serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def normalize_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def is_expired(value: datetime | None) -> bool:
    normalized = normalize_utc_datetime(value)
    if normalized is None:
        return True
    return normalized <= now_utc()


def request_meta_from_request(request: Any) -> dict[str, Any]:
    forwarded = request.headers.get("x-forwarded-for")
    ip = forwarded.split(",")[0].strip() if forwarded else (request.client.host if request.client else None)
    return {
        "ip": ip,
        "user_agent": request.headers.get("user-agent"),
        "method": request.method,
        "path": request.url.path,
    }


class PermissionCatalog:
    def __init__(self) -> None:
        self._permissions: dict[str, PermissionDefinition] = {}

    def register(self, key: str, description: str) -> None:
        self._permissions[key] = PermissionDefinition(key=key, description=description)

    def register_many(self, permissions: dict[str, str]) -> None:
        for key, description in permissions.items():
            self.register(key, description)

    def register_plugin_permissions(self, definitions: list[PermissionDefinition]) -> None:
        for item in definitions:
            self._permissions[item.key] = item

    def list(self) -> list[PermissionDefinition]:
        return sorted(self._permissions.values(), key=lambda item: item.key)

    def keys(self) -> list[str]:
        return sorted(self._permissions.keys())

    def exists(self, key: str) -> bool:
        return key in self._permissions


class AuditService:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db
        self.logger = get_logger("multiplex.audit")

    async def record(
        self,
        event_type: str,
        *,
        actor: UserPrincipal | None,
        request_meta: dict[str, Any],
        target: dict[str, Any] | None = None,
        result: str = "success",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        created_at = now_utc()
        document = {
            "_id": uuid4().hex,
            "event_type": event_type,
            "actor_user_id": actor.user_id if actor else None,
            "actor_username": actor.username if actor else None,
            "target": target or {},
            "result": result,
            "ip": request_meta.get("ip"),
            "user_agent": request_meta.get("user_agent"),
            "metadata": metadata or {},
            "created_at": created_at,
        }
        await self.db.collection(AUDIT_EVENTS).insert_one(document)
        self.logger.info(
            f"Audit event recorded: {event_type}",
            extra={"event_type": f"audit.{event_type}", "payload": self._serialize_doc(document)},
        )
        return document

    async def list_events(self, limit: int = 100) -> list[dict[str, Any]]:
        cursor = self.db.collection(AUDIT_EVENTS).find().sort("created_at", -1).limit(limit)
        return [self._serialize_doc(item) async for item in cursor]

    def _serialize_doc(self, document: dict[str, Any]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in document.items():
            output[key] = value.isoformat() if isinstance(value, datetime) else value
        return output


class SettingsService:
    def __init__(self, db: DatabaseManager, settings: Settings, rate_limiter: RateLimiter, audit: AuditService) -> None:
        self.db = db
        self.settings = settings
        self.rate_limiter = rate_limiter
        self.audit = audit

    def _runtime_insert_defaults(self) -> dict[str, Any]:
        return {
            "_id": "runtime",
            "kind": "runtime",
            "registration_enabled": False,
            "mcp_enabled": True,
            "redis_runtime_enabled": self.settings.redis.enabled_on_startup or self.settings.redis.mode == "required",
            "created_at": now_utc(),
        }

    def _runtime_upsert_update(self, **changes: Any) -> dict[str, Any]:
        insert_defaults = self._runtime_insert_defaults()
        for key in changes:
            insert_defaults.pop(key, None)
        return {
            "$setOnInsert": insert_defaults,
            "$set": {**changes, "updated_at": now_utc()},
        }

    async def ensure_runtime_settings(self) -> dict[str, Any]:
        await self.db.collection(SETTINGS).update_one(
            {"_id": "runtime"},
            self._runtime_upsert_update(),
            upsert=True,
        )
        return await self.get_runtime_settings()

    async def get_runtime_settings(self) -> dict[str, Any]:
        document = await self.db.collection(SETTINGS).find_one({"_id": "runtime"})
        if not document:
            document = await self.ensure_runtime_settings()
        return document

    async def set_registration(self, enabled: bool, *, actor: UserPrincipal, request_meta: dict[str, Any]) -> dict[str, Any]:
        await self.db.collection(SETTINGS).update_one(
            {"_id": "runtime"},
            self._runtime_upsert_update(registration_enabled=enabled),
            upsert=True,
        )
        document = await self.get_runtime_settings()
        await self.audit.record(
            "settings.registration.update",
            actor=actor,
            request_meta=request_meta,
            target={"scope": "runtime"},
            metadata={"enabled": enabled},
        )
        return document

    async def set_mcp(self, enabled: bool, *, actor: UserPrincipal, request_meta: dict[str, Any]) -> dict[str, Any]:
        await self.db.collection(SETTINGS).update_one(
            {"_id": "runtime"},
            self._runtime_upsert_update(mcp_enabled=enabled),
            upsert=True,
        )
        document = await self.get_runtime_settings()
        await self.audit.record(
            "settings.mcp.update",
            actor=actor,
            request_meta=request_meta,
            target={"scope": "runtime"},
            metadata={"enabled": enabled},
        )
        return document

    async def set_redis_runtime(self, enabled: bool, *, actor: UserPrincipal, request_meta: dict[str, Any]) -> dict[str, Any]:
        if self.settings.redis.mode == "required" and not enabled:
            raise ValueError("Redis cannot be disabled while REDIS__MODE is set to 'required'")
        await self.rate_limiter.set_runtime_enabled(enabled)
        await self.db.collection(SETTINGS).update_one(
            {"_id": "runtime"},
            self._runtime_upsert_update(redis_runtime_enabled=enabled),
            upsert=True,
        )
        document = await self.get_runtime_settings()
        await self.audit.record(
            "settings.redis.update",
            actor=actor,
            request_meta=request_meta,
            target={"scope": "runtime"},
            metadata={"enabled": enabled},
        )
        return document


class UserService:
    def __init__(self, db: DatabaseManager, settings: Settings, permissions: PermissionCatalog, audit: AuditService) -> None:
        self.db = db
        self.settings = settings
        self.permissions = permissions
        self.audit = audit

    async def ensure_root_user(self) -> dict[str, Any]:
        collection = self.db.collection(USERS)
        current = await collection.find_one({"_id": "user_root"})
        password_hash = current.get("password_hash") if current else None
        pepper = self.settings.security.password_pepper.get_secret_value()
        root_password = self.settings.root.password.get_secret_value()
        if not password_hash or not verify_password(root_password, password_hash, pepper):
            password_hash = hash_password(root_password, pepper)
        created_at = current.get("created_at") if current else now_utc()
        document = {
            "_id": "user_root",
            "username": self.settings.root.username,
            "password_hash": password_hash,
            "is_root": True,
            "permissions": self.permissions.keys(),
            "email": str(self.settings.root.email),
            "tg_id": current.get("tg_id") if current else None,
            "vk_id": current.get("vk_id") if current else None,
            "created_at": created_at,
            "updated_at": now_utc(),
        }
        await collection.replace_one({"_id": "user_root"}, document, upsert=True)
        return document

    async def get_user_by_id(self, user_id: str) -> dict[str, Any] | None:
        return await self.db.collection(USERS).find_one({"_id": user_id})

    async def get_user_by_username(self, username: str) -> dict[str, Any] | None:
        return await self.db.collection(USERS).find_one({"username": username})

    async def list_users(self, limit: int = 200) -> list[dict[str, Any]]:
        cursor = self.db.collection(USERS).find().sort("username", 1).limit(limit)
        return [self.to_response(item) async for item in cursor]

    async def create_user(
        self,
        *,
        username: str,
        password: str,
        email: str | None = None,
        tg_id: str | None = None,
        vk_id: str | None = None,
        actor: UserPrincipal | None,
        request_meta: dict[str, Any],
    ) -> dict[str, Any]:
        existing = await self.get_user_by_username(username)
        if existing:
            raise ValueError("User already exists")
        pepper = self.settings.security.password_pepper.get_secret_value()
        created_at = now_utc()
        document = {
            "_id": f"user_{uuid4().hex}",
            "username": username,
            "password_hash": hash_password(password, pepper),
            "is_root": False,
            "permissions": [],
            "email": email,
            "tg_id": tg_id,
            "vk_id": vk_id,
            "created_at": created_at,
            "updated_at": created_at,
        }
        await self.db.collection(USERS).insert_one(document)
        await self.audit.record(
            "auth.register",
            actor=actor,
            request_meta=request_meta,
            target={"user_id": document["_id"], "username": username},
        )
        return document

    async def authenticate(self, username: str, password: str) -> UserPrincipal | None:
        user = await self.get_user_by_username(username)
        if not user:
            return None
        pepper = self.settings.security.password_pepper.get_secret_value()
        if not verify_password(password, user["password_hash"], pepper):
            return None
        return self.to_principal(user)

    async def update_profile(
        self,
        user: UserPrincipal,
        *,
        email: str | None,
        tg_id: str | None,
        vk_id: str | None,
        request_meta: dict[str, Any],
    ) -> dict[str, Any]:
        await self.db.collection(USERS).update_one(
            {"_id": user.user_id},
            {"$set": {"email": email, "tg_id": tg_id, "vk_id": vk_id, "updated_at": now_utc()}},
        )
        document = await self.get_user_by_id(user.user_id)
        assert document is not None
        await self.audit.record(
            "account.profile.update",
            actor=user,
            request_meta=request_meta,
            target={"user_id": user.user_id},
            metadata={"email": email, "tg_id": tg_id, "vk_id": vk_id},
        )
        return document

    async def mutate_permissions(
        self,
        user_id: str,
        permissions: list[str],
        mode: str,
        *,
        actor: UserPrincipal,
        request_meta: dict[str, Any],
    ) -> dict[str, Any]:
        if user_id == "user_root":
            raise ValueError("Root permissions are managed implicitly and cannot be modified")
        for permission in permissions:
            if not self.permissions.exists(permission):
                raise ValueError(f"Unknown permission: {permission}")
        document = await self.get_user_by_id(user_id)
        if not document:
            raise LookupError("User not found")
        current = set(document.get("permissions", []))
        if mode == "grant":
            current.update(permissions)
        elif mode == "revoke":
            current.difference_update(permissions)
        else:
            raise ValueError("mode must be either 'grant' or 'revoke'")
        await self.db.collection(USERS).update_one(
            {"_id": user_id},
            {"$set": {"permissions": sorted(current), "updated_at": now_utc()}},
        )
        updated = await self.get_user_by_id(user_id)
        assert updated is not None
        await self.audit.record(
            "users.permission.mutate",
            actor=actor,
            request_meta=request_meta,
            target={"user_id": user_id},
            metadata={"mode": mode, "permissions": permissions},
        )
        return updated

    def to_principal(self, document: dict[str, Any]) -> UserPrincipal:
        permissions = self.permissions.keys() if document.get("is_root") else sorted(document.get("permissions", []))
        return UserPrincipal(
            user_id=document["_id"],
            username=document["username"],
            is_root=bool(document.get("is_root")),
            permissions=permissions,
            email=document.get("email"),
            tg_id=document.get("tg_id"),
            vk_id=document.get("vk_id"),
        )

    def to_response(self, document: dict[str, Any]) -> dict[str, Any]:
        principal = self.to_principal(document)
        return {
            "user_id": principal.user_id,
            "username": principal.username,
            "is_root": principal.is_root,
            "permissions": principal.permissions,
            "email": principal.email,
            "tg_id": principal.tg_id,
            "vk_id": principal.vk_id,
            "created_at": serialize_datetime(document.get("created_at")),
            "updated_at": serialize_datetime(document.get("updated_at")),
        }


class AuthService:
    def __init__(self, db: DatabaseManager, settings: Settings, users: UserService) -> None:
        self.db = db
        self.settings = settings
        self.users = users

    async def issue_api_tokens(self, user: UserPrincipal, request_meta: dict[str, Any]) -> TokenBundle:
        access_token = create_jwt(
            subject=user.user_id,
            secret=self.settings.security.api_jwt_secret.get_secret_value(),
            issuer=self.settings.security_issuer,
            audience=self.settings.security.api_audience,
            token_type="api_access",
            ttl=timedelta(minutes=self.settings.security.access_token_ttl_minutes),
            extra={"username": user.username, "is_root": user.is_root, "permissions": user.permissions},
        )
        refresh_token = random_token(48)
        expires_at = now_utc() + timedelta(days=self.settings.security.refresh_token_ttl_days)
        await self.db.collection(REFRESH_TOKENS).insert_one(
            {
                "_id": uuid4().hex,
                "token_hash": sha256_text(refresh_token),
                "user_id": user.user_id,
                "purpose": "api",
                "client_id": None,
                "created_at": now_utc(),
                "expires_at": expires_at,
                "metadata": request_meta,
            }
        )
        return TokenBundle(access_token=access_token, refresh_token=refresh_token, expires_in=self.settings.security.access_token_ttl_minutes * 60)

    async def refresh_api_tokens(self, refresh_token: str, request_meta: dict[str, Any]) -> TokenBundle:
        token_hash = sha256_text(refresh_token)
        document = await self.db.collection(REFRESH_TOKENS).find_one({"token_hash": token_hash, "purpose": "api"})
        if not document:
            raise ValueError("Refresh token is invalid")
        if is_expired(document.get("expires_at")):
            raise ValueError("Refresh token expired")
        user_doc = await self.users.get_user_by_id(document["user_id"])
        if not user_doc:
            raise LookupError("User not found")
        await self.db.collection(REFRESH_TOKENS).delete_one({"_id": document["_id"]})
        return await self.issue_api_tokens(self.users.to_principal(user_doc), request_meta)

    async def revoke_refresh_token(self, refresh_token: str) -> None:
        await self.db.collection(REFRESH_TOKENS).delete_one({"token_hash": sha256_text(refresh_token)})

    def verify_api_access_token(self, token: str) -> dict[str, Any]:
        return decode_jwt(
            token,
            self.settings.security.api_jwt_secret.get_secret_value(),
            issuer=self.settings.security_issuer,
            audience=self.settings.security.api_audience,
            token_type="api_access",
        )


class OAuthService:
    def __init__(self, db: DatabaseManager, settings: Settings, users: UserService, audit: AuditService) -> None:
        self.db = db
        self.settings = settings
        self.users = users
        self.audit = audit

    async def list_clients(self) -> list[dict[str, Any]]:
        cursor = self.db.collection(OAUTH_CLIENTS).find().sort("created_at", -1)
        clients = []
        async for item in cursor:
            clients.append(self.serialize_client(item))
        return clients

    async def create_client(self, name: str, redirect_uris: list[str], allowed_scopes: list[str], client_id: str | None, confidential: bool) -> dict[str, Any]:
        supported = set(self.settings.oauth.supported_scopes)
        scopes = sorted({scope for scope in allowed_scopes if scope in supported}) or ["mcp"]
        client_identifier = client_id or f"mcp_{uuid4().hex}"
        client_secret = random_token(24) if confidential else None
        document = {
            "_id": client_identifier,
            "client_id": client_identifier,
            "name": name,
            "redirect_uris": redirect_uris,
            "allowed_scopes": scopes,
            "confidential": confidential,
            "client_secret_hash": sha256_text(client_secret) if client_secret else None,
            "created_at": now_utc(),
        }
        await self.db.collection(OAUTH_CLIENTS).insert_one(document)
        serialized = self.serialize_client(document)
        serialized["client_secret"] = client_secret
        return serialized

    async def validate_client(self, client_id: str, redirect_uri: str) -> dict[str, Any]:
        client = await self.db.collection(OAUTH_CLIENTS).find_one({"client_id": client_id})
        if not client:
            raise LookupError("Unknown OAuth client")
        if redirect_uri not in client.get("redirect_uris", []):
            raise ValueError("Redirect URI is not registered for this client")
        return client

    async def create_authorization_code(
        self,
        *,
        client_id: str,
        redirect_uri: str,
        user: UserPrincipal,
        scopes: list[str],
        code_challenge: str,
        code_challenge_method: str,
        request_meta: dict[str, Any],
    ) -> str:
        code = random_token(32)
        await self.db.collection(OAUTH_CODES).insert_one(
            {
                "_id": uuid4().hex,
                "code": code,
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "user_id": user.user_id,
                "scopes": scopes,
                "code_challenge": code_challenge,
                "code_challenge_method": code_challenge_method,
                "created_at": now_utc(),
                "expires_at": now_utc() + timedelta(minutes=self.settings.security.oauth_authorization_code_ttl_minutes),
            }
        )
        await self.audit.record(
            "oauth.authorize",
            actor=user,
            request_meta=request_meta,
            target={"client_id": client_id},
            metadata={"scopes": scopes},
        )
        return code

    async def exchange_code(
        self,
        *,
        code: str,
        client_id: str,
        redirect_uri: str,
        code_verifier: str,
        request_meta: dict[str, Any],
    ) -> dict[str, Any]:
        client = await self.validate_client(client_id, redirect_uri)
        document = await self.db.collection(OAUTH_CODES).find_one({"code": code, "client_id": client_id})
        if not document:
            raise ValueError("Authorization code is invalid")
        if is_expired(document.get("expires_at")):
            raise ValueError("Authorization code expired")
        if document.get("redirect_uri") != redirect_uri:
            raise ValueError("Redirect URI mismatch")
        if not verify_pkce(code_verifier, document["code_challenge"], document["code_challenge_method"]):
            raise ValueError("PKCE verification failed")
        await self.db.collection(OAUTH_CODES).delete_one({"_id": document["_id"]})
        user_doc = await self.users.get_user_by_id(document["user_id"])
        if not user_doc:
            raise LookupError("User not found")
        user = self.users.to_principal(user_doc)
        access_token = create_jwt(
            subject=user.user_id,
            secret=self.settings.security.oauth_jwt_secret.get_secret_value(),
            issuer=self.settings.oauth_issuer,
            audience=self.settings.security.mcp_audience,
            token_type="oauth_access",
            ttl=timedelta(minutes=self.settings.security.oauth_access_token_ttl_minutes),
            extra={
                "client_id": client_id,
                "scopes": document["scopes"],
                "username": user.username,
                "is_root": user.is_root,
                "permissions": user.permissions,
            },
        )
        refresh_token = random_token(48)
        await self.db.collection(REFRESH_TOKENS).insert_one(
            {
                "_id": uuid4().hex,
                "token_hash": sha256_text(refresh_token),
                "user_id": user.user_id,
                "purpose": "oauth",
                "client_id": client_id,
                "scopes": document["scopes"],
                "created_at": now_utc(),
                "expires_at": now_utc() + timedelta(days=self.settings.security.oauth_refresh_token_ttl_days),
                "metadata": request_meta,
            }
        )
        await self.audit.record(
            "oauth.token.issue",
            actor=user,
            request_meta=request_meta,
            target={"client_id": client_id},
            metadata={"scopes": document["scopes"], "client_name": client["name"]},
        )
        return {
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": self.settings.security.oauth_access_token_ttl_minutes * 60,
            "refresh_token": refresh_token,
            "scope": " ".join(document["scopes"]),
            "client_name": client["name"],
        }

    async def refresh_token(self, *, refresh_token: str, client_id: str, request_meta: dict[str, Any]) -> dict[str, Any]:
        document = await self.db.collection(REFRESH_TOKENS).find_one({"token_hash": sha256_text(refresh_token), "purpose": "oauth", "client_id": client_id})
        if not document:
            raise ValueError("Refresh token is invalid")
        if is_expired(document.get("expires_at")):
            raise ValueError("Refresh token expired")
        user_doc = await self.users.get_user_by_id(document["user_id"])
        if not user_doc:
            raise LookupError("User not found")
        user = self.users.to_principal(user_doc)
        await self.db.collection(REFRESH_TOKENS).delete_one({"_id": document["_id"]})
        access_token = create_jwt(
            subject=user.user_id,
            secret=self.settings.security.oauth_jwt_secret.get_secret_value(),
            issuer=self.settings.oauth_issuer,
            audience=self.settings.security.mcp_audience,
            token_type="oauth_access",
            ttl=timedelta(minutes=self.settings.security.oauth_access_token_ttl_minutes),
            extra={
                "client_id": client_id,
                "scopes": document.get("scopes", ["mcp"]),
                "username": user.username,
                "is_root": user.is_root,
                "permissions": user.permissions,
            },
        )
        next_refresh = random_token(48)
        await self.db.collection(REFRESH_TOKENS).insert_one(
            {
                "_id": uuid4().hex,
                "token_hash": sha256_text(next_refresh),
                "user_id": user.user_id,
                "purpose": "oauth",
                "client_id": client_id,
                "scopes": document.get("scopes", ["mcp"]),
                "created_at": now_utc(),
                "expires_at": now_utc() + timedelta(days=self.settings.security.oauth_refresh_token_ttl_days),
                "metadata": request_meta,
            }
        )
        return {
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": self.settings.security.oauth_access_token_ttl_minutes * 60,
            "refresh_token": next_refresh,
            "scope": " ".join(document.get("scopes", ["mcp"])),
        }

    async def revoke_token(self, token: str) -> None:
        await self.db.collection(REFRESH_TOKENS).delete_one({"token_hash": sha256_text(token)})

    def verify_access_token(self, token: str) -> dict[str, Any]:
        return decode_jwt(
            token,
            self.settings.security.oauth_jwt_secret.get_secret_value(),
            issuer=self.settings.oauth_issuer,
            audience=self.settings.security.mcp_audience,
            token_type="oauth_access",
        )

    def authorization_server_metadata(self) -> dict[str, Any]:
        return {
            "issuer": self.settings.oauth_issuer,
            "authorization_endpoint": self.settings.authorization_endpoint,
            "token_endpoint": self.settings.token_endpoint,
            "registration_endpoint": f"{self.settings.oauth_issuer}/register",
            "revocation_endpoint": self.settings.revocation_endpoint,
            "jwks_uri": self.settings.jwks_uri,
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "token_endpoint_auth_methods_supported": ["none"],
            "code_challenge_methods_supported": ["S256"] + (["plain"] if self.settings.oauth.allow_plain_pkce else []),
            "scopes_supported": self.settings.oauth.supported_scopes,
        }

    def protected_resource_metadata(self) -> dict[str, Any]:
        return {
            "resource": f"{self.settings.public_base_url}{self.settings.mcp_path}",
            "authorization_servers": [self.settings.oauth_issuer],
            "bearer_methods_supported": ["header"],
            "scopes_supported": self.settings.oauth.supported_scopes,
        }

    def serialize_client(self, document: dict[str, Any]) -> dict[str, Any]:
        return {
            "client_id": document["client_id"],
            "name": document["name"],
            "redirect_uris": document["redirect_uris"],
            "allowed_scopes": document["allowed_scopes"],
            "confidential": bool(document.get("confidential")),
            "created_at": serialize_datetime(document.get("created_at")),
        }


class PluginRegistry:
    def __init__(
        self,
        db: DatabaseManager,
        settings: Settings,
        permissions: PermissionCatalog,
        audit: AuditService,
        settings_service: SettingsService,
        rate_limiter: RateLimiter,
    ) -> None:
        self.db = db
        self.settings = settings
        self.permissions = permissions
        self.audit = audit
        self.settings_service = settings_service
        self.rate_limiter = rate_limiter
        self.logger = get_logger("multiplex.plugins")
        self.plugins: dict[str, PluginDefinition] = {}
        self._services: ApplicationServices | None = None

    def attach_services(self, services: "ApplicationServices") -> None:
        self._services = services

    async def load_plugins(self) -> None:
        for module_name in self._iter_plugin_module_names():
            await self._load_plugin_module(module_name)

    async def reload_plugins(self, plugin_keys: list[str] | None = None) -> list[str]:
        loaded: list[str] = []
        targets = plugin_keys or [module_name.rsplit(".", 1)[-1] for module_name in self._iter_plugin_module_names()]
        for key in targets:
            old_plugin = self.plugins.get(key)
            if old_plugin and old_plugin.shutdown and self._services:
                await old_plugin.shutdown(self._services)
            await self._load_plugin_module(f"server.mcp.plugins.{key}", reload_existing=True)
            loaded.append(key)
        return loaded

    def _iter_plugin_module_names(self) -> list[str]:
        plugins_dir = Path(__file__).resolve().parent / "mcp" / "plugins"
        return sorted(
            f"server.mcp.plugins.{module.name}"
            for module in pkgutil.iter_modules([str(plugins_dir)])
            if not module.name.startswith("_")
        )

    async def _load_plugin_module(self, module_name: str, reload_existing: bool = False) -> None:
        module = importlib.import_module(module_name)
        if reload_existing:
            module = importlib.reload(module)
        plugin: PluginDefinition = getattr(module, "PLUGIN")
        self.permissions.register_plugin_permissions(plugin.manifest.permissions)
        self.plugins[plugin.manifest.key] = plugin
        await self._persist_plugin(plugin)
        if plugin.startup and self._services:
            await plugin.startup(self._services)

    async def _persist_plugin(self, plugin: PluginDefinition) -> None:
        await self.db.collection(PLUGINS).update_one(
            {"key": plugin.manifest.key},
            {
                "$setOnInsert": {
                    "_id": plugin.manifest.key,
                    "key": plugin.manifest.key,
                    "enabled": plugin.manifest.enabled_by_default,
                    "created_at": now_utc(),
                },
                "$set": {
                    "name": plugin.manifest.name,
                    "version": plugin.manifest.version,
                    "description": plugin.manifest.description,
                    "os_support": plugin.manifest.os_support,
                    "updated_at": now_utc(),
                },
            },
            upsert=True,
        )
        for tool in plugin.tools.values():
            default_enabled = tool.manifest.default_global_enabled and tool.manifest.read_only
            await self.db.collection(TOOL_POLICIES).update_one(
                {"tool_key": tool.manifest.key, "scope": "global", "subject_id": "*"},
                {
                    "$setOnInsert": {
                        "_id": f"global:{tool.manifest.key}",
                        "tool_key": tool.manifest.key,
                        "scope": "global",
                        "subject_id": "*",
                        "enabled": default_enabled,
                        "created_at": now_utc(),
                    },
                    "$set": {"updated_at": now_utc()},
                },
                upsert=True,
            )

    async def set_plugin_enabled(self, plugin_key: str, enabled: bool, *, actor: UserPrincipal, request_meta: dict[str, Any]) -> None:
        if plugin_key not in self.plugins:
            raise LookupError("Plugin not found")
        await self.db.collection(PLUGINS).update_one(
            {"key": plugin_key},
            {"$set": {"enabled": enabled, "updated_at": now_utc()}},
            upsert=False,
        )
        await self.audit.record(
            "mcp.plugin.update",
            actor=actor,
            request_meta=request_meta,
            target={"plugin_key": plugin_key},
            metadata={"enabled": enabled},
        )

    async def list_plugins(self) -> list[dict[str, Any]]:
        cursor = self.db.collection(PLUGINS).find().sort("key", 1)
        documents: list[dict[str, Any]] = []
        async for item in cursor:
            runtime_plugin = self.plugins.get(item["key"])
            availability = await self._plugin_availability(runtime_plugin) if runtime_plugin else RuntimeAvailability(available=False, reason="Plugin is not loaded")
            documents.append(
                {
                    "key": item["key"],
                    "name": item["name"],
                    "version": item["version"],
                    "description": item["description"],
                    "enabled": bool(item.get("enabled", True)),
                    "os_support": item.get("os_support", []),
                    "tool_keys": sorted(runtime_plugin.tools.keys()) if runtime_plugin else [],
                    "available": availability.available,
                    "availability_reason": availability.reason,
                    "required_backends": availability.required_backends,
                    "providers": availability.providers,
                }
            )
        return documents

    async def list_tools(self) -> list[dict[str, Any]]:
        documents: list[dict[str, Any]] = []
        for plugin in self.plugins.values():
            plugin_doc = await self.db.collection(PLUGINS).find_one({"key": plugin.manifest.key})
            if plugin_doc and not plugin_doc.get("enabled", True):
                continue
            for tool in plugin.tools.values():
                global_policy = await self.db.collection(TOOL_POLICIES).find_one({"tool_key": tool.manifest.key, "scope": "global", "subject_id": "*"})
                availability = await self._tool_availability(plugin, tool)
                documents.append(
                    {
                        "key": tool.manifest.key,
                        "plugin_key": plugin.manifest.key,
                        "name": tool.manifest.name,
                        "description": tool.manifest.description,
                        "read_only": tool.manifest.read_only,
                        "permissions": tool.manifest.permissions,
                        "tags": tool.manifest.tags,
                        "global_enabled": bool(global_policy.get("enabled", True)) if global_policy else True,
                        "available": availability.available,
                        "availability_reason": availability.reason,
                        "os_support": tool.manifest.os_support,
                        "required_backends": availability.required_backends,
                        "providers": availability.providers,
                    }
                )
        return documents

    async def set_global_tool_enabled(self, tool_key: str, enabled: bool, *, actor: UserPrincipal, request_meta: dict[str, Any]) -> None:
        if self.get_tool(tool_key) is None:
            raise LookupError("Tool not found")
        await self.db.collection(TOOL_POLICIES).update_one(
            {"tool_key": tool_key, "scope": "global", "subject_id": "*"},
            {
                "$set": {"enabled": enabled, "updated_at": now_utc()},
                "$setOnInsert": {"_id": f"global:{tool_key}", "created_at": now_utc()},
            },
            upsert=True,
        )
        await self.audit.record(
            "mcp.tool.global.update",
            actor=actor,
            request_meta=request_meta,
            target={"tool_key": tool_key},
            metadata={"enabled": enabled},
        )

    async def set_user_tool_enabled(
        self,
        user_id: str,
        tool_key: str,
        enabled: bool,
        *,
        actor: UserPrincipal,
        request_meta: dict[str, Any],
    ) -> None:
        if self.get_tool(tool_key) is None:
            raise LookupError("Tool not found")
        await self.db.collection(TOOL_POLICIES).update_one(
            {"tool_key": tool_key, "scope": "user", "subject_id": user_id},
            {
                "$set": {"enabled": enabled, "updated_at": now_utc()},
                "$setOnInsert": {"_id": f"user:{user_id}:{tool_key}", "created_at": now_utc()},
            },
            upsert=True,
        )
        await self.audit.record(
            "mcp.tool.user.update",
            actor=actor,
            request_meta=request_meta,
            target={"tool_key": tool_key, "user_id": user_id},
            metadata={"enabled": enabled},
        )

    def get_tool(self, tool_key: str) -> MCPTool | None:
        for plugin in self.plugins.values():
            if tool_key in plugin.tools:
                return plugin.tools[tool_key]
        return None

    def get_plugin_for_tool(self, tool_key: str) -> PluginDefinition | None:
        for plugin in self.plugins.values():
            if tool_key in plugin.tools:
                return plugin
        return None

    async def _plugin_availability(self, plugin: PluginDefinition | None) -> RuntimeAvailability:
        if plugin is None:
            return RuntimeAvailability(available=False, reason="Plugin is not loaded")
        availability = RuntimeAvailability(
            available=True,
            required_backends=list(plugin.manifest.required_backends),
            providers=list(plugin.manifest.providers),
        )
        if self._services is None:
            return availability
        availability = self._merge_availability(
            availability,
            self._services.host_ops.availability_for_os(plugin.manifest.os_support),
        )
        if plugin.availability:
            availability = self._merge_availability(availability, await plugin.availability(self._services))
        return availability

    async def _tool_availability(self, plugin: PluginDefinition, tool: MCPTool) -> RuntimeAvailability:
        availability = self._merge_availability(
            await self._plugin_availability(plugin),
            RuntimeAvailability(
                available=True,
                required_backends=list(tool.manifest.required_backends),
                providers=list(tool.manifest.providers),
            ),
        )
        if self._services is None:
            return availability
        availability = self._merge_availability(
            availability,
            self._services.host_ops.availability_for_os(tool.manifest.os_support),
        )
        if tool.availability:
            availability = self._merge_availability(availability, await tool.availability(self._services))
        return availability

    def _merge_availability(self, *items: RuntimeAvailability) -> RuntimeAvailability:
        available = all(item.available for item in items)
        reason = next((item.reason for item in items if not item.available and item.reason), None)
        required_backends = sorted({backend for item in items for backend in item.required_backends})
        providers = sorted({provider for item in items for provider in item.providers})
        return RuntimeAvailability(
            available=available,
            reason=reason,
            required_backends=required_backends,
            providers=providers,
        )

    async def is_tool_enabled_for_user(self, user: UserPrincipal, tool_key: str) -> bool:
        runtime = await self.settings_service.get_runtime_settings()
        if not runtime.get("mcp_enabled", True):
            return False
        tool = self.get_tool(tool_key)
        if tool is None:
            return False
        plugin = self.get_plugin_for_tool(tool_key)
        if plugin is None:
            return False
        if not (await self._tool_availability(plugin, tool)).available:
            return False
        plugin_doc = await self.db.collection(PLUGINS).find_one({"key": plugin.manifest.key})
        if plugin_doc and not plugin_doc.get("enabled", True):
            return False
        global_policy = await self.db.collection(TOOL_POLICIES).find_one({"tool_key": tool_key, "scope": "global", "subject_id": "*"})
        if global_policy and not global_policy.get("enabled", True):
            return False
        if user.is_root:
            return True
        if not set(tool.manifest.permissions).issubset(set(user.permissions)):
            return False
        user_policy = await self.db.collection(TOOL_POLICIES).find_one({"tool_key": tool_key, "scope": "user", "subject_id": user.user_id})
        return bool(user_policy and user_policy.get("enabled"))

    async def describe_tools_for_user(self, user: UserPrincipal) -> list[dict[str, Any]]:
        runtime = await self.settings_service.get_runtime_settings()
        if not runtime.get("mcp_enabled", True):
            return []
        tools: list[dict[str, Any]] = []
        for plugin in self.plugins.values():
            plugin_doc = await self.db.collection(PLUGINS).find_one({"key": plugin.manifest.key})
            if plugin_doc and not plugin_doc.get("enabled", True):
                continue
            for tool in plugin.tools.values():
                if await self.is_tool_enabled_for_user(user, tool.manifest.key):
                    tools.append(
                        {
                            "name": tool.manifest.key,
                            "title": tool.manifest.name,
                            "description": tool.manifest.description,
                            "inputSchema": tool.manifest.input_schema,
                            "annotations": {"readOnlyHint": tool.manifest.read_only},
                            "tags": tool.manifest.tags,
                        }
                    )
        return sorted(tools, key=lambda item: item["name"])

    async def call_tool(self, user: UserPrincipal, tool_key: str, arguments: dict[str, Any], request_meta: dict[str, Any]) -> dict[str, Any]:
        tool = self.get_tool(tool_key)
        if tool is None:
            raise LookupError("Tool not found")
        plugin = self.get_plugin_for_tool(tool_key)
        if plugin is None:
            raise LookupError("Plugin not found")
        availability = await self._tool_availability(plugin, tool)
        if not availability.available:
            raise RuntimeError(availability.reason or "Tool is currently unavailable")
        if not await self.is_tool_enabled_for_user(user, tool_key):
            raise PermissionError("Tool access denied")
        policy_name = "mcp_read" if tool.manifest.read_only else "mcp_write"
        await self.rate_limiter.enforce(policy_name, f"{user.user_id}:{tool_key}")
        if self._services is None:
            raise RuntimeError("Plugin services are not attached")
        context = ToolExecutionContext(user=user, services=self._services, request_meta=request_meta)
        result = await tool.handler(context, arguments)
        redacted_arguments = self._services.host_ops.redact_arguments(
            arguments,
            sensitive_fields=tool.manifest.audit_redact_fields,
            max_string_length=tool.manifest.audit_max_string_length,
        )
        await self.audit.record(
            "mcp.tool.call",
            actor=user,
            request_meta=request_meta,
            target={"tool_key": tool_key},
            metadata={"arguments": redacted_arguments, "read_only": tool.manifest.read_only},
        )
        return result if isinstance(result, dict) else {"result": result}


@dataclass(slots=True)
class ApplicationServices:
    settings: Settings
    db: DatabaseManager
    logger_manager: IntegrityLogManager
    mailer: Mailer
    host_ops: HostOpsService
    alerts: AlertingService
    permissions: PermissionCatalog
    audit: AuditService
    rate_limiter: RateLimiter
    settings_service: SettingsService
    users: UserService
    auth: AuthService
    oauth: OAuthService
    plugins: PluginRegistry
    verifier_task: asyncio.Task[Any] | None = None


def build_rate_limit_policies(settings: Settings) -> dict[str, RateLimitPolicy]:
    return {
        "login": RateLimitPolicy("login", settings.rate_limits.login_limit, settings.rate_limits.login_window_seconds),
        "register": RateLimitPolicy("register", settings.rate_limits.register_limit, settings.rate_limits.register_window_seconds),
        "oauth_token": RateLimitPolicy("oauth_token", settings.rate_limits.oauth_token_limit, settings.rate_limits.oauth_token_window_seconds),
        "rest_read": RateLimitPolicy("rest_read", settings.rate_limits.rest_read_limit, settings.rate_limits.rest_read_window_seconds),
        "rest_write": RateLimitPolicy("rest_write", settings.rate_limits.rest_write_limit, settings.rate_limits.rest_write_window_seconds),
        "mcp_read": RateLimitPolicy("mcp_read", settings.rate_limits.mcp_read_limit, settings.rate_limits.mcp_read_window_seconds),
        "mcp_write": RateLimitPolicy("mcp_write", settings.rate_limits.mcp_write_limit, settings.rate_limits.mcp_write_window_seconds),
    }


async def build_application_services(settings: Settings, logger_manager: IntegrityLogManager, mailer: Mailer) -> ApplicationServices:
    validate_runtime_security(settings)
    db = DatabaseManager(settings)
    await db.connect()
    await db.ensure_indexes()
    host_ops = HostOpsService(settings)

    permissions = PermissionCatalog()
    permissions.register_many(CORE_PERMISSIONS)

    rate_limiter = RateLimiter(
        build_rate_limit_policies(settings),
        redis_mode=settings.redis.mode,
        redis_url=settings.redis.url,
        redis_runtime_enabled=settings.redis.enabled_on_startup or settings.redis.mode == "required",
    )
    await rate_limiter.initialize()

    audit = AuditService(db)
    settings_service = SettingsService(db, settings, rate_limiter, audit)
    await settings_service.ensure_runtime_settings()

    users = UserService(db, settings, permissions, audit)
    auth = AuthService(db, settings, users)
    oauth = OAuthService(db, settings, users, audit)
    alerts = AlertingService(db, host_ops, mailer, settings.host_ops.alert_poll_interval_seconds)
    plugins = PluginRegistry(db, settings, permissions, audit, settings_service, rate_limiter)

    services = ApplicationServices(
        settings=settings,
        db=db,
        logger_manager=logger_manager,
        mailer=mailer,
        host_ops=host_ops,
        alerts=alerts,
        permissions=permissions,
        audit=audit,
        rate_limiter=rate_limiter,
        settings_service=settings_service,
        users=users,
        auth=auth,
        oauth=oauth,
        plugins=plugins,
    )
    plugins.attach_services(services)
    await users.ensure_root_user()
    await plugins.load_plugins()
    await users.ensure_root_user()
    runtime = await settings_service.get_runtime_settings()
    await rate_limiter.set_runtime_enabled(bool(runtime.get("redis_runtime_enabled")))
    return services


async def shutdown_application_services(services: ApplicationServices) -> None:
    for plugin in services.plugins.plugins.values():
        if plugin.shutdown:
            await plugin.shutdown(services)
    await services.alerts.stop()
    if services.verifier_task is not None:
        services.verifier_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await services.verifier_task
    await services.rate_limiter.shutdown()
    await services.db.disconnect()


async def periodic_integrity_verifier(services: ApplicationServices) -> None:
    interval = services.settings.logging.verifier_interval_seconds
    while True:
        try:
            await services.logger_manager.verify_integrity()
        except Exception:
            LOGGER.exception("Integrity verification loop failed")
        await asyncio.sleep(interval)
