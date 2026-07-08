"""Home Assistant integration service.

Provides authentication (access + refresh tokens with 2FA support),
state collection for the single /api/ha/state endpoint,
device diagnostics, and action execution for switches/buttons.
"""

from __future__ import annotations

import asyncio
import platform
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import psutil

from server.core.config import HAConfig
from server.core.database import (
    HA_ACCESS_TOKENS,
    HA_REFRESH_TOKENS,
    SYSTEM_CONFIG,
)
from server.core.logging import get_logger
from server.core.security import (
    SecurityError,
    create_jwt,
    decode_jwt,
    now_utc,
    random_token,
    verify_password,
    verify_totp_code,
)
from server.models import (
    HABinarySensorsData,
    HAChallengeResponse,
    HAConnectionInfo,
    HADiagnosticsResponse,
    HASensorsData,
    HAStateResponse,
    HAStateMeta,
    HASwitchesData,
    HATokenResponse,
)

if TYPE_CHECKING:
    from server.core.database import DatabaseManager

LOGGER = get_logger("multiplex.ha")

# Token type identifiers — isolated from API and OAuth tokens
_HA_ACCESS_TYPE = "ha_access"
_HA_REFRESH_TYPE = "ha_refresh"
_HA_CHALLENGE_TYPE = "ha_2fa_challenge"
_HA_AUDIENCE = "home-assistant"
_HA_REFRESH_AUDIENCE = "home-assistant-refresh"
_HA_CHALLENGE_AUDIENCE = "ha-2fa-challenge"
_CHALLENGE_TTL_MINUTES = 5


class HAService:
    """Service layer for the Home Assistant integration."""

    def __init__(self, db: DatabaseManager, settings_obj: Any, issuer: str) -> None:
        self._db = db
        self._settings: HAConfig = settings_obj.ha
        self._full_settings = settings_obj
        self._issuer = issuer

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _access_token(self, user_id: str, label: str) -> tuple[str, str, int]:
        """Create HA access token. Returns (token, jti, expires_in_seconds)."""
        ttl = timedelta(minutes=self._settings.access_token_ttl_minutes)
        jti = random_token(16)
        token = create_jwt(
            subject=user_id,
            secret=self._settings.jwt_secret.get_secret_value(),
            issuer=self._issuer,
            audience=_HA_AUDIENCE,
            token_type=_HA_ACCESS_TYPE,
            ttl=ttl,
            extra={"account_label": label, "jti": jti},
        )
        return token, jti, int(ttl.total_seconds())

    def _refresh_token_jwt(self, user_id: str, label: str) -> tuple[str, str]:
        """Create HA refresh token. Returns (token, jti)."""
        ttl = timedelta(days=self._settings.refresh_token_ttl_days)
        jti = random_token(16)
        token = create_jwt(
            subject=user_id,
            secret=self._settings.refresh_jwt_secret.get_secret_value(),
            issuer=self._issuer,
            audience=_HA_REFRESH_AUDIENCE,
            token_type=_HA_REFRESH_TYPE,
            ttl=ttl,
            extra={"account_label": label, "jti": jti},
        )
        return token, jti

    def _challenge_token(self, user_id: str, label: str) -> str:
        """Create short-lived 2FA challenge token (5 min)."""
        ttl = timedelta(minutes=_CHALLENGE_TTL_MINUTES)
        return create_jwt(
            subject=user_id,
            secret=self._settings.jwt_secret.get_secret_value(),
            issuer=self._issuer,
            audience=_HA_CHALLENGE_AUDIENCE,
            token_type=_HA_CHALLENGE_TYPE,
            ttl=ttl,
            extra={"account_label": label},
        )

    async def _store_access_token(
        self,
        jti: str,
        user_id: str,
        label: str,
        expires_at: datetime,
        client_ip: str | None,
    ) -> None:
        col = self._db.collection(HA_ACCESS_TOKENS)
        now = now_utc()
        await col.insert_one(
            {
                "jti": jti,
                "user_id": user_id,
                "account_label": label,
                "created_at": now,
                "expires_at": expires_at,
                "last_used_at": now,
                "is_revoked": False,
                "client_ip": client_ip,
            }
        )

    async def _store_refresh_token(
        self,
        jti: str,
        user_id: str,
        label: str,
        expires_at: datetime,
        client_ip: str | None,
    ) -> None:
        col = self._db.collection(HA_REFRESH_TOKENS)
        now = now_utc()
        await col.insert_one(
            {
                "jti": jti,
                "user_id": user_id,
                "account_label": label,
                "created_at": now,
                "expires_at": expires_at,
                "last_used_at": now,
                "is_revoked": False,
                "client_ip": client_ip,
            }
        )

    async def _build_token_response(
        self,
        user_id: str,
        label: str,
        client_ip: str | None,
    ) -> HATokenResponse:
        """Issue a fresh access + refresh token pair and persist metadata."""
        access_token, access_jti, expires_in = self._access_token(user_id, label)
        refresh_token, refresh_jti = self._refresh_token_jwt(user_id, label)

        now = now_utc()
        access_exp = now + timedelta(minutes=self._settings.access_token_ttl_minutes)
        refresh_exp = now + timedelta(days=self._settings.refresh_token_ttl_days)

        await asyncio.gather(
            self._store_access_token(access_jti, user_id, label, access_exp, client_ip),
            self._store_refresh_token(refresh_jti, user_id, label, refresh_exp, client_ip),
        )

        return HATokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=expires_in,
            account_label=label,
        )

    # ── Auth ────────────────────────────────────────────────────────────────────

    async def authenticate(
        self,
        username: str,
        password: str,
        label: str,
        client_ip: str | None,
        *,
        users_service: Any,
    ) -> HATokenResponse | HAChallengeResponse:
        """Primary authentication. Returns tokens or 2FA challenge."""
        user = await users_service.get_user_by_username(username)
        if user is None or not verify_password(
            password,
            user.get("password_hash", ""),
            self._full_settings.security.password_pepper.get_secret_value(),
        ):
            raise ValueError("Invalid username or password")

        user_id = str(user["_id"])

        # Check if 2FA is enabled
        totp_secret = user.get("totp_secret")
        totp_enabled = user.get("totp_enabled", False) and totp_secret

        if totp_enabled:
            challenge = self._challenge_token(user_id, label)
            return HAChallengeResponse(challenge_token=challenge)

        return await self._build_token_response(user_id, label, client_ip)

    async def authenticate_2fa(
        self,
        challenge_token: str,
        totp_code: str,
        client_ip: str | None,
        *,
        users_service: Any,
    ) -> HATokenResponse:
        """Complete 2FA challenge and issue tokens."""
        try:
            payload = decode_jwt(
                challenge_token,
                self._settings.jwt_secret.get_secret_value(),
                issuer=self._issuer,
                audience=_HA_CHALLENGE_AUDIENCE,
                token_type=_HA_CHALLENGE_TYPE,
            )
        except SecurityError as exc:
            raise ValueError("Invalid or expired challenge token") from exc

        user_id = payload["sub"]
        label = payload.get("account_label", "Home Assistant")

        user = await users_service.get_user_by_id(user_id)
        if user is None:
            raise ValueError("User not found")

        totp_secret = user.get("totp_secret")
        if not totp_secret or not verify_totp_code(totp_secret, totp_code):
            raise ValueError("Invalid TOTP code")

        return await self._build_token_response(user_id, label, client_ip)

    async def refresh_access_token(
        self,
        refresh_token: str,
        client_ip: str | None,
    ) -> HATokenResponse:
        """Exchange refresh token for a new access + refresh token pair (rotation)."""
        try:
            payload = decode_jwt(
                refresh_token,
                self._settings.refresh_jwt_secret.get_secret_value(),
                issuer=self._issuer,
                audience=_HA_REFRESH_AUDIENCE,
                token_type=_HA_REFRESH_TYPE,
            )
        except SecurityError as exc:
            raise ValueError("Invalid or expired refresh token") from exc

        jti = payload["jti"]
        user_id = payload["sub"]
        label = payload.get("account_label", "Home Assistant")

        col = self._db.collection(HA_REFRESH_TOKENS)
        doc = await col.find_one({"jti": jti})

        if doc is None or doc.get("is_revoked"):
            # Possible replay attack — revoke all tokens for this user
            if doc and doc.get("is_revoked"):
                LOGGER.warning(
                    "HA refresh token replay detected, revoking all tokens",
                    extra={
                        "event_type": "ha.auth.replay",
                        "payload": {"user_id": user_id},
                    },
                )
                await self.revoke_all_user_tokens(user_id)
            raise ValueError("Refresh token is invalid or has been revoked")

        # Rotate: revoke old refresh token immediately
        await col.update_one({"jti": jti}, {"$set": {"is_revoked": True}})

        return await self._build_token_response(user_id, label, client_ip)

    async def revoke_refresh_token(self, jti: str, user_id: str) -> None:
        """Revoke a specific refresh token (user logout)."""
        col = self._db.collection(HA_REFRESH_TOKENS)
        await col.update_one(
            {"jti": jti, "user_id": user_id},
            {"$set": {"is_revoked": True}},
        )

    async def revoke_all_user_tokens(self, user_id: str) -> None:
        """Revoke all HA access and refresh tokens for a user."""
        access_col = self._db.collection(HA_ACCESS_TOKENS)
        refresh_col = self._db.collection(HA_REFRESH_TOKENS)
        await asyncio.gather(
            access_col.update_many(
                {"user_id": user_id, "is_revoked": False},
                {"$set": {"is_revoked": True}},
            ),
            refresh_col.update_many(
                {"user_id": user_id, "is_revoked": False},
                {"$set": {"is_revoked": True}},
            ),
        )
        LOGGER.info(
            "Revoked all HA tokens for user",
            extra={
                "event_type": "ha.auth.revoke_all",
                "payload": {"user_id": user_id},
            },
        )

    # ── Token Verification ─────────────────────────────────────────────────────

    def verify_ha_access_token(self, token: str) -> dict[str, Any]:
        """Verify HA access token signature and claims (synchronous)."""
        return decode_jwt(
            token,
            self._settings.jwt_secret.get_secret_value(),
            issuer=self._issuer,
            audience=_HA_AUDIENCE,
            token_type=_HA_ACCESS_TYPE,
        )

    async def is_access_token_revoked(self, jti: str) -> bool:
        """Check whether an access token JTI has been revoked."""
        col = self._db.collection(HA_ACCESS_TOKENS)
        doc = await col.find_one({"jti": jti}, {"is_revoked": 1})
        if doc is None:
            return True  # unknown token → treat as revoked
        return bool(doc.get("is_revoked", False))

    async def touch_access_token(self, jti: str) -> None:
        """Update last_used_at for an access token (fire-and-forget)."""
        try:
            col = self._db.collection(HA_ACCESS_TOKENS)
            await col.update_one({"jti": jti}, {"$set": {"last_used_at": now_utc()}})
        except Exception:  # noqa: BLE001
            pass

    # ── Connections (Profile UI) ───────────────────────────────────────────────

    async def list_user_connections(self, user_id: str) -> list[HAConnectionInfo]:
        """Return active HA connections for a user (for Profile UI)."""
        col = self._db.collection(HA_REFRESH_TOKENS)
        cursor = col.find(
            {"user_id": user_id, "is_revoked": False},
            sort=[("created_at", -1)],
        )
        connections: list[HAConnectionInfo] = []
        async for doc in cursor:
            connections.append(
                HAConnectionInfo(
                    jti=doc["jti"],
                    account_label=doc.get("account_label", "Home Assistant"),
                    created_at=doc["created_at"].isoformat(),
                    last_used_at=(
                        doc["last_used_at"].isoformat() if doc.get("last_used_at") else None
                    ),
                    expires_at=doc["expires_at"].isoformat(),
                    client_ip=doc.get("client_ip"),
                )
            )
        return connections

    # ── Instance Serial ────────────────────────────────────────────────────────

    async def get_or_create_instance_serial(self) -> str:
        """Get persistent instance serial (created once, stored in MongoDB)."""
        col = self._db.collection(SYSTEM_CONFIG)
        doc = await col.find_one({"key": "instance_serial"})
        if doc:
            return str(doc["value"])
        serial = f"multiplex-{uuid4().hex[:8]}"
        await col.update_one(
            {"key": "instance_serial"},
            {
                "$setOnInsert": {
                    "key": "instance_serial",
                    "value": serial,
                    "created_at": now_utc(),
                }
            },
            upsert=True,
        )
        # Re-read in case of race condition
        doc = await col.find_one({"key": "instance_serial"})
        return str(doc["value"]) if doc else serial

    # ── State (Single Polling Endpoint) ────────────────────────────────────────

    async def get_full_state(
        self,
        *,
        redis_client: Any | None,
        ha_config: HAConfig,
        runtime_settings: dict[str, Any],
        mcp_healthy: bool = True,
        python_mirror_running: bool = True,
        pypi_mirror_running: bool = True,
    ) -> HAStateResponse:
        """Collect all state data in a single call (parallel)."""
        sensors_task = asyncio.create_task(self._collect_sensors(redis_client))
        binary_sensors_task = asyncio.create_task(
            self._collect_binary_sensors(redis_client)
        )

        sensors, binary_sensors = await asyncio.gather(
            sensors_task, binary_sensors_task
        )

        # Override binary sensor placeholders with real service state
        binary_sensors.mcp_healthy = mcp_healthy
        binary_sensors.python_mirror_running = python_mirror_running
        binary_sensors.pypi_mirror_running = pypi_mirror_running

        # Switches — only if enabled in config
        switches = HASwitchesData()
        if ha_config.switches_enabled:
            switches = HASwitchesData(
                enable_registration=runtime_settings.get("registration_enabled", False),
                enable_mcp=runtime_settings.get("mcp_enabled", True),
                enable_redis=runtime_settings.get("redis_runtime_enabled", False),
            )

        return HAStateResponse(
            sensors=sensors,
            binary_sensors=binary_sensors,
            switches=switches,
            meta=HAStateMeta(
                server_time=now_utc().isoformat(),
                poll_interval_hint=ha_config.default_poll_interval_seconds,
                destructive_buttons_enabled=ha_config.destructive_buttons_enabled,
            ),
        )

    async def _collect_sensors(self, redis_client: Any | None) -> HASensorsData:
        """Collect system sensor values."""
        cpu = psutil.cpu_percent(interval=0.1)
        vm = psutil.virtual_memory()
        disk_path = "/" if platform.system() != "Windows" else "C:\\"
        disk = psutil.disk_usage(disk_path)
        net = psutil.net_io_counters()
        boot_time = psutil.boot_time()
        uptime_sec = int(time.time() - boot_time)

        # Temperature — optional, platform-dependent
        temperature: float | None = None
        try:
            temps = psutil.sensors_temperatures()  # type: ignore[attr-defined]
            if temps:
                first_sensor_list = next(iter(temps.values()))
                if first_sensor_list:
                    temperature = first_sensor_list[0].current
        except (AttributeError, NotImplementedError):
            pass

        # Docker containers running
        docker_running = 0
        try:
            result = subprocess.run(  # noqa: S603
                ["docker", "ps", "-q"],  # noqa: S607
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            if result.returncode == 0:
                docker_running = len(
                    [ln for ln in result.stdout.strip().splitlines() if ln]
                )
        except Exception:  # noqa: BLE001
            docker_running = 0

        # Redis connected clients
        redis_clients: int | None = None
        if redis_client is not None:
            try:
                info = await redis_client.info("clients")
                redis_clients = int(info.get("connected_clients", 0))
            except Exception:  # noqa: BLE001
                pass

        # MongoDB connected clients
        mongo_clients: int | None = None
        try:
            status = await self._db.db.command("serverStatus")
            mongo_clients = status.get("connections", {}).get("current")
        except Exception:  # noqa: BLE001
            pass

        return HASensorsData(
            cpu_usage=round(cpu, 1),
            ram_usage=round(vm.percent, 1),
            disk_usage=round(disk.percent, 1),
            uptime_seconds=uptime_sec,
            network_rx_bytes=net.bytes_recv,
            network_tx_bytes=net.bytes_sent,
            temperature=round(temperature, 1) if temperature is not None else None,
            docker_containers_running=docker_running,
            running_processes=len(psutil.pids()),
            redis_connected_clients=redis_clients,
            mongo_connected_clients=mongo_clients,
        )

    async def _collect_binary_sensors(
        self, redis_client: Any | None
    ) -> HABinarySensorsData:
        """Collect binary sensor states."""
        # MongoDB online
        mongodb_online = False
        try:
            await self._db.db.command("ping")
            mongodb_online = True
        except Exception:  # noqa: BLE001
            pass

        # Redis online
        redis_online = False
        if redis_client is not None:
            try:
                await redis_client.ping()
                redis_online = True
            except Exception:  # noqa: BLE001
                pass

        return HABinarySensorsData(
            mongodb_online=mongodb_online,
            redis_online=redis_online,
            # These placeholders are overridden in get_full_state
            api_healthy=True,
            mcp_healthy=True,
            python_mirror_running=True,
            pypi_mirror_running=True,
        )

    # ── Diagnostics (on-demand) ────────────────────────────────────────────────

    async def get_diagnostics(self) -> HADiagnosticsResponse:
        """Collect device diagnostics. Called only on user request, not during polling."""
        serial = await self.get_or_create_instance_serial()

        vm = psutil.virtual_memory()
        disk_path = "/" if platform.system() != "Windows" else "C:\\"
        disk = psutil.disk_usage(disk_path)
        uptime_sec = int(time.time() - psutil.boot_time())

        os_name = platform.system()
        os_version: str
        try:
            if os_name == "Linux":
                try:
                    import distro  # noqa: PLC0415

                    os_version = distro.name(pretty=True) or platform.release()
                except ImportError:
                    os_version = platform.release()
            else:
                os_version = platform.release()
        except Exception:  # noqa: BLE001
            os_version = "Unknown"

        return HADiagnosticsResponse(
            firmware=self._full_settings.app.version,
            software_version=f"ASFES Multiplex {self._full_settings.app.version}",
            serial=serial,
            os=os_name,
            os_version=os_version,
            python_version=sys.version.split()[0],
            hostname=socket.gethostname(),
            uptime_seconds=uptime_sec,
            cpu_architecture=platform.machine(),
            ram_total_gb=round(vm.total / (1024**3), 2),
            disk_total_gb=round(disk.total / (1024**3), 2),
        )

    # ── Actions ────────────────────────────────────────────────────────────────

    async def set_switch(
        self,
        name: str,
        value: bool,
        *,
        settings_service: Any,
        actor: Any,
    ) -> None:
        """Apply a switch change via the runtime settings service."""
        if name == "enable_registration":
            await settings_service.set_registration(value, actor=actor)
        elif name == "enable_mcp":
            await settings_service.set_mcp(value, actor=actor)
        elif name == "enable_redis":
            await settings_service.set_redis_runtime(value, actor=actor)
        else:
            raise ValueError(f"Unknown switch: {name}")

    async def press_button(
        self,
        name: str,
        *,
        plugin_manager: Any | None = None,
        python_mirror_service: Any | None = None,
        pypi_service: Any | None = None,
    ) -> dict[str, Any]:
        """Execute a button action. Returns a result dict."""
        if name == "restart_multiplex":
            subprocess.Popen(["systemctl", "restart", "asfes-multiplex"])  # noqa: S603, S607
            return {"success": True, "message": "Restart initiated"}

        if name == "restart_docker":
            subprocess.Popen(["systemctl", "restart", "docker"])  # noqa: S603, S607
            return {"success": True, "message": "Docker restart initiated"}

        if name == "reload_plugins":
            if plugin_manager is not None and hasattr(plugin_manager, "load_plugins"):
                await plugin_manager.load_plugins()
            return {"success": True, "message": "Plugins reloaded"}

        if name == "refresh_python_mirror":
            if python_mirror_service is not None and hasattr(
                python_mirror_service, "refresh_cache"
            ):
                asyncio.create_task(  # noqa: RUF006
                    python_mirror_service.refresh_cache()
                )
            return {"success": True, "message": "Python mirror refresh started"}

        if name == "refresh_pypi":
            if pypi_service is not None and hasattr(pypi_service, "refresh_cache"):
                asyncio.create_task(pypi_service.refresh_cache())  # noqa: RUF006
            return {"success": True, "message": "PyPI cache refresh started"}

        raise ValueError(f"Unknown button: {name}")
