from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import stat
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from server.core.config import HostOpsConfig, Settings
from server.models import RuntimeAvailability

try:
    import psutil as _psutil  # type: ignore
except ImportError:  # pragma: no cover - проверяется в тестах через monkeypatch
    _psutil = None


DEFAULT_SENSITIVE_FIELD_NAMES = {
    "body",
    "certificate",
    "cert",
    "content",
    "contents",
    "data",
    "dump",
    "file_content",
    "password",
    "passphrase",
    "private_key",
    "secret",
    "smtp_password",
    "token",
}


@dataclass(slots=True)
class CommandResult:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str
    truncated: bool = False
    duration_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": self.command,
            "returncode": self.returncode,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "truncated": self.truncated,
            "duration_ms": self.duration_ms,
        }


class HostOpsError(RuntimeError):
    pass


class HostOpsService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.config = settings.host_ops
        self.platform_name = "windows" if os.name == "nt" else "linux"

    @property
    def is_windows(self) -> bool:
        return self.platform_name == "windows"

    @property
    def is_linux(self) -> bool:
        return self.platform_name == "linux"

    def psutil_available(self) -> bool:
        return _psutil is not None

    def executable_path(self, alias: str) -> str | None:
        override = self.config.executable_overrides.get(alias)
        if override:
            return override
        return shutil.which(alias)

    def command_exists(self, alias: str) -> bool:
        return self.executable_path(alias) is not None

    def provider_override(self, provider_group: str) -> str | None:
        return self.config.provider_overrides.get(provider_group)

    def availability_for_os(self, supported_os: list[str]) -> RuntimeAvailability:
        if self.platform_name in supported_os:
            return RuntimeAvailability(available=True)
        return RuntimeAvailability(
            available=False,
            reason=f"Unsupported on {self.platform_name}. Supported OS: {', '.join(sorted(supported_os))}",
        )

    def availability_for_command(
        self,
        alias: str,
        *,
        reason: str | None = None,
        providers: list[str] | None = None,
    ) -> RuntimeAvailability:
        executable = self.executable_path(alias)
        if executable:
            return RuntimeAvailability(available=True, required_backends=[alias], providers=providers or [])
        return RuntimeAvailability(
            available=False,
            reason=reason or f"Required executable '{alias}' is not available in PATH",
            required_backends=[alias],
            providers=providers or [],
        )

    def availability_for_any_command(
        self,
        aliases: list[str],
        *,
        reason: str | None = None,
        providers: list[str] | None = None,
    ) -> RuntimeAvailability:
        for alias in aliases:
            if self.command_exists(alias):
                return RuntimeAvailability(available=True, required_backends=aliases, providers=providers or [])
        joined = ", ".join(aliases)
        return RuntimeAvailability(
            available=False,
            reason=reason or f"None of the required executables are available: {joined}",
            required_backends=aliases,
            providers=providers or [],
        )

    def availability_for_psutil(self) -> RuntimeAvailability:
        if self.psutil_available():
            return RuntimeAvailability(available=True, required_backends=["psutil"])
        return RuntimeAvailability(available=False, reason="Python package 'psutil' is not installed", required_backends=["psutil"])

    async def run(
        self,
        command: list[str],
        *,
        check: bool = False,
        timeout_seconds: int | None = None,
        input_text: str | None = None,
        env: dict[str, str] | None = None,
        cwd: Path | None = None,
    ) -> CommandResult:
        if not command:
            raise HostOpsError("Command cannot be empty")
        started = time.monotonic()
        timeout = timeout_seconds or self.config.command_timeout_seconds

        def _run_sync() -> subprocess.CompletedProcess[bytes]:
            return subprocess.run(
                command,
                cwd=str(cwd) if cwd else None,
                env={**os.environ, **(env or {})},
                input=input_text.encode("utf-8") if input_text is not None else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False,
            )

        try:
            completed = await asyncio.to_thread(_run_sync)
        except subprocess.TimeoutExpired as exc:
            raise HostOpsError(f"Command timed out after {timeout} seconds: {' '.join(command)}") from exc

        stdout, stdout_truncated = self._decode_and_truncate(completed.stdout)
        stderr, stderr_truncated = self._decode_and_truncate(completed.stderr)
        result = CommandResult(
            command=command,
            returncode=completed.returncode,
            stdout=stdout,
            stderr=stderr,
            truncated=stdout_truncated or stderr_truncated,
            duration_ms=int((time.monotonic() - started) * 1000),
        )
        if check and result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or f"Command exited with code {result.returncode}"
            raise HostOpsError(detail)
        return result

    async def run_backend(self, alias: str, *args: str, check: bool = False, **kwargs: Any) -> CommandResult:
        executable = self.executable_path(alias)
        if not executable:
            raise HostOpsError(f"Executable '{alias}' is not available in PATH")
        return await self.run([executable, *args], check=check, **kwargs)

    def _decode_and_truncate(self, payload: bytes) -> tuple[str, bool]:
        max_bytes = max(256, int(self.config.max_output_bytes))
        truncated = len(payload) > max_bytes
        if truncated:
            payload = payload[:max_bytes]
        text = payload.decode("utf-8", errors="replace")
        if truncated:
            text += "\n...[truncated]"
        return text, truncated

    def managed_file_roots(self) -> list[Path]:
        return [self._normalize_root(path) for path in self.config.managed_file_roots]

    def managed_log_roots(self) -> list[Path]:
        return [self._normalize_root(path) for path in self.config.managed_log_roots]

    def backup_directory(self) -> Path:
        return self._normalize_root(self.config.backup_directory)

    def profile_directory(self, profile_type: str) -> Path:
        mapping = {
            "database": self.config.database_profiles_directory,
            "vpn": self.config.vpn_profiles_directory,
            "ssl": self.config.ssl_profiles_directory,
        }
        return self._normalize_root(mapping[profile_type])

    def configured_nginx_paths(self) -> list[Path]:
        return [self._normalize_root(path) for path in self.config.nginx_config_paths]

    def resolve_managed_path(self, raw_path: str, *, roots: list[Path] | None = None) -> Path:
        candidate = Path(raw_path)
        allowed_roots = [self._normalize_root(root) for root in (roots or self.managed_file_roots())]
        if not allowed_roots:
            raise HostOpsError("No managed roots are configured")

        if candidate.is_absolute():
            resolved = candidate.resolve(strict=False)
            if not self._is_within_any_root(resolved, allowed_roots):
                raise HostOpsError("Path is outside of managed roots")
            return resolved

        for root in allowed_roots:
            existing = (root / candidate).resolve(strict=False)
            if existing.exists():
                if not self._is_within_any_root(existing, allowed_roots):
                    raise HostOpsError("Resolved path escapes managed roots")
                return existing
        resolved = (allowed_roots[0] / candidate).resolve(strict=False)
        if not self._is_within_any_root(resolved, allowed_roots):
            raise HostOpsError("Resolved path escapes managed roots")
        return resolved

    def list_directory(self, raw_path: str = ".", *, roots: list[Path] | None = None) -> dict[str, Any]:
        path = self.resolve_managed_path(raw_path, roots=roots)
        if not path.exists():
            raise HostOpsError("Directory does not exist")
        if not path.is_dir():
            raise HostOpsError("Target path is not a directory")
        entries = []
        for item in sorted(path.iterdir(), key=lambda entry: entry.name.lower()):
            try:
                stat = item.stat()
            except OSError:
                continue
            entries.append(
                {
                    "name": item.name,
                    "path": str(item),
                    "is_dir": item.is_dir(),
                    "size": stat.st_size,
                    "modified_at": int(stat.st_mtime),
                }
            )
        return {"path": str(path), "entries": entries, "count": len(entries)}

    def read_text(
        self,
        raw_path: str,
        *,
        roots: list[Path] | None = None,
        offset: int = 0,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        path = self.resolve_managed_path(raw_path, roots=roots)
        if not path.exists():
            raise HostOpsError("File does not exist")
        if path.is_dir():
            raise HostOpsError("Target path is a directory")
        limit = max_bytes or self.config.max_output_bytes
        with path.open("rb") as handle:
            handle.seek(max(0, offset))
            payload = handle.read(limit + 1)
        truncated = len(payload) > limit
        if truncated:
            payload = payload[:limit]
        return {
            "path": str(path),
            "offset": max(0, offset),
            "content": payload.decode("utf-8", errors="replace"),
            "truncated": truncated,
        }

    def tail_text(self, raw_path: str, *, roots: list[Path] | None = None, tail_lines: int = 100) -> dict[str, Any]:
        path = self.resolve_managed_path(raw_path, roots=roots)
        if not path.exists():
            raise HostOpsError("File does not exist")
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        window = max(1, tail_lines)
        tail = lines[-window:]
        payload = "\n".join(tail)
        truncated = len(lines) > len(tail)
        if len(payload.encode("utf-8")) > self.config.max_output_bytes:
            encoded = payload.encode("utf-8")[: self.config.max_output_bytes]
            payload = encoded.decode("utf-8", errors="replace") + "\n...[truncated]"
            truncated = True
        return {"path": str(path), "content": payload, "line_count": len(tail), "truncated": truncated}

    def mkdir(self, raw_path: str, *, roots: list[Path] | None = None) -> dict[str, Any]:
        path = self.resolve_managed_path(raw_path, roots=roots)
        path.mkdir(parents=True, exist_ok=True)
        return {"path": str(path), "created": True}

    def delete_path(self, raw_path: str, *, roots: list[Path] | None = None, recursive: bool = False) -> dict[str, Any]:
        path = self.resolve_managed_path(raw_path, roots=roots)
        self._ensure_safe_write_path(path, roots or self.managed_file_roots())
        if not path.exists():
            raise HostOpsError("Path does not exist")
        if path.is_dir():
            if not recursive:
                raise HostOpsError("Refusing to delete a directory without recursive=true")
            shutil.rmtree(path)
        else:
            path.unlink()
        return {"path": str(path), "deleted": True}

    def move_path(self, source: str, destination: str, *, roots: list[Path] | None = None) -> dict[str, Any]:
        source_path = self.resolve_managed_path(source, roots=roots)
        destination_path = self.resolve_managed_path(destination, roots=roots)
        write_roots = roots or self.managed_file_roots()
        self._ensure_safe_write_path(source_path, write_roots)
        self._ensure_safe_write_path(destination_path, write_roots)
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source_path), str(destination_path))
        return {"source": str(source_path), "destination": str(destination_path), "moved": True}

    def atomic_write_text(
        self,
        raw_path: str,
        content: str,
        *,
        roots: list[Path] | None = None,
        backup_existing: bool = True,
        append: bool = False,
    ) -> dict[str, Any]:
        path = self.resolve_managed_path(raw_path, roots=roots)
        self._ensure_safe_write_path(path, roots or self.managed_file_roots())
        path.parent.mkdir(parents=True, exist_ok=True)
        if append:
            created_backup = False
            if path.exists():
                self._backup_file(path)
                created_backup = True
            with path.open("a", encoding="utf-8") as handle:
                handle.write(content)
            return {
                "path": str(path),
                "written": len(content.encode("utf-8")),
                "appended": True,
                "backup_created": created_backup,
            }

        created_backup = False
        if path.exists() and backup_existing:
            self._backup_file(path)
            created_backup = True
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(path.parent)) as handle:
            handle.write(content)
            temp_name = handle.name
        Path(temp_name).replace(path)
        return {"path": str(path), "written": len(content.encode("utf-8")), "backup_created": created_backup}

    def load_json_profile(self, profile_type: str, profile_name: str) -> dict[str, Any]:
        safe_name = Path(profile_name).name
        if safe_name != profile_name:
            raise HostOpsError("Profile name cannot contain path separators")
        path = self.profile_directory(profile_type) / f"{safe_name}.json"
        if not path.exists():
            raise HostOpsError(f"{profile_type.title()} profile '{profile_name}' does not exist")
        return json.loads(path.read_text(encoding="utf-8"))

    def list_profiles(self, profile_type: str) -> list[dict[str, Any]]:
        directory = self.profile_directory(profile_type)
        if not directory.exists():
            return []
        profiles = []
        for item in sorted(directory.glob("*.json")):
            try:
                data = json.loads(item.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            profiles.append({"name": item.stem, "path": str(item), "metadata": data.get("metadata", {})})
        return profiles

    def redact_arguments(
        self,
        arguments: dict[str, Any],
        *,
        sensitive_fields: list[str] | None = None,
        max_string_length: int = 512,
    ) -> dict[str, Any]:
        sensitive = {name.lower() for name in DEFAULT_SENSITIVE_FIELD_NAMES}
        sensitive.update(name.lower() for name in (sensitive_fields or []))
        return self._redact_value(arguments, sensitive, max_string_length, path=())

    def _redact_value(
        self,
        value: Any,
        sensitive_fields: set[str],
        max_string_length: int,
        *,
        path: tuple[str, ...],
    ) -> Any:
        current_name = path[-1].lower() if path else ""
        if current_name in sensitive_fields:
            return "[REDACTED]"
        if isinstance(value, dict):
            return {
                str(key): self._redact_value(item, sensitive_fields, max_string_length, path=(*path, str(key)))
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._redact_value(item, sensitive_fields, max_string_length, path=path) for item in value]
        if isinstance(value, str) and len(value) > max_string_length:
            return f"{value[:max_string_length]}...[truncated]"
        return value

    def _normalize_root(self, path: Path) -> Path:
        return path.resolve(strict=False)

    def _is_within_any_root(self, path: Path, roots: list[Path]) -> bool:
        return any(self._is_within_root(path, root) for root in roots)

    def _is_within_root(self, path: Path, root: Path) -> bool:
        try:
            path.relative_to(root)
            return True
        except ValueError:
            return False

    def _ensure_safe_write_path(self, path: Path, roots: list[Path]) -> None:
        normalized_roots = [self._normalize_root(root) for root in roots]
        resolved_parent = path.parent.resolve(strict=False)
        if not self._is_within_any_root(resolved_parent, normalized_roots):
            raise HostOpsError("Resolved parent escapes managed roots")
        for root in normalized_roots:
            if self._is_within_root(path.resolve(strict=False), root):
                relative = path.resolve(strict=False).relative_to(root)
                current = root
                for part in relative.parts:
                    current = current / part
                    if current.exists() and self._is_link_or_reparse_point(current):
                        raise HostOpsError("Refusing to write through a symlink or reparse point")
                return
        raise HostOpsError("Path is outside of managed roots")

    def _is_link_or_reparse_point(self, path: Path) -> bool:
        if path.is_symlink():
            return True
        if os.name == "nt":
            try:
                return bool(path.stat().st_file_attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
            except (AttributeError, OSError):
                return False
        return False

    def _backup_file(self, path: Path) -> None:
        if not path.exists() or not path.is_file():
            return
        backup_root = self.backup_directory()
        backup_root.mkdir(parents=True, exist_ok=True)
        timestamp = int(time.time())
        target = backup_root / f"{timestamp}-{path.name}.bak"
        shutil.copy2(path, target)


def host_ops_config_paths(config: HostOpsConfig) -> list[Path]:
    return [
        *config.managed_file_roots,
        *config.managed_log_roots,
        config.backup_directory,
        config.database_profiles_directory,
        config.vpn_profiles_directory,
        config.ssl_profiles_directory,
        *config.nginx_config_paths,
    ]
