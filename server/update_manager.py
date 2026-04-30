from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator
from uuid import uuid4

from server.core.config import BASE_DIR, Settings
from server.core.security import now_utc
from server.host_ops import CommandResult


INSTALL_DIR = Path("/opt/asfes-multiplex")
DATA_DIR = Path("/var/lib/asfes-multiplex")
LOG_DIR = Path("/var/log/asfes-multiplex")
SERVICE_NAME = "asfes-multiplex"
UPDATE_STAGES = ("code", "python", "frontend", "restart")
STAGE_TITLES = {
    "code": "Код приложения",
    "python": "Python-зависимости",
    "frontend": "Frontend-зависимости и сборка",
    "restart": "Перезапуск сервиса",
}


@dataclass(slots=True)
class UpdateStageState:
    key: str
    title: str
    status: str = "pending"
    needed: bool = False
    forced: bool = False
    detail: str | None = None
    returncode: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "title": self.title,
            "status": self.status,
            "needed": self.needed,
            "forced": self.forced,
            "detail": self.detail,
            "returncode": self.returncode,
        }


@dataclass(slots=True)
class UpdateSession:
    session_id: str
    kind: str
    status: str = "queued"
    stages: dict[str, UpdateStageState] = field(default_factory=dict)
    result: CommandResult | None = None
    error: str | None = None
    requires_restart: bool = False
    created_at: Any = field(default_factory=now_utc)
    updated_at: Any = field(default_factory=now_utc)
    logs: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=800))
    events: list[dict[str, Any]] = field(default_factory=list)
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    task: asyncio.Task[Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "kind": self.kind,
            "status": self.status,
            "stages": [self.stages[key].to_dict() for key in UPDATE_STAGES if key in self.stages],
            "result": self.result.to_dict() if self.result else None,
            "error": self.error,
            "requires_restart": self.requires_restart,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class UpdateManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.sessions: dict[str, UpdateSession] = {}
        self._lock = asyncio.Lock()

    async def start_check(self) -> UpdateSession:
        return await self._start_session("check", self._run_check)

    async def start_update(self, stages: list[str], force_stages: list[str]) -> UpdateSession:
        normalized_stages = self._normalize_stages(stages or ["code", "python", "frontend", "restart"])
        normalized_force = self._normalize_stages(force_stages)
        return await self._start_session("update", self._run_update, normalized_stages, normalized_force, block_active=True)

    async def start_restart(self) -> UpdateSession:
        return await self._start_session("restart", self._run_update, ["restart"], ["restart"], block_active=True)

    def get_session(self, session_id: str) -> UpdateSession | None:
        return self.sessions.get(session_id)

    async def events(self, session: UpdateSession) -> AsyncIterator[dict[str, Any]]:
        index = 0
        while True:
            async with session.condition:
                while index >= len(session.events) and session.status not in {"success", "error"}:
                    await session.condition.wait()
                pending = session.events[index:]
                index = len(session.events)
                finished = session.status in {"success", "error"} and not pending
            for event in pending:
                yield event
            if finished:
                return

    async def _start_session(self, kind: str, runner, *args: Any, block_active: bool = False) -> UpdateSession:
        async with self._lock:
            if block_active and self._active_session() is not None:
                raise RuntimeError("Update session is already running")
            session = self._new_session(kind)
            self.sessions[session.session_id] = session
            self._trim_sessions()
            session.task = asyncio.create_task(runner(session, *args))
            return session

    def _active_session(self) -> UpdateSession | None:
        for session in self.sessions.values():
            if session.status in {"queued", "running"}:
                return session
        return None

    def _new_session(self, kind: str) -> UpdateSession:
        stages = {
            key: UpdateStageState(key=key, title=STAGE_TITLES[key])
            for key in UPDATE_STAGES
        }
        return UpdateSession(session_id=uuid4().hex, kind=kind, stages=stages)

    def _trim_sessions(self) -> None:
        if len(self.sessions) <= 20:
            return
        ordered = sorted(self.sessions.values(), key=lambda item: item.created_at)
        for session in ordered[:-20]:
            if session.status not in {"queued", "running"}:
                self.sessions.pop(session.session_id, None)

    def _normalize_stages(self, stages: list[str]) -> list[str]:
        normalized: list[str] = []
        for stage in stages:
            if stage not in UPDATE_STAGES:
                raise ValueError(f"Unknown update stage: {stage}")
            if stage not in normalized:
                normalized.append(stage)
        return normalized

    async def _run_check(self, session: UpdateSession) -> None:
        await self._set_status(session, "running")
        try:
            code_needed = await self._check_code(session)
            python_needed = await self._check_python(session)
            frontend_needed = await self._check_frontend(session)
            session.stages["code"].needed = code_needed
            session.stages["python"].needed = python_needed
            session.stages["frontend"].needed = frontend_needed
            session.stages["restart"].needed = any([code_needed, python_needed, frontend_needed])
            session.requires_restart = session.stages["restart"].needed
            await self._set_status(session, "success")
        except Exception as exc:
            session.error = str(exc)
            await self._emit(session, "error", {"message": str(exc)})
            await self._set_status(session, "error")

    async def _run_update(self, session: UpdateSession, stages: list[str], force_stages: list[str]) -> None:
        started = time.monotonic()
        stdout_parts: list[str] = []
        stderr_parts: list[str] = []
        command_names: list[str] = []
        await self._set_status(session, "running")
        try:
            for stage in stages:
                state = session.stages[stage]
                state.forced = stage in force_stages
                state.needed = True
                await self._run_stage(session, stage, stdout_parts, stderr_parts, command_names)
            session.requires_restart = "restart" in stages
            session.result = CommandResult(
                command=command_names or [session.kind],
                returncode=0,
                stdout="".join(stdout_parts),
                stderr="".join(stderr_parts),
                truncated=False,
                duration_ms=int((time.monotonic() - started) * 1000),
            )
            await self._emit(session, "result", {"result": session.result.to_dict(), "requires_restart": session.requires_restart})
            await self._set_status(session, "success")
        except Exception as exc:
            session.error = str(exc)
            session.result = CommandResult(
                command=command_names or [session.kind],
                returncode=1,
                stdout="".join(stdout_parts),
                stderr=("".join(stderr_parts) + str(exc)),
                truncated=False,
                duration_ms=int((time.monotonic() - started) * 1000),
            )
            await self._emit(session, "error", {"message": str(exc), "result": session.result.to_dict()})
            await self._set_status(session, "error")

    async def _run_stage(
        self,
        session: UpdateSession,
        stage: str,
        stdout_parts: list[str],
        stderr_parts: list[str],
        command_names: list[str],
    ) -> None:
        state = session.stages[stage]
        state.status = "running"
        await self._emit(session, "stage", {"stage": state.to_dict()})
        if stage == "code":
            commands = self._code_commands()
        elif stage == "python":
            commands = self._python_commands()
        elif stage == "frontend":
            commands = self._frontend_commands()
        else:
            commands = [self._restart_command()]
        for command, cwd, timeout in commands:
            command_names.append(" ".join(command))
            result = await self._run_command(session, command, cwd=cwd, timeout_seconds=timeout)
            stdout_parts.append(result.stdout)
            stderr_parts.append(result.stderr)
            state.returncode = result.returncode
            if result.returncode != 0:
                state.status = "error"
                state.detail = result.stderr.strip() or result.stdout.strip() or f"Command exited with code {result.returncode}"
                await self._emit(session, "stage", {"stage": state.to_dict()})
                raise RuntimeError(state.detail)
        state.status = "success"
        await self._emit(session, "stage", {"stage": state.to_dict()})

    async def _check_code(self, session: UpdateSession) -> bool:
        state = session.stages["code"]
        state.status = "running"
        await self._emit(session, "stage", {"stage": state.to_dict()})
        if not (BASE_DIR / ".git").exists():
            state.status = "success"
            state.detail = "Git-репозиторий не найден"
            await self._emit(session, "stage", {"stage": state.to_dict()})
            return False
        await self._run_command(session, self._git_command("fetch", "--all", "--prune"), cwd=BASE_DIR, timeout_seconds=180)
        upstream = await self._run_command(session, self._git_command("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"), cwd=BASE_DIR, timeout_seconds=30, success_codes={0, 128})
        if upstream.returncode != 0:
            state.status = "success"
            state.detail = "Upstream-ветка не настроена"
            await self._emit(session, "stage", {"stage": state.to_dict()})
            return False
        diff = await self._run_command(session, self._git_command("rev-list", "--count", "HEAD..@{u}"), cwd=BASE_DIR, timeout_seconds=30)
        needed = int((diff.stdout.strip() or "0").splitlines()[-1]) > 0
        state.status = "success"
        state.detail = "Доступны изменения кода" if needed else "Код актуален"
        await self._emit(session, "stage", {"stage": state.to_dict()})
        return needed

    async def _check_python(self, session: UpdateSession) -> bool:
        state = session.stages["python"]
        state.status = "running"
        await self._emit(session, "stage", {"stage": state.to_dict()})
        result = await self._run_command(session, [sys.executable, "-m", "pip", "list", "--outdated", "--format=json"], cwd=BASE_DIR, timeout_seconds=120, success_codes={0})
        try:
            packages = json.loads(result.stdout or "[]")
        except json.JSONDecodeError:
            packages = []
        needed = bool(packages)
        state.status = "success"
        state.detail = f"Устаревших пакетов: {len(packages)}" if needed else "Python-зависимости актуальны"
        await self._emit(session, "stage", {"stage": state.to_dict()})
        return needed

    async def _check_frontend(self, session: UpdateSession) -> bool:
        state = session.stages["frontend"]
        state.status = "running"
        await self._emit(session, "stage", {"stage": state.to_dict()})
        npm = self._executable("npm")
        if not npm:
            state.status = "success"
            state.detail = "npm не найден"
            await self._emit(session, "stage", {"stage": state.to_dict()})
            return False
        result = await self._run_command(session, [npm, "outdated", "--json"], cwd=BASE_DIR / "frontend", timeout_seconds=120, success_codes={0, 1})
        try:
            packages = json.loads(result.stdout or "{}")
        except json.JSONDecodeError:
            packages = {}
        changed_frontend = await self._frontend_changed()
        needed = bool(packages) or changed_frontend
        state.status = "success"
        if packages:
            state.detail = f"Устаревших npm-пакетов: {len(packages)}"
        elif changed_frontend:
            state.detail = "Есть изменения frontend-кода для сборки"
        else:
            state.detail = "Frontend актуален"
        await self._emit(session, "stage", {"stage": state.to_dict()})
        return needed

    async def _frontend_changed(self) -> bool:
        if not (BASE_DIR / ".git").exists():
            return False
        result = await self._run_command_silent(self._git_command("diff", "--name-only", "HEAD..@{u}"), cwd=BASE_DIR, timeout_seconds=30)
        if result.returncode != 0:
            return False
        return any(line.startswith("frontend/") or line == "package-lock.json" for line in result.stdout.splitlines())

    def _code_commands(self) -> list[tuple[list[str], Path, int]]:
        commands: list[tuple[list[str], Path, int]] = []
        if (BASE_DIR / ".git").exists():
            commands.extend([
                (self._git_command("fetch", "--all", "--prune"), BASE_DIR, 180),
                (self._git_command("pull", "--ff-only"), BASE_DIR, 180),
            ])
        if os.name != "nt":
            commands.append((self._bash_command(self._rsync_script()), BASE_DIR, 300))
        return commands

    def _python_commands(self) -> list[tuple[list[str], Path, int]]:
        python_bin = INSTALL_DIR / ".venv" / "bin" / "python" if os.name != "nt" else Path(sys.executable)
        if os.name != "nt":
            create_venv = f"test -x '{python_bin}' || python3 -m venv '{INSTALL_DIR / '.venv'}'"
            return [
                (self._bash_command(create_venv), BASE_DIR, 120),
                ([str(python_bin), "-m", "pip", "install", "--upgrade", "pip"], BASE_DIR, 300),
                ([str(python_bin), "-m", "pip", "install", "-r", str(INSTALL_DIR / "requirements.txt")], BASE_DIR, 600),
            ]
        return [
            ([str(python_bin), "-m", "pip", "install", "--upgrade", "pip"], BASE_DIR, 300),
            ([str(python_bin), "-m", "pip", "install", "-r", str(BASE_DIR / "requirements.txt")], BASE_DIR, 600),
        ]

    def _frontend_commands(self) -> list[tuple[list[str], Path, int]]:
        npm = self._executable("npm") or "npm"
        prefix = INSTALL_DIR / "frontend" if os.name != "nt" else BASE_DIR / "frontend"
        install = "ci" if (prefix / "package-lock.json").exists() else "install"
        return [
            ([npm, "--prefix", str(prefix), install], BASE_DIR, 600),
            ([npm, "--prefix", str(prefix), "run", "build"], BASE_DIR, 600),
        ]

    def _restart_command(self) -> tuple[list[str], Path, int]:
        if os.name == "nt":
            return ([sys.executable, "-c", "print('restart is unavailable on Windows')"], BASE_DIR, 30)
        return (self._bash_command(f"nohup bash -c \"sleep 1; systemctl restart '{SERVICE_NAME}'\" >/dev/null 2>&1 &"), BASE_DIR, 60)

    def _rsync_script(self) -> str:
        excludes = " ".join(
            f"--exclude '{item}'"
            for item in [
                ".venv",
                ".env",
                "frontend/node_modules",
                "frontend/dist",
                "runtime",
                "data",
                ".test_runtime",
                ".pytest_cache",
                "pytest-cache-files-*",
            ]
        )
        return f"mkdir -p '{INSTALL_DIR}' && rsync -a --delete {excludes} '{BASE_DIR}/' '{INSTALL_DIR}/' && chown -R '{SERVICE_NAME}:{SERVICE_NAME}' '{INSTALL_DIR}' '{DATA_DIR}' '{LOG_DIR}'"

    def _git_command(self, *args: str) -> list[str]:
        return ["git", "-c", f"safe.directory={BASE_DIR}", *args]

    def _bash_command(self, script: str) -> list[str]:
        if os.name != "nt" and hasattr(os, "geteuid") and os.geteuid() != 0:
            return ["sudo", "-n", "/bin/bash", "-lc", script]
        return ["/bin/bash", "-lc", script]

    def _executable(self, alias: str) -> str | None:
        return shutil.which(alias)

    async def _run_command(
        self,
        session: UpdateSession,
        command: list[str],
        *,
        cwd: Path,
        timeout_seconds: int,
        success_codes: set[int] | None = None,
    ) -> CommandResult:
        success_codes = success_codes or {0}
        started = time.monotonic()
        stdout_parts: list[str] = []
        stderr_parts: list[str] = []
        await self._emit(session, "log", {"stream": "system", "line": f"$ {' '.join(command)}"})
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(cwd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(f"Executable not found: {command[0]}") from exc
        assert process.stdout is not None
        assert process.stderr is not None

        async def read_stream(stream: asyncio.StreamReader, name: str, target: list[str]) -> None:
            while True:
                line = await stream.readline()
                if not line:
                    return
                text = line.decode("utf-8", errors="replace")
                target.append(text)
                await self._emit(session, "log", {"stream": name, "line": text.rstrip("\n")})

        try:
            await asyncio.wait_for(asyncio.gather(read_stream(process.stdout, "stdout", stdout_parts), read_stream(process.stderr, "stderr", stderr_parts), process.wait()), timeout=timeout_seconds)
        except asyncio.TimeoutError as exc:
            process.kill()
            await process.wait()
            raise RuntimeError(f"Command timed out after {timeout_seconds} seconds: {' '.join(command)}") from exc
        returncode = process.returncode if process.returncode is not None else 1
        result = CommandResult(
            command=command,
            returncode=returncode,
            stdout="".join(stdout_parts),
            stderr="".join(stderr_parts),
            truncated=False,
            duration_ms=int((time.monotonic() - started) * 1000),
        )
        if returncode not in success_codes:
            detail = result.stderr.strip() or result.stdout.strip() or f"Command exited with code {returncode}"
            raise RuntimeError(detail)
        return result

    async def _run_command_silent(self, command: list[str], *, cwd: Path, timeout_seconds: int) -> CommandResult:
        started = time.monotonic()
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(cwd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
        except Exception as exc:
            return CommandResult(command=command, returncode=1, stdout="", stderr=str(exc), duration_ms=int((time.monotonic() - started) * 1000))
        return CommandResult(
            command=command,
            returncode=process.returncode or 0,
            stdout=stdout.decode("utf-8", errors="replace"),
            stderr=stderr.decode("utf-8", errors="replace"),
            duration_ms=int((time.monotonic() - started) * 1000),
        )

    async def _set_status(self, session: UpdateSession, status: str) -> None:
        session.status = status
        session.updated_at = now_utc()
        await self._emit(session, "status", {"status": status, "session": session.to_dict()})

    async def _emit(self, session: UpdateSession, event_type: str, payload: dict[str, Any]) -> None:
        session.updated_at = now_utc()
        event = {"type": event_type, **payload}
        if event_type == "log":
            session.logs.append(event)
        async with session.condition:
            session.events.append(event)
            session.condition.notify_all()
