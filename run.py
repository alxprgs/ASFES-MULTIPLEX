from __future__ import annotations

import os
import signal
import asyncio
import uvicorn
from server.core.config import settings


def env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on"}


def get_host() -> str:
    return os.getenv("HOST", "0.0.0.0")


def get_port() -> int:
    raw = os.getenv("PORT")
    if raw is None:
        return int(getattr(settings, "PORT", 8000))
    try:
        return int(raw)
    except ValueError:
        print(f"[WARN] Invalid PORT={raw!r}. Fallback to settings.PORT")
        return int(getattr(settings, "PORT", 8000))


def get_log_level() -> str:
    allowed = {"critical", "error", "warning", "info", "debug", "trace"}
    lvl = os.getenv("LOG_LEVEL", "info").strip().lower()
    return lvl if lvl in allowed else "info"


def is_unix() -> bool:
    return os.name != "nt"


def enable_uvloop_if_possible(config: uvicorn.Config) -> None:
    if not is_unix():
        return
    try:
        import uvloop  # type: ignore # noqa: F401
        config.loop = "uvloop"
    except Exception:
        pass


def install_graceful_shutdown(server: uvicorn.Server) -> None:
    def request_shutdown() -> None:
        if not server.should_exit:
            print("\n[INFO] Graceful shutdown requested")
        server.should_exit = True

    try:
        loop = asyncio.get_running_loop()
        if is_unix():
            loop.add_signal_handler(signal.SIGTERM, request_shutdown)
            loop.add_signal_handler(signal.SIGINT, request_shutdown)
        else:
            signal.signal(signal.SIGTERM, lambda *_: request_shutdown())
            signal.signal(signal.SIGINT, lambda *_: request_shutdown())
    except Exception:
        signal.signal(signal.SIGINT, lambda *_: request_shutdown())


async def run_uvicorn() -> None:
    reload_enabled = bool(getattr(settings, "DEV", False)) and env_bool("RELOAD", False)

    app_path = os.getenv("APP", "server:app")

    config = uvicorn.Config(
        app_path,
        host=get_host(),
        port=get_port(),
        log_level=get_log_level(),
        reload=reload_enabled,
        proxy_headers=True,
        forwarded_allow_ips=os.getenv("FORWARDED_ALLOW_IPS", "127.0.0.1"),
        lifespan="on",

        timeout_keep_alive=int(os.getenv("KEEPALIVE", "5")),
        limit_concurrency=int(os.getenv("LIMIT_CONCURRENCY", "0")) or None,
    )

    enable_uvloop_if_possible(config)

    server = uvicorn.Server(config)
    install_graceful_shutdown(server)
    await server.serve()


def main() -> None:
    try:
        asyncio.run(run_uvicorn())
    except KeyboardInterrupt:
        print("\n[INFO] Server stopped by user.")
        raise SystemExit(0)
    except SystemExit:
        raise
    except Exception as e:
        print(f"[ERROR] Startup error: {type(e).__name__}: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()