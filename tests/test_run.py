from __future__ import annotations

import warnings

import uvicorn

import run


def test_get_ws_protocol_defaults_to_websockets_sansio(monkeypatch) -> None:
    monkeypatch.delenv("UVICORN_WS", raising=False)

    assert run.get_ws_protocol() == "websockets-sansio"


def test_get_ws_protocol_respects_override(monkeypatch) -> None:
    monkeypatch.setenv("UVICORN_WS", "wsproto")

    assert run.get_ws_protocol() == "wsproto"


def test_uvicorn_config_load_avoids_websockets_deprecation_warnings(monkeypatch) -> None:
    monkeypatch.delenv("UVICORN_WS", raising=False)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        config = uvicorn.Config("server:app", ws=run.get_ws_protocol())
        config.load()

    messages = [str(item.message) for item in caught if issubclass(item.category, DeprecationWarning)]
    assert not any("websockets.legacy is deprecated" in message for message in messages)
    assert not any("WebSocketServerProtocol is deprecated" in message for message in messages)
