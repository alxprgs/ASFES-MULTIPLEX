from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from server.core.config import Settings
from server.core.database import DatabaseManager


def test_database_manager_initialization() -> None:
    settings = Settings.model_validate({
        "app": {"name": "TestApp"},
        "mongo": {"uri": "mongodb://localhost:27017", "database": "test_db"},
        "smtp": {},
        "root": {"username": "root", "email": "root@example.com"},
        "security": {
            "api_jwt_secret": "secret",
            "oauth_jwt_secret": "secret",
            "password_pepper": "pepper",
        },
    })
    db_manager = DatabaseManager(settings)
    assert db_manager.settings == settings
    assert db_manager.client is None
    assert db_manager.db is None


@pytest.mark.asyncio
async def test_database_connect_disconnect() -> None:
    settings = Settings.model_validate({
        "app": {"name": "TestApp"},
        "mongo": {"uri": "mongodb://localhost:27017", "database": "test_db"},
        "smtp": {},
        "root": {"username": "root", "email": "root@example.com"},
        "security": {
            "api_jwt_secret": "secret",
            "oauth_jwt_secret": "secret",
            "password_pepper": "pepper",
        },
    })
    db_manager = DatabaseManager(settings)

    mock_client = MagicMock()
    mock_client.admin.command = AsyncMock(return_value={"ok": 1.0})
    mock_client.aclose = AsyncMock()

    with patch("server.core.database.AsyncMongoClient", return_value=mock_client):
        await db_manager.connect()
        assert db_manager.client == mock_client
        assert db_manager.db is not None

        await db_manager.disconnect()
        assert db_manager.client is None
        assert db_manager.db is None
        mock_client.aclose.assert_called_once()


def test_collection_raises_when_not_connected() -> None:
    settings = Settings.model_validate({
        "app": {"name": "TestApp"},
        "mongo": {"uri": "mongodb://localhost:27017", "database": "test_db"},
        "smtp": {},
        "root": {"username": "root", "email": "root@example.com"},
        "security": {
            "api_jwt_secret": "secret",
            "oauth_jwt_secret": "secret",
            "password_pepper": "pepper",
        },
    })
    db_manager = DatabaseManager(settings)
    with pytest.raises(RuntimeError) as exc:
        db_manager.collection("users")
    assert "Mongo database is not connected" in str(exc.value)


@pytest.mark.asyncio
async def test_ensure_indexes() -> None:
    settings = Settings.model_validate({
        "app": {"name": "TestApp"},
        "mongo": {"uri": "mongodb://localhost:27017", "database": "test_db"},
        "smtp": {},
        "root": {"username": "root", "email": "root@example.com"},
        "security": {
            "api_jwt_secret": "secret",
            "oauth_jwt_secret": "secret",
            "password_pepper": "pepper",
        },
    })
    db_manager = DatabaseManager(settings)

    mock_db = MagicMock()
    mock_collection = MagicMock()
    mock_collection.create_indexes = AsyncMock()
    mock_db.__getitem__.return_value = mock_collection
    db_manager.db = mock_db

    await db_manager.ensure_indexes()

    # The db dict access should be called for each table: users, settings, oauth_clients, oauth_codes,
    # refresh_tokens, passkeys, passkey_challenges, plugins, tool_policies, audit_events, alert_rules,
    # alert_events, api_keys
    assert mock_db.__getitem__.call_count >= 13
    assert mock_collection.create_indexes.call_count >= 13
