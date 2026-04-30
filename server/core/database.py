from __future__ import annotations

from typing import Any

from pymongo import ASCENDING, IndexModel
from pymongo.asynchronous.mongo_client import AsyncMongoClient

from server.core.config import Settings


USERS = "users"
SETTINGS = "settings"
OAUTH_CLIENTS = "oauth_clients"
OAUTH_CODES = "oauth_codes"
REFRESH_TOKENS = "refresh_tokens"
PLUGINS = "plugins"
TOOL_POLICIES = "tool_policies"
AUDIT_EVENTS = "audit_events"
ALERT_RULES = "alert_rules"
ALERT_EVENTS = "alert_events"
PASSKEYS = "passkeys"
PASSKEY_CHALLENGES = "passkey_challenges"


class DatabaseManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client: AsyncMongoClient | None = None
        self.db: Any | None = None

    async def connect(self) -> None:
        self.client = AsyncMongoClient(
            self.settings.mongo.uri,
            serverSelectionTimeoutMS=self.settings.mongo.server_selection_timeout_ms,
            connectTimeoutMS=self.settings.mongo.connect_timeout_ms,
            maxPoolSize=self.settings.mongo.max_pool_size,
            appname=self.settings.app.name,
        )
        self.db = self.client[self.settings.mongo.database]
        await self.client.admin.command("ping")

    async def disconnect(self) -> None:
        if self.client is None:
            return
        await self.client.aclose()
        self.client = None
        self.db = None

    def collection(self, name: str) -> Any:
        if self.db is None:
            raise RuntimeError("Mongo database is not connected")
        return self.db[name]

    async def ensure_indexes(self) -> None:
        users = self.collection(USERS)
        await users.create_indexes(
            [
                IndexModel([("username", ASCENDING)], unique=True),
                IndexModel([("email", ASCENDING)], unique=True, partialFilterExpression={"email": {"$type": "string"}}),
                IndexModel([("tg_id", ASCENDING)], unique=True, partialFilterExpression={"tg_id": {"$type": "string"}}),
                IndexModel([("vk_id", ASCENDING)], unique=True, partialFilterExpression={"vk_id": {"$type": "string"}}),
                IndexModel([("is_root", ASCENDING)]),
            ]
        )

        settings_collection = self.collection(SETTINGS)
        await settings_collection.create_indexes([IndexModel([("kind", ASCENDING)], unique=True)])

        oauth_clients = self.collection(OAUTH_CLIENTS)
        await oauth_clients.create_indexes(
            [
                IndexModel([("client_id", ASCENDING)], unique=True),
                IndexModel([("name", ASCENDING)]),
            ]
        )

        oauth_codes = self.collection(OAUTH_CODES)
        await oauth_codes.create_indexes(
            [
                IndexModel([("code", ASCENDING)], unique=True),
                IndexModel([("expires_at", ASCENDING)], expireAfterSeconds=0),
                IndexModel([("client_id", ASCENDING), ("user_id", ASCENDING)]),
            ]
        )

        refresh_tokens = self.collection(REFRESH_TOKENS)
        await refresh_tokens.create_indexes(
            [
                IndexModel([("token_hash", ASCENDING)], unique=True),
                IndexModel([("expires_at", ASCENDING)], expireAfterSeconds=0),
                IndexModel([("user_id", ASCENDING), ("purpose", ASCENDING)]),
            ]
        )

        passkeys = self.collection(PASSKEYS)
        await passkeys.create_indexes(
            [
                IndexModel([("credential_id", ASCENDING)], unique=True),
                IndexModel([("user_id", ASCENDING)]),
                IndexModel([("created_at", ASCENDING)]),
            ]
        )

        passkey_challenges = self.collection(PASSKEY_CHALLENGES)
        await passkey_challenges.create_indexes(
            [
                IndexModel([("expires_at", ASCENDING)], expireAfterSeconds=0),
                IndexModel([("challenge", ASCENDING)], unique=True),
                IndexModel([("user_id", ASCENDING), ("purpose", ASCENDING)]),
            ]
        )

        plugins = self.collection(PLUGINS)
        await plugins.create_indexes(
            [
                IndexModel([("key", ASCENDING)], unique=True),
                IndexModel([("enabled", ASCENDING)]),
            ]
        )

        tool_policies = self.collection(TOOL_POLICIES)
        await tool_policies.create_indexes(
            [
                IndexModel([("tool_key", ASCENDING), ("scope", ASCENDING), ("subject_id", ASCENDING)], unique=True),
                IndexModel([("tool_key", ASCENDING), ("enabled", ASCENDING)]),
            ]
        )

        audit_events = self.collection(AUDIT_EVENTS)
        await audit_events.create_indexes(
            [
                IndexModel([("created_at", ASCENDING)]),
                IndexModel([("actor_user_id", ASCENDING), ("created_at", ASCENDING)]),
                IndexModel([("event_type", ASCENDING), ("created_at", ASCENDING)]),
            ]
        )

        alert_rules = self.collection(ALERT_RULES)
        await alert_rules.create_indexes(
            [
                IndexModel([("name", ASCENDING)], unique=True),
                IndexModel([("enabled", ASCENDING)]),
                IndexModel([("source", ASCENDING)]),
            ]
        )

        alert_events = self.collection(ALERT_EVENTS)
        await alert_events.create_indexes(
            [
                IndexModel([("created_at", ASCENDING)]),
                IndexModel([("rule_id", ASCENDING), ("created_at", ASCENDING)]),
            ]
        )
