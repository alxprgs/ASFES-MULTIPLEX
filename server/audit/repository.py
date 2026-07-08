import typing
from typing import Any

from pymongo import DESCENDING


from server.audit.migrations import migration_registry
from server.audit.models import BaseAuditEventEnvelope
from server.core.database import AUDIT_EVENTS, DatabaseManager


class AuditRepository:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    async def insert_many(self, events: list[BaseAuditEventEnvelope]) -> None:
        if not events:
            return

        collection = self.db.collection(AUDIT_EVENTS)
        documents = [event.model_dump(mode="json") for event in events]
        # set _id for MongoDB to match event_id
        for doc in documents:
            doc["_id"] = doc["event_id"]

        await collection.insert_many(documents, ordered=False)

    async def list_events(
        self, limit: int = 50, skip: int = 0, **filters: Any
    ) -> list[dict[str, Any]]:
        collection = self.db.collection(AUDIT_EVENTS)
        query = self._build_query(filters)

        cursor = (
            collection.find(query).sort("timestamp", DESCENDING).skip(skip).limit(limit)
        )
        items = []
        async for doc in cursor:
            doc.pop("_id", None)
            items.append(migration_registry.upgrade(doc))
        return items

    async def export_stream(
        self, start_date: str, end_date: str, **filters: Any
    ) -> typing.AsyncGenerator[dict[str, Any], None]:
        collection = self.db.collection(AUDIT_EVENTS)
        query = self._build_query(filters)
        query["timestamp"] = {"$gte": start_date, "$lte": end_date}

        # hint timestamp index to prevent collection scans
        cursor = (
            collection.find(query)
            .sort("timestamp", DESCENDING)
            .hint([("timestamp", DESCENDING)])
        )
        # MaxTimeMS to prevent CPU hogging on Mongo side
        cursor.max_time_ms(10000)

        async for doc in cursor:
            doc.pop("_id", None)
            yield migration_registry.upgrade(doc)

    def _build_query(self, filters: dict[str, Any]) -> dict[str, Any]:
        query = {}
        # exclude archived from normal views unless specifically requested
        if "archived" not in filters:
            query["archived"] = False

        for key, value in filters.items():
            if value is not None:
                query[key] = value

        return query
