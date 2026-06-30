import asyncio
import logging
from typing import Any

from server.audit.enricher import AuditEnricher
from server.audit.models import AuditActor, AuditContext, BaseAuditEventEnvelope, SystemAuditEvent
from server.audit.repository import AuditRepository


LOGGER = logging.getLogger("multiplex.audit.collector")


class AuditCollector:
    """Thin pipeline manager with backpressure and drop strategy."""
    
    def __init__(self, enricher: AuditEnricher, repository: AuditRepository, max_size: int = 5000) -> None:
        self.enricher = enricher
        self.repository = repository
        self.max_size = max_size
        self._queue: asyncio.Queue[BaseAuditEventEnvelope] = asyncio.Queue(maxsize=max_size)
        self._dropped_events = 0
        self._worker_task: asyncio.Task[Any] | None = None
        self._running = False

    def dispatch(self, event: BaseAuditEventEnvelope) -> None:
        """Synchronously enqueue an event, dropping if queue is full."""
        try:
            enriched = self.enricher.enrich(event)
            self._queue.put_nowait(enriched)
        except asyncio.QueueFull:
            self._dropped_events += 1
            LOGGER.warning(
                f"Audit queue full! Dropped event {event.event_id} ({event.event_type}). "
                f"Total dropped: {self._dropped_events}"
            )

    async def record(
        self,
        event_type: str,
        *,
        actor: Any, # UserPrincipal
        audit_ctx: AuditContext | None = None,
        request_meta: AuditContext | None = None,
        target: dict[str, Any] | None = None,
        result: str = "success",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Adapter for legacy internal service logging, mapping to Leaf Events."""
        ctx = audit_ctx or request_meta
        if ctx is None:
            raise ValueError("Either audit_ctx or request_meta must be provided")
        
        # Merge principal with context actor
        current_actor = ctx.actor.model_copy()
        if actor:
            current_actor.user_id = actor.user_id
            current_actor.username = actor.username

        event = SystemAuditEvent.create(
            event_type=event_type,
            correlation_id=ctx.correlation_id,
            parent_event_id=ctx.parent_event_id or ctx.correlation_id, # Link to parent boundary
            actor=current_actor,
            source=ctx.source,
            target=target,
            metadata=metadata,
            result=result,
        )
        self.dispatch(event)
        # return dummy doc for legacy compatibility
        return {"event_id": event.event_id}


    async def list_events(self, limit: int = 50, skip: int = 0, **filters: Any) -> list[dict[str, Any]]:
        return await self.repository.list_events(limit=limit, skip=skip, **filters)
        
    async def export_stream(self, start_date: str, end_date: str, **filters: Any):
        return self.repository.export_stream(start_date=start_date, end_date=end_date, **filters)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        LOGGER.info("AuditCollector worker started")

    async def stop(self) -> None:
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        
        # Flush remaining
        await self._flush()
        LOGGER.info("AuditCollector worker stopped")

    async def _worker_loop(self) -> None:
        while self._running:
            try:
                # Wait for at least one item, then gather up to batch size
                first_item = await self._queue.get()
                batch = [first_item]
                self._queue.task_done()
                
                # Drain queue up to 100 items
                while len(batch) < 100:
                    try:
                        item = self._queue.get_nowait()
                        batch.append(item)
                        self._queue.task_done()
                    except asyncio.QueueEmpty:
                        break
                        
                await self._insert_batch_with_circuit_breaker(batch)
            except asyncio.CancelledError:
                break
            except Exception as e:
                LOGGER.error(f"AuditCollector worker error: {e}", exc_info=True)
                await asyncio.sleep(1.0)  # Backoff on error
                
    async def _insert_batch_with_circuit_breaker(self, batch: list[BaseAuditEventEnvelope]) -> None:
        try:
            # simple attempt
            await asyncio.wait_for(self.repository.insert_many(batch), timeout=5.0)
        except asyncio.TimeoutError:
            LOGGER.error(f"MongoDB write timeout when inserting {len(batch)} audit events")
            # In a real circuit breaker, we'd toggle state. For now we just log and potentially drop if queue fills.
            # We don't re-enqueue to avoid poison pills or infinite loops.
        except Exception as e:
            LOGGER.error(f"Failed to insert audit events: {e}", exc_info=True)

    async def _flush(self) -> None:
        batch = []
        while not self._queue.empty():
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if batch:
            await self._insert_batch_with_circuit_breaker(batch)
