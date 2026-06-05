import asyncio
import json
from datetime import UTC, datetime
from typing import AsyncGenerator

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from . import deps

router = APIRouter()


class ConnectionManager:
    def __init__(self) -> None:
        self._next_id = 0
        self._connections: dict[int, tuple[asyncio.Queue, set[str]]] = {}

    def add(self, channels: set[str]) -> tuple[int, asyncio.Queue]:
        conn_id = self._next_id
        self._next_id += 1
        q: asyncio.Queue = asyncio.Queue()
        self._connections[conn_id] = (q, channels)
        return conn_id, q

    def remove(self, conn_id: int) -> None:
        self._connections.pop(conn_id, None)

    def broadcast(self, channel: str, event: dict) -> None:
        for q, channels in list(self._connections.values()):
            if channel in channels:
                try:
                    q.put_nowait(event)
                except asyncio.QueueFull:
                    pass


_cm = ConnectionManager()


async def publish_order_update(
    order_id: str,
    status: str,
    trip: dict | None = None,
    result: dict | None = None,
) -> None:
    _cm.broadcast(
        "orders",
        {
            "event": "order_updated",
            "data": {
                "order_id": order_id,
                "status": status,
                "trip": trip,
                "result": result,
                "updated_at": datetime.now(UTC).isoformat(),
            },
        },
    )


# async def publish_cluster_update(
#     action: str,
#     worker_id: str | None,
#     worker_count: int,
#     pending_tasks: int,
# ) -> None:
#     _cm.broadcast(
#         "cluster",
#         {
#             "event": "cluster_updated",
#             "data": {
#                 "action": action,
#                 "worker_id": worker_id,
#                 "worker_count": worker_count,
#                 "pending_tasks": pending_tasks,
#                 "timestamp": datetime.now(UTC).isoformat(),
#             },
#         },
#     )


async def poll_order_events() -> None:
    last_index = 0
    while True:
        await asyncio.sleep(1)
        if deps.manager is None:
            continue
        try:
            events = await asyncio.to_thread(deps.manager.get_events_since, last_index)
        except Exception:
            continue
        for ev in events:
            last_index += 1
            order_id = ev["order_id"]
            status = str(ev["status"])
            try:
                order = await asyncio.to_thread(deps.manager.get_order, order_id)
                trip = order.trip.model_dump() if order.trip else None
                result = order.result
            except Exception:
                trip = None
                result = None
            await publish_order_update(order_id, status, trip=trip, result=result)


@router.get("/sse")
async def sse_endpoint(
    channel: str | None = Query(default=None),
) -> StreamingResponse:
    channels: set[str] = {channel} if channel else {"orders", "cluster"}
    conn_id, queue = _cm.add(channels)

    async def event_generator() -> AsyncGenerator[str, None]:
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=5.0)
                    yield f"data: {json.dumps(event, default=str, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    if deps.manager is None:
                        continue
                    hb = await asyncio.to_thread(deps.manager.heartbeat)
                    yield f"data: {json.dumps({'event': 'heartbeat', 'data': hb}, default=str, ensure_ascii=False)}\n\n"
        finally:
            _cm.remove(conn_id)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
