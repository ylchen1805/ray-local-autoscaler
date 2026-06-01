import asyncio
import json

import ray
from fastapi import APIRouter
from sse_starlette.sse import EventSourceResponse

router = APIRouter()


@router.get("/dashboard/stream")
async def dashboard_stream():
    async def event_generator():
        last_index = 0
        manager = ray.get_actor("order_manager", namespace="default")
        loop = asyncio.get_running_loop()
        while True:
            events = await loop.run_in_executor(
                None, ray.get, manager.get_events_since.remote(last_index)
            )
            for event in events:
                yield {"event": "message", "data": json.dumps(event)}
                last_index += 1
            await asyncio.sleep(0.5)

    return EventSourceResponse(event_generator())


@router.get("/dashboard/snapshot")
def get_snapshot():
    manager = ray.get_actor("order_manager", namespace="default")
    return ray.get(manager.get_snapshot.remote())
