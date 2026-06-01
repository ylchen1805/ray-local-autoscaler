from contextlib import asynccontextmanager

import ray
from fastapi import FastAPI

from backend.api.order.create import router as create_router
from backend.api.order.status import router as status_router
from backend.api.monitor.dashboard import router as dashboard_router
from backend.api.monitor.cluster import router as cluster_router
from backend.order.manager import OrderManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    ray.init(
        address="ray://localhost:10001",
        ignore_reinit_error=True,
        runtime_env={
            "working_dir": ".",
        },
    )
    try:
        ray.get_actor("order_manager", namespace="default")
        print("[backend] connected to existing OrderManager")
    except ValueError:
        OrderManager.options(
            name="order_manager",
            lifetime="detached",
            namespace="default",
        ).remote()
        print("[backend] created new OrderManager")
    yield


app = FastAPI(title="Ray Dispatch Backend", lifespan=lifespan)

PREFIX = "/api/v1"
app.include_router(create_router, prefix=PREFIX)
app.include_router(status_router, prefix=PREFIX)
app.include_router(dashboard_router, prefix=PREFIX)
app.include_router(cluster_router, prefix=PREFIX)
