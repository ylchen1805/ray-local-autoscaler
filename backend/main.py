import asyncio
import os
from contextlib import asynccontextmanager

import ray
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .cluster.monitor import ClusterMonitor
from .order.manager import OrderManager
from .order.service import RayOrderService
from .order.driver import DriverPool
from .api import deps
from .api.cluster import router as cluster_router
from .api.order import router as order_router
from .api.sse import router as sse_router, poll_cluster_events, poll_order_events


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        ray.init(
            address="auto",
            ignore_reinit_error=True,
            namespace="default",
            runtime_env={"working_dir": "."},
        )
        try:
            handle = ray.get_actor("order_manager", namespace="default")
            print("[api] connected to existing OrderManager")
        except ValueError:
            handle = OrderManager.options(
                name="order_manager",
                lifetime="detached",
                namespace="default",
            ).remote()
            print("[api] created new OrderManager")

        try:
            ray.get_actor("driver_pool", namespace="default")
            print("[api] connected to existing DriverPool")
        except ValueError:
            DriverPool.options(
                name="driver_pool",
                lifetime="detached",
                namespace="default",
            ).remote()
            print("[api] created new DriverPool")

        deps.manager = RayOrderService(handle)
        deps.cluster_monitor = ClusterMonitor()
    except Exception as exc:
        print(f"[api] Error initializing Ray: {exc}")
        exit()

    order_task = asyncio.create_task(poll_order_events())
    cluster_task = asyncio.create_task(poll_cluster_events())
    yield
    for task in (order_task, cluster_task):
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="Ray Task Dispatch API",
    version="0.1.0",
    description="Frontend/backend API for Ray task dispatch and cluster status.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(order_router)
app.include_router(cluster_router)
app.include_router(sse_router)
