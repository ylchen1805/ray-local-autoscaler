import asyncio

from fastapi import FastAPI, Query, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.models import (
    ClusterStatusResponse,
    CreateOrderResponse,
    CreateOrderRequest,
    ErrorResponse,
    EtaResponse,
    OrderListResponse,
    OrderResponse,
    ScalingHistoryResponse,
    TaskStatus,
)

# 假設的訂單管理器 ----------未來要刪除------------
from api.order_manager import InMemoryOrderManager, OrderNotFoundError

app = FastAPI(
    title="Ray Task Dispatch API",
    version="0.1.0",
    description="Mock frontend/backend API for Ray task dispatch and cluster status.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

#測試用 ------------未來要刪除----------------
manager = InMemoryOrderManager()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/orders",
    response_model=CreateOrderResponse,
    status_code=201,
    responses={500: {"model": ErrorResponse}},
)
def create_order(request: CreateOrderRequest) -> CreateOrderResponse:
    # backend function ( 目前InMemoryOrderManager.create_order())
    return manager.create_order(request)


@app.get("/orders", response_model=OrderListResponse)
def list_orders(
    status: TaskStatus | None = None,
    limit: int = Query(default=50, ge=1, le=500),
) -> OrderListResponse:
    return manager.list_orders(status=status, limit=limit)


@app.get(
    "/orders/{order_id}",
    response_model=OrderResponse,
    responses={404: {"model": ErrorResponse}},
)
def get_order(order_id: str) -> OrderResponse | JSONResponse:
    try:
        return manager.get_order(order_id)
    except OrderNotFoundError as exc:
        return _error_response(404, str(exc))


@app.get("/cluster/status", response_model=ClusterStatusResponse)
def get_cluster_status() -> ClusterStatusResponse:
    return manager.get_cluster_status()


@app.get("/cluster/eta", response_model=EtaResponse)
def get_eta() -> EtaResponse:
    return manager.get_eta()


@app.get("/cluster/scaling-history", response_model=ScalingHistoryResponse)
def get_scaling_history(
    limit: int = Query(default=20, ge=1, le=200),
) -> ScalingHistoryResponse:
    return manager.get_scaling_history(limit=limit)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    while True:
        await websocket.send_json({"event": "heartbeat", "data": manager.heartbeat()})
        await asyncio.sleep(5)


def _error_response(status_code: int, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": message},
    )
