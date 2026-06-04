from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from . import deps
from ..models import (
    CreateOrderRequest,
    CreateOrderResponse,
    ErrorResponse,
    InvalidPayloadError,
    OrderCreationError,
    OrderListResponse,
    OrderNotFoundError,
    OrderResponse,
    TaskStatus,
)

router = APIRouter()


@router.post(
    "/orders",
    response_model=CreateOrderResponse,
    status_code=201,
    responses={
        400: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
)
def create_order(request: CreateOrderRequest) -> CreateOrderResponse | JSONResponse:
    try:
        return deps.manager.create_order(request)
    except InvalidPayloadError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    except (OrderCreationError, Exception):
        return JSONResponse(status_code=500, content={"error": "failed to create order"})


@router.get(
    "/orders",
    response_model=OrderListResponse,
    responses={500: {"model": ErrorResponse}},
)
def list_orders(
    status: TaskStatus | None = None,
    limit: int = Query(default=50, ge=1, le=500),
) -> OrderListResponse | JSONResponse:
    try:
        return deps.manager.list_orders(status=status, limit=limit)
    except Exception:
        return JSONResponse(status_code=500, content={"error": "failed to list orders"})


@router.get(
    "/orders/{order_id}",
    response_model=OrderResponse,
    responses={
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
)
def get_order(order_id: str) -> OrderResponse | JSONResponse:
    try:
        return deps.manager.get_order(order_id)
    except OrderNotFoundError:
        return JSONResponse(status_code=404, content={"error": "order not found"})
    except Exception:
        return JSONResponse(status_code=500, content={"error": "failed to get order"})
