import threading
from datetime import UTC, datetime
from typing import Optional

import ray

from ..models import (
    CreateOrderRequest,
    CreateOrderResponse,
    InvalidPayloadError,
    OrderCreationError,
    OrderListResponse,
    OrderNotFoundError,
    OrderPayload,
    OrderResponse,
    TaskStatus,
    TripInfo,
)


class RayOrderService:
    def __init__(self, manager_handle) -> None:
        self._manager = manager_handle
        self._last_worker_count: int = 0
        self._lock = threading.Lock()

    # --- for order related API ---
    def create_order(self, request: CreateOrderRequest) -> CreateOrderResponse:
        payload = request.payload
        if not (-90 <= payload.origin_lat <= 90) or not (
            -90 <= payload.destination_lat <= 90
        ):
            raise InvalidPayloadError("latitude must be between -90 and 90")
        if not (-180 <= payload.origin_lng <= 180) or not (
            -180 <= payload.destination_lng <= 180
        ):
            raise InvalidPayloadError("longitude must be between -180 and 180")

        try:
            payload_dict = payload.model_dump()
            order_id = ray.get(self._manager.create_order.remote(payload=payload_dict))
            raw = ray.get(self._manager.get_order.remote(order_id))
        except (InvalidPayloadError, OrderCreationError):
            raise
        except Exception as exc:
            raise OrderCreationError("failed to create order") from exc

        return CreateOrderResponse(
            order_id=order_id,
            order_type=request.order_type,
            status=TaskStatus.PENDING,
            created_at=raw["status_timestamps"][TaskStatus.PENDING],
        )

    def list_orders(
        self,
        status: TaskStatus | None = None,
        limit: int = 50,
    ) -> OrderListResponse:
        raw_orders = ray.get(
            self._manager.list_orders.remote(
                status_filter=None,
                limit=500,
            )
        )
        result = [self._to_order_response(r) for r in raw_orders]
        if status is not None:
            result = [o for o in result if o.status == status]
        result = result[:limit]
        return OrderListResponse(orders=result, total=len(result))

    def get_order(self, order_id: str) -> OrderResponse:
        raw = ray.get(self._manager.get_order.remote(order_id))
        if raw is None:
            raise OrderNotFoundError(f"order {order_id} not found")
        return self._to_order_response(raw)

    # --- private helpers ---

    def _to_order_response(self, raw: dict) -> OrderResponse:
        status_str = raw.get("status", TaskStatus.PENDING)

        ts = raw.get("status_timestamps", {})
        # Keys are TaskStatus string values ("pending", "matching", …); values are datetime objects
        created_at: datetime = ts.get(TaskStatus.PENDING) or datetime.now(UTC)

        started_at: Optional[datetime] = None
        for key in (TaskStatus.MATCHING, TaskStatus.DRIVER_ASSIGNED):
            if key in ts:
                started_at = ts[key]
                break

        completed_at: Optional[datetime] = None
        if TaskStatus.COMPLETED in ts:
            completed_at = ts[TaskStatus.COMPLETED]

        trip = TripInfo(**raw["trip"]) if raw.get("trip") else None

        payload_data = raw.get("payload") or {}
        payload = OrderPayload(**payload_data) if payload_data else OrderPayload()

        result = None
        if status_str == TaskStatus.COMPLETED and trip:
            result = {
                "fare": trip.fare_estimate,
                "duration_minutes": trip.estimated_duration,
            }

        return OrderResponse(
            order_id=raw["order_id"],
            order_type=raw.get("order_type", "ride"),
            status=status_str,
            created_at=created_at,
            started_at=started_at,
            completed_at=completed_at,
            worker_node=raw.get("worker_node"),
            payload=payload,
            trip=trip,
            result=result,
        )
