from datetime import UTC, datetime
from threading import RLock

from api.models import (
    AutoscalerStatus,
    ClusterStatusResponse,
    CpuUsage,
    CreateOrderResponse,
    CreateOrderRequest,
    EtaResponse,
    HeadNodeStatus,
    OrderListResponse,
    OrderResponse,
    ScalingHistoryItem,
    ScalingHistoryResponse,
    TaskStatus,
    TripInfo,
    WorkerStatus,
)


class OrderNotFoundError(Exception):
    pass


class InMemoryOrderManager:
    """Mock backend manager used before the Ray OrderManager is connected."""

    def __init__(self) -> None:
        self._orders: dict[str, OrderResponse] = {}
        self._next_order_number = 1
        self._lock = RLock()
        self._scaling_history: list[ScalingHistoryItem] = [
            ScalingHistoryItem(
                timestamp=_now(),
                action="none",
                worker_id=None,
                trigger_reason="mock API server started",
            )
        ]

    def create_order(self, request: CreateOrderRequest) -> CreateOrderResponse:
        with self._lock:
            order = OrderResponse(
                order_id=self._next_order_id(),
                order_type=request.order_type,
                status=TaskStatus.PENDING,
                created_at=_now(),
                payload=request.payload,
            )
            self._orders[order.order_id] = order
            return CreateOrderResponse(
                order_id=order.order_id,
                order_type=order.order_type,
                status=order.status,
                created_at=order.created_at,
            )

    def list_orders(
        self,
        status: TaskStatus | None = None,
        limit: int = 50,
    ) -> OrderListResponse:
        with self._lock:
            self._refresh_orders()
            orders = list(self._orders.values())
            if status is not None:
                orders = [order for order in orders if order.status == status]
            orders = orders[:limit]
            return OrderListResponse(orders=orders, total=len(orders))

    def get_order(self, order_id: str) -> OrderResponse:
        with self._lock:
            self._refresh_orders()
            return self._get_order(order_id)

    def get_cluster_status(self) -> ClusterStatusResponse:
        with self._lock:
            self._refresh_orders()
            pending_tasks = sum(
                1 for order in self._orders.values() if order.status == TaskStatus.PENDING
            )
            running_tasks = sum(
                1
                for order in self._orders.values()
                if order.status
                in {
                    TaskStatus.MATCHING,
                    TaskStatus.DRIVER_ASSIGNED,
                    TaskStatus.ON_TRIP,
                }
            )
            worker_count = max(1, min(3, pending_tasks + running_tasks))
            cpu_used = round(min(worker_count, running_tasks * 0.65 + pending_tasks * 0.15), 2)
            cpu_total = float(worker_count)
            cpu_percent = round(cpu_used / cpu_total, 2) if cpu_total else 0.0

            workers = [
                WorkerStatus(
                    node_id=f"ray-worker-{index}",
                    ip=f"172.18.0.{index + 2}",
                    status="alive",
                    cpu_used=round(min(1.0, cpu_used / worker_count), 2),
                    cpu_total=1.0,
                )
                for index in range(1, worker_count + 1)
            ]

            return ClusterStatusResponse(
                head_node=HeadNodeStatus(ip="172.18.0.2", status="alive"),
                workers=workers,
                worker_count=worker_count,
                pending_tasks=pending_tasks,
                cpu_usage=CpuUsage(used=cpu_used, total=cpu_total, percent=cpu_percent),
                autoscaler=AutoscalerStatus(
                    min_workers=0,
                    max_workers=5,
                    cooldown_remaining=0,
                    last_scaled_at=self._scaling_history[-1].timestamp,
                    last_action=self._scaling_history[-1].action,
                ),
            )

    def get_scaling_history(self, limit: int = 20) -> ScalingHistoryResponse:
        with self._lock:
            return ScalingHistoryResponse(history=self._scaling_history[-limit:])

    def get_eta(self) -> EtaResponse:
        status = self.get_cluster_status()
        average_ride_seconds = 45
        estimated_wait_seconds = int(
            status.pending_tasks / max(status.worker_count, 1) * average_ride_seconds
        )
        return EtaResponse(
            pending_tasks=status.pending_tasks,
            worker_count=status.worker_count,
            estimated_wait_seconds=estimated_wait_seconds,
            surge=status.pending_tasks >= 5,
        )

    def heartbeat(self) -> dict[str, int | float]:
        status = self.get_cluster_status()
        return {
            "pending_tasks": status.pending_tasks,
            "worker_count": status.worker_count,
            "cpu_percent": status.cpu_usage.percent,
        }

    def _refresh_orders(self) -> None:
        now = _now()
        for order_id, order in list(self._orders.items()):
            elapsed = (now - order.created_at).total_seconds()

            if order.status == TaskStatus.PENDING and elapsed >= 1:
                self._orders[order_id] = order.model_copy(
                    update={
                        "status": TaskStatus.MATCHING,
                        "started_at": now,
                        "worker_node": self._worker_for(order_id),
                    }
                )
                continue

            if order.status == TaskStatus.MATCHING and elapsed >= 3:
                self._orders[order_id] = order.model_copy(
                    update={
                        "status": TaskStatus.DRIVER_ASSIGNED,
                        "trip": self._trip_for(order_id),
                    }
                )
                continue

            if order.status == TaskStatus.DRIVER_ASSIGNED and elapsed >= 6:
                self._orders[order_id] = order.model_copy(
                    update={
                        "status": TaskStatus.ON_TRIP,
                    }
                )
                continue

            if order.status == TaskStatus.ON_TRIP and elapsed >= 10:
                self._orders[order_id] = order.model_copy(
                    update={
                        "status": TaskStatus.COMPLETED,
                        "completed_at": now,
                        "result": {
                            "fare": order.trip.fare_estimate if order.trip else 268,
                            "duration_minutes": (
                                order.trip.estimated_duration if order.trip else 18
                            ),
                            "distance_km": 8.3,
                        },
                    }
                )

    def _get_order(self, order_id: str) -> OrderResponse:
        try:
            return self._orders[order_id]
        except KeyError as exc:
            raise OrderNotFoundError(f"order {order_id} not found") from exc

    def _next_order_id(self) -> str:
        order_id = f"order-{self._next_order_number:03d}"
        self._next_order_number += 1
        return order_id

    def _worker_for(self, order_id: str) -> str:
        numeric_id = int(order_id.rsplit("-", maxsplit=1)[1])
        return f"ray-worker-{(numeric_id % 3) + 1}"

    def _trip_for(self, order_id: str) -> TripInfo:
        numeric_id = int(order_id.rsplit("-", maxsplit=1)[1])
        drivers = [
            ("driver-007", "王大明", "ABC-1234"),
            ("driver-021", "林小美", "TPE-5521"),
            ("driver-108", "陳志豪", "RAY-8265"),
        ]
        driver_id, driver_name, license_plate = drivers[numeric_id % len(drivers)]
        return TripInfo(
            driver_id=driver_id,
            driver_name=driver_name,
            driver_rating=4.8,
            license_plate=license_plate,
            estimated_arrival=4,
            estimated_duration=18,
            fare_estimate=320,
        )


def _now() -> datetime:
    return datetime.now(UTC)
