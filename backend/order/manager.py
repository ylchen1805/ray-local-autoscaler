import uuid
from collections import deque
from datetime import datetime
from typing import Dict, List, Optional
import threading

import ray
from pydantic import BaseModel, Field

from ..models import (
    TaskStatus,
    OrderPayload,
    TripInfo,
    OrderNotFoundError,
    OrderCancelConflictError,
)
from .actor import OrderActor

_MAX_EVENTS = 100


class Order(BaseModel):
    order_id: str
    payload: OrderPayload
    trip: TripInfo | None = None
    status: TaskStatus = TaskStatus.PENDING
    status_timestamps: Dict[TaskStatus, datetime] = Field(default_factory=dict)
    order_type: str = "ride"
    worker_node: str | None = None


class Event(BaseModel):
    order_id: str
    status: TaskStatus
    timestamp: datetime


@ray.remote
class OrderManager:
    def __init__(self):
        self.orders: Dict[str, Order] = {}
        self.events: deque = deque(maxlen=_MAX_EVENTS)
        self._event_offset: int = 0
        self.actor_handles: Dict[str, ray.actor.ActorHandle] = {}
        self._thread_lock = threading.Lock()
        self._scaling_history: List[dict] = []

    def create_order(self, payload: dict | None = None) -> str:
        order_id = self._get_unique_order_id()
        order = Order(
            order_id=order_id,
            payload=OrderPayload(**(payload or {})),
        )
        with self._thread_lock:
            self.orders[order_id] = order
        self.update_status(order_id, TaskStatus.PENDING)

        self_handle = ray.get_actor("order_manager", namespace="default")
        actor = OrderActor.remote(order_id, self_handle)
        run_ref = actor.run.remote()
        self.actor_handles[order_id] = {"actor_handle": actor, "run_ref": run_ref}
        return order_id

    def _get_unique_order_id(self) -> str:
        ID_LENGTH = 16
        order_id = f"order_{uuid.uuid4().hex[:ID_LENGTH]}"
        while order_id in self.orders:
            order_id = f"order_{uuid.uuid4().hex[:ID_LENGTH]}"
        return order_id

    def register_worker(self, order_id: str, worker_node: str) -> None:
        if order_id in self.orders:
            self.orders[order_id].worker_node = worker_node

    def set_trip(self, order_id: str, trip: dict) -> None:
        if order_id in self.orders:
            self.orders[order_id].trip = TripInfo(**trip)

    def update_status(self, order_id: str, status: TaskStatus) -> None:
        if order_id not in self.orders:
            return
        o = self.orders[order_id]
        o.status = status
        o.status_timestamps[status] = datetime.now()
        self._append_event(order_id, status)

        if o.status == TaskStatus.COMPLETED:
            self._archive_order(order_id)

    def cancel_order(self, order_id: str) -> dict:
        o = self.orders.get(order_id)
        if o is None:
            raise OrderNotFoundError(f"order {order_id} not found")

        _CANCELLABLE = {TaskStatus.PENDING, TaskStatus.MATCHING}
        if o.status not in _CANCELLABLE:
            raise OrderCancelConflictError(
                f"order cannot be cancelled from status: {o.status.value}"
            )

        actor_info = self.actor_handles.pop(order_id, None)
        if actor_info:
            ray.kill(actor_info["actor_handle"])

        self.update_status(order_id, TaskStatus.CANCELLED)
        return {"order_id": order_id, "status": "cancelled"}

    def get_order(self, order_id: str) -> Optional[dict]:
        o = self.orders.get(order_id)
        if o is None:
            return None
        return o.model_dump()

    def list_orders(
        self, status_filter: Optional[str] = None, limit: int = 50
    ) -> List[dict]:
        with self._thread_lock:
            orders = list(self.orders.values())
        if status_filter:
            orders = [o for o in orders if o.status.value == status_filter]
        orders.sort(
            key=lambda o: o.status_timestamps.get(TaskStatus.PENDING, datetime.min),
            reverse=True,
        )
        return [o.model_dump() for o in orders[:limit]]

    def get_events_since(self, last_index: int) -> List[dict]:
        start = max(0, last_index - self._event_offset)
        return [event.model_dump() for event in list(self.events)[start:]]

    def _append_event(self, order_id: str, status: TaskStatus) -> None:
        with self._thread_lock:
            if len(self.events) == _MAX_EVENTS:
                self._event_offset += 1
            self.events.append(
                Event(
                    order_id=order_id,
                    status=status,
                    timestamp=datetime.now(),
                )
            )

    def _archive_order(self, order_id: str) -> None:
        actor_info = self.actor_handles.pop(order_id, None)
        handle = actor_info["actor_handle"] if actor_info else None
        run_ref = actor_info["run_ref"] if actor_info else None
        if handle is not None:
            print("kill actor for order_id:", order_id)
            # ensure the actor has finished its run method before killing
            if run_ref is not None:
                try:
                    ray.get(run_ref)
                except Exception:
                    self.update_status(order_id, TaskStatus.FAILED)
            ray.kill(handle)
