import uuid
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import asdict

import ray

from backend.models import Order, OrderStatus
from backend.order.actor import OrderActor


@ray.remote
class OrderManager:
    def __init__(self):
        self.orders: Dict[str, Order] = {}
        self.events: List[dict] = []
        self.actor_handles: Dict[str, ray.actor.ActorHandle] = {}

    def create_order(
        self, passenger_id: str, pickup_location: str, dropoff_location: str
    ) -> str:
        order_id = self._get_unique_order_id()
        now = datetime.now().isoformat()
        order = Order(
            order_id=order_id,
            passenger_id=passenger_id,
            pickup_location=pickup_location,
            dropoff_location=dropoff_location,
            status=OrderStatus.OrderCreated,
            status_timestamps={OrderStatus.OrderCreated: now},
        )
        self.orders[order_id] = order
        self._append_event(order_id, OrderStatus.OrderCreated)

        self_handle = ray.get_actor("order_manager", namespace="default")
        actor = OrderActor.remote(order_id, self_handle)
        actor.run.remote()
        self.actor_handles[order_id] = actor
        return order_id

    def _get_unique_order_id(self) -> str:
        ID_LENGTH = 16
        order_id = f"order_{uuid.uuid4().hex[:ID_LENGTH]}"
        while order_id in self.orders:
            order_id = f"order_{uuid.uuid4().hex[:ID_LENGTH]}"
        return order_id

    def update_status(self, order_id: str, status: str) -> None:
        if order_id not in self.orders:
            return
        s = OrderStatus(status)
        o = self.orders[order_id]
        o.status = s
        o.status_timestamps[s] = datetime.now().isoformat()
        self._append_event(order_id, s)

        if o.status == OrderStatus.TripCompleted:
            self._archive_order(order_id)

    def get_order(self, order_id: str) -> Optional[dict]:
        o = self.orders.get(order_id)
        if o is None:
            return None
        return asdict(o)

    def get_events_since(self, last_index: int) -> List[dict]:
        return self.events[last_index:]

    def _append_event(self, order_id: str, status: OrderStatus) -> None:
        self.events.append(
            {
                "order_id": order_id,
                "status": status.value,
                "timestamp": datetime.now().isoformat(),
            }
        )

    def get_snapshot(self) -> dict:
        orders = [asdict(o) for o in self.orders.values()]
        return {
            "orders": orders,
            "total_orders": len(orders),
        }

    def _archive_order(self, order_id: str) -> None:
        handle = self.actor_handles.pop(order_id, None)
        if handle is not None:
            print("kill actor for order_id:", order_id)
            ray.kill(handle)
        self.update_status(order_id, OrderStatus.Archived.value)
        self._append_event(order_id, OrderStatus.Archived)
