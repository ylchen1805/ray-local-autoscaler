import random
import time
from datetime import datetime

import ray

from backend.models import OrderStatus


@ray.remote
class OrderActor:
    def __init__(self, order_id: str, manager_handle):
        self.order_id = order_id
        self.manager = manager_handle
        self.status = OrderStatus.OrderCreated

    def run(self):
        transitions = [
            OrderStatus.DriverMatched,
            OrderStatus.PassengerBoarded,
            OrderStatus.TripCompleted,
        ]
        for status in transitions:
            time.sleep(random.uniform(5.0, 30.0))
            print(
                f"[{datetime.now().isoformat()}] OrderActor {self.order_id} status: {self.status} -> {status.value}"
            )
            self.status = status.value
            self.manager.update_status.remote(self.order_id, status.value)
