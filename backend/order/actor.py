import random
import time
from datetime import datetime

import ray

from ..models import TaskStatus
from .driver import DriverPool, Driver


@ray.remote(num_cpus=1)
class OrderActor:
    def __init__(self, order_id: str, manager_handle):
        self.order_id = order_id
        self.manager = manager_handle
        self.status = TaskStatus.PENDING
        self._driver: Driver | None = None
        try:
            self._driver_pool = ray.get_actor("driver_pool", namespace="default")
        except ValueError:
            self._driver_pool = DriverPool.options(
                name="driver_pool", lifetime="detached", namespace="default"
            ).remote()

    def run(self):
        worker_node = ray.get_runtime_context().get_node_id()
        self.manager.register_worker.remote(self.order_id, worker_node)
        estimated_arrival, estimated_duration, fare_estimate = (
            self._simulate_trip_metrics()
        )

        transitions = [
            # matching time: slightly related to arrival
            (
                TaskStatus.MATCHING,
                max(0.5, estimated_arrival * random.uniform(0.3, 0.8)),
            ),
            # driver assignment: short buffer after matching
            (TaskStatus.DRIVER_ASSIGNED, random.uniform(0.5, 1.5)),
            # on trip: strongly tied to duration
            (
                TaskStatus.ON_TRIP,
                max(1.0, estimated_duration * random.uniform(0.7, 1.2)),
            ),
            # completion: cleanup / drop-off variance
            (TaskStatus.COMPLETED, random.uniform(1.0, 3.0)),
        ]
        for status, delay in transitions:
            time.sleep(delay)
            print(
                f"[{datetime.now().isoformat()}] OrderActor {self.order_id}: {self.status} -> {status.value}"
            )
            self.status = status

            if status == TaskStatus.MATCHING:
                self._driver = ray.get(self._driver_pool.acquire.remote())
                trip = {
                    **self._driver.model_dump(),
                    "estimated_arrival": estimated_arrival,
                    "estimated_duration": estimated_duration,
                    "fare_estimate": fare_estimate,
                }
                ray.get(self.manager.set_trip.remote(self.order_id, trip))

            self.manager.update_status.remote(self.order_id, status)

        if self._driver is not None:
            self._driver_pool.release.remote(self._driver.driver_id)

    def _simulate_trip_metrics(self):
        # simulate different order / driver

        # arrival
        estimated_arrival = max(1, int(random.expovariate(1 / 4)))

        # duration
        base_duration = estimated_arrival + random.randint(8, 25)
        noise = random.randint(-3, 5)
        estimated_duration = max(5, base_duration + noise)

        # fare
        base_fare = 80
        per_minute = 8
        noise = random.randint(-20, 40)

        fare_estimate = base_fare + estimated_duration * per_minute + noise
        fare_estimate = max(50, fare_estimate)

        return estimated_arrival, estimated_duration, fare_estimate
