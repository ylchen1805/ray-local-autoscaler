from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class OrderStatus(str, Enum):
    OrderCreated = "OrderCreated"  # when order happened
    DriverMatched = "DriverMatched"  # when actor scheduled
    PassengerBoarded = "PassengerBoarded"  # simulate
    TripCompleted = "TripCompleted"  # when actor complete
    Archived = "Archived"  # when node manager clean up the order actor


@dataclass
class Order:
    order_id: str
    passenger_id: str
    pickup_location: str
    dropoff_location: str
    status: OrderStatus
    driver_id: Optional[str] = None
    status_timestamps: dict = field(default_factory=dict)
