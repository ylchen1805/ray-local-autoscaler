import ray
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class CreateOrderRequest(BaseModel):
    passenger_id: str
    pickup_location: str
    dropoff_location: str


@router.post("/orders", status_code=202)
def create_order(body: CreateOrderRequest):
    manager = ray.get_actor("order_manager", namespace="default")
    order_id = ray.get(
        manager.create_order.remote(
            body.passenger_id, body.pickup_location, body.dropoff_location
        )
    )
    return {"order_id": order_id, "status": "OrderCreated"}
