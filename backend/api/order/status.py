import ray
from fastapi import APIRouter, HTTPException

router = APIRouter()


@router.get("/orders/{order_id}")
def get_order_status(order_id: str):
    manager = ray.get_actor("order_manager", namespace="default")
    result = ray.get(manager.get_order.remote(order_id))
    if result is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return result
