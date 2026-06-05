from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field


class OrderNotFoundError(Exception):
    pass


class InvalidPayloadError(Exception):
    pass


class OrderCreationError(Exception):
    pass


class TaskStatus(StrEnum):
    PENDING = "pending"
    MATCHING = "matching"
    DRIVER_ASSIGNED = "driver_assigned"
    ON_TRIP = "on_trip"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class OrderPayload(BaseModel):
    origin: str = Field(default="台北車站", min_length=1)
    destination: str = Field(default="松山機場", min_length=1)
    origin_lat: float = 25.0478
    origin_lng: float = 121.517
    destination_lat: float = 25.063
    destination_lng: float = 121.553
    ride_type: Literal["standard", "premium"] = "standard"


class CreateOrderRequest(BaseModel):
    order_type: Literal["ride"] = "ride"
    payload: OrderPayload = Field(default_factory=OrderPayload)


class TripInfo(BaseModel):
    driver_id: str
    driver_name: str
    driver_rating: float
    license_plate: str
    estimated_arrival: int
    estimated_duration: int
    fare_estimate: int


class OrderResponse(BaseModel):
    order_id: str
    order_type: str
    status: TaskStatus
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    worker_node: str | None = None
    payload: OrderPayload
    trip: TripInfo | None = None
    result: dict[str, Any] | None = None


class OrderListResponse(BaseModel):
    orders: list[OrderResponse]
    total: int


class CreateOrderResponse(BaseModel):
    order_id: str
    order_type: str
    status: TaskStatus
    created_at: datetime


class EtaResponse(BaseModel):
    pending_tasks: int
    worker_count: int
    estimated_wait_seconds: int
    surge: bool


class HeadNodeStatus(BaseModel):
    ip: str
    status: str


class WorkerStatus(BaseModel):
    node_id: str
    ip: str
    status: str
    cpu_used: float
    cpu_total: float


class CpuUsage(BaseModel):
    used: float
    total: float
    percent: float


class AutoscalerStatus(BaseModel):
    min_workers: int
    max_workers: int
    cooldown_remaining: int
    last_scaled_at: datetime | None = None
    last_action: Literal["scale_up", "scale_down", "none"]


class ClusterStatusResponse(BaseModel):
    head_node: HeadNodeStatus
    workers: list[WorkerStatus]
    worker_count: int
    pending_tasks: int
    cpu_usage: CpuUsage
    autoscaler: AutoscalerStatus


class ScalingHistoryItem(BaseModel):
    timestamp: datetime
    action: Literal["scale_up", "scale_down", "none"]
    worker_id: str | None = None
    trigger_reason: str


class ScalingHistoryResponse(BaseModel):
    history: list[ScalingHistoryItem]


class ErrorResponse(BaseModel):
    error: str
