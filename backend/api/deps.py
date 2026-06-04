from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..order.service import RayOrderService

# Set by backend/main.py lifespan before any requests are served.
manager: "RayOrderService | None" = None
