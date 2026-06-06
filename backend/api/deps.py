from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..cluster.monitor import ClusterMonitor
    from ..order.service import RayOrderService

# Set by backend/main.py lifespan before any requests are served.
manager: "RayOrderService | None" = None
cluster_monitor: "ClusterMonitor | None" = None
