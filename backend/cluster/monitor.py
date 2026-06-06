import os
import time
from datetime import UTC, datetime
from typing import Literal

import requests
import ray
import yaml

from ..models import (
    AutoscalerStatus,
    ClusterStatusResponse,
    CpuUsage,
    EtaResponse,
    HeadNodeStatus,
    ScalingHistoryItem,
    ScalingHistoryResponse,
    WorkerStatus,
)

_AVERAGE_RIDE_SECONDS = 300
_SURGE_THRESHOLD = 20


class ClusterMonitor:
    def __init__(self, policy_path: str = "scale_policy.yaml") -> None:
        try:
            with open(policy_path) as f:
                cfg = yaml.safe_load(f) or {}
        except FileNotFoundError:
            cfg = {}
        self._min_workers: int = cfg.get("min_workers", 0)
        self._max_workers: int = cfg.get("max_workers", 5)
        self._cooldown: int = cfg.get("cooldown", 15)
        self._last_scale_time: float = 0.0
        self._last_action: Literal["scale_up", "scale_down", "none"] = "none"
        self._last_worker_count: int = -1
        self._scaling_history: list[dict] = []
        # list_tasks() only skips the internal ray.init() when address is http://...
        # Derive from RAY_ADDRESS: "ray://ray-head:10001" → "http://ray-head:8265"
        addr = os.environ.get("RAY_ADDRESS", "ray://localhost:10001")
        host = (
            addr[len("ray://") :].split(":")[0]
            if addr.startswith("ray://")
            else "localhost"
        )
        self._dashboard_url = f"http://{host}:8265"

    def get_cluster_status(self) -> ClusterStatusResponse:
        try:
            nodes = ray.nodes()
            alive = [n for n in nodes if n.get("Alive", False)]

            head_nodes = [
                n for n in alive if "node:InternalHead" in n.get("Resources", {})
            ]
            worker_nodes = [
                n for n in alive if "node:InternalHead" not in n.get("Resources", {})
            ]

            head_ip = head_nodes[0]["NodeManagerAddress"] if head_nodes else "unknown"
            head = HeadNodeStatus(ip=head_ip, status="alive")

            total_res = ray.cluster_resources()
            avail_res = ray.available_resources()
            cpu_total = total_res.get("CPU", 0.0)
            cpu_avail = avail_res.get("CPU", 0.0)
            cpu_used = max(0.0, cpu_total - cpu_avail)
            cpu_ratio = cpu_used / cpu_total if cpu_total > 0 else 0.0

            workers = []
            for n in worker_nodes:
                node_cpu_total = n["Resources"].get("CPU", 0.0)
                node_cpu_used = round(node_cpu_total * cpu_ratio, 3)
                workers.append(
                    WorkerStatus(
                        node_id=n["NodeManagerHostname"] or n["NodeID"][:12],
                        ip=n["NodeManagerAddress"],
                        status="alive",
                        cpu_used=node_cpu_used,
                        cpu_total=round(node_cpu_total, 3),
                    )
                )

            # Fetch all tasks from Dashboard HTTP API and count non-terminal ones.
            _PENDING_STATES = {
                "PENDING_ARGS_AVAIL",
                "PENDING_NODE_ASSIGNMENT",
                "PENDING_OBJ_STORE_MEM_AVAIL",
                "PENDING_ARGS_FETCH",
            }
            pending_tasks = 0
            resp = requests.get(
                f"{self._dashboard_url}/api/v0/tasks",
                params={"limit": 10000, "detail": 0},
                timeout=5,
            )
            all_tasks = resp.json().get("data", {}).get("result", {}).get("result", [])
            pending_tasks = sum(
                1 for t in all_tasks if t.get("state") in _PENDING_STATES
            )

            worker_count = len(workers)
            cpu_percent = round(cpu_used / cpu_total, 3) if cpu_total > 0 else 0.0
            cpu_usage = CpuUsage(
                used=round(cpu_used, 3),
                total=round(cpu_total, 3),
                percent=cpu_percent,
            )

            cooldown_remaining = max(
                0, int(self._cooldown - (time.time() - self._last_scale_time))
            )
            last_scaled_at = (
                datetime.fromtimestamp(self._last_scale_time, tz=UTC)
                if self._last_scale_time > 0
                else None
            )

            self._check_and_record_scale(
                worker_count, pending_tasks, [w.node_id for w in workers]
            )

            return ClusterStatusResponse(
                head_node=head,
                workers=workers,
                worker_count=worker_count,
                pending_tasks=pending_tasks,
                cpu_usage=cpu_usage,
                autoscaler=AutoscalerStatus(
                    min_workers=self._min_workers,
                    max_workers=self._max_workers,
                    cooldown_remaining=cooldown_remaining,
                    last_scaled_at=last_scaled_at,
                    last_action=self._last_action,
                ),
            )
        except Exception as exc:
            import traceback

            traceback.print_exc()
            raise RuntimeError("failed to get cluster status") from exc

    def _check_and_record_scale(
        self, worker_count: int, pending_tasks: int, worker_ids: list[str]
    ) -> None:
        if self._last_worker_count == -1:
            self._last_worker_count = worker_count
            return
        if worker_count == self._last_worker_count:
            return
        action: Literal["scale_up", "scale_down"] = (
            "scale_up" if worker_count > self._last_worker_count else "scale_down"
        )
        self._last_action = action
        self._last_scale_time = time.time()
        self._scaling_history.append(
            {
                "timestamp": datetime.now(UTC),
                "action": action,
                "worker_id": worker_ids[-1] if worker_ids else None,
                "trigger_reason": (
                    f"pending_tasks={pending_tasks},"
                    f" worker_count={self._last_worker_count}→{worker_count}"
                ),
            }
        )
        self._last_worker_count = worker_count

    def get_eta(self) -> EtaResponse:
        status = self.get_cluster_status()
        wait = int(
            status.pending_tasks / max(status.worker_count, 1) * _AVERAGE_RIDE_SECONDS
        )
        return EtaResponse(
            pending_tasks=status.pending_tasks,
            worker_count=status.worker_count,
            estimated_wait_seconds=wait,
            surge=status.pending_tasks >= _SURGE_THRESHOLD,
        )

    def get_scaling_history(self, limit: int = 20) -> ScalingHistoryResponse:
        items = [ScalingHistoryItem(**e) for e in self._scaling_history[-limit:]]
        return ScalingHistoryResponse(history=items)

    def get_heartbeat_data(self) -> dict:
        status = self.get_cluster_status()
        return {
            "pending_tasks": status.pending_tasks,
            "worker_count": status.worker_count,
            "cpu_percent": status.cpu_usage.percent,
        }
