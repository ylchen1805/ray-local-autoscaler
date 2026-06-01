from fastapi import APIRouter, HTTPException

from autoscaler.monitor import (
    get_alive_nodes,
    get_cluster_status,
    get_cpu_usage,
    get_gpu_usage,
    get_pending_resources,
)

router = APIRouter()


@router.get("/cluster/status")
def cluster_status():
    try:
        status = get_cluster_status()
        pending_resources = get_pending_resources(status)
        nodes = get_alive_nodes()
        cpu_used, cpu_total = get_cpu_usage(status)
        gpu_used, gpu_total = get_gpu_usage(status)
        return {
            "pending_resources": pending_resources,
            "alive_nodes": len(nodes),
            "cpu_used": cpu_used,
            "cpu_total": cpu_total,
            "gpu_used": gpu_used,
            "gpu_total": gpu_total,
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))
