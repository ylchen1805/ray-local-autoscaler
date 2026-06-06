from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from ..models import ClusterStatusResponse, EtaResponse, ScalingHistoryResponse
from . import deps

router = APIRouter()


@router.get("/cluster/eta", response_model=EtaResponse)
def cluster_eta() -> EtaResponse | JSONResponse:
    if deps.cluster_monitor is None:
        return JSONResponse({"error": "cluster monitor unavailable"}, status_code=503)
    try:
        return deps.cluster_monitor.get_eta()
    except Exception:
        return JSONResponse({"error": "failed to get eta"}, status_code=500)


@router.get("/cluster/status", response_model=ClusterStatusResponse)
def cluster_status() -> ClusterStatusResponse | JSONResponse:
    if deps.cluster_monitor is None:
        return JSONResponse({"error": "cluster monitor unavailable"}, status_code=503)
    try:
        return deps.cluster_monitor.get_cluster_status()
    except Exception:
        return JSONResponse({"error": "failed to get cluster status"}, status_code=500)


@router.get("/cluster/scaling-history", response_model=ScalingHistoryResponse)
def scaling_history(
    limit: int = Query(default=20, ge=1, le=200),
) -> ScalingHistoryResponse | JSONResponse:
    if deps.cluster_monitor is None:
        return JSONResponse({"error": "cluster monitor unavailable"}, status_code=503)
    try:
        return deps.cluster_monitor.get_scaling_history(limit=limit)
    except Exception:
        return JSONResponse({"error": "failed to get scaling history"}, status_code=500)
