# autoscaler/monitor.py
import requests

DASHBOARD_URL = "http://localhost:8265"  # Use Ray API to get the current status


class HardwareNotSupportedError(Exception):
    "The cluster does not have this kind of resource."

    pass


def get_cluster_status() -> dict:
    "Return cluster status in dict"

    response = requests.get(f"{DASHBOARD_URL}/api/cluster_status")
    return response.json()["data"]["clusterStatus"]


def get_pending_resources(status: dict) -> list:
    """Return the list of pending resource demands.

    Each entry represents a task( actor)wating for resources.
    [] if no task( actor) are queued.
    """
    return status["loadMetricsReport"]["resourceDemand"]


def get_alive_nodes() -> list[dict]:
    "Return a list of currently alive nodes excluding the head node."

    response = requests.get(f"{DASHBOARD_URL}/nodes?view=summary")
    nodes = response.json()["data"]["summary"]
    return [node for node in nodes if node["raylet"]["state"] == "ALIVE"]


def get_cpu_usage(status: dict) -> tuple[float, float]:
    "Return the current CPU usage as a tuple of (used, total)."
    usage = status["loadMetricsReport"]["usage"]
    cpu_usage = usage["CPU"]
    return (cpu_usage[0], cpu_usage[1])


def get_gpu_usage(status: dict) -> tuple[float, float]:
    "Return the current GPU usage as a tuple of (used, total)."
    usage = status["loadMetricsReport"]["usage"]
    if "GPU" not in usage:
        raise HardwareNotSupportedError("The cluster doesn't have any GPU resource.")
    gpu_usage = usage["GPU"]
    return (gpu_usage[0], gpu_usage[1])
