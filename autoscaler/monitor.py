# autoscaler/monitor.py
import requests

DASHBOARD_URL = "http://localhost:8265"  # Use Ray API to get the current status


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
    pass


def get_gpu_usage(status: dict) -> tuple[float, float]:
    "Return the current GPU usage as a tuple of (used, total)."
    pass
