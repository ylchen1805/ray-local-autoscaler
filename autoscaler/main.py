# autoscaler/main.py
import time
from autoscaler.monitor import (
    get_cluster_status,
    get_pending_resources,
    get_cpu_usage,
    get_alive_nodes,
)
from autoscaler.scaler import scale_up, scale_down
from autoscaler.policy import load_policy

POLL_INTERVAL = 10  # seconds between each check


def run():
    policy = load_policy()
    worker_counter = 0  # used to generate unique worker container names

    print("[autoscaler] starting...")

    while True:
        try:
            status = get_cluster_status()
            pending = get_pending_resources(status)
            cpu_used, _ = get_cpu_usage(status)
            alive_nodes = get_alive_nodes()
            alive_workers = len(alive_nodes) - 1  # exclude head node

            print(
                f"[autoscaler] pending={len(pending)} cpu={cpu_used} workers={alive_workers}"
            )

            if policy.should_scale_up(pending, alive_workers):
                worker_counter += 1
                print(f"[autoscaler] scaling up → ray-worker-{worker_counter}")
                scale_up(worker_counter)
                policy.records_scale()

            elif policy.should_scale_down(cpu_used, None, pending, alive_workers):
                print(f"[autoscaler] scaling down → ray-worker-{worker_counter}")
                scale_down(worker_counter)
                worker_counter -= 1
                policy.records_scale()

        except Exception as e:
            print(f"[autoscaler] error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
