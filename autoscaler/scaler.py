# autosclaer/scaler.py
import docker
from docker.types import containers

# Environment
WORKER_IMAGE = "ray-autoscaler-node"  # docker image
RAY_HEAD_ADDRESS = "ray-head:6379"  # Ray head for workers to join
NETWORK_NAME = "ray-autoscaler-net"  # docker network for ray nodes


def scale_up(worker_id: int) -> None:
    "Add worker node to the network by creating the nodes from docker image."
    client = docker.from_env()
    container_name = f"ray-worker-{worker_id}"

    client.containers.run(
        image=WORKER_IMAGE,
        name=container_name,
        network=NETWORK_NAME,
        command=f"ray start --address={RAY_HEAD_ADDRESS} --block",  # Add block to keep container alive.
        detach=True,
    )

    print(f"[scaler] Worker {worker_id} say hello.")


def scale_down(worker_id: int) -> None:
    "Remove woker node from the network by stop container and delete them."
    client = docker.from_env()
    container_name = f"ray-worker-{worker_id}"

    container = client.containers.get(container_name)
    container.exec_run(cmd="ray stop ")
    container.stop()
    container.remove()
    print(f"[scaler] Worker {worker_id} are buried.")
