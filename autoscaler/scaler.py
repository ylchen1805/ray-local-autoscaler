# autosclaer/scaler.py
import docker

# Environment
WORKER_IMAGE = "ray-autoscaler-node"  # docker image
RAY_HEAD_ADDRESS = "ray-head:6379"  # Ray head for workers to join
NETWORK_NAME = "ray-autoscaler-net"  # docker network for ray nodes


def scale_up(worker_id: int) -> None:
    "Add worker node to the network by creating the nodes from docker image."
    pass


def scale_down(worker_id: int) -> None:
    "Remove woker node from the network by stop container and delete them."
    pass
