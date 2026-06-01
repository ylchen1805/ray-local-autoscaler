import os
import socket
import time
from collections import Counter

import ray

ray_address = os.environ.get("RAY_ADDRESS")

if ray_address:
    ray.init(address=ray_address)
else:
    ray.init()


@ray.remote
def work(i):
    host = socket.gethostname()
    pid = os.getpid()
    time.sleep(30)
    return {"task": i, "host": host, "pid": pid}


if __name__ == "__main__":
    print("Connected to Ray")
    print("Cluster nodes:")
    for n in ray.nodes():
        print(
            {
                "Alive": n["Alive"],
                "NodeManagerAddress": n["NodeManagerAddress"],
            }
        )

    refs = [work.remote(i) for i in range(60)]
    results = ray.get(refs)

    print("\nTask results:")
    for r in results:
        print(r)

    counts = Counter(r["host"] for r in results)
    print("\nTask distribution by host:")
    for host, cnt in counts.items():
        print(f"{host}: {cnt}")
