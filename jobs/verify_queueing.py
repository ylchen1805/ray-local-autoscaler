# jobs/verfy_queueing.py

import ray

ray.init()


@ray.remote(num_cpus=999)
def block():
    pass


block.remote()
