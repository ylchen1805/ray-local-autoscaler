# jobs/verify.py
import ray

ray.init()


@ray.remote
def hello(x):
    "Be a nice worker, say hello to your master"
    return f"This is task {x}. Worker say hello."


results = [hello.remote(i) for i in range(5)]
print([ray.get(results)])
