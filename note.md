# Note

Personal note for developing

## 1. Setting up docker and test it with ray

After `docker compose up`

### UI

Open the `http://localhost:8265`

### Check the container
### Check the network

Find the network's name, since it may have some prefix.

```shell
docker network ls | grep ray-net
```

### Display detailed information about the ray-net

Check that both head container and worker container are in the network.

```shell
docker network inspect <network name>
```

### Test the worker node to check their link 

```shell
docker exec ray-worker-1 python -c "import socket; print(socket.gethostbyname('ray-head'))"
```

## Submit job to the head

There's a known issue on [Github](https://github.com/ray-project/ray/issues?q=ray+client+SpecificServer+startup+failed), so using `Ray Job submission API` to submit jobs to head.

```shell 
uv run ray job submit \
  --address http://localhost:8265 \
  --working-dir . \
  -- python jobs/verify.py
```
