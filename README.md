# Distributed System Final Project

Implement a resource-aware dynamic worker scaling system for ray.

## 1. Architecture

```mermaid
flowchart
    subgraph APP["Application Layer"]
        F["Frontend"]
        API["API"]
        S["Service"]
        SS["System Start"]

        F --> |Order| API
        API --> |Order status| F
        API <--> S 
    end

    subgraph INFRA["Infrastructure Layer"]
        RH["Ray Head Node"]
        RW["Ray Worker Nodes"]
        DB["Dashboard"]
        AS["Autoscaler"]

        RH <--> |Data| RW
        RH --> AS
        AS --> DB   
        AS --> |Add/ Remove wokers| RW
    end

    SS --> |Send order Manager| RH
    S <--> |Order Manager| RH
```


The [Autoscaler](/docs/autoscaler.md) runs as a separate process that continuously polls the Ray Dashboard API. Based on the configured scaling policy, it calls the Docker API to spin up or tear down worker containers.

## 2. Workflow 

### Monitor Service
```mermaid
sequenceDiagram
    participant F as Frontend
    participant API
    participant B as Backend
    participant RH as Ray head
    participant OM as Order manager

    B ->> RH : required actor( order manager)
    RH ->> B : actor( order manager)
    OM ->> B : all order states
    B ->> API : all order states
    API ->> F : all order states
 
```

### Order

```mermaid
sequenceDiagram
    participant F as Frontend
    participant API
    participant B as Backend
    participant RH as Ray head
    participant OM as Order manager

    F ->> API : send order
    API ->> B : order
    B ->> OM: create order
    OM -->> OM : Register order
    OM ->> RH: create actor
    RH -->> RH : schedule
    
    OM ->> RH : update state
    RH ->> OM : actor state
    OM ->> B : order status
    B ->> API : order state
    API ->> F : order state
```


## Getting Started

### Prerequisites
- Docker
- Python 3.11
- uv

### Install dependencies
```bash
uv sync --all-groups
```

### Update Docker dependencies
Whenever `pyproject.toml` changes, re-export before rebuilding:
```bash
uv export --no-hashes --no-group local -o docker-requirements.txt
```

### Start the cluster
```bash
docker compose up --build
```

### Start the autoscaler
```bash
uv run python -m autoscaler.main
```

### Start backend server
```bash
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

### Submit a test job
```bash
uv run ray job submit \
  --address http://localhost:8265 \
  --working-dir . \
  -- python jobs/heavy_task.py
```
