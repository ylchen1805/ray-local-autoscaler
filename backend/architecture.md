# Backend System Architecture

## Actor Interaction: OrderManager, OrderActor, and DriverPool

### Ownership and Data Responsibility

```mermaid
flowchart TB
    subgraph OM["OrderManager  (detached, lifetime=detached)"]
        OM_orders["orders: Dict[order_id → Order]\n— source of truth for all order state"]
        OM_events["events: deque[Event]\n— bounded log driving SSE"]
        OM_handles["actor_handles: Dict[order_id → {actor_handle, run_ref}]\n— used to kill actors on complete or cancel"]
    end

    subgraph OA["OrderActor  (one per order, num_cpus=1)"]
        OA_state["local status: TaskStatus\n— drives the transition loop"]
        OA_driver["_driver: Driver | None\n— held for the duration of the trip"]
        OA_mgr["manager handle\n— passed in at construction"]
        OA_dp["_driver_pool handle\n— looked up via ray.get_actor()"]
    end

    subgraph DP["DriverPool  (detached, lifetime=detached)"]
        DP_idle["idle: list[Driver]\n— available drivers"]
        DP_busy["busy: Dict[driver_id → Driver]\n— drivers on active trips"]
    end

    OM -- "spawns / kills" --> OA
    OA -- "register_worker\nset_trip\nupdate_status" --> OM
    OA -- "acquire()\nrelease()" --> DP
```

**Who owns what:**

| Actor | State it owns | Created by |
|---|---|---|
| `OrderManager` | All `Order` objects, the event log, actor handle registry | FastAPI lifespan (or reconnected on restart) |
| `OrderActor` | Its own local status copy; the `Driver` it holds during a trip | `OrderManager.create_order()` |
| `DriverPool` | The idle and busy driver pools | FastAPI lifespan (or `OrderActor.__init__` as fallback) |

---

### Normal Order Lifecycle

Solid arrows (`->>`) are fire-and-forget `.remote()` calls. Arrows with `+`/`-` activation bars are blocking `ray.get(x.remote())` calls that suspend the caller until the result is ready.

```mermaid
sequenceDiagram
    participant OM as OrderManager
    participant OA as OrderActor
    participant DP as DriverPool

    Note over OM: create_order() called by FastAPI
    OM->>OM: store Order, update_status(PENDING), _append_event
    OM->>OA: OrderActor.remote(order_id, self_handle)
    OM->>OA: run.remote()  — stores run_ref, does not block
    OM-->>OM: return order_id to caller

    Note over OA: Executing on a Ray worker node
    OA->>OM: register_worker.remote(node_id)
    OA->>OA: burn_cpu(matching delay)

    OA->>+DP: ray.get(acquire.remote())
    Note right of OA: OA blocks here until a driver is available
    DP->>DP: pop from idle list, add to busy dict
    DP-->>-OA: Driver{driver_id, name, rating, plate}

    OA->>+OM: ray.get(set_trip.remote(order_id, trip))
    Note right of OA: OA blocks until OM has stored the trip
    OM->>OM: orders[order_id].trip = TripInfo(...)
    OM-->>-OA: ok

    OA->>OM: update_status.remote(MATCHING)
    OM->>OM: _append_event(MATCHING)

    OA->>OA: burn_cpu(driver_assigned delay)
    OA->>OM: update_status.remote(DRIVER_ASSIGNED)
    OM->>OM: _append_event(DRIVER_ASSIGNED)

    OA->>OA: burn_cpu(on_trip delay)
    OA->>OM: update_status.remote(ON_TRIP)
    OM->>OM: _append_event(ON_TRIP)

    OA->>OA: burn_cpu(completed delay)
    OA->>OM: update_status.remote(COMPLETED)
    OM->>OM: _append_event(COMPLETED)
    OM->>OM: _archive_order()

    Note over OM: _archive_order blocks OM's message queue<br/>while waiting for OA.run() to return
    OM->>+OA: ray.get(run_ref)

    OA->>DP: release.remote(driver_id)
    DP->>DP: move driver back to idle list
    OA-->>-OM: run() returns

    OM->>OA: ray.kill(handle)
```

**Key blocking points:**

- `ray.get(DriverPool.acquire.remote())` in `OrderActor.run()` — OA suspends until a driver is available. If all drivers are busy this is where OA waits.
- `ray.get(manager.set_trip.remote(...))` in `OrderActor.run()` — OA waits to confirm trip info is saved before broadcasting MATCHING status.
- `ray.get(run_ref)` in `OrderManager._archive_order()` — OM's actor thread is blocked while OA finishes its last statements (`release.remote()`). During this window OM cannot process other incoming messages.

---

### Cancel Flow

Cancel can only happen when order status is `pending` or `matching`. The actor is killed immediately — no graceful shutdown.

```mermaid
sequenceDiagram
    participant API as FastAPI
    participant OM as OrderManager
    participant OA as OrderActor
    participant DP as DriverPool

    API->>OM: cancel_order.remote(order_id)
    OM->>OM: validate status ∈ {PENDING, MATCHING}
    OM->>OM: actor_handles.pop(order_id)
    OM->>OA: ray.kill(handle)
    Note over OA: SIGKILL — run() is interrupted mid-execution.<br/>If killed during acquire(), the driver remains<br/>in DriverPool's busy dict and is never released.

    OM->>OM: update_status(CANCELLED), _append_event(CANCELLED)
    OM-->>API: {order_id, status: "cancelled"}
```
