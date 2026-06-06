# 後端 Server Function 契約

> 給後端 API server 成員實作使用。  
> 本文件定義每個前端 API route 背後需要呼叫的 backend/service function，
> 以及 function 的輸入、輸出、錯誤格式與 Ray 整合方向。  
> HTTP/WebSocket API 格式請參考 [frontend_api.md](frontend_api.md)。

---

## 0. 架構邊界

```text
FastAPI Route
  -> Backend Service Function
  -> Ray OrderManager Actor / Autoscaler Monitor / Scaling History Store
  -> Response Model
```

後端 server 成員主要要實作本文件中的 service functions。API route 只負責：

```text
1. 接收 HTTP/WebSocket request
2. 驗證 request 格式
3. 呼叫對應的 backend service function
4. 將 function 回傳值轉成 JSON response 或 WebSocket event
```

---

## 1. 共用型別與錯誤規範

### 1.1 錯誤格式

service function 可以丟出 typed exception，或回傳錯誤物件；但 HTTP route
最後都要轉成以下格式：

```json
{ "error": "message" }
```

建議 HTTP status code 對應：

| 錯誤情境 | HTTP Status |
| --- | --- |
| request payload 格式錯誤 | `400 Bad Request` |
| 找不到 order | `404 Not Found` |
| 訂單目前狀態不可執行該操作 | `409 Conflict` |
| 後端或 Ray 操作失敗 | `500 Internal Server Error` |

### 1.2 RidePayload

`POST /orders` 使用的 payload 型別：

```python
class RidePayload:
    origin: str
    destination: str
    origin_lat: float
    origin_lng: float
    destination_lat: float
    destination_lng: float
    ride_type: Literal["standard", "premium"]
```

### 1.3 OrderDetail

`create_order`、`get_order`、`list_orders` 共用的訂單輸出格式：

```python
class OrderDetail:
    order_id: str
    order_type: str
    status: str
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    worker_node: str | None
    payload: dict
    trip: dict | None
    result: dict | None
```

允許的 `status`：

```text
pending, matching, driver_assigned, on_trip, completed, failed, cancelled
```

---

## 2. Route 與 Function 對照

| Frontend API | 後端需實作的 function |
| --- | --- |
| `GET /cluster/eta` | `get_eta()` |
| `POST /orders` | `create_order(order_type, payload)` |
| `POST /orders/{order_id}/cancel` | `cancel_order(order_id)` |
| `GET /orders/{order_id}` | `get_order(order_id)` |
| `GET /orders` | `list_orders(status=None, limit=50)` |
| `GET /cluster/status` | `get_cluster_status()` |
| `GET /cluster/scaling-history` | `get_scaling_history(limit=20)` |
| `GET /sse` | `sse_endpoint(channel=None)` |

---

## 3. 使用者叫車相關 Functions

### 3.1 `get_eta()`

對應 API：

```text
GET /cluster/eta
```

用途：

根據 Ray pending queue 與 worker 數量，換算使用者介面顯示的預估等待時間。

Function signature：

```python
def get_eta() -> EtaResponse:
    ...
```

輸入：

無。

內部資料來源：

```text
autoscaler/monitor.py 的 get_cluster_status()
或後端自行封裝的 cluster monitor service
```

輸出：

```python
class EtaResponse:
    pending_tasks: int
    worker_count: int
    estimated_wait_seconds: int
    surge: bool
```

範例輸出：

```json
{
  "pending_tasks": 5,
  "worker_count": 2,
  "estimated_wait_seconds": 90,
  "surge": false
}
```

建議換算邏輯：

```text
estimated_wait_seconds = pending_tasks / max(worker_count, 1) * average_ride_seconds
surge = pending_tasks >= surge_threshold
```

失敗時回傳：

```json
{ "error": "failed to get eta" }
```

---

### 3.2 `create_order(order_type, payload)`

對應 API：

```text
POST /orders
```

用途：

建立叫車訂單，註冊到 Ray OrderManager，並立即回傳 `pending` 狀態。

Function signature：

```python
def create_order(order_type: str, payload: RidePayload) -> CreateOrderResult:
    ...
```

輸入：

```json
{
  "order_type": "ride",
  "payload": {
    "origin": "台北車站",
    "destination": "松山機場",
    "origin_lat": 25.0478,
    "origin_lng": 121.517,
    "destination_lat": 25.063,
    "destination_lng": 121.553,
    "ride_type": "standard"
  }
}
```

驗證規則：

| 欄位 | 規則 |
| --- | --- |
| `order_type` | 使用者叫車介面固定為 `"ride"`。 |
| `origin`, `destination` | 不可為空字串。 |
| `origin_lat`, `destination_lat` | 必須是合法緯度。 |
| `origin_lng`, `destination_lng` | 必須是合法經度。 |
| `ride_type` | 只能是 `"standard"` 或 `"premium"`。 |

後端預期行為：

```text
1. 產生 order_id。
2. 建立 pending 狀態的 order。
3. 呼叫 Ray OrderManager 建立並註冊 order。
4. 觸發 Ray actor 非同步執行。
5. 立即回傳，不等待整趟行程完成。
```

輸出：

```python
class CreateOrderResult:
    order_id: str
    order_type: str
    status: Literal["pending"]
    created_at: datetime
```

範例輸出：

```json
{
  "order_id": "order-uuid-1234",
  "order_type": "ride",
  "status": "pending",
  "created_at": "2025-05-30T14:23:00Z"
}
```

副作用：

```text
Ray OrderManager 會建立 order actor。
後續狀態變化會透過 event broadcaster 推送給 WebSocket client。
```

失敗時回傳：

```json
{ "error": "invalid ride payload" }
```

```json
{ "error": "failed to create order" }
```

---

### 3.3 `cancel_order(order_id)`

對應 API：

```text
POST /orders/{order_id}/cancel
```

用途：

取消使用者主動停止、且仍在配對階段的訂單。後端必須在同一次取消操作中停止對應的 Ray Order Actor，並將訂單狀態更新為 `cancelled`，避免 Actor 後續繼續把訂單推進到 `driver_assigned`、`on_trip` 或 `completed`。

Function signature：

```python
def cancel_order(order_id: str) -> CancelOrderResult:
    ...
```

可取消狀態：

```text
pending, matching
```

不可取消狀態：

```text
driver_assigned, on_trip, completed, failed, cancelled
```

未來若要允許取消 `driver_assigned` 狀態，後端還需要將已配對的司機釋放回 Driver Pool。

後端預期行為：

```text
1. 查詢 order 是否存在。
2. 驗證目前狀態只能是 pending 或 matching。
3. 使用 ray.kill 停止對應的 Ray Order Actor。
4. 從 actor_handles 移除對應的 Actor handle。
5. 將訂單狀態更新為 TaskStatus.CANCELLED。
6. 使用既有狀態時間紀錄邏輯保存 cancelled 時間。
7. 新增取消事件，讓既有 SSE polling 流程推送 cancelled 狀態。
8. 回傳取消結果。
```

輸出：

```python
class CancelOrderResult:
    order_id: str
    status: Literal["cancelled"]
```

Response 200 範例：

```json
{
  "order_id": "order-uuid-1234",
  "status": "cancelled"
}
```

失敗時回傳：

```json
{ "error": "order not found" }
```

```json
{ "error": "order cannot be cancelled from status: on_trip" }
```

取消成功後，後端必須透過 SSE 推送：

```json
{
  "event": "order_updated",
  "data": {
    "order_id": "order-uuid-1234",
    "status": "cancelled",
    "updated_at": "2026-06-05T14:23:10Z"
  }
}
```

---

### 3.4 `get_order(order_id)`

對應 API：

```text
GET /orders/{order_id}
```

用途：

回傳單筆 order 的最新狀態，供使用者介面和 Admin 介面查詢。

Function signature：

```python
def get_order(order_id: str) -> OrderDetail:
    ...
```

輸入：

| 參數 | 型別 | 說明 |
| --- | --- | --- |
| `order_id` | string | `create_order` 回傳的 order ID。 |

資料來源：

```text
Ray OrderManager.get_order(order_id)
或後端同步 Ray event 後維護的 order state cache
```

輸出：

```json
{
  "order_id": "order-uuid-1234",
  "order_type": "ride",
  "status": "driver_assigned",
  "created_at": "2025-05-30T14:23:00Z",
  "started_at": "2025-05-30T14:23:02Z",
  "completed_at": null,
  "worker_node": "ray-worker-1",
  "payload": {
    "origin": "台北車站",
    "destination": "松山機場",
    "origin_lat": 25.0478,
    "origin_lng": 121.517,
    "destination_lat": 25.063,
    "destination_lng": 121.553,
    "ride_type": "standard"
  },
  "trip": {
    "driver_id": "driver-007",
    "driver_name": "王大明",
    "driver_rating": 4.8,
    "license_plate": "ABC-1234",
    "estimated_arrival": 5,
    "estimated_duration": 18,
    "fare_estimate": 320
  },
  "result": null
}
```

失敗時回傳：

```json
{ "error": "order not found" }
```

---

## 4. Admin Dashboard 相關 Functions

### 4.1 `list_orders(status=None, limit=50)`

對應 API：

```text
GET /orders
```

用途：

回傳 Admin Dashboard 的 order/task 列表。

Function signature：

```python
def list_orders(status: str | None = None, limit: int = 50) -> OrderListResult:
    ...
```

輸入：

| 參數 | 型別 | 必填 | 說明 |
| --- | --- | --- | --- |
| `status` | string | 否 | 依狀態篩選。 |
| `limit` | int | 否 | 回傳數量上限。 |

輸出：

```python
class OrderListResult:
    orders: list[OrderDetail]
    total: int
```

範例輸出：

```json
{
  "orders": [
    {
      "order_id": "order-uuid-1234",
      "order_type": "ride",
      "status": "matching",
      "created_at": "2025-05-30T14:23:00Z",
      "started_at": "2025-05-30T14:23:02Z",
      "completed_at": null,
      "worker_node": "ray-worker-1",
      "payload": {
        "origin": "台北車站",
        "destination": "松山機場",
        "ride_type": "standard"
      },
      "trip": null,
      "result": null
    }
  ],
  "total": 12
}
```

---

### 4.2 `get_cluster_status()`

對應 API：

```text
GET /cluster/status
```

用途：

回傳 Ray head、worker nodes、pending queue、CPU 使用率與 autoscaler 狀態快照。

Function signature：

```python
def get_cluster_status() -> ClusterStatus:
    ...
```

輸入：

無。

資料來源：

```text
autoscaler/monitor.py get_cluster_status()
autoscaler policy/scaler state
Ray Dashboard API
```

輸出：

```python
class ClusterStatus:
    head_node: HeadNode
    workers: list[WorkerNode]
    worker_count: int
    pending_tasks: int
    cpu_usage: CpuUsage
    autoscaler: AutoscalerStatus
```

範例輸出：
 
```json
{
  "head_node": {
    "ip": "172.18.0.2",
    "status": "alive"
  },
  "workers": [
    {
      "node_id": "ray-worker-1",
      "ip": "172.18.0.3",
      "status": "alive",
      "cpu_used": 0.72,
      "cpu_total": 1.0
    }
  ],
  "worker_count": 2,
  "pending_tasks": 5,
  "cpu_usage": {
    "used": 1.44,
    "total": 2.0,
    "percent": 0.72
  },
  "autoscaler": {
    "min_workers": 0,
    "max_workers": 5,
    "cooldown_remaining": 8,
    "last_scaled_at": "2025-05-30T14:20:00Z",
    "last_action": "scale_up"
  }
}
```

---

### 4.3 `get_scaling_history(limit=20)`

對應 API：

```text
GET /cluster/scaling-history
```

用途：

回傳最近 autoscaler scale up / scale down 事件。

Function signature：

```python
def get_scaling_history(limit: int = 20) -> ScalingHistoryResult:
    ...
```

輸入：

| 參數 | 型別 | 必填 | 說明 |
| --- | --- | --- | --- |
| `limit` | int | 否 | 回傳事件數量上限。 |

輸出：

```python
class ScalingHistoryResult:
    history: list[ScalingHistoryItem]
```

範例輸出：

```json
{
  "history": [
    {
      "timestamp": "2025-05-30T14:20:00Z",
      "action": "scale_up",
      "worker_id": "ray-worker-2",
      "trigger_reason": "pending_tasks=5, consecutive_polls=3"
    }
  ]
}
```

---

## 5. Event / SSE 相關 Functions

### 5.1 `sse_endpoint(channel=None)`

對應 API：

```text
GET /sse
```

用途：

建立 SSE (Server-Sent Events) 長連線，持續推送後端事件給前端。

Function signature：

```python
async def sse_endpoint(channel: str | None = None) -> StreamingResponse:
    ...
```

Client 透過 query parameter 選擇訂閱頻道（不指定則訂閱全部）：

```text
GET /sse                    → 訂閱 orders + cluster
GET /sse?channel=orders     → 只訂閱 order_updated 事件
GET /sse?channel=cluster    → 只訂閱 cluster_updated 事件
```

Server 會推送的事件（SSE `data:` 行，每則結尾 `\n\n`）：

```json
{
  "event": "order_updated",
  "data": {
    "order_id": "order-uuid-1234",
    "status": "driver_assigned",
    "trip": {
      "driver_name": "王大明",
      "driver_rating": 4.8,
      "license_plate": "ABC-1234",
      "estimated_arrival": 4
    },
    "updated_at": "2025-05-30T14:23:10Z"
  }
}
```

```json
{
  "event": "cluster_updated",
  "data": {
    "action": "scale_up",
    "worker_id": "ray-worker-2",
    "worker_count": 3,
    "pending_tasks": 2,
    "timestamp": "2025-05-30T14:23:10Z"
  }
}
```

```json
{
  "event": "heartbeat",
  "data": {
    "pending_tasks": 3,
    "worker_count": 2,
    "cpu_percent": 0.65
  }
}
```

---

### 5.2 `publish_order_update(order_id, status, trip=None, result=None)`

呼叫來源：

```text
Ray OrderManager callback 或 backend event bridge（poll_order_events 背景任務）
```

用途：

把 order 狀態變更包成 `order_updated` event，推送給已連線的 SSE clients。

Function signature：

```python
async def publish_order_update(
    order_id: str,
    status: str,
    trip: dict | None = None,
    result: dict | None = None,
) -> None:
    ...
```

輸入：

| 參數 | 型別 | 說明 |
| --- | --- | --- |
| `order_id` | string | 更新的 order ID。 |
| `status` | string | 新狀態。 |
| `trip` | dict or null | `driver_assigned` 狀態時使用的司機/行程資訊。 |
| `result` | dict or null | `completed` 狀態時使用的行程結果。 |

輸出：

無回傳值。此 function 的結果是將事件放入所有訂閱 `"orders"` 頻道的 SSE client queue。

---

### 5.3 `publish_cluster_update(action, worker_id, worker_count, pending_tasks)`

呼叫來源：

```text
Autoscaler scale up / scale down 後
```

用途：

把 cluster 變更包成 `cluster_updated` event，推送給已連線的 SSE clients。

Function signature：

```python
async def publish_cluster_update(
    action: str,
    worker_id: str | None,
    worker_count: int,
    pending_tasks: int,
) -> None:
    ...
```

輸出：

無回傳值。此 function 的結果是將事件放入所有訂閱 `"cluster"` 頻道的 SSE client queue。

---

## 6. Ray 整合備註

### 6.1 OrderManager Actor 需求

Ray `OrderManager` actor 建議提供等價行為：

```python
@ray.remote
class OrderManager:
    def create_order(self, order_type: str, payload: dict) -> dict:
        ...

    def cancel_order(self, order_id: str) -> dict:
        ...

    def get_order(self, order_id: str) -> dict | None:
        ...

    def list_orders(self, status: str | None = None, limit: int = 50) -> dict:
        ...

    def on_order_update(self, order_id: str, status: str, data: dict) -> None:
        ...
```

### 6.2 狀態更新流程

```text
1. API route 收到 POST /orders。
2. API route 呼叫 create_order(order_type, payload)。
3. Backend service 呼叫 Ray OrderManager.create_order()。
4. OrderManager 註冊 pending order 並建立 Ray actor。
5. Actor 狀態依序變化：
   pending -> matching -> driver_assigned -> on_trip -> completed
6. Actor 或 OrderManager 產生 update event。
7. Backend event bridge 呼叫 publish_order_update()。
8. 前端收到 SSE order_updated。
```

### 6.3 取消訂單流程

```text
1. 前端只在已建立後端訂單後保存目前 order_id。
2. 使用者在配對頁面點擊「取消訂單」。
3. 前端呼叫 POST /orders/{order_id}/cancel。
4. API route 呼叫 cancel_order(order_id)。
5. Backend service 呼叫 Ray OrderManager.cancel_order(order_id)。
6. OrderManager 驗證 order 狀態是否為 pending 或 matching。
7. OrderManager 使用 ray.kill 停止對應 OrderActor，並移除 actor handle。
8. OrderManager 將狀態更新為 cancelled，保存 cancelled 時間並新增 event。
9. Backend event bridge 透過 publish_order_update() 推送 cancelled。
10. 前端收到 API 成功或 SSE cancelled 後，返回下單頁面並清除目前 order_id。
```

確認頁面的「返回修改」不會呼叫取消 API，因為該階段前端尚未呼叫 `POST /orders`，後端不應存在對應訂單。
