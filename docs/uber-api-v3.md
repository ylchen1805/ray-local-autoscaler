# Frontend ↔ Backend API Specification v2.1
# 使用者叫車介面 + Ray 儀表板整合版

> v2.1 更新：修正使用者介面的 API 範圍、補齊 on_trip / completed WS event、移除 driver_arrived 暫不實作、修正系統串接全景圖。

---

## 系統串接全景

```
[使用者介面]                    [Ray Admin 儀表板]
     │                                │
     ├─ GET /cluster/eta              ├─ GET /orders
     ├─ POST /orders                  ├─ GET /cluster/status
     ├─ GET /orders/{id}              ├─ GET /cluster/scaling-history
     └─ WS /ws                        └─ WS /ws
          └─ order_updated                 └─ cluster_updated / heartbeat

兩個前端共用同一個後端，WebSocket 同一條連線，用 event type 區分。
使用者介面不直接呼叫 /cluster/status，只透過 /cluster/eta 拿換算後的等待時間。
```

---

## 使用者叫車介面 API

### POST /orders（建立叫車訂單）

```json
{
  "order_type": "ride",
  "payload": {
    "origin": "台北車站",
    "destination": "松山機場",
    "origin_lat": 25.0478,
    "origin_lng": 121.5170,
    "destination_lat": 25.0630,
    "destination_lng": 121.5530,
    "ride_type": "standard"
  }
}
```

Response 201：
```json
{
  "order_id": "order-uuid-1234",
  "order_type": "ride",
  "status": "pending",
  "created_at": "2025-05-30T14:23:00Z"
}
```

> 後端立即回傳 order_id，不等 Actor 執行完。前端收到後顯示「配對中」畫面，後續狀態靠 WebSocket 推送。

---

### GET /orders/{order_id}（取得訂單詳情）

Response 200：
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
    "destination": "松山機場"
  },
  "trip": {
    "driver_id": "driver-007",
    "driver_name": "王大明",
    "driver_rating": 4.8,
    "license_plate": "ABC-1234",
    "estimated_arrival": 5,
    "estimated_duration": 18,
    "fare_estimate": 320
  }
}
```

---

### GET /cluster/eta（預估等待時間，使用者介面專用）

> 只在叫車頁首頁載入時呼叫一次，用來顯示右上角「約 N 分鐘」badge。
> 配對中、行程中畫面不使用此 API，也不顯示任何 Ray 內部狀態。

```
GET /cluster/eta
```

Response 200：
```json
{
  "pending_tasks": 5,
  "worker_count": 2,
  "estimated_wait_seconds": 90,
  "surge": false
}
```

> 後端換算邏輯：`estimated_wait_seconds = pending_tasks / worker_count * 平均行程秒數`
> surge = true 時前端 badge 顯示「⚡ 尖峰時段」

---

## 叫車狀態表

| Status | 對應 Ray Actor 狀態 | 使用者看到的文字 | 前端畫面 |
|---|---|---|---|
| `pending` | Actor 等待排程 | 配對中... | 配對中畫面（spinner）|
| `matching` | Actor 開始執行，尋找司機 | 正在尋找司機 | 配對中畫面 |
| `driver_assigned` | Actor 已配對司機 | 司機前往中 | 司機前往中畫面 |
| `on_trip` | Actor 追蹤行程中 | 行程中 | 行程中畫面（進度條）|
| `completed` | Actor 執行完成 | 行程完成 | 行程完成畫面 |
| `failed` | Actor 失敗 | 叫車失敗 | 錯誤提示 |
| `cancelled` | Actor 被中止 | 已取消 | 返回首頁 |

> `driver_arrived`（司機已抵達）需要 GPS 偵測，**暫不實作**，demo 階段跳過此狀態。

---

## WebSocket：Server → Client 推送格式

所有狀態變更都透過同一個 `order_updated` event 推送，前端根據 `status` 決定切換哪個畫面。

### A. pending → matching
```json
{
  "event": "order_updated",
  "data": {
    "order_id": "order-uuid-1234",
    "status": "matching",
    "updated_at": "2025-05-30T14:23:01Z"
  }
}
```

### B. matching → driver_assigned（含司機資訊）
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
    "updated_at": "2025-05-30T14:23:05Z"
  }
}
```

### C. driver_assigned → on_trip
```json
{
  "event": "order_updated",
  "data": {
    "order_id": "order-uuid-1234",
    "status": "on_trip",
    "updated_at": "2025-05-30T14:27:00Z"
  }
}
```

### D. on_trip → completed（含行程結果）
```json
{
  "event": "order_updated",
  "data": {
    "order_id": "order-uuid-1234",
    "status": "completed",
    "result": {
      "fare": 268,
      "duration_minutes": 18,
      "distance_km": 8.3
    },
    "updated_at": "2025-05-30T14:45:00Z"
  }
}
```

### E. Cluster 狀態變更（Admin 儀表板用）
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

### F. Heartbeat（每 5 秒，Admin 儀表板用）
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

## Admin 儀表板 API（維持不變）

| API | 用途 | 更新方式 |
|---|---|---|
| `GET /orders` | 所有訂單列表 | WebSocket `order_updated` |
| `GET /cluster/status` | Worker 節點狀態、CPU、autoscaler | WebSocket `heartbeat` |
| `GET /cluster/scaling-history` | Scaling 歷史紀錄 | 定時 poll |

---

## 前端頁面與 API 對照

| 介面 | 頁面/區塊 | API | 更新方式 |
|---|---|---|---|
| 使用者 | 叫車頁首頁 | `GET /cluster/eta` | 頁面載入時一次 |
| 使用者 | 確認叫車 | `POST /orders` | 一次性 REST |
| 使用者 | 配對中畫面 | `WS order_updated` (matching) | WebSocket push |
| 使用者 | 司機前往中畫面 | `WS order_updated` (driver_assigned + trip{}) | WebSocket push |
| 使用者 | 行程中畫面 | `WS order_updated` (on_trip) | WebSocket push |
| 使用者 | 行程完成畫面 | `WS order_updated` (completed + result{}) | WebSocket push |
| Admin | 訂單列表 | `GET /orders` | WebSocket push |
| Admin | Cluster 節點狀態 | `GET /cluster/status` | WebSocket heartbeat |
| Admin | Scaling 歷史 | `GET /cluster/scaling-history` | 定時 poll |

---

## CORS 設定（後端需要）

```python
# FastAPI
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)
```

---

*文件版本：v2.1 | 最後更新：2025-06-01*
