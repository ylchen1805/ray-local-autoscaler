# 前端 API 規格

> 給前端與後端 API server 成員對齊使用。  
> 本文件定義前端會呼叫的 REST API、WebSocket 連線方式、request/response 格式。  
> 專案情境為 Uber-like 叫車系統 + Ray Admin 儀表板。  


---

## 0. 共用規範

| 項目 | 規格 |
| --- | --- |
| Base URL | `http://localhost:8000` |
| Content-Type | `application/json` |
| 時間格式 | ISO 8601，例如 `"2025-05-30T14:23:00Z"` |
| 錯誤格式 | `{ "error": "message" }` |
| WebSocket URL | `ws://localhost:8000/ws` |

### 訂單狀態

| Status | 系統意義 | 使用者畫面文字 |
| --- | --- | --- |
| `pending` | Ray actor 等待排程。 | 配對中... |
| `matching` | Actor 開始執行，正在尋找司機。 | 正在尋找司機 |
| `driver_assigned` | 已配對司機。 | 司機前往中 |
| `on_trip` | 行程進行中。 | 行程中 |
| `completed` | 行程或任務完成。 | 行程完成 |
| `failed` | 行程或任務失敗。 | 叫車失敗 |
| `cancelled` | 行程或任務被取消。 | 已取消 |

Demo 階段暫不實作 `driver_arrived`。

---

## 1. 使用者叫車介面 API

### 1.1 查詢預估等待時間

使用時機：使用者進入叫車首頁時，用來顯示「約 N 分鐘」等待時間。

```text
GET /cluster/eta
```

Request body：無

Response 200：

```json
{
  "pending_tasks": 5,
  "worker_count": 2,
  "estimated_wait_seconds": 90,
  "surge": false
}
```

欄位說明：

| 欄位 | 型別 | 說明 |
| --- | --- | --- |
| `pending_tasks` | integer | 目前等待中的 order/task 數量。 |
| `worker_count` | integer | 目前 Ray worker 數量。 |
| `estimated_wait_seconds` | integer | 後端換算出的預估等待秒數。 |
| `surge` | boolean | 是否為尖峰狀態，前端可顯示尖峰提示。 |

錯誤回應：

```json
{ "error": "failed to get eta" }
```

---

### 1.2 建立叫車訂單

使用時機：使用者按下確認叫車按鈕。

```text
POST /orders
```

Request body：

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

欄位說明：

| 欄位 | 型別 | 必填 | 說明 |
| --- | --- | --- | --- |
| `order_type` | string | 是 | 叫車訂單固定使用 `"ride"`。 |
| `payload.origin` | string | 是 | 上車地點名稱。 |
| `payload.destination` | string | 是 | 下車地點名稱。 |
| `payload.origin_lat` | number | 是 | 上車地點緯度。 |
| `payload.origin_lng` | number | 是 | 上車地點經度。 |
| `payload.destination_lat` | number | 是 | 下車地點緯度。 |
| `payload.destination_lng` | number | 是 | 下車地點經度。 |
| `payload.ride_type` | string | 是 | `"standard"` 或 `"premium"`。 |

Response 201：

```json
{
  "order_id": "order-uuid-1234",
  "order_type": "ride",
  "status": "pending",
  "created_at": "2025-05-30T14:23:00Z"
}
```

後端在建立並註冊 order 後應立即回傳，不等待 Ray actor 完整跑完。

錯誤回應：

```json
{ "error": "invalid ride payload" }
```

```json
{ "error": "failed to create order" }
```

---

### 1.3 取得單一叫車訂單詳情

使用時機：使用者叫車流程頁面、Admin 點擊單筆訂單詳情時使用。

```text
GET /orders/{order_id}
```

Path params：

| 參數 | 型別 | 說明 |
| --- | --- | --- |
| `order_id` | string | `POST /orders` 回傳的訂單 ID。 |

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

Response 404：

```json
{ "error": "order not found" }
```

---

## 2. Admin Dashboard API

### 2.1 取得訂單列表

使用時機：Ray Admin 儀表板的訂單 / task 表格。

```text
GET /orders
```

Query params：

| 參數 | 型別 | 必填 | 說明 |
| --- | --- | --- | --- |
| `status` | string | 否 | 依狀態篩選。 |
| `limit` | integer | 否 | 回傳筆數上限，預設 `50`。 |

Response 200：

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

### 2.2 取得 Cluster 狀態

使用時機：Ray Admin 儀表板顯示 worker、CPU、pending tasks、autoscaler 狀態。

```text
GET /cluster/status
```

Response 200：

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

### 2.3 取得 Scaling 歷史紀錄

使用時機：Ray Admin 儀表板顯示 autoscaler scale up / scale down 紀錄。

```text
GET /cluster/scaling-history
```

Query params：

| 參數 | 型別 | 必填 | 說明 |
| --- | --- | --- | --- |
| `limit` | integer | 否 | 回傳事件數量上限，預設 `20`。 |

Response 200：

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

## 3. WebSocket API

使用者叫車介面與 Ray Admin 儀表板共用同一條 WebSocket。前端依照 `event`
欄位判斷事件類型。

```text
WS ws://localhost:8000/ws
```

Client 可選擇送出訂閱訊息：

```json
{ "action": "subscribe", "channel": "orders" }
```

```json
{ "action": "subscribe", "channel": "cluster" }
```

### 3.1 Order Updated

order 狀態變更時推送。

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

`completed` 狀態需帶上行程結果：

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

### 3.2 Cluster Updated

autoscaler 新增或移除 worker 時推送。

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

### 3.3 Heartbeat

每 5 秒推送一次，供 Admin Dashboard 更新摘要資訊。

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

## 4. 前端頁面與 API 對照

| 介面 | 頁面 / 區塊 | API | 更新方式 |
| --- | --- | --- | --- |
| 使用者 | 叫車首頁 | `GET /cluster/eta` | 頁面載入時呼叫一次 |
| 使用者 | 確認叫車 | `POST /orders` | REST |
| 使用者 | 配對中畫面 | `WS order_updated` | WebSocket push |
| 使用者 | 司機前往中畫面 | `WS order_updated` | WebSocket push |
| 使用者 | 行程中畫面 | `WS order_updated` | WebSocket push |
| 使用者 | 行程完成畫面 | `WS order_updated` | WebSocket push |
| Admin | 訂單列表 | `GET /orders` | 初始載入 + WebSocket push |
| Admin | Cluster 狀態 | `GET /cluster/status` | 初始載入 + heartbeat |
| Admin | Scaling 歷史 | `GET /cluster/scaling-history` | 定時 poll 或手動刷新 |
