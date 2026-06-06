# API Folder 導覽

> 本文件說明 `api/` 目錄裡各個檔案的角色、彼此關係，以及前端與後端
> server 成員應該如何閱讀這幾份程式。  
> 真正的 API 規格請看 [frontend_api.md](frontend_api.md)。  
> 後端 server 需要實作的 function contract 請看 [backend_api.md](backend_api.md)。

---

## 1. 目前 API Folder 的定位

`api/` 目前是一個 **FastAPI mock API server**。

它的目的不是完成最終 Ray backend，而是先提供：

```text
1. 前端可以呼叫的 HTTP / WebSocket API
2. request / response 的資料格式
3. 給後端 server 成員參考的 route 與 function 接線方式
4. 在 Ray OrderManager 完成前可測試的 mock 行為
```

目前資料流可以理解成：

```text
Frontend
  -> api/main.py
  -> api/order_manager.py
  -> api/models.py 定義的 response
  -> Frontend
```

未來接上真正後端後，會變成：

```text
Frontend
  -> api/main.py
  -> Backend Service Function
  -> Ray OrderManager / Autoscaler Monitor
  -> api/models.py 定義的 response
  -> Frontend
```

---

## 2. 檔案關係總覽

```text
api/
├── __init__.py
├── main.py
├── models.py
└── order_manager.py
```

| 檔案 | 角色 | 誰主要會看 |
| --- | --- | --- |
| `api/main.py` | 定義 API route，接收前端 request，呼叫後端 function。 | 前端、後端 server |
| `api/models.py` | 定義 request / response schema，也就是資料格式。 | 前端、後端 server |
| `api/order_manager.py` | 暫時 mock 後端邏輯，之後會被真正 Ray / backend service 取代。 | 後端 server |
| `api/__init__.py` | 讓 Python 把 `api/` 視為 package。 | 通常不用看 |

---

## 3. `api/main.py`

`main.py` 是 **API route 層**。

它負責：

```text
1. 建立 FastAPI app
2. 設定 CORS
3. 定義 HTTP endpoint
4. 定義 WebSocket endpoint
5. 接收 request
6. 呼叫 manager / backend service function
7. 將結果回傳給前端
```

### 3.1 `app = FastAPI(...)`

```python
app = FastAPI(...)
```

這是建立 FastAPI 應用程式物件。下面所有 `@app.get`、`@app.post`、
`@app.websocket` 都是把 route 掛到這個 `app` 上。

`title`、`version`、`description` 主要用於 FastAPI 自動產生的 Swagger UI。
它們不是前端頁面，也不是測試網頁，只是 API 文件 metadata。

### 3.2 `@app.get` / `@app.post` 是什麼

例如：

```python
@app.get("/orders/{order_id}")
def get_order(order_id: str):
    ...
```

意思是：

```text
當前端呼叫 GET /orders/{order_id}
FastAPI 會執行 get_order(order_id)
```

`@app.get(...)` 括號裡定義的是 API route 的對外契約，例如：

```text
1. API path
2. 成功 response model
3. 可能的錯誤 response model
4. HTTP status code
```

function 裡面才是實際要執行的事情，例如呼叫：

```python
manager.get_order(order_id)
```

### 3.3 `main.py` 目前有哪些 API

| API | 對應 function | 用途 |
| --- | --- | --- |
| `GET /health` | `health()` | 健康檢查。 |
| `POST /orders` | `create_order()` | 使用者建立叫車訂單。 |
| `GET /orders` | `list_orders()` | Admin 取得訂單列表。 |
| `GET /orders/{order_id}` | `get_order()` | 取得單筆訂單詳情。 |
| `POST /orders/{order_id}/cancel` | `cancel_order()` | 使用者取消仍在配對中的訂單。 |
| `GET /cluster/status` | `get_cluster_status()` | Admin 取得 cluster 狀態。 |
| `GET /cluster/eta` | `get_eta()` | 使用者取得預估等待時間。 |
| `GET /cluster/scaling-history` | `get_scaling_history()` | Admin 取得 autoscaler 歷史。 |
| `WS /ws` | `websocket_endpoint()` | WebSocket heartbeat / event 推送。 |

### 3.4 哪些地方會接後端組員的 function

目前 `main.py` 會呼叫 mock manager：

```python
manager.create_order(...)
manager.cancel_order(...)
manager.list_orders(...)
manager.get_order(...)
manager.get_cluster_status()
manager.get_eta()
manager.get_scaling_history(...)
manager.heartbeat()
```

這些就是未來要換成後端 server 成員實作 function 的地方。

例如現在是：

```python
return manager.create_order(request)
```

未來可以改成：

```python
return backend_service.create_order(
    order_type=request.order_type,
    payload=request.payload,
)
```

也就是：

```text
main.py 保留 API route
真正邏輯交給 backend service / Ray OrderManager
```

---

## 4. `api/models.py`

`models.py` 是 **API 資料格式層**。

它負責定義：

```text
1. 前端送進來的 request body 格式
2. 後端回給前端的 response body 格式
3. 欄位型別
4. 合法 enum 值
5. Swagger UI 會顯示的 schema
```

FastAPI 會根據 `models.py` 自動做 request 驗證與文件產生。

### 4.1 重要 model

| Model | 用途 |
| --- | --- |
| `TaskStatus` | 定義合法訂單狀態。 |
| `OrderPayload` | `POST /orders` 裡的叫車 payload。 |
| `CreateOrderRequest` | `POST /orders` 的 request body。 |
| `CreateOrderResponse` | `POST /orders` 成功後的簡短回應。 |
| `OrderResponse` | 單筆訂單完整詳情。 |
| `OrderListResponse` | `GET /orders` 的列表回應。 |
| `TripInfo` | 司機與行程資訊。 |
| `EtaResponse` | `GET /cluster/eta` 的回應。 |
| `ClusterStatusResponse` | `GET /cluster/status` 的回應。 |
| `ScalingHistoryResponse` | `GET /cluster/scaling-history` 的回應。 |
| `ErrorResponse` | 統一錯誤格式。 |

### 4.2 前端如何看 `models.py`

前端主要看：

```text
1. POST /orders 要送什麼欄位
2. API 會回什麼欄位
3. status 可能有哪些值
4. WebSocket event data 中會包含哪些資料
```

例如 `OrderPayload` 代表前端建立叫車訂單時要送：

```json
{
  "origin": "台北車站",
  "destination": "松山機場",
  "origin_lat": 25.0478,
  "origin_lng": 121.517,
  "destination_lat": 25.063,
  "destination_lng": 121.553,
  "ride_type": "standard"
}
```

### 4.3 後端如何看 `models.py`

後端 server 成員主要看：

```text
1. service function 回傳資料必須符合哪個 response model
2. Ray OrderManager 回傳資料需要轉成什麼格式
3. 錯誤要用什麼格式回給 main.py
```

例如 `get_order(order_id)` 最後需要能組出 `OrderResponse`。

---

## 5. `api/order_manager.py`

`order_manager.py` 目前是 **mock backend manager**。

它的目的：

```text
1. 在真正 Ray OrderManager 完成前，讓 API 可以先跑
2. 讓前端能先串接 request / response 格式
3. 模擬叫車狀態變化
4. 模擬 cluster status、ETA、scaling history
```

### 5.1 目前 mock 的狀態流程

```text
pending -> matching -> driver_assigned -> on_trip -> completed
```

例如：

```text
POST /orders
  -> 建立 pending order

GET /orders/{order_id}
  -> mock manager 依時間推進狀態
```

### 5.2 未來如何替換

未來真正後端完成後，`order_manager.py` 可以逐步被替換成：

```text
Backend Service
Ray OrderManager client
Autoscaler monitor adapter
Event broadcaster
```

也就是說，`order_manager.py` 不是最終核心，而是目前讓 API contract
可以被前端測試的暫時實作。

---

## 6. `api/__init__.py`

`__init__.py` 讓 Python 把 `api/` 當成 package。

有了它後，才能使用：

```bash
uv run uvicorn api.main:app --port 8000
```

通常不需要修改這個檔案。

---

## 7. 前後端應該如何使用這些檔案

### 7.1 前端成員

前端主要看：

```text
1. docs/frontend_api.md
2. api/models.py
3. Swagger UI: http://localhost:8000/docs
```

前端不需要理解 Ray，也不需要看 `order_manager.py` 的內部邏輯。

前端只需要知道：

```text
按鈕或頁面操作 -> 呼叫哪個 API -> 送什麼資料 -> 收什麼資料
```

例如：

```text
叫車首頁載入 -> GET /cluster/eta
按下確認叫車 -> POST /orders
按下取消訂單 -> POST /orders/{order_id}/cancel
訂單狀態更新 -> WS /ws 的 order_updated event
Admin 訂單列表 -> GET /orders
Admin cluster 卡片 -> GET /cluster/status
```

### 7.2 後端 server 成員

後端主要看：

```text
1. docs/backend_api.md
2. api/main.py
3. api/models.py
4. api/order_manager.py
```

後端要做的是：

```text
1. 依照 backend_api.md 實作 service functions
2. 讓這些 function 回傳符合 models.py 的資料格式
3. 將 main.py 目前呼叫 mock manager 的地方，接到真正後端 service
4. 串接 Ray OrderManager、autoscaler monitor、event broadcaster
```

### 7.3 你的 API 工作範圍

你的部分主要是：

```text
1. 將前端需求轉成 HTTP / WebSocket API 規格
2. 定義 request / response schema
3. 定義 route 對應哪些後端 function
4. 提供 mock API server 讓前端可以先串接
```

目前 `api/` 就是在支援這個工作範圍。

---

## 8. 取消訂單 API

取消訂單 API 讓使用者在訂單仍處於配對階段時停止後端 Order Actor，避免前端返回下單頁面後，後端仍繼續把同一筆訂單推進到 `completed`。

```text
POST /orders/{order_id}/cancel
```

Request body：無

成功回應：

```json
{
  "order_id": "order-uuid-1234",
  "status": "cancelled"
}
```

找不到訂單時：

```json
{ "error": "order not found" }
```

訂單狀態不可取消時：

```json
{ "error": "order cannot be cancelled from status: on_trip" }
```

可取消狀態：

```text
pending, matching
```

不可取消狀態：

```text
driver_assigned, on_trip, completed, failed, cancelled
```

取消成功後，後端應推送 `order_updated` 事件：

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

確認頁面的「返回修改」只做前端頁面切換，不呼叫取消 API，因為此階段尚未建立後端訂單。

---

## 9. 啟動方式

```bash
uv run uvicorn api.main:app --reload --port 8000
```

打開 Swagger UI：

```text
http://localhost:8000/docs
```

Swagger UI 是 FastAPI 自動產生的 API 文件頁，不是額外寫的前端網頁，
也不是需要推上 GitHub 的測試 UI。
