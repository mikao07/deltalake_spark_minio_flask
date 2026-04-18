## car_rental_flask_spark_delta

Flask 後端 + PySpark + Delta Lake：透過 **S3 相容 API（MinIO）** 讀寫資料、維護 Delta table，並提供網頁（Dashboard、上傳等）與 REST API。

### 專案結構（重點）

| 路徑 | 說明 |
|------|------|
| `app.py` | Flask 入口：路由、管理員 token、Delta 白名單、背景任務等 |
| `config.py` | 由環境變數讀取 MinIO/S3A、各層 Delta 路徑、OCR、上傳策略 |
| `services/spark_service.py` | SparkSession、Delta 讀寫、Silver/Gold ETL、系統狀態等 |
| `services/minio_upload.py` | MinIO SDK 上傳、`dataset_id` 目錄、同名物件策略 |
| `services/ocr_spark.py` | Bronze OCR：binaryFile → Tesseract → Delta |
| `services/async_jobs.py` | 記憶體內背景任務（長時間 Spark 工作先回 job_id） |
| `templates/` | `index.html`、`upload.html`、`layers.html`（首頁以詞頻與痛點主題快照為主） |
| `tests/` | `pytest`：API 驗證、MinIO 檔名邏輯（可不透過真 Spark/MinIO） |
| `.github/workflows/ci.yml` | Push／PR 至 `main`/`master` 時自動執行 `pytest`（GitHub Actions） |

### 資料管線（摘要）

- **Bronze**：`raw/images/{dataset_id}/` 影像 → OCR（Tesseract）→ Delta；可選擋掉 `OCR_ERROR_*` 不寫入；讀檔時限圖片副檔名與檔頭（非圖片不進 OCR）。
- **Silver**：OCR 文字去重、MERGE 至 `SILVER_OCR_TABLE_PATH`。
- **Gold**：Jieba 分詞、自訂辭典／停用詞（可依 `dataset_id` 綁定路徑）、詞頻表；**痛點主題**（規則式）寫入 **`GOLD_TOPIC_SNAPSHOT_PATH`** 快照表（可對照多個 `snapshot_at`）。
- **一鍵**：`POST /delta/pipeline/to-gold/run`（Bronze→Silver→Gold）；可設定 `skip_gold_if_no_new_ocr`（無新 OCR 時略過後段）。

### 功能（API 摘要）

- **健康檢查**：`GET /health`
- **系統狀態**：`GET /api/status`
- **Delta 預覽**：`POST /delta/read`（路徑須符合 `ALLOWED_DELTA_PATH_PREFIXES`）
- **Delta Upsert**：`POST /delta/upsert`（需 `ADMIN_TOKEN` 時帶 `X-Admin-Token`）
- **僅保留最新批次**：`POST /delta/cleanup-latest-only`（同上）
- **Silver OCR 預覽**：`GET /api/silver`、`GET /api/silver/ocr`
- **Gold 詞頻預覽**：`GET /api/gold/word-frequency`
- **金層詞頻 ETL**（Silver→Gold）：`POST /delta/gold/word-frequency/run`（body 或 **Query** 可補 `dataset_id`、`dry_run` 等）
- **痛點快照僅重建**（不寫詞頻表）：`POST /delta/gold/topic-snapshot/rebuild`（`dataset_id` 必填）
- **一鍵至金層**：`POST /delta/pipeline/to-gold/run`
- **Bronze OCR 攝入**：`POST /delta/ocr/bronze/run`
- **Silver OCR ETL**：`POST /delta/silver/ocr/run`
- **圖片上傳至 MinIO**：`POST /api/upload/images`（`multipart/form-data`，`dataset_id` 必填；可選 `run_ocr`；`MAX_UPLOAD_MB` 限制大小）
- **查詢 dataset_id**：`GET /api/datasets`（需 `ADMIN_TOKEN` 時）
- **背景任務**：`GET /api/jobs/<job_id>`（建立非同步 pipeline 時）
- **Storage 健康檢查**：`GET /api/health/storage`、`GET /api/debug/storage-check`（需 token 時）

（完整行為以 `app.py` 為準。）

**小提示**：部分 `POST` API 的 JSON 若因 `Content-Type` 未帶好而為空，可改用 **URL 查詢字串**補上 `dataset_id` 等欄位（與 body 合併），例如  
`POST /delta/gold/word-frequency/run?dataset_id=drinks&dry_run=false`。

### 首頁 /layers 與 Gold 顯示規則（重要）

- 首頁 **詞頻**與 **`/layers`** 的 Gold 預覽以 **落盤 Gold 詞頻 Delta** 為來源。
- **痛點主題**圖表以 **`GOLD_TOPIC_SNAPSHOT_PATH`** 快照表為來源（可選多個 `snapshot_at` 對照）；**請以金層 ETL 或** `topic-snapshot/rebuild` **寫入快照**，並確認 ETL 有帶正確 `dataset_id`。
- 帶 `dataset_id` 時會以表內 `dataset_id` 欄位過濾。
- `/layers` 可切換時間排序；並可檢視 **辭典／停用詞套用狀態**（目前篩選之 `dataset_id`）。
- 一鍵 ETL 完成後會回傳白話說明與指標（含 `gold_recompute_mode` 等）。

### 需求

- **Python**：本機開發建議 **3.12**（與 `Dockerfile` 一致）；`requirements.txt` 為 PySpark 3.5 / Delta 3.0 系）
- **Java**：Spark 需要 JVM（容器內為 **JDK 17**）
- **MinIO 或 S3 相容儲存**：本機、遠端或容器內皆可，由 `MINIO_ENDPOINT` 等設定
- **OCR**：需 [Tesseract](https://github.com/tesseract-ocr/tesseract) 與語言包（繁中、英文）。Windows 可設定 `TESSERACT_CMD` 指向 `tesseract.exe`；**Docker 映像（slim / ubuntu）已含 Tesseract 語言包**

### 快速開始（Windows / PowerShell）

1) 建立虛擬環境並安裝依賴：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2) 設定環境變數（建議複製 `.env.example` 為 `.env` 再修改）：

- **至少**：`MINIO_ACCESS_KEY`、`MINIO_SECRET_KEY`
- **MinIO 位址**：見 `.env.example`；`MINIO_ENDPOINT` 可為 `host:port`（Spark S3A 相容）或 `http://host:port`

```powershell
$env:MINIO_ACCESS_KEY="your_access_key"
$env:MINIO_SECRET_KEY="your_secret_key"
$env:MINIO_ENDPOINT="127.0.0.1:9000"
$env:BUCKET_NAME="data-lake"
```

3) 啟動 Flask：

```powershell
python .\app.py
```

預設：`http://127.0.0.1:5000`  
上傳頁：`http://127.0.0.1:5000/upload`

---

### Docker：檔案該用哪一個？

本專案有 **多份 Compose**，用途不同；**沒有**「一個檔適用所有情境」。

| Compose 檔 | 內容 | 適合情境 |
|--------------|------|----------|
| **`docker-compose.yml`** | 僅 **`web`** 服務（預設 **`Dockerfile`** 建置），**不**包含 MinIO | MinIO 在**別台／區網／宿主機**；連線與路徑皆由 **`.env`（環境變數）** 設定，compose 內僅保留預設值 |
| **`docker-compose(new_minio).yml`** | **MinIO + minio-init + web**（**`Dockerfile`** slim 映像） | 本機想 **一鍵起 MinIO + 應用**（開發／示範） |
| **`docker-compose - ubuntu.yml`** | **MinIO + minio-init + app**（**`Dockerfile - ubuntu`**，內含 Spark 二進位等） | 需與 **Ubuntu/VM 式** 環境接近、或掛載本機目錄除錯 |

**映像建置對照**

| Dockerfile | 說明 |
|------------|------|
| **`Dockerfile`** | `python:3.12-slim` + JDK 17 + Tesseract；`pip install -r requirements-lock.txt` |
| **`Dockerfile - ubuntu`** | `ubuntu:24.04` + JDK + 下載 **Spark 3.5.0** 至 `/opt/spark` + `pip install -r requirements.txt`；映像較大、建置較久 |

需安裝 [Docker Desktop](https://www.docker.com/products/docker-desktop/)（含 Compose V2）。

#### 情境 A：預設檔（只起 Web，連外部 MinIO）

```powershell
docker compose up --build
```

- 會讀取專案根目錄的 **`docker-compose.yml`**，**不會**啟動 MinIO。
- **`MINIO_ENDPOINT`、`MINIO_ENDPOINT_CLIENT`、Delta 路徑、`WEB_PORT` 等**請在 **`.env`** 設定（複製 `.env.example`）；未設定時使用 compose 內建預設值（例如 `MINIO_ENDPOINT` 預設 `http://127.0.0.1:9000`，連外部 MinIO 時務必改成可連線的位址）。
- 需有 **`.env`**（若無，請由 `.env.example` 複製），因 compose 使用 `env_file`。

#### 情境 B：本機一鍵 MinIO + Web（slim 映像）

```powershell
docker compose -f "docker-compose(new_minio).yml" up --build
```

- **Web**：`http://127.0.0.1:5000`（或 `WEB_PORT`）
- **MinIO API**：`http://127.0.0.1:9000`
- **MinIO Console**：`http://127.0.0.1:9001`（預設帳密見 compose / `.env`）
- `minio-init` 會建立預設 bucket（`BUCKET_NAME`，預設 `data-lake`）。

#### 情境 C：MinIO + Ubuntu/Spark 映像

```powershell
docker compose -f "docker-compose - ubuntu.yml" up --build
```

- 使用 **`Dockerfile - ubuntu`**；並掛載 `.:/app` 與 `./spark-warehouse`（本機改碼可反映進容器，依需求使用）。

#### 僅建置／執行單一 Web 映像（自行連外部 MinIO）

```powershell
docker build -t car-rental-web .
docker run --rm -p 5000:5000 `
  -e MINIO_ENDPOINT=http://host.docker.internal:9000 `
  -e MINIO_ACCESS_KEY=... `
  -e MINIO_SECRET_KEY=... `
  car-rental-web
```

（Linux 上將 `host.docker.internal` 改為宿主 IP 或 `--add-host=host.docker.internal:host-gateway`。）

---

### 安全與治理（建議）

- **`ADMIN_TOKEN`**：設定後，多數 **寫入／ETL／上傳／除錯** API 需帶 header `X-Admin-Token`；**未設定**時這些檢查不會生效（僅適合本機開發）。
- **仍可能為公開的讀取**：例如 `GET /health`、`GET /api/gold/word-frequency`、`POST /delta/read` 等（詳見 `app.py`）；若對外服務請評估是否再加網路隔離或反向代理。
- **路徑白名單**：`ALLOWED_DELTA_PATH_PREFIXES`（逗號分隔）；未設定時預設僅 `s3a://<BUCKET_NAME>/`
- **上傳**：`MAX_UPLOAD_MB`（預設 15）；Docker 埠是否只綁 `127.0.0.1` 請依部署決定。

### Delta / 痛點快照（建議）

- **寫入後驗證**：環境變數 **`GOLD_TOPIC_SNAPSHOT_VERIFY_AFTER_WRITE`**（預設 `true`）會在每次寫入 `topic_snapshot` 後以**不忽略缺檔**方式讀表驗證；失敗則 ETL 報錯，避免誤以為成功。表極大或除錯可設 `false`。
- **勿手動刪除單一 parquet**：若需清空快照，應**整個 prefix（含 `_delta_log`）**處理，或先備份再刪；再執行 `POST /delta/gold/topic-snapshot/rebuild` 依 Silver 補寫。

### API 範例

Delta 預覽：

```powershell
$body = @{
  table_path = "s3a://data-lake/bronze/raw_features/"
  limit = 20
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:5000/delta/read" -ContentType "application/json" -Body $body
```

Upsert（若啟用 `ADMIN_TOKEN` 請加 `-Headers @{ "X-Admin-Token"="..." }`）：

```powershell
$body = @{
  target_path = "s3a://data-lake/silver/cleaned_features/"
  key_col = "item_id"
  records = @(
    @{ item_id = "A1"; price = 100 }
    @{ item_id = "A2"; price = 120 }
  )
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:5000/delta/upsert" -ContentType "application/json" -Body $body
```

Cleanup（dry-run）：

```powershell
$body = @{
  target_path = "s3a://data-lake/silver/ocr_features/"
  timestamp_col = "ingestion_timestamp"
  dry_run = $true
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:5000/delta/cleanup-latest-only" -ContentType "application/json" -Body $body
```

上傳圖片（可選跑 OCR）：

```powershell
curl.exe -s -X POST "http://127.0.0.1:5000/api/upload/images" `
  -H "X-Admin-Token: YOUR_TOKEN" `
  -F "file=@C:\path\to\photo.png" `
  -F "dataset_id=invoice_ocr" `
  -F "run_ocr=true" `
  -F "write_mode=append"
```

Bronze OCR（dry-run）：

```powershell
$body = @{ dry_run = $true; dataset_id = "invoice_ocr" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:5000/delta/ocr/bronze/run" -ContentType "application/json" -Body $body
```

金層詞頻（dry-run，建議用 `Invoke-RestMethod` 避免 JSON 引號問題）：

```powershell
$body = @{ dataset_id = "drinks"; dry_run = $true } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:5000/delta/gold/word-frequency/run" `
  -ContentType "application/json" -Body $body
```

痛點快照僅重建（需 Silver 已有資料；若啟用 `ADMIN_TOKEN` 請加 `-Headers`）：

```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:5000/delta/gold/topic-snapshot/rebuild?dataset_id=drinks"
```

### 開發：測試與 CI

本機：

```powershell
pip install -r requirements-dev.txt
pytest -q
```

GitHub：推送至 **`main` 或 `master`** 或開 PR 時，會執行 **`.github/workflows/ci.yml`**（Ubuntu、Python 3.11、JDK 17、`pytest tests/`）。不需另起「CI 服務」。

### Requirements 檔案用途

| 檔案 | 用途 |
|------|------|
| `requirements.txt` | 主依賴（開發、維護時編輯） |
| `requirements-dev.txt` | 開發／測試（如 `pytest`） |
| `requirements-lock.txt` | 鎖定版本；**`Dockerfile`（slim）建置時使用** |
| `requirements.lock` | 舊版／備份鎖檔；若與 `requirements-lock.txt` 並存，**以 `requirements-lock.txt` + `Dockerfile` 為準** |

```powershell
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install -r requirements-lock.txt
```
