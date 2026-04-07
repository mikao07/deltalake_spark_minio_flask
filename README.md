## car_rental_flask_spark_delta

Flask + Spark + Delta Lake 的簡化示範專案，透過 API 讀寫/維護 Delta table，並提供簡單 Dashboard。

### 功能

- **健康檢查**：`GET /health`
- **系統狀態**：`GET /api/status`
- **Delta 預覽**：`POST /delta/read`
- **Delta Upsert**：`POST /delta/upsert`（可用 `ADMIN_TOKEN` 保護）
- **僅保留最新批次**：`POST /delta/cleanup-latest-only`（可用 `ADMIN_TOKEN` 保護，支援 `dry_run`）
- **Bronze OCR 攝入**（對齊 `MinIO_DeltaLake_Spark_1.1.ipynb`）：`POST /delta/ocr/bronze/run` — 以 `binaryFile` 讀 MinIO 影像 → Tesseract → 寫入 Delta Bronze
- **圖片上傳至 MinIO**：`POST /api/upload/images`（`multipart/form-data`，欄位 `file` 或 `files`）— 寫入 `BUCKET_NAME` 下 `RAW_IMAGE_PREFIX`；可選 `run_ocr=true` 上傳後接續跑 Bronze OCR

### 需求

- **Python**：建議 3.10+（3.9 也常見可行）
- **Java**：Spark 需要 Java（通常 8/11/17 皆可能，依你的 Spark/環境而定）
- **MinIO**：本機或遠端皆可
- **OCR（本機或 Docker 映像已含）**：需安裝 [Tesseract](https://github.com/tesseract-ocr/tesseract) 與語言包（繁中 `chi_tra`、英文 `eng`）。Windows 請安裝後可選設定 `TESSERACT_CMD` 指向 `tesseract.exe`

### 快速開始（Windows / PowerShell）

1) 建立虛擬環境並安裝依賴：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2) 設定環境變數（建議用 `.env` 的方式自行載入，或直接在 PowerShell 設定）：

- 請參考 `.env.example`
- **至少要設定** `MINIO_ACCESS_KEY`、`MINIO_SECRET_KEY`

範例（僅示意）：

```powershell
$env:MINIO_ACCESS_KEY="your_access_key"
$env:MINIO_SECRET_KEY="your_secret_key"
$env:MINIO_ENDPOINT="http://127.0.0.1:9000"
$env:BUCKET_NAME="data-lake"
```

3) 啟動 Flask：

```powershell
python .\app.py
```

預設會在 `http://127.0.0.1:5000` 服務。

### Docker Compose（MinIO + Web 一鍵起）

需安裝 [Docker Desktop](https://www.docker.com/products/docker-desktop/)（含 Compose V2）。

```powershell
docker compose up --build
```

- **Web**：`http://127.0.0.1:5000`
- **MinIO API**：`http://127.0.0.1:9000`
- **MinIO Console**：`http://127.0.0.1:9001`（預設帳密與 `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` 相同，預設為 `minioadmin` / `minioadmin`）

`minio-init` 會自動建立 bucket（預設名稱 `data-lake`，可用環境變數 `BUCKET_NAME` 覆寫）。容器內應用透過 `MINIO_ENDPOINT=http://minio:9000` 連 MinIO，無須改程式。

可選：在專案目錄放 `.env` 覆寫 `MINIO_ROOT_USER`、`MINIO_ROOT_PASSWORD`、`BUCKET_NAME`、`WEB_PORT`、`ADMIN_TOKEN` 等（勿提交真實密碼）。

僅建置並執行 Web 映像（自行連外部 MinIO 時）：

```powershell
docker build -t car-rental-web .
docker run --rm -p 5000:5000 `
  -e MINIO_ENDPOINT=http://host.docker.internal:9000 `
  -e MINIO_ACCESS_KEY=... `
  -e MINIO_SECRET_KEY=... `
  car-rental-web
```

（Linux 上將 `host.docker.internal` 改為宿主 IP 或 `--add-host=host.docker.internal:host-gateway`。）

### 安全與治理（建議務必設定）

- **保護高風險端點**：設定 `ADMIN_TOKEN` 後，呼叫需帶 header `X-Admin-Token`
- **限制可操作路徑**：用 `ALLOWED_DELTA_PATH_PREFIXES` 限制 API 只能處理特定 `s3a://.../` 前綴（逗號分隔）
  - 未設定時，預設只允許 `s3a://<BUCKET_NAME>/`

### API 範例

Delta 預覽（最多 200 筆；超過會被強制裁切）：

```powershell
$body = @{
  table_path = "s3a://data-lake/bronze/raw_features/"
  limit = 20
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:5000/delta/read" -ContentType "application/json" -Body $body
```

Upsert（若設定 `ADMIN_TOKEN`，請加 `-Headers @{ "X-Admin-Token"="..." }`）：

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

上傳一張圖到 MinIO 再（可選）執行 OCR（`curl` 範例，相容性較佳）：

```powershell
curl.exe -s -X POST "http://127.0.0.1:5000/api/upload/images" `
  -H "X-Admin-Token: YOUR_TOKEN" `
  -F "file=@C:\path\to\photo.png" `
  -F "run_ocr=true" `
  -F "write_mode=append"
```

（PowerShell 7+ 也可用 `Invoke-RestMethod -Form`。）

Bronze OCR（僅跑 OCR、不上傳；先 dry-run 確認路徑）：

```powershell
$body = @{ dry_run = $true } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:5000/delta/ocr/bronze/run" -ContentType "application/json" -Body $body
```

### 開發：測試

安裝 dev 依賴並執行 pytest：

```powershell
pip install -r requirements-dev.txt
pytest -q
```
