## car_rental_flask_spark_delta

Flask 後端 + PySpark + Delta Lake：透過 **S3 相容 API（MinIO）** 讀寫資料、維護 Delta table，並提供網頁（Dashboard、上傳等）與 REST API。

### 專案結構（重點）

| 路徑 | 說明 |
|------|------|
| `app.py` | Flask 入口：路由、管理員 token、Delta 白名單、背景任務等 |
| `config.py` | 由環境變數讀取 MinIO/S3A、各層 Delta 路徑、OCR、上傳策略 |
| `services/spark_service.py` | SparkSession、Delta 讀寫、Silver/Gold ETL、系統狀態等 |
| `services/minio_upload.py` | MinIO SDK 上傳、`dataset_id` 目錄、同名物件策略 |
| `services/ocr_spark.py` | Bronze OCR：binaryFile → 前處理（含二值化）→ Tesseract（含 user-words）→ Delta |
| `services/domain_lexicons.py` | 依 `dataset_id` 內建停用詞、Jieba 詞、OCR user-words |
| `services/pain_funnel.py` | 痛點漏斗：撈網 → 過濾 → 情緒（`pain_candidates` / `sentiment`） |
| `services/pain_topic_rules.py` | 痛點主題詞庫與極性規則（供漏斗使用） |
| `services/text_similarity.py` | 痛點規則用的字串相似度（rapidfuzz / difflib fallback） |
| `services/async_jobs.py` | 記憶體內背景任務（長時間 Spark 工作先回 job_id） |
| `dic/` | 領域辭典：`stop_words/`、`jieba_dicts/`、`ocr_user_words/`（可上傳 MinIO 擴充） |
| `templates/` | `index.html`（Dashboard）、`pipeline_bronze|silver|gold.html`（管線分頁）、`layers.html`（除錯預覽） |
| `tests/` | `pytest`：API 驗證、MinIO 檔名邏輯（可不透過真 Spark/MinIO） |
| `.github/workflows/ci.yml` | Push／PR 至 `main`/`master` 時自動執行 `pytest`（GitHub Actions） |

### 資料管線（摘要）

- **Bronze**：`raw/images/{dataset_id}/` 影像 → **可調前處理**（放大、灰階、對比、銳化、**二值化**）→ **Tesseract**（預設 **`OCR_PSM=11`**，可載入 **user-words**）→ Delta；可選擋掉 `OCR_ERROR_*` 不寫入。詳見 **OCR 調校**、**領域辭典**。
- **Silver**：OCR 原文保留於 `extracted_text`；**`cleaned_text`** 物理清洗（去標點、剝純數字雜訊）；**Jieba + 內建虛詞停用詞** 產出冪等 `tokens`（`SILVER_TRANSFORM_VERSION`）。ETL 後執行**三道品質防線**（Schema／Token 分佈／留存率）。**不**套用領域停用詞。
- **Gold**：讀銀層 `tokens` → 套用版本化 lexicon（`effective_stop = stop − 痛點保護詞`）→ 痛點漏斗、TF-IDF、PMI。規則 **`v1.4-drinks-funnel`**；辭典 **`STOPWORDS_LEXICON_VERSION`**。
- **Gold（資料驅動）**：**Phase A** TF-IDF 痛點候選詞 → `GOLD_TFIDF_KEYWORDS_PATH`；**Phase B** PMI 片語候選 → `GOLD_PHRASE_CANDIDATES_PATH`（隨金層 ETL 一併執行）。
- **一鍵**：`POST /delta/pipeline/to-gold/run`（Bronze→Silver→Gold）；可設定 `skip_gold_if_no_new_ocr`（無新 OCR 時略過後段）。**金層頁** `/pipeline/gold` 有同名勾選（預設開啟）；**銅層頁**上傳表單亦有一鍵選項。

### 功能（API 摘要）

- **健康檢查（輕量，存活探針）**：`GET /health`
- **就緒／依賴檢查（JSON，MinIO；可選 Spark）**：`GET /ready`（可選查詢 `spark=true`；或環境變數 `READY_CHECK_INCLUDE_SPARK`；失敗時 HTTP 503）
- **系統狀態**：`GET /api/status`
- **Delta 預覽**：`POST /delta/read`（路徑須符合 `ALLOWED_DELTA_PATH_PREFIXES`）
- **Delta Upsert**：`POST /delta/upsert`（需 `ADMIN_TOKEN` 時帶 `X-Admin-Token`）
- **僅保留最新批次**：`POST /delta/cleanup-latest-only`（同上）
- **Silver OCR 預覽**：`GET /api/silver`、`GET /api/silver/ocr`
- **TF-IDF 痛點候選（Phase A）**：`GET /api/gold/tfidf-keywords`
- **PMI 片語候選（Phase B）**：`GET /api/gold/phrase-candidates`
- **金層 ETL**（Silver→Gold）：`POST /delta/gold/run`（body 或 **Query** 可補 `dataset_id`、`dry_run` 等）
- **痛點快照僅重建**：`POST /delta/gold/topic-snapshot/rebuild`（`dataset_id` 必填）
- **痛點快照刪除**（依 `dataset_id` + `snapshot_at` 刪列，Delta DELETE）：`POST /delta/gold/topic-snapshot/delete`（可先 `dry_run: true`；`snapshot_at` 用首頁對照或列表之 ISO 字串）
- **一鍵至金層**：`POST /delta/pipeline/to-gold/run`（body 可帶 `skip_gold_if_no_new_ocr`；見金層管線頁說明）
- **Bronze OCR 攝入**：`POST /delta/ocr/bronze/run`
- **Silver OCR ETL**：`POST /delta/silver/ocr/run`
- **圖片上傳至 MinIO**：`POST /api/upload/images`（`multipart/form-data`，`dataset_id` 必填；可選 `run_ocr`；`MAX_UPLOAD_MB` 限制大小）
- **查詢 dataset_id**：`GET /api/datasets`（需 `ADMIN_TOKEN` 時）
- **背景任務**：`GET /api/jobs/<job_id>`（建立非同步 pipeline 時）
- **Storage 健康檢查**：`GET /api/health/storage`、`GET /api/debug/storage-check`（需 token 時）

（完整行為以 `app.py` 為準。）

**小提示**：部分 `POST` API 的 JSON 若因 `Content-Type` 未帶好而為空，可改用 **URL 查詢字串**補上 `dataset_id` 等欄位（與 body 合併），例如  
`POST /delta/gold/run?dataset_id=drinks&dry_run=false`。

### 首頁 /layers 與 Gold 顯示規則（重要）

- 首頁 **TF-IDF**與 **`/layers`** 的 Gold 預覽以 **落盤 TF-IDF Delta** 為來源（輔助探索）。
- **痛點主題**圖表以 **`GOLD_TOPIC_SNAPSHOT_PATH`** 快照為**主要商業輸出**（規則 + 模糊匹配）；請以金層 ETL 或 `topic-snapshot/rebuild` 寫入，並確認帶正確 `dataset_id`。
- 帶 `dataset_id` 時會以表內 `dataset_id` 欄位過濾。
- `/layers` 可切換時間排序；並可檢視 **辭典／停用詞套用狀態**（目前篩選之 `dataset_id`）。
- 一鍵 ETL 完成後會回傳白話說明與指標（含 `gold_recompute_mode` 等）。

### 需求

- **Python**：本機開發建議 **3.12**（與 `Dockerfile` 一致）；`requirements.txt` 為 PySpark 3.5 / Delta 3.0 系）
- **Java**：Spark 需要 JVM（容器內為 **JDK 17**）
- **MinIO 或 S3 相容儲存**：本機、遠端或容器內皆可，由 `MINIO_ENDPOINT` 等設定
- **OCR**：需 [Tesseract](https://github.com/tesseract-ocr/tesseract) 與語言包（繁中、英文）。Windows 可設定 `TESSERACT_CMD`；**Docker 映像已含 Tesseract**。Bronze 前處理另需 **`opencv-python-headless`**（二值化）；Gold 模糊痛點需 **`rapidfuzz`**（見 `requirements.txt`）。

### OCR 調校（Bronze / Tesseract）

銅層 OCR 由 `services/ocr_spark.py` 執行：讀取 MinIO 影像 → **可調前處理** → **Tesseract**（可帶 **user-words**）→ 寫入 Bronze Delta。

| 變數 | 預設 | 說明 |
|------|------|------|
| `OCR_LANG` | `chi_tra+eng` | Tesseract 語言包；純中文評論可試 `chi_tra` |
| `OCR_PSM` | `11` | Page Segmentation Mode；**11**＝稀疏文字；**6**＝單一文字區塊 |
| `OCR_SCALE_MIN_SIDE` | `0` | 短邊低於此像素時等比放大；手機截圖建議 **1400～1800** |
| `OCR_CONTRAST` | `1.5` | 灰階後對比度倍率；可試 **1.8～2.0** |
| `OCR_SHARPNESS` | `1.0` | 銳利度倍率；可試 **1.1～1.3** |
| `OCR_BINARIZE` | `off` | 二值化：`off` \| `otsu` \| `adaptive`；**粉紅外送 UI／深色模式** 截圖可試 **`otsu`** |
| `OCR_BINARIZE_INVERT` | `auto` | `auto` 依平均亮度自動反相；或 `true` / `false` |
| `OCR_BINARIZE_BLOCK_SIZE` | `31` | `adaptive` 時區塊大小（奇數） |
| `OCR_BINARIZE_C` | `10` | `adaptive` 常數 C |
| `OCR_USER_WORDS_PATH` | （空） | 全域 Tesseract user-words（本機或 `s3a://`） |
| `OCR_USER_WORDS_DATASET_PATTERN` | （空） | 依 dataset，例：`s3a://data-lake/dic/ocr_user_words/{dataset_id}.txt` |
| `TESSERACT_CMD` | （空） | Windows 本機可指向 `tesseract.exe` |
| `OCR_PREPROCESS_VERSION` | `v1` | 前處理版本；調參後請 bump（目前建議 **`v3`**） |
| `OCR_SIGNATURE` | （空） | 自訂簽章；未設時由 lang / psm / pre / bin 等自動組成 |

**調校順序建議**（由低成本到高成本）：

1. **前處理**：`OCR_SCALE_MIN_SIDE` + `OCR_CONTRAST`
2. **二值化**：`OCR_BINARIZE=otsu`（彩色背景截圖）
3. **領域 OCR 詞**：repo 內 `dic/ocr_user_words/drinks.txt` 或上傳 MinIO（見 **領域辭典**）
4. **PSM / 語言**：`OCR_PSM=6` 與 `11` 對照
5. **重跑 Bronze** → Silver → Gold
6. 仍不足再評估 **PaddleOCR**

`.env` 範例（評論截圖起點）：

```env
OCR_LANG=chi_tra
OCR_PSM=11
OCR_SCALE_MIN_SIDE=1600
OCR_CONTRAST=1.8
OCR_SHARPNESS=1.2
OCR_BINARIZE=otsu
OCR_PREPROCESS_VERSION=v3
```

**變更 OCR 設定後**，需讓銅層重新辨識影像：

1. **重建 Docker**（若用容器）：`docker compose up --build -d`（映像需含 `opencv-python-headless`）
2. 至 **`/pipeline/bronze`** 執行 Bronze OCR；`write_mode` 建議 **`overwrite`**，或 **`append`**（僅處理 `ocr_signature` 變更後尚未處理的圖）
3. 再執行 **Silver ETL** → **Gold ETL**

本機可先對單張圖試 PSM（需已安裝 Tesseract）：

```powershell
tesseract 你的截圖.png stdout -l chi_tra --psm 11
tesseract 你的截圖.png stdout -l chi_tra --psm 6
```

### 領域辭典（drinks 範例）

三種辭典分工不同，請勿混用：

| 類型 | 路徑（repo / MinIO） | 作用層 | 目的 |
|------|----------------------|--------|------|
| **停用詞** | `dic/stop_words/{version}/{dataset_id}.txt` | **Gold** | 擋中性詞；變更後只重跑 Gold |
| **Jieba 詞典** | `dic/jieba_dicts/{dataset_id}.txt` | Silver | 避免「服務態度」「50嵐」被切開 |
| **OCR user-words** | `dic/ocr_user_words/{dataset_id}.txt` | Bronze | 提升 Tesseract 品牌／規格辨識 |

- **內建詞**：`services/domain_lexicons.py` 在 MinIO 無檔時仍會於 **Gold** 合併 `drinks` 停用詞；Jieba 會 **fallback** 至 repo 內 `dic/jieba_dicts/drinks.txt`。
- **環境變數**（見 `.env.example`）：
  - `STOPWORDS_DATASET_PATTERN=s3a://data-lake/dic/stop_words/{dataset_id}.txt`
  - `JIEBA_USERDICT_DATASET_PATTERN=s3a://data-lake/dic/jieba_dicts/{dataset_id}.txt`
  - `OCR_USER_WORDS_DATASET_PATTERN=s3a://data-lake/dic/ocr_user_words/{dataset_id}.txt`

**維護節奏**：TF-IDF Top 雜詞 → 加**停用詞**；分詞切壞 → 加 **Jieba 詞**；整詞 OCR 認錯 → 加 **OCR user-words**。

**重跑對照**：

| 改了什麼 | 重跑 |
|----------|------|
| 二值化、OCR user-words | Bronze → Silver → Gold |
| Jieba 詞典 | Silver → Gold |
| 停用詞（lexicon 版本） | **Gold**（Silver 不重跑） |
| 痛點規則／模糊匹配 | Gold（或 `topic-snapshot/rebuild`） |

### 痛點主題與模糊匹配（Gold）

- **主輸出**：首頁 **痛點主題快照**（`GOLD_TOPIC_SNAPSHOT_PATH`），由痛點**漏斗**對銀層 `tokens` 打標。
- **漏斗**（`services/pain_funnel.py`）：
  1. **第一層撈網**：關鍵字 + 模糊匹配，產出 `pain_candidates`（高 recall）
  2. **第二層過濾**：片語／極性規則；**負面證據優先**；僅正向（如很好、不錯）且無負面 → 剔除痛點
  3. **情緒**：`positive` \| `neutral` \| `negative`（有痛點主題或負面證據 → negative）
- **規則版本**：`services/pain_topic_rules.py`（`TOPIC_RULE_VERSION=v1.4-drinks-funnel`）
- **模糊匹配**：`services/text_similarity.py`（`PAIN_FUZZY_*`）

| 變數 | 預設 | 說明 |
|------|------|------|
| `PAIN_FUZZY_ENABLED` | `true` | 是否啟用模糊匹配 |
| `PAIN_FUZZY_MIN_RATIO` | `0.78` | 片語規則相似度門檻 |
| `PAIN_FUZZY_ANCHOR_RATIO` | `0.88` | 極性規則 anchor 門檻（較嚴） |
| `PAIN_FUZZY_MIN_CHARS` | `3` | 少於此字數不做模糊（防誤殺） |

- **TF-IDF / PMI**：Phase A／B 候選詞供探索與加詞參考，**不等同**最終痛點結論。

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
首頁 Dashboard：`http://127.0.0.1:5000/`（選定 `dataset_id` 後可使用 **刪除痛點快照** 區塊，呼叫 `POST /delta/gold/topic-snapshot/delete`）  
**資料管線（分頁）**：

| URL | 用途 |
|-----|------|
| `/pipeline/bronze` | 上傳、Bronze OCR、`write_mode`、Bronze 預覽 |
| `/pipeline/silver` | Silver ETL、分詞／停用詞狀態、銀層預覽 |
| `/pipeline/gold` | Gold ETL、一鍵管線、金層預覽 |
| `/layers` | 三層表格除錯（進階） |
| `/upload` | 相容舊連結，302 導向 `/pipeline/bronze` |

頂部導覽列可帶 `?dataset_id=` 跨頁共用同一分類。

**金層頁（一鍵管線）**：可勾選 **「無新 OCR 時跳過銀層／金層」**（預設**開啟**）。**關閉**後，即使本次 Bronze 沒有新增筆數，仍會重跑 Silver／金層，適合升級 **`SILVER_TRANSFORM_VERSION`** 或更新 **Jieba 詞典**（Silver）或 **停用詞 lexicon／痛點規則**（Gold）後要重算。行為等同 API 的 `skip_gold_if_no_new_ocr`。`write_mode`（append／overwrite）**僅在銅層頁**設定。

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
| **`Dockerfile`** | `python:3.12-slim` + JDK 17 + Tesseract；`pip install -r requirements-lock.txt`；含 **`dic/`** 領域辭典 |
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
- **仍可能為公開的讀取**：例如 `GET /health`、`GET /api/gold/tfidf-keywords`、`POST /delta/read` 等（詳見 `app.py`）；若對外服務請評估是否再加網路隔離或反向代理。
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

金層 ETL（dry-run，建議用 `Invoke-RestMethod` 避免 JSON 引號問題）：

```powershell
$body = @{ dataset_id = "drinks"; dry_run = $true } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:5000/delta/gold/run" `
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
| `requirements.txt` | 主依賴（含 `opencv-python-headless`、`rapidfuzz`） |
| `requirements-dev.txt` | 開發／測試（如 `pytest`） |
| `requirements-lock.txt` | 鎖定版本；**`Dockerfile`（slim）建置時使用** |
| `requirements.lock` | 舊版／備份鎖檔；若與 `requirements-lock.txt` 並存，**以 `requirements-lock.txt` + `Dockerfile` 為準** |

```powershell
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install -r requirements-lock.txt
```
