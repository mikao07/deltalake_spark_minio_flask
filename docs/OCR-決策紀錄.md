# OCR 與管線決策紀錄（公開摘要）

本文件為 **外送平台客訴截圖 · 商業痛點分析資料湖** 的對外決策摘要；完整除錯與維運手冊保留於本機，不納入版本庫。

---

## 實作現況（對照用）

| 項目 | 狀態 |
|------|------|
| 銅層封板（PSM 6 · dark_ui · `pre=v1.1`，50 張） | ✅ 已完成 |
| broadcast OCR 設定 + router 骨架（預設 `dark_ui`） | ✅ 已完成 |
| 銀層 `v2.1.0`（Lookaround CJK + emoji／垃圾清洗） | ✅ 已完成 |
| Gold 痛點漏斗 + TF-IDF 探索 token 分流 | ✅ 已完成 |
| 停用詞雙版本（黃金 `v1.0.0` / 探索 `dev`） | ✅ 已完成 |
| 管線守護神 + `manifests/drinks.json` | ✅ 已完成 |
| Compose 全檔 OCR 環境變數明列 | ✅ 三份 compose 已對齊 |
| per-row `ocr_signature` | ✅ UDF 依實際 profile 產生 |
| Bronze 子集 MERGE | ✅ `write_mode=merge` + `image_paths` |
| preset router 子集調校（階段 4） | ❌ 未執行（可選；需子集 AB） |

---

## 分層職責

| 層級 | 職責 |
|------|------|
| **Bronze** | Tesseract OCR 原文（`extracted_text`），保留稽核用完整輸出 |
| **Silver** | 物理清洗（`cleaned_text`）→ Jieba 分詞（`tokens`）；版本由 `SILVER_TRANSFORM_VERSION` 驅動 MERGE 重算 |
| **Gold** | 領域 lexicon、痛點漏斗、TF-IDF 探索、PMI 片語 |

原則：**token 被刪難以救回；錯字可在 Gold 模糊匹配補救。**

**Gold 內部分流（雙版本詞表）：**

| 欄位 | 詞表 | 用途 |
|------|------|------|
| `analytics_tokens` | **黃金發行** `v1.0.0/` | 痛點漏斗（`effective_stop = 停用詞 − 痛點保護詞`）→ 主題快照 |
| `tfidf_exploration_tokens` | **探索測試** `dev/` | Phase A TF-IDF（探索停用詞 + 虛詞，**不**扣痛點保護） |

**治理原則**：對外簡報／模型只吃 manifest 核准的黃金發行版；探索軌可持續調詞，不必每次 bump 黃金版。

---

## 銅層 OCR 定案（drinks 深色 UI 截圖）

### 前處理 profile：`dark_ui`

| 參數 | 定案值 | 說明 |
|------|--------|------|
| `OCR_PSM` | **6** | 單一文字區塊；較少 emoji／圖示誤認垃圾 |
| `OCR_SCALE_MIN_SIDE` | **0** | 不放大；強制放大易使深色 UI 字糊（例：燕麥→蒸座） |
| `OCR_CONTRAST` | **1.5** | 灰階後對比 |
| `OCR_SHARPNESS` | **1.0** | 預設 |
| `OCR_BINARIZE` | **off** | 彩色／深色 UI 先不二值化 |
| `OCR_PREPROCESS_VERSION` | **v1.1** | 寫入 `ocr_signature` |

### PSM A/B 結論（各 20 張樣本，`scale=0`）

| 對照 | 結果 |
|------|------|
| 11 vs **6** | 6 勝：關鍵詞 9 vs 7；PSM 11 易產生 `BARE` 等洋文垃圾 |
| 4 vs **6** | 6 略優：關鍵詞平手，字數均值略高 |
| 13 vs **6** | 6 壓倒性勝：PSM 13 不適用整張手機截圖 |

**不再以 4、11、13 作為 drinks 主力 PSM。**

### 銅層封板與局部調校

- **全量 `overwrite` 只做一次**（2026-06-22，drinks 50 列）；此後視為封板。
- **preset router** 骨架已在 `ocr_spark.py`；`OCR_PRESET_ROUTER_ENABLED=false` 時一律 `dark_ui`。
- 封板後若少數爛圖需 `low_res` 等 profile：**禁止**第二次全表 overwrite；使用 **`write_mode=merge`** + **`image_paths`** 子集 Upsert。
- `ocr_signature` 為 **per-row**（依 UDF 內實際 profile／前處理參數）。
- OCR 參數傳遞以 **`sc.broadcast(ocr_config)`** 為準；容器 `environment` 與 `spark.executorEnv` 為一致化備援。

---

## 銀層定案（`SILVER_TRANSFORM_VERSION=v2.1.0`）

- CJK 間 OCR 空格合併（例：珍珠 燕麥 → 珍珠燕麥）
- emoji／獨立洋文垃圾 token 清洗（保留 `line pay` 等白名單片語）
- 變更後只重跑 Silver → Gold，**不**重跑 Bronze

**CJK 去空格實作注意**：必須使用 **Lookaround** 正則 `(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])`，且須在 **洋文白名單保護之後** 執行；不可用「刪除所有空白」，否則會誤殺 `line pay` 中間空格。

---

## 已知限制與後續方向

| 項目 | 決策 |
|------|------|
| TF-IDF Top 雜詞 | 改 **`dic/stop_words/dev/`** 探索詞表；滿意後再合併進黃金 `v1.0.0/` + manifest |
| 銅層與 `.env` 脫節 | 變更 OCR 參數後須 Bronze 重寫 `extracted_text`；僅重跑銀層無效 |
| 銅層第二次全表 overwrite | **已取消**為預設路徑；改以 `merge` + `image_paths` |
| per-row `ocr_signature` | ✅ 已實作 |
| 前處理 preset 分流 | 預設關閉；開啟前需子集 AB |
| PaddleOCR | 架構可接；ROI 偏低時維持 Tesseract 封板 |

---

## 變更後重跑對照（摘要）

| 改了什麼 | 重跑 |
|----------|------|
| OCR 參數 / PSM / 前處理（**整批**，罕見） | Bronze **overwrite** → Silver → Gold |
| 少數圖需換 profile（封板後） | Bronze **merge**（`image_paths`）→ Silver → Gold |
| `SILVER_TRANSFORM_VERSION` | Silver → Gold |
| 探索停用詞（`dev/`） | **Gold**（不更新 manifest） |
| 黃金發行停用詞（`v1.0.0/`）／痛點規則 | **Gold** + 更新 `manifests/*.json` |

---

## 相關模組

- `services/ocr_spark.py` — 前處理、OCR、`ocr_signature`、router、broadcast
- `services/ocr_psm_ab.py` — PSM A/B 測試（`test/ocr_psm_ab/`）
- `services/text_tokens.py` — 銀層清洗與分詞
- `services/lexicon.py` — Gold 雙版本停用詞與 TF-IDF 探索過濾
- `services/pipeline_guardian.py` — 管線守護神（銅銀品質、黃金 lexicon hash）
- `manifests/drinks.json` — 黃金發行 manifest
