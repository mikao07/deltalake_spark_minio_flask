# OCR 與管線決策紀錄（公開摘要）

本文件為 **外送平台客訴截圖 · 商業痛點分析資料湖** 的對外決策摘要；完整除錯與維運手冊保留於本機，不納入版本庫。

---

## 分層職責

| 層級 | 職責 |
|------|------|
| **Bronze** | Tesseract OCR 原文（`extracted_text`），保留稽核用完整輸出 |
| **Silver** | 物理清洗（`cleaned_text`）→ Jieba 分詞（`tokens`）；版本由 `SILVER_TRANSFORM_VERSION` 驅動 MERGE 重算 |
| **Gold** | 領域 lexicon、痛點漏斗、TF-IDF 探索、PMI 片語 |

原則：**token 被刪難以救回；錯字可在 Gold 模糊匹配補救。**

**Gold 內部分流：**

- `analytics_tokens` → 痛點漏斗（`effective_stop = 停用詞 − 痛點保護詞`）
- `tfidf_exploration_tokens` → Phase A 探索（完整停用詞 + 虛詞／場景詞，**不**扣痛點保護）

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

---

## 銀層定案（`SILVER_TRANSFORM_VERSION=v2.1.0`）

- CJK 間 OCR 空格合併（例：珍珠 燕麥 → 珍珠燕麥）
- emoji／獨立洋文垃圾 token 清洗（保留 `line pay` 等白名單片語）
- 變更後只重跑 Silver → Gold，**不**重跑 Bronze

---

## 已知限制與後續方向

| 項目 | 決策 |
|------|------|
| TF-IDF Top 雜詞 | 已以 `tfidf_exploration_tokens` 分流；持續依 Top 補充探索停用詞 |
| 銅層與 `.env` 脫節 | 變更 OCR 參數後須 Bronze `overwrite`，僅重跑銀層不會更新 `extracted_text` |
| 前處理 preset 分流 | `OCR_PRESET_ROUTER_ENABLED` 預設 false；開啟前需子集 AB |
| PaddleOCR | 架構可接；ROI 偏低時維持 Tesseract 封板 |

---

## 變更後重跑對照（摘要）

| 改了什麼 | 重跑 |
|----------|------|
| OCR 參數 / PSM / 前處理 | Bronze overwrite → Silver → Gold |
| `SILVER_TRANSFORM_VERSION` | Silver → Gold |
| 停用詞 / TF-IDF 探索詞 / 痛點規則 | **Gold** |

---

## 相關模組

- `services/ocr_spark.py` — 前處理、OCR、`ocr_signature`
- `services/ocr_psm_ab.py` — PSM A/B 測試（`test/ocr_psm_ab/`）
- `services/text_tokens.py` — 銀層清洗與分詞
- `services/lexicon.py` — Gold 停用詞與 TF-IDF 探索過濾
