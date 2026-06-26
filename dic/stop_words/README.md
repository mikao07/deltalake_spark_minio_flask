# 領域停用詞（Gold lexicon）

**Silver 不套用**本目錄詞表；僅在 Gold 讀取銀層 `tokens` 後過濾。

## 雙版本（黃金發行 vs 探索測試）

| 版本目錄 | 環境變數 | 用途 | 誰可改 |
|----------|----------|------|--------|
| **`v1.0.0/`** | `STOPWORDS_LEXICON_VERSION` | `analytics_tokens`、痛點快照（**黃金發行**） | 僅發版時 bump |
| **`dev/`** | `STOPWORDS_EXPLORATION_LEXICON_VERSION` | `tfidf_exploration_tokens`（**探索**） | 日常可改 |

```
Silver tokens
  → release lexicon (v1.0.0) → analytics_tokens → 痛點漏斗
  → exploration lexicon (dev)  → tfidf_exploration_tokens → TF-IDF
```

## 路徑

| 用途 | 路徑 |
|------|------|
| 黃金發行 | `dic/stop_words/v1.0.0/{dataset_id}.txt` |
| 探索測試 | `dic/stop_words/dev/{dataset_id}.txt` |
| 相容舊路徑 | `dic/stop_words/{dataset_id}.txt` |
| MinIO 模板 | `STOPWORDS_DATASET_PATTERN=s3a://data-lake/dic/stop_words/{version}/{dataset_id}.txt` |

## 維護節奏

- **調 TF-IDF 雜詞**：只改 `dev/{dataset_id}.txt` → 重跑 Gold（不動 manifest 黃金 hash）
- **升級黃金發行**：複製 dev → `v1.0.0` 或 bump 版本 → 更新 `manifests/{dataset}.json` 的 `lexicon_content_hash` → 重跑 Gold
- 守護神：`python scripts/pipeline_guardian.py --dataset drinks --offline`

內建 fallback：`services/domain_lexicons.py`（兩版詞表皆會合併內建詞）。

## 遷移

若 Silver 曾在舊版套用停用詞，請在升級 `SILVER_TRANSFORM_VERSION` 後 **全量重跑 Silver 一次**，再重跑 Gold。
