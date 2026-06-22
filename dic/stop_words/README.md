# 領域停用詞（Gold lexicon）

**Silver 不套用**本目錄詞表；僅在 Gold 讀取銀層 `tokens` 後過濾（`effective_stop = stop − 痛點保護詞`）。

## 路徑

| 用途 | 路徑 |
|------|------|
| 版本化（建議） | `dic/stop_words/v1.0.0/drinks.txt` |
| 相容舊路徑 | `dic/stop_words/drinks.txt` |
| MinIO 模板 | `STOPWORDS_DATASET_PATTERN=s3a://data-lake/dic/stop_words/{version}/{dataset_id}.txt` |

環境變數 `STOPWORDS_LEXICON_VERSION=v1.0.0` 與 Git tag 對齊；變更詞表後 **只重跑 Gold**，不必重跑 Silver。

內建領域詞見 `services/domain_lexicons.py`（無檔案時仍會合併）。

## 遷移

若 Silver 曾在舊版套用停用詞，請在升級 `SILVER_TRANSFORM_VERSION` 後 **全量重跑 Silver 一次**，再重跑 Gold。
