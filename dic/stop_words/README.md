# 領域辭典（上傳至 MinIO）

路徑需與 `.env` 的 `STOPWORDS_DATASET_PATTERN` 對齊，例如：

- 本機範本：`dic/stop_words/drinks.txt`
- MinIO：`s3a://data-lake/dic/stop_words/drinks.txt`

上傳範例（mc 已設定 alias）：

```bash
mc cp dic/stop_words/drinks.txt local/data-lake/dic/stop_words/drinks.txt
```

**注意**：`dataset_id=drinks` 即使未上傳 MinIO 檔，也會套用 `services/domain_lexicons.py` 內建停用詞；上傳檔案會與內建詞 **合併**。

重跑 **Silver ETL**（更新 tokens 停用詞）→ **Gold ETL**（更新詞頻與痛點快照）。
