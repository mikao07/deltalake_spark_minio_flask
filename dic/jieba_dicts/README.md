# Jieba 自訂詞典

- 格式：`詞 詞頻 詞性`（每行一詞）
- 上傳至 MinIO：`dic/jieba_dicts/{dataset_id}.txt`
- `.env`：`JIEBA_USERDICT_DATASET_PATTERN=s3a://data-lake/dic/jieba_dicts/{dataset_id}.txt`
- MinIO 無檔時會 fallback 至 repo 內建 `drinks.txt` 與 `services/domain_lexicons.py`
