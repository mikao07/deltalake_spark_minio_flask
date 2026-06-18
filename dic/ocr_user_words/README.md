# Tesseract OCR user-words

- 格式：每行一詞；`#` 開頭為註解
- 上傳至 MinIO：`dic/ocr_user_words/{dataset_id}.txt`
- `.env`：`OCR_USER_WORDS_DATASET_PATTERN=s3a://data-lake/dic/ocr_user_words/{dataset_id}.txt`
- 內建 `drinks.txt` 與 `domain_lexicons` 會自動合併；改後請重跑 **Bronze ETL**
