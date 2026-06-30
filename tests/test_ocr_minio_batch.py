"""MinIO SDK fallback 分批讀圖（P4；單元測試：不依賴 Spark／MinIO）。"""

from services.ocr_spark import chunk_s3a_paths, resolve_ocr_minio_batch_size


def test_resolve_ocr_minio_batch_size_caps_at_max_bronze():
    assert resolve_ocr_minio_batch_size(batch_size=50, max_bronze_images=100) == 50
    assert resolve_ocr_minio_batch_size(batch_size=200, max_bronze_images=100) == 100
    assert resolve_ocr_minio_batch_size(batch_size=0, max_bronze_images=10) == 1


def test_chunk_s3a_paths_splits_evenly():
    paths = [f"s3a://b/raw/images/d/img{i}.png" for i in range(5)]
    batches = chunk_s3a_paths(paths, 2)
    assert batches == [
        paths[0:2],
        paths[2:4],
        paths[4:5],
    ]


def test_chunk_s3a_paths_skips_blanks():
    batches = chunk_s3a_paths(["  ", "s3a://b/a.png", ""], 10)
    assert batches == [["s3a://b/a.png"]]
