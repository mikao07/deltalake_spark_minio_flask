"""
Bronze 層 OCR 攝入（對齊 MinIO_DeltaLake_Spark_1.1.ipynb）：
從 MinIO（S3A）以 binaryFile 讀取影像 → Tesseract（pytesseract）→ 寫入 Delta Bronze。

須安裝系統套件：Tesseract OCR 與語言包（例如 chi_tra、eng）。
"""

from __future__ import annotations

import os
import re
import logging
from typing import Optional
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, length, lit, lower, regexp_extract, sha2, udf
from pyspark.sql.types import StringType, StructField, StructType
from minio.error import S3Error
from delta.tables import DeltaTable

from config import (
    BUCKET_NAME,
    BRONZE_HISTORY_ON_MERGE,
    BRONZE_HISTORY_PATH,
    MAX_BRONZE_OCR_IMAGES,
    OCR_MINIO_BATCH_SIZE,
    OCR_TIMEOUT_SECONDS,
    SPARK_JOB_TIMEOUT_SECONDS,
    OCR_BINARIZE,
    OCR_CONTRAST,
    OCR_LANG,
    OCR_LIGHT_DOC_MEAN_LUMA,
    OCR_LOW_RES_SHORT_SIDE,
    OCR_LOW_RES_TARGET_SIDE,
    OCR_PREPROCESS_VERSION,
    OCR_PRESET_ROUTER_ENABLED,
    OCR_PSM,
    OCR_SCALE_MIN_SIDE,
    OCR_SHARPNESS,
    OCR_SIGNATURE,
    OCR_USER_WORDS_PATH,
    RAW_IMAGE_PREFIX,
    TESSERACT_CMD,
)
from services.domain_lexicons import (
    materialize_merged_ocr_user_words_file,
    resolve_local_ocr_user_words_path,
)
from services.minio_upload import ensure_bucket, get_minio_client
from services.etl_runtime_limits import apply_ocr_repartition, resolve_ocr_repartition, run_ocr_with_timeout

from services.media_validation import (
    SUPPORTED_IMAGE_EXTENSIONS,
    has_supported_image_extension,
    looks_like_image_bytes,
)

_SUPPORTED_IMAGE_EXTS = tuple(SUPPORTED_IMAGE_EXTENSIONS)
_VALID_PSM = frozenset(str(i) for i in range(14))
_logger = logging.getLogger(__name__)
_OCR_RESULT_SCHEMA = StructType(
    [
        StructField("extracted_text", StringType(), True),
        StructField("ocr_signature", StringType(), True),
    ]
)


def normalize_psm(psm: str | None, *, default: str | None = None) -> str:
    """Tesseract PSM 0–13；無效值拋 ValueError。"""
    fallback = (default or os.getenv("OCR_PSM") or OCR_PSM or "6").strip() or "6"
    s = str(psm).strip() if psm is not None and str(psm).strip() else fallback
    if s not in _VALID_PSM:
        raise ValueError(f"PSM 必須為 0–13 的整數字串（目前：{s!r}）。")
    return s


def _has_supported_image_extension(path: str) -> bool:
    return has_supported_image_extension(path)


def _looks_like_image_bytes(data: bytes) -> bool:
    return looks_like_image_bytes(data)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_str(name: str, default: str = "") -> str:
    raw = os.getenv(name, "").strip()
    return raw or default


def build_ocr_runtime_config() -> dict:
    """
    Driver 啟動 Bronze OCR 時組裝設定（可 broadcast 至 executor）。
    優先 os.environ（單測 monkeypatch），fallback config 模組預設。
    """
    return {
        "lang": (os.getenv("OCR_LANG") or OCR_LANG or "chi_tra+eng").strip(),
        "psm": normalize_psm(os.getenv("OCR_PSM") or OCR_PSM),
        "tesseract_cmd": (os.getenv("TESSERACT_CMD") or TESSERACT_CMD or "").strip(),
        "preprocess_version": (os.getenv("OCR_PREPROCESS_VERSION") or OCR_PREPROCESS_VERSION or "v1").strip(),
        "scale_min_side": max(0, _env_int("OCR_SCALE_MIN_SIDE", int(str(OCR_SCALE_MIN_SIDE or "0") or "0"))),
        "contrast": _env_float("OCR_CONTRAST", float(str(OCR_CONTRAST or "1.5"))),
        "sharpness": _env_float("OCR_SHARPNESS", float(str(OCR_SHARPNESS or "1.0"))),
        "binarize": (_env_str("OCR_BINARIZE", str(OCR_BINARIZE or "off")) or "off").lower(),
        "binarize_block_size": _env_int("OCR_BINARIZE_BLOCK_SIZE", 31),
        "binarize_c": _env_int("OCR_BINARIZE_C", 10),
        "binarize_invert": (_env_str("OCR_BINARIZE_INVERT", "auto") or "auto").lower(),
        "binarize_morph": (_env_str("OCR_BINARIZE_MORPH", "off") or "off").lower(),
        "signature_override": (os.getenv("OCR_SIGNATURE") or OCR_SIGNATURE or "").strip(),
        "preset_router_enabled": _env_bool_from_env("OCR_PRESET_ROUTER_ENABLED", OCR_PRESET_ROUTER_ENABLED),
        "low_res_short_side": _env_int("OCR_LOW_RES_SHORT_SIDE", int(OCR_LOW_RES_SHORT_SIDE)),
        "low_res_target_side": _env_int("OCR_LOW_RES_TARGET_SIDE", int(OCR_LOW_RES_TARGET_SIDE)),
        "light_doc_mean_luma": _env_float("OCR_LIGHT_DOC_MEAN_LUMA", float(OCR_LIGHT_DOC_MEAN_LUMA)),
    }


def _env_bool_from_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _preprocess_params_from_config(cfg: dict) -> dict:
    return {
        "scale_min_side": int(cfg.get("scale_min_side", 0)),
        "contrast": float(cfg.get("contrast", 1.5)),
        "sharpness": float(cfg.get("sharpness", 1.0)),
        "binarize": str(cfg.get("binarize", "off")).lower(),
        "binarize_block_size": int(cfg.get("binarize_block_size", 31)),
        "binarize_c": int(cfg.get("binarize_c", 10)),
        "binarize_invert": str(cfg.get("binarize_invert", "auto")).lower(),
        "binarize_morph": str(cfg.get("binarize_morph", "off")).lower(),
    }


def _dark_ui_preprocess_params(cfg: dict) -> dict:
    return _preprocess_params_from_config(cfg)


def _low_res_preprocess_params(cfg: dict) -> dict:
    base = _dark_ui_preprocess_params(cfg)
    target = max(1, int(cfg.get("low_res_target_side", 1080)))
    base["scale_min_side"] = target
    return base


def _light_doc_preprocess_params(cfg: dict) -> dict:
    base = _dark_ui_preprocess_params(cfg)
    base["contrast"] = min(base["contrast"], 1.2)
    base["binarize"] = "otsu"
    return base


def select_preprocess_profile(img, cfg: dict) -> tuple[str, dict]:
    """
    單次開圖內選擇前處理 profile（預設關閉 router → 一律 dark_ui）。
  回傳 (profile_name, preprocess_params)。
    """
    if not cfg.get("preset_router_enabled"):
        return "dark_ui", _dark_ui_preprocess_params(cfg)

    from PIL import Image

    if not isinstance(img, Image.Image):
        raise TypeError("select_preprocess_profile 需要 PIL.Image。")

    w, h = img.size
    short = min(w, h)
    gray = img.convert("L")
    pixels = list(gray.getdata())
    mean_luma = float(sum(pixels)) / max(1, len(pixels))

    if short < int(cfg.get("low_res_short_side", 720)):
        return "low_res", _low_res_preprocess_params(cfg)
    if mean_luma > float(cfg.get("light_doc_mean_luma", 180.0)):
        return "light_doc", _light_doc_preprocess_params(cfg)
    return "dark_ui", _dark_ui_preprocess_params(cfg)


def build_ocr_signature(cfg: dict, *, profile: str = "dark_ui", preprocess: dict | None = None) -> str:
    override = str(cfg.get("signature_override") or "").strip()
    if override:
        return override
    params = preprocess or _dark_ui_preprocess_params(cfg)
    lang = str(cfg.get("lang") or "chi_tra+eng").strip()
    psm = normalize_psm(str(cfg.get("psm")))
    pre = str(cfg.get("preprocess_version") or "v1").strip()
    scale = str(max(0, int(params.get("scale_min_side", 0))))
    contrast = str(params.get("contrast", 1.5))
    sharp = str(params.get("sharpness", 1.0))
    binarize = str(params.get("binarize", "off")).lower() or "off"
    return (
        f"tesseract|lang={lang}|psm={psm}|pre={pre}|profile={profile}"
        f"|scale={scale}|ctr={contrast}|shp={sharp}|bin={binarize}"
    )


def broadcast_ocr_runtime_config(spark: SparkSession, cfg: dict | None = None):
    """將 OCR 設定 broadcast 給 Spark executor（避免 worker 讀不到 driver .env）。"""
    payload = cfg or build_ocr_runtime_config()
    return spark.sparkContext.broadcast(payload)


def _apply_binarization(gray_img, preprocess: dict):
    """灰階 PIL Image → 二值化（preprocess['binarize']=otsu|adaptive；off 則原樣回傳）。"""
    mode = str(preprocess.get("binarize", "off")).lower()
    if mode in ("off", "none", ""):
        return gray_img

    try:
        import cv2
        import numpy as np
        from PIL import Image
    except ImportError:
        return gray_img

    arr = np.array(gray_img)
    if mode == "otsu":
        _, binary = cv2.threshold(arr, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    elif mode == "adaptive":
        block = max(3, int(preprocess.get("binarize_block_size", 31)))
        if block % 2 == 0:
            block += 1
        c = int(preprocess.get("binarize_c", 10))
        binary = cv2.adaptiveThreshold(
            arr,
            255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY,
            block,
            c,
        )
    else:
        return gray_img

    invert = str(preprocess.get("binarize_invert", "auto")).lower()
    if invert == "auto":
        if float(np.mean(binary)) < 127.0:
            binary = cv2.bitwise_not(binary)
    elif invert in ("1", "true", "yes", "on"):
        binary = cv2.bitwise_not(binary)

    morph = str(preprocess.get("binarize_morph", "off")).lower()
    if morph == "open":
        kernel = np.ones((2, 2), np.uint8)
        binary = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel)

    return Image.fromarray(binary)


def preprocess_image_for_ocr(
    img,
    *,
    runtime_config: dict | None = None,
    preprocess_params: dict | None = None,
):
    """
    Bronze OCR 前處理（可由 runtime_config / preset profile 調校）。
    """
    from PIL import Image, ImageEnhance

    cfg = runtime_config or build_ocr_runtime_config()
    if preprocess_params is None:
        _profile, preprocess_params = select_preprocess_profile(img, cfg)

    scale_min = max(0, int(preprocess_params.get("scale_min_side", 0)))
    if scale_min > 0:
        w, h = img.size
        short = min(w, h)
        if 0 < short < scale_min:
            factor = scale_min / short
            new_size = (max(1, int(w * factor)), max(1, int(h * factor)))
            img = img.resize(new_size, Image.Resampling.LANCZOS)

    img = img.convert("L")

    contrast = float(preprocess_params.get("contrast", 1.5))
    if contrast != 1.0:
        img = ImageEnhance.Contrast(img).enhance(contrast)

    sharpness = float(preprocess_params.get("sharpness", 1.0))
    if sharpness != 1.0:
        img = ImageEnhance.Sharpness(img).enhance(sharpness)

    return _apply_binarization(img, preprocess_params)


_ocr_user_words_registered: str | None = None
_ocr_user_words_worker_path: str | None = None


def register_ocr_user_words_if_needed(
    spark: SparkSession,
    *,
    ocr_user_words_path: str | None = None,
    dataset_id: str | None = None,
) -> str | None:
    """
    於 driver 合併內建／檔案 OCR 詞彙，addFile 分發給 executors。
    回傳 driver 端暫存路徑（供本機單執行緒測試）；Spark UDF 會從 SparkFiles 讀取。
    """
    global _ocr_user_words_registered

    extra_paths: list[str] = []
    env_path = str(ocr_user_words_path or OCR_USER_WORDS_PATH or "").strip()
    if env_path:
        resolved = _resolve_readable_words_path(spark, env_path)
        if resolved:
            extra_paths.append(resolved)

    pattern = os.getenv("OCR_USER_WORDS_DATASET_PATTERN", "").strip()
    ds = str(dataset_id or "").strip().lower()
    if ds and pattern:
        try:
            candidate = pattern.format(dataset_id=ds).strip()
            if candidate:
                resolved = _resolve_readable_words_path(spark, candidate)
                if resolved:
                    extra_paths.append(resolved)
        except Exception:
            pass

    local_path = resolve_local_ocr_user_words_path(ds) if ds else None
    if local_path:
        extra_paths.append(local_path)

    merged_path = materialize_merged_ocr_user_words_file(
        extra_paths=extra_paths,
        dataset_ids=[ds] if ds else None,
    )
    if not merged_path:
        return None

    if _ocr_user_words_registered != merged_path:
        spark.sparkContext.addFile(merged_path)
        _ocr_user_words_registered = merged_path
    return merged_path


def _resolve_readable_words_path(spark: SparkSession, path: str) -> str | None:
    """將本機或 s3a:// 詞彙檔轉成 driver 可讀的暫存路徑。"""
    raw = str(path or "").strip()
    if not raw:
        return None
    if os.path.isfile(raw):
        return raw
    if not raw.startswith("s3a://"):
        return None
    try:
        import tempfile

        lines = spark.read.text(raw).collect()
        fd, out_path = tempfile.mkstemp(suffix="_ocr_user_words_s3a.txt", prefix="ocr_words_")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            for row in lines:
                w = str(row[0]).strip()
                if not w or w.startswith("#"):
                    continue
                fh.write(f"{w}\n")
        return out_path
    except Exception:
        return None


def _resolve_ocr_user_words_path_for_worker() -> str:
    """於 OCR UDF 內取得 --user-words 路徑（含內建詞 fallback）。"""
    global _ocr_user_words_worker_path
    if _ocr_user_words_worker_path is not None:
        return _ocr_user_words_worker_path

    basename = os.path.basename(_ocr_user_words_registered or "")
    if basename:
        try:
            from pyspark import SparkFiles

            local = SparkFiles.get(basename)
            if local and os.path.isfile(local):
                _ocr_user_words_worker_path = local
                return local
        except Exception:
            pass

    if _ocr_user_words_registered and os.path.isfile(_ocr_user_words_registered):
        _ocr_user_words_worker_path = _ocr_user_words_registered
        return _ocr_user_words_registered

    fallback = materialize_merged_ocr_user_words_file()
    _ocr_user_words_worker_path = fallback or ""
    return _ocr_user_words_worker_path


def _build_tesseract_config(psm: str, user_words_path: str | None = None) -> str:
    psm_norm = normalize_psm(psm)
    config = f"--psm {psm_norm}"
    path = user_words_path
    if not path:
        path = _resolve_ocr_user_words_path_for_worker()
    if path:
        config += f' --user-words "{path}"'
    return config


def ocr_image_bytes_with_meta(
    image_content,
    *,
    psm: str | None = None,
    user_words_path: str | None = None,
    runtime_config: dict | None = None,
) -> dict[str, str] | None:
    """
    將影像二進位轉成 OCR 文字與 per-row ocr_signature（依實際 profile／前處理參數）。
  單張逾時見 OCR_TIMEOUT_SECONDS（0=關閉）。
    """

    def _run() -> dict[str, str] | None:
        return _ocr_image_bytes_with_meta_impl(
            image_content,
            psm=psm,
            user_words_path=user_words_path,
            runtime_config=runtime_config,
        )

    result = run_ocr_with_timeout(_run, timeout_seconds=OCR_TIMEOUT_SECONDS)
    if result is not None:
        return result
    if image_content is None:
        return None
    cfg = runtime_config or build_ocr_runtime_config()
    sig = build_ocr_signature(cfg, profile="dark_ui", preprocess=_dark_ui_preprocess_params(cfg))
    return {"extracted_text": "OCR_ERROR_TIMEOUT", "ocr_signature": sig}


def _ocr_image_bytes_with_meta_impl(
    image_content,
    *,
    psm: str | None = None,
    user_words_path: str | None = None,
    runtime_config: dict | None = None,
) -> dict[str, str] | None:
    """OCR 實作本體（不含逾時包裝）。"""
    try:
        import pytesseract
        from io import BytesIO

        from PIL import Image

        cfg = runtime_config or build_ocr_runtime_config()
        cmd = str(cfg.get("tesseract_cmd") or "").strip()
        if cmd:
            pytesseract.pytesseract.tesseract_cmd = cmd

        ocr_lang = str(cfg.get("lang") or "chi_tra+eng")
        ocr_psm = normalize_psm(psm, default=str(cfg.get("psm")))

        if image_content is None:
            return None

        if isinstance(image_content, memoryview):
            data = image_content.tobytes()
        elif isinstance(image_content, bytearray):
            data = bytes(image_content)
        else:
            data = bytes(image_content)

        buf = BytesIO(data)
        buf.seek(0)
        img = Image.open(buf)
        profile, params = select_preprocess_profile(img, cfg)
        img = preprocess_image_for_ocr(img, runtime_config=cfg, preprocess_params=params)

        tesseract_config = _build_tesseract_config(ocr_psm, user_words_path)
        text = pytesseract.image_to_string(img, lang=ocr_lang, config=tesseract_config)
        result = text.strip() or "OCR_EMPTY_RESULT"
        signature = build_ocr_signature(cfg, profile=profile, preprocess=params)
        return {"extracted_text": result, "ocr_signature": signature}

    except ImportError as ie:
        err = f"OCR_ERROR_IMPORT: {ie}"
        cfg = runtime_config or build_ocr_runtime_config()
        sig = build_ocr_signature(cfg, profile="dark_ui", preprocess=_dark_ui_preprocess_params(cfg))
        return {"extracted_text": err, "ocr_signature": sig}
    except Exception as e:
        err = f"OCR_ERROR_REAL: {e}"
        cfg = runtime_config or build_ocr_runtime_config()
        sig = build_ocr_signature(cfg, profile="dark_ui", preprocess=_dark_ui_preprocess_params(cfg))
        return {"extracted_text": err, "ocr_signature": sig}


def ocr_image_bytes(
    image_content,
    *,
    psm: str | None = None,
    user_words_path: str | None = None,
    runtime_config: dict | None = None,
) -> Optional[str]:
    """
    將影像二進位內容轉成文字（driver 或 Spark UDF 皆可呼叫）。
    runtime_config 應由 driver broadcast；未傳入時於本機組裝（測試／AB 頁）。
    """
    meta = ocr_image_bytes_with_meta(
        image_content,
        psm=psm,
        user_words_path=user_words_path,
        runtime_config=runtime_config,
    )
    return meta["extracted_text"] if meta else None


def make_ocr_result_udf(config_bc):
    """建立回傳 (extracted_text, ocr_signature) 的 Spark UDF。"""

    def _ocr_binary_to_row(image_content):
        meta = ocr_image_bytes_with_meta(image_content, runtime_config=config_bc.value)
        if meta is None:
            return None
        return (meta["extracted_text"], meta["ocr_signature"])

    return udf(_ocr_binary_to_row, _OCR_RESULT_SCHEMA)


def make_ocr_udf(config_bc):
    """建立綁定 broadcast OCR 設定的 Spark UDF（僅文字，相容舊呼叫）。"""

    def _ocr_binary_to_text(image_content) -> Optional[str]:
        return ocr_image_bytes(image_content, runtime_config=config_bc.value)

    return udf(_ocr_binary_to_text, StringType())


def _get_ocr_signature(cfg: dict | None = None) -> str:
    """相容舊呼叫；預設 dark_ui profile（router 關閉時整批一致）。"""
    base = cfg or build_ocr_runtime_config()
    return build_ocr_signature(base, profile="dark_ui", preprocess=_dark_ui_preprocess_params(base))


def _extract_bucket_and_prefix(raw_images_path: str) -> tuple[str, str]:
    path = (raw_images_path or "").strip()
    if not path.startswith("s3a://"):
        raise ValueError("raw_images_path 必須是 s3a://bucket/prefix 形式。")
    u = urlparse(path)
    bucket = (u.netloc or "").strip()
    prefix = (u.path or "").lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    if not bucket:
        raise ValueError("raw_images_path 缺少 bucket。")
    return bucket, prefix


def normalize_bronze_image_paths(
    image_paths: list[str] | None,
    *,
    raw_images_path: str,
) -> list[str]:
    """將檔名或相對路徑正規化為完整 s3a:// image_path（供 merge 子集 OCR）。"""
    if not image_paths:
        return []
    base = str(raw_images_path or "").strip().replace("\\", "/")
    if not base.endswith("/"):
        base += "/"
    bucket = BUCKET_NAME
    if base.startswith("s3a://"):
        bucket, _ = _extract_bucket_and_prefix(base)

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in image_paths:
        p = str(raw or "").strip().replace("\\", "/")
        if not p or p in seen:
            continue
        if p.startswith("s3a://"):
            full = p
        elif p.startswith("raw/"):
            full = f"s3a://{bucket}/{p.lstrip('/')}"
        elif "/" in p:
            full = f"s3a://{bucket}/raw/images/{p.lstrip('/')}"
        else:
            full = f"{base}{p}" if base.startswith("s3a://") else f"s3a://{bucket}/{base.lstrip('/')}{p}"
        if full in seen:
            continue
        seen.add(full)
        normalized.append(full)
    return normalized


def _bronze_table_exists(spark: SparkSession, table_path: str) -> bool:
    try:
        return bool(spark._jsparkSession.catalog().tableExists(f"delta.`{table_path}`"))
    except Exception:
        return bool(DeltaTable.isDeltaTable(spark, table_path))


def should_archive_bronze_before_merge(
    *,
    enabled: bool | None = None,
    history_path: str | None = None,
) -> bool:
    """merge 前是否歸檔至 history（路徑先設定；表於第一次 append 時懶建立）。"""
    on = BRONZE_HISTORY_ON_MERGE if enabled is None else enabled
    path = (history_path if history_path is not None else BRONZE_HISTORY_PATH) or ""
    return bool(on) and bool(str(path).strip())


def resolve_bronze_history_path() -> str:
    return str(BRONZE_HISTORY_PATH or "").strip()


def _archive_bronze_rows_before_merge(
    spark: SparkSession,
    *,
    bronze_path: str,
    history_path: str,
    df_ocr,
) -> int:
    """
    MERGE 更新前，將將被覆寫的既有列 append 至 history（懶建立 Delta 表）。
    僅處理 image_path 已存在於 Bronze 的列；純 INSERT 不寫 history。
    """
    if not should_archive_bronze_before_merge(history_path=history_path):
        return 0

    path = str(history_path).strip()
    paths = [row["image_path"] for row in df_ocr.select("image_path").distinct().collect()]
    if not paths:
        return 0

    df_old = (
        spark.read.format("delta")
        .load(bronze_path)
        .filter(col("image_path").isin(paths))
    )
    if int(df_old.limit(1).count()) == 0:
        return 0

    archived_rows = int(df_old.count())
    (
        df_old.withColumn("archived_at", current_timestamp())
        .withColumn("archive_reason", lit("pre_merge"))
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(path)
    )
    return archived_rows


def _write_bronze_merge(spark: SparkSession, bronze_path: str, df_ocr) -> dict:
    """Delta MERGE by image_path（局部覆寫子集，不動其餘列）。"""
    history_path = resolve_bronze_history_path()
    out = {
        "history_archived_rows": 0,
        "bronze_history_path": history_path if should_archive_bronze_before_merge() else "",
    }
    if int(df_ocr.limit(1).count()) == 0:
        return out

    if not _bronze_table_exists(spark, bronze_path):
        (
            df_ocr.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(bronze_path)
        )
        return out

    existing_cols = set(spark.read.format("delta").load(bronze_path).columns)
    for name, dtype in (
        ("file_hash", "STRING"),
        ("dataset_id", "STRING"),
        ("ocr_signature", "STRING"),
    ):
        if name not in existing_cols:
            spark.sql(f"ALTER TABLE delta.`{bronze_path}` ADD COLUMNS ({name} {dtype})")

    out["history_archived_rows"] = _archive_bronze_rows_before_merge(
        spark,
        bronze_path=bronze_path,
        history_path=history_path,
        df_ocr=df_ocr,
    )

    delta_table = DeltaTable.forPath(spark, bronze_path)
    update_set = {
        "extracted_text": col("source.extracted_text"),
        "ocr_signature": col("source.ocr_signature"),
        "ingestion_timestamp": col("source.ingestion_timestamp"),
        "source_bucket": col("source.source_bucket"),
        "file_hash": col("source.file_hash"),
        "dataset_id": col("source.dataset_id"),
    }
    insert_values = {
        "image_path": col("source.image_path"),
        "extracted_text": col("source.extracted_text"),
        "ocr_signature": col("source.ocr_signature"),
        "ingestion_timestamp": col("source.ingestion_timestamp"),
        "source_bucket": col("source.source_bucket"),
        "file_hash": col("source.file_hash"),
        "dataset_id": col("source.dataset_id"),
    }
    (
        delta_table.alias("target")
        .merge(df_ocr.alias("source"), "target.image_path = source.image_path")
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
    return out


def resolve_ocr_minio_batch_size(
    *,
    batch_size: int | None = None,
    max_bronze_images: int | None = None,
) -> int:
    """MinIO SDK fallback 每批讀圖上限（不超過 Pipeline Guard 的 Bronze 張數上限）。"""
    batch = max(1, int(batch_size if batch_size is not None else OCR_MINIO_BATCH_SIZE))
    cap = max(1, int(max_bronze_images if max_bronze_images is not None else MAX_BRONZE_OCR_IMAGES))
    return min(batch, cap)


def chunk_s3a_paths(paths: list[str], batch_size: int) -> list[list[str]]:
    """將路徑清單切成固定大小批次（純函數，供分批讀圖）。"""
    n = max(1, int(batch_size))
    items = [str(p).strip() for p in paths if str(p).strip()]
    return [items[i : i + n] for i in range(0, len(items), n)]


def _parse_s3a_uri(s3a_path: str) -> tuple[str, str]:
    path = str(s3a_path or "").strip()
    if not path.startswith("s3a://"):
        raise ValueError(f"image_path 必須為 s3a:// 形式：{path!r}")
    u = urlparse(path)
    bucket = (u.netloc or "").strip()
    key = (u.path or "").lstrip("/")
    if not bucket or not key:
        raise ValueError(f"無法解析 s3a URI：{path!r}")
    return bucket, key


def _binaryfile_image_df(spark: SparkSession, raw_images_path: str):
    """Spark binaryFile 可見的影像路徑（不含 MinIO SDK fallback）。"""
    return (
        spark.read.format("binaryFile")
        .load(raw_images_path)
        .filter(
            lower(col("path")).rlike(r".*\.(png|jpg|jpeg|bmp|gif|webp|tif|tiff)$")
        )
        .select(col("path").alias("image_path"))
    )


def count_binaryfile_images(
    spark: SparkSession,
    raw_images_path: str,
    *,
    limit: int | None = None,
) -> int:
    """純 Spark binaryFile 可見影像數（不 fallback MinIO SDK）。"""
    df_paths = _binaryfile_image_df(spark, raw_images_path)
    try:
        if limit is not None:
            return int(df_paths.limit(max(1, int(limit))).count())
        return int(df_paths.count())
    except Exception:
        return 0


def _spark_binaryfile_has_images(spark: SparkSession, raw_images_path: str) -> bool:
    """Spark S3A binaryFile 是否至少能讀到一張圖。"""
    try:
        return count_binaryfile_images(spark, raw_images_path, limit=1) > 0
    except Exception:
        return False


def resolve_bronze_image_source(spark: SparkSession, raw_images_path: str) -> str:
    """bronze OCR 原圖來源：binaryFile（主）或 minio_sdk（fallback）。"""
    if _spark_binaryfile_has_images(spark, raw_images_path):
        return "binaryFile"
    return "minio_sdk"


def _list_minio_image_s3a_paths(
    raw_images_path: str,
    *,
    only_paths: list[str] | None = None,
    max_count: int | None = None,
) -> list[str]:
    """僅列出 MinIO 下影像 s3a 路徑，不讀取 bytes。merge 子集時只回傳指定路徑（不 list 全目錄）。"""
    if only_paths:
        paths = []
        seen: set[str] = set()
        for raw in only_paths:
            p = str(raw or "").strip()
            if not p or p in seen:
                continue
            seen.add(p)
            paths.append(p)
        if max_count is not None:
            return paths[: max(1, int(max_count))]
        return paths

    bucket, prefix = _extract_bucket_and_prefix(raw_images_path)
    client = get_minio_client()
    ensure_bucket(client, bucket)

    limit = None if max_count is None else max(1, int(max_count))
    paths: list[str] = []
    try:
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
            name = getattr(obj, "object_name", "") or ""
            if not name or name.endswith("/"):
                continue
            if not _has_supported_image_extension(name):
                continue
            full = f"s3a://{bucket}/{name}"
            paths.append(full)
            if limit is not None and len(paths) >= limit:
                break
    except S3Error as e:
        raise RuntimeError(f"MinIO SDK 列舉影像失敗：{e}") from e
    return paths


def _read_minio_image_rows(s3a_paths: list[str]) -> list[dict]:
    """依 s3a 路徑點讀影像 bytes（不 list 全目錄）。"""
    if not s3a_paths:
        return []
    client = get_minio_client()
    rows: list[dict] = []
    for s3a_path in s3a_paths:
        bucket, key = _parse_s3a_uri(s3a_path)
        try:
            resp = client.get_object(bucket, key)
            try:
                data = resp.read()
            finally:
                resp.close()
                resp.release_conn()
        except S3Error as e:
            raise RuntimeError(f"MinIO SDK 讀取影像失敗 {s3a_path}：{e}") from e
        if not _looks_like_image_bytes(data):
            continue
        rows.append({"image_path": s3a_path, "image_content": data})
    return rows


def _list_and_read_via_minio(
    raw_images_path: str,
    limit: int | None = None,
    *,
    only_s3a_paths: list[str] | None = None,
) -> list[dict]:
    """
    使用 MinIO SDK 列檔並讀取 bytes。回傳 list[{"image_path","image_content"}]。
    預覽／小樣本用；Bronze OCR 大量時請走分批 ingest（resolve_bronze_image_source）。
    """
    if only_s3a_paths:
        paths = [str(p).strip() for p in only_s3a_paths if str(p).strip()]
        if limit is not None:
            paths = paths[: max(1, int(limit))]
        return _read_minio_image_rows(paths)

    paths = _list_minio_image_s3a_paths(raw_images_path, max_count=limit)
    return _read_minio_image_rows(paths)


def _read_df_paths_binaryfile(spark: SparkSession, raw_images_path: str):
    """Spark binaryFile 讀取原圖（主路徑）。"""
    return (
        spark.read.format("binaryFile")
        .load(raw_images_path)
        .filter(
            lower(col("path")).rlike(r".*\.(png|jpg|jpeg|bmp|gif|webp|tif|tiff)$")
        )
        .select(
            col("path").alias("image_path"),
            col("content").alias("image_content"),
        )
    )


def _build_df_paths(spark: SparkSession, raw_images_path: str):
    """
    優先使用 Spark binaryFile；若為 0 筆則 fallback 到 MinIO SDK（整批，僅供預覽相容）。
    Bronze OCR 主流程請用 resolve_bronze_image_source + 分批 ingest。
    """
    if _spark_binaryfile_has_images(spark, raw_images_path):
        return _read_df_paths_binaryfile(spark, raw_images_path)

    rows = _list_and_read_via_minio(raw_images_path, limit=None)
    if not rows:
        return spark.createDataFrame([], "image_path string, image_content binary")
    return spark.createDataFrame(rows)


def _apply_append_skip_keys(spark: SparkSession, bronze_path: str, df_base):
    """append 模式：略過 Bronze 已有鍵。"""
    try:
        df_existing = spark.read.format("delta").load(bronze_path)
        cols = set(df_existing.columns)
        if {"dataset_id", "file_hash", "ocr_signature"}.issubset(cols):
            key_cols = ["dataset_id", "file_hash", "ocr_signature"]
            existing_keys = df_existing.select(*key_cols).dropDuplicates()
            return df_base.join(existing_keys, on=key_cols, how="left_anti")
        if "image_path" in cols:
            existing_keys = df_existing.select("image_path").dropDuplicates()
            return df_base.join(existing_keys, on=["image_path"], how="left_anti")
    except Exception:
        pass
    return df_base


def _ocr_and_write_bronze_batch(
    spark: SparkSession,
    df_paths,
    *,
    bronze_path: str,
    write_mode: str,
    ocr_result_udf,
    inferred_ds: str,
    apply_append_skip: bool,
) -> dict:
    """單批：OCR + 寫入 Bronze。回傳本批統計。"""
    df_base = (
        df_paths.withColumn("file_hash", sha2(col("image_content"), 256))
        .withColumn("dataset_id", regexp_extract(col("image_path"), r"/raw/images/([^/]+)/", 1))
    )
    if inferred_ds:
        df_base = df_base.withColumn("dataset_id", lit(inferred_ds))

    total_input = int(df_base.count())
    if total_input == 0:
        return {
            "input_rows": 0,
            "processed_rows": 0,
            "skipped_rows": 0,
            "ocr_error_rows_dropped": 0,
            "write_rows": 0,
            "history_archived_rows": 0,
            "bronze_history_path": "",
            "ocr_repartition": 0,
        }

    if apply_append_skip and write_mode in ("append", "overwrite"):
        df_base = _apply_append_skip_keys(spark, bronze_path, df_base)

    candidate_rows = int(df_base.count())
    skipped_rows = max(0, total_input - candidate_rows)
    if candidate_rows == 0:
        return {
            "input_rows": total_input,
            "processed_rows": 0,
            "skipped_rows": skipped_rows,
            "ocr_error_rows_dropped": 0,
            "write_rows": 0,
            "history_archived_rows": 0,
            "bronze_history_path": "",
            "ocr_repartition": 0,
        }

    df_base, ocr_repartition = apply_ocr_repartition(df_base)

    df_ocr = (
        df_base.withColumn("ocr_result", ocr_result_udf(col("image_content")))
        .withColumn("extracted_text", col("ocr_result.extracted_text"))
        .withColumn("ocr_signature", col("ocr_result.ocr_signature"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_bucket", lit("raw_images"))
        .drop("image_content", "ocr_result")
    )
    ocr_error_rows = int(df_ocr.filter(col("extracted_text").startswith("OCR_ERROR_")).count())
    df_ocr = df_ocr.filter(~col("extracted_text").startswith("OCR_ERROR_"))
    write_rows = int(df_ocr.count())
    if write_rows == 0:
        return {
            "input_rows": total_input,
            "processed_rows": 0,
            "skipped_rows": skipped_rows,
            "ocr_error_rows_dropped": ocr_error_rows,
            "write_rows": 0,
            "history_archived_rows": 0,
            "bronze_history_path": "",
            "ocr_repartition": ocr_repartition,
        }

    if write_mode == "merge":
        merge_info = _write_bronze_merge(spark, bronze_path, df_ocr)
    else:
        merge_info = {"history_archived_rows": 0, "bronze_history_path": ""}
        writer = df_ocr.write.format("delta").mode(write_mode)
        if write_mode == "append":
            writer = writer.option("mergeSchema", "true")
        else:
            writer = writer.option("overwriteSchema", "true")
        writer.save(bronze_path)

    return {
        "input_rows": total_input,
        "processed_rows": write_rows,
        "skipped_rows": skipped_rows,
        "ocr_error_rows_dropped": ocr_error_rows,
        "write_rows": write_rows,
        "history_archived_rows": int(merge_info.get("history_archived_rows") or 0),
        "bronze_history_path": str(merge_info.get("bronze_history_path") or ""),
        "ocr_repartition": ocr_repartition,
    }


def _run_bronze_ocr_minio_batched(
    spark: SparkSession,
    *,
    raw_images_path: str,
    bronze_path: str,
    write_mode: str,
    ocr_result_udf,
    inferred_ds: str,
    normalized_paths: list[str],
    batch_sig: str,
    ocr_cfg: dict,
    batch_size: int,
) -> dict:
    """MinIO SDK fallback：分批讀圖 → OCR → 寫 Bronze（每批釋放 driver 記憶體）。"""
    if normalized_paths:
        all_paths = _list_minio_image_s3a_paths(
            raw_images_path,
            only_paths=normalized_paths,
        )
    else:
        all_paths = _list_minio_image_s3a_paths(raw_images_path)

    path_batches = chunk_s3a_paths(all_paths, batch_size)
    _logger.info(
        "bronze_ocr_minio_batched: paths=%s batches=%s batch_size=%s write_mode=%s",
        len(all_paths),
        len(path_batches),
        batch_size,
        write_mode,
    )

    totals = {
        "input_rows": 0,
        "processed_rows": 0,
        "skipped_rows": 0,
        "ocr_error_rows_dropped": 0,
        "history_archived_rows": 0,
    }
    bronze_history_path = ""
    batches_processed = 0
    wrote_any = False
    effective_mode = write_mode
    ocr_repartition_applied = 0

    for batch_paths in path_batches:
        rows = _read_minio_image_rows(batch_paths)
        if not rows:
            continue
        df_paths = spark.createDataFrame(rows)

        if effective_mode == "overwrite":
            batch_write_mode = "append" if wrote_any else "overwrite"
        else:
            batch_write_mode = effective_mode

        batch_stats = _ocr_and_write_bronze_batch(
            spark,
            df_paths,
            bronze_path=bronze_path,
            write_mode=batch_write_mode,
            ocr_result_udf=ocr_result_udf,
            inferred_ds=inferred_ds,
            apply_append_skip=(write_mode == "append"),
        )
        batches_processed += 1
        if int(batch_stats.get("write_rows") or 0) > 0:
            wrote_any = True

        totals["input_rows"] += int(batch_stats.get("input_rows") or 0)
        totals["processed_rows"] += int(batch_stats.get("processed_rows") or 0)
        totals["skipped_rows"] += int(batch_stats.get("skipped_rows") or 0)
        totals["ocr_error_rows_dropped"] += int(batch_stats.get("ocr_error_rows_dropped") or 0)
        totals["history_archived_rows"] += int(batch_stats.get("history_archived_rows") or 0)
        if batch_stats.get("bronze_history_path"):
            bronze_history_path = str(batch_stats["bronze_history_path"])
        ocr_repartition_applied = max(
            ocr_repartition_applied,
            int(batch_stats.get("ocr_repartition") or 0),
        )

    return {
        "input_rows": totals["input_rows"],
        "processed_rows": totals["processed_rows"],
        "skipped_rows": totals["skipped_rows"],
        "ocr_error_rows_dropped": totals["ocr_error_rows_dropped"],
        "ocr_signature": batch_sig,
        "write_mode": write_mode,
        "image_paths": normalized_paths,
        "per_row_ocr_signature": True,
        "history_archived_rows": totals["history_archived_rows"],
        "bronze_history_path": bronze_history_path,
        "image_source": "minio_sdk",
        "minio_batches_processed": batches_processed,
        "ocr_minio_batch_size": batch_size,
        "ocr_runtime_config": {
            "psm": ocr_cfg.get("psm"),
            "preprocess_version": ocr_cfg.get("preprocess_version"),
            "preset_router_enabled": ocr_cfg.get("preset_router_enabled"),
        },
        **_bronze_p4_runtime_meta(ocr_repartition_applied=ocr_repartition_applied),
    }


def _bronze_p4_runtime_meta(*, ocr_repartition_applied: int = 0) -> dict:
    """P4 執行期節流：API 回傳用（config + 本批實際套用）。"""
    return {
        "ocr_timeout_seconds": int(OCR_TIMEOUT_SECONDS),
        "spark_job_timeout_seconds": int(SPARK_JOB_TIMEOUT_SECONDS),
        "ocr_repartition_config": resolve_ocr_repartition(),
        "ocr_repartition_applied": int(ocr_repartition_applied),
    }


def run_bronze_ocr_ingest(
    spark: SparkSession,
    *,
    raw_images_path: str,
    bronze_path: str,
    write_mode: str = "overwrite",
    dataset_id: str | None = None,
    image_paths: list[str] | None = None,
) -> dict:
    """
    從 raw_images_path（s3a://.../ 目錄，內含圖檔）讀取影像 → Tesseract → 寫入 bronze_path。

    原圖來源：Spark binaryFile（主）或 MinIO SDK 分批 fallback（見 resolve_bronze_image_source）。

    write_mode:
      - \"overwrite\"：全表覆寫
      - \"append\"：追加（可跳過已處理鍵）
      - \"merge\"：依 image_path Upsert 子集（須提供 image_paths）
    """

    if write_mode not in ("overwrite", "append", "merge"):
        raise ValueError('write_mode 必須是 \"overwrite\"、\"append\" 或 \"merge\"。')
    if write_mode == "merge" and not image_paths:
        raise ValueError('write_mode=\"merge\" 時必須提供 image_paths（至少一筆）。')

    inferred_ds = str(dataset_id or "").strip()
    if not inferred_ds:
        m = re.search(r"/raw/images/([^/]+)/?", raw_images_path.replace("\\", "/"))
        if m:
            inferred_ds = m.group(1).strip()

    register_ocr_user_words_if_needed(spark, dataset_id=inferred_ds or None)

    ocr_cfg = build_ocr_runtime_config()
    config_bc = broadcast_ocr_runtime_config(spark, ocr_cfg)
    ocr_result_udf = make_ocr_result_udf(config_bc)
    normalized_paths = normalize_bronze_image_paths(image_paths, raw_images_path=raw_images_path)
    batch_sig = _get_ocr_signature(ocr_cfg)
    batch_size = resolve_ocr_minio_batch_size()
    image_source = resolve_bronze_image_source(spark, raw_images_path)

    if image_source == "minio_sdk":
        return _run_bronze_ocr_minio_batched(
            spark,
            raw_images_path=raw_images_path,
            bronze_path=bronze_path,
            write_mode=write_mode,
            ocr_result_udf=ocr_result_udf,
            inferred_ds=inferred_ds,
            normalized_paths=normalized_paths,
            batch_sig=batch_sig,
            ocr_cfg=ocr_cfg,
            batch_size=batch_size,
        )

    df_paths = _read_df_paths_binaryfile(spark, raw_images_path)
    if normalized_paths:
        df_paths = df_paths.filter(col("image_path").isin(normalized_paths))

    batch_stats = _ocr_and_write_bronze_batch(
        spark,
        df_paths,
        bronze_path=bronze_path,
        write_mode=write_mode,
        ocr_result_udf=ocr_result_udf,
        inferred_ds=inferred_ds,
        apply_append_skip=(write_mode == "append"),
    )

    if int(batch_stats.get("input_rows") or 0) == 0:
        return {
            "input_rows": 0,
            "processed_rows": 0,
            "skipped_rows": 0,
            "ocr_signature": batch_sig,
            "write_mode": write_mode,
            "image_paths": normalized_paths,
            "image_source": "binaryFile",
            "minio_batches_processed": 0,
            "ocr_minio_batch_size": batch_size,
            **_bronze_p4_runtime_meta(
                ocr_repartition_applied=int(batch_stats.get("ocr_repartition") or 0),
            ),
        }

    return {
        "input_rows": batch_stats["input_rows"],
        "processed_rows": batch_stats["processed_rows"],
        "skipped_rows": batch_stats["skipped_rows"],
        "ocr_error_rows_dropped": batch_stats.get("ocr_error_rows_dropped", 0),
        "ocr_signature": batch_sig,
        "write_mode": write_mode,
        "image_paths": normalized_paths,
        "per_row_ocr_signature": True,
        "history_archived_rows": batch_stats.get("history_archived_rows", 0),
        "bronze_history_path": batch_stats.get("bronze_history_path", ""),
        "image_source": "binaryFile",
        "minio_batches_processed": 0,
        "ocr_minio_batch_size": batch_size,
        "ocr_runtime_config": {
            "psm": ocr_cfg.get("psm"),
            "preprocess_version": ocr_cfg.get("preprocess_version"),
            "preset_router_enabled": ocr_cfg.get("preset_router_enabled"),
        },
        **_bronze_p4_runtime_meta(
            ocr_repartition_applied=int(batch_stats.get("ocr_repartition") or 0),
        ),
    }


def preview_raw_images_sample(
    spark: SparkSession,
    raw_images_path: str,
    *,
    limit: int = 5,
) -> list[dict]:
    """回傳即將送 OCR 的檔案路徑與內容長度（不執行 Tesseract，供 dry_run 用）。"""

    lim = max(1, min(int(limit), 50))

    # 先走 Spark binaryFile
    df = (
        spark.read.format("binaryFile")
        .load(raw_images_path)
        .filter(
            lower(col("path")).rlike(r".*\.(png|jpg|jpeg|bmp|gif|webp|tif|tiff)$")
        )
        .select(
            col("path").alias("image_path"),
            length(col("content")).alias("content_length"),
        )
        .orderBy(col("path"))
        .limit(lim)
    )
    rows = [row.asDict(recursive=True) for row in df.collect()]
    if rows:
        return rows

    # Spark 看不到時 fallback 到 MinIO SDK
    sdk_rows = _list_and_read_via_minio(raw_images_path, limit=lim)
    return [
        {"image_path": r["image_path"], "content_length": len(r["image_content"])}
        for r in sdk_rows
    ]
