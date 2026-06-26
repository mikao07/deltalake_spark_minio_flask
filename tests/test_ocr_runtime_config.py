"""Bronze OCR runtime config 與 preset router（階段 0）。"""

from PIL import Image

from services.ocr_spark import (
    build_ocr_runtime_config,
    build_ocr_signature,
    normalize_bronze_image_paths,
    normalize_psm,
    select_preprocess_profile,
)


def test_normalize_psm_defaults_to_six(monkeypatch):
    monkeypatch.delenv("OCR_PSM", raising=False)
    assert normalize_psm(None) == "6"


def test_build_ocr_runtime_config_reads_env(monkeypatch):
    monkeypatch.setenv("OCR_PSM", "6")
    monkeypatch.setenv("OCR_PREPROCESS_VERSION", "v1.1")
    monkeypatch.setenv("OCR_PRESET_ROUTER_ENABLED", "false")
    cfg = build_ocr_runtime_config()
    assert cfg["psm"] == "6"
    assert cfg["preprocess_version"] == "v1.1"
    assert cfg["preset_router_enabled"] is False


def test_build_ocr_signature_includes_profile():
    cfg = build_ocr_runtime_config()
    sig = build_ocr_signature(cfg, profile="dark_ui")
    assert "psm=6" in sig
    assert "profile=dark_ui" in sig
    assert "pre=v1" in sig or "pre=v1.1" in sig


def test_select_preprocess_profile_router_off():
    cfg = build_ocr_runtime_config()
    cfg["preset_router_enabled"] = False
    img = Image.new("RGB", (1080, 1920), color=(20, 20, 20))
    profile, params = select_preprocess_profile(img, cfg)
    assert profile == "dark_ui"
    assert params["scale_min_side"] == 0


def test_select_preprocess_profile_low_res_when_enabled():
    cfg = build_ocr_runtime_config()
    cfg["preset_router_enabled"] = True
    cfg["low_res_short_side"] = 720
    cfg["low_res_target_side"] = 1080
    img = Image.new("RGB", (600, 900), color=(30, 30, 30))
    profile, params = select_preprocess_profile(img, cfg)
    assert profile == "low_res"
    assert params["scale_min_side"] == 1080


def test_build_ocr_signature_differs_by_profile():
    cfg = build_ocr_runtime_config()
    dark = build_ocr_signature(cfg, profile="dark_ui", preprocess={"scale_min_side": 0, "contrast": 1.5, "sharpness": 1.0, "binarize": "off"})
    low = build_ocr_signature(cfg, profile="low_res", preprocess={"scale_min_side": 1080, "contrast": 1.5, "sharpness": 1.0, "binarize": "off"})
    assert "profile=dark_ui" in dark
    assert "profile=low_res" in low
    assert "scale=0" in dark
    assert "scale=1080" in low


def test_normalize_bronze_image_paths_filename_and_s3a():
    raw = "s3a://data-lake/raw/images/drinks/"
    out = normalize_bronze_image_paths(
        ["081401.png", "s3a://data-lake/raw/images/drinks/131558.png"],
        raw_images_path=raw,
    )
    assert out == [
        "s3a://data-lake/raw/images/drinks/081401.png",
        "s3a://data-lake/raw/images/drinks/131558.png",
    ]
