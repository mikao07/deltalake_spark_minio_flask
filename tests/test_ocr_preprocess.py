"""Bronze OCR 前處理單元測試。"""

from PIL import Image

from services.ocr_spark import preprocess_image_for_ocr


def test_preprocess_scales_up_short_side(monkeypatch):
    monkeypatch.setenv("OCR_SCALE_MIN_SIDE", "200")
    monkeypatch.setenv("OCR_CONTRAST", "1.0")
    monkeypatch.setenv("OCR_SHARPNESS", "1.0")
    img = Image.new("RGB", (100, 50), color=(128, 128, 128))
    out = preprocess_image_for_ocr(img)
    assert out.mode == "L"
    assert min(out.size) == 200


def test_preprocess_no_scale_when_disabled(monkeypatch):
    monkeypatch.setenv("OCR_SCALE_MIN_SIDE", "0")
    monkeypatch.setenv("OCR_CONTRAST", "1.0")
    monkeypatch.setenv("OCR_SHARPNESS", "1.0")
    monkeypatch.setenv("OCR_BINARIZE", "off")
    img = Image.new("RGB", (100, 50), color=(200, 200, 200))
    out = preprocess_image_for_ocr(img)
    assert out.size == (100, 50)


def test_preprocess_otsu_binarize(monkeypatch):
    monkeypatch.setenv("OCR_SCALE_MIN_SIDE", "0")
    monkeypatch.setenv("OCR_CONTRAST", "1.0")
    monkeypatch.setenv("OCR_SHARPNESS", "1.0")
    monkeypatch.setenv("OCR_BINARIZE", "otsu")
    img = Image.new("L", (80, 40), color=180)
    out = preprocess_image_for_ocr(img)
    assert out.mode == "L"
    pixels = set(out.getdata())
    assert pixels.issubset({0, 255})
