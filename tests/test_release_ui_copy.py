from services.release_contract import (
    build_topic_hint,
    format_release_card_subtitle,
    format_release_filter_summary,
)


def test_format_release_filter_summary_plain():
    ctx = {
        "release_id": "drinks-gold-v1",
        "approved_snapshot_at": "2026-06-23T05:29:04.667911",
        "processed_image_count": 50,
    }
    text = format_release_filter_summary(ctx)
    assert "drinks-gold-v1" in text
    assert "50 張截圖" in text
    assert "13:29:04" in text
    assert "release_id" not in text
    assert "processed_image_count" not in text


def test_build_topic_hint_release_success():
    hint = build_topic_hint(
        snapshot_mode="release",
        selected_dataset_id="drinks",
        release_context={
            "release_id": "drinks-gold-v1",
            "approved_snapshot_at": "2026-06-23T05:29:04+00:00",
            "processed_image_count": 50,
        },
        latest_snapshot_at=None,
        topic_rows=[{"topic": "等待", "frequency": 1}],
        approved="2026-06-23T05:29:04+00:00",
    )
    assert "正式對外版" in hint
    assert "50 張截圖" in hint
    assert "發行水位" not in hint


def test_build_topic_hint_preview():
    hint = build_topic_hint(
        snapshot_mode="preview",
        selected_dataset_id="drinks",
        release_context={},
        latest_snapshot_at="2026-06-23T05:29:04+00:00",
        topic_rows=[],
        approved=None,
    )
    assert hint is not None
    assert "試看版" in hint
    assert "13:29:04" in hint


def test_format_release_card_subtitle():
    sub = format_release_card_subtitle(
        {
            "release_id": "drinks-gold-v1",
            "processed_image_count": 50,
            "approved_snapshot_at": "2026-06-23T05:29:04+00:00",
        }
    )
    assert "版本 drinks-gold-v1" in sub
    assert "納入 50 張截圖" in sub
    assert "核准於" in sub
