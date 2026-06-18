"""
依 dataset_id 提供的領域辭典（停用詞等）。

MinIO 上無對應檔案時，仍會套用內建詞表，方便本機／開發先跑通 Gold 再看資料。
上傳 dic/stop_words/{dataset_id}.txt 後會與內建詞合併（檔案可覆寫擴充）。
"""

from __future__ import annotations

from typing import FrozenSet, Iterable, List

# drinks：飲料評論常見中性／產品描述詞（非痛點信號）
_DRINKS_DOMAIN_STOPWORDS: FrozenSet[str] = frozenset(
    w.lower()
    for w in (
        # 正面／中性評價
        "好喝",
        "很好喝",
        "超好喝",
        "推薦",
        "大推",
        "不錯",
        "還不錯",
        "可以",
        "喜歡",
        "喜欢",
        "愛",
        "满意",
        "滿意",
        "回購",
        "回购",
        # 產品／品項
        "珍珠",
        "奶茶",
        "茶",
        "飲料",
        "饮料",
        "咖啡",
        "果汁",
        "紅茶",
        "绿茶",
        "綠茶",
        "乌龙",
        "烏龍",
        "拿鐵",
        "latte",
        "波霸",
        # 規格／口味
        "甜度",
        "半糖",
        "全糖",
        "微糖",
        "少冰",
        "多冰",
        "去冰",
        "正常冰",
        "溫",
        "热",
        "熱",
        "冰",
        "大杯",
        "中杯",
        "小杯",
        # 評論場景雜詞
        "一杯",
        "杯",
        "今天",
        "今天下午",
        "下午",
        "第一次",
        "覺得",
        "感觉",
        "感覺",
        "真的",
        "好好",
        "會不會",
        "會",
        "不會",
        "根本",
        "電話",
        "打電話",
        "反應",
        "星星",
        "一星",
        "二星",
        "三星",
        "四星",
        "五星",
        "評論",
        "评论",
        "google",
        "maps",
        # 角色（保留「店員」供痛點規則；只擋較中性的）
        "消費者",
        "客人",
        "顧客",
        # 從「教育訓練」等片語切出的單字（保留完整痛點由主題規則處理）
        "教育",
        "訓練",
    )
)

_BUILTIN_DOMAIN_STOPWORDS: dict[str, FrozenSet[str]] = {
    "drinks": _DRINKS_DOMAIN_STOPWORDS,
}


def get_builtin_domain_stopwords(dataset_id: str | None) -> List[str]:
    if not dataset_id:
        return []
    key = str(dataset_id).strip().lower()
    words = _BUILTIN_DOMAIN_STOPWORDS.get(key)
    if not words:
        return []
    return sorted(words)


def merge_stopword_lists(*sources: Iterable[str] | None) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for src in sources:
        if not src:
            continue
        for raw in src:
            w = str(raw).strip().lower()
            if not w or w in seen:
                continue
            seen.add(w)
            out.append(w)
    return out
