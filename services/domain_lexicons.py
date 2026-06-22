"""
依 dataset_id 提供的領域辭典（停用詞、Jieba 分詞、OCR user-words 等）。

領域停用詞於 Gold 層套用（見 services.lexicon）；Silver 僅用內建虛詞。
MinIO / dic/stop_words/{version}/{dataset_id}.txt 與內建詞合併後，經痛點保護詞扣減為 effective_stop。
"""

from __future__ import annotations

from pathlib import Path
from typing import FrozenSet, Iterable, List, Tuple

_REPO_ROOT = Path(__file__).resolve().parent.parent

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

# TF-IDF Phase A 探索專用：虛詞 + 漏斗 anchor（場景詞）；不扣痛點保護詞。
# 與 analytics_tokens（漏斗用）分離，避免珍珠／店員／自己等佔據 Top。
_DRINKS_TFIDF_EXPLORATION_STOPWORDS: FrozenSet[str] = frozenset(
    w.lower()
    for w in (
        # 敘事／虛詞
        "自己",
        "不用",
        "知道",
        "結果",
        "不要",
        "一下",
        "真的",
        "覺得",
        "感觉",
        "感覺",
        "認為",
        "认为",
        "已经",
        "已經",
        "還是",
        "还是",
        "可以",
        "應該",
        "应该",
        "他們",
        "他们",
        "我們",
        "我们",
        "這樣",
        "这样",
        "那樣",
        "那样",
        "什麼",
        "什么",
        "沒有",
        "没有",
        "就是",
        "因為",
        "因为",
        "所以",
        "如果",
        "但是",
        "但",
        "而且",
        "再",
        "都",
        "也",
        "很",
        "太",
        "更",
        "最",
        "才",
        "只",
        "給",
        "给",
        "讓",
        "让",
        "被",
        "把",
        "對",
        "对",
        "向",
        "從",
        "从",
        "跟",
        "和",
        "與",
        "与",
        "或",
        "而",
        "且",
        "又",
        "嗎",
        "吗",
        "呢",
        "吧",
        "啊",
        "一定",
        "完全",
        "非常",
        "超級",
        "超级",
        "有點",
        "有点",
        "一些",
        "一點",
        "一点",
        "怎麼",
        "怎么",
        "為什麼",
        "为什么",
        "還要",
        "还要",
        "還會",
        "还会",
        "人家",
        "大家",
        "東西",
        "东西",
        "事情",
        "希望",
        "可能",
        "本身",
        # 場景／角色 anchor（漏斗用，TF-IDF 探索無單詞價值）
        "店員",
        "店员",
        "服務人員",
        "服务人员",
        "服務員",
        "服务员",
        "員工",
        "员工",
        "櫃台",
        "柜台",
        "收銀",
        "收银",
        "杯",
        "訂單",
        "订单",
        "點餐",
        "口感",
        "味道",
        "態度",
        "态度",
        "等待",
        "排隊",
        "排队",
        "出餐",
        "速度",
        "製作",
        "制作",
        "等候",
        "分鐘",
        "分钟",
        "時間",
        "时间",
        "今天",
        "明天",
        "昨天",
        "現在",
        "现在",
        "以後",
        "以后",
        "之前",
        "之後",
        "之后",
        "第一次",
        "第二次",
        "第三次",
        # 第二輪補強（TF-IDF Top 殘留敘事詞）
        "只是",
        "另外",
        "到底",
        "兩次",
        "確認",
        "店家",
        "店裡",
        "店里",
        "品項",
        "品项",
        "送到",
        "竟然",
        "然後",
        "然后",
        "最後",
        "最后",
        "直接",
        "馬上",
        "马上",
        "立刻",
        "關於",
        "关于",
        "其實",
        "其实",
        "當然",
        "当然",
        "雖然",
        "虽然",
        "因為",
        "因为",
        "所以",
        "不過",
        "不过",
        "而且",
        "並且",
        "并且",
        "還有",
        "还有",
        "以及",
        "或是",
        "或是",
        "或是",
        "這次",
        "这次",
        "每次",
        "任何",
        "任何",
        "其他",
        "其它",
        "全部",
        "所有",
        "整個",
        "整个",
        "部分",
        "有些",
        "有些",
        "沒什麼",
        "没什么",
        "沒啥",
        "没啥",
    )
)

_BUILTIN_TFIDF_EXPLORATION_STOPWORDS: dict[str, FrozenSet[str]] = {
    "drinks": _DRINKS_TFIDF_EXPLORATION_STOPWORDS,
}

_BUILTIN_DOMAIN_STOPWORDS: dict[str, FrozenSet[str]] = {
    "drinks": _DRINKS_DOMAIN_STOPWORDS,
}

# drinks：Jieba 自訂詞（詞 詞頻 詞性）；避免專有名詞被切開
_DRINKS_JIEBA_TERMS: Tuple[Tuple[str, int, str], ...] = (
    ("服務態度", 10, "n"),
    ("店員態度", 10, "n"),
    ("出餐速度", 10, "n"),
    ("等待時間", 10, "n"),
    ("電子發票", 10, "n"),
    ("行動支付", 10, "n"),
    ("50嵐", 100, "nz"),
    ("清心", 100, "nz"),
    ("可不可", 100, "nz"),
    ("迷客夏", 100, "nz"),
    ("一芳", 100, "nz"),
    ("CoCo", 100, "nz"),
    ("五十嵐", 100, "nz"),
    ("Line Pay", 100, "nz"),
    ("LinePay", 100, "nz"),
    ("Uber Eats", 100, "nz"),
    ("foodpanda", 100, "nz"),
    ("微冰", 50, "n"),
    ("微糖", 50, "n"),
    ("一分糖", 50, "n"),
    ("半糖", 50, "n"),
    ("全糖", 50, "n"),
    ("少冰", 50, "n"),
    ("去冰", 50, "n"),
    ("波霸", 50, "n"),
    ("珍珠奶茶", 50, "n"),
)

# drinks：Tesseract user-words（每行一詞，提升品牌／規格辨識）
_DRINKS_OCR_USER_WORDS: FrozenSet[str] = frozenset(
    {
        "50嵐",
        "五十嵐",
        "清心",
        "可不可",
        "迷客夏",
        "一芳",
        "CoCo",
        "清心福全",
        "茶湯會",
        "大苑子",
        "麻古",
        "珍煮丹",
        "波霸",
        "珍珠",
        "微冰",
        "微糖",
        "一分糖",
        "半糖",
        "全糖",
        "少冰",
        "去冰",
        "正常冰",
        "LinePay",
        "Line Pay",
        "LINE Pay",
        "line pay",
        "UberEats",
        "foodpanda",
        "載具",
        "發票",
        "電子發票",
        "服務態度",
        "店員態度",
    }
)

_BUILTIN_JIEBA_TERMS: dict[str, Tuple[Tuple[str, int, str], ...]] = {
    "drinks": _DRINKS_JIEBA_TERMS,
}

_BUILTIN_OCR_USER_WORDS: dict[str, FrozenSet[str]] = {
    "drinks": _DRINKS_OCR_USER_WORDS,
}


def get_builtin_domain_stopwords(dataset_id: str | None) -> List[str]:
    if not dataset_id:
        return []
    key = str(dataset_id).strip().lower()
    words = _BUILTIN_DOMAIN_STOPWORDS.get(key)
    if not words:
        return []
    return sorted(words)


def get_builtin_tfidf_exploration_stopwords(dataset_id: str | None) -> List[str]:
    """TF-IDF Phase A 額外停用詞（虛詞 + 場景 anchor）；與 merged domain stop 合併使用。"""
    if not dataset_id:
        return []
    key = str(dataset_id).strip().lower()
    words = _BUILTIN_TFIDF_EXPLORATION_STOPWORDS.get(key)
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


def get_builtin_jieba_terms(dataset_id: str | None) -> List[Tuple[str, int, str]]:
    if not dataset_id:
        return []
    key = str(dataset_id).strip().lower()
    terms = _BUILTIN_JIEBA_TERMS.get(key)
    if not terms:
        return []
    return list(terms)


def get_builtin_ocr_user_words(dataset_id: str | None) -> List[str]:
    if not dataset_id:
        return []
    key = str(dataset_id).strip().lower()
    words = _BUILTIN_OCR_USER_WORDS.get(key)
    if not words:
        return []
    return sorted(words)


def get_all_builtin_ocr_user_words() -> List[str]:
    merged: set[str] = set()
    for words in _BUILTIN_OCR_USER_WORDS.values():
        merged.update(words)
    return sorted(merged)


def resolve_local_jieba_userdict_path(dataset_id: str | None) -> str | None:
    if not dataset_id:
        return None
    key = str(dataset_id).strip().lower()
    path = _REPO_ROOT / "dic" / "jieba_dicts" / f"{key}.txt"
    return str(path) if path.is_file() else None


def resolve_local_ocr_user_words_path(dataset_id: str | None) -> str | None:
    if not dataset_id:
        return None
    key = str(dataset_id).strip().lower()
    path = _REPO_ROOT / "dic" / "ocr_user_words" / f"{key}.txt"
    return str(path) if path.is_file() else None


def materialize_merged_ocr_user_words_file(
    *,
    extra_paths: Iterable[str] | None = None,
    dataset_ids: Iterable[str] | None = None,
) -> str | None:
    """
    合併內建與檔案 OCR 詞彙，寫入暫存檔供 Tesseract --user-words 使用。
    回傳檔案路徑；若無任何詞彙則回傳 None。
    """
    import tempfile

    words: set[str] = set(get_all_builtin_ocr_user_words())
    for ds in dataset_ids or ():
        words.update(get_builtin_ocr_user_words(str(ds)))
    for raw_path in extra_paths or ():
        path = str(raw_path or "").strip()
        if not path or not Path(path).is_file():
            continue
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                w = line.strip()
                if not w or w.startswith("#"):
                    continue
                words.add(w)

    if not words:
        return None

    fd, out_path = tempfile.mkstemp(suffix="_ocr_user_words.txt", prefix="ocr_words_")
    with open(fd, "w", encoding="utf-8") as fh:
        for w in sorted(words):
            fh.write(f"{w}\n")
    return out_path
