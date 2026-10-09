# -*- coding: utf-8 -*-
"""解析 Telegram / CLI 提问文本。"""
from __future__ import annotations

import re
from typing import List, Optional

MISSING_SCOPE_MSG = "请指定用户id或时间范围"

# 触发 AI 画像的关键词
_AI_TRIGGER_RE = re.compile(r"(用AI分析|AI分析)", re.IGNORECASE)

# 时间语义粗检测（用于判断是否需要解析时间）
_TIME_HINT_PATTERNS = [
    r"近\s*\d+\s*(?:个)?(?:日|天|周|週|月)",
    r"(?:昨日|昨天|今日|今天|上周|上个月|上月|本月|当周)",
    r"\d{4}-\d{2}-\d{2}",
    r"\d{1,2}\s*月\s*\d{1,2}\s*日",
    r"(?:至今|到目前为止|以来)",
    r"(?:全部|全量)",
]


def is_ai_profile_trigger(query: str) -> bool:
    return _AI_TRIGGER_RE.search(query or "") is not None


def strip_ai_trigger(query: str) -> str:
    q = query or ""
    q = re.sub(r"用AI分析", "", q, flags=re.IGNORECASE)
    q = re.sub(r"AI分析", "", q, flags=re.IGNORECASE)
    return q.strip()


def parse_user_ids(query: str) -> List[str]:
    """提取 6 位用户 ID，顺序去重。"""
    seen = set()
    out: List[str] = []
    for m in re.finditer(r"(?<!\d)(\d{6})(?!\d)", query or ""):
        u = m.group(1)
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def has_raw_answer_mode(query: str) -> bool:
    return "原始回答" in (query or "")


def has_time_hint(query: str) -> bool:
    q = (query or "").replace(" ", "")
    for p in _TIME_HINT_PATTERNS:
        if re.search(p, q):
            return True
    return False


def validate_scope(user_ids: List[str], time_resolved: bool, query: str) -> Optional[str]:
    """无 user_id 且无时间 → 返回错误提示；否则 None。"""
    if user_ids:
        return None
    if time_resolved or has_time_hint(query):
        return None
    return MISSING_SCOPE_MSG
