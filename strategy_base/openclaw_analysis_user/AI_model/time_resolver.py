# -*- coding: utf-8 -*-
"""自然语言时间 → 北京时间日期区间。"""
from __future__ import annotations

import calendar
import re
from datetime import date, datetime, timedelta
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

from AI_model.gemini_client import generate_content
from AI_model.prompt_templates import build_time_resolve_prompt

BJT = ZoneInfo("Asia/Shanghai")

DateRange = Tuple[date, date]


def _today_bjt() -> date:
    return datetime.now(BJT).date()


def _yesterday_bjt() -> date:
    return _today_bjt() - timedelta(days=1)


def _parse_range_line(line: str) -> Optional[DateRange]:
    s = (line or "").strip()
    m = re.search(
        r"(\d{4}-\d{2}-\d{2})\s*到\s*(\d{4}-\d{2}-\d{2})",
        s,
    )
    if not m:
        return None
    d1 = datetime.strptime(m.group(1), "%Y-%m-%d").date()
    d2 = datetime.strptime(m.group(2), "%Y-%m-%d").date()
    if d2 < d1:
        d1, d2 = d2, d1
    return d1, d2


def resolve_time_via_gemini(query_text: str) -> Optional[DateRange]:
    try:
        today_s = _today_bjt().strftime("%Y-%m-%d")
        prompt = build_time_resolve_prompt(query_text, today_s)
        raw = generate_content(prompt)
        first_line = (raw.splitlines() or [""])[0].strip()
        if first_line.upper() == "NONE":
            return None
        return _parse_range_line(first_line)
    except Exception:
        return None


def _local_parse_absolute_range(query: str) -> Optional[DateRange]:
    q = query.replace(" ", "")
    m = re.search(r"(\d{4}-\d{2}-\d{2})到(\d{4}-\d{2}-\d{2})", q)
    if m:
        d1 = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        d2 = datetime.strptime(m.group(2), "%Y-%m-%d").date()
        if d2 < d1:
            d1, d2 = d2, d1
        return d1, d2
    m = re.search(r"(\d{4}-\d{2}-\d{2})", query)
    if m:
        d1 = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        return d1, d1
    return None


def _local_parse_relative_window(query: str) -> Optional[DateRange]:
    q = query.replace(" ", "")
    end_day = _yesterday_bjt()

    if re.search(r"(昨日|昨天)", q):
        return end_day, end_day
    if re.search(r"(今日|今天)", q):
        t = _today_bjt()
        return t, t

    m = re.search(r"近(\d+)(?:个)?(日|天|周|週|月)", q)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if unit in ("日", "天"):
            days = max(1, n)
        elif unit in ("周", "週"):
            days = max(1, 7 * n)
        else:
            days = max(1, 30 * n)
        start_day = end_day - timedelta(days=days - 1)
        return start_day, end_day

    if "上周" in q:
        # 上周一至上周日
        today = _today_bjt()
        last_mon = today - timedelta(days=today.weekday() + 7)
        last_sun = last_mon + timedelta(days=6)
        return last_mon, last_sun

    if "上个月" in q or "上月" in q:
        today = _today_bjt()
        first_this = today.replace(day=1)
        last_prev = first_this - timedelta(days=1)
        first_prev = last_prev.replace(day=1)
        return first_prev, last_prev

    if "本月" in q:
        today = _today_bjt()
        first = today.replace(day=1)
        return first, _yesterday_bjt() if _yesterday_bjt() >= first else today

    # x月x日至今
    m = re.search(r"(\d{1,2})月(\d{1,2})日(?:至今|到目前为止|以来)?", q)
    if m:
        year = _today_bjt().year
        month, day = int(m.group(1)), int(m.group(2))
        try:
            start_d = date(year, month, day)
        except ValueError:
            return None
        return start_d, _yesterday_bjt()

    if re.search(r"(全部|全量)", q):
        return None  # 全量由 pkl_loader 扫目录

    return None


def resolve_time_range(query_text: str, use_gemini: bool = True) -> Optional[DateRange]:
    """
    解析时间区间。优先 Gemini；失败则本地规则。
    返回 None 表示未识别到时间（非「全量」）。
    """
    if use_gemini:
        gr = resolve_time_via_gemini(query_text)
        if gr is not None:
            return gr

    abs_r = _local_parse_absolute_range(query_text)
    if abs_r is not None:
        return abs_r

    rel = _local_parse_relative_window(query_text)
    if rel is not None:
        return rel

    return None
