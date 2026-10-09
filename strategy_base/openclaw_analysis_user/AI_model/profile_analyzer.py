# -*- coding: utf-8 -*-
"""Gemini 用户画像主编排。"""
from __future__ import annotations

import os
from typing import Optional

from AI_model.gemini_client import generate_content
from AI_model.pkl_loader import DEFAULT_DATA_DIR, load_and_build_data_block
from AI_model.prompt_templates import build_profile_prompt, build_raw_answer_prompt
from AI_model.query_parser import (
    MISSING_SCOPE_MSG,
    has_raw_answer_mode,
    has_time_hint,
    is_ai_profile_trigger,
    parse_user_ids,
    strip_ai_trigger,
    validate_scope,
)
from AI_model.time_resolver import resolve_time_range


def analyze_query(
    query_text: str,
    data_dir: Optional[str] = None,
    use_gemini: bool = True,
    gemini_model: Optional[str] = None,
) -> str:
    """
    解析提问、加载 PKL、调用 Gemini，返回分析文本。
    use_gemini=False 时仅返回组装好的 data_block（调试用）。
    """
    original = (query_text or "").strip()
    if not original:
        return MISSING_SCOPE_MSG

    work_query = strip_ai_trigger(original) if is_ai_profile_trigger(original) else original
    user_ids = parse_user_ids(work_query)
    raw_mode = has_raw_answer_mode(work_query)

    if raw_mode and not user_ids:
        return "原始回答模式请指定用户id"

    date_range = None
    need_time = has_time_hint(work_query) or not user_ids
    if need_time:
        # 主分析不调 Gemini 时，时间解析也仅用本地规则，避免隐式 API 调用
        date_range = resolve_time_range(work_query, use_gemini=use_gemini)

    scope_err = validate_scope(user_ids, date_range is not None, work_query)
    if scope_err:
        return scope_err

    pkl_dir = (data_dir or DEFAULT_DATA_DIR).strip()
    if not os.path.isdir(pkl_dir):
        return "[ERROR] PKL 目录不存在: {}".format(pkl_dir)

    if raw_mode:
        data_block = load_and_build_data_block(
            pkl_dir,
            original,
            user_ids,
            date_range,
            raw_mode=True,
        )
        prompt = build_raw_answer_prompt(original, data_block)
    else:
        data_block = load_and_build_data_block(
            pkl_dir,
            original,
            user_ids,
            date_range,
            raw_mode=False,
        )
        prompt = build_profile_prompt(original, data_block)

    if not use_gemini:
        return prompt

    try:
        return generate_content(prompt, model_name=gemini_model)
    except Exception as e:
        return "[ERROR] Gemini 调用失败: {}".format(
            e,
        )
