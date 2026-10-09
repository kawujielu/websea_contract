# -*- coding: utf-8 -*-
"""Gemini API 封装（Python 3.9+）。"""
from __future__ import annotations

import os
from typing import Optional

try:
    import google.generativeai as genai
except ImportError:
    genai = None  # type: ignore

DEFAULT_GEMINI_MODEL = "models/gemini-2.5-flash"
DEFAULT_GOOGLE_API_KEY = "AIzaSyBvJJUIqoQ93CZCOUSwMRcOLBt6FmWkWF8"


def get_api_key() -> str:
    return (os.environ.get("GOOGLE_API_KEY") or DEFAULT_GOOGLE_API_KEY).strip()


def generate_content(prompt: str, model_name: Optional[str] = None) -> str:
    """调用 Gemini generate_content，返回文本；失败抛异常。"""
    if genai is None:
        raise RuntimeError("未安装 google-generativeai，请执行: pip install google-generativeai")
    api_key = get_api_key()
    genai.configure(api_key=api_key)
    model_id = (model_name or os.environ.get("GEMINI_MODEL") or DEFAULT_GEMINI_MODEL).strip()
    model = genai.GenerativeModel(model_id)
    response = model.generate_content(prompt)
    text_out = getattr(response, "text", None)
    if text_out:
        return str(text_out).strip()
    return str(response).strip()
