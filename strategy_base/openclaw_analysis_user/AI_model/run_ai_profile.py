# -*- coding: utf-8 -*-
"""
Telegram / CLI 入口：Gemini 交易用户 AI 画像。

示例::

  python run_ai_profile.py --query-text "用AI分析123456用户交易特征" --print-stdout
  python run_ai_profile.py --query-text "用AI分析近7天用户交易特征" --no-ai --print-stdout
"""
from __future__ import annotations

import argparse
import os
import sys

_DIR = os.path.dirname(os.path.abspath(__file__))
_CODE_DIR = os.path.dirname(_DIR)
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)

from AI_model.pkl_loader import DEFAULT_DATA_DIR  # noqa: E402
from AI_model.profile_analyzer import analyze_query  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="Gemini 交易用户 AI 画像")
    ap.add_argument("--query-text", type=str, required=True, help="Telegram 原始提问")
    ap.add_argument(
        "--data-dir",
        type=str,
        default=os.environ.get("UPM_PKL_DEALS_DIR", "").strip() or DEFAULT_DATA_DIR,
    )
    ap.add_argument("--no-ai", action="store_true", help="仅组装 prompt，不调用 Gemini")
    ap.add_argument("--gemini-model", type=str, default=os.environ.get("GEMINI_MODEL", "").strip() or None)
    ap.add_argument("--print-stdout", action="store_true")
    args = ap.parse_args()

    out = analyze_query(
        args.query_text,
        data_dir=args.data_dir,
        use_gemini=not args.no_ai,
        gemini_model=args.gemini_model,
    )
    if args.print_stdout:
        print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
