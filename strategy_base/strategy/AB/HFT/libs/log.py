# -*- coding: utf-8 -*-
"""轻量日志：不依赖项目外路径，供 signals / 回测脚本使用。"""
from __future__ import annotations

import logging
import sys

_logger = logging.getLogger("ab_signals")
if not _logger.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _logger.addHandler(_h)
    _logger.setLevel(logging.INFO)

logger = _logger
