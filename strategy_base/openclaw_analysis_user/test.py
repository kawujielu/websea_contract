#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
现货交易对列表：查询全部币对，按深度空盘识别已下架，并列出未下架币对。

判定（与 set_fund_rate_new / spot_kline_monitor 一致）：
  depth 的 bids、asks 均为空 -> 视为已下架
  否则 -> 未下架（可交易）

用法：
  python3 spot_list_symbols.py
  python3 spot_list_symbols.py --host https://oapi.websea.com
  # 服务器内网:
  # python3 spot_list_symbols.py --host https://exqv.websea.work

依赖：
  pip3 install requests
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 桌面/外网默认 oapi；服务器内网可改 SPOT_HOST=https://exqv.websea.work
DEFAULT_HOST = os.environ.get("SPOT_HOST", "https://oapi.websea.com")


def parse_args():
    p = argparse.ArgumentParser(description="现货：列出未下架交易对（深度空盘=下架）")
    p.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help="现货 API host，默认 https://oapi.websea.com（可用环境变量 SPOT_HOST）",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=0.05,
        help="查询每个交易对深度的间隔秒数，默认 0.05",
    )
    return p.parse_args()


def fetch_all_symbols(host: str) -> List[str]:
    url = host.rstrip("/") + "/openApi/market/symbols"
    r = requests.get(url, timeout=30, verify=False)
    r.raise_for_status()
    data = r.json()
    if data.get("errno") != 0:
        raise RuntimeError("symbols 接口失败: {}".format(data))
    symbols = []
    for item in data.get("result") or []:
        if isinstance(item, dict) and item.get("symbol"):
            symbols.append(str(item["symbol"]))
    return symbols


def fetch_depth(host: str, symbol: str, retries: int = 3) -> Dict[str, Any]:
    url = host.rstrip("/") + "/openApi/market/depth"
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params={"symbol": symbol}, timeout=20, verify=False)
            r.raise_for_status()
            data = r.json()
            if data.get("errno") != 0:
                return {}
            result = data.get("result") or {}
            return result if isinstance(result, dict) else {}
        except Exception as e:
            last_err = e
            time.sleep(0.3 * (attempt + 1))
    raise RuntimeError("{}".format(last_err))


def is_delisted(depth: Dict[str, Any]) -> bool:
    """深度买卖盘均为空则视为已下架。"""
    bids = depth.get("bids") or []
    asks = depth.get("asks") or []
    return bids == [] and asks == []


def classify(
    host: str, symbols: List[str], sleep_s: float
) -> Tuple[List[str], List[str], List[str]]:
    active: List[str] = []
    delisted: List[str] = []
    unknown: List[str] = []
    total = len(symbols)
    for i, s in enumerate(symbols, start=1):
        try:
            depth = fetch_depth(host, s)
            if is_delisted(depth):
                delisted.append(s)
            else:
                active.append(s)
        except Exception as e:
            unknown.append(s)
            sys.stderr.write("depth 失败 {}: {}\n".format(s, e))
        if i % 50 == 0 or i == total:
            sys.stderr.write("进度 {}/{}\n".format(i, total))
        if sleep_s > 0:
            time.sleep(sleep_s)
    return active, delisted, unknown


def main() -> int:
    args = parse_args()
    host = args.host.strip()
    try:
        all_symbols = fetch_all_symbols(host)
    except Exception as e:
        sys.stderr.write("获取交易对失败: {}\n".format(e))
        return 1

    all_symbols = sorted(set(all_symbols))
    active, delisted, unknown = classify(host, all_symbols, args.sleep)

    lines = [
        "现货交易对状态",
        "host: {}".format(host),
        "全部: {} | 未下架: {} | 已下架: {} | 深度失败: {}".format(
            len(all_symbols), len(active), len(delisted), len(unknown)
        ),
        "",
        "【已下架】({})".format(len(delisted)),
    ]
    lines.extend(delisted if delisted else ["（无）"])
    if unknown:
        lines.append("")
        lines.append("【深度查询失败】({})".format(len(unknown)))
        lines.extend(unknown)
    lines.append("")
    lines.append("【未下架】({})".format(len(active)))
    lines.extend(active if active else ["（无）"])
    lines.append("")

    sys.stdout.write("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

