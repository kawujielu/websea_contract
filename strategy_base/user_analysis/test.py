#!/usr/bin/env python3
"""查询指定用户、指定交易对、指定时间段的合约资金费用（资费）收取明细。"""

from __future__ import annotations

import asyncio
import datetime
import os
import sys
from pathlib import Path

# ---------- 固定配置 ----------
USER_ID = 351417
SYMBOL = "BNB-USDT"
BEGIN_DATE = "2026-06-10"  # 含当天
END_DATE = "2026-06-12"    # 含当天

USER_TYPE = 2  # 0全部 | 1量化 | 2普通用户
PAGE_SIZE = 1000
API_TOKEN = "c1cf4185b2bed317aeb6e6674491fbef"
# ------------------------------

UBUNTU_SCRIPT_DIR = Path("/home/ubuntu/strategy_base/strategy/user_analysis")
STRATEGY_BASE = Path("/home/ubuntu/strategy_base")
CRYPTO_CENTER_PATHS = (
    Path(r"D:\websea"),
    Path(r"C:\Users\linji\OneDrive\websea"),
)


def setup_import_path() -> Path:
    if UBUNTU_SCRIPT_DIR.exists():
        os.chdir(UBUNTU_SCRIPT_DIR)
        script_dir = UBUNTU_SCRIPT_DIR
        for p in (STRATEGY_BASE, STRATEGY_BASE.parent):
            s = str(p)
            if s not in sys.path:
                sys.path.append(s)
        sys.path.append("../..")
        sys.path.append("../../..")
    else:
        script_dir = Path(__file__).resolve().parent
        for p in CRYPTO_CENTER_PATHS:
            if p.exists():
                s = str(p)
                if s not in sys.path:
                    sys.path.insert(0, s)
                break
        else:
            raise SystemExit("未找到 crypto_center，请确认路径或放到 Ubuntu strategy_base 环境运行")
    return script_dir


SCRIPT_DIR = setup_import_path()

from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract


def date_range(begin_date: str, end_date: str) -> tuple[int, int]:
    """[begin_date 00:00:00, end_date+1 00:00:00)，即 end_date 当天也包含在内。"""
    begin = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d") + datetime.timedelta(days=1)
    return int(begin.timestamp()), int(end.timestamp())


async def fetch_user_symbol_cost(
    rest: Contract,
    *,
    symbol: str,
    begin_ts: int,
    end_ts: int,
    user_type: int,
) -> list[dict]:
    rows: list[dict] = []
    page = 1
    total_page = 1
    while page <= total_page:
        data = await rest.fetch_treaty_cost(
            symbol=symbol,
            page=page,
            page_size=PAGE_SIZE,
            user_type=user_type,
            min_time=begin_ts,
            max_time=end_ts,
            timeout=60,
        )
        total_page = int(data["pager"]["total_page"])
        rows.extend(data["data"])
        page += 1
        if page <= total_page:
            await asyncio.sleep(0.2)
    return rows


def filter_user_rows(rows: list[dict], user_id: int | str) -> list[dict]:
    uid = str(user_id)
    return [r for r in rows if str(r.get("user_id", "")) == uid]


async def main() -> None:
    begin_ts, end_ts = date_range(BEGIN_DATE, END_DATE)
    begin_text = datetime.datetime.fromtimestamp(begin_ts)
    end_text = datetime.datetime.fromtimestamp(end_ts)

    rest = Contract(API_TOKEN, "")
    rest.DEBUG = False
    rest.rest_timeout = 60

    rows_all = await fetch_user_symbol_cost(
        rest,
        symbol=SYMBOL,
        begin_ts=begin_ts,
        end_ts=end_ts,
        user_type=USER_TYPE,
    )
    rows = filter_user_rows(rows_all, USER_ID)
    rows.sort(key=lambda r: r.get("create_time_text", ""))

    total = sum(float(r.get("settle_cost") or 0) for r in rows)

    print(f"用户ID: {USER_ID}")
    print(f"交易对: {SYMBOL}")
    print(f"日期范围: {BEGIN_DATE} ~ {END_DATE}（含首尾）")
    print(f"时间戳范围: {begin_text} ~ {end_text}  ({begin_ts} ~ {end_ts})")
    print(f"API 原始记录数({SYMBOL}): {len(rows_all)}")
    print(f"匹配用户记录数: {len(rows)}")
    print("-" * 88)
    print(
        f"{'时间':<20} {'用户ID':<10} {'交易对':<12} {'结算费用':>18} "
        f"{'方向':<8} {'持仓量':>14} {'标记价':>12} {'费率':>10}"
    )
    for r in rows:
        print(
            f"{r.get('create_time_text', ''):<20} "
            f"{str(r.get('user_id', '')):<10} "
            f"{str(r.get('symbol_name', '')):<12} "
            f"{float(r.get('settle_cost') or 0):>18.8f} "
            f"{str(r.get('direction_text', '')):<8} "
            f"{str(r.get('hold_amount', '')):>14} "
            f"{str(r.get('mark_price', '')):>12} "
            f"{str(r.get('fund_rate', '')):>10}"
        )
    print("-" * 88)
    print(f"用户 {USER_ID} 在 {SYMBOL} 上述时间段资费合计: {total:.8f}")


if __name__ == "__main__":
    asyncio.run(main())

