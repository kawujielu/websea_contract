#!/usr/bin/env python3
"""查询指定日期的全市场合约资金费用收取情况，并保存为 CSV（参考 count_daily_fund_cost）。"""

from __future__ import annotations

import asyncio
import csv
import datetime
import os
import sys
import traceback
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

# ---------- 固定配置（日期格式同 count_daily_fund_cost：年/月/日分别传入） ----------
QUERY_YEAR = 2026
QUERY_MONTH = 6
QUERY_DAY = 10
USER_TYPE = 2  # 0全部 | 1量化 | 2普通用户
PAGE_SIZE = 1000
API_TOKEN = "c1cf4185b2bed317aeb6e6674491fbef"
SIM_TAG_COUNT = 40  # E1~E20 模拟金用户标签（同 count_daily_fund_cost）
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


def calc_date_ts(
    year: int,
    month: int,
    day: int,
) -> tuple[int, int, datetime.datetime, datetime.datetime, str]:
    """
    计算统计区间时间戳（同 count_daily_fund_cost.main）：
    开始时间为当日 0 点（包含 0 点结算数据），结束时间为次日 0 点（不包含次日 0 点数据）。
    """
    begin_time = datetime.datetime(year, month, day, 0, 0, 0)
    begin_ts = int(begin_time.timestamp())
    next_day = begin_time + datetime.timedelta(days=2)
    end_time = datetime.datetime(next_day.year, next_day.month, next_day.day, 0, 0, 0)
    end_ts = int(end_time.timestamp())
    date = datetime.datetime.fromtimestamp(begin_ts).strftime("%Y-%m-%d")
    return begin_ts, end_ts, begin_time, end_time, date


def output_csv_paths(date: str) -> tuple[Path, Path, Path, Path]:
    date_tag = date.replace("-", "")
    return (
        SCRIPT_DIR / f"fund_fee_detail_{date_tag}.csv",
        SCRIPT_DIR / f"fund_fee_by_symbol_{date_tag}.csv",
        SCRIPT_DIR / f"fund_fee_daily_{date_tag}.csv",
        SCRIPT_DIR / f"fund_fee_by_user_{date_tag}.csv",
    )


async def fetch_sim_user_ids(rest: Contract) -> set[str]:
    """拉取 E1~E20 标签下所有模拟金用户 ID。"""
    sim_ids: set[str] = set()
    for num in range(1, SIM_TAG_COUNT + 1):
        tag = f"E{num}"
        page = 1
        total_page = 1
        while page <= total_page:
            data = await rest.fetch_tag_list(tag=tag, page=page, page_size=1000)
            total_page = int(data["pager"]["total_page"])
            for item in data["data"]:
                sim_ids.add(str(item["user_id"]))
            page += 1
            if page <= total_page:
                await asyncio.sleep(0.5)
    return sim_ids


async def fetch_treaty_cost_all(
    rest: Contract,
    *,
    begin_ts: int,
    end_ts: int,
    user_type: int,
) -> list[dict]:
    """分页拉取全市场资费记录（同 count_daily_fund_cost.get_cost）。"""
    cost_data: list[dict] = []
    last_page = 0
    total_page = 1

    while True:
        try:
            if last_page == 0:
                data = await rest.fetch_treaty_cost(
                    page_size=PAGE_SIZE,
                    user_type=user_type,
                    min_time=begin_ts,
                    max_time=end_ts,
                    timeout=60,
                )
                if not data.get("data"):
                    break
                total_page = int(data["pager"]["total_page"])
                for item in data["data"]:
                    if item not in cost_data:
                        cost_data.append(item)

            begin = 2 if last_page == 0 else last_page
            for page in range(begin, total_page + 1):
                last_page = page
                data = await rest.fetch_treaty_cost(
                    page=page,
                    page_size=PAGE_SIZE,
                    user_type=user_type,
                    min_time=begin_ts,
                    max_time=end_ts,
                    timeout=60,
                )
                for item in data["data"]:
                    if item not in cost_data:
                        cost_data.append(item)
                await asyncio.sleep(0.2)
            break
        except Exception:
            print(traceback.format_exc())
            await asyncio.sleep(5)

    return cost_data


def count_user_cost(rows: list[dict]) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for row in rows:
        uid = str(row.get("user_id", ""))
        totals[uid] += Decimal(str(row.get("settle_cost") or 0))
    return dict(totals)


def count_cost(
    rows: list[dict],
    sim_user_ids: set[str],
) -> tuple[Decimal, Decimal, dict[str, Decimal], dict[str, Decimal]]:
    """计算当日资费总和 & 分币对资费（同 count_daily_fund_cost.count_cost）。"""
    user_cost_sum = sim_cost_sum = Decimal("0")
    user_symbol_cost: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    sim_symbol_cost: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))

    for row in rows:
        uid = str(row.get("user_id", ""))
        symbol = str(row.get("symbol_name", ""))
        cost = Decimal(str(row.get("settle_cost") or 0))
        if uid in sim_user_ids:
            sim_cost_sum += cost
            sim_symbol_cost[symbol] += cost
        else:
            user_cost_sum += cost
            user_symbol_cost[symbol] += cost

    return user_cost_sum, sim_cost_sum, dict(user_symbol_cost), dict(sim_symbol_cost)


def save_detail_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return

    preferred = [
        "create_time_text",
        "user_id",
        "symbol_name",
        "settle_cost",
        "direction_text",
        "hold_amount",
        "mark_price",
        "fund_rate",
        "id",
    ]
    fieldnames: list[str] = []
    seen: set[str] = set()
    for key in preferred:
        if any(key in r for r in rows):
            fieldnames.append(key)
            seen.add(key)
    for row in rows:
        for key in row:
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def save_symbol_csv(
    path: Path,
    *,
    date: str,
    user_symbol_cost: dict[str, Decimal],
    sim_symbol_cost: dict[str, Decimal],
) -> None:
    symbols = sorted(set(user_symbol_cost) | set(sim_symbol_cost))
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "symbol", "user_cost", "sim_cost"])
        for symbol in symbols:
            writer.writerow(
                [
                    date,
                    symbol,
                    format(user_symbol_cost.get(symbol, Decimal("0")), "f"),
                    format(sim_symbol_cost.get(symbol, Decimal("0")), "f"),
                ]
            )


def save_daily_csv(
    path: Path,
    *,
    date: str,
    user_cost_sum: Decimal,
    sim_cost_sum: Decimal,
) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "user_cost", "sim_cost"])
        writer.writerow([date, format(user_cost_sum, "f"), format(sim_cost_sum, "f")])


def save_user_csv(path: Path, *, date: str, user_cost: dict[str, Decimal]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "user_id", "fund_cost"])
        for uid in sorted(user_cost, key=lambda x: (not x.isdigit(), int(x) if x.isdigit() else x)):
            writer.writerow([date, uid, format(user_cost[uid], "f")])


async def main() -> None:
    begin_ts, end_ts, begin_time, end_time, date = calc_date_ts(QUERY_YEAR, QUERY_MONTH, QUERY_DAY)
    detail_csv, symbol_csv, daily_csv, user_csv = output_csv_paths(date)

    rest = Contract(API_TOKEN, "")
    rest.DEBUG = False
    rest.rest_timeout = 60

    print(f"查询日期: {date}")
    print(f"开始时间:{begin_time}——结束时间:{end_time} {begin_ts}——{end_ts}")
    print(f"user_type: {USER_TYPE} (2=普通用户)")

    print("拉取模拟金用户标签 E1~E{} ...".format(SIM_TAG_COUNT))
    sim_user_ids = await fetch_sim_user_ids(rest)
    print(sim_user_ids)
    print(f"模拟金用户数: {len(sim_user_ids)}")

    print("拉取资费记录 ...")
    rows = await fetch_treaty_cost_all(
        rest,
        begin_ts=begin_ts,
        end_ts=end_ts,
        user_type=USER_TYPE,
    )
    print(f"原始记录数: {len(rows)}")

    user_cost_by_user = count_user_cost(rows)
    user_cost_sum, sim_cost_sum, user_symbol_cost, sim_symbol_cost = count_cost(rows, sim_user_ids)

    save_detail_csv(rows, detail_csv)
    save_symbol_csv(symbol_csv, date=date, user_symbol_cost=user_symbol_cost, sim_symbol_cost=sim_symbol_cost)
    save_daily_csv(daily_csv, date=date, user_cost_sum=user_cost_sum, sim_cost_sum=sim_cost_sum)
    save_user_csv(user_csv, date=date, user_cost=user_cost_by_user)

    print("-" * 60)
    print(f"{date} 普通用户资费总金额: {user_cost_sum}")
    print(f"{date} 模拟金资费总金额: {sim_cost_sum}")
    print(f"分币对数量: {len(set(user_symbol_cost) | set(sim_symbol_cost))}")
    print(f"用户数: {len(user_cost_by_user)}")
    print("-" * 60)
    print(f"明细 CSV: {detail_csv}")
    print(f"分币对汇总 CSV: {symbol_csv}")
    print(f"日汇总 CSV: {daily_csv}")
    print(f"分用户汇总 CSV: {user_csv}")


if __name__ == "__main__":
    asyncio.run(main())

