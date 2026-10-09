# -*- coding: utf-8 -*-
"""
Mongo 合约成交日切导出：从 MongoDB 拉取成交，按北京时间自然日写入本地 PKL。

作用
----
为后续 PKL 离线分析脚本（用户统计、重合度、AB 仓对比、盈利 Top 报表等）提供
统一数据源。每个自然日一个文件：`{output_dir}/YYYY-MM-DD.pkl`。

数据来源
--------
- 库：环境变量 UPM_MONGO_URI（默认内网 Mongo）
- 集合：exchange.real_contract_deal
- 时间：按 ts 字段，以北京时间 [当日 00:00:00, 23:59:59] 为窗口

拉取与落盘逻辑
--------------
1. 将单日按 10 分钟切片查询（SLICE_SECONDS=600），降低单次查询压力；失败切片最多重试 2 次。
2. 每条 Mongo 成交拆成两行：taker 腿 + maker 腿（与线上下单分析口径一致）。
3. 字段映射为：ts_text, symbol, buy_sell(开多/开空/平多/平空), price, amount,
   profit_loss, fee, taker_user, maker_user, multiple, face_value,
   deal_is_protected, deal_sub_id。
4. 仅处理「已结束」的自然日（日期 < 北京时间今天）；当天数据标记 not_finished 跳过。
5. 目标 pkl 已存在且未加 --force 时跳过（skipped）。

默认日期与定时任务
------------------
- 不传 --start-date / --end-date 时，默认只导出「北京时间昨天」一天（适合 crontab 每日 00:00 跑）。
- 传入起止日期时，必须两个都传，且 start <= end，按天循环导出。

导出后联动（可选）
------------------
成功写入「昨天」的 pkl 后，默认按顺序子进程执行：
1) daily_profit_top10_kline_report.py — 盈利 Top30 结果先发 Telegram，再发 K 线 zip；
2) check_ab_users.py — 前一天 AB 仓关联组分析，结果发 Telegram；
3) pkl_user_ab_warehouse_overlap_by_ids.py --all-users — 昨日全体成交用户 AB 仓排查，结果发 Telegram；
4) pkl_CFD_daily_report.py — 昨日 500-USDT/CFD 成交量与盈亏 TopN，结果发 Telegram。

可用 --no-post-profit-report / --no-post-ab-check / --no-post-cfd-report 分别关闭
（跳过盈利报表时也会跳过 check_ab_users）。
子进程通过环境变量 UPM_PKL_DEALS_DIR 指向本次 output-dir。

命令行参数
----------
  --start-date / --end-date   导出日期范围 YYYY-MM-DD（同时省略则昨天）
  --output-dir                PKL 输出目录（默认 code/data/mongo_daily_deals_pkl）
  --force                     覆盖已存在的 pkl
  --no-post-profit-report     不自动跑盈利 Top10 报表
  --no-post-ab-check          不自动跑 AB 仓排查
  --no-post-cfd-report        不自动跑 CFD/500-USDT 日报

环境变量
--------
  UPM_MONGO_URI               Mongo 连接串
  UPM_PKL_DEALS_DIR           子进程报表读取的 PKL 目录（脚本内部也会设置）
  UPM_POST_REPORT_TIMEOUT_SEC 子进程报表超时秒数，默认 7200

依赖：motor, pandas。Python 3.9+。

示例（Ubuntu）
--------------
  # 每日定时：导出昨天
  python3 mongo_daily_deals_dump.py

  # 补历史区间
  python3 mongo_daily_deals_dump.py --start-date 2026-05-01 --end-date 2026-05-31

  # 指定目录并强制覆盖
  python3 mongo_daily_deals_dump.py --output-dir /path/to/pkl --force --no-post-profit-report
"""
from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

import motor.motor_asyncio
import pandas as pd

BJT = ZoneInfo("Asia/Shanghai")
BUY_SELL_SIDE = {"1": "开多", "2": "开空", "3": "平多", "4": "平空"}

MONGO_URI = os.environ.get(
    "UPM_MONGO_URI",
    "mongodb://root:GD0wBplOGwxQTaUI@10.0.96.71:27020/",
)
DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data", "mongo_daily_deals_pkl")
SLICE_SECONDS = 600  # 10min
SLICE_MAX_RETRY = 2
# 日切后 AB 仓排查：默认扫描昨日 PKL 内全部成交用户（传 --all-users 给子进程）
AB_CHECK_ALL_USERS = True


def parse_args():
    p = argparse.ArgumentParser(description="Dump Mongo daily deals to local PKL")
    p.add_argument("--start-date", required=False, default=None, help="YYYY-MM-DD")
    p.add_argument("--end-date", required=False, default=None, help="YYYY-MM-DD")
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--force", action="store_true", help="overwrite existing pkl")
    p.add_argument(
        "--no-post-profit-report",
        action="store_true",
        help="成功写入「北京时间昨天」的 pkl 后，不自动运行 daily_profit_top10_kline_report.py",
    )
    p.add_argument(
        "--no-post-ab-check",
        action="store_true",
        help="盈利报表完成后，不自动运行 pkl_user_ab_warehouse_overlap_by_ids.py（AB仓排查）",
    )
    p.add_argument(
        "--no-post-cfd-report",
        action="store_true",
        help="不自动运行 pkl_CFD_daily_report.py（昨日 CFD/500-USDT 日报）",
    )
    args, _ = p.parse_known_args()
    return args


def parse_day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def day_range(start_d: date, end_d: date) -> List[date]:
    cur = start_d
    out = []
    while cur <= end_d:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def bjt_start_ts(d: date) -> int:
    dt = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=BJT)
    return int(dt.timestamp())


def bjt_end_ts(d: date) -> int:
    dt = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=BJT)
    return int(dt.timestamp())


def build_slices(day_start_ts: int, day_end_ts: int) -> List[Tuple[int, int]]:
    arr = []
    s = int(day_start_ts)
    while s <= day_end_ts:
        e = min(day_end_ts, s + SLICE_SECONDS - 1)
        arr.append((s, e))
        s = e + 1
    return arr


def output_path(output_dir: str, d: date) -> str:
    return os.path.join(output_dir, "{}.pkl".format(d.strftime("%Y-%m-%d")))


def run_daily_profit_top10_kline_report(output_dir: str) -> None:
    """与 dump 同目录的 PKL 已由本进程写完，子进程读 UPM_PKL_DEALS_DIR。"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    report_path = os.path.join(script_dir, "daily_profit_top10_kline_report.py")
    if not os.path.isfile(report_path):
        print("[WARN] post-report skipped: missing {}".format(report_path))
        return
    env = os.environ.copy()
    env["UPM_PKL_DEALS_DIR"] = os.path.abspath(output_dir)
    timeout_sec = int(os.environ.get("UPM_POST_REPORT_TIMEOUT_SEC", "7200"))
    print("[INFO] post-report start timeout_sec={} data_dir={}".format(timeout_sec, env["UPM_PKL_DEALS_DIR"]))
    try:
        cp = subprocess.run(
            [sys.executable, report_path],
            cwd=script_dir,
            env=env,
            timeout=timeout_sec,
            capture_output=True,
            text=True,
        )
    except subprocess.TimeoutExpired:
        print("[ERROR] post-report: subprocess timeout ({}s)".format(timeout_sec))
        return
    if cp.stdout:
        print(cp.stdout.rstrip())
    if cp.returncode != 0:
        err = (cp.stderr or "").strip()
        if err:
            print("[ERROR] post-report exit={} stderr_tail=\n{}".format(cp.returncode, err[-4000:]))
        else:
            print("[ERROR] post-report exit={}".format(cp.returncode))
    else:
        if cp.stderr:
            print(cp.stderr.rstrip())
        print("[INFO] post-report finished ok")


def run_check_ab_users(output_dir: str) -> None:
    """盈利报表后：运行 check_ab_users.py（前一天 AB 仓关联组）。"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, "check_ab_users.py")
    if not os.path.isfile(path):
        print("[WARN] post-check-ab-users skipped: missing {}".format(path))
        return
    env = os.environ.copy()
    env["UPM_PKL_DEALS_DIR"] = os.path.abspath(output_dir)
    timeout_sec = int(os.environ.get("UPM_POST_AB_TIMEOUT_SEC", os.environ.get("UPM_POST_REPORT_TIMEOUT_SEC", "7200")))
    print("[INFO] post-check-ab-users start timeout_sec={} data_dir={}".format(
        timeout_sec, env["UPM_PKL_DEALS_DIR"]
    ))
    try:
        cp = subprocess.run(
            [sys.executable, path],
            cwd=script_dir,
            env=env,
            timeout=timeout_sec,
            capture_output=True,
            text=True,
        )
    except subprocess.TimeoutExpired:
        print("[ERROR] post-check-ab-users: subprocess timeout ({}s)".format(timeout_sec))
        return
    if cp.stdout:
        print(cp.stdout.rstrip())
    if cp.returncode != 0:
        err = (cp.stderr or "").strip()
        if err:
            print("[ERROR] post-check-ab-users exit={} stderr_tail=\n{}".format(cp.returncode, err[-4000:]))
        else:
            print("[ERROR] post-check-ab-users exit={}".format(cp.returncode))
    else:
        if cp.stderr:
            print(cp.stderr.rstrip())
        print("[INFO] post-check-ab-users finished ok")


def run_ab_warehouse_overlap_check(output_dir: str, stat_day: date) -> None:
    """昨日 PKL 就绪且盈利报表已结束后：全体用户 AB 仓排查并推送 Telegram。"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    ab_path = os.path.join(script_dir, "pkl_user_ab_warehouse_overlap_by_ids.py")
    if not os.path.isfile(ab_path):
        print("[WARN] post-ab-check skipped: missing {}".format(ab_path))
        return

    day_str = stat_day.strftime("%Y-%m-%d")
    env = os.environ.copy()
    env["UPM_PKL_DEALS_DIR"] = os.path.abspath(output_dir)
    timeout_sec = int(os.environ.get("UPM_POST_AB_TIMEOUT_SEC", os.environ.get("UPM_POST_REPORT_TIMEOUT_SEC", "7200")))
    cmd = [
        sys.executable,
        ab_path,
        "--data-dir",
        env["UPM_PKL_DEALS_DIR"],
        "--start-date",
        day_str,
        "--end-date",
        day_str,
        "--send-telegram",
        "--print-details",
    ]
    if AB_CHECK_ALL_USERS:
        cmd.append("--all-users")
    print("[INFO] post-ab-check start day={} timeout_sec={} data_dir={}".format(
        day_str, timeout_sec, env["UPM_PKL_DEALS_DIR"]
    ))
    try:
        cp = subprocess.run(
            cmd,
            cwd=script_dir,
            env=env,
            timeout=timeout_sec,
            capture_output=True,
            text=True,
        )
    except subprocess.TimeoutExpired:
        print("[ERROR] post-ab-check: subprocess timeout ({}s)".format(timeout_sec))
        return
    if cp.stdout:
        print(cp.stdout.rstrip())
    if cp.returncode != 0:
        err = (cp.stderr or "").strip()
        if err:
            print("[ERROR] post-ab-check exit={} stderr_tail=\n{}".format(cp.returncode, err[-4000:]))
        else:
            print("[ERROR] post-ab-check exit={}".format(cp.returncode))
    else:
        if cp.stderr:
            print(cp.stderr.rstrip())
        print("[INFO] post-ab-check finished ok")


def run_cfd_daily_report(output_dir: str, stat_day: date) -> None:
    """全部后处理完成后：昨日 CFD/500-USDT 成交量与盈亏 TopN。"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, "pkl_CFD_daily_report.py")
    if not os.path.isfile(path):
        print("[WARN] post-cfd-report skipped: missing {}".format(path))
        return
    day_str = stat_day.strftime("%Y-%m-%d")
    data_dir = os.path.abspath(output_dir)
    env = os.environ.copy()
    env["UPM_PKL_DEALS_DIR"] = data_dir
    timeout_sec = int(os.environ.get("UPM_POST_CFD_TIMEOUT_SEC", os.environ.get("UPM_POST_REPORT_TIMEOUT_SEC", "7200")))
    cmd = [
        sys.executable,
        path,
        "--data-dir",
        data_dir,
        "--day",
        day_str,
    ]
    print("[INFO] post-cfd-report start day={} timeout_sec={} data_dir={}".format(
        day_str, timeout_sec, data_dir
    ))
    try:
        cp = subprocess.run(
            cmd,
            cwd=script_dir,
            env=env,
            timeout=timeout_sec,
            capture_output=True,
            text=True,
        )
    except subprocess.TimeoutExpired:
        print("[ERROR] post-cfd-report: subprocess timeout ({}s)".format(timeout_sec))
        return
    if cp.stdout:
        print(cp.stdout.rstrip())
    if cp.returncode != 0:
        err = (cp.stderr or "").strip()
        if err:
            print("[ERROR] post-cfd-report exit={} stderr_tail=\n{}".format(cp.returncode, err[-4000:]))
        else:
            print("[ERROR] post-cfd-report exit={}".format(cp.returncode))
    else:
        if cp.stderr:
            print(cp.stderr.rstrip())
        print("[INFO] post-cfd-report finished ok")


def day_finished(d: date) -> bool:
    today_bjt = datetime.now(BJT).date()
    return d < today_bjt


async def fetch_slice_rows(collection, ts_lo: int, ts_hi: int) -> List[List]:
    q = {"ts": {"$gte": ts_lo, "$lte": ts_hi}}
    projection = {
        "_id": 0,
        "ts": 1,
        "symbol": 1,
        "price": 1,
        "amount": 1,
        "takerUser": 1,
        "makerUser": 1,
        "takerBuyOrSell": 1,
        "makerBuyOrSell": 1,
        "takerProfitLoss": 1,
        "makerProfitLoss": 1,
        "takerFee": 1,
        "makerFee": 1,
        "takerMultiple": 1,
        "makerMultiple": 1,
        "takerFaceValue": 1,
        "makerFaceValue": 1,
        "takerIsProtected": 1,
        "makerIsProtected": 1,
        "takerSubId": 1,
        "makerSubId": 1,
        "takerOrder": 1,
        "makerOrder": 1,
    }

    rows = []
    cursor = collection.find(q, projection=projection).batch_size(2000)
    async for deal in cursor:
        ts_i = int(deal["ts"])
        dt_str = datetime.fromtimestamp(ts_i, tz=BJT).strftime("%Y-%m-%d %H:%M:%S")
        symbol = deal["symbol"]
        price = float(deal["price"])
        amount = float(deal["amount"])
        taker = str(deal["takerUser"])
        maker = str(deal["makerUser"])

        side_t = BUY_SELL_SIDE.get(str(deal["takerBuyOrSell"]), str(deal["takerBuyOrSell"]))
        row_t = [
            dt_str, symbol, side_t, price, amount,
            float(deal.get("takerProfitLoss") or 0.0),
            float(deal.get("takerFee") or 0.0),
            taker, maker,
            int(deal.get("takerMultiple") or 0),
            float(deal.get("takerFaceValue") or 0.0),
            int(deal.get("takerIsProtected") or 0),
            int(deal.get("takerSubId") or 0),
            str(deal.get("takerOrder") or ""),
        ]
        rows.append(row_t)

        side_m = BUY_SELL_SIDE.get(str(deal["makerBuyOrSell"]), str(deal["makerBuyOrSell"]))
        row_m = [
            dt_str, symbol, side_m, price, amount,
            float(deal.get("makerProfitLoss") or 0.0),
            float(deal.get("makerFee") or 0.0),
            taker, maker,
            int(deal.get("makerMultiple") or 0),
            float(deal.get("makerFaceValue") or 0.0),
            int(deal.get("makerIsProtected") or 0),
            int(deal.get("makerSubId") or 0),
            str(deal.get("makerOrder") or ""),
        ]
        rows.append(row_m)
    return rows


async def dump_one_day(collection, d: date, output_dir: str, force: bool) -> Tuple[str, int]:
    fp = output_path(output_dir, d)
    if (not force) and os.path.exists(fp):
        return "skipped", 0
    if not day_finished(d):
        return "not_finished", 0

    day_start = bjt_start_ts(d)
    day_end = bjt_end_ts(d)
    slices = build_slices(day_start, day_end)
    all_rows = []

    for ts_lo, ts_hi in slices:
        ok = False
        last_err = None
        for _ in range(SLICE_MAX_RETRY + 1):
            try:
                rows = await fetch_slice_rows(collection, ts_lo, ts_hi)
                all_rows.extend(rows)
                ok = True
                break
            except Exception as e:
                last_err = e
                await asyncio.sleep(0.3)
        if not ok:
            raise RuntimeError(
                "slice failed date={} ts=[{}, {}], err={}".format(
                    d.strftime("%Y-%m-%d"), ts_lo, ts_hi, last_err
                )
            )

    df = pd.DataFrame(
        all_rows,
        columns=[
            "ts_text",
            "symbol",
            "buy_sell",
            "price",
            "amount",
            "profit_loss",
            "fee",
            "taker_user",
            "maker_user",
            "multiple",
            "face_value",
            "deal_is_protected",
            "deal_sub_id",
            "order_id",
        ],
    )
    df.to_pickle(fp)
    return "processed", int(df.shape[0])


async def run():
    args = parse_args()
    if (args.start_date is None) and (args.end_date is None):
        # 默认给“昨天”，以便配合每天 00:00 定时任务
        yesterday = datetime.now(BJT).date() - timedelta(days=1)
        start_d = yesterday
        end_d = yesterday
    else:
        if (args.start_date is None) != (args.end_date is None):
            raise ValueError("start-date 和 end-date 必须同时提供，或都不提供（则默认昨天）")
        start_d = parse_day(args.start_date)
        end_d = parse_day(args.end_date)
        if start_d > end_d:
            raise ValueError("start-date must <= end-date")
    os.makedirs(args.output_dir, exist_ok=True)

    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    col = client.exchange.real_contract_deal

    processed = 0
    skipped = 0
    failed = 0
    not_finished = 0
    details = []
    yesterday_bjt = datetime.now(BJT).date() - timedelta(days=1)
    want_post_report = False

    try:
        for d in day_range(start_d, end_d):
            t0 = time.perf_counter()
            try:
                status, rows_cnt = await dump_one_day(col, d, args.output_dir, args.force)
                dt = time.perf_counter() - t0
                if status == "processed":
                    processed += 1
                    if d == yesterday_bjt:
                        want_post_report = True
                    print("date={} status=processed rows={} elapsed={:.2f}s".format(d, rows_cnt, dt))
                elif status == "skipped":
                    skipped += 1
                    print("date={} status=skipped elapsed={:.2f}s".format(d, dt))
                else:
                    not_finished += 1
                    print("date={} status=not_finished elapsed={:.2f}s".format(d, dt))
                details.append((d.strftime("%Y-%m-%d"), status))
            except Exception as e:
                failed += 1
                dt = time.perf_counter() - t0
                print("date={} status=failed elapsed={:.2f}s err={}".format(d, dt, e))
                details.append((d.strftime("%Y-%m-%d"), "failed"))

        if want_post_report and (not args.no_post_profit_report):
            run_daily_profit_top10_kline_report(args.output_dir)
            run_check_ab_users(args.output_dir)
        elif want_post_report and args.no_post_profit_report:
            print("[INFO] post-report skipped (--no-post-profit-report)")
            print("[INFO] post-check-ab-users skipped (随盈利报表一并跳过)")

        if want_post_report and (not args.no_post_ab_check):
            if args.no_post_profit_report:
                print("[INFO] post-ab-check start（已跳过盈利报表，仍执行 AB 仓排查）")
            run_ab_warehouse_overlap_check(args.output_dir, yesterday_bjt)
        elif want_post_report and args.no_post_ab_check:
            print("[INFO] post-ab-check skipped (--no-post-ab-check)")

        if want_post_report and (not args.no_post_cfd_report):
            run_cfd_daily_report(args.output_dir, yesterday_bjt)
        elif want_post_report and args.no_post_cfd_report:
            print("[INFO] post-cfd-report skipped (--no-post-cfd-report)")
    finally:
        client.close()

    print(
        "summary processed={} skipped={} not_finished={} failed={}".format(
            processed, skipped, not_finished, failed
        )
    )
    if details:
        print("details={}".format(details))


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()


