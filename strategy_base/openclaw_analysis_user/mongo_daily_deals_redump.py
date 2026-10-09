# -*- coding: utf-8 -*-
"""全量重导 Mongo 日切 PKL 到新目录（含 order_id），不覆盖原 pkl。"""
from __future__ import annotations

import argparse
import asyncio
import glob
import os
import time
from datetime import date

import motor.motor_asyncio

from mongo_daily_deals_dump import MONGO_URI, day_range, dump_one_day, parse_day

OLD_DIR = (
    "/home/ubuntu/strategy_base/strategy/openclaw_analysis_user/data/mongo_daily_deals_pkl"
)
NEW_DIR = OLD_DIR + "_v2"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="重导 Mongo 日切 PKL 到新目录")
    p.add_argument("--start-date", help="YYYY-MM-DD，默认取原目录最早日期")
    p.add_argument("--end-date", help="YYYY-MM-DD，默认取原目录最晚日期")
    p.add_argument("--old-dir", default=OLD_DIR, help="参考日期范围的原 pkl 目录")
    p.add_argument("--output-dir", default=NEW_DIR, help="新 pkl 输出目录")
    p.add_argument("--force", action="store_true", help="覆盖新目录已有 pkl")
    return p.parse_args()


def infer_range(old_dir: str) -> tuple[date, date]:
    days = [parse_day(os.path.basename(p)[:-4]) for p in glob.glob(os.path.join(old_dir, "????-??-??.pkl"))]
    if not days:
        raise SystemExit("原目录无 pkl：{}".format(old_dir))
    return min(days), max(days)


async def run() -> None:
    args = parse_args()
    if args.start_date and args.end_date:
        start_d, end_d = parse_day(args.start_date), parse_day(args.end_date)
    elif args.start_date or args.end_date:
        raise SystemExit("start-date 与 end-date 需同时提供，或都不提供")
    else:
        start_d, end_d = infer_range(args.old_dir)

    os.makedirs(args.output_dir, exist_ok=True)
    print("导出区间 {} ~ {} -> {}".format(start_d, end_d, args.output_dir))

    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    col = client.exchange.real_contract_deal
    processed = skipped = failed = not_finished = 0
    try:
        for d in day_range(start_d, end_d):
            t0 = time.perf_counter()
            try:
                status, rows = await dump_one_day(col, d, args.output_dir, args.force)
                dt = time.perf_counter() - t0
                print("date={} status={} rows={} elapsed={:.2f}s".format(d, status, rows, dt))
                if status == "processed":
                    processed += 1
                elif status == "skipped":
                    skipped += 1
                elif status == "not_finished":
                    not_finished += 1
            except Exception as e:
                failed += 1
                print("date={} status=failed err={}".format(d, e))
    finally:
        client.close()

    print("汇总 processed={} skipped={} not_finished={} failed={}".format(
        processed, skipped, not_finished, failed
    ))


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()

