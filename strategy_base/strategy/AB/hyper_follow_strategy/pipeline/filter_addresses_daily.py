#!/usr/bin/env python3
"""
功能：筛选活跃地址，按账户权益过滤后写入 parquet_file/filter/{日期}_filter_address.parquet。

用法：
    cd /home/ubuntu/strategy_base/strategy/AB/HYPER
    python pipeline/filter_addresses_daily.py

    # crontab 每天 0 点:
    # 0 0 * * * cd /home/ubuntu/strategy_base/strategy/AB/HYPER && python3 pipeline/filter_addresses_daily.py >> parquet_file/filter.log 2>&1
"""

import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hyper_config import DATA_DIR
from query_user_fills import fetch_user_fills

INPUT_FILE = DATA_DIR / "active_address.parquet"
FILTER_DIR = DATA_DIR / "filter"
ADDRESS_PNL_FILE = DATA_DIR / "address_pnl.parquet"
ADDRESS_PROFIT_FILE = DATA_DIR / "address_profit.parquet"
API_URL = "https://api.hyperliquid.xyz/info"
TIMEOUT = 30
CACHE_TTL_DAYS = 3
LAST_ACTIVE_MAX_DAYS_AGO = 3
TRACKED_COINS = {"BTC", "ETH", "SOL", "HYPE"}
TRADES_MIN = 6
TRADES_MAX = 72
MIN_EQUITY = 500
MAX_EQUITY = 1_000_000
STALE_DAYS = 3
WEIGHT_LIMIT = 1200
RATE_RATIO = 0.2
REQ_INTERVAL = 60.0 / int(WEIGHT_LIMIT * RATE_RATIO / 2)
FILLS_INTERVAL = 60.0 / int(WEIGHT_LIMIT * RATE_RATIO / 20)
CACHE_TTL_MS = CACHE_TTL_DAYS * 86400 * 1000


def load_address_cache(path, col):
    if not path.exists():
        return {}
    df = pd.read_parquet(path)
    return dict(zip(df["address"], zip(df[col], df["time"].astype(int))))


def save_address_cache(path, cache, col):
    df = pd.DataFrame([{"address": a, col: v, "time": t} for a, (v, t) in cache.items()])
    df.to_parquet(path, index=False)


def get_cached_value(cache, address, now_ms):
    entry = cache.get(address)
    if not entry or now_ms - entry[1] > CACHE_TTL_MS:
        return None
    return entry[0]


def load_active_addresses():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"文件不存在: {INPUT_FILE}")
    df = pd.read_parquet(INPUT_FILE)
    cutoff = int((datetime.now() - timedelta(days=LAST_ACTIVE_MAX_DAYS_AGO)).timestamp() * 1000)
    df = df[df["time"] >= cutoff]
    return df["address"].drop_duplicates().tolist()


def count_recent_trades_by_coin(fills, cutoff_ms):
    counts = {}
    for f in fills:
        coin = f.get("coin")
        if coin not in TRACKED_COINS:
            continue
        t = f.get("time")
        if t is None or t < cutoff_ms:
            continue
        counts[coin] = counts.get(coin, 0) + 1
    return counts


DEALS_FLUSH_N = 100  # 每处理 N 个地址写一次 deals parquet


def append_tracked_deals(buffers, address, fills):
    for f in fills:
        coin = f.get("coin")
        if coin not in TRACKED_COINS:
            continue
        buffers[coin].append({
            "address": address, "time": f.get("time"), "px": f.get("px"), "sz": f.get("sz"),
            "side": f.get("side"), "dir": f.get("dir"), "closedPnl": f.get("closedPnl"), "fee": f.get("fee"),
        })


def _clear_deals_files():
    for coin in TRACKED_COINS:
        for p in DATA_DIR.glob(f"{coin}_deals*.parquet"):
            p.unlink()


def flush_deals(buffers, batch_id):
    """把当前 buffer 写成 part 文件后清空。"""
    for coin in sorted(TRACKED_COINS):
        rows = buffers.get(coin) or []
        if not rows:
            continue
        path = DATA_DIR / f"{coin}_deals_part_{batch_id:04d}.parquet"
        pd.DataFrame(rows).to_parquet(path, index=False)
        print(f"  已写入 {len(rows)} 条 -> {path.name}")
        buffers[coin].clear()


def merge_deals_parts():
    """合并各 coin 的 part 文件为最终 *_deals.parquet。"""
    for coin in sorted(TRACKED_COINS):
        parts = sorted(DATA_DIR.glob(f"{coin}_deals_part_*.parquet"))
        out = DATA_DIR / f"{coin}_deals.parquet"
        if not parts:
            pd.DataFrame(columns=[
                "address", "time", "px", "sz", "side", "dir", "closedPnl", "fee",
            ]).to_parquet(out, index=False)
            continue
        df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
        df.to_parquet(out, index=False)
        for p in parts:
            p.unlink()
        print(f"  已合并 {len(df)} 条 -> {out.name}")


def filter_by_fills(addresses):
    cache = load_address_cache(ADDRESS_PROFIT_FILE, "profit")
    deals_buf = {coin: [] for coin in TRACKED_COINS}
    _clear_deals_files()
    cutoff_ms = int((datetime.now() - timedelta(days=LAST_ACTIVE_MAX_DAYS_AGO)).timestamp() * 1000)
    result, fail_count, total, batch_id = [], 0, len(addresses), 0
    print(f"  [fills] 待查询 {total} 个地址")
    t0 = time.time()
    for i, addr in enumerate(addresses, 1):
        now_ms = int(time.time() * 1000)
        try:
            fills = fetch_user_fills(addr)  # userFillsByTime 分页，尽量拉全历史
            append_tracked_deals(deals_buf, addr, fills)
            profit = sum(float(f.get("closedPnl") or 0) for f in fills)
            cache[addr] = (profit, now_ms)
            by_coin = count_recent_trades_by_coin(fills, cutoff_ms)
            if any(TRADES_MIN <= n <= TRADES_MAX for n in by_coin.values()) and profit < 0:
                result.append(addr)
        except Exception as e:
            fail_count += 1
            print(f"  [{addr}] fills 失败: {e}")
        if i % DEALS_FLUSH_N == 0 or i == total:
            batch_id += 1
            flush_deals(deals_buf, batch_id)
        if i % 50 == 0 or i == total:
            print(f"  [fills] 进度 {i}/{total}，已筛选 {len(result)} 个，耗时 {time.time() - t0:.1f}s")
        time.sleep(FILLS_INTERVAL)
    merge_deals_parts()
    save_address_cache(ADDRESS_PROFIT_FILE, cache, "profit")
    print(f"  [fills] 查询失败: {fail_count} 次")
    return result


def fetch_account_value(address):
    try:
        resp = requests.post(
            API_URL, json={"type": "clearinghouseState", "user": address},
            headers={"Content-Type": "application/json"}, timeout=TIMEOUT,
        )
        if not resp.ok:
            return None
        val = (resp.json().get("marginSummary") or {}).get("accountValue")
        return float(val) if val is not None else None
    except Exception as e:
        print(f"  [{address}] 查询失败: {e}")
        return None


def filter_by_equity(addresses):
    cache = load_address_cache(ADDRESS_PNL_FILE, "pnl")
    result, fail_count, total = [], 0, len(addresses)
    print(f"  [equity] 待查询 {total} 个地址")
    t0 = time.time()
    for i, addr in enumerate(addresses, 1):
        now_ms = int(time.time() * 1000)
        val = get_cached_value(cache, addr, now_ms)
        if val is None:
            val = fetch_account_value(addr)
            if val is not None:
                cache[addr] = (val, now_ms)
            time.sleep(REQ_INTERVAL)
        if val is None:
            fail_count += 1
        elif MIN_EQUITY < val < MAX_EQUITY:
            result.append(addr)
        if i % 50 == 0 or i == total:
            print(f"  [equity] 进度 {i}/{total}，已筛选 {len(result)} 个，耗时 {time.time() - t0:.1f}s")
    save_address_cache(ADDRESS_PNL_FILE, cache, "pnl")
    print(f"  [equity] 查询失败: {fail_count} 次")
    return result


def save_filter_addresses(addresses):
    FILTER_DIR.mkdir(parents=True, exist_ok=True)
    out = FILTER_DIR / f"{datetime.now():%Y-%m-%d}_filter_address.parquet"
    pd.DataFrame({"address": addresses}).to_parquet(out, index=False)
    print(f"已保存 {len(addresses)} 个地址 -> {out}")


def prune_stale_addresses():
    if not INPUT_FILE.exists():
        return
    df = pd.read_parquet(INPUT_FILE)
    before = len(df)
    cutoff = int((datetime.now() - timedelta(days=STALE_DAYS)).timestamp() * 1000)
    df = df[df["time"] >= cutoff]
    df.to_parquet(INPUT_FILE, index=False)
    print(f"清理 inactive: 删除 {before - len(df)} 个，剩余 {len(df)} 个")


def run_job():
    t1 = int(time.time())
    print(f"\n[{datetime.now():%Y-%m-%d %H:%M:%S}] 开始筛选")
    addrs = load_active_addresses()
    print(f"步骤1 近{LAST_ACTIVE_MAX_DAYS_AGO}d有成交: {len(addrs)} 个")
    if addrs:
        addrs = filter_by_equity(addrs)
        print(f"步骤2 权益符合: {len(addrs)} 个")
        addrs = filter_by_fills(addrs)
        print(f"步骤3 笔数+盈亏符合: {len(addrs)} 个")
    save_filter_addresses(addrs)
    prune_stale_addresses()
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from score_address_deals import main as run_score
    print(f"\n[{datetime.now():%Y-%m-%d %H:%M:%S}] 开始打分")
    run_score()
    t2 = int(time.time())
    print(f"总耗时: {t2 - t1} 秒 / {(t2-t1)/60:.1f} 分钟 / {(t2-t1)/3600:.1f} 小时")


def main():
    run_job()


if __name__ == "__main__":
    main()
