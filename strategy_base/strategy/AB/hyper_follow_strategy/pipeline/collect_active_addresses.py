#!/usr/bin/env python3
"""
功能：订阅多交易对成交，累积活跃地址，定期写入 parquet。

用法：
    cd /home/ubuntu/strategy_base/strategy/AB/HYPER
    python pipeline/collect_active_addresses.py
"""

import sys
import threading
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hyper_config import DATA_DIR
from sub_hyper_deals import run

COINS = ["BTC", "ETH", "SOL", "HYPE"]
ACTIVE_FILE = DATA_DIR / "active_address.parquet"
FLUSH_INTERVAL = 10
PRUNE_INTERVAL = 60
STALE_MS = 24 * 3600 * 1000  # 超过 24h 删除

_addr_buffer = {}
_lock = threading.Lock()


def on_subscribe(data):
    sub = (data or {}).get("subscription") or {}
    print(f"[{sub.get('coin', '?')}] 订阅确认")


def on_trade(trade):
    users = trade.get("users") or []
    ts = trade.get("time")
    print(f"[{trade.get('coin')}] px={trade.get('px')} sz={trade.get('sz')} users={users}")
    if not ts:
        return
    with _lock:
        for addr in users:
            if isinstance(addr, str) and addr.startswith("0x"):
                key = addr.lower()
                _addr_buffer[key] = max(_addr_buffer.get(key, 0), ts)


def flush_active():
    with _lock:
        if not _addr_buffer:
            return
        batch = pd.DataFrame([{"address": k, "time": v} for k, v in _addr_buffer.items()])
        _addr_buffer.clear()
    if ACTIVE_FILE.exists():
        try:
            batch = pd.concat([pd.read_parquet(ACTIVE_FILE), batch], ignore_index=True)
        except Exception as e:
            print(f"读取 {ACTIVE_FILE.name} 失败: {e}")
    batch = batch.groupby("address", as_index=False)["time"].max()
    batch.to_parquet(ACTIVE_FILE, index=False)
    print(f"已保存 {len(batch)} 个地址 -> {ACTIVE_FILE}")


def prune_stale():
    """删除 active_address.parquet 中超过 24h 未活跃的地址。"""
    if not ACTIVE_FILE.exists():
        return
    df = pd.read_parquet(ACTIVE_FILE)
    before = len(df)
    cutoff = int(time.time() * 1000) - STALE_MS
    df = df[df["time"] >= cutoff]
    if len(df) == before:
        return
    df.to_parquet(ACTIVE_FILE, index=False)
    print(f"清理过期地址: 删除 {before - len(df)} 个，剩余 {len(df)} 个")


def flush_loop():
    while True:
        time.sleep(FLUSH_INTERVAL)
        try:
            flush_active()
        except Exception as e:
            print(f"保存失败: {e}")


def prune_loop():
    while True:
        time.sleep(PRUNE_INTERVAL)
        try:
            prune_stale()
        except Exception as e:
            print(f"清理失败: {e}")


def main():
    threading.Thread(target=flush_loop, daemon=True).start()
    threading.Thread(target=prune_loop, daemon=True).start()
    print(f"订阅 {COINS}，每 {FLUSH_INTERVAL}s 写入，每 {PRUNE_INTERVAL}s 清理 24h 过期地址")
    run(COINS, on_trade=on_trade, on_subscribe=on_subscribe)


if __name__ == "__main__":
    main()
