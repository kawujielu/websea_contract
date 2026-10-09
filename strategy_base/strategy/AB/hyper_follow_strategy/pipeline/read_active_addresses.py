#!/usr/bin/env python3
"""
功能：读取 parquet_file 目录下所有 parquet 摘要。

用法：
    cd /home/ubuntu/strategy_base/strategy/AB/HYPER
    python pipeline/read_active_addresses.py
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hyper_config import DATA_DIR

ACTIVE_FILE = "active_address.parquet"
TOP_N = 10


def main():
    files = sorted(DATA_DIR.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"目录下无 parquet: {DATA_DIR}")
    active = DATA_DIR / ACTIVE_FILE
    if active in files:
        files.remove(active)
        files.insert(0, active)
    for path in files:
        df = pd.read_parquet(path)
        label = "总地址数" if path.name == ACTIVE_FILE else "总条数"
        print(f"\n=== {path.name} ===")
        print(f"{label}: {len(df)}")
        print(df.head(TOP_N).to_string(index=False))


if __name__ == "__main__":
    main()
