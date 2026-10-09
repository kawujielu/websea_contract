#!/usr/bin/env python3
"""读取桌面 order_records.log：同 exchange+coin 相反 side 计一笔盈亏。用法: python 统计盈亏.py [log路径]"""

import ast
import sys
from collections import defaultdict
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

LOG_CANDIDATES = [
    Path(sys.argv[1]) if len(sys.argv) > 1 else None,
    Path.home() / "Desktop" / "order_records.log",
    Path(r"C:\Users\linji\Desktop\order_records.log"),
    Path(__file__).resolve().parent / "order_records.log",
]


def load_orders(path: Path):
    text = path.read_text(encoding="utf-8").strip()
    if "记录订单:" in text:
        text = text.split("记录订单:", 1)[1].strip()
    return ast.literal_eval(text)


def main():
    log_path = next((p for p in LOG_CANDIDATES if p and p.is_file()), None)
    if not log_path:
        raise FileNotFoundError(f"未找到 order_records.log")
    print(f"读取: {log_path}")

    orders = load_orders(log_path)
    print(f"订单数: {len(orders)}")

    # win, lose, ratios, holds
    stats = defaultdict(lambda: [0, 0, [], []])
    prev = {}  # (exchange, coin) -> 上一笔

    for od in orders:
        key = (od["exchange"], od["coin"])
        # 用 raw_price（无则用 price）
        cur = {**od, "price": od.get("raw_price") or od["price"]}
        p = prev.get(key)
        if p and p["side"] != cur["side"]:
            # 相反方向 -> 一笔盈亏
            win = cur["price"] > p["price"] if p["side"] == "buy" else cur["price"] < p["price"]
            ratio = (
                (cur["price"] - p["price"]) / p["price"]
                if p["side"] == "buy"
                else (p["price"] - cur["price"]) / p["price"]
            )
            stats[key][0 if win else 1] += 1
            stats[key][2].append(ratio)
            stats[key][3].append(abs(cur["ts"] - p["ts"]) / 1000)
            prev[key] = None
        else:
            # 同向连续：覆盖为最新一笔
            prev[key] = cur

    for (ex, coin), (w, l, ratios, holds) in sorted(stats.items()):
        n = w + l
        print(
            f"{ex}_{coin} 胜:{w} 负:{l} 胜率:{(w / n if n else 0):.2%} "
            f"单笔盈利比例:{(sum(ratios) / len(ratios) if ratios else 0):.4%} "
            f"平均持仓时长:{(sum(holds) / len(holds) if holds else 0):.1f}s"
        )


if __name__ == "__main__":
    main()
