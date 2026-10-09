"""导出 XAU 资管账户历史成交为 CSV，每个 login 一个文件。

数据源与 xau_trade_pnl_stats.py 一致：
- MetaAPI get_deals_by_time_range（默认近 3650 天）
- 账户配置复用 xau_account_monitor.ACCOUNTS

输出（默认脚本同目录 deals_csv/）:
  deals_{login}.csv

运行: py xau_deals_to_csv.py
依赖: pip install metaapi-cloud-sdk
"""
from __future__ import annotations

import asyncio
import csv
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from metaapi_cloud_sdk import MetaApi
from metaapi_cloud_sdk.logger import NativeLogger

NativeLogger.warning = lambda self, msg, *args, **kwargs: None

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from xau_account_monitor import ACCOUNTS  # noqa: E402

# ========== 配置 ==========
DAYS = 3650  # 回溯天数，与 xau_trade_pnl_stats 一致
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "deals_csv")
# 导出列顺序（有则写；没有的字段留空）
CSV_COLUMNS = [
    "id",
    "platform",
    "type",
    "entryType",
    "symbol",
    "magic",
    "time",
    "brokerTime",
    "commission",
    "swap",
    "profit",
    "brokerComment",
    "comment",
    "positionId",
    "orderId",
    "reason",
    "volume",
    "price",
    "accountCurrencyExchangeRate",
    "updateSequenceNumber",
]
# ==========================


def _as_dict(deal) -> Dict[str, Any]:
    """尽量展开 MetaAPI deal 对象全部字段。"""
    if isinstance(deal, dict):
        return dict(deal)
    out: Dict[str, Any] = {}
    # 优先已知字段
    for k in CSV_COLUMNS:
        if hasattr(deal, k):
            out[k] = getattr(deal, k)
    # 再扫 __dict__ / 公开属性
    raw = getattr(deal, "__dict__", None)
    if isinstance(raw, dict):
        for k, v in raw.items():
            if k.startswith("_"):
                continue
            out.setdefault(k, v)
    return out


def _fix_mojibake(s: str) -> str:
    """修复 comment 等字段的中文乱码。

    典型：券商/终端侧为 GBK（如「千金量化[tp]」），被当成 Latin-1 读成
    「Ç§½ðÁ¿»¯[tp]」。已是正常中文则不改。
    """
    if not s:
        return s
    # 已有汉字，视为正常 UTF-8
    if any("\u4e00" <= c <= "\u9fff" for c in s):
        return s
    # 无可疑高位字符，无需修
    if not any(ord(c) > 127 for c in s):
        return s
    for src in ("latin-1", "cp1252"):
        for dst in ("gbk", "gb18030"):
            try:
                fixed = s.encode(src).decode(dst)
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
            if any("\u4e00" <= c <= "\u9fff" for c in fixed):
                return fixed
    return s


def _cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.isoformat()
        return v.astimezone(timezone.utc).isoformat()
    if isinstance(v, (dict, list)):
        return _fix_mojibake(str(v))
    if isinstance(v, str):
        return _fix_mojibake(v)
    return _fix_mojibake(str(v))


def _sort_key(d: Dict[str, Any]):
    t = d.get("time") or d.get("brokerTime") or ""
    return str(t), str(d.get("id") or "")


async def fetch_deals(cfg: dict) -> tuple[str, List[Dict[str, Any]]]:
    login = str(cfg["login"])
    api = MetaApi(cfg["token"])
    account = await api.metatrader_account_api.get_account(cfg["account_id"])
    if account.state != "DEPLOYED":
        await account.deploy()
    await account.wait_connected()

    rpc = account.get_rpc_connection()
    await rpc.connect()
    await rpc.wait_synchronized()
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=DAYS)
        result = await rpc.get_deals_by_time_range(start_time=start, end_time=end)
        deals = result.get("deals", result) if isinstance(result, dict) else (result or [])
        rows = [_as_dict(d) for d in deals]
        rows.sort(key=_sort_key)
        return login, rows
    finally:
        await rpc.close()


def write_csv(login: str, rows: List[Dict[str, Any]], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"deals_{login}.csv"

    # 列 = 固定列 + 额外出现过的字段（稳定排序）
    extra = sorted(
        {k for r in rows for k in r.keys()} - set(CSV_COLUMNS),
        key=str,
    )
    columns = list(CSV_COLUMNS) + extra

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({c: _cell(r.get(c)) for c in columns})
    return path


async def export_one(cfg: dict, out_dir: Path) -> tuple[str, Optional[Path], int, Optional[str]]:
    login = str(cfg["login"])
    try:
        login, rows = await fetch_deals(cfg)
        path = write_csv(login, rows, out_dir)
        return login, path, len(rows), None
    except Exception as e:
        return login, None, 0, str(e)


async def main() -> None:
    out_dir = Path(OUTPUT_DIR)
    print(f"共导出 {len(ACCOUNTS)} 个账户历史成交 → {out_dir}")
    print(f"回溯天数 DAYS={DAYS}")

    results = await asyncio.gather(
        *(export_one(cfg, out_dir) for cfg in ACCOUNTS),
        return_exceptions=False,
    )

    ok = 0
    for login, path, n, err in results:
        if err:
            print(f"[{login}] 失败: {err}")
            continue
        print(f"[{login}] 成交 {n} 条 → {path}")
        ok += 1
    print(f"完成: 成功 {ok}/{len(ACCOUNTS)}")


if __name__ == "__main__":
    asyncio.run(main())

