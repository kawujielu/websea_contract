"""查询 XAU 资管账户 10041327 最近 N 天成交明细。

运行: py xau_latest_deals.py
依赖: pip install metaapi-cloud-sdk
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from metaapi_cloud_sdk import MetaApi
from metaapi_cloud_sdk.logger import NativeLogger

NativeLogger.warning = lambda self, msg, *args, **kwargs: None

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ========== 配置 ==========
LOGIN = "10041327"
ACCOUNT_ID = "eca68df4-1ad4-40b5-b705-9808dad93fde"
TOKEN = (
    "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI0MDI2MzRhMzczY2IwMTk3OGM3NjBhMDU3MjI0MDRkZCIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmVjYTY4ZGY0LTFhZDQtNDBiNS1iNzA1LTk4MDhkYWQ5M2ZkZSJdfSx7ImlkIjoibWV0YWFwaS1yZXN0LWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZWNhNjhkZjQtMWFkNC00MGI1LWI3MDUtOTgwOGRhZDkzZmRlIl19LHsiaWQiOiJtZXRhYXBpLXJwYy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDplY2E2OGRmNC0xYWQ0LTQwYjUtYjcwNS05ODA4ZGFkOTNmZGUiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDplY2E2OGRmNC0xYWQ0LTQwYjUtYjcwNS05ODA4ZGFkOTNmZGUiXX0seyJpZCI6Im1ldGFzdGF0cy1hcGkiLCJtZXRob2RzIjpbIm1ldGFzdGF0cy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZWNhNjhkZjQtMWFkNC00MGI1LWI3MDUtOTgwOGRhZDkzZmRlIl19LHsiaWQiOiJyaXNrLW1hbmFnZW1lbnQtYXBpIiwibWV0aG9kcyI6WyJyaXNrLW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmVjYTY4ZGY0LTFhZDQtNDBiNS1iNzA1LTk4MDhkYWQ5M2ZkZSJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiNDAyNjM0YTM3M2NiMDE5NzhjNzYwYTA1NzIyNDA0ZGQiLCJpYXQiOjE3ODgyODAwNjgsImV4cCI6MTc5NjA1NjA2OH0.j3OxtUTTusS8P5Sap9LfFMf_rnXIlR4V7MMfo-Glt8_R62oDJHwGmf1M6JcLGqh8O6y3AkQxszPykbX2XkEL6jrGDrBkPnP6Go2wBmXdHJTxaeqF1GWF2NE1LSaAePLsGtwFi29yw3c7whP3sebF35WaW-bR5EQsKV98o2XRt1o4-ECos2VonOeuWcmU-U091pft70ax02sJOfJAMj6gKcx-6qSyufr3xLgnck8hjnH25N2Wi9UkD_6nWQT1l5WZjskiUdRxSOImsrb2TYeG1YCUzEaBIJL_RYTibGDfWBU8icTd25oy2vkuSIr42Zby0B547iqaSqNlsovd9qb47-6ZDdIIgnkprg9PEXWZ2HilzgRzntJf0KlwoeUFYsjD9kEHwUeFPhAmIJwZTo3HinJnKYz0rHYn7bH1CIhIpKO_xqrIzoWaRSs6hk7oEt__amUUgaA-QjY58atwGDWDfcAKCGTZKOqDsfzt1jxs27W4-6KbRIlDhi8Q3e1jpmS_x0kyDt4PDuAxZ3HBy-aJdIoEQ7IpFXcUmgsp3CHOiXTlwCy3lTgTDI5vCKZMIoeSYyyoMFynUWdaEF8KNb-XqHNxHb28OrfBdW9QZG0pQDlJvGK0fUd5vsol7QeLE3uWqOFEYFkcEFM1kMAXhl8xHwUuKvptZ4YuM8e_Vbh_SOw"
)
DAYS = 2  # 查询最近 N 天成交
# ==========================

BJ = timezone(timedelta(hours=8))

_SIDE_CN = {
    "DEAL_TYPE_BUY": "买入",
    "DEAL_TYPE_SELL": "卖出",
}


def _as_dict(deal) -> Dict[str, Any]:
    if isinstance(deal, dict):
        return dict(deal)
    out: Dict[str, Any] = {}
    raw = getattr(deal, "__dict__", None)
    if isinstance(raw, dict):
        for k, v in raw.items():
            if not str(k).startswith("_"):
                out[k] = v
    for k in ("id", "type", "time", "brokerTime", "volume", "price"):
        if k not in out and hasattr(deal, k):
            out[k] = getattr(deal, k)
    return out


def _to_bj(t) -> str:
    """UTC time → 北京时间字符串。"""
    if t is None:
        return "-"
    if isinstance(t, datetime):
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return t.astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")
    return str(t)


def _deal_sort_key(d: Dict[str, Any]):
    t = d.get("time")
    if isinstance(t, datetime):
        return t
    return str(t or d.get("brokerTime") or "")


def _fmt_deals(login: str, deals: List[Dict[str, Any]], days: int) -> str:
    lines = [
        f"=== 账户 [{login}] 最近 {days} 天成交（共 {len(deals)} 笔）===",
        f"{'北京时间':<20} {'价格':>10} {'数量':>8} {'方向':<4}",
    ]
    if not deals:
        lines.append("（无成交）")
        return "\n".join(lines)
    for d in deals:
        side = _SIDE_CN.get(d.get("type"), d.get("type", "-"))
        price = d.get("price")
        vol = d.get("volume")
        try:
            price_s = f"{float(price):.2f}"
        except (TypeError, ValueError):
            price_s = str(price)
        try:
            vol_s = f"{float(vol):.2f}"
        except (TypeError, ValueError):
            vol_s = str(vol)
        lines.append(f"{_to_bj(d.get('time')):<20} {price_s:>10} {vol_s:>8} {side}")
    return "\n".join(lines)


async def fetch_deals() -> str:
    api = MetaApi(TOKEN)
    account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
    print(f"[{LOGIN}] 状态: {account.state}  login: {account.login}")

    if account.state != "DEPLOYED":
        print(f"[{LOGIN}] 账户未部署，尝试 deploy…")
        try:
            await account.deploy()
        except Exception as e:
            raise RuntimeError(
                f"账户未部署且当前 token 无 deploy 权限，请先在 MetaAPI 控制台部署 "
                f"或先跑 xau_account_monitor.py 保持 DEPLOYED。原始错误: {e}"
            ) from e
    await account.wait_connected()

    rpc = account.get_rpc_connection()
    await rpc.connect()
    await rpc.wait_synchronized()
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=DAYS)
        result = await rpc.get_deals_by_time_range(start_time=start, end_time=end)
        raw = result.get("deals", result) if isinstance(result, dict) else (result or [])
        trades = [
            _as_dict(d)
            for d in raw
            if _as_dict(d).get("type") != "DEAL_TYPE_BALANCE"
        ]
        trades.sort(key=_deal_sort_key, reverse=True)
        return _fmt_deals(LOGIN, trades, DAYS)
    finally:
        await rpc.close()


async def main():
    print(f"查询账户 [{LOGIN}] 最近 {DAYS} 天成交明细")
    try:
        print("\n" + await fetch_deals())
    except Exception as e:
        print(f"\n[{LOGIN}] 查询失败: {e}")


if __name__ == "__main__":
    asyncio.run(main())

