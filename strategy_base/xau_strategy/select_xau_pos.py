#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询 XAU 资管账户 10041327 当前权益与持仓（只读，查完即退出）。

口径对齐 策略/资管监控/xau_account_monitor.py：MetaAPI RPC
  - get_account_information() → 权益/余额/保证金等
  - get_positions() → 当前持仓

运行:
  py xau_query_10041327_equity_positions.py
依赖: pip install metaapi-cloud-sdk
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

from metaapi_cloud_sdk import MetaApi

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ========== 账户 10041327（同 xau_account_monitor.py） ==========
LOGIN = "10041327"
ACCOUNT_ID = "eca68df4-1ad4-40b5-b705-9808dad93fde"
TOKEN = (
    "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI0MDI2MzRhMzczY2IwMTk3OGM3NjBhMDU3MjI0MDRkZCIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmVjYTY4ZGY0LTFhZDQtNDBiNS1iNzA1LTk4MDhkYWQ5M2ZkZSJdfSx7ImlkIjoibWV0YWFwaS1yZXN0LWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZWNhNjhkZjQtMWFkNC00MGI1LWI3MDUtOTgwOGRhZDkzZmRlIl19LHsiaWQiOiJtZXRhYXBpLXJwYy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDplY2E2OGRmNC0xYWQ0LTQwYjUtYjcwNS05ODA4ZGFkOTNmZGUiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDplY2E2OGRmNC0xYWQ0LTQwYjUtYjcwNS05ODA4ZGFkOTNmZGUiXX0seyJpZCI6Im1ldGFzdGF0cy1hcGkiLCJtZXRob2RzIjpbIm1ldGFzdGF0cy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZWNhNjhkZjQtMWFkNC00MGI1LWI3MDUtOTgwOGRhZDkzZmRlIl19LHsiaWQiOiJyaXNrLW1hbmFnZW1lbnQtYXBpIiwibWV0aG9kcyI6WyJyaXNrLW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmVjYTY4ZGY0LTFhZDQtNDBiNS1iNzA1LTk4MDhkYWQ5M2ZkZSJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiNDAyNjM0YTM3M2NiMDE5NzhjNzYwYTA1NzIyNDA0ZGQiLCJpYXQiOjE3ODgyODAwNjgsImV4cCI6MTc5NjA1NjA2OH0.j3OxtUTTusS8P5Sap9LfFMf_rnXIlR4V7MMfo-Glt8_R62oDJHwGmf1M6JcLGqh8O6y3AkQxszPykbX2XkEL6jrGDrBkPnP6Go2wBmXdHJTxaeqF1GWF2NE1LSaAePLsGtwFi29yw3c7whP3sebF35WaW-bR5EQsKV98o2XRt1o4-ECos2VonOeuWcmU-U091pft70ax02sJOfJAMj6gKcx-6qSyufr3xLgnck8hjnH25N2Wi9UkD_6nWQT1l5WZjskiUdRxSOImsrb2TYeG1YCUzEaBIJL_RYTibGDfWBU8icTd25oy2vkuSIr42Zby0B547iqaSqNlsovd9qb47-6ZDdIIgnkprg9PEXWZ2HilzgRzntJf0KlwoeUFYsjD9kEHwUeFPhAmIJwZTo3HinJnKYz0rHYn7bH1CIhIpKO_xqrIzoWaRSs6hk7oEt__amUUgaA-QjY58atwGDWDfcAKCGTZKOqDsfzt1jxs27W4-6KbRIlDhi8Q3e1jpmS_x0kyDt4PDuAxZ3HBy-aJdIoEQ7IpFXcUmgsp3CHOiXTlwCy3lTgTDI5vCKZMIoeSYyyoMFynUWdaEF8KNb-XqHNxHb28OrfBdW9QZG0pQDlJvGK0fUd5vsol7QeLE3uWqOFEYFkcEFM1kMAXhl8xHwUuKvptZ4YuM8e_Vbh_SOw"
)
# ================================================================

BJ = timezone(timedelta(hours=8))

_TYPE_CN = {
    "POSITION_TYPE_BUY": "多",
    "POSITION_TYPE_SELL": "空",
    "buy": "多",
    "sell": "空",
}


def _g(obj: Any, key: str, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _f(v: Any, nd: int = 2) -> str:
    if v is None or v == "":
        return "-"
    try:
        return f"{float(v):,.{nd}f}"
    except (TypeError, ValueError):
        return str(v)


def _as_list(result: Any) -> list:
    if result is None:
        return []
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        for k in ("positions", "items", "data"):
            if isinstance(result.get(k), list):
                return result[k]
        return [result]
    return list(result) if result else []


def _patch_subscribe_rate_limit():
    """与 follow_xau_strategy 相同：避免 429 metadata 缺 type 时 KeyError。"""
    try:
        from metaapi_cloud_sdk.clients.error_handler import TooManyRequestsException
        from metaapi_cloud_sdk.clients.metaapi.subscription_manager import SubscriptionManager
    except Exception:
        return
    orig = SubscriptionManager.subscribe

    def subscribe(self, account_id, instance_number):
        coro = orig(self, account_id, instance_number)

        async def wrapped():
            try:
                return await coro
            except TooManyRequestsException as err:
                meta = err.metadata if isinstance(getattr(err, "metadata", None), dict) else {}
                if "type" not in meta:
                    meta = {**meta, "type": "LIMIT_REQUEST_RATE_PER_USER"}
                    try:
                        err.metadata = meta
                    except Exception:
                        pass
                wait = float(meta.get("recommendedRetryTime") or meta.get("waitTimeInSeconds") or 5)
                print(f"[warn] MetaAPI 限流，{wait:.1f}s 后重试…")
                await asyncio.sleep(wait)
                return await orig(self, account_id, instance_number)

        return wrapped()

    SubscriptionManager.subscribe = subscribe


async def main() -> None:
    _patch_subscribe_rate_limit()
    now = datetime.now(BJ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"查询时间(北京): {now}")
    print(f"账户 login={LOGIN}  account_id={ACCOUNT_ID}")
    print()

    api = MetaApi(TOKEN)
    account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
    print(f"MetaAPI 状态: {account.state}  login={account.login}")

    if account.state != "DEPLOYED":
        print("部署中…")
        await account.deploy()
    await account.wait_connected()

    rpc = account.get_rpc_connection()
    await rpc.connect()
    await rpc.wait_synchronized()
    try:
        info = await rpc.get_account_information()
        positions = _as_list(await rpc.get_positions())

        cur = _g(info, "currency") or ""
        print("======== 账户权益 ========")
        print(f"  权益 equity      : {_f(_g(info, 'equity'))} {cur}")
        print(f"  余额 balance     : {_f(_g(info, 'balance'))} {cur}")
        print(f"  已用保证金 margin: {_f(_g(info, 'margin'))} {cur}")
        print(f"  可用保证金 free  : {_f(_g(info, 'freeMargin'))} {cur}")
        print(f"  保证金水平       : {_f(_g(info, 'marginLevel'))} %")
        print(f"  杠杆             : 1:{_g(info, 'leverage')}")
        print(f"  浮盈 profit      : {_f(_g(info, 'profit'))} {cur}")
        print(f"  信用 credit      : {_f(_g(info, 'credit'))} {cur}")

        print()
        print(f"======== 当前持仓（{len(positions)} 笔）========")
        if not positions:
            print("  （无持仓）")
        else:
            print(
                f"{'ticket':<12} {'方向':<4} {'品种':<14} {'手数':>8} "
                f"{'开仓价':>12} {'现价':>12} {'浮盈':>12} {'swap':>10}"
            )
            total_profit = 0.0
            total_volume = 0.0
            for p in positions:
                typ = str(_g(p, "type") or _g(p, "side") or "")
                side = _TYPE_CN.get(typ, typ.replace("POSITION_TYPE_", "") or "-")
                vol = float(_g(p, "volume") or 0)
                profit = float(_g(p, "profit") or 0) + float(_g(p, "swap") or 0) + float(
                    _g(p, "commission") or 0
                )
                total_profit += profit
                total_volume += vol
                print(
                    f"{str(_g(p, 'id') or _g(p, 'ticket') or '-'):<12} "
                    f"{side:<4} {str(_g(p, 'symbol') or '-'):<14} "
                    f"{vol:>8.2f} {_f(_g(p, 'openPrice'), 3):>12} "
                    f"{_f(_g(p, 'currentPrice'), 3):>12} {_f(profit):>12} "
                    f"{_f(_g(p, 'swap')):>10}"
                )
            print("-" * 90)
            print(f"  持仓手数合计={total_volume:.2f}  持仓浮盈合计(含swap)={total_profit:,.2f} {cur}")
    finally:
        await rpc.close()
    print("\n完成。")


if __name__ == "__main__":
    asyncio.run(main())

