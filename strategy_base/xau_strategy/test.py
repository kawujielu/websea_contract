# -*- coding: utf-8 -*-
"""
导出 XAU 资管账户 10041327 与 Binance 跟单账户成交明细为 CSV。

口径对齐 follow_xau_strategy.py：
  - MetaAPI RPC get_deals_by_time_range
  - Binance U 本位 /fapi/v1/userTrades（XAUTUSDT）

时间窗口：北京时间 START 起至当前。
每边 CSV 列：成交时间(北京), 成交价格, 数量, 方向

运行:
  py export_xau_bn_trades_csv.py
依赖: pip install metaapi-cloud-sdk
"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import hmac
import json
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from metaapi_cloud_sdk import MetaApi
from metaapi_cloud_sdk.logger import NativeLogger

NativeLogger.warning = lambda self, msg, *args, **kwargs: None

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ========== 配置（与 follow_xau_strategy.py 同账户） ==========
LOGIN = "10041327"
ACCOUNT_ID = "eca68df4-1ad4-40b5-b705-9808dad93fde"
TOKEN = (
    "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI0MDI2MzRhMzczY2IwMTk3OGM3NjBhMDU3MjI0MDRkZCIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmVjYTY4ZGY0LTFhZDQtNDBiNS1iNzA1LTk4MDhkYWQ5M2ZkZSJdfSx7ImlkIjoibWV0YWFwaS1yZXN0LWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZWNhNjhkZjQtMWFkNC00MGI1LWI3MDUtOTgwOGRhZDkzZmRlIl19LHsiaWQiOiJtZXRhYXBpLXJwYy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDplY2E2OGRmNC0xYWQ0LTQwYjUtYjcwNS05ODA4ZGFkOTNmZGUiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDplY2E2OGRmNC0xYWQ0LTQwYjUtYjcwNS05ODA4ZGFkOTNmZGUiXX0seyJpZCI6Im1ldGFzdGF0cy1hcGkiLCJtZXRob2RzIjpbIm1ldGFzdGF0cy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZWNhNjhkZjQtMWFkNC00MGI1LWI3MDUtOTgwOGRhZDkzZmRlIl19LHsiaWQiOiJyaXNrLW1hbmFnZW1lbnQtYXBpIiwibWV0aG9kcyI6WyJyaXNrLW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmVjYTY4ZGY0LTFhZDQtNDBiNS1iNzA1LTk4MDhkYWQ5M2ZkZSJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiNDAyNjM0YTM3M2NiMDE5NzhjNzYwYTA1NzIyNDA0ZGQiLCJpYXQiOjE3ODgyODAwNjgsImV4cCI6MTc5NjA1NjA2OH0.j3OxtUTTusS8P5Sap9LfFMf_rnXIlR4V7MMfo-Glt8_R62oDJHwGmf1M6JcLGqh8O6y3AkQxszPykbX2XkEL6jrGDrBkPnP6Go2wBmXdHJTxaeqF1GWF2NE1LSaAePLsGtwFi29yw3c7whP3sebF35WaW-bR5EQsKV98o2XRt1o4-ECos2VonOeuWcmU-U091pft70ax02sJOfJAMj6gKcx-6qSyufr3xLgnck8hjnH25N2Wi9UkD_6nWQT1l5WZjskiUdRxSOImsrb2TYeG1YCUzEaBIJL_RYTibGDfWBU8icTd25oy2vkuSIr42Zby0B547iqaSqNlsovd9qb47-6ZDdIIgnkprg9PEXWZ2HilzgRzntJf0KlwoeUFYsjD9kEHwUeFPhAmIJwZTo3HinJnKYz0rHYn7bH1CIhIpKO_xqrIzoWaRSs6hk7oEt__amUUgaA-QjY58atwGDWDfcAKCGTZKOqDsfzt1jxs27W4-6KbRIlDhi8Q3e1jpmS_x0kyDt4PDuAxZ3HBy-aJdIoEQ7IpFXcUmgsp3CHOiXTlwCy3lTgTDI5vCKZMIoeSYyyoMFynUWdaEF8KNb-XqHNxHb28OrfBdW9QZG0pQDlJvGK0fUd5vsol7QeLE3uWqOFEYFkcEFM1kMAXhl8xHwUuKvptZ4YuM8e_Vbh_SOw"
)

BN_API_KEY = "ZlbbTVrOSfCUBtj6b6eNTCiTNjsojoxMRaDGIEZjORCRTm2dSax7sdwi9PgyMvGu"
BN_API_SECRET = "M62FUBt8Prey2Q2B0YszyB2Ms9nXb5hd5o01sF8LEablSG1qtVev7bjJXkbT0fSu"
BN_SYMBOL = "XAUTUSDT"
FAPI = "https://fapi.binance.com"

# 北京时间起点：2026-09-24 上午 10:00
START_BJ = datetime(2026, 9, 24, 10, 0, 0)

_HERE = Path(__file__).resolve().parent
OUT_DIR = _HERE / "deals_csv"
XAU_CSV = OUT_DIR / f"xau_{LOGIN}_trades.csv"
BN_CSV = OUT_DIR / f"bn_{BN_SYMBOL}_trades.csv"
# ==========================

BJ = timezone(timedelta(hours=8))
CSV_FIELDS = ["成交时间", "成交价格", "数量", "方向"]

_SIDE_CN = {
    "DEAL_TYPE_BUY": "买入",
    "DEAL_TYPE_SELL": "卖出",
    "BUY": "买入",
    "SELL": "卖出",
}


def _log(msg: str) -> None:
    ts = datetime.now(BJ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _as_dict(deal) -> Dict[str, Any]:
    if isinstance(deal, dict):
        return dict(deal)
    out: Dict[str, Any] = {}
    raw = getattr(deal, "__dict__", None)
    if isinstance(raw, dict):
        for k, v in raw.items():
            if not str(k).startswith("_"):
                out[k] = v
    for k in ("id", "type", "time", "brokerTime", "volume", "price", "symbol"):
        if k not in out and hasattr(deal, k):
            out[k] = getattr(deal, k)
    return out


def _to_bj_str(t) -> str:
    if t is None:
        return ""
    if isinstance(t, datetime):
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return t.astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(t, (int, float)):
        ms = float(t)
        if ms > 1e12:
            ms /= 1000.0
        return datetime.fromtimestamp(ms, tz=timezone.utc).astimezone(BJ).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    return str(t)


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in CSV_FIELDS})
    _log(f"已写入 {path} 共 {len(rows)} 行")


def _patch_subscribe_rate_limit() -> None:
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
                if wait > 120:
                    wait = 5
                _log(f"MetaAPI 限流，{wait:.1f}s 后重试…")
                await asyncio.sleep(wait)
                return await orig(self, account_id, instance_number)

        return wrapped()

    SubscriptionManager.subscribe = subscribe


async def fetch_xau_rows(start_utc: datetime, end_utc: datetime) -> List[Dict[str, Any]]:
    """与 select_xau_deals.py 相同：RPC get_deals_by_time_range。

    token 为 account access（management=reader）时无法 deployAccount；
    若 deploy 被拒，则短暂等待账户被控制台/其他进程置为 DEPLOYED。
    """
    _patch_subscribe_rate_limit()
    api = MetaApi(TOKEN)
    account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
    _log(f"[{LOGIN}] state={account.state} login={account.login}")
    if account.state != "DEPLOYED":
        try:
            await account.deploy()  # 同 select_xau_deals
        except Exception as e:
            _log(f"[{LOGIN}] deploy 不可用（token 无权限）: {e}")
            _log(f"[{LOGIN}] 等待账户变为 DEPLOYED（最长 90s，请在 MetaAPI 控制台 Deploy）…")
            deadline = time.time() + 90
            while time.time() < deadline:
                await asyncio.sleep(5)
                account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
                _log(f"[{LOGIN}] state={account.state}")
                if account.state == "DEPLOYED":
                    break
            if account.state != "DEPLOYED":
                raise RuntimeError(
                    f"[{LOGIN}] 仍为 {account.state}。select_xau_deals 同样需要 DEPLOYED；"
                    f"请在 https://app.metaapi.cloud 对该账户点 Deploy 后重跑"
                ) from None
    await account.wait_connected()

    rpc = account.get_rpc_connection()
    await rpc.connect()
    await rpc.wait_synchronized()
    try:
        result = await rpc.get_deals_by_time_range(start_time=start_utc, end_time=end_utc)
        raw = result.get("deals", result) if isinstance(result, dict) else (result or [])
        rows: List[Dict[str, Any]] = []
        for d in raw:
            dd = _as_dict(d)
            if dd.get("type") == "DEAL_TYPE_BALANCE":
                continue
            side = _SIDE_CN.get(str(dd.get("type") or ""), str(dd.get("type") or ""))
            rows.append(
                {
                    "成交时间": _to_bj_str(dd.get("time")),
                    "成交价格": dd.get("price"),
                    "数量": dd.get("volume"),
                    "方向": side,
                }
            )
        rows.sort(key=lambda r: str(r.get("成交时间") or ""))
        return rows
    finally:
        await rpc.close()


def _bn_request(method: str, path: str, params: dict):
    params = dict(params)
    params["timestamp"] = int(time.time() * 1000)
    query = urllib.parse.urlencode(params)
    sig = hmac.new(BN_API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request(
        f"{FAPI}{path}?{query}&signature={sig}",
        method=method,
        headers={"X-MBX-APIKEY": BN_API_KEY},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode()) from e


def fetch_bn_rows(start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
    """与 follow_xau_strategy._fetch_user_trades 相同分页逻辑。"""
    trades = []
    seen = set()
    cursor = start_ms
    while cursor < end_ms:
        chunk_end = min(cursor + 7 * 24 * 3600 * 1000 - 1, end_ms)
        batch = _bn_request(
            "GET",
            "/fapi/v1/userTrades",
            {
                "symbol": BN_SYMBOL,
                "startTime": cursor,
                "endTime": chunk_end,
                "limit": 1000,
            },
        )
        while batch:
            for t in batch:
                tid = int(t["id"])
                if tid in seen:
                    continue
                ts = int(t["time"])
                if ts < cursor or ts > chunk_end:
                    continue
                seen.add(tid)
                trades.append(t)
            if len(batch) < 1000:
                break
            nxt = _bn_request(
                "GET",
                "/fapi/v1/userTrades",
                {
                    "symbol": BN_SYMBOL,
                    "fromId": int(batch[-1]["id"]) + 1,
                    "limit": 1000,
                },
            )
            if not nxt or int(nxt[0]["time"]) > chunk_end:
                break
            batch = nxt
            time.sleep(0.05)
        cursor = chunk_end + 1

    trades.sort(key=lambda t: (int(t["time"]), int(t["id"])))
    rows: List[Dict[str, Any]] = []
    for t in trades:
        rows.append(
            {
                "成交时间": _to_bj_str(int(t["time"])),
                "成交价格": t.get("price"),
                "数量": t.get("qty"),
                "方向": _SIDE_CN.get(str(t.get("side") or ""), str(t.get("side") or "")),
            }
        )
    return rows


async def main() -> None:
    start_bj = START_BJ.replace(tzinfo=BJ)
    end_bj = datetime.now(BJ)
    start_utc = start_bj.astimezone(timezone.utc)
    end_utc = end_bj.astimezone(timezone.utc)
    start_ms = int(start_bj.timestamp() * 1000)
    end_ms = int(end_bj.timestamp() * 1000)

    _log(
        f"导出窗口(北京) {start_bj.strftime('%Y-%m-%d %H:%M:%S')} ~ "
        f"{end_bj.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    # 先导 BN（不依赖 MetaAPI 部署）
    try:
        bn_rows = fetch_bn_rows(start_ms, end_ms)
        _write_csv(BN_CSV, bn_rows)
    except Exception:
        _log(f"BN 导出失败:\n{traceback.format_exc()}")
        bn_rows = []

    try:
        xau_rows = await fetch_xau_rows(start_utc, end_utc)
        _write_csv(XAU_CSV, xau_rows)
    except Exception:
        _log(f"XAU/MetaAPI 导出失败:\n{traceback.format_exc()}")
        xau_rows = []

    _log(f"完成：XAU={len(xau_rows)} 笔，BN={len(bn_rows)} 笔")
    if XAU_CSV.exists():
        _log(f"  {XAU_CSV}")
    if BN_CSV.exists():
        _log(f"  {BN_CSV}")


if __name__ == "__main__":
    asyncio.run(main())

