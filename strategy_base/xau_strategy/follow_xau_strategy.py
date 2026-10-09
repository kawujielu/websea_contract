"""用 MetaAPI Streaming(WebSocket) 订阅账户 10041327 的成交，打印原始数据。

运行: py follow_xau_strategy.py
依赖: pip install metaapi-cloud-sdk
同步阶段的历史回放不打印。新成交会打印原始数据，并向 Binance XAUTUSDT 合约限价跟单，下单后等待 3 秒再查询持仓。
"""
import asyncio
import hashlib
import hmac
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

from metaapi_cloud_sdk import MetaApi, SynchronizationListener

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

LOGIN = "10041327"
ACCOUNT_ID = "eca68df4-1ad4-40b5-b705-9808dad93fde"
TOKEN = (
    "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI0MDI2MzRhMzczY2IwMTk3OGM3NjBhMDU3MjI0MDRkZCIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmVjYTY4ZGY0LTFhZDQtNDBiNS1iNzA1LTk4MDhkYWQ5M2ZkZSJdfSx7ImlkIjoibWV0YWFwaS1yZXN0LWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZWNhNjhkZjQtMWFkNC00MGI1LWI3MDUtOTgwOGRhZDkzZmRlIl19LHsiaWQiOiJtZXRhYXBpLXJwYy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDplY2E2OGRmNC0xYWQ0LTQwYjUtYjcwNS05ODA4ZGFkOTNmZGUiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDplY2E2OGRmNC0xYWQ0LTQwYjUtYjcwNS05ODA4ZGFkOTNmZGUiXX0seyJpZCI6Im1ldGFzdGF0cy1hcGkiLCJtZXRob2RzIjpbIm1ldGFzdGF0cy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZWNhNjhkZjQtMWFkNC00MGI1LWI3MDUtOTgwOGRhZDkzZmRlIl19LHsiaWQiOiJyaXNrLW1hbmFnZW1lbnQtYXBpIiwibWV0aG9kcyI6WyJyaXNrLW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmVjYTY4ZGY0LTFhZDQtNDBiNS1iNzA1LTk4MDhkYWQ5M2ZkZSJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiNDAyNjM0YTM3M2NiMDE5NzhjNzYwYTA1NzIyNDA0ZGQiLCJpYXQiOjE3ODgyODAwNjgsImV4cCI6MTc5NjA1NjA2OH0.j3OxtUTTusS8P5Sap9LfFMf_rnXIlR4V7MMfo-Glt8_R62oDJHwGmf1M6JcLGqh8O6y3AkQxszPykbX2XkEL6jrGDrBkPnP6Go2wBmXdHJTxaeqF1GWF2NE1LSaAePLsGtwFi29yw3c7whP3sebF35WaW-bR5EQsKV98o2XRt1o4-ECos2VonOeuWcmU-U091pft70ax02sJOfJAMj6gKcx-6qSyufr3xLgnck8hjnH25N2Wi9UkD_6nWQT1l5WZjskiUdRxSOImsrb2TYeG1YCUzEaBIJL_RYTibGDfWBU8icTd25oy2vkuSIr42Zby0B547iqaSqNlsovd9qb47-6ZDdIIgnkprg9PEXWZ2HilzgRzntJf0KlwoeUFYsjD9kEHwUeFPhAmIJwZTo3HinJnKYz0rHYn7bH1CIhIpKO_xqrIzoWaRSs6hk7oEt__amUUgaA-QjY58atwGDWDfcAKCGTZKOqDsfzt1jxs27W4-6KbRIlDhi8Q3e1jpmS_x0kyDt4PDuAxZ3HBy-aJdIoEQ7IpFXcUmgsp3CHOiXTlwCy3lTgTDI5vCKZMIoeSYyyoMFynUWdaEF8KNb-XqHNxHb28OrfBdW9QZG0pQDlJvGK0fUd5vsol7QeLE3uWqOFEYFkcEFM1kMAXhl8xHwUuKvptZ4YuM8e_Vbh_SOw"
)
# Binance U 本位合约
BN_API_KEY = "ZlbbTVrOSfCUBtj6b6eNTCiTNjsojoxMRaDGIEZjORCRTm2dSax7sdwi9PgyMvGu"
BN_API_SECRET = "M62FUBt8Prey2Q2B0YszyB2Ms9nXb5hd5o01sF8LEablSG1qtVev7bjJXkbT0fSu"
BN_SYMBOL = "XAUTUSDT"
FAPI = "https://fapi.binance.com"
SLIPPAGE = 0.001
_SIDE = {"DEAL_TYPE_BUY": "BUY", "DEAL_TYPE_SELL": "SELL"}
CST = timezone(timedelta(hours=8))


def _log(msg):
    """带北京时间前缀打印。"""
    ts = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _patch_subscribe_rate_limit():
    """429 的 metadata 有时没有 type。SDK 直接取这个键会 KeyError，补上后再按建议时间重试。"""
    from metaapi_cloud_sdk.clients.error_handler import TooManyRequestsException
    from metaapi_cloud_sdk.clients.metaapi.subscription_manager import SubscriptionManager

    orig = SubscriptionManager.subscribe

    def subscribe(self, account_id, instance_number):
        coro = orig(self, account_id, instance_number)

        async def wrapped():
            try:
                return await coro
            except TooManyRequestsException as err:
                meta = err.metadata if isinstance(getattr(err, "metadata", None), dict) else {}
                if "type" not in meta:
                    meta = dict(meta)
                    meta["type"] = "RATE_LIMIT"
                    if not meta.get("recommendedRetryTime"):
                        retry_at = datetime.now(timezone.utc) + timedelta(seconds=5)
                        meta["recommendedRetryTime"] = retry_at.isoformat(timespec="milliseconds").replace("+00:00", "Z")
                    err.metadata = meta
                    _log(f"[{LOGIN}] 订阅被限流，将按建议时间重试: {err}")
                raise

        return wrapped()

    SubscriptionManager.subscribe = subscribe


def _bn_request(method, path, params):
    """签名请求 Binance U 本位合约接口。"""
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
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode()) from e


def _place_and_check(plan):
    """限价跟单，等待 3 秒后打印该合约最新持仓。"""
    order = _bn_request("POST", "/fapi/v1/order", {
        "symbol": plan["symbol"],
        "side": plan["side"],
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": plan["quantity"],
        "price": plan["price"],
    })
    _log("下单结果 " + json.dumps(order, ensure_ascii=False))
    time.sleep(3)
    pos = _bn_request("GET", "/fapi/v2/positionRisk", {"symbol": plan["symbol"]})
    brief = [
        {k: p.get(k) for k in ("symbol", "positionSide", "positionAmt", "entryPrice", "unRealizedProfit")}
        for p in pos
    ]
    _log("最新持仓 " + json.dumps(brief, ensure_ascii=False))


def _follow_plan(raw):
    """同向、同数量跟到 XAUTUSDT。买向上加 0.1% 滑点，卖向下减 0.1%。非买卖成交返回 None。"""
    side = _SIDE.get(raw.get("type"))
    if not side or raw.get("price") is None or not raw.get("volume"):
        return None
    px = float(raw["price"])
    order_px = px * (1 + SLIPPAGE) if side == "BUY" else px * (1 - SLIPPAGE)
    return {"symbol": BN_SYMBOL, "side": side, "quantity": float(raw["volume"]), "price": round(order_px, 2)}


def _as_dict(deal):
    """把成交转成普通字典，方便原样打印。SDK 有时给 dict，有时给对象。"""
    if isinstance(deal, dict):
        return deal
    return {k: getattr(deal, k) for k in dir(deal) if not k.startswith("_")}


def _parse_time(deal):
    """取出成交时间并统一成带时区的 UTC。解析失败返回 None，调用方会丢掉这笔。"""
    raw = _as_dict(deal).get("time") or _as_dict(deal).get("brokerTime")
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    s = str(raw).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


class DealPrintListener(SynchronizationListener):
    def __init__(self):
        """ready 为 False 时丢弃同步回放；_seen 用来按成交 id 去重。"""
        super().__init__()
        self.ready = False
        self._seen = set()

    async def on_deals_synchronized(self, instance_index: str, synchronization_id: str):
        """历史成交同步结束。多实例会重复回调，只在第一次把 ready 打开。"""
        if self.ready:
            return
        self.ready = True
        _log(f"[{LOGIN}] 成交同步完成，开始打印实时成交")

    async def on_deal_added(self, instance_index: str, deal):
        """新成交回调。同步未完成、超过 120 秒、或同一 id 已打印过的都不输出。"""
        if not self.ready:
            return
        dt = _parse_time(deal)
        if dt is None or dt < datetime.now(timezone.utc) - timedelta(seconds=120):
            return
        raw = _as_dict(deal)
        deal_id = raw.get("id")
        if deal_id is not None:
            if deal_id in self._seen:
                return
            self._seen.add(deal_id)
        _log("原始成交 " + json.dumps(raw, ensure_ascii=False, default=str))
        plan = _follow_plan(raw)
        if plan:
            _log(
                f"跟单下单 {plan['symbol']} 方向={plan['side']} 数量={plan['quantity']} 价格={plan['price']}"
            )
            try:
                await asyncio.to_thread(_place_and_check, plan)
            except Exception as e:
                _log(f"跟单失败: {e}")


async def main():
    """连接账户 10041327 的 Streaming，挂上成交监听后一直挂起，直到手动停止。"""
    _patch_subscribe_rate_limit()
    api = MetaApi(TOKEN)
    account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
    _log(f"[{LOGIN}] account={account.id} state={account.state}")
    if account.state != "DEPLOYED":
        _log(f"[{LOGIN}] 部署中…")
        await account.deploy()
    await account.wait_connected()

    stream = account.get_streaming_connection()
    listener = DealPrintListener()
    stream.add_synchronization_listener(listener)
    try:
        await stream.connect()
        await stream.wait_synchronized()
        _log(f"[{LOGIN}] Streaming 已连接，等待成交… 跟单将真实下单 {BN_SYMBOL}")
        while True:
            await asyncio.sleep(3600)
    finally:
        stream.remove_synchronization_listener(listener)
        await stream.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        _log("已停止")

