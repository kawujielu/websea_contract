# -*- coding: utf-8 -*-
"""对比 binance / okx / websea：现货与合约同 symbol 的前N档深度USDT、近M日成交额比值。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ---- 参数 ----
DEPTH_N = 20
VOL_DAYS = 7
# 为空=自动取各所「现货∩合约」USDT 对；也可手动指定
SYMBOLS: list[str] = []
EXCHANGES = ("binance", "okx", "websea")
WORKERS = 16
TIMEOUT = 12
WEBSEA_SPOT = "https://exqv.websea.work"
WEBSEA_CON = "https://coqv.websea.work"

S = requests.Session()
S.headers.update({"User-Agent": "spot-fut-ratio/1.0"})


def get(url, **params):
    r = S.get(url, params=params or None, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def depth_usdt(bids, asks, n=DEPTH_N, mult=1.0) -> float:
    total = 0.0
    for side in (bids or [])[:n], (asks or [])[:n]:
        for row in side:
            total += float(row[0]) * float(row[1]) * mult
    return total


def ratio(a: float, b: float) -> str:
    if a <= 0 or b <= 0:
        return "-"
    r = b / a
    return "1:{:.2f}".format(r) if r >= 1 else "{:.2f}:1".format(a / b)


def fmt(v: float) -> str:
    if v >= 1e8:
        return "{:.2f}亿".format(v / 1e8)
    if v >= 1e4:
        return "{:.2f}万".format(v / 1e4)
    return "{:.2f}".format(v)


def parallel(items, fn):
    rows = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futs = {pool.submit(fn, x): x for x in items}
        for fut in as_completed(futs):
            key = futs[fut]
            try:
                rows.append(fut.result())
            except Exception as e:
                print("  {} 失败: {}".format(key, e))
    rows.sort(key=lambda r: r["fut_vol"], reverse=True)
    return rows


# ---------- binance ----------
def bn_symbols():
    spot = {x["symbol"] for x in get("https://api.binance.com/api/v3/exchangeInfo")["symbols"]
            if x.get("status") == "TRADING" and x.get("quoteAsset") == "USDT"}
    fut = {x["symbol"] for x in get("https://fapi.binance.com/fapi/v1/exchangeInfo")["symbols"]
           if x.get("status") == "TRADING" and x.get("quoteAsset") == "USDT" and x.get("contractType") == "PERPETUAL"}
    raw = sorted(spot & fut)
    if SYMBOLS:
        want = {s.replace("-", "") for s in SYMBOLS}
        raw = [s for s in raw if s in want]
    return raw


def bn_one(raw: str) -> dict:
    sd = get("https://api.binance.com/api/v3/depth", symbol=raw, limit=DEPTH_N)
    fd = get("https://fapi.binance.com/fapi/v1/depth", symbol=raw, limit=DEPTH_N)
    sk = get("https://api.binance.com/api/v3/klines", symbol=raw, interval="1d", limit=VOL_DAYS)
    fk = get("https://fapi.binance.com/fapi/v1/klines", symbol=raw, interval="1d", limit=VOL_DAYS)
    return {
        "symbol": raw[:-4] + "-USDT",
        "spot_depth": depth_usdt(sd["bids"], sd["asks"]),
        "fut_depth": depth_usdt(fd["bids"], fd["asks"]),
        "spot_vol": sum(float(x[7]) for x in sk),
        "fut_vol": sum(float(x[7]) for x in fk),
    }


# ---------- okx ----------
def okx_meta():
    spot = {x["instId"] for x in get("https://www.okx.com/api/v5/public/instruments", instType="SPOT")["data"]
            if x.get("state") == "live" and x.get("quoteCcy") == "USDT"}
    ct = {x["instId"].removesuffix("-SWAP"): float(x.get("ctVal") or 0) * float(x.get("ctMult") or 1)
          for x in get("https://www.okx.com/api/v5/public/instruments", instType="SWAP")["data"]
          if x.get("state") == "live" and x.get("settleCcy") == "USDT" and x.get("ctType") == "linear"
          and str(x["instId"]).endswith("-USDT-SWAP")}
    common = sorted(spot & set(ct))
    if SYMBOLS:
        common = [s for s in SYMBOLS if s in ct]
    return common, ct


def okx_one(sym: str, ct_val: float) -> dict:
    sd = get("https://www.okx.com/api/v5/market/books", instId=sym, sz=DEPTH_N)["data"][0]
    fd = get("https://www.okx.com/api/v5/market/books", instId=sym + "-SWAP", sz=DEPTH_N)["data"][0]
    sk = get("https://www.okx.com/api/v5/market/candles", instId=sym, bar="1D", limit=VOL_DAYS)["data"]
    fk = get("https://www.okx.com/api/v5/market/candles", instId=sym + "-SWAP", bar="1D", limit=VOL_DAYS)["data"]
    return {
        "symbol": sym,
        "spot_depth": depth_usdt(sd["bids"], sd["asks"]),
        "fut_depth": depth_usdt(fd["bids"], fd["asks"], mult=ct_val),
        "spot_vol": sum(float(x[7]) for x in sk),
        "fut_vol": sum(float(x[7]) for x in fk),
    }


# ---------- websea ----------
def ws_meta():
    spot_res = get(WEBSEA_SPOT + "/openApi/market/symbols")
    spot = set()
    for x in (spot_res.get("result") or []):
        sym = x.get("symbol") if isinstance(x, dict) else x
        if isinstance(sym, str) and sym.endswith("-USDT"):
            spot.add(sym)
    face = {}
    for x in (get(WEBSEA_CON + "/qapi-v1/symbol/symbols").get("result") or []):
        if isinstance(x, dict) and str(x.get("symbol", "")).endswith("-USDT"):
            face[x["symbol"]] = float(x.get("contract_size") or x.get("faceValue") or 1)
    common = sorted(spot & set(face))
    if SYMBOLS:
        common = [s for s in SYMBOLS if s in face]
    return common, face


def ws_vol(host: str, path: str, sym: str) -> float:
    res = get(host + path, symbol=sym, period="1day", size=VOL_DAYS)
    rows = ((res.get("result") or {}).get("data") or []) if isinstance(res.get("result"), dict) else []
    return sum(float(x.get("vol") or 0) for x in rows if isinstance(x, dict))


def ws_one(sym: str, face: float) -> dict:
    sd = get(WEBSEA_SPOT + "/openApi/market/depth", symbol=sym).get("result") or {}
    fd = get(WEBSEA_CON + "/qapi-v1/market/depth", symbol=sym, limit=DEPTH_N).get("result") or {}
    return {
        "symbol": sym,
        "spot_depth": depth_usdt(sd.get("bids"), sd.get("asks")),
        "fut_depth": depth_usdt(fd.get("bids"), fd.get("asks"), mult=face),
        "spot_vol": ws_vol(WEBSEA_SPOT, "/openApi/market/kline", sym),
        "fut_vol": ws_vol(WEBSEA_CON, "/qapi-v1/market/kline", sym),
    }


def print_rows(name: str, rows: list):
    print("\n===== {} 现货 vs 合约 (depth前{}档 / 近{}日成交额) =====".format(name.upper(), DEPTH_N, VOL_DAYS))
    print("{:<14} {:>12} {:>12} {:>10} {:>12} {:>12} {:>10}".format(
        "symbol", "现货深度", "合约深度", "depth比", "现货成交额", "合约成交额", "vol比"
    ))
    for r in rows:
        print("{:<14} {:>12} {:>12} {:>10} {:>12} {:>12} {:>10}".format(
            r["symbol"],
            fmt(r["spot_depth"]), fmt(r["fut_depth"]), ratio(r["spot_depth"], r["fut_depth"]),
            fmt(r["spot_vol"]), fmt(r["fut_vol"]), ratio(r["spot_vol"], r["fut_vol"]),
        ))


def main():
    for name in EXCHANGES:
        try:
            if name == "binance":
                rows = parallel(bn_symbols(), bn_one)
            elif name == "okx":
                symbols, ct = okx_meta()
                rows = parallel(symbols, lambda s: okx_one(s, ct[s]))
            else:
                symbols, face = ws_meta()
                rows = parallel(symbols, lambda s: ws_one(s, face[s]))
            print_rows(name, rows)
        except Exception as e:
            print("\n===== {} 整体失败: {} =====".format(name.upper(), e))


if __name__ == "__main__":
    main()

