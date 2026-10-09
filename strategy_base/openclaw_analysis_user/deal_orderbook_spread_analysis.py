from __future__ import annotations

"""
Mongo 成交 vs 本地 PKL 一档价差分析。

PKL 固定列：symbol, exchange, timestamp, ts_asia, recv_ts, recv_ts_asia, bid1, ask1
时间匹配使用 timestamp 列与成交 ts（秒）对比。

PKL 文件名格式：factor_{交易对}__{binance|websea}__bid1_ask1_prices.pkl
（交易对可含连字符，如 factor_SAHARA-USDT__binance__bid1_ask1_prices.pkl）
同一合约两个文件分别对应 websea / binance；价差与「可成交」仅按 binance 一档计算，结果表分列展示两家 bid1/ask1。

部署（Ubuntu）：脚本目录 /home/ubuntu/strategy_base/strategy/ana_deal_price_diff
默认 PKL：.../ana_deal_price_diff/data

依赖: pip install pandas openpyxl pymongo
环境变量 UPM_MONGO_URI 与 daily_profit_push_v2_no_mysql.py 一致。
价差容差：DEAL_SPREAD_ABS_EPS、DEAL_SPREAD_REL_EPS（或用命令行 --spread-abs-eps / --spread-rel-eps）。
"""

import argparse
import bisect
import glob
import gzip
import io
import os
import pickle
import sys
import zlib
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from zoneinfo import ZoneInfo

BJT = ZoneInfo("Asia/Shanghai")
BUY_SELL_SIDE = {"1": "开多", "2": "开空", "3": "平多", "4": "平空"}
BUY_SIDE_RAW = frozenset({"1", "4"})
SELL_SIDE_RAW = frozenset({"2", "3"})

MONGO_URI = os.environ.get(
    "UPM_MONGO_URI",
    "mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/",
)
# 价差浮点容差：绝对 + 相对（相对成交价与盘口价中较大者），避免相同数字相减出现微小负值误判
DEFAULT_SPREAD_ABS_EPS = float(os.environ.get("DEAL_SPREAD_ABS_EPS", "1e-8"))
DEFAULT_SPREAD_REL_EPS = float(os.environ.get("DEAL_SPREAD_REL_EPS", "1e-10"))

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_PKL_DIR = "/home/ubuntu/strategy_base/strategy/ana_deal_price_diff/data"

# PKL 固定 schema（列名不可改）
PKL_COLUMNS = (
    "symbol",
    "exchange",
    "timestamp",
    "ts_asia",
    "recv_ts",
    "recv_ts_asia",
    "bid1",
    "ask1",
)

# 匹配用时间列
PKL_MATCH_TS_COL = "timestamp"


def _ts_to_bjt_ms_str(ts_sec: float) -> str:
    dt = datetime.fromtimestamp(float(ts_sec), tz=BJT)
    return dt.strftime("%Y-%m-%d %H:%M:%S") + ".{:03d}".format(int(dt.microsecond / 1000))


def _normalize_ts_value(v: Any, unit: str) -> float:
    x = float(v)
    if unit == "ms":
        return x / 1000.0
    return x


def _infer_ts_unit_from_series(s: pd.Series) -> str:
    s2 = pd.to_numeric(s, errors="coerce").dropna()
    if s2.shape[0] == 0:
        return "s"
    med = float(s2.abs().median())
    if med > 1e11:
        return "ms"
    return "s"


def _ensure_pkl_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in PKL_COLUMNS if c not in df.columns]
    if missing:
        raise SystemExit("PKL 缺少固定列 {}，当前列: {}".format(missing, list(df.columns)))
    return df


def load_pkl_paths(pkl_dir: str, pkl_glob: str) -> List[str]:
    pattern = os.path.join(pkl_dir, pkl_glob)
    paths = sorted(glob.glob(pattern))
    if not paths:
        raise SystemExit("未找到 PKL 文件: {}".format(pattern))
    return paths


# factor_SAHARA-USDT__binance__bid1_ask1_prices
PKL_FILENAME_PREFIX = "factor_"
PKL_FILENAME_SUFFIX = "__bid1_ask1_prices"


def _symbol_key(s: str) -> str:
    """用于交易对匹配：大写并去掉 - _，使 SAHARA-USDT 与 SAHARAUSDT 一致。"""
    return "".join(c for c in str(s).strip().upper() if c not in "-_")


def _sym_cell_key(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except TypeError:
        pass
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return ""
    return _symbol_key(s)


def _parse_factor_pkl_stem(stem: str) -> Optional[Tuple[str, str]]:
    """
    解析 factor_{SYMBOL}__{exchange}__bid1_ask1_prices
    返回 (symbol段原文, exchange段小写) 或 None。
    """
    if not stem.startswith(PKL_FILENAME_PREFIX):
        return None
    if not stem.endswith(PKL_FILENAME_SUFFIX):
        return None
    rest = stem[len(PKL_FILENAME_PREFIX) : -len(PKL_FILENAME_SUFFIX)]
    if "__" not in rest:
        return None
    sym_part, ex_part = rest.split("__", 1)
    if not sym_part or not ex_part:
        return None
    return sym_part, ex_part.strip().lower()


def filter_pkl_paths_by_symbols(paths: List[str], symbols: List[str]) -> List[str]:
    """仅保留 factor_{交易对}__*__bid1_ask1_prices.pkl 且交易对匹配 --symbols 的文件。"""
    if not symbols:
        return paths
    want = {_symbol_key(s) for s in symbols if str(s).strip()}
    if not want:
        return paths
    out: List[str] = []
    for p in paths:
        stem = os.path.splitext(os.path.basename(p))[0]
        parsed = _parse_factor_pkl_stem(stem)
        if parsed is not None:
            sym_part, _ex = parsed
            if _symbol_key(sym_part) in want:
                out.append(p)
            continue
        # 非标准命名：退回「整段或分段」匹配（兼容旧文件）
        if _symbol_key(stem) in want:
            out.append(p)
            continue
        for chunk in stem.replace("-", "_").split("_"):
            if chunk and _symbol_key(chunk) in want:
                out.append(p)
                break
    return out


def filter_pkl_dataframe_by_symbols(df: pd.DataFrame, symbols: List[str], symbol_upper: bool) -> pd.DataFrame:
    """按 PKL 内 symbol 列过滤；用 _symbol_key 对齐连字符写法。"""
    del symbol_upper  # 与 key 归一化等价
    if not symbols:
        return df
    raw = [s.strip() for s in symbols if s.strip()]
    if not raw:
        return df
    want = {_symbol_key(s) for s in raw}
    col = df["symbol"].map(_sym_cell_key)
    return df[col.isin(want)].copy()


def parse_symbols_arg(symbols_csv: Optional[str]) -> List[str]:
    if not symbols_csv or not str(symbols_csv).strip():
        return []
    return [x.strip() for x in str(symbols_csv).split(",") if x.strip()]


def _bytes_head_hex(data: bytes, n: int = 32) -> str:
    return data[:n].hex()


def _strip_leading_junk(data: bytes) -> bytes:
    """去掉前导 NUL/空白，修复 invalid load key '\\x00' 等由填充或损坏头导致的问题。"""
    return data.lstrip(b"\x00\r\n\t \x1a")


def _decode_pickle_blob(blob: bytes) -> Any:
    return pickle.loads(blob)


def _load_pickle_from_bytes(path: str, data: bytes, sz: int) -> Any:
    """
    依次尝试：裸 pickle、去 NUL 后 pickle、gzip 包装、zlib 包装、pandas.read_pickle、joblib。
    """
    s = _strip_leading_junk(data)
    if not s:
        raise ValueError("文件去前导空字节后长度为 0")

    attempts: List[str] = []

    # 1) 原始 / 去 junk 后 pickle
    for label, blob in (("raw", data), ("stripped", s)):
        try:
            return _decode_pickle_blob(blob)
        except Exception as e:
            attempts.append("pickle.loads({}): {}".format(label, e))

    # 2) gzip（标准头 1f 8b）
    for label, blob in (("stripped", s), ("raw", data)):
        if len(blob) < 2 or blob[:2] != b"\x1f\x8b":
            continue
        try:
            dec = gzip.decompress(blob)
            return _decode_pickle_blob(dec)
        except Exception as e:
            attempts.append("gzip({}): {}".format(label, e))

    # 3) zlib（部分写入脚本会用 zlib 再 pickle）
    try:
        dec = zlib.decompress(s)
        return _decode_pickle_blob(dec)
    except Exception as e:
        attempts.append("zlib.decompress: {}".format(e))

    # 4) pandas 自带读 pkl（部分版本对存储格式更宽松）
    try:
        return pd.read_pickle(io.BytesIO(s))
    except Exception as e:
        attempts.append("pd.read_pickle(BytesIO): {}".format(e))

    # 5) joblib（若生成端用的是 joblib.dump）
    try:
        import joblib  # type: ignore[import-untyped]

        return joblib.load(path)
    except ImportError:
        attempts.append("joblib: 未安装")
    except Exception as e:
        attempts.append("joblib.load: {}".format(e))

    hint = (
        "文件头 hex(前32字节)={!r}，大小={} bytes。"
        "若仍失败，请在服务器执行: xxd {} | head 或 file {}，确认是否为裸 pickle / gzip / 其它格式。".format(
            _bytes_head_hex(data),
            sz,
            path,
            path,
        )
    )
    raise ValueError("{} 已尝试: {}".format(hint, " | ".join(attempts)))


def _load_pickle_object(path: str, skip_bad: bool) -> Optional[Any]:
    """
    读取单个 pickle 文件。失败时 skip_bad 则打印告警并返回 None，否则 SystemExit。
    EOFError 常见于：空文件、写入未完成被截断、非 pickle 内容。
    """
    try:
        sz = os.path.getsize(path)
    except OSError as e:
        if skip_bad:
            print("[WARN] 无法 stat PKL，已跳过: {} ({})".format(path, e), file=sys.stderr)
            return None
        raise SystemExit("无法访问 PKL: {} ({})".format(path, e))
    if sz == 0:
        msg = "PKL 为空文件(0字节): {}".format(path)
        if skip_bad:
            print("[WARN] {}，已跳过".format(msg), file=sys.stderr)
            return None
        raise SystemExit(
            "{}。请删除占位文件或收紧 --pkl-glob；若需忽略可加 --skip-bad-pkl。".format(msg)
        )
    try:
        with open(path, "rb") as f:
            data = f.read()
        return _load_pickle_from_bytes(path, data, sz)
    except Exception as e:
        if skip_bad:
            print("[WARN] PKL 读取失败，已跳过: {} ({})".format(path, e), file=sys.stderr)
            return None
        raise SystemExit("PKL 读取失败: {}\n{}".format(path, e))


def load_pkl_merged(paths: List[str], skip_bad: bool = False) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
    frames_or_lists: List[Any] = []
    used_paths: List[str] = []
    for p in paths:
        obj = _load_pickle_object(p, skip_bad=skip_bad)
        if obj is None:
            continue
        frames_or_lists.append(obj)
        used_paths.append(p)
    if not frames_or_lists:
        raise SystemExit(
            "无有效 PKL：共匹配 {} 个路径，全部为空/损坏或被跳过。请检查 data 目录或加 --skip-bad-pkl 确认是否有可读文件。".format(
                len(paths)
            )
        )
    first = frames_or_lists[0]
    if isinstance(first, pd.DataFrame):
        dfs = []
        for i, o in enumerate(frames_or_lists):
            if not isinstance(o, pd.DataFrame):
                raise SystemExit("PKL 类型不一致: 文件 {} 非 DataFrame".format(used_paths[i]))
            dfs.append(_ensure_pkl_dataframe(o))
        return pd.concat(dfs, ignore_index=True)
    if isinstance(first, list):
        out: List[Dict[str, Any]] = []
        for i, o in enumerate(frames_or_lists):
            if not isinstance(o, list):
                raise SystemExit("PKL 类型不一致: 文件 {} 非 list".format(used_paths[i]))
            for row in o:
                if not isinstance(row, dict):
                    raise SystemExit("list PKL 元素须为 dict，得到 {}".format(type(row)))
                miss = [c for c in PKL_COLUMNS if c not in row]
                if miss:
                    raise SystemExit("PKL dict 缺少键: {}".format(miss))
                out.append(row)
        return out
    raise SystemExit("不支持的 PKL 根类型: {}".format(type(first)))


def inspect_pkl(paths: List[str]) -> None:
    print("[inspect] 文件数: {}".format(len(paths)))
    for p in paths:
        print("[inspect] --- {} ---".format(p))
        stem = os.path.splitext(os.path.basename(p))[0]
        parsed = _parse_factor_pkl_stem(stem)
        if parsed:
            print("  factor_pkl: symbol段={!r} exchange={!r}".format(parsed[0], parsed[1]))
        sz = os.path.getsize(p) if os.path.isfile(p) else -1
        print("  size_bytes: {}".format(sz))
        if sz == 0:
            print("  ERROR: 空文件，无法 pickle.load")
            continue
        try:
            with open(p, "rb") as f:
                raw = f.read()
            obj = _load_pickle_from_bytes(p, raw, sz)
        except Exception as e:
            print("  ERROR: {}".format(e))
            continue
        print("  type: {}".format(type(obj)))
        if isinstance(obj, pd.DataFrame):
            print("  shape: {}".format(obj.shape))
            print("  columns: {}".format(list(obj.columns)))
            print(obj.head(3).to_string())
        elif isinstance(obj, list):
            print("  len: {}".format(len(obj)))
            if obj:
                print("  first keys: {}".format(list(obj[0].keys()) if isinstance(obj[0], dict) else type(obj[0])))
                for j, row in enumerate(obj[:3]):
                    print("  [{}] {}".format(j, row))
        else:
            print("  repr: {!r}".format(obj)[:500])


# 每条盘口：(ts_sec, bid1, ask1, exchange, ts_asia 原样)
BookRow = Tuple[float, float, float, str, Any]


def df_to_book_rows_for_exchange(
    df: pd.DataFrame,
    ts_unit: str,
    symbol_upper: bool,
    exchange_canonical: str,
) -> List[Tuple[str, BookRow]]:
    """按 PKL 列 exchange（大小写不敏感）筛出单所盘口；无该所则返回空列表。"""
    exu = exchange_canonical.strip().upper()
    dfx = df[df["exchange"].astype(str).str.strip().str.upper() == exu].copy()
    if dfx.shape[0] == 0:
        return []

    unit = ts_unit
    if unit == "auto":
        unit = _infer_ts_unit_from_series(dfx[PKL_MATCH_TS_COL])

    rows: List[Tuple[str, BookRow]] = []
    for _, r in dfx.iterrows():
        sym = r["symbol"]
        if sym is None or (isinstance(sym, float) and pd.isna(sym)):
            continue
        sk = str(sym).strip()
        if symbol_upper:
            sk = sk.upper()
        ts_sec = _normalize_ts_value(r[PKL_MATCH_TS_COL], unit)
        bid1 = float(r["bid1"])
        ask1 = float(r["ask1"])
        ex = str(r["exchange"]).strip()
        ts_asia = r["ts_asia"]
        rows.append((sk, (ts_sec, bid1, ask1, ex, ts_asia)))
    return rows


def build_symbol_book(
    book_rows: List[Tuple[str, BookRow]]
) -> Dict[str, Tuple[List[float], List[BookRow]]]:
    by_sym: Dict[str, List[BookRow]] = {}
    for sk, tup in book_rows:
        by_sym.setdefault(sk, []).append(tup)
    out: Dict[str, Tuple[List[float], List[BookRow]]] = {}
    for sk, arr in by_sym.items():
        arr.sort(key=lambda x: x[0])
        compact_ts: List[float] = []
        compact_rows: List[BookRow] = []
        for tup in arr:
            t = tup[0]
            if compact_ts and compact_ts[-1] == t:
                compact_rows[-1] = tup
            else:
                compact_ts.append(t)
                compact_rows.append(tup)
        out[sk] = (compact_ts, compact_rows)
    return out


def match_book(
    symbol_book: Dict[str, Tuple[List[float], List[BookRow]]],
    symbol: str,
    deal_ts: float,
) -> Optional[BookRow]:
    if symbol not in symbol_book:
        return None
    ts_list, rows = symbol_book[symbol]
    if not ts_list:
        return None
    i = bisect.bisect_left(ts_list, deal_ts) - 1
    if i < 0:
        return None
    return rows[i]


def side_to_bucket(raw: str) -> str:
    s = str(raw)
    if s in BUY_SIDE_RAW:
        return "buy"
    if s in SELL_SIDE_RAW:
        return "sell"
    return "unknown"


def _normalized_spread_executable(
    raw_spread: float,
    trade_px: float,
    book_px: float,
    abs_eps: float,
    rel_eps: float,
) -> Tuple[float, bool]:
    """
    若 |raw_spread| <= max(abs_eps, rel_eps*scale) 则视为 0，消除浮点噪声；
    可成交仍为 spread>0（抹零后严格大于 0）。
    """
    scale = max(abs(float(trade_px)), abs(float(book_px)), 1e-12)
    tol = max(float(abs_eps), float(rel_eps) * scale)
    if abs(raw_spread) <= tol:
        spread = 0.0
    else:
        spread = float(raw_spread)
    return spread, spread > 0


def fetch_deals_sync(
    uri: str,
    user_id: str,
    ts_min: int,
    ts_max: int,
    symbols: Optional[List[str]] = None,
    symbol_upper: bool = False,
) -> List[Dict[str, Any]]:
    client = MongoClient(uri)
    try:
        col = client.exchange.real_contract_deal
        q: Dict[str, Any] = {
            "ts": {"$gte": int(ts_min), "$lte": int(ts_max)},
            "$or": [{"takerUser": user_id}, {"makerUser": user_id}],
        }
        if symbols:
            sym_list = [s.strip() for s in symbols if s and str(s).strip()]
            if sym_list:
                if symbol_upper:
                    sym_list = [s.upper() for s in sym_list]
                q["symbol"] = {"$in": sym_list}
        projection = {
            "_id": 0,
            "ts": 1,
            "symbol": 1,
            "price": 1,
            "takerUser": 1,
            "makerUser": 1,
            "takerBuyOrSell": 1,
            "makerBuyOrSell": 1,
            "takerIsProtected": 1,
            "makerIsProtected": 1,
            "takerSubId": 1,
            "makerSubId": 1,
        }
        try:
            cur = col.find(q, projection=projection).sort("ts", 1).batch_size(2000)
            try:
                cur = cur.allow_disk_use(True)
            except Exception:
                pass
            return list(cur)
        except OperationFailure:
            rows = list(col.find(q, projection=projection).batch_size(2000))
            rows.sort(key=lambda x: int(x.get("ts") or 0))
            return rows
    finally:
        client.close()


def deals_to_user_rows(deals: List[Dict[str, Any]], user_id: str, symbol_upper: bool) -> List[Dict[str, Any]]:
    uid = str(user_id)
    out: List[Dict[str, Any]] = []
    for d in deals:
        taker = str(d.get("takerUser", ""))
        maker = str(d.get("makerUser", ""))
        ts_i = int(d["ts"])
        sym = d.get("symbol")
        sk = str(sym).strip() if sym is not None else ""
        if symbol_upper:
            sk = sk.upper()
        price = float(d["price"])
        if taker == uid:
            raw = d.get("takerBuyOrSell")
            role = "taker"
        elif maker == uid:
            raw = d.get("makerBuyOrSell")
            role = "maker"
        else:
            continue
        out.append(
            {
                "deal_ts": ts_i,
                "symbol": sk,
                "trade_price": price,
                "side_raw": str(raw),
                "user_role": role,
            }
        )
    return out


def build_result_dataframe(
    user_rows: List[Dict[str, Any]],
    symbol_book_binance: Dict[str, Tuple[List[float], List[BookRow]]],
    symbol_book_websea: Dict[str, Tuple[List[float], List[BookRow]]],
    spread_abs_eps: float = DEFAULT_SPREAD_ABS_EPS,
    spread_rel_eps: float = DEFAULT_SPREAD_REL_EPS,
) -> pd.DataFrame:
    """
    分别用 binance / websea 时间序列做「timestamp < 成交时间」最近一档匹配。
    价差、可成交、match_ok 仅依据 binance 一档；websea 列仅展示。
    """
    recs: List[Dict[str, Any]] = []
    for r in user_rows:
        deal_ts = float(r["deal_ts"])
        sym = r["symbol"]
        side_raw = r["side_raw"]
        label = BUY_SELL_SIDE.get(side_raw, "未知({})".format(side_raw))
        bucket = side_to_bucket(side_raw)
        direction = "{}/{}".format(label, bucket) if bucket != "unknown" else label
        deal_time = _ts_to_bjt_ms_str(deal_ts)

        mb = match_book(symbol_book_binance, sym, deal_ts)
        mw = match_book(symbol_book_websea, sym, deal_ts)

        base: Dict[str, Any] = {
            "symbol": sym,
            "成交方向": direction,
            "成交价格": r["trade_price"],
            "成交时间": deal_time,
            "websea_bid1": None,
            "websea_ask1": None,
            "websea_ts_asia": None,
            "binance_bid1": None,
            "binance_ask1": None,
            "binance_ts_asia": None,
            "deal_ts": int(deal_ts),
            "盘口timestamp_websea": None,
            "盘口时间_websea_北京毫秒": None,
            "lag_sec_websea": None,
            "盘口timestamp_binance": None,
            "盘口时间_binance_北京毫秒": None,
            "lag_sec_binance": None,
            "价差": None,
            "可成交": None,
            "match_ok": False,
            "user_role": r["user_role"],
            "side_raw": side_raw,
        }

        if mw is not None:
            w_ts, w_bid, w_ask, _w_ex, w_ts_asia = mw
            base.update(
                {
                    "websea_bid1": w_bid,
                    "websea_ask1": w_ask,
                    "websea_ts_asia": w_ts_asia,
                    "盘口timestamp_websea": float(w_ts),
                    "盘口时间_websea_北京毫秒": _ts_to_bjt_ms_str(w_ts),
                    "lag_sec_websea": float(deal_ts - w_ts),
                }
            )

        if mb is None:
            recs.append(base)
            continue

        b_ts, b_bid, b_ask, _b_ex, b_ts_asia = mb
        spread: Optional[float] = None
        executable: Optional[bool] = None
        tp = float(r["trade_price"])
        if bucket == "buy":
            raw = tp - float(b_ask)
            spread, executable = _normalized_spread_executable(
                raw, tp, float(b_ask), spread_abs_eps, spread_rel_eps
            )
        elif bucket == "sell":
            raw = float(b_bid) - tp
            spread, executable = _normalized_spread_executable(
                raw, tp, float(b_bid), spread_abs_eps, spread_rel_eps
            )

        base.update(
            {
                "binance_bid1": b_bid,
                "binance_ask1": b_ask,
                "binance_ts_asia": b_ts_asia,
                "盘口timestamp_binance": float(b_ts),
                "盘口时间_binance_北京毫秒": _ts_to_bjt_ms_str(b_ts),
                "lag_sec_binance": float(deal_ts - b_ts),
                "价差": spread,
                "可成交": executable,
                "match_ok": True,
            }
        )
        recs.append(base)

    col_order = [
        "symbol",
        "成交方向",
        "成交价格",
        "成交时间",
        "websea_bid1",
        "websea_ask1",
        "websea_ts_asia",
        "binance_bid1",
        "binance_ask1",
        "binance_ts_asia",
        "deal_ts",
        "盘口timestamp_websea",
        "盘口时间_websea_北京毫秒",
        "lag_sec_websea",
        "盘口timestamp_binance",
        "盘口时间_binance_北京毫秒",
        "lag_sec_binance",
        "价差",
        "可成交",
        "match_ok",
        "user_role",
        "side_raw",
    ]
    df = pd.DataFrame(recs)
    if df.shape[0] == 0:
        return pd.DataFrame(columns=col_order)
    return df[col_order]


def print_summary(df: pd.DataFrame) -> None:
    n = len(df)
    if n == 0:
        print("[汇总] 无数据行")
        return
    ok = int(df["match_ok"].sum()) if "match_ok" in df.columns else 0
    bad = n - ok
    ex = df["可成交"]
    ex_true = int((ex == True).sum())  # noqa: E712
    ex_false = int((ex == False).sum())  # noqa: E712
    ex_na = int(ex.isna().sum())
    print(
        "[汇总] 总条数={} 匹配盘口={} 未匹配={} 可成交=True/False/NA={}/{}/{}".format(
            n, ok, bad, ex_true, ex_false, ex_na
        )
    )
    valid = ex.notna()
    n_valid = int(valid.sum())
    if n_valid > 0:
        false_in_valid = int((ex[valid] == False).sum())  # noqa: E712
        pct = 100.0 * false_in_valid / float(n_valid)
        extra = ""
        if false_in_valid > 0:
            false_mask = df["可成交"] == False  # noqa: E712
            sub = df.loc[false_mask, ["价差", "成交价格"]].dropna()
            tp = pd.to_numeric(sub["成交价格"], errors="coerce")
            sp = pd.to_numeric(sub["价差"], errors="coerce")
            ok_row = tp.notna() & sp.notna() & (tp.abs() > 1e-12)
            n_ratio = int(ok_row.sum())
            if n_ratio > 0:
                rel_pct = (sp[ok_row] / tp[ok_row] * 100.0).astype(float)
                mean_rel = float(rel_pct.mean())
                extra = " 可成交=False 的子样本中，价差相对成交价平均偏差={:.4f}%（{} 笔参与，为每笔 价差/成交价格×100% 再平均）。".format(
                    mean_rel, n_ratio
                )
            else:
                extra = " 可成交=False 但无法算相对偏差（价差缺失或成交价为 0）。"
        msg = "[结论] 在已计算价差、可成交字段的 {} 笔中，可成交=False 占比 {:.2f}%（{} 笔）。{}".format(
            n_valid, pct, false_in_valid, extra
        )
        print(msg)
    else:
        print("[结论] 无可成交判定数据（价差均为空），无法统计 False 占比")


def print_table(df: pd.DataFrame, mode: str, max_rows: int) -> None:
    if df.shape[0] == 0:
        print("(无表格行)")
        return
    if mode == "summary":
        return
    display_cols = [
        "symbol",
        "成交方向",
        "成交价格",
        "成交时间",
        "websea_bid1",
        "websea_ask1",
        "websea_ts_asia",
        "binance_bid1",
        "binance_ask1",
        "binance_ts_asia",
        "价差",
        "可成交",
        "match_ok",
    ]
    sub = df[[c for c in display_cols if c in df.columns]]
    if mode == "all":
        print(sub.to_string(index=False))
        return
    mr = max_rows if max_rows > 0 else 50
    print(sub.head(mr).to_string(index=False))
    if len(sub) > mr:
        print("... 仅显示前 {} 行，共 {} 行（--print-mode all 查看全部）".format(mr, len(sub)))


def parse_day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def bjt_day_ts_range(d: date) -> Tuple[int, int]:
    start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=BJT)
    end = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=BJT)
    return int(start.timestamp()), int(end.timestamp())


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Mongo 成交 vs PKL 一档（固定列）；匹配列 timestamp。Python 3.9+"
    )
    p.add_argument("--user-id", required=True, help="用户 ID（与 Mongo 中 takerUser/makerUser 一致）")
    p.add_argument(
        "--symbols",
        default=None,
        help="仅分析这些交易对，逗号分隔；与 Mongo symbol、PKL 列 symbol 用去连字符规则对齐。"
        "PKL 文件名须为 factor_{交易对}__binance|websea__bid1_ask1_prices.pkl，按交易对筛选要加载的文件",
    )
    p.add_argument(
        "--pkl-ignore-filename-filter",
        action="store_true",
        help="与 --symbols 联用：不按文件名筛 PKL，仍用 glob 全部加载，仅用 PKL 内 symbol 列过滤（适合单文件多交易对）",
    )
    p.add_argument("--ts-min", type=int, default=None, help="成交 ts 下限（秒）")
    p.add_argument("--ts-max", type=int, default=None, help="成交 ts 上限（秒）")
    p.add_argument("--start-date", default=None, help="BJT 日期 YYYY-MM-DD（与 end-date 合用）")
    p.add_argument("--end-date", default=None, help="BJT 日期 YYYY-MM-DD")

    p.add_argument(
        "--pkl-dir",
        default=DEFAULT_PKL_DIR,
        help="PKL 目录（默认 Ubuntu: .../ana_deal_price_diff/data）",
    )
    p.add_argument(
        "--pkl-glob",
        default="*.pkl",
        help="PKL glob，建议 factor_*__*__bid1_ask1_prices.pkl 或默认 *.pkl",
    )
    p.add_argument(
        "--skip-bad-pkl",
        action="store_true",
        help="跳过空文件或 EOF/反序列化失败的 PKL，合并其余文件",
    )
    p.add_argument(
        "--ts-unit",
        choices=("auto", "s", "ms"),
        default="auto",
        help="PKL 列 timestamp 的单位；auto 根据数值推断",
    )
    p.add_argument("--symbol-upper", action="store_true", help="合约名转大写再匹配")
    p.add_argument("--inspect-pkl", action="store_true", help="仅检查 PKL 结构后退出")

    p.add_argument("--output-xlsx", default=None, help="输出 xlsx；默认写到脚本目录下带时间戳文件名")
    p.add_argument("--output-csv", default=None, help="可选 CSV 路径")
    p.add_argument(
        "--print-mode",
        choices=("head", "all", "summary"),
        default="head",
        help="终端展示：head 截断 / all 全量 / summary 仅汇总",
    )
    p.add_argument("--print-max-rows", type=int, default=50, help="print-mode=head 时最大行数")
    p.add_argument(
        "--spread-abs-eps",
        type=float,
        default=DEFAULT_SPREAD_ABS_EPS,
        help="价差浮点绝对容差，|价差|<=max(本值,rel*scale) 视为0；默认 1e-8 或 DEAL_SPREAD_ABS_EPS",
    )
    p.add_argument(
        "--spread-rel-eps",
        type=float,
        default=DEFAULT_SPREAD_REL_EPS,
        help="价差相对容差系数，与成交价、盘口价较大者相乘；默认 1e-10 或 DEAL_SPREAD_REL_EPS",
    )

    args = p.parse_args(argv)
    if args.start_date or args.end_date:
        if not (args.start_date and args.end_date):
            p.error("start-date 与 end-date 需同时指定")
        args._ts_min_auto = bjt_day_ts_range(parse_day(args.start_date))[0]
        args._ts_max_auto = bjt_day_ts_range(parse_day(args.end_date))[1]
    else:
        args._ts_min_auto = None
        args._ts_max_auto = None
    args.symbols_list = parse_symbols_arg(args.symbols)
    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    paths = load_pkl_paths(args.pkl_dir, args.pkl_glob)
    sym_list = args.symbols_list

    if sym_list and (not args.pkl_ignore_filename_filter):
        paths = filter_pkl_paths_by_symbols(paths, sym_list)
        if not paths:
            raise SystemExit(
                "按 --symbols={} 过滤后没有匹配的 PKL 文件。"
                "标准命名 factor_交易对__binance|websea__bid1_ask1_prices.pkl；或放宽 --pkl-glob / --pkl-ignore-filename-filter。".format(
                    ",".join(sym_list)
                )
            )
        print("[INFO] 交易对过滤后待加载 PKL 文件数: {}".format(len(paths)), file=sys.stderr)
    elif sym_list and args.pkl_ignore_filename_filter:
        print("[INFO] 已跳过文件名过滤，按 glob 加载 {} 个 PKL，再用 symbol 列过滤".format(len(paths)), file=sys.stderr)

    if args.inspect_pkl:
        inspect_pkl(paths)
        return

    ts_min = args.ts_min if args.ts_min is not None else args._ts_min_auto
    ts_max = args.ts_max if args.ts_max is not None else args._ts_max_auto
    if ts_min is None or ts_max is None:
        print("请指定 --ts-min/--ts-max 或 --start-date/--end-date（BJT）", file=sys.stderr)
        sys.exit(2)

    merged = load_pkl_merged(paths, skip_bad=args.skip_bad_pkl)
    if isinstance(merged, pd.DataFrame):
        if merged.shape[0] == 0:
            raise SystemExit("PKL DataFrame 为空")
        df_pkl = merged
    else:
        if not merged:
            raise SystemExit("PKL list 为空")
        df_pkl = pd.DataFrame(merged)

    if sym_list:
        df_pkl = filter_pkl_dataframe_by_symbols(df_pkl, sym_list, args.symbol_upper)
        if df_pkl.shape[0] == 0:
            raise SystemExit(
                "PKL 内按 symbol 过滤后无数据，交易对={}。请检查 PKL 列 symbol 与 --symbols 是否一致。".format(
                    sym_list
                )
            )

    book_rows_bn = df_to_book_rows_for_exchange(df_pkl, args.ts_unit, args.symbol_upper, "BINANCE")
    book_rows_ws = df_to_book_rows_for_exchange(df_pkl, args.ts_unit, args.symbol_upper, "WEBSEA")
    if not book_rows_bn:
        print(
            "[WARN] PKL 中无 exchange=binance（大小写不敏感）数据行，价差/可成交/match_ok 将无法计算",
            file=sys.stderr,
        )
    if not book_rows_ws:
        print("[WARN] PKL 中无 exchange=websea 数据行，websea_* 列将为空", file=sys.stderr)
    symbol_book_bn = build_symbol_book(book_rows_bn)
    symbol_book_ws = build_symbol_book(book_rows_ws)

    deals = fetch_deals_sync(
        MONGO_URI,
        args.user_id,
        ts_min,
        ts_max,
        symbols=sym_list if sym_list else None,
        symbol_upper=args.symbol_upper,
    )
    user_rows = deals_to_user_rows(deals, args.user_id, args.symbol_upper)
    df = build_result_dataframe(
        user_rows,
        symbol_book_bn,
        symbol_book_ws,
        spread_abs_eps=args.spread_abs_eps,
        spread_rel_eps=args.spread_rel_eps,
    )

    print_summary(df)
    print_table(df, args.print_mode, args.print_max_rows)

    out_xlsx = args.output_xlsx
    if not out_xlsx:
        out_xlsx = os.path.join(
            _SCRIPT_DIR,
            "deal_orderbook_spread_{}_{}.xlsx".format(args.user_id, datetime.now(BJT).strftime("%Y%m%d_%H%M%S")),
        )
    df.to_excel(out_xlsx, index=False, engine="openpyxl")
    print("[写入] {}".format(out_xlsx))

    if args.output_csv:
        df.to_csv(args.output_csv, index=False, encoding="utf-8-sig")
        print("[写入] {}".format(args.output_csv))


if __name__ == "__main__":
    main()
