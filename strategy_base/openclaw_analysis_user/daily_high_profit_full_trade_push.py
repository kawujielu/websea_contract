from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List
from zoneinfo import ZoneInfo

import pandas as pd

from mongdb_order_stats import calc_trade_stats
from exclude_user_ids import get_excluded_user_ids_async

BJT = ZoneInfo("Asia/Shanghai")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_DIR = os.environ.get(
    "UPM_PKL_DEALS_DIR",
    os.path.join(SCRIPT_DIR, "data", "mongo_daily_deals_pkl"),
)

TG_CHAT_ID = os.environ.get("UPM_TG_CHAT_ID", "-5223829567")
PROFIT_THRESHOLD = float(os.environ.get("UPM_PROFIT_THRESHOLD", "500"))
WIN_RATE_THRESHOLD = float(os.environ.get("UPM_WIN_RATE_THRESHOLD", "0.6"))
PL_RATIO_THRESHOLD = float(os.environ.get("UPM_PL_RATIO_THRESHOLD", "2"))
TRADE_COUNT_THRESHOLD = int(os.environ.get("UPM_TRADE_COUNT_THRESHOLD", "30"))
SEND_EMPTY_RESULT = os.environ.get("UPM_SEND_EMPTY_RESULT", "1") == "1"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="筛选昨日高盈利用户，并基于全量交易统计后推送TG"
    )
    p.add_argument(
        "--data-dir",
        default=DEFAULT_DATA_DIR,
        help="日切pkl目录，文件名格式 YYYY-MM-DD.pkl",
    )
    p.add_argument(
        "--day",
        default="",
        help="统计日期 YYYY-MM-DD（默认昨天，北京时间）",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="仅打印不发送TG",
    )
    args, _ = p.parse_known_args()
    return args


def _parse_day(day_str: str) -> date:
    return datetime.strptime(day_str, "%Y-%m-%d").date()


def _pick_day(day_arg: str) -> date:
    if day_arg:
        return _parse_day(day_arg)
    return datetime.now(BJT).date() - timedelta(days=1)


def _pkl_path(data_dir: str, d: date) -> str:
    return os.path.join(data_dir, "{}.pkl".format(d.strftime("%Y-%m-%d")))


def _list_pkl_paths(data_dir: str) -> List[str]:
    if not os.path.isdir(data_dir):
        raise FileNotFoundError("PKL目录不存在: {}".format(data_dir))
    out = []
    for fn in sorted(os.listdir(data_dir)):
        if not fn.endswith(".pkl"):
            continue
        stem = fn[:-4]
        try:
            datetime.strptime(stem, "%Y-%m-%d")
        except ValueError:
            continue
        out.append(os.path.join(data_dir, fn))
    return out


def _ensure_user_id(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "user_id" in d.columns:
        d["user_id"] = d["user_id"].astype(str)
        return d

    required = ["ts_text", "symbol", "price", "amount", "taker_user", "maker_user"]
    for c in required:
        if c not in d.columns:
            raise ValueError("pkl缺少字段 {}，无法恢复user_id".format(c))

    d = d.reset_index(drop=True)
    d["_idx_in_pair"] = d.groupby(required).cumcount()
    d["user_id"] = d["taker_user"].astype(str)
    mask = (d["_idx_in_pair"] % 2) == 1
    d.loc[mask, "user_id"] = d.loc[mask, "maker_user"].astype(str)
    d = d.drop(columns=["_idx_in_pair"])
    return d


def _pkl_legs_to_calc_frame(d: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "userid",
        "tsText",
        "symbol",
        "buy_sell",
        "profit_loss",
        "price",
        "amount",
        "amt",
        "multiple",
        "fee",
        "taker_num",
        "maker_num",
        "taker_amt",
        "maker_amt",
        "deal_is_protected",
        "deal_sub_id",
    ]
    if d.shape[0] == 0:
        return pd.DataFrame(columns=cols)

    x = d.copy()
    x["user_id"] = x["user_id"].astype(str)
    tk = x["taker_user"].astype(str)
    mk = x["maker_user"].astype(str)
    fv = pd.to_numeric(x["face_value"], errors="coerce").fillna(1.0)
    base_amt = pd.to_numeric(x["amount"], errors="coerce").fillna(0.0)
    pr = pd.to_numeric(x["price"], errors="coerce").fillna(0.0)
    qty = base_amt * fv
    amt_notional = pr * base_amt * fv
    is_taker = x["user_id"] == tk
    is_maker = x["user_id"] == mk

    taker_amt = is_taker.astype(float) * amt_notional
    maker_amt = is_maker.astype(float) * amt_notional

    if "deal_is_protected" in x.columns and "deal_sub_id" in x.columns:
        dip = pd.to_numeric(x["deal_is_protected"], errors="coerce").fillna(0).astype(int)
        dsid = pd.to_numeric(x["deal_sub_id"], errors="coerce").fillna(0).astype(int)
    else:
        dip = pd.Series(0, index=x.index, dtype=int)
        dsid = pd.Series(0, index=x.index, dtype=int)

    out = pd.DataFrame(
        {
            "userid": x["user_id"],
            "tsText": x["ts_text"],
            "symbol": x["symbol"],
            "buy_sell": x["buy_sell"],
            "profit_loss": pd.to_numeric(x["profit_loss"], errors="coerce").fillna(0.0),
            "price": pr,
            "amount": qty,
            "amt": amt_notional,
            "multiple": pd.to_numeric(x["multiple"], errors="coerce").fillna(0).astype(int),
            "fee": pd.to_numeric(x["fee"], errors="coerce").fillna(0.0),
            "taker_num": is_taker.astype(int),
            "maker_num": is_maker.astype(int),
            "taker_amt": taker_amt,
            "maker_amt": maker_amt,
            "deal_is_protected": dip,
            "deal_sub_id": dsid,
        }
    )
    return out.sort_values("tsText", kind="mergesort").reset_index(drop=True)


def _parse_percent(s) -> float:
    t = str(s or "").strip()
    if t.endswith("%"):
        try:
            return float(t[:-1]) / 100.0
        except ValueError:
            return 0.0
    try:
        v = float(t)
    except ValueError:
        return 0.0
    return v / 100.0 if v > 1 else v


def _parse_float(v) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _realized_by_user(day_df: pd.DataFrame, excluded_ids: set[str]) -> Dict[str, float]:
    d = _ensure_user_id(day_df)
    if excluded_ids:
        d = d[~d["user_id"].astype(str).isin(excluded_ids)].copy()
    pnl = pd.to_numeric(d["profit_loss"], errors="coerce").fillna(0.0)
    d = d.assign(_pnl=pnl)
    g = d.groupby(d["user_id"].astype(str))["_pnl"].sum()
    return {str(uid): float(v) for uid, v in g.items()}


def _build_candidates(realized: Dict[str, float]) -> List[str]:
    return sorted([uid for uid, pnl in realized.items() if pnl > PROFIT_THRESHOLD])


def _send_text_chunks(text: str, max_len: int = 3900) -> List[str]:
    if not text:
        return ["(空结果)"]
    chunks: List[str] = []
    cur: List[str] = []
    cur_len = 0
    for line in text.splitlines(True):
        if cur and cur_len + len(line) > max_len:
            chunks.append("".join(cur))
            cur = [line]
            cur_len = len(line)
        else:
            cur.append(line)
            cur_len += len(line)
    if cur:
        chunks.append("".join(cur))
    return chunks


async def _send_tg(text: str, dry_run: bool):
    if dry_run:
        print(text)
        return
    if not TG_CHAT_ID:
        print("[WARN] 未配置 UPM_TG_CHAT_ID，跳过发送")
        return

    from ToolBoxNew import ToolBox

    tb = ToolBox()
    for chunk in _send_text_chunks(text):
        await tb.send_tg(TG_CHAT_ID, chunk)


def _build_report_lines(
    stat_day: date,
    candidates: Iterable[str],
    realized: Dict[str, float],
    full_df: pd.DataFrame,
) -> List[str]:
    passed_lines: List[str] = []
    for uid in candidates:
        user_df = full_df[full_df["userid"].astype(str) == str(uid)].copy()
        if user_df.shape[0] == 0:
            continue
        stats = calc_trade_stats(user_df, userid=str(uid))
        win_rate = _parse_percent(stats.get("胜率", "0%"))
        pl_ratio = _parse_float(stats.get("盈亏比", 0.0))
        trade_count = int(_parse_float(stats.get("总交易轮次(开平算一次)", 0)))
        if (
            win_rate > WIN_RATE_THRESHOLD
            and pl_ratio > PL_RATIO_THRESHOLD
            and trade_count > TRADE_COUNT_THRESHOLD
        ):
            passed_lines.append(
                "uid={uid} 昨日盈亏={pnl:.4f} 胜率={win:.2%} 盈亏比={pl:.4f} 总交易轮次={cnt}".format(
                    uid=uid,
                    pnl=realized.get(uid, 0.0),
                    win=win_rate,
                    pl=pl_ratio,
                    cnt=trade_count,
                )
            )

    title = "[高盈利用户筛选] 日期={} 昨日盈利>{} 候选={} 命中={}".format(
        stat_day.strftime("%Y-%m-%d"),
        PROFIT_THRESHOLD,
        len(list(candidates)),
        len(passed_lines),
    )
    if not passed_lines:
        return [title, "无命中用户"]
    return [title] + passed_lines


def _load_concat_pkl(paths: List[str]) -> pd.DataFrame:
    if not paths:
        return pd.DataFrame()
    dfs = [pd.read_pickle(p) for p in paths]
    return pd.concat(dfs, ignore_index=True)


async def run_once(args: argparse.Namespace) -> int:
    stat_day = _pick_day(args.day)
    excluded_ids = {str(u) for u in await get_excluded_user_ids_async()}
    day_fp = _pkl_path(args.data_dir, stat_day)
    if not os.path.exists(day_fp):
        print("[ERROR] 前一日pkl不存在: {}".format(day_fp))
        return 2

    day_df = pd.read_pickle(day_fp)
    realized = _realized_by_user(day_df, excluded_ids)
    candidates = _build_candidates(realized)
    if not candidates:
        msg = "[高盈利用户筛选] 日期={} 昨日盈利>{} 候选=0".format(
            stat_day.strftime("%Y-%m-%d"), PROFIT_THRESHOLD
        )
        if SEND_EMPTY_RESULT:
            await _send_tg(msg, args.dry_run)
        else:
            print(msg)
        return 0

    all_paths = _list_pkl_paths(args.data_dir)
    all_df = _load_concat_pkl(all_paths)
    all_df = _ensure_user_id(all_df)
    if excluded_ids:
        all_df = all_df[~all_df["user_id"].astype(str).isin(excluded_ids)].copy()
    all_df = all_df[all_df["user_id"].isin(candidates)].copy()
    full_df = _pkl_legs_to_calc_frame(all_df)
    lines = _build_report_lines(stat_day, candidates, realized, full_df)
    text = "\n".join(lines)

    if len(lines) <= 2 and lines[-1] == "无命中用户" and not SEND_EMPTY_RESULT:
        print(text)
        return 0

    await _send_tg(text, args.dry_run)
    return 0


def main():
    args = parse_args()
    try:
        rc = asyncio.run(run_once(args))
    except KeyboardInterrupt:
        rc = 130
    except Exception as e:
        print("[ERROR] 脚本执行失败: {}".format(e))
        rc = 1
    sys.exit(rc)


if __name__ == "__main__":
    main()
