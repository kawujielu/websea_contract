from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

from exclude_user_ids import get_excluded_user_ids_sync
from mongdb_order_stats import calc_trade_stats

BJT = ZoneInfo("Asia/Shanghai")
DEFAULT_DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "mongo_daily_deals_pkl")


def parse_args():
    p = argparse.ArgumentParser(description="Analyze local PKL daily deals by natural query")
    p.add_argument("--query-text", type=str, required=True)
    p.add_argument("--data-dir", type=str, default=DEFAULT_DATA_DIR)
    p.add_argument("--print-stdout", action="store_true")
    args, _ = p.parse_known_args()
    return args


def parse_window_days(query: str) -> int:
    q = query.replace(" ", "")
    if re.search(r"(昨日|昨天)", q):
        return 1
    m = re.search(r"近(\d+)(日|天|周|月)", q)
    if not m:
        raise ValueError("未识别窗口，请使用“昨日/昨天”或“近N日/天/周/月”")
    n = int(m.group(1))
    unit = m.group(2)
    if unit in ("日", "天"):
        return n
    if unit == "周":
        return n * 7
    return n * 30


def expected_dates(window_days: int) -> List[date]:
    # 仅使用已结束自然日，窗口结束日取“昨天”
    end_day = datetime.now(BJT).date() - timedelta(days=1)
    start_day = end_day - timedelta(days=max(1, window_days) - 1)
    arr = []
    cur = start_day
    while cur <= end_day:
        arr.append(cur)
        cur += timedelta(days=1)
    return arr


def load_window_df(data_dir: str, days: List[date]) -> Tuple[pd.DataFrame, List[str], List[str]]:
    exists = []
    miss = []
    dfs = []
    for d in days:
        fp = os.path.join(data_dir, "{}.pkl".format(d.strftime("%Y-%m-%d")))
        if os.path.exists(fp):
            exists.append(d.strftime("%Y-%m-%d"))
            dfs.append(pd.read_pickle(fp))
        else:
            miss.append(d.strftime("%Y-%m-%d"))
    if not dfs:
        return pd.DataFrame(), exists, miss
    return pd.concat(dfs, ignore_index=True), exists, miss


def ensure_user_id(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "user_id" in d.columns:
        d["user_id"] = d["user_id"].astype(str)
        return d
    if "taker_user" not in d.columns or "maker_user" not in d.columns:
        raise ValueError("pkl缺少 user_id / taker_user / maker_user 字段，无法按用户分析")

    # 兼容当前日切脚本 row 结构：同一笔成交通常追加两行（taker行后maker行）
    d = d.reset_index(drop=True)
    grp_cols = ["ts_text", "symbol", "price", "amount", "taker_user", "maker_user"]
    for c in grp_cols:
        if c not in d.columns:
            raise ValueError("pkl缺少字段 {}，无法恢复 user_id".format(c))
    d["_idx_in_pair"] = d.groupby(grp_cols).cumcount()
    d["user_id"] = d["taker_user"].astype(str)
    d.loc[(d["_idx_in_pair"] % 2) == 1, "user_id"] = d.loc[(d["_idx_in_pair"] % 2) == 1, "maker_user"].astype(str)
    d = d.drop(columns=["_idx_in_pair"])
    return d


def parse_query_type(query: str) -> str:
    q = query.replace(" ", "")
    q_l = q.lower()
    if "盈利金额排名前" in q or re.search(r"盈利排名前\d+", q) is not None:
        return "profit_topn"
    if "单日盈利超过1000" in q and "前" in q and "名" in q:
        return "daily_gt1000_count_topn"
    if ("交易笔数累积排名前" in q or "交易笔数累计排名前" in q) and "名" in q:
        return "trade_topn_with_pnl"
    if "盈利超过" in q and ("用户" in q or "id" in q_l):
        return "profit_over"
    raise ValueError("未识别查询类型")


def parse_threshold(query: str) -> float:
    q = query.replace(" ", "").lower()
    m = re.search(r"盈利超过(\d+(?:\.\d+)?)", q)
    if not m:
        raise ValueError("未识别盈利阈值，请使用“盈利超过N usdt”")
    return float(m.group(1))


def parse_top_n(query: str, default_n: int = 10) -> int:
    q = query.replace(" ", "")
    m = re.search(r"前(\d+)(?:名|个|位)?(?:的用户)?", q)
    if not m:
        return default_n
    return max(1, int(m.group(1)))


def build_header(query: str, total_days: int, exists: List[str], miss: List[str]) -> List[str]:
    lines = [
        "查询语句: {}".format(query.strip()),
        "窗口天数: {}".format(total_days),
        "数据覆盖: {}/{}".format(len(exists), total_days),
    ]
    if miss:
        lines.append("缺失日期: {}".format(", ".join(miss)))
    else:
        lines.append("缺失日期: 无")
    lines.append("")
    return lines


def _trade_nature_key(deal_is_protected: int, deal_sub_id: int) -> str:
    """与 mongdb_order / user_deal_analysis_on_demand 逐笔腿分类一致。"""
    if deal_is_protected == 0 and deal_sub_id == 0:
        return "自主交易"
    if deal_is_protected != 0 and deal_sub_id != 0:
        return "跟单交易"
    return "其他"


def _leg_trade_type_stats(deal_is_protected: int, deal_sub_id: int) -> Dict[str, int]:
    """单笔腿 -> trade_type_stats 字典（供 calc_trade_stats 按订单行汇总）。"""
    key = _trade_nature_key(int(deal_is_protected), int(deal_sub_id))
    return {key: 1}


def run_profit_over(df: pd.DataFrame, threshold: float) -> List[str]:
    d = df.copy()
    d["profit_loss"] = pd.to_numeric(d["profit_loss"], errors="coerce").fillna(0.0)
    gp = d.groupby("user_id", as_index=False)["profit_loss"].sum()
    gp = gp[gp["profit_loss"] > threshold].sort_values("profit_loss", ascending=False)
    lines = ["查询结果: 盈利超过 {} USDT 的用户ID".format(threshold)]
    if gp.shape[0] == 0:
        lines.append("无结果")
        return lines
    for _, r in gp.iterrows():
        lines.append("user_id={} total_profit={:.4f}".format(r["user_id"], float(r["profit_loss"])))
    return lines


def _pkl_legs_to_calc_trade_stats_df(sub: pd.DataFrame, uid: str) -> pd.DataFrame:
    """将单日切 PKL 中某 user_id 的成交腿，转为 mongdb_order_stats.calc_trade_stats 所需列。

    PKL 无 order_id 时按「逐笔腿」输出；含 deal_is_protected / deal_sub_id / trade_type_stats，
    与 mongdb_order.py、user_deal_analysis_on_demand 聚合后字段对齐。
    """
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
        "trade_type_stats",
    ]
    if sub is None or sub.shape[0] == 0:
        return pd.DataFrame(columns=cols)

    d = sub.copy()
    su = str(uid)
    fv = pd.to_numeric(d["face_value"], errors="coerce").fillna(1.0)
    base_amt = pd.to_numeric(d["amount"], errors="coerce").fillna(0.0)
    pr = pd.to_numeric(d["price"], errors="coerce").fillna(0.0)
    qty = base_amt * fv
    amt_notional = pr * base_amt * fv

    tu = d["taker_user"].astype(str)
    mu = d["maker_user"].astype(str)
    is_taker = tu == su
    is_maker = mu == su

    if "deal_is_protected" in d.columns and "deal_sub_id" in d.columns:
        dip = pd.to_numeric(d["deal_is_protected"], errors="coerce").fillna(0).astype(int)
        dsid = pd.to_numeric(d["deal_sub_id"], errors="coerce").fillna(0).astype(int)
    else:
        dip = pd.Series(0, index=d.index, dtype=int)
        dsid = pd.Series(0, index=d.index, dtype=int)

    trade_type_stats_col = [
        _leg_trade_type_stats(int(p), int(s)) for p, s in zip(dip.tolist(), dsid.tolist())
    ]

    out = pd.DataFrame(
        {
            "userid": su,
            "tsText": d["ts_text"].astype(str),
            "symbol": d["symbol"],
            "buy_sell": d["buy_sell"].astype(str),
            "profit_loss": pd.to_numeric(d["profit_loss"], errors="coerce").fillna(0.0),
            "price": pr,
            "amount": qty,
            "amt": amt_notional,
            "multiple": pd.to_numeric(d["multiple"], errors="coerce").fillna(0).astype(int),
            "fee": pd.to_numeric(d["fee"], errors="coerce").fillna(0.0),
            "taker_num": is_taker.astype(int),
            "maker_num": is_maker.astype(int),
            "taker_amt": amt_notional.where(is_taker, 0.0),
            "maker_amt": amt_notional.where(is_maker, 0.0),
            "deal_is_protected": dip,
            "deal_sub_id": dsid,
            "trade_type_stats": trade_type_stats_col,
        }
    )
    return out.sort_values("tsText", kind="mergesort").reset_index(drop=True)


def _format_trade_nature(tn) -> str:
    if isinstance(tn, dict):
        return "自主{} 跟单{} 其他{}".format(
            tn.get("自主交易", 0),
            tn.get("跟单交易", 0),
            tn.get("其他", 0),
        )
    return str(tn)


def _format_profit_top_user_line_mongdb_style(uid: str, res: dict) -> str:
    """与 mongdb_order / monthly_profit_top_users 输出字段对齐。"""
    return (
        "id:{} "
        "总盈利:{} "
        "开平仓轮次:{} "
        "胜率:{} "
        "盈亏比:{} "
        "持仓时间中位数:{} "
        "交易性质:{}".format(
            uid,
            res.get("总盈亏", ""),
            res.get("总交易轮次(开平算一次)", ""),
            res.get("胜率", ""),
            res.get("盈亏比", ""),
            res.get("中位数持仓时长", ""),
            _format_trade_nature(res.get("交易性质", {})),
        )
    )


def run_profit_topn(df: pd.DataFrame, top_n: int) -> List[str]:
    d = df.copy()
    d["profit_loss"] = pd.to_numeric(d["profit_loss"], errors="coerce").fillna(0.0)
    gp = d.groupby("user_id", as_index=False)["profit_loss"].sum()
    gp = gp.sort_values(["profit_loss", "user_id"], ascending=[False, True]).head(top_n)
    lines = ["查询结果: 盈利金额排名 Top{} 用户".format(top_n)]
    if gp.shape[0] == 0:
        lines.append("无结果")
        return lines
    for _, r in gp.iterrows():
        uid = str(r["user_id"])
        sub = d[d["user_id"].astype(str) == uid].copy()
        s_df = _pkl_legs_to_calc_trade_stats_df(sub, uid)
        if s_df.shape[0] == 0:
            lines.append("id:{} 无数据".format(uid))
            continue
        try:
            res = calc_trade_stats(s_df, userid=str(uid))
            lines.append(_format_profit_top_user_line_mongdb_style(uid, res))
        except Exception as e:
            lines.append("id:{} 统计失败: {}".format(uid, e))
    return lines


def load_day_df(data_dir: str, day: date) -> pd.DataFrame:
    fp = os.path.join(data_dir, "{}.pkl".format(day.strftime("%Y-%m-%d")))
    if not os.path.exists(fp):
        raise FileNotFoundError("pkl文件不存在: {}".format(fp))
    return pd.read_pickle(fp)


def get_topn_user_ids_for_day(data_dir: str, day: date, top_n: int = 10) -> List[str]:
    df_raw = load_day_df(data_dir, day)
    if df_raw.shape[0] == 0:
        return []
    d = ensure_user_id(df_raw)
    excluded = get_excluded_user_ids_sync()
    if excluded:
        d = d[~d["user_id"].astype(str).isin(excluded)].copy()
    d["profit_loss"] = pd.to_numeric(d["profit_loss"], errors="coerce").fillna(0.0)
    gp = d.groupby("user_id", as_index=False)["profit_loss"].sum()
    gp = gp.sort_values(["profit_loss", "user_id"], ascending=[False, True]).head(max(1, int(top_n)))
    return [str(x) for x in gp["user_id"].tolist()]


def build_profit_topn_lines_for_day(data_dir: str, day: date, top_n: int = 10) -> List[str]:
    df_raw = load_day_df(data_dir, day)
    d = ensure_user_id(df_raw)
    excluded = get_excluded_user_ids_sync()
    if excluded:
        d = d[~d["user_id"].astype(str).isin(excluded)].copy()
    lines = [
        "查询语句: 昨日盈利排名前{}用户".format(max(1, int(top_n))),
        "窗口天数: 1",
        "数据覆盖: 1/1",
        "缺失日期: 无",
        "",
    ]
    lines.extend(run_profit_topn(d, max(1, int(top_n))))
    return lines


def run_daily_gt1000_count_topn(df: pd.DataFrame, top_n: int) -> List[str]:
    d = df.copy()
    d["profit_loss"] = pd.to_numeric(d["profit_loss"], errors="coerce").fillna(0.0)
    d["day"] = pd.to_datetime(d["ts_text"], errors="coerce").dt.strftime("%Y-%m-%d")
    daily = d.groupby(["day", "user_id"], as_index=False)["profit_loss"].sum()
    hit = daily[daily["profit_loss"] > 1000.0]
    cnt = hit.groupby("user_id", as_index=False)["day"].count().rename(columns={"day": "days_over_1000"})
    cnt = cnt.sort_values(["days_over_1000", "user_id"], ascending=[False, True]).head(top_n)
    lines = ["查询结果: 单日盈利超过1000U出现次数 Top{}".format(top_n)]
    if cnt.shape[0] == 0:
        lines.append("无结果")
        return lines
    for _, r in cnt.iterrows():
        lines.append("user_id={} days_over_1000={}".format(r["user_id"], int(r["days_over_1000"])))
    return lines


def run_trade_topn_with_pnl(df: pd.DataFrame, top_n: int) -> List[str]:
    d = df.copy()
    d["profit_loss"] = pd.to_numeric(d["profit_loss"], errors="coerce").fillna(0.0)
    gp = d.groupby("user_id", as_index=False).agg(
        trade_count=("user_id", "count"),
        total_profit=("profit_loss", "sum"),
    )
    gp = gp.sort_values(["trade_count", "user_id"], ascending=[False, True]).head(top_n)
    lines = ["查询结果: 用户交易笔数累积 Top{}（含对应盈亏）".format(top_n)]
    if gp.shape[0] == 0:
        lines.append("无结果")
        return lines
    for _, r in gp.iterrows():
        lines.append(
            "user_id={} trade_count={} total_profit={:.4f}".format(
                r["user_id"], int(r["trade_count"]), float(r["total_profit"])
            )
        )
    return lines


def analyze(query: str, data_dir: str) -> str:
    window_days = parse_window_days(query)
    days = expected_dates(window_days)
    df_raw, exists, miss = load_window_df(data_dir, days)

    out = build_header(query, len(days), exists, miss)
    if df_raw.shape[0] == 0:
        out.append("查询结果: 无可用数据（窗口内文件全部缺失）")
        return "\n".join(out)

    df = ensure_user_id(df_raw)
    excluded = get_excluded_user_ids_sync()
    if excluded:
        df = df[~df["user_id"].astype(str).isin(excluded)].copy()
    qtype = parse_query_type(query)
    if qtype == "profit_topn":
        top_n = parse_top_n(query, default_n=10)
        out.extend(run_profit_topn(df, top_n))
    elif qtype == "profit_over":
        threshold = parse_threshold(query)
        out.extend(run_profit_over(df, threshold))
    elif qtype == "daily_gt1000_count_topn":
        top_n = parse_top_n(query, default_n=10)
        out.extend(run_daily_gt1000_count_topn(df, top_n))
    elif qtype == "trade_topn_with_pnl":
        top_n = parse_top_n(query, default_n=10)
        out.extend(run_trade_topn_with_pnl(df, top_n))
    else:
        out.append("查询结果: 未识别查询类型")
    return "\n".join(out)


def main():
    args = parse_args()
    text = analyze(args.query_text, args.data_dir)
    if args.print_stdout:
        print(text)


if __name__ == "__main__":
    main()

