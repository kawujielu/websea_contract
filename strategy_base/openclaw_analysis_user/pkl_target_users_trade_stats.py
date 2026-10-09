# -*- coding: utf-8 -*-
"""
从 Ubuntu/本机日切 PKL（mongo_daily_deals_dump 格式）读取成交明细，
对指定用户列表做两套窗口统计：①当年 4 月 1 日至今（北京时间）；②目录内全部 PKL 全量。

口径与 user_deal_analysis_on_demand + mongdb_order_stats.calc_trade_stats 一致。

依赖：同目录 mongdb_order_stats.py；Python 3.9+（在 3.9.19 下编写）。
导出 Excel：成交明细与/或控制台同内容的「统计输出」表。.xlsx 需 openpyxl；.xls 需 xlwt（单表≤65535 行）。

执行方式:
python3 pkl_target_users_trade_stats.py --excel-print summary.xlsx --print-stdout
"""
from __future__ import annotations

import argparse
import os
import sys
import warnings
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo

_DIR = os.path.dirname(os.path.abspath(__file__))
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)

from mongdb_order_stats import calc_trade_stats  # noqa: E402
from exclude_user_ids import get_excluded_user_ids_sync  # noqa: E402

# NumPy 2.x 部分路径下 cumprod/prod 溢出仍可能打印；按消息屏蔽以免刷屏（主统计不依赖该路径）
warnings.filterwarnings(
    "ignore",
    message="overflow encountered",
    category=RuntimeWarning,
)

BJT = ZoneInfo("Asia/Shanghai")

# 指定分析用户（与需求一致）
TARGET_USER_IDS: Tuple[str, ...] = tuple(
    str(x)
    for x in (
        632588,621582,539308,556300,643053,591460,618378,638112,647069,538033,646515,641559,649984,580060,548089,645459,645429,620804,641168,640839,546375,639519,556374,612461,574305,639481,575822,588138,628459,457138,640053,571775,633213,623388,552445,578085,574458,632681,523679,573455,552522,593896,633241,640276,618413,628914,572522,640055,592256,590648
        # 482767,
        # 580060,
        # 641273,
        # 632910,
        # 642243,
        # 115,
        # 632588,
        # 621582,
        # 539308,
        # 642226,
        # 643053,
        # 626305,
        # 618378,
        # 638112,
    )
)

DEFAULT_DATA_DIR = os.path.join(_DIR, "data", "mongo_daily_deals_pkl")

# calc_trade_stats 键 -> 输出展示名
OUTPUT_METRICS: Tuple[Tuple[str, str], ...] = (
    ("总盈亏", "总盈亏"),
    ("成交笔数(按订单算)", "成交笔数(按订单算)"),
    ("交易性质", "交易性质"),
    ("总手续费", "总手续费"),
    ("刨除手续费总盈亏", "刨除手续费总盈亏"),
    ("手续费占比", "手续费占比"),
    ("胜率", "胜率"),
    ("盈亏比", "盈亏比"),
    ("夏普比率", "夏普比率"),
    ("总交易轮次(开平算一次)", "开平仓轮次"),
    ("中位数持仓时长", "持仓时长中位数"),
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="指定用户：PKL 全量 + 4月1日起 两套窗口，输出精简交易统计"
    )
    p.add_argument(
        "--data-dir",
        type=str,
        default=os.environ.get("UPM_PKL_DEALS_DIR", DEFAULT_DATA_DIR),
        help="日切 PKL 目录（文件名 YYYY-MM-DD.pkl），可与 Ubuntu 上路径一致",
    )
    p.add_argument(
        "--april-year",
        type=int,
        default=None,
        help="4 月 1 日起算的年份，默认当前北京时间年份",
    )
    p.add_argument("--print-stdout", action="store_true", help="打印到 stdout")
    p.add_argument(
        "--csv",
        type=str,
        default="",
        help="可选：结果 CSV 路径",
    )
    p.add_argument(
        "--excel-deals",
        type=str,
        default="",
        help="可选：每用户成交明细表；有指标时另写「指标汇总」列式表；与 --excel-print 同路径合并",
    )
    p.add_argument(
        "--excel-print",
        type=str,
        default="",
        help="可选：写入「指标汇总」(多列指标表)+「运行说明」(PKL 说明)；可与 --excel-deals 同路径合并",
    )
    args, _ = p.parse_known_args()
    return args


def ensure_user_id(df: pd.DataFrame) -> pd.DataFrame:
    """与 pkl_user_query_analyzer.ensure_user_id 一致：从 taker/maker 双行恢复 user_id。"""
    d = df.copy()
    if "user_id" in d.columns:
        d["user_id"] = d["user_id"].astype(str)
        return d
    if "taker_user" not in d.columns or "maker_user" not in d.columns:
        raise ValueError("pkl 缺少 user_id / taker_user / maker_user，无法按用户分析")

    d = d.reset_index(drop=True)
    grp_cols = ["ts_text", "symbol", "price", "amount", "taker_user", "maker_user"]
    for c in grp_cols:
        if c not in d.columns:
            raise ValueError("pkl 缺少字段 {}，无法恢复 user_id".format(c))
    d["_idx_in_pair"] = d.groupby(grp_cols).cumcount()
    d["user_id"] = d["taker_user"].astype(str)
    d.loc[(d["_idx_in_pair"] % 2) == 1, "user_id"] = d.loc[
        (d["_idx_in_pair"] % 2) == 1, "maker_user"
    ].astype(str)
    d = d.drop(columns=["_idx_in_pair"])
    return d


def list_pkl_paths(data_dir: str) -> List[str]:
    if not os.path.isdir(data_dir):
        raise FileNotFoundError("PKL 目录不存在: {}".format(data_dir))
    out: List[str] = []
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


def load_concat_pkls(paths: Iterable[str]) -> pd.DataFrame:
    ps = list(paths)
    if not ps:
        return pd.DataFrame()
    dfs = [pd.read_pickle(p) for p in ps]
    return pd.concat(dfs, ignore_index=True)


def filter_target_users_raw(df: pd.DataFrame, uids: Iterable[str]) -> pd.DataFrame:
    """只保留「任一侧在目标用户列表」的成交行，避免拆对。"""
    if df.shape[0] == 0:
        return df
    uset = {str(u) for u in uids}
    t = df["taker_user"].astype(str)
    m = df["maker_user"].astype(str)
    return df[t.isin(uset) | m.isin(uset)].copy()


def filter_bjt_date_range(
    df: pd.DataFrame, start_d: date, end_d: date
) -> pd.DataFrame:
    """闭区间 [start_d 00:00, end_d 23:59:59]（按 ts_text 解析出的本地 naive 时间比较）。"""
    if df.shape[0] == 0:
        return df
    ts = pd.to_datetime(df["ts_text"], errors="coerce")
    lo = pd.Timestamp(datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0))
    hi = pd.Timestamp(
        datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59)
    )
    return df[(ts >= lo) & (ts <= hi)].copy()


def pkl_legs_to_calc_frame(d: pd.DataFrame) -> pd.DataFrame:
    """
    mongo_daily_deals PKL 每行一笔用户腿 -> calc_trade_stats 所需列。
    amount 为带 face_value 的张数；amt 为名义金额（与 user_deal 聚合后一致）。
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
    taker_num = is_taker.astype(int)
    maker_num = is_maker.astype(int)
    taker_amt = np.where(is_taker, amt_notional, 0.0)
    maker_amt = np.where(is_maker, amt_notional, 0.0)

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
            "taker_num": taker_num,
            "maker_num": maker_num,
            "taker_amt": taker_amt,
            "maker_amt": maker_amt,
            "deal_is_protected": dip,
            "deal_sub_id": dsid,
        }
    )
    out = out.sort_values("tsText", kind="mergesort").reset_index(drop=True)
    return out


def _excel_sheet_name(uid: str) -> str:
    """Excel 工作表名 ≤31 字符且不含非法字符。"""
    s = str(uid).strip()
    if not s:
        s = "user"
    for ch in ("\\", "/", "?", "*", "[", "]", ":"):
        s = s.replace(ch, "_")
    return s[:31]


def _deals_export_dataframe(sub: pd.DataFrame) -> pd.DataFrame:
    """PKL 腿表 -> 导出用列（中文表头）。"""
    if sub.shape[0] == 0:
        return pd.DataFrame(
            columns=[
                "用户ID",
                "成交时间",
                "合约",
                "方向",
                "价格",
                "基础数量",
                "面值",
                "名义金额",
                "盈亏",
                "手续费",
                "杠杆",
                "taker用户",
                "maker用户",
            ]
        )
    d = sub.copy()
    pr = pd.to_numeric(d["price"], errors="coerce").fillna(0.0)
    ba = pd.to_numeric(d["amount"], errors="coerce").fillna(0.0)
    fv = pd.to_numeric(d["face_value"], errors="coerce").fillna(1.0)
    out = pd.DataFrame(
        {
            "用户ID": d["user_id"].astype(str),
            "成交时间": d["ts_text"],
            "合约": d["symbol"],
            "方向": d["buy_sell"],
            "价格": pr,
            "基础数量": ba,
            "面值": fv,
            "名义金额": (pr * ba * fv).round(8),
            "盈亏": pd.to_numeric(d["profit_loss"], errors="coerce").fillna(0.0),
            "手续费": pd.to_numeric(d["fee"], errors="coerce").fillna(0.0),
            "杠杆": pd.to_numeric(d["multiple"], errors="coerce").fillna(0).astype(int),
            "taker用户": d["taker_user"].astype(str),
            "maker用户": d["maker_user"].astype(str),
        }
    )
    return out


METRICS_SHEET_BASE = "指标汇总"
NOTES_SHEET_BASE = "运行说明"


def _prepare_excel_writer(path: str) -> Tuple[str, str]:
    path = os.path.abspath(path.strip())
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xls":
        try:
            import xlwt  # noqa: F401
        except ImportError as e:
            raise ImportError("导出 .xls 需要安装 xlwt：pip install xlwt") from e
        return path, "xlwt"
    if ext not in (".xlsx", ".xlsm"):
        path = path + ".xlsx"
    try:
        import openpyxl  # noqa: F401
    except ImportError as e:
        raise ImportError("导出 .xlsx 需要安装 openpyxl：pip install openpyxl") from e
    return path, "openpyxl"


def _summary_lines_dataframe(summary_text: str) -> pd.DataFrame:
    lines = summary_text.splitlines()
    return pd.DataFrame({"行号": range(1, len(lines) + 1), "内容": lines})


def _stats_df_for_excel(stats_df: pd.DataFrame) -> pd.DataFrame:
    """指标表：列名与单元格一一对应；user_id 导出为「用户ID」。"""
    out = stats_df.copy()
    if "user_id" in out.columns:
        out = out.rename(columns={"user_id": "用户ID"})
    return out


def write_excel_workbook(
    path: str,
    legs: pd.DataFrame,
    uids: Tuple[str, ...],
    write_deals: bool,
    stats_df: Optional[pd.DataFrame] = None,
    notes_for_sheet: str = "",
) -> None:
    """
    同一工作簿：
    - 可选：每用户成交明细表
    - 有数据时：「指标汇总」表（多列结构化，非 to_string 文本）
    - 可选：「运行说明」（仅 PKL 说明等按行，不含指标宽表）
    """
    path, engine = _prepare_excel_writer(path)
    used_names: Dict[str, int] = {}

    def unique_sheet(base: str) -> str:
        name = _excel_sheet_name(base)
        if name not in used_names:
            used_names[name] = 0
            return name
        used_names[name] += 1
        suffix = "_{}".format(used_names[name])
        return (name[: 31 - len(suffix)] + suffix)[:31]

    has_notes = bool((notes_for_sheet or "").strip())
    has_stats = stats_df is not None and stats_df.shape[0] > 0
    deals_ok = write_deals and legs is not None and legs.shape[0] > 0
    if not deals_ok and not has_notes and not has_stats:
        raise ValueError("无成交明细、无指标表且无运行说明，跳过生成 Excel")

    with pd.ExcelWriter(path, engine=engine) as writer:
        if deals_ok:
            for uid in uids:
                sub = legs[legs["user_id"].astype(str) == str(uid)].copy()
                sub = sub.sort_values("ts_text", kind="mergesort")
                exp = _deals_export_dataframe(sub)
                if engine == "xlwt" and exp.shape[0] > 65535:
                    raise ValueError(
                        "用户 {} 明细行数 {} 超过 .xls 单表上限 65535，请改用 .xlsx".format(
                            uid, exp.shape[0]
                        )
                    )
                sheet = unique_sheet(str(uid))
                exp.to_excel(writer, sheet_name=sheet, index=False)

        if has_stats:
            mdf = _stats_df_for_excel(stats_df)
            ms = unique_sheet(METRICS_SHEET_BASE)
            mdf.to_excel(writer, sheet_name=ms, index=False)

        if has_notes:
            sdf = _summary_lines_dataframe(notes_for_sheet or "")
            if engine == "xlwt" and sdf.shape[0] > 65535:
                raise ValueError(
                    "运行说明行数 {} 超过 .xls 单表上限 65535，请改用 .xlsx".format(
                        sdf.shape[0]
                    )
                )
            sn = unique_sheet(NOTES_SHEET_BASE)
            sdf.to_excel(writer, sheet_name=sn, index=False)


def extract_metrics(stats: Dict) -> Dict[str, object]:
    row: Dict[str, object] = {}
    fee = stats.get("总手续费")
    if fee is None:
        fee = stats.get("手续费总和")
    stats = dict(stats)
    stats["总手续费"] = fee
    for key_in, label in OUTPUT_METRICS:
        row[label] = stats.get(key_in)
    return row


def run_analysis(
    data_dir: str,
    april_year: int,
    uids: Tuple[str, ...],
) -> Tuple[pd.DataFrame, List[str], pd.DataFrame]:
    paths = list_pkl_paths(data_dir)
    notes: List[str] = []
    notes.append("PKL 目录: {}".format(os.path.abspath(data_dir)))
    notes.append("载入文件数: {}".format(len(paths)))
    empty_legs = pd.DataFrame()
    if not paths:
        notes.append("警告: 未找到任何 YYYY-MM-DD.pkl")
        return pd.DataFrame(), notes, empty_legs

    raw = load_concat_pkls(paths)
    notes.append("原始行数: {}".format(raw.shape[0]))
    excluded = get_excluded_user_ids_sync()
    effective_uids = tuple(u for u in uids if str(u) not in excluded)
    if len(effective_uids) != len(uids):
        notes.append("剔除模拟金/做市后目标用户数: {} -> {}".format(len(uids), len(effective_uids)))
    if not effective_uids:
        notes.append("目标用户全部命中模拟金/做市剔除名单，无可分析用户")
        return pd.DataFrame(), notes, empty_legs

    raw = filter_target_users_raw(raw, effective_uids)
    notes.append("目标用户相关行数: {}".format(raw.shape[0]))

    if raw.shape[0] == 0:
        return pd.DataFrame(), notes, empty_legs

    legs = ensure_user_id(raw)
    legs = legs[legs["user_id"].isin(set(effective_uids))].copy()

    today_bjt = datetime.now(BJT).date()
    start_april = date(april_year, 4, 1)
    legs_april = filter_bjt_date_range(legs, start_april, today_bjt)
    notes.append(
        "窗口「{}-04-01 ~ {}」行数: {}".format(
            april_year, today_bjt.isoformat(), legs_april.shape[0]
        )
    )

    rows_out: List[Dict] = []
    for uid in effective_uids:
        for label, sub in (
            (
                "{}-04-01至今".format(april_year),
                legs_april[legs_april["user_id"] == uid],
            ),
            ("全量PKL", legs[legs["user_id"] == uid]),
        ):
            cdf = pkl_legs_to_calc_frame(sub)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                stats = calc_trade_stats(cdf, userid=str(uid))
            m = extract_metrics(stats)
            m["user_id"] = uid
            m["窗口"] = label
            rows_out.append(m)

    return pd.DataFrame(rows_out), notes, legs


def main() -> int:
    args = parse_args()
    year = args.april_year
    if year is None:
        year = datetime.now(BJT).year

    df, notes, legs = run_analysis(args.data_dir, year, TARGET_USER_IDS)
    col_order = ["user_id", "窗口"] + [b for _, b in OUTPUT_METRICS]
    if df.shape[0]:
        df = df[[c for c in col_order if c in df.columns]]

    text_lines = list(notes) + [""]
    if df.shape[0] == 0:
        text_lines.append("无输出（数据为空或目录无 pkl）")
    else:
        text_lines.append(df.to_string(index=False))
    text = "\n".join(text_lines)

    notes_only_lines = list(notes)
    if df.shape[0] == 0:
        notes_only_lines.append("无输出（数据为空或目录无 pkl）")
    notes_for_excel = "\n".join(notes_only_lines)
    stats_for_excel = df if df.shape[0] else None

    deals_p = (args.excel_deals or "").strip()
    print_p = (args.excel_print or "").strip()

    def _norm_excel_path(p: str) -> str:
        return os.path.normcase(os.path.abspath(p)) if p else ""

    if deals_p or print_p:
        try:
            if deals_p and print_p and _norm_excel_path(deals_p) == _norm_excel_path(print_p):
                write_excel_workbook(
                    deals_p,
                    legs,
                    tuple(sorted(legs["user_id"].astype(str).unique().tolist())),
                    write_deals=True,
                    stats_df=stats_for_excel,
                    notes_for_sheet=notes_for_excel,
                )
                sys.stderr.write(
                    "Excel 已写入（成交明细+指标汇总+运行说明）: {}\n".format(
                        os.path.abspath(deals_p)
                    )
                )
            elif deals_p:
                write_excel_workbook(
                    deals_p,
                    legs,
                    tuple(sorted(legs["user_id"].astype(str).unique().tolist())),
                    write_deals=True,
                    stats_df=stats_for_excel,
                    notes_for_sheet="",
                )
                sys.stderr.write(
                    "Excel 已写入（成交明细+指标汇总）: {}\n".format(
                        os.path.abspath(deals_p)
                    )
                )
            else:
                write_excel_workbook(
                    print_p,
                    legs,
                    tuple(sorted(legs["user_id"].astype(str).unique().tolist())),
                    write_deals=False,
                    stats_df=stats_for_excel,
                    notes_for_sheet=notes_for_excel,
                )
                sys.stderr.write(
                    "Excel 已写入（指标汇总+运行说明）: {}\n".format(
                        os.path.abspath(print_p)
                    )
                )
        except Exception as e:
            sys.stderr.write("Excel 写入失败: {}\n".format(e))

    if args.print_stdout:
        print(text)

    if args.csv:
        if df.shape[0]:
            df.to_csv(args.csv, index=False, encoding="utf-8-sig")
        else:
            pd.DataFrame().to_csv(args.csv, index=False, encoding="utf-8-sig")

    return 0


if __name__ == "__main__":
    sys.exit(main())
