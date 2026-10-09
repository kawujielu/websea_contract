# -*- coding: utf-8 -*-
"""从 mongdb_order 抽取的统计函数：修正交易起止时间、空持仓与除零。"""
import warnings
from collections import deque

import numpy as np
import pandas as pd
from scipy import stats

_TRADE_NATURE_KEYS = ("自主交易", "跟单交易", "其他")


def _summarize_trade_nature_df(d: pd.DataFrame) -> dict:
    """逐笔腿：deal_is_protected + deal_sub_id；或聚合订单行上的 trade_type_stats 字典。"""
    out = {k: 0 for k in _TRADE_NATURE_KEYS}
    if d is None or d.shape[0] == 0:
        return out
    if "trade_type_stats" in d.columns:
        for v in d["trade_type_stats"]:
            if isinstance(v, dict):
                for k in _TRADE_NATURE_KEYS:
                    out[k] += int(v.get(k, 0))
        return out
    if "deal_is_protected" in d.columns and "deal_sub_id" in d.columns:
        for _, r in d.iterrows():
            p, s = r["deal_is_protected"], r["deal_sub_id"]
            if pd.isna(p):
                p = 0
            else:
                try:
                    p = int(p)
                except (TypeError, ValueError):
                    p = 0
            if pd.isna(s):
                s = 0
            else:
                try:
                    s = int(s)
                except (TypeError, ValueError):
                    s = 0
            if p == 0 and s == 0:
                out["自主交易"] += 1
            elif p != 0 and s != 0:
                out["跟单交易"] += 1
            else:
                out["其他"] += 1
        return out
    return out


def _ensure_amt(df: pd.DataFrame) -> pd.Series:
    if "amt" in df.columns and pd.api.types.is_numeric_dtype(df["amt"]):
        return df["amt"].astype(float)
    return (df["price"].astype(float) * df["amount"].astype(float))


def _fee_ratio_pct(total_fee: float, total_profit: float) -> str:
    """手续费占「总盈亏(平仓盈亏合计)」比例；总盈亏为 0 时返回 N/A（避免除零）。"""
    try:
        tp = float(total_profit)
    except (TypeError, ValueError):
        return "N/A"
    if abs(tp) < 1e-12:
        return "N/A"
    return str(round(float(total_fee) / tp * 100, 2)) + "%"


def max_consecutive(arr):
    max_cnt = cnt = 0
    for x in arr:
        if x:
            cnt += 1
            max_cnt = max(max_cnt, cnt)
        else:
            cnt = 0
    return max_cnt


def _total_return_from_simple_returns_no_prod(r: np.ndarray) -> float:
    """prod(1+r)-1 的等价：sum(log1p(r)) 再 expm1；避免 np.prod 在 NumPy 2.x 仍刷 overflow 告警。"""
    r = np.asarray(r, dtype=np.float64)
    if np.any(r <= -1.0):
        return float("nan")
    s = float(np.sum(np.log1p(r)))
    # exp(709) 量级贴近 float64 上限，略留余量避免个别平台对 exp 仍报 overflow
    if not np.isfinite(s) or s > 650.0:
        return float("nan")
    out = float(np.expm1(s))
    return out if np.isfinite(out) else float("nan")


def _max_drawdown_from_returns(returns: np.ndarray) -> float:
    if len(returns) == 0:
        return np.nan
    r = np.asarray(returns, dtype=np.float64)
    if np.any(r <= -1.0):
        return np.nan
    # 权益曲线：exp(cumsum(log1p(r)))，等价 cumprod(1+r)，但不走 multiply 累乘
    c = np.cumsum(np.log1p(r))
    if not np.all(np.isfinite(c)) or float(np.nanmax(c)) > 650.0:
        return np.nan
    with np.errstate(over="ignore", invalid="ignore"):
        equity = np.exp(c)
    if not np.all(np.isfinite(equity)) or np.any(equity <= 0):
        return np.nan
    peak = np.maximum.accumulate(equity)
    dd = (peak - equity) / peak
    if not np.all(np.isfinite(dd)):
        return np.nan
    return float(np.max(dd))


def _metrics_from_returns(r: np.ndarray, rf: float = 0.0) -> dict:
    r = np.asarray(r, dtype=float)
    r = r[~np.isnan(r)]
    n = len(r)
    if n < 2:
        return {
            "n": n,
            "pToR_mean_vol": np.nan,
            "T_tstat": np.nan,
            "ks_D": np.nan,
            "ks_p": np.nan,
            "total_return": np.nan,
            "mdd": np.nan,
            "pToR_total_mdd": np.nan,
        }

    ex = r - rf
    mu = ex.mean()
    sd = ex.std(ddof=1)
    ptor = np.nan if sd == 0 else float(mu / sd)
    se = sd / np.sqrt(n)
    T = np.nan if se == 0 else float(mu / se)
    z = (r - r.mean()) / r.std(ddof=1)
    ks_D, ks_p = stats.kstest(z, "norm")
    total_ret = _total_return_from_simple_returns_no_prod(r)
    if not np.isfinite(total_ret):
        total_ret = np.nan
    mdd = _max_drawdown_from_returns(r)
    ptor_mdd = np.nan if (mdd == 0 or np.isnan(mdd)) else float(total_ret / mdd)
    if not np.isfinite(ptor_mdd):
        ptor_mdd = np.nan

    return {
        "n": f"{int(n)} 成交笔数",
        "pToR_mean_vol": f"{round(ptor, 4)} 收益能力风险比。越大越好，单位波动的平均收益更高",
        "T_tstat": f"{round(T, 4)} 收益显著性。越大越稳定显著",
        "ks_D": f"{round(ks_D, 4)} 检验统计量D值。越大说明分布越偏离正态",
        "ks_p": f"{round(ks_p, 4)} 正态性检验p-value",
        "pToR_total_mdd": f"{round(ptor_mdd, 4)} 收益回撤比（总收益/最大回撤）（越大越好）",
    }


def compute_user_metrics(
    df: pd.DataFrame,
    capital_mode: str = "margin",
    rf: float = 0.0,
    min_trades: int = 30,
):
    d = df.copy()
    d["ts"] = pd.to_datetime(d["tsText"], errors="coerce")
    d = d.sort_values(["userid", "ts"])
    d["net_pnl"] = pd.to_numeric(d["profit_loss"], errors="coerce") - pd.to_numeric(d["fee"], errors="coerce")
    amt = pd.to_numeric(d["amt"], errors="coerce").abs()
    mult = pd.to_numeric(d["multiple"], errors="coerce")
    if capital_mode == "margin":
        mult = mult.replace([0, np.inf, -np.inf], np.nan)
        d["capital"] = amt / mult
    elif capital_mode == "notional":
        d["capital"] = amt
    else:
        raise ValueError("capital_mode must be 'margin' or 'notional'")
    d["ret"] = d["net_pnl"] / d["capital"]
    d = d.replace([np.inf, -np.inf], np.nan)
    d = d.dropna(subset=["userid", "ret"])
    rows = []
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        for _, g in d.groupby("userid", sort=False):
            r = g["ret"].to_numpy(dtype=float)
            if len(r) < min_trades:
                continue
            rows.append({**_metrics_from_returns(r, rf=rf)})
    return rows


def holding_records_dataframe(d: pd.DataFrame) -> pd.DataFrame:
    """输入已按时间排序、含 ts(datetime)、buy_sell、amount 的逐笔 df，返回持仓片段表。"""
    d = d.sort_values("ts").reset_index(drop=True)
    positions = {'long': deque(), 'short': deque()}
    in_position = {'long': False, 'short': False}
    holding_records = []
    for _, r in d.iterrows():
        ts, side, amt = r['ts'], r['buy_sell'], float(r['amount'])
        if side == '开多':
            if not in_position['long']:
                in_position['long'] = True
                positions['long'].append({'open_time': ts, 'amount': amt})
        elif side == '开空':
            if not in_position['short']:
                in_position['short'] = True
                positions['short'].append({'open_time': ts, 'amount': amt})
        elif side in ('平多', '平空'):
            key = 'long' if side == '平多' else 'short'
            remain = amt
            while remain > 0 and positions[key]:
                pos = positions[key][0]
                close_qty = min(remain, pos['amount'])
                holding_records.append({
                    'open_time': pos['open_time'],
                    'close_time': ts,
                    'amount': close_qty,
                    'holding_seconds': (ts - pos['open_time']).total_seconds(),
                })
                pos['amount'] -= close_qty
                remain -= close_qty
                if pos['amount'] == 0:
                    positions[key].popleft()
            if side == '平多' and not positions['long'] and in_position['long']:
                in_position['long'] = False
            if side == '平空' and not positions['short'] and in_position['short']:
                in_position['short'] = False
    return pd.DataFrame(holding_records)


def calc_trade_stats(df: pd.DataFrame, userid=None) -> dict:
    """返回中文键字典；交易开始/结束时间为窗口内 df 的最早/最晚 tsText 日期。"""
    empty = {
        '交易开始时间': None,
        '交易结束时间': None,
        '成交笔数(按订单算)': 0,
        '交易性质': {k: 0 for k in _TRADE_NATURE_KEYS},
        '开仓次数(按订单算)': 0,
        '平仓次数(按订单算)': 0,
        '总交易轮次(开平算一次)': 0,
        '是否重点标签套利用户': None,
        # 'taker金额': 0.0,
        'taker金额占比': '0%',
        # 'maker金额': 0.0,
        'maker金额占比': '0%',
        '手续费总和': 0.0,
        '总盈亏': 0.0,
        # '刨除手续费总盈亏': 0.0,
        '总手续费': 0.0,
        '手续费占比': 'N/A',
        '分币对盈亏': '',
        '最大盈利': 0.0,
        '最大亏损': 0.0,
        '总成交金额': 0.0,
        '盈利/成交额': '0%',
        '胜率': '0%',
        '盈亏比': 0.0,
        '期望收益': 0.0,
        '夏普比率': 0.0,
        '平均杠杆': 0.0,
        # '最大持仓时长': '0 days 00:00:00',
        # '最短持仓时长': '0 days 00:00:00',
        '平均持仓时长': '0 days 00:00:00',
        '中位数持仓时长': '0 days 00:00:00',
        '众数持仓时长(聚合到min)': '0 days 00:00:00',
        '持仓时间小于5min的交易轮次占比': '0%',
        '最大连续盈利': 0,
        '最大连续亏损': 0,
        '日均交易次数': 0.0,
        # '周均交易次数': 0.0,
        # '月均交易次数': 0.0,
        '收益率年化标准差': 0.0,
        '平均仓位规模': 0.0,
        '平均交易间隔': 'NaT',
        # '统计指标': '无成交',
    }
    if df is None or df.shape[0] == 0:
        return empty

    d = df.copy()
    if userid is not None and "userid" in d.columns:
        d = d[d["userid"].astype(str) == str(userid)].copy()
    if d.shape[0] == 0:
        return empty

    positions = {'long': deque(), 'short': deque()}
    round_trip_count = {'long': 0, 'short': 0}
    in_position = {'long': False, 'short': False}
    holding_records = []

    d["ts"] = pd.to_datetime(d["tsText"])
    d = d.sort_values("ts").reset_index(drop=True)

    ts_min = d["ts"].min()
    ts_max = d["ts"].max()
    begin_date = ts_min.strftime("%Y-%m-%d") if pd.notna(ts_min) else None
    end_date = ts_max.strftime("%Y-%m-%d") if pd.notna(ts_max) else None

    for _, r in d.iterrows():
        ts, side, amt = r['ts'], r['buy_sell'], float(r['amount'])
        if side == '开多':
            if not in_position['long']:
                in_position['long'] = True
                positions['long'].append({'open_time': ts, 'amount': amt})
        elif side == '开空':
            if not in_position['short']:
                in_position['short'] = True
                positions['short'].append({'open_time': ts, 'amount': amt})
        elif side in ('平多', '平空'):
            key = 'long' if side == '平多' else 'short'
            remain = amt
            while remain > 0 and positions[key]:
                pos = positions[key][0]
                close_qty = min(remain, pos['amount'])
                holding_records.append({
                    'direction': key,
                    'open_time': pos['open_time'],
                    'close_time': ts,
                    'amount': close_qty,
                    'holding_seconds': (ts - pos['open_time']).total_seconds()
                })
                pos['amount'] -= close_qty
                remain -= close_qty
                if pos['amount'] == 0:
                    positions[key].popleft()
            if side == '平多':
                if not positions['long'] and in_position['long']:
                    round_trip_count['long'] += 1
                    in_position['long'] = False
            if side == '平空':
                if not positions['short'] and in_position['short']:
                    round_trip_count['short'] += 1
                    in_position['short'] = False

    holding_df = pd.DataFrame(holding_records)
    d["amt_"] = _ensure_amt(d).astype(float)
    if "fee" in d.columns:
        d["fee_"] = pd.to_numeric(d["fee"], errors="coerce").fillna(0.0)
    else:
        d["fee_"] = 0.0
    total_fee = round(d['fee_'].sum(), 2)

    trade_count = len(d)
    open_count = int(d['buy_sell'].isin(['开多', '开空']).sum())
    close_count = int(d['buy_sell'].isin(['平多', '平空']).sum())
    total_round_trips = round_trip_count['long'] + round_trip_count['short']
    pnl_df = d[d['profit_loss'] != 0]
    if pnl_df.shape[0] == 0:
        total_profit = 0.0
        max_profit = 0.0
        max_loss = 0.0
        win_rate = '0%'
        profit_loss_ratio = 0.0
        expect_profit = 0.0
        profit_trade_ratio = '0%'
        sharpe = 0.0
        symbol_profit_info = "\n"
    else:
        total_profit = round(float(pnl_df['profit_loss'].sum()), 2)
        max_profit = round(float(pnl_df['profit_loss'].max()), 2)
        max_loss = round(float(pnl_df['profit_loss'].min()), 2)
        win_rate = str(round((pnl_df['profit_loss'] > 0).mean() * 100, 1)) + '%'
        pos_mean = pnl_df[pnl_df['profit_loss'] > 0]['profit_loss'].mean()
        neg = pnl_df[pnl_df['profit_loss'] < 0]['profit_loss']
        if neg.shape[0] == 0 or pos_mean is None or np.isnan(pos_mean):
            profit_loss_ratio = 0.0
        else:
            profit_loss_ratio = round(float(pos_mean / abs(neg.mean())), 2)
        expect_profit = round(float(pnl_df['profit_loss'].mean()), 2)
        total_trade_amount = round(float(d['amt'].sum()), 2)
        profit_trade_ratio = str(round(total_profit / total_trade_amount * 100, 4)) + '%' if total_trade_amount else '0%'
        returns = pnl_df['profit_loss']
        std_r = returns.std()
        sharpe = round(float(returns.mean() / std_r * np.sqrt(len(returns))), 2) if std_r and std_r > 0 else 0.0
        symbol_profit = (
            d[d['profit_loss'] != 0]
            .groupby('symbol')['profit_loss']
            .sum()
            .sort_values(ascending=False)
        )
        symbol_profit_info = "\n"
        for symbol, profit in symbol_profit.items():
            symbol_profit_info += f"{symbol}:{round(float(profit), 2)}\n"

    pnl_ex_fee = round(float(total_profit) - float(total_fee), 2)
    fee_ratio_str = _fee_ratio_pct(total_fee, total_profit)

    total_trade_amount = round(float(d['amt'].sum()), 2)
    if pnl_df.shape[0] == 0:
        profit_trade_ratio = '0%'
    taker_amt = round(float(d['taker_amt'].sum()), 2)
    maker_amt = round(float(d['maker_amt'].sum()), 2)
    amt_sum = taker_amt + maker_amt
    if amt_sum == 0:
        taker_ratio = '0%'
        maker_ratio = '0%'
    else:
        taker_ratio = str(round(taker_amt / amt_sum * 100, 2)) + '%'
        maker_ratio = str(round(maker_amt / amt_sum * 100, 2)) + '%'

    if holding_df.shape[0] == 0:
        max_holding = min_holding = median_holding = mode_holding = weighted_avg_holding = '0 days 00:00:00'
        mode_ratio = '0%'
        holding_round_trips_ratio = '0%'
        max_win_streak = max_loss_streak = 0
    else:
        holding_df = holding_df.copy()
        holding_df['holding_minutes'] = holding_df['holding_seconds'] / 60
        wsum = (holding_df['holding_seconds'] * holding_df['amount']).sum()
        wamt = holding_df['amount'].sum()
        weighted_avg_holding = str(pd.Timedelta(seconds=float(wsum / wamt))) if wamt else '0 days 00:00:00'
        max_holding = str(pd.Timedelta(seconds=float(holding_df['holding_seconds'].max())))
        min_holding = str(pd.Timedelta(seconds=float(holding_df['holding_seconds'].min())))
        med = holding_df['holding_minutes'].median()
        median_holding = str(pd.Timedelta(minutes=float(med))) if pd.notna(med) else '0 days 00:00:00'
        mode_series = holding_df['holding_minutes'].round().mode()
        if mode_series.empty:
            mode_holding = '0 days 00:00:00'
            mode_ratio = '0%'
        else:
            mode_val = mode_series.iloc[0]
            mode_holding = str(pd.Timedelta(minutes=float(mode_val)))
            rounded_series = holding_df['holding_minutes'].round()
            mode_count = int((rounded_series == mode_val).sum())
            trt = total_round_trips if total_round_trips > 0 else 1
            mode_ratio = str(round(mode_count / trt * 100, 2)) + '%'
        profits = pnl_df['profit_loss'] if pnl_df.shape[0] else pd.Series(dtype=float)
        max_win_streak = max_consecutive(profits > 0) if len(profits) else 0
        max_loss_streak = max_consecutive(profits < 0) if len(profits) else 0
        sholding_count = (holding_df['holding_seconds'] < 300).sum()
        trt = total_round_trips if total_round_trips > 0 else 1
        holding_round_trips_ratio = str(round(float(sholding_count) / trt * 100, 2)) + '%'

    avg_leverage = round(float(d['multiple'].mean()), 2) if len(d) else 0.0

    d['date'] = d['ts'].dt.date
    d['week'] = d['ts'].dt.to_period('W')
    d['month'] = d['ts'].dt.to_period('M')
    daily_freq = round(float(d.groupby('date').size().mean()), 2) if len(d) else 0.0
    weekly_freq = round(float(d.groupby('week').size().mean()), 2) if len(d) else 0.0
    monthly_freq = round(float(d.groupby('month').size().mean()), 2) if len(d) else 0.0

    if pnl_df.shape[0] > 0:
        returns = pnl_df['profit_loss']
        returns_annualized_std = round(float(returns.std() * np.sqrt(len(returns))), 2)
    else:
        returns_annualized_std = 0.0

    avg_position_size = round(float(d['amount'].mean()), 2) if len(d) else 0.0
    d['time_diff'] = d['ts'].diff()
    avg_time_diff = str(d['time_diff'].mean())

    user_metrics_mess = ""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            user_metrics = compute_user_metrics(df, capital_mode="margin", rf=0.0, min_trades=30)
        if user_metrics:
            for i in user_metrics:
                for _, v in i.items():
                    parts = str(v).split(' ', 1)
                    if len(parts) >= 2:
                        user_metrics_mess += f"{parts[1]}:{parts[0]}\n"
                    else:
                        user_metrics_mess += f"{v}\n"
        else:
            user_metrics_mess = "数据不足30笔,结果没有参考性"
    except Exception:
        user_metrics_mess = "数据不足30笔,结果没有参考性"

    trade_nature = _summarize_trade_nature_df(d)

    return {
        '交易开始时间': begin_date,
        '交易结束时间': end_date,
        '成交笔数(按订单算)': trade_count,
        '交易性质': trade_nature,
        '开仓次数(按订单算)': open_count,
        '平仓次数(按订单算)': close_count,
        '总交易轮次(开平算一次)': total_round_trips,
        '是否重点标签套利用户': None,
        # 'taker金额': taker_amt,
        'taker金额占比': taker_ratio,
        # 'maker金额': maker_amt,
        'maker金额占比': maker_ratio,
        '手续费总和': total_fee,
        '总盈亏': total_profit if pnl_df.shape[0] else 0.0,
        # '刨除手续费总盈亏': pnl_ex_fee,
        '总手续费': total_fee,
        '手续费占比': fee_ratio_str,
        '分币对盈亏': symbol_profit_info if pnl_df.shape[0] else "\n",
        '最大盈利': max_profit if pnl_df.shape[0] else 0.0,
        '最大亏损': max_loss if pnl_df.shape[0] else 0.0,
        '总成交金额': total_trade_amount,
        '盈利/成交额': profit_trade_ratio if pnl_df.shape[0] else '0%',
        '胜率': win_rate if pnl_df.shape[0] else '0%',
        '盈亏比': profit_loss_ratio if pnl_df.shape[0] else 0.0,
        '期望收益': expect_profit if pnl_df.shape[0] else 0.0,
        '夏普比率': sharpe if pnl_df.shape[0] else 0.0,
        '平均杠杆': avg_leverage,
        # '最大持仓时长': max_holding,
        # '最短持仓时长': min_holding,
        '平均持仓时长': weighted_avg_holding,
        '中位数持仓时长': median_holding,
        '众数持仓时长(聚合到min)': mode_holding,
        '持仓时间小于5min的交易轮次占比': holding_round_trips_ratio,
        '最大连续盈利': max_win_streak,
        '最大连续亏损': max_loss_streak,
        '日均交易次数': daily_freq,
        # '周均交易次数': weekly_freq,
        # '月均交易次数': monthly_freq,
        '收益率年化标准差': returns_annualized_std,
        '平均仓位规模': avg_position_size,
        '平均交易间隔': avg_time_diff,
        # '统计指标': user_metrics_mess,
    }


STATS_KEYS_FOR_MERGE = [
    '成交笔数(按订单算)', '交易性质', '开仓次数(按订单算)', '平仓次数(按订单算)', '总交易轮次(开平算一次)',
    '是否重点标签套利用户',
    # 'taker金额', 'taker金额占比', 'maker金额', 'maker金额占比',
    'taker金额占比', 'maker金额占比',
    '手续费总和', '总盈亏',  # '刨除手续费总盈亏',
    '总手续费', '手续费占比',
    '分币对盈亏', '最大盈利', '最大亏损', '总成交金额', '盈利/成交额',
    '胜率', '盈亏比', '期望收益', '夏普比率', '平均杠杆',
    # '最大持仓时长', '最短持仓时长',
    '平均持仓时长', '中位数持仓时长', '众数持仓时长(聚合到min)',
    '持仓时间小于5min的交易轮次占比', '最大连续盈利', '最大连续亏损',
    '日均交易次数',  # '周均交易次数', '月均交易次数',
    '收益率年化标准差', '平均仓位规模', '平均交易间隔',  # '统计指标',
]

