"""统计 XAU 资管账户历史平仓盈亏（金额 + 相对期初资金比例）。

口径对齐 xau_account_monitor.py：
- 数据源：MetaAPI get_deals_by_time_range（近 3650 天）
- 统计区间：第一笔入金 ~ 当前时间
- 仅统计平仓腿 DEAL_ENTRY_OUT（不含出入金）
- 单笔盈亏 = profit + commission + swap
- 单笔盈亏比例% = 单笔盈亏 / 期初资金 * 100
  期初资金 = max(历史入金合计, peak_equity_init)
- 多单/空单：平仓腿 DEAL_TYPE_SELL=平多，DEAL_TYPE_BUY=平空；拆分盈/亏笔数与各自胜率
- 盈亏比 = 平均盈利 / |平均亏损|；并输出平均每笔盈利/亏损金额

运行: py xau_trade_pnl_stats.py
依赖: pip install metaapi-cloud-sdk
"""
from __future__ import annotations

import asyncio
import statistics
import sys
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

from metaapi_cloud_sdk import MetaApi
from metaapi_cloud_sdk.logger import NativeLogger

NativeLogger.warning = lambda self, msg, *args, **kwargs: None

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from xau_account_monitor import ACCOUNTS  # noqa: E402

BJT = timezone(timedelta(hours=8))


def _as_dict(deal) -> dict:
    if isinstance(deal, dict):
        return deal
    return {k: getattr(deal, k) for k in (
        "id", "type", "entryType", "symbol", "time", "brokerTime",
        "price", "volume", "profit", "commission", "swap", "comment",
    ) if hasattr(deal, k)}


def _deal_pnl(d: dict) -> float:
    return float(d.get("profit") or 0) + float(d.get("commission") or 0) + float(d.get("swap") or 0)


def _parse_deal_time(d: dict) -> Optional[datetime]:
    raw = d.get("time") or d.get("brokerTime")
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    s = str(raw).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        dt = None
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s[:26], fmt)
                break
            except ValueError:
                continue
        if dt is None:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _fmt_dt(dt: Optional[datetime]) -> str:
    if dt is None:
        return "-"
    return dt.astimezone(BJT).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_range(start: Optional[datetime], end: Optional[datetime]) -> str:
    if start is None or end is None:
        return f"{_fmt_dt(start)} ~ {_fmt_dt(end)}（北京时间）"
    days = max(0.0, (end - start).total_seconds() / 86400.0)
    return f"{_fmt_dt(start)} ~ {_fmt_dt(end)}（北京时间，共{days:.1f}天）"


def _sum_deposits(deals: Iterable[dict]) -> float:
    return sum(
        float(d.get("profit") or 0)
        for d in deals
        if d.get("type") == "DEAL_TYPE_BALANCE" and float(d.get("profit") or 0) > 0
    )


def _first_deposit_time(deals: Sequence[dict]) -> Optional[datetime]:
    times: List[datetime] = []
    for d in deals:
        if d.get("type") != "DEAL_TYPE_BALANCE":
            continue
        if float(d.get("profit") or 0) <= 0:
            continue
        dt = _parse_deal_time(d)
        if dt is not None:
            times.append(dt)
    return min(times) if times else None


def _close_side(d: dict) -> Optional[str]:
    """平仓腿方向：SELL=平多(多单)，BUY=平空(空单)。"""
    t = str(d.get("type") or "").upper()
    if "SELL" in t:
        return "long"
    if "BUY" in t:
        return "short"
    return None


def _mean(xs: Sequence[float]) -> float:
    return float(statistics.mean(xs)) if xs else 0.0


def _median(xs: Sequence[float]) -> float:
    return float(statistics.median(xs)) if xs else 0.0


def _max_streak(flags: Sequence[bool]) -> int:
    best = cur = 0
    for f in flags:
        if f:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def _fmt_block(title: str, amounts: List[float], ratios: List[float], *, loss: bool = False) -> str:
    n = len(amounts)
    if not n:
        return f"{title}\n  笔数=0"
    # 亏损侧“最大值”取亏损幅度最大（最负金额 / 最负比例）
    if loss:
        max_amt = min(amounts)
        max_ratio = min(ratios)
        max_label = "最大亏损"
    else:
        max_amt = max(amounts)
        max_ratio = max(ratios)
        max_label = "最大盈利"
    return "\n".join([
        title,
        f"  笔数={n}",
        f"  金额  均值={_mean(amounts):.4f}  中位数={_median(amounts):.4f}  {max_label}={max_amt:.4f}",
        f"  比例% 均值={_mean(ratios):.4f}  中位数={_median(ratios):.4f}  {max_label}={max_ratio:.4f}",
    ])


def _side_stats(pnls: Sequence[float], sides: Sequence[Optional[str]], side: str) -> dict:
    xs = [p for p, s in zip(pnls, sides) if s == side]
    n = len(xs)
    win_n = sum(1 for p in xs if p > 0)
    loss_n = sum(1 for p in xs if p < 0)
    win_rate = (win_n / n * 100.0) if n else 0.0
    return {"n": n, "win_n": win_n, "loss_n": loss_n, "win_rate": win_rate}


def _trade_metrics(pnls: Sequence[float], sides: Sequence[Optional[str]]) -> dict:
    n = len(pnls)
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    win_rate = (len(wins) / n * 100.0) if n else 0.0
    avg_win = _mean(wins)
    avg_loss = _mean(losses)
    pl_ratio = (avg_win / abs(avg_loss)) if losses and abs(avg_loss) > 1e-12 else 0.0
    long_s = _side_stats(pnls, sides, "long")
    short_s = _side_stats(pnls, sides, "short")
    max_win = max(wins) if wins else 0.0
    max_loss = min(losses) if losses else 0.0
    max_win_streak = _max_streak([p > 0 for p in pnls])
    max_loss_streak = _max_streak([p < 0 for p in pnls])
    return {
        "win_rate": win_rate,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "long": long_s,
        "short": short_s,
        "pl_ratio": pl_ratio,
        "max_win": max_win,
        "max_loss": max_loss,
        "max_win_streak": max_win_streak,
        "max_loss_streak": max_loss_streak,
    }


def _fmt_core_metrics(m: dict) -> List[str]:
    lo, sh = m["long"], m["short"]
    return [
        f"胜率={m['win_rate']:.2f}%  "
        f"多单={lo['n']}(盈{lo['win_n']}/亏{lo['loss_n']},胜率{lo['win_rate']:.2f}%)  "
        f"空单={sh['n']}(盈{sh['win_n']}/亏{sh['loss_n']},胜率{sh['win_rate']:.2f}%)",
        f"平均每笔盈利={m['avg_win']:.4f}  平均每笔亏损={m['avg_loss']:.4f}  盈亏比={m['pl_ratio']:.4f}",
        f"最大单笔盈利={m['max_win']:.4f}  最大单笔亏损={m['max_loss']:.4f}",
        f"最多连续盈利={m['max_win_streak']}  最多连续亏损={m['max_loss_streak']}",
    ]


async def load_close_pnls(
    cfg: dict,
) -> Tuple[str, float, Optional[datetime], datetime, List[float], List[float], List[Optional[str]]]:
    login = str(cfg["login"])
    peak_init = float(cfg.get("peak_equity_init") or 20000.0)
    api = MetaApi(cfg["token"])
    account = await api.metatrader_account_api.get_account(cfg["account_id"])
    if account.state != "DEPLOYED":
        await account.deploy()
    await account.wait_connected()

    rpc = account.get_rpc_connection()
    await rpc.connect()
    await rpc.wait_synchronized()
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=3650)
        result = await rpc.get_deals_by_time_range(start_time=start, end_time=end)
        deals = result.get("deals", result) if isinstance(result, dict) else (result or [])
        deals = [_as_dict(d) for d in deals]

        deposit_sum = _sum_deposits(deals)
        basis = max(deposit_sum, peak_init) if abs(deposit_sum) > 1e-8 else peak_init
        if abs(basis) < 1e-8:
            basis = peak_init

        first_dep = _first_deposit_time(deals)
        range_start = first_dep or start

        closes: List[Tuple[datetime, dict]] = []
        for d in deals:
            if d.get("type") == "DEAL_TYPE_BALANCE":
                continue
            if d.get("entryType") != "DEAL_ENTRY_OUT":
                continue
            dt = _parse_deal_time(d)
            if dt is None:
                continue
            if dt < range_start or dt > end:
                continue
            closes.append((dt, d))
        closes.sort(key=lambda x: x[0])

        pnls = [_deal_pnl(d) for _, d in closes]
        ratios = [(p / basis * 100.0) for p in pnls]
        sides = [_close_side(d) for _, d in closes]
        return login, basis, first_dep, end, pnls, ratios, sides
    finally:
        await rpc.close()


def summarize(
    login: str,
    basis: float,
    first_dep: Optional[datetime],
    end: datetime,
    pnls: List[float],
    ratios: List[float],
    sides: List[Optional[str]],
) -> str:
    wins_a = [p for p in pnls if p > 0]
    wins_r = [r for p, r in zip(pnls, ratios) if p > 0]
    loss_a = [p for p in pnls if p < 0]
    loss_r = [r for p, r in zip(pnls, ratios) if p < 0]
    zero_n = sum(1 for p in pnls if abs(p) < 1e-12)
    m = _trade_metrics(pnls, sides)

    total_pnl = sum(pnls)
    ret_pct = total_pnl / basis * 100.0 if abs(basis) > 1e-8 else 0.0
    lines = [
        f"======== 账户 {login} ========",
        f"统计区间(第一笔入金~当前)={_fmt_range(first_dep, end)}",
        f"期初资金(统计基数)={basis:.2f}",
        f"交易总笔数={len(pnls)}  盈利={len(wins_a)}  亏损={len(loss_a)}  持平={zero_n}",
        *_fmt_core_metrics(m),
        f"全部平仓盈亏合计={total_pnl:.4f}",
        f"整体收益率={ret_pct:.4f}%",
        _fmt_block("【盈利单】", wins_a, wins_r, loss=False),
        _fmt_block("【亏损单】", loss_a, loss_r, loss=True),
    ]
    return "\n".join(lines)


async def main() -> None:
    print(f"共统计 {len(ACCOUNTS)} 个账户历史平仓…")
    results = await asyncio.gather(
        *(load_close_pnls(cfg) for cfg in ACCOUNTS),
        return_exceptions=True,
    )
    all_pnls: List[float] = []
    all_ratios: List[float] = []
    all_sides: List[Optional[str]] = []
    basis_sum = 0.0
    range_starts: List[datetime] = []
    range_ends: List[datetime] = []
    for cfg, result in zip(ACCOUNTS, results):
        if isinstance(result, Exception):
            print(f"[{cfg['login']}] 失败: {result}")
            continue
        login, basis, first_dep, end, pnls, ratios, sides = result
        print(summarize(login, basis, first_dep, end, pnls, ratios, sides))
        print()
        all_pnls.extend(pnls)
        all_ratios.extend(ratios)
        all_sides.extend(sides)
        basis_sum += basis
        if first_dep is not None:
            range_starts.append(first_dep)
        range_ends.append(end)

    if all_pnls:
        wins_a = [p for p in all_pnls if p > 0]
        wins_r = [r for p, r in zip(all_pnls, all_ratios) if p > 0]
        loss_a = [p for p in all_pnls if p < 0]
        loss_r = [r for p, r in zip(all_pnls, all_ratios) if p < 0]
        m = _trade_metrics(all_pnls, all_sides)
        total_pnl = sum(all_pnls)
        ret_pct = total_pnl / basis_sum * 100.0 if abs(basis_sum) > 1e-8 else 0.0
        agg_start = min(range_starts) if range_starts else None
        agg_end = max(range_ends) if range_ends else None
        print("======== 全部账户合计 ========")
        print(f"统计区间(最早入金~当前)={_fmt_range(agg_start, agg_end)}")
        print(f"交易总笔数={len(all_pnls)}  盈利={len(wins_a)}  亏损={len(loss_a)}")
        print("\n".join(_fmt_core_metrics(m)))
        print(f"全部平仓盈亏合计={total_pnl:.4f}")
        print(f"整体收益率={ret_pct:.4f}%（基数合计={basis_sum:.2f}）")
        print(_fmt_block("【盈利单】", wins_a, wins_r, loss=False))
        print(_fmt_block("【亏损单】", loss_a, loss_r, loss=True))


if __name__ == "__main__":
    asyncio.run(main())

