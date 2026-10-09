# -*- coding: utf-8 -*-
"""
Z2 组用户合约成交盈亏汇总（全量 + 近 N 天，默认 30 天即近 1 个月）。

- 用户 ID：参考策略侧 ToolBoxNew.get_sim_user 的 fetch_tag_list 分页方式，仅拉取标签 Z2。
- 盈亏统计：与 daily_profit_push_v2_no_mysql 一致，使用 exchange.real_contract_deal +
  user_deal_analysis_on_demand.mongo_orders_to_dataframe + mongdb_order_stats.calc_trade_stats。

运行前请配置：
  1) 将本文件所在目录加入 Python 路径（通常在该目录下执行即可）。
  2) WEBSEA_REPO_ROOT：包含 utils、crypto_center 等包的工程根（与策略环境一致）。
  3) WEBSEA_ACCOUNT_CONFIG：account.config.json 路径（与 ToolBoxNew.load_account 相同结构）。
  4) UPM_MONGO_URI：Mongo 连接串（可选，默认与 daily_profit_push_v2_no_mysql 相同）。

可选：不拉 API 时提供 --user-ids-file（每行一个 uid）或环境变量 Z2_USER_IDS（逗号分隔）。

兼容 Python 3.9.19。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import motor.motor_asyncio

BJT = ZoneInfo("Asia/Shanghai")

# 与 daily_profit_push_v2_no_mysql 一致
MONGO_URI = os.environ.get(
    "UPM_MONGO_URI",
    "mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/",
)


def _script_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _ensure_code_import_path() -> None:
    d = _script_dir()
    if d not in sys.path:
        sys.path.insert(0, d)


def _ensure_websea_repo_path(explicit: Optional[str]) -> None:
    root = (explicit or os.environ.get("WEBSEA_REPO_ROOT", "")).strip()
    if root and os.path.isdir(root) and root not in sys.path:
        sys.path.insert(0, root)


def load_risk_api_keys(account_config_path: str) -> Tuple[str, str]:
    with open(account_config_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    api_key_list: Dict[str, List[str]] = {}
    for _k, v in data.get("websea", {}).items():
        desc = v.get("description", "") or ""
        if "测试" in desc:
            continue
        uid = v["uid"].split(",")[0] if "," in v.get("uid", "") else v.get("uid", "")
        if uid == "":
            uid = "risk"
        api_key_list[uid] = [v["apikey"], v["secret"]]
    if "risk" not in api_key_list:
        raise KeyError("account.config.json 中未找到非测试账户映射到 risk 的项")
    pair = api_key_list["risk"]
    return str(pair[0]), str(pair[1])


async def fetch_user_ids_by_tag(
    tag: str,
    page_sleep_s: float,
) -> List[str]:
    """
    与 ToolBoxNew.get_sim_user 相同的数据源与分页方式，只请求单个 tag（如 Z2）。
    """
    _ensure_websea_repo_path(None)
    from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract

    cfg_path = os.environ.get(
        "WEBSEA_ACCOUNT_CONFIG",
        "/home/ubuntu/CCGo/resources/account.config.json",
    )
    ak, sk = load_risk_api_keys(cfg_path)
    rest = Contract(ak, sk, dev=False)
    rest.DEBUG = False

    seen: set[str] = set()
    out: List[str] = []

    first = await rest.fetch_tag_list(tag=tag, page=1, page_size=1000)
    pager = first.get("pager") or {}
    total_page = int(pager.get("total_page", 1))

    def consume(rows: Any) -> None:
        if not rows:
            return
        for row in rows:
            if not isinstance(row, dict):
                continue
            uid = row.get("user_id")
            if uid is None:
                continue
            s = str(uid)
            if s not in seen:
                seen.add(s)
                out.append(s)

    consume(first.get("data"))

    for page in range(2, total_page + 1):
        await asyncio.sleep(page_sleep_s)
        data = await rest.fetch_tag_list(tag=tag, page=page, page_size=1000)
        consume(data.get("data"))

    out.sort()
    return out


def parse_user_ids_from_file(path: str) -> List[str]:
    ids: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            ids.append(s)
    return sorted(set(ids))


def parse_user_ids_from_env() -> List[str]:
    raw = os.environ.get("Z2_USER_IDS", "").strip()
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return sorted(set(parts))


def stats_to_row(stats: Dict[str, Any]) -> Dict[str, Any]:
    """提取输出字段（与 calc_trade_stats 中文键一致）。"""
    fee = stats.get("总手续费", stats.get("手续费总和", 0.0))
    return {
        "总盈亏": stats.get("总盈亏", 0.0),
        "总手续费": fee,
        "胜率": stats.get("胜率", "0%"),
        "盈亏比": stats.get("盈亏比", 0.0),
        "持仓时间中位数": stats.get("中位数持仓时长", "0 days 00:00:00"),
        "交易笔数": stats.get("成交笔数(按订单算)", 0),
        "交易性质": stats.get("交易性质", {}),
    }


def format_block(uid: str, label: str, row: Dict[str, Any]) -> str:
    return (
        "  [{label}] 总盈亏={pnl} 手续费={fee} 胜率={wr} 盈亏比={plr} "
        "持仓时间中位数={hold} 交易笔数={n} 交易性质={tn}"
    ).format(
        label=label,
        pnl=row["总盈亏"],
        fee=row["总手续费"],
        wr=row["胜率"],
        plr=row["盈亏比"],
        hold=row["持仓时间中位数"],
        n=row["交易笔数"],
        tn=row.get("交易性质", {}),
    )


def format_recent_window_label(recent_days: float, recent_seconds: int) -> str:
    """控制台展示用：默认 30 天显示为「近1个月」，避免「近2592000秒」等秒数文案。"""
    if abs(float(recent_days) - 30.0) < 1e-6:
        return "近1个月"
    if recent_seconds > 0 and recent_seconds % 86400 == 0:
        d = recent_seconds // 86400
        if d == 30:
            return "近1个月"
        if d == 14:
            return "近2周"
        if d == 7:
            return "近1周"
        if d == 1:
            return "近1天"
        return "近{}天".format(d)
    return "近{}秒".format(recent_seconds)


async def run_report(
    user_ids: Sequence[str],
    mongo_uri: str,
    ts_max: int,
    recent_seconds: int,
    recent_label: str,
    json_path: Optional[str],
) -> List[Dict[str, Any]]:
    _ensure_code_import_path()
    from mongdb_order_stats import calc_trade_stats
    from user_deal_analysis_on_demand import mongo_orders_to_dataframe

    ts_recent_min = max(0, ts_max - recent_seconds)
    client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
    col = client.exchange.real_contract_deal
    results: List[Dict[str, Any]] = []

    try:
        for uid in user_ids:
            uid_s = str(uid)
            df_all = await mongo_orders_to_dataframe(col, uid_s, 0, ts_max)
            st_all = calc_trade_stats(df_all, userid=uid_s)
            df_recent = await mongo_orders_to_dataframe(col, uid_s, ts_recent_min, ts_max)
            st_recent = calc_trade_stats(df_recent, userid=uid_s)

            row_all = stats_to_row(st_all)
            row_recent = stats_to_row(st_recent)
            rec = {
                "user_id": uid_s,
                "ts_max": ts_max,
                "recent_window_sec": recent_seconds,
                "recent_window_label": recent_label,
                "recent_ts_min": ts_recent_min,
                "overall": row_all,
                "recent": row_recent,
            }
            results.append(rec)

            print("user_id={}".format(uid_s))
            print(format_block(uid_s, "整体", row_all))
            print(format_block(uid_s, recent_label, row_recent))
            print("")
    finally:
        client.close()

    if json_path:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print("已写入 JSON: {}".format(json_path))

    return results


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Z2 组（或指定 tag）用户盈亏汇总")
    p.add_argument(
        "--tag",
        default=os.environ.get("SIM_USER_TAG", "Z2"),
        help="fetch_tag_list 的标签，默认 Z2",
    )
    p.add_argument(
        "--user-ids-file",
        default="",
        help="每行一个 uid，提供则不从 API 拉取标签用户",
    )
    p.add_argument(
        "--mongo-uri",
        default=MONGO_URI,
        help="Mongo URI，默认读环境变量 UPM_MONGO_URI",
    )
    p.add_argument(
        "--recent-days",
        type=float,
        default=30.0,
        help="近 N 天（按 86400 秒/天换算），默认 30（展示为近1个月）",
    )
    p.add_argument(
        "--websea-root",
        default="",
        help="含 crypto_center 的工程根目录；可替代环境变量 WEBSEA_REPO_ROOT",
    )
    p.add_argument(
        "--page-sleep",
        type=float,
        default=0.5,
        help="标签分页请求间隔（秒）",
    )
    p.add_argument(
        "--json-out",
        default="",
        help="将结果写入该 JSON 文件",
    )
    return p


async def async_main(args: argparse.Namespace) -> None:
    _ensure_websea_repo_path(args.websea_root or None)

    ids_file = (args.user_ids_file or "").strip()
    if ids_file:
        user_ids = parse_user_ids_from_file(ids_file)
    else:
        env_ids = parse_user_ids_from_env()
        if env_ids:
            user_ids = env_ids
        else:
            user_ids = await fetch_user_ids_by_tag(
                tag=str(args.tag),
                page_sleep_s=float(args.page_sleep),
            )

    if not user_ids:
        print("未得到任何用户 ID（检查标签、--user-ids-file 或 Z2_USER_IDS）")
        return

    now_bjt = datetime.now(BJT)
    ts_max = int(now_bjt.timestamp())
    recent_seconds = int(float(args.recent_days) * 86400.0)
    recent_label = format_recent_window_label(float(args.recent_days), recent_seconds)

    print(
        "共 {} 个用户 | Mongo={} | 统计截止(北京时间)={} | 近窗={} ({})\n".format(
            len(user_ids),
            args.mongo_uri.split("@")[-1] if "@" in args.mongo_uri else args.mongo_uri,
            now_bjt.strftime("%Y-%m-%d %H:%M:%S"),
            args.recent_days,
            recent_label,
        )
    )

    json_out = (args.json_out or "").strip() or None
    await run_report(
        user_ids=user_ids,
        mongo_uri=args.mongo_uri,
        ts_max=ts_max,
        recent_seconds=recent_seconds,
        recent_label=recent_label,
        json_path=json_out,
    )


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
