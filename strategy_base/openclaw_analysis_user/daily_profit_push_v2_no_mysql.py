from __future__ import annotations

import asyncio
import gc
import json
import os
from datetime import datetime, date
from typing import Dict, Set, Tuple
from zoneinfo import ZoneInfo

import motor.motor_asyncio

from mongdb_order_stats import calc_trade_stats
from user_deal_analysis_on_demand import mongo_orders_to_dataframe


BJT = ZoneInfo("Asia/Shanghai")
CLOSE_SIDES = frozenset({"3", "4"})  # 3=平多, 4=平空

MONGO_URI = os.environ.get(
    "UPM_MONGO_URI",
    "mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/",
)
TG_CHAT_ID = os.environ.get("UPM_TG_CHAT_ID", "-5223829567")
PROFIT_THRESHOLD = float(os.environ.get("UPM_PROFIT_THRESHOLD", "1000"))
DEAL_ALERT_THRESHOLD = int(os.environ.get("UPM_DEAL_ALERT_THRESHOLD", "500"))
STRICT_10MIN_WINDOW = os.environ.get("UPM_STRICT_10MIN_WINDOW", "1") == "1"
STATE_DIR = os.environ.get("UPM_STATE_DIR", "./state_daily_profit_push_v2")

# 内存保护：系统内存使用率超过阈值则本轮降级/跳过，默认 70%
MAX_SYSTEM_MEM_RATIO = float(os.environ.get("UPM_MAX_SYSTEM_MEM_RATIO", "0.70"))
MEM_CHECK_EVERY_N_DEALS = int(os.environ.get("UPM_MEM_CHECK_EVERY_N_DEALS", "5000"))


def _read_meminfo() -> Dict[str, int]:
    info = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                p = line.split(":", 1)
                if len(p) != 2:
                    continue
                k = p[0].strip()
                rest = p[1].strip().split()
                if not rest:
                    continue
                info[k] = int(rest[0])  # kB
    except Exception:
        return {}
    return info


def get_system_mem_ratio() -> float:
    info = _read_meminfo()
    total = float(info.get("MemTotal", 0))
    avail = float(info.get("MemAvailable", 0))
    if total <= 0:
        return 0.0
    used = total - avail
    return max(0.0, min(1.0, used / total))


def _bjt_day_start_ts(now_bjt: datetime) -> int:
    day0 = datetime(now_bjt.year, now_bjt.month, now_bjt.day, 0, 0, 0, tzinfo=BJT)
    return int(day0.timestamp())


class DailyProfitPushV2NoMySQL(object):
    def __init__(self):
        self._toolbox_inst = None
        self._last_stat_date = None  # type: date
        self._today_seen_pushed = set()  # type: Set[str]
        self._deal_alerted_today = set()  # type: Set[str]
        self._full_push_done = False
        self._last_10m_minute = None  # type: str
        self._realized = {}  # type: Dict[str, float]
        self._trade_num = {}  # type: Dict[str, int]
        self._last_query_ts = 0  # type: int
        self._excluded_user_ids = set()  # type: Set[str]
        os.makedirs(STATE_DIR, exist_ok=True)

    def _get_toolbox(self):
        if self._toolbox_inst is None:
            from ToolBoxNew import ToolBox
            self._toolbox_inst = ToolBox()
        return self._toolbox_inst

    async def _refresh_excluded_ids(self):
        try:
            from exclude_user_ids import get_excluded_user_ids_async

            self._excluded_user_ids = await get_excluded_user_ids_async()
        except Exception as e:
            print("[WARN] 加载过滤用户ID失败: {}".format(e))
            self._excluded_user_ids = set()

    async def send_tg_chunks(self, text: str):
        if not TG_CHAT_ID:
            print("[WARN] 未配置 UPM_TG_CHAT_ID，跳过 TG 推送")
            return
        try:
            await self._get_toolbox().send_tg(TG_CHAT_ID, text)
        except Exception as e:
            print("[WARN] ToolBox.send_tg 失败: {}".format(e))

    def _reset_if_new_day(self, stat_date: date):
        if self._last_stat_date != stat_date:
            self._today_seen_pushed.clear()
            self._deal_alerted_today.clear()
            self._last_stat_date = stat_date
            self._full_push_done = False
            self._realized = {}
            self._trade_num = {}
            # 默认从最近10分钟开始，避免首轮扫全天导致内存风险
            self._last_query_ts = int(datetime.now(BJT).timestamp()) - 600
            self._load_state(stat_date)

    def _state_file(self, stat_date: date) -> str:
        return os.path.join(STATE_DIR, "{}.json".format(stat_date.strftime("%Y-%m-%d")))

    def _load_state(self, stat_date: date):
        fp = self._state_file(stat_date)
        if not os.path.exists(fp):
            return
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._last_query_ts = int(data.get("last_query_ts", self._last_query_ts))
            self._realized = {str(k): float(v) for k, v in data.get("realized", {}).items()}
            self._trade_num = {str(k): int(v) for k, v in data.get("trade_num", {}).items()}
            self._today_seen_pushed = set([str(x) for x in data.get("today_seen_pushed", [])])
            self._deal_alerted_today = set([str(x) for x in data.get("deal_alerted_today", [])])
            self._full_push_done = bool(data.get("full_push_done", False))
        except Exception as e:
            print("[WARN] 加载状态文件失败 {}: {}".format(fp, e))

    def _save_state(self, stat_date: date):
        fp = self._state_file(stat_date)
        tmp = fp + ".tmp"
        data = {
            "last_query_ts": int(self._last_query_ts),
            "realized": self._realized,
            "trade_num": self._trade_num,
            "today_seen_pushed": sorted(self._today_seen_pushed),
            "deal_alerted_today": sorted(self._deal_alerted_today),
            "full_push_done": bool(self._full_push_done),
        }
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            os.replace(tmp, fp)
        except Exception as e:
            print("[WARN] 保存状态文件失败 {}: {}".format(fp, e))

    async def _aggregate_increment(self, ts_from_exclusive: int, ts_to_inclusive: int) -> Tuple[int, int]:
        client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        col = client.exchange.real_contract_deal
        q = {"ts": {"$gt": int(ts_from_exclusive), "$lte": int(ts_to_inclusive)}}
        projection = {
            "_id": 0,
            "ts": 1,
            "takerUser": 1,
            "makerUser": 1,
            "takerBuyOrSell": 1,
            "makerBuyOrSell": 1,
            "takerProfitLoss": 1,
            "makerProfitLoss": 1,
        }
        cnt = 0
        max_ts_seen = int(ts_from_exclusive)
        try:
            # 无排序流式读取，避免 Mongo 侧排序占用过高
            cursor = col.find(q, projection=projection).batch_size(2000)
            async for deal in cursor:
                cnt += 1
                dts = int(deal.get("ts") or 0)
                if dts > max_ts_seen:
                    max_ts_seen = dts
                # 内存高水位保护：只做流式聚合，不缓存全量明细；若系统压力过大则提前结束本轮
                if MEM_CHECK_EVERY_N_DEALS > 0 and cnt % MEM_CHECK_EVERY_N_DEALS == 0:
                    ratio = get_system_mem_ratio()
                    if ratio >= MAX_SYSTEM_MEM_RATIO:
                        gc.collect()
                        ratio2 = get_system_mem_ratio()
                        if ratio2 >= MAX_SYSTEM_MEM_RATIO:
                            print("[WARN] 系统内存占用 {:.2%} 超阈值 {:.2%}，提前结束本轮聚合".format(
                                ratio2, MAX_SYSTEM_MEM_RATIO
                            ))
                            break

                # taker
                side_t = str(deal.get("takerBuyOrSell", ""))
                uid_t = str(deal.get("takerUser", ""))
                if uid_t and uid_t not in self._excluded_user_ids:
                    self._trade_num[uid_t] = self._trade_num.get(uid_t, 0) + 1
                    if side_t in CLOSE_SIDES:
                        pnl_t = float(deal.get("takerProfitLoss") or 0.0)
                        self._realized[uid_t] = self._realized.get(uid_t, 0.0) + pnl_t

                # maker
                side_m = str(deal.get("makerBuyOrSell", ""))
                uid_m = str(deal.get("makerUser", ""))
                if uid_m and uid_m not in self._excluded_user_ids:
                    self._trade_num[uid_m] = self._trade_num.get(uid_m, 0) + 1
                    if side_m in CLOSE_SIDES:
                        pnl_m = float(deal.get("makerProfitLoss") or 0.0)
                        self._realized[uid_m] = self._realized.get(uid_m, 0.0) + pnl_m
        finally:
            client.close()
        return cnt, max_ts_seen

    async def _build_added_user_analysis_lines(self, user_ids: Set[str], ts_max: int) -> Dict[str, str]:
        """
        对新增用户补充全量交易指标（从0到当前时刻）。
        返回: {uid: "格式化指标行"}
        """
        out = {}  # type: Dict[str, str]
        if not user_ids:
            return out
        client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        col = client.exchange.real_contract_deal
        try:
            for uid in sorted(user_ids):
                try:
                    df = await mongo_orders_to_dataframe(col, str(uid), 0, int(ts_max))
                    stats = calc_trade_stats(df, userid=str(uid))
                    line = (
                        "user={} 总盈亏:{} 刨除手续费总盈亏:{} 总手续费:{} 手续费占比:{} "
                        "成交笔数(按订单算):{} 交易性质:{} "
                        "开平仓次数:{} 日均交易次数:{} 胜率:{} 盈亏比:{} 持仓时间中位数:{}"
                    ).format(
                        uid,
                        stats.get("总盈亏", 0.0),
                        stats.get("刨除手续费总盈亏", 0.0),
                        stats.get("总手续费", stats.get("手续费总和", 0.0)),
                        stats.get("手续费占比", "N/A"),
                        stats.get("成交笔数(按订单算)", 0),
                        stats.get("交易性质", {}),
                        stats.get("总交易轮次(开平算一次)", 0),
                        stats.get("日均交易次数", 0.0),
                        stats.get("胜率", "0%"),
                        stats.get("盈亏比", 0.0),
                        stats.get("中位数持仓时长", "0 days 00:00:00"),
                    )
                    out[str(uid)] = line
                except Exception as e:
                    out[str(uid)] = "user={} 交易分析失败: {}".format(uid, e)
        finally:
            client.close()
        return out

    async def run_once(self, force_full_push: bool = False):
        now_bjt = datetime.now(BJT)
        stat_date = now_bjt.date()
        self._reset_if_new_day(stat_date)
        await self._refresh_excluded_ids()
        ex = self._excluded_user_ids
        for k in list(self._realized.keys()):
            if k in ex:
                del self._realized[k]
        for k in list(self._trade_num.keys()):
            if k in ex:
                del self._trade_num[k]
        if ex:
            self._today_seen_pushed -= ex
            self._deal_alerted_today -= ex

        mem_ratio = get_system_mem_ratio()
        if mem_ratio >= MAX_SYSTEM_MEM_RATIO:
            msg = "[内存保护] {} 系统内存占用 {:.2%} 超过阈值 {:.2%}，本轮跳过".format(
                now_bjt.strftime("%Y-%m-%d %H:%M:%S"), mem_ratio, MAX_SYSTEM_MEM_RATIO
            )
            print(msg)
            await self.send_tg_chunks(msg)
            return

        ts_now = int(now_bjt.timestamp())
        ts_floor = _bjt_day_start_ts(now_bjt)
        if STRICT_10MIN_WINDOW:
            ts_from = max(int(self._last_query_ts), ts_now - 600, ts_floor - 1)
        else:
            ts_from = max(int(self._last_query_ts), ts_floor - 1)
        deal_cnt, max_ts_seen = await self._aggregate_increment(ts_from, ts_now)
        self._last_query_ts = max(int(self._last_query_ts), int(max_ts_seen), int(ts_now))

        ex = self._excluded_user_ids
        current_set = set(
            [
                uid
                for uid, p in self._realized.items()
                if p > PROFIT_THRESHOLD and uid and uid not in ex
            ]
        )
        current_deal_set = set(
            [
                uid
                for uid, n in self._trade_num.items()
                if n > DEAL_ALERT_THRESHOLD and uid and uid not in ex
            ]
        )

        added = current_set - self._today_seen_pushed
        removed = self._today_seen_pushed - current_set

        if added or removed:
            lines = [
                "[日榜变更] {} 盈利>{:.0f}U".format(now_bjt.strftime("%Y-%m-%d %H:%M"), PROFIT_THRESHOLD),
                "本轮扫描成交条数: {} (增量区间: {} -> {})".format(
                    deal_cnt,
                    datetime.fromtimestamp(ts_from, tz=BJT).strftime("%H:%M:%S"),
                    datetime.fromtimestamp(ts_now, tz=BJT).strftime("%H:%M:%S"),
                ),
            ]
            if added:
                lines.append("新增({}): {}".format(len(added), ", ".join(sorted(added))))
                lines.append("")
                lines.append("新增用户交易分析（全量）:")
                detail_map = await self._build_added_user_analysis_lines(added, ts_now)
                for uid in sorted(added):
                    lines.append(detail_map.get(uid, "user={} 交易分析失败: 无结果".format(uid)))
            if removed:
                lines.append("删减({}): {}".format(len(removed), ", ".join(sorted(removed))))
            await self.send_tg_chunks("\n".join(lines))

        # 今日新增/删减状态机：推送过且未被删除的不重复推送
        self._today_seen_pushed = (self._today_seen_pushed - removed) | added

        # 高成交报警：当日内只报一次，忽略回落
        deal_new = current_deal_set - self._deal_alerted_today
        if deal_new:
            lines = [
                "[高成交报警] {} 当日成交笔数>{}".format(now_bjt.strftime("%Y-%m-%d %H:%M"), DEAL_ALERT_THRESHOLD),
                "新增报警人数: {}".format(len(deal_new)),
            ]
            for uid in sorted(deal_new):
                lines.append("user={} trade_num={}".format(uid, int(self._trade_num.get(uid, 0))))
            lines.append("")
            lines.append("高成交用户交易分析（全量）:")
            detail_map = await self._build_added_user_analysis_lines(deal_new, ts_now)
            for uid in sorted(deal_new):
                lines.append(detail_map.get(uid, "user={} 交易分析失败: 无结果".format(uid)))
            await self.send_tg_chunks("\n".join(lines))
            self._deal_alerted_today |= deal_new

        need_full = force_full_push
        if (not need_full) and now_bjt.hour == 23 and now_bjt.minute == 59 and (not self._full_push_done):
            need_full = True

        if need_full:
            all_profit_ids = sorted(current_set)
            all_deal_ids = sorted(current_deal_set)
            lines = [
                "[日终汇总] {}".format(stat_date),
                "盈利条件名单（>{:.0f}U）人数: {}".format(PROFIT_THRESHOLD, len(all_profit_ids)),
                ", ".join(all_profit_ids) if all_profit_ids else "(空)",
                "",
                "成交笔数条件名单（>{}）人数: {}".format(DEAL_ALERT_THRESHOLD, len(all_deal_ids)),
                ", ".join(all_deal_ids) if all_deal_ids else "(空)",
            ]
            await self.send_tg_chunks("\n".join(lines))
            self._full_push_done = True

        self._save_state(stat_date)

    async def run_forever(self):
        await self.run_once(force_full_push=False)
        while True:
            now_bjt = datetime.now(BJT)
            minute_key = now_bjt.strftime("%Y-%m-%d %H:%M")
            try:
                # 每10分钟轮询一次
                if now_bjt.minute % 10 == 0 and self._last_10m_minute != minute_key:
                    self._last_10m_minute = minute_key
                    await self.run_once(force_full_push=False)

                # 每日23:59强制全量推送
                if now_bjt.hour == 23 and now_bjt.minute == 59 and (not self._full_push_done):
                    await self.run_once(force_full_push=True)
            except Exception as e:
                err = "[ERROR] run loop failed: {}".format(e)
                print(err)
                await self.send_tg_chunks(err)
            # 轮询心跳每15秒，减少空转
            await asyncio.sleep(15)


def main():
    runner = DailyProfitPushV2NoMySQL()
    asyncio.run(runner.run_forever())


if __name__ == "__main__":
    main()

