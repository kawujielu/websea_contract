# -*- coding: utf-8 -*-
"""
user_profit_monitor 日榜聚合：daily_profit_user_rank / daily_deal_rank / daily_median_hold_lt10m + Telegram。
Telegram 使用 ToolBoxNew.ToolBox.send_tg（与策略目录下 ToolBoxNew.py 一致）。
"""
import os
import sys
import json
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

import mysql.connector
import motor.motor_asyncio
import pandas as pd

_STRATEGY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _STRATEGY_ROOT not in sys.path:
    sys.path.insert(0, _STRATEGY_ROOT)

from template.template_timer import TemplateTimer, CronTrigger

from mongdb_order_stats import holding_records_dataframe
from exclude_user_ids import get_excluded_user_ids_async

BJT = ZoneInfo('Asia/Shanghai')
BUY_SELL_SIDE = {'1': '开多', '2': '开空', '3': '平多', '4': '平空'}
CLOSE_SIDES = frozenset({'平多', '平空'})

MONGO_URI = os.environ.get(
    'UPM_MONGO_URI',
    'mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/', #'mongodb://root:GD0wBplOGwxQTaUI@10.0.96.71:27020/',
)
MYSQL_CFG = dict(
    host=os.environ.get('UPM_MYSQL_HOST', '10.0.208.249'),
    user=os.environ.get('UPM_MYSQL_USER', 'lh_sky'),
    password=os.environ.get('UPM_MYSQL_PASSWORD', 'sky_123456'),
    database=os.environ.get('UPM_MYSQL_DB', 'contract_dws'),
    port=int(os.environ.get('UPM_MYSQL_PORT', '33306')),
)

PROFIT_THRESHOLD = float(os.environ.get('UPM_PROFIT_THRESHOLD', '1000'))
TOP_PROFIT_RANK = int(os.environ.get('UPM_TOP_PROFIT_RANK', '10'))
TOP_TRADE_RANK = int(os.environ.get('UPM_TOP_TRADE_RANK', '10'))
TRADE_NUM_THRESHOLD = int(os.environ.get('UPM_TRADE_NUM_THRESHOLD', '100'))
MINUTES = int(os.environ.get('UPM_CRON_MINUTES', '10'))
PAIR_LOOKBACK_DAYS = int(os.environ.get('UPM_PAIR_LOOKBACK_DAYS', '14'))
HOLD_MEDIAN_MAX_SEC = int(os.environ.get('UPM_HOLD_MEDIAN_MAX_SEC', '600'))
MIN_CLOSE_SEGMENTS = int(os.environ.get('UPM_MIN_CLOSE_SEGMENTS', '3'))

TG_CHAT_ID = os.environ.get('UPM_TG_CHAT_ID', '')


def expand_deal_legs(deal) -> list:
    ts = int(deal['ts'])
    dt_naive = datetime.fromtimestamp(ts, tz=BJT).replace(tzinfo=None)
    symbol = deal['symbol']
    base_amt = float(deal['amount'])
    fv_t = float(deal['takerFaceValue'])
    fv_m = float(deal['makerFaceValue'])
    uid_t = str(deal['takerUser'])
    uid_m = str(deal['makerUser'])
    return [
        {
            'user_id': uid_t,
            'ts': dt_naive,
            'ts_i': ts,
            'symbol': symbol,
            'buy_sell': BUY_SELL_SIDE[str(deal['takerBuyOrSell'])],
            'profit_loss': float(deal['takerProfitLoss'] or 0),
            'amount': base_amt * fv_t,
            'deal_is_protected': int(deal.get('takerIsProtected') or 0),
            'deal_sub_id': int(deal.get('takerSubId') or 0),
        },
        {
            'user_id': uid_m,
            'ts': dt_naive,
            'ts_i': ts,
            'symbol': symbol,
            'buy_sell': BUY_SELL_SIDE[str(deal['makerBuyOrSell'])],
            'profit_loss': float(deal['makerProfitLoss'] or 0),
            'amount': base_amt * fv_m,
            'deal_is_protected': int(deal.get('makerIsProtected') or 0),
            'deal_sub_id': int(deal.get('makerSubId') or 0),
        },
    ]


def top_n_cutoff_users(pairs, n, key_fn):
    if not pairs:
        return set()
    pairs = sorted(pairs, key=lambda x: -key_fn(x))
    if len(pairs) <= n:
        return {p[0] for p in pairs}
    cutoff = key_fn(pairs[n - 1])
    return {p[0] for p in pairs if key_fn(p) >= cutoff}


class UserProfitMonitorDaily(TemplateTimer):
    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self._toolbox_inst = None
        self._last_stat_date = None
        self._violation_alerted = set()
        self._profitable_alerted = set()

    def _get_toolbox(self):
        if self._toolbox_inst is None:
            from ToolBoxNew import ToolBox
            self._toolbox_inst = ToolBox()
        return self._toolbox_inst

    async def on_first(self):
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        log_file = f"log/{script_name}.log"
        self.log.add(log_file, rotation="100 MB", retention=10)
        await self.create_tables()
        self.log.info('user_profit_monitor_daily 初始化完成')
        await self.main()

    async def on_timer(self):
        self.schedule.add_job(self.main, CronTrigger(minute=f'*/{MINUTES}'))

    def connect_mysql(self):
        self.db = mysql.connector.connect(**MYSQL_CFG)
        self.cursor = self.db.cursor()

    def close_mysql(self):
        try:
            self.cursor.close()
            self.db.close()
        except Exception:
            pass

    async def create_tables(self):
        self.connect_mysql()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS `daily_profit_user_rank` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `stat_date` DATE NOT NULL COMMENT '北京日期',
                `user_id` VARCHAR(36) NOT NULL COMMENT '用户id',
                `realized_profit` DOUBLE NOT NULL COMMENT '当日已实现盈亏合计',
                `symbol_profit` JSON NOT NULL COMMENT '分交易对盈利',
                `symbol_trade_count` JSON NOT NULL COMMENT '分交易对平仓笔数',
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `uk_date_user` (`stat_date`, `user_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='日榜盈利用户';
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS `daily_deal_rank` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `stat_date` DATE NOT NULL,
                `user_id` VARCHAR(36) NOT NULL,
                `trade_num` BIGINT NOT NULL COMMENT '用户侧成交条数',
                `open_close_num` INT NOT NULL COMMENT '当日平仓腿条数',
                `symbol_realized_profit` JSON NOT NULL,
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `uk_dd_date_user` (`stat_date`, `user_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS `daily_median_hold_lt10m` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `stat_date` DATE NOT NULL,
                `user_id` VARCHAR(36) NOT NULL,
                `open_close_count` INT NOT NULL COMMENT '当日平仓闭环片段数',
                `symbols_traded` JSON NOT NULL,
                `holding_median_seconds` DOUBLE NOT NULL,
                `holding_avg_seconds` DOUBLE NOT NULL,
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `uk_hold_date_user` (`stat_date`, `user_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        self.db.commit()
        self.close_mysql()

    async def _load_deals_from_mongo(self, ts_min: int, ts_max: int | None = None):
        client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        col = client.exchange.real_contract_deal
        q = {'ts': {'$gte': ts_min}}
        if ts_max is not None:
            q['ts']['$lte'] = ts_max
        cursor = col.find(q).sort('dealId', 1)
        legs_all = []
        async for deal in cursor:
            legs_all.extend(expand_deal_legs(deal))
        client.close()
        return legs_all

    async def send_tg_chunks(self, text: str):
        """与 ToolBoxNew.ToolBox.send_tg 相同：按行累加，超 4000 字分片发。"""
        if not TG_CHAT_ID:
            self.log.warning('未配置 UPM_TG_CHAT_ID，跳过 TG')
            return
        try:
            await self._get_toolbox().send_tg(TG_CHAT_ID, text)
        except Exception as e:
            self.log.warning(f'ToolBox.send_tg 失败: {e}')

    async def main(self):
        now_bjt = datetime.now(BJT)
        stat_date: date = now_bjt.date()
        if self._last_stat_date != stat_date:
            self._violation_alerted.clear()
            self._profitable_alerted.clear()
            self._last_stat_date = stat_date

        pair_lo = int(
            (datetime(stat_date.year, stat_date.month, stat_date.day, 0, 0, 0, tzinfo=BJT)
             - timedelta(days=PAIR_LOOKBACK_DAYS)).timestamp()
        )

        self.log.info(f'拉取 Mongo ts>={pair_lo} 用于配对, 当日切片 {stat_date}')
        legs = await self._load_deals_from_mongo(pair_lo, None)
        excluded = await get_excluded_user_ids_async()
        legs = [x for x in legs if str(x['user_id']) not in excluded]

        legs_today = [x for x in legs if x['ts'].date() == stat_date]
        users_today = {x['user_id'] for x in legs_today}

        trade_num = defaultdict(int)
        close_count = defaultdict(int)
        realized = defaultdict(float)
        sym_p = defaultdict(lambda: defaultdict(float))
        sym_c = defaultdict(lambda: defaultdict(int))

        for x in legs_today:
            u = x['user_id']
            trade_num[u] += 1
            if x['buy_sell'] in CLOSE_SIDES:
                close_count[u] += 1
                p = x['profit_loss']
                realized[u] += p
                sym_p[u][x['symbol']] += p
                sym_c[u][x['symbol']] += 1

        rank_pairs = list(realized.items())

        rank_by_user = {}
        sorted_pairs = sorted(rank_pairs, key=lambda x: -x[1])
        last_p = None
        r = 0
        for idx, (u, p) in enumerate(sorted_pairs):
            if last_p is None or p != last_p:
                r = idx + 1
                last_p = p
            rank_by_user[u] = r

        rank_rows = []
        for u, p in realized.items():
            if p <= 0:
                continue
            if p > PROFIT_THRESHOLD or rank_by_user.get(u, 999999) <= TOP_PROFIT_RANK:
                sp = {k: round(v, 8) for k, v in sym_p[u].items()}
                sc = {k: int(v) for k, v in sym_c[u].items()}
                rank_rows.append((stat_date, u, float(p), json.dumps(sp, ensure_ascii=False), json.dumps(sc, ensure_ascii=False)))

        tn_pairs = list(trade_num.items())
        top_trade_users = top_n_cutoff_users(tn_pairs, TOP_TRADE_RANK, lambda t: t[1])
        deal_users = set(top_trade_users) | {u for u, n in trade_num.items() if n > TRADE_NUM_THRESHOLD}
        deal_rows = []
        for u in deal_users:
            sp = {k: round(v, 8) for k, v in sym_p[u].items()}
            deal_rows.append((
                stat_date, u, int(trade_num[u]), int(close_count[u]),
                json.dumps(sp, ensure_ascii=False),
            ))

        legs_by_user = defaultdict(list)
        for x in legs:
            legs_by_user[x['user_id']].append(x)

        hold_rows = []
        violation_ids = set()
        for u in users_today:
            ul = legs_by_user.get(u)
            if not ul:
                continue
            ul = sorted(ul, key=lambda z: (z['ts'], z['ts_i']))
            df = pd.DataFrame(ul)
            hdf = holding_records_dataframe(df[['ts', 'buy_sell', 'amount']].astype({'amount': float}))
            if hdf.shape[0] == 0:
                continue
            hdf['close_date'] = hdf['close_time'].apply(
                lambda t: t.date() if hasattr(t, 'date') else pd.Timestamp(t).date())
            sub = hdf[hdf['close_date'] == stat_date]
            if sub.shape[0] < MIN_CLOSE_SEGMENTS:
                continue
            med = float(sub['holding_seconds'].median())
            if med >= HOLD_MEDIAN_MAX_SEC:
                continue
            avg = float(sub['holding_seconds'].mean())
            syms = sorted({x['symbol'] for x in ul if x['ts'].date() == stat_date})
            hold_rows.append((
                stat_date, u, int(sub.shape[0]), json.dumps(syms, ensure_ascii=False),
                med, avg,
            ))
            violation_ids.add(u)

        self.connect_mysql()
        try:
            for tbl in ('daily_profit_user_rank', 'daily_deal_rank', 'daily_median_hold_lt10m'):
                self.cursor.execute(f'DELETE FROM {tbl} WHERE stat_date = %s', (stat_date,))
            if rank_rows:
                self.cursor.executemany(
                    """INSERT INTO daily_profit_user_rank
                    (stat_date, user_id, realized_profit, symbol_profit, symbol_trade_count)
                    VALUES (%s,%s,%s,%s,%s)""",
                    rank_rows,
                )
            if deal_rows:
                self.cursor.executemany(
                    """INSERT INTO daily_deal_rank
                    (stat_date, user_id, trade_num, open_close_num, symbol_realized_profit)
                    VALUES (%s,%s,%s,%s,%s)""",
                    deal_rows,
                )
            if hold_rows:
                self.cursor.executemany(
                    """INSERT INTO daily_median_hold_lt10m
                    (stat_date, user_id, open_close_count, symbols_traded, holding_median_seconds, holding_avg_seconds)
                    VALUES (%s,%s,%s,%s,%s,%s)""",
                    hold_rows,
                )
            self.db.commit()
        finally:
            self.close_mysql()

        users_checked = len(users_today)
        violations_detected = len(violation_ids)
        profitable_users_found = len({r[1] for r in rank_rows})

        self.log.info(
            f'summary users_checked={users_checked} violations_detected={violations_detected} '
            f'profitable_users_found={profitable_users_found} rank_rows={len(rank_rows)} deal_rows={len(deal_rows)} hold_rows={len(hold_rows)}'
        )

        new_v = violation_ids - self._violation_alerted
        if new_v:
            lines = [f'[短持仓违规] {stat_date} 新增 {len(new_v)} 人']
            vmap = {r[1]: r for r in hold_rows}
            for uid in sorted(new_v):
                row = vmap.get(uid)
                if not row:
                    continue
                occ, syms_j, hmed, havg = row[2], row[3], row[4], row[5]
                lines.append(
                    f'user={uid} 平仓片段={occ} 持仓中位秒={hmed:.0f} 均值秒={havg:.0f} 交易对={syms_j}'
                )
            await self.send_tg_chunks('\n'.join(lines))
            self._violation_alerted |= new_v

        profitable_ids = {r[1] for r in rank_rows}
        to_alert_prof = profitable_ids - violation_ids - self._profitable_alerted
        if to_alert_prof:
            pmap = {r[1]: r for r in rank_rows}
            lines = [f'[盈利用户] {stat_date} 新增 {len(to_alert_prof)} 人']
            for uid in sorted(to_alert_prof):
                r = pmap[uid]
                lines.append(f'user={uid} realized={r[2]:.4f}')
            await self.send_tg_chunks('\n'.join(lines))
            self._profitable_alerted |= to_alert_prof


def main():
    UserProfitMonitorDaily().run()


if __name__ == '__main__':
    main()
