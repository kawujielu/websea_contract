# -*- coding: utf-8 -*-
from __future__ import annotations
"""
读取 daily_profit_user_rank 当日用户，按 mongdb_order 聚合后四窗口 calc_trade_stats，写入 profit_rank_user_analysis。
"""
import os
import sys
import argparse
from collections import Counter
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import mysql.connector
import motor.motor_asyncio
import pandas as pd
from pymongo.errors import OperationFailure

_STRATEGY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _STRATEGY_ROOT not in sys.path:
    sys.path.insert(0, _STRATEGY_ROOT)

from template.template_timer import TemplateTimer, CronTrigger

from mongdb_order_stats import calc_trade_stats
from exclude_user_ids import get_excluded_user_ids_async, get_excluded_user_ids_sync

BJT = ZoneInfo('Asia/Shanghai')
BUY_SELL_SIDE = {'1': '开多', '2': '开空', '3': '平多', '4': '平空'}

MONGO_URI = os.environ.get(
    'UPM_MONGO_URI',
    'mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/',
)
MYSQL_CFG = dict(
    host=os.environ.get('UPM_MYSQL_HOST', '10.0.208.249'),
    user=os.environ.get('UPM_MYSQL_USER', 'lh_sky'),
    password=os.environ.get('UPM_MYSQL_PASSWORD', 'sky_123456'),
    database=os.environ.get('UPM_MYSQL_DB', 'contract_dws'),
    port=int(os.environ.get('UPM_MYSQL_PORT', '33306')),
)

CRON_HOURS = os.environ.get('PRA_CRON_HOURS', '*/6')


KEY_TO_COL = {
    '成交笔数(按订单算)': 'trade_count_orders',
    '交易性质': 'trade_nature',
    '开仓次数(按订单算)': 'open_count_orders',
    '平仓次数(按订单算)': 'close_count_orders',
    '总交易轮次(开平算一次)': 'total_round_trips',
    '是否重点标签套利用户': 'is_arb_tag_user',
    'taker金额': 'taker_amt',
    'taker金额占比': 'taker_amt_ratio',
    'maker金额': 'maker_amt',
    'maker金额占比': 'maker_amt_ratio',
    '手续费总和': 'total_fee',
    '总盈亏': 'total_pnl',
    '刨除手续费总盈亏': 'pnl_ex_fee',
    '总手续费': 'total_fee_gross',
    '手续费占比': 'fee_to_pnl_ratio',
    '分币对盈亏': 'symbol_pnl_breakdown',
    '最大盈利': 'max_profit',
    '最大亏损': 'max_loss',
    '总成交金额': 'total_trade_amount',
    '盈利/成交额': 'pnl_to_amount_ratio',
    '胜率': 'win_rate',
    '盈亏比': 'profit_loss_ratio',
    '期望收益': 'expect_profit',
    '夏普比率': 'sharpe',
    '平均杠杆': 'avg_leverage',
    '最大持仓时长': 'max_holding',
    '最短持仓时长': 'min_holding',
    '平均持仓时长': 'avg_holding',
    '中位数持仓时长': 'median_holding',
    '众数持仓时长(聚合到min)': 'mode_holding',
    '持仓时间小于5min的交易轮次占比': 'holding_lt5m_ratio',
    '最大连续盈利': 'max_win_streak',
    '最大连续亏损': 'max_loss_streak',
    '日均交易次数': 'daily_avg_trades',
    '周均交易次数': 'weekly_avg_trades',
    '月均交易次数': 'monthly_avg_trades',
    '收益率年化标准差': 'annualized_return_std',
    '平均仓位规模': 'avg_position_size',
    '平均交易间隔': 'avg_trade_interval',
    '统计指标': 'stats_block',
}


def _fmt_cell(v):
    if v is None:
        return 'N/A'
    if isinstance(v, float) and (pd.isna(v) if hasattr(pd, 'isna') else False):
        return 'N/A'
    return str(v)


def merge_four(sa, sm, sw, sd, key):
    vals = [sa.get(key), sm.get(key), sw.get(key), sd.get(key)]
    labels = ['all', 'month', 'week', '3day']
    return ', '.join(f'{_fmt_cell(vals[i])}({labels[i]})' for i in range(4))


def bjt_end_of_day(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 23, 59, 59)


def bjt_start_of_day(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 0, 0, 0)


def filter_df_window(df: pd.DataFrame, start_d: date | None, end_d: date) -> pd.DataFrame:
    if df.shape[0] == 0:
        return df
    ts = pd.to_datetime(df['tsText'])
    end_dt = pd.Timestamp(bjt_end_of_day(end_d))
    if start_d is None:
        return df[ts <= end_dt].copy()
    start_dt = pd.Timestamp(bjt_start_of_day(start_d))
    return df[(ts >= start_dt) & (ts <= end_dt)].copy()


async def mongo_orders_to_dataframe(collection, usr_id: str, ts_max: int, ts_min: int) -> pd.DataFrame:
    if ts_min is None:
        raise ValueError('ts_min 不能为空，请按时间窗口传入下界时间戳')
    ts_cond = {'$gte': ts_min, '$lte': ts_max}
    query = {
        'ts': ts_cond,
        '$or': [
            {'takerUser': usr_id},
            {'makerUser': usr_id},
        ],
    }
    projection = {
        '_id': 0,
        'ts': 1,
        'symbol': 1,
        'price': 1,
        'amount': 1,
        'takerUser': 1,
        'makerUser': 1,
        'takerBuyOrSell': 1,
        'makerBuyOrSell': 1,
        'takerFee': 1,
        'makerFee': 1,
        'takerProfitLoss': 1,
        'makerProfitLoss': 1,
        'takerMultiple': 1,
        'makerMultiple': 1,
        'takerOrder': 1,
        'makerOrder': 1,
        'takerFaceValue': 1,
        'takerIsProtected': 1,
        'makerIsProtected': 1,
        'takerSubId': 1,
        'makerSubId': 1,
    }
    deals_by_order_id = {}
    async def consume_cursor(cursor):
        async for deal in cursor:
            ts_i = int(deal['ts'])
            dt_str = datetime.fromtimestamp(ts_i, tz=BJT).strftime('%Y-%m-%d %H:%M:%S')
            symbol = deal['symbol']
            taker = deal['takerUser']
            maker = deal['makerUser']
            is_t = str(deal['takerUser']) == str(usr_id)
            is_m = str(deal['makerUser']) == str(usr_id)
            if is_t:
                side_type = deal['takerBuyOrSell']
                fee = deal['takerFee']
                profit = deal['takerProfitLoss']
                multiple = deal['takerMultiple']
                order_id = deal['takerOrder']
                deal_is_protected = int(deal.get('takerIsProtected') or 0)
                deal_sub_id = int(deal.get('takerSubId') or 0)
            elif is_m:
                side_type = deal['makerBuyOrSell']
                fee = deal['makerFee']
                profit = deal['makerProfitLoss']
                multiple = deal['makerMultiple']
                order_id = deal['makerOrder']
                deal_is_protected = int(deal.get('makerIsProtected') or 0)
                deal_sub_id = int(deal.get('makerSubId') or 0)
            else:
                continue
            side = BUY_SELL_SIDE[str(side_type)]
            row = [
                dt_str, symbol, side, float(deal['price']), float(deal['amount']), profit, fee,
                taker, maker, int(multiple), float(deal['takerFaceValue']),
                deal_is_protected, deal_sub_id,
            ]
            deals_by_order_id.setdefault(order_id, []).append(row)

    # 直接无排序拉取，避免 Mongo 端触发 100MB sort 内存限制。
    # 后续统计阶段会按 tsText/ts 二次排序，不影响结果正确性。
    try:
        cursor = collection.find(query, projection=projection).batch_size(2000)
        await consume_cursor(cursor)
    except OperationFailure:
        # 保留原异常信息，便于日志定位
        raise

    user_list = []
    for order_id, deals in deals_by_order_id.items():
        date_s = deals[0][0]
        symbol = deals[0][1]
        side = deals[0][2]
        multiple = deals[0][9]
        face_value = float(deals[0][10])
        amount = sum(float(i[4]) for i in deals) * face_value
        amt = sum(float(i[3]) * float(i[4]) for i in deals) * face_value
        taker_amt = sum(float(i[3]) * float(i[4]) for i in deals if str(i[7]) == str(usr_id)) * face_value
        maker_amt = sum(float(i[3]) * float(i[4]) for i in deals if str(i[8]) == str(usr_id)) * face_value
        price = round(amt / amount, 6) if amount else 0.0
        profit = sum(float(i[5]) for i in deals)
        fee = sum(float(i[6]) for i in deals)
        taker_num = len([1 for i in deals if str(i[7]) == str(usr_id)])
        maker_num = len([1 for i in deals if str(i[8]) == str(usr_id)])
        order_trade_stats = Counter()
        for leg in deals:
            dip, sid = leg[11], leg[12]
            if dip == 0 and sid == 0:
                order_trade_stats['自主交易'] += 1
            elif dip != 0 and sid != 0:
                order_trade_stats['跟单交易'] += 1
            else:
                order_trade_stats['其他'] += 1
        user_list.append([
            usr_id, date_s, symbol, side, float(profit), float(price), float(amount), amt,
            int(multiple), fee, taker_num, maker_num, taker_amt, maker_amt, dict(order_trade_stats),
        ])

    if not user_list:
        return pd.DataFrame()
    return pd.DataFrame(user_list, columns=[
        'userid', 'tsText', 'symbol', 'buy_sell', 'profit_loss', 'price', 'amount', 'amt',
        'multiple', 'fee', 'taker_num', 'maker_num', 'taker_amt', 'maker_amt', 'trade_type_stats',
    ])


def empty_stats_like():
    return calc_trade_stats(pd.DataFrame(columns=[
        'userid', 'tsText', 'symbol', 'buy_sell', 'profit_loss', 'price', 'amount', 'amt',
        'multiple', 'fee', 'taker_num', 'maker_num', 'taker_amt', 'maker_amt', 'trade_type_stats',
    ]))


class ProfitRankUserAnalysisRunner(TemplateTimer):
    def __init__(self):
        super().__init__(scheduler=True, gcc=True)

    async def on_first(self):
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        self.log.add(f"log/{script_name}.log", rotation="100 MB", retention=10)
        await self.create_table()
        self.log.info('profit_rank_user_analysis_runner 初始化')
        await self.main()

    async def on_timer(self):
        self.schedule.add_job(self.main, CronTrigger(hour=CRON_HOURS))

    def connect_mysql(self):
        self.db = mysql.connector.connect(**MYSQL_CFG)
        self.cursor = self.db.cursor()

    def close_mysql(self):
        try:
            self.cursor.close()
            self.db.close()
        except Exception:
            pass

    def _ensure_profit_rank_columns(self):
        """已有表补齐 KEY_TO_COL 中新增列（TEXT）。"""
        self.connect_mysql()
        dbname = MYSQL_CFG["database"]
        self.cursor.execute(
            """
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'profit_rank_user_analysis'
            """,
            (dbname,),
        )
        existing = {r[0] for r in self.cursor.fetchall()}
        for coln in KEY_TO_COL.values():
            if coln in existing:
                continue
            try:
                self.cursor.execute(
                    "ALTER TABLE profit_rank_user_analysis ADD COLUMN `{}` TEXT".format(coln)
                )
                self.db.commit()
                self.log.info("profit_rank_user_analysis 已增加列 `{}`".format(coln))
            except Exception as e:
                self.log.warning("增加列 `{}` 失败: {}".format(coln, e))
        self.close_mysql()

    async def create_table(self):
        col_defs = ',\n'.join(f'`{c}` TEXT' for c in KEY_TO_COL.values())
        self.connect_mysql()
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS profit_rank_user_analysis (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `user_id` VARCHAR(36) NOT NULL,
                `analysis_end_date` DATE NOT NULL,
                `trade_start_date` VARCHAR(32) COMMENT '全量最早日',
                `trade_end_date` DATE COMMENT '跑批北京日',
                {col_defs},
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `uk_user_day` (`user_id`, `analysis_end_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        self.db.commit()
        self.close_mysql()
        self._ensure_profit_rank_columns()

    async def main(self):
        end_day = datetime.now(BJT).date()
        ts_max = int(datetime.now(BJT).timestamp())
        # 批量模式至少覆盖 month/week/3day 窗口；all 窗口仍使用当前可查询范围
        ts_min = int((datetime.now(BJT) - timedelta(days=90)).timestamp())

        self.connect_mysql()
        self.cursor.execute(
            'SELECT DISTINCT user_id FROM daily_profit_user_rank WHERE stat_date = %s',
            (end_day,),
        )
        user_ids = [r[0] for r in self.cursor.fetchall()]
        self.close_mysql()

        excluded = await get_excluded_user_ids_async()
        user_ids = [u for u in user_ids if str(u) not in excluded]

        if not user_ids:
            self.log.info(f'{end_day} daily_profit_user_rank 无用户，跳过')
            return

        client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        col = client.exchange.real_contract_deal

        empty = empty_stats_like()
        rows_sql = []

        for uid in user_ids:
            try:
                df = await mongo_orders_to_dataframe(col, str(uid), ts_max, ts_min=ts_min)
                if df.shape[0] == 0:
                    sa = sm = sw = sd = empty
                else:
                    d_end = end_day
                    d_month = end_day - timedelta(days=30)
                    d_week = end_day - timedelta(days=7)
                    d3 = end_day - timedelta(days=2)
                    dfa = filter_df_window(df, None, d_end)
                    dfm = filter_df_window(df, d_month, d_end)
                    dfw = filter_df_window(df, d_week, d_end)
                    dfd = filter_df_window(df, d3, d_end)
                    sa = calc_trade_stats(dfa) if dfa.shape[0] else empty
                    sm = calc_trade_stats(dfm) if dfm.shape[0] else empty
                    sw = calc_trade_stats(dfw) if dfw.shape[0] else empty
                    sd = calc_trade_stats(dfd) if dfd.shape[0] else empty

                trade_start = sa.get('交易开始时间') or ''

                row = {
                    'user_id': str(uid),
                    'analysis_end_date': end_day,
                    'trade_start_date': str(trade_start) if trade_start else '',
                    'trade_end_date': end_day,
                }
                for ck, coln in KEY_TO_COL.items():
                    row[coln] = merge_four(sa, sm, sw, sd, ck)

                rows_sql.append(row)
            except Exception as e:
                self.log.exception(f'用户 {uid} 分析失败: {e}')

        client.close()

        if not rows_sql:
            return

        col_names = ['user_id', 'analysis_end_date', 'trade_start_date', 'trade_end_date'] + list(KEY_TO_COL.values())
        self.connect_mysql()
        try:
            self.cursor.execute(
                'DELETE FROM profit_rank_user_analysis WHERE analysis_end_date = %s',
                (end_day,),
            )
            placeholders = ','.join(['%s'] * len(col_names))
            quoted = ','.join(f'`{c}`' for c in col_names)
            ins = f"INSERT INTO profit_rank_user_analysis ({quoted}) VALUES ({placeholders})"
            batch = [[r[c] for c in col_names] for r in rows_sql]
            self.cursor.executemany(ins, batch)
            self.db.commit()
            self.log.info(f'写入 profit_rank_user_analysis {len(batch)} 行, analysis_end_date={end_day}')
        finally:
            self.close_mysql()


def main():
    parser = argparse.ArgumentParser(description='profit_rank_user_analysis_runner')
    parser.add_argument('--user_id', type=str, default='')
    parser.add_argument('--mode', type=str, default='window', help='window | all')
    parser.add_argument('--days', type=int, default=3)
    parser.add_argument('--print_stdout', action='store_true')
    args, _ = parser.parse_known_args()

    if args.user_id:
        if str(args.user_id) in get_excluded_user_ids_sync():
            print("该用户为做市账户或模拟金账户，不提供查询结果")
            return

        async def _single():
            end_day = datetime.now(BJT).date()
            now_ts = int(datetime.now(BJT).timestamp())
            mode = (args.mode or 'window').lower().strip()
            days = max(1, int(args.days))

            if mode == 'all':
                # all 模式给较大但有限的窗口，避免无界查询
                start_day = end_day - timedelta(days=3650)
                label = '全部/全量'
            else:
                start_day = end_day - timedelta(days=days - 1)
                label = '近{}天'.format(days)

            start_dt = datetime(start_day.year, start_day.month, start_day.day, 0, 0, 0, tzinfo=BJT)
            ts_min = int(start_dt.timestamp())

            client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
            col = client.exchange.real_contract_deal
            try:
                df = await mongo_orders_to_dataframe(col, str(args.user_id), now_ts, ts_min=ts_min)
                if df.shape[0] == 0:
                    print('用户 {} 在{}无数据'.format(args.user_id, label))
                    return
                dff = filter_df_window(df, start_day if mode != 'all' else None, end_day)
                ss = calc_trade_stats(dff) if dff.shape[0] else empty_stats_like()
                lines = ['用户 {} 交易分析（{}）'.format(args.user_id, label)]
                for k, v in ss.items():
                    lines.append('{}: {}'.format(k, v))
                text = '\n'.join(lines)
                if args.print_stdout:
                    print(text)
            finally:
                client.close()

        import asyncio
        asyncio.run(_single())
        return

    ProfitRankUserAnalysisRunner().run()


if __name__ == '__main__':
    main()
