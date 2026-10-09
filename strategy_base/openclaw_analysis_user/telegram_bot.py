from __future__ import annotations

import asyncio
import logging
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.ext import (
    Application,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)
LOG = logging.getLogger(__name__)
# 屏蔽长轮询产生的 httpx INFO 噪音日志
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.INFO)

BOT_TOKEN = os.environ.get('TG_BOT_TOKEN', '6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q')
PYTHON_PATH = os.environ.get('PYTHON_PATH', sys.executable)

# Ubuntu 生产目录（与 openclaw_start.sh 一致）
DEFAULT_OPENCLAW_APP_DIR = '/home/ubuntu/strategy_base/strategy/openclaw_analysis_user'


def _resolve_openclaw_app_dir() -> str:
    """
    应用根目录：telegram_bot.py、AI_model/、各 on_demand 脚本、data/mongo_daily_deals_pkl/ 同级。

    优先级：环境变量 OPENCLAW_APP_DIR > 本文件所在目录（含 AI_model）> 默认 Ubuntu 路径 > 本文件目录。
    """
    env_dir = os.environ.get('OPENCLAW_APP_DIR', '').strip()
    if env_dir:
        return env_dir
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if os.path.isdir(os.path.join(script_dir, 'AI_model')):
        return script_dir
    if os.path.isdir(os.path.join(DEFAULT_OPENCLAW_APP_DIR, 'AI_model')):
        return DEFAULT_OPENCLAW_APP_DIR
    return script_dir


OPENCLAW_APP_DIR = _resolve_openclaw_app_dir()
AI_MODEL_DIR = os.path.join(OPENCLAW_APP_DIR, 'AI_model')
PKL_DEALS_DIR = (
    os.environ.get('UPM_PKL_DEALS_DIR', '').strip()
    or os.path.join(OPENCLAW_APP_DIR, 'data', 'mongo_daily_deals_pkl')
)

ANALYSIS_SCRIPT = os.environ.get(
    'ANALYSIS_SCRIPT',
    os.path.join(OPENCLAW_APP_DIR, 'user_deal_analysis_on_demand.py'),
)
WASH_ANALYSIS_SCRIPT = os.environ.get(
    'WASH_ANALYSIS_SCRIPT',
    os.path.join(OPENCLAW_APP_DIR, 'wash_trade_risk_on_demand.py'),
)
PKL_ANALYSIS_SCRIPT = os.environ.get(
    'PKL_ANALYSIS_SCRIPT',
    os.path.join(OPENCLAW_APP_DIR, 'pkl_user_query_analyzer.py'),
)
PKL_MULTI_RANK_SCRIPT = os.environ.get(
    'PKL_MULTI_RANK_SCRIPT',
    os.path.join(OPENCLAW_APP_DIR, 'pkl_multi_dimension_top_users_analysis.py'),
)
PKL_MULTI_RANK_TIMEOUT_SEC = int(os.environ.get('TG_PKL_MULTI_RANK_TIMEOUT_SEC', '300'))
AI_PROFILE_SCRIPT = os.environ.get(
    'AI_PROFILE_SCRIPT',
    os.path.join(AI_MODEL_DIR, 'run_ai_profile.py'),
)
AI_PROFILE_TIMEOUT_SEC = int(os.environ.get('TG_AI_PROFILE_TIMEOUT_SEC', '300'))
PKL_SIMILARITY_SCRIPT = os.environ.get(
    'PKL_SIMILARITY_SCRIPT',
    os.path.join(OPENCLAW_APP_DIR, 'pkl_user_group_trade_similarity.py'),
)
FOLLOW_ACCOUNTS_ANALYSIS_SCRIPT = os.environ.get(
    'FOLLOW_ACCOUNTS_ANALYSIS_SCRIPT',
    os.path.join(OPENCLAW_APP_DIR, 'follow_accounts_net_value_positions_trades_on_demand.py'),
)
CONTRACT_ACTIVITY_SCRIPT = os.environ.get(
    'CONTRACT_ACTIVITY_SCRIPT',
    os.path.join(OPENCLAW_APP_DIR, 'contract_active_rank.py'),
)
SPOT_ACTIVITY_SCRIPT = os.environ.get(
    'SPOT_ACTIVITY_SCRIPT',
    os.path.join(OPENCLAW_APP_DIR, 'spot_active_rank.py'),
)
MONTHLY_PROFIT_TOP_SCRIPT = os.environ.get(
    'MONTHLY_PROFIT_TOP_SCRIPT',
    os.path.join(OPENCLAW_APP_DIR, 'monthly_profit_top_users_on_demand.py'),
)
SYMBOL_HOLD_SCRIPT = os.environ.get(
    'SYMBOL_HOLD_SCRIPT',
    os.path.join(OPENCLAW_APP_DIR, 'tg_symbol_hold_snapshot.py'),
)
SPOT_BALANCE_SCRIPT = os.environ.get(
    'SPOT_BALANCE_SCRIPT',
    os.path.join(OPENCLAW_APP_DIR, 'select_spot_balance.py'),
)
SPOT_BALANCE_TIMEOUT_SEC = int(os.environ.get('TG_SPOT_BALANCE_TIMEOUT_SEC', '120'))
ANALYSIS_TIMEOUT_SEC = int(os.environ.get('TG_ANALYSIS_TIMEOUT_SEC', '180'))
SYMBOL_HOLD_TIMEOUT_SEC = int(os.environ.get('TG_SYMBOL_HOLD_TIMEOUT_SEC', '600'))
POSITION_DIST_SCRIPT = os.environ.get(
    'POSITION_DIST_SCRIPT',
    os.path.join(OPENCLAW_APP_DIR, 'pkl_positions_cost_dist.py'),
)
POSITION_DIST_TIMEOUT_SEC = int(os.environ.get('TG_POSITION_DIST_TIMEOUT_SEC', '900'))
# 与 AI 画像、PKL 查询共用日切成交目录
POSITION_DIST_DATA_DIR = PKL_DEALS_DIR

BJT = ZoneInfo('Asia/Shanghai')
# 与 user_deal_analysis_on_demand 中活动窗口一致：仅允许该段日历日内的指定日/时段查询
CAMPAIGN_MIN_DATE = date(2026, 4, 1)
CAMPAIGN_MAX_DATE = date(2026, 4, 10)

_CHAT_LOCKS = {}


def split_text(text: str, max_len: int = 3900):
    if not text:
        return ['(空结果)']
    chunks = []
    cur = []
    cur_len = 0
    for line in text.splitlines(True):
        if cur_len + len(line) > max_len and cur:
            chunks.append(''.join(cur))
            cur = [line]
            cur_len = len(line)
        else:
            cur.append(line)
            cur_len += len(line)
    if cur:
        chunks.append(''.join(cur))
    return chunks
    

def parse_user_id(query: str):
    q = query.strip()
    patterns = [
        r'分析\s*([0-9A-Za-z_-]{1,64})\s*用户?',
        r'用户\s*([0-9A-Za-z_-]{1,64})',
        r'分析\s*([0-9A-Za-z_-]{1,64})\s*(?:是否|有无|是不是|刷单|刷量)',
    ]
    for p in patterns:
        m = re.search(p, q)
        if m:
            return m.group(1)
    return None


def parse_detail_user_id(query: str) -> Optional[str]:
    # 明细场景更宽松：出现 6～7 位用户ID即可识别
    uid = parse_user_id(query)
    if uid:
        return uid
    q = query.strip()
    patterns = [
        r'(?i)\buid\s*[:=：]?\s*(\d{6,7})\b',
        r'(?i)\bid\s*[:=：]\s*(\d{6,7})\b',
        r'(?i)用户\s*id\s*[:=：]?\s*(\d{6,7})\b',
        r'(?i)用户id\s*[:=：]?\s*(\d{6,7})\b',
    ]
    for p in patterns:
        m2 = re.search(p, q)
        if m2:
            return m2.group(1)
    m = re.search(r'(?<!\d)(\d{6,7})(?!\d)', query)
    if m:
        return m.group(1)
    return None


def _first_n_specs(specs: List[Tuple[int, str, Optional[int]]], limit: int = 3):
    if not specs:
        return []
    specs.sort(key=lambda x: x[0])
    picked = specs[:limit]
    return [(mode, days) for _, mode, days in picked]


def parse_time_specs(query: str):
    q = query.replace(' ', '')
    specs = []  # type: List[Tuple[int, str, Optional[int]]]

    if re.search(r'(全部|全量)', q):
        return [('all', None)]

    for m in re.finditer(r'(?:近|最近)?(\d+)\s*月', q):
        n = int(m.group(1))
        if n > 0:
            specs.append((m.start(), 'window', max(1, 30 * n)))

    for m in re.finditer(r'(?:近|最近)?(\d+)\s*[周週]', q):
        n = int(m.group(1))
        if n > 0:
            specs.append((m.start(), 'window', max(1, 7 * n)))

    for m in re.finditer(r'(?:近|最近)?(\d+)\s*[天日]', q):
        n = int(m.group(1))
        if n > 0:
            specs.append((m.start(), 'window', max(1, n)))

    return _first_n_specs(specs, limit=3)


def _campaign_date_in_range(d: date) -> bool:
    return CAMPAIGN_MIN_DATE <= d <= CAMPAIGN_MAX_DATE


def _build_cli_datetime(d: date, hour: int, minute: int, second: int = 0) -> str:
    dt = datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=BJT)
    if second:
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    return dt.strftime('%Y-%m-%d %H:%M')


def parse_campaign_bjt_range(query: str) -> Optional[Tuple[str, str]]:
    """解析 2026-04-01～2026-04-10（北京时间）内的指定日/时段，返回传给脚本的 --detail-start / --detail-end 字符串。"""
    q = re.sub(r'\s+', ' ', (query or '').strip())

    def _parse_ymd(s: str) -> date:
        y, m, d = s.split('-')
        return date(int(y), int(m), int(d))

    # 2026-04-01 到 2026-04-10：仅日历日、跨多日整日（起止当天 00:00～23:59）
    m = re.search(r'(\d{4}-\d{2}-\d{2})\s*到\s*(\d{4}-\d{2}-\d{2})', q)
    if m:
        d1 = _parse_ymd(m.group(1))
        d2 = _parse_ymd(m.group(2))
        if d2 < d1:
            return None
        if not (_campaign_date_in_range(d1) and _campaign_date_in_range(d2)):
            return None
        return (_build_cli_datetime(d1, 0, 0, 0), _build_cli_datetime(d2, 23, 59, 0))

    # 2026-04-05 09:00 到 2026-04-05 18:00（可选秒）
    m = re.search(
        r'(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*到\s*(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?',
        q,
    )
    if m:
        d1 = _parse_ymd(m.group(1))
        h1, mi1 = int(m.group(2)), int(m.group(3))
        s1 = int(m.group(4) or 0)
        d2 = _parse_ymd(m.group(5))
        h2, mi2 = int(m.group(6)), int(m.group(7))
        s2 = int(m.group(8) or 0)
        if not (_campaign_date_in_range(d1) and _campaign_date_in_range(d2)):
            return None
        return (_build_cli_datetime(d1, h1, mi1, s1), _build_cli_datetime(d2, h2, mi2, s2))

    # 2026-04-05 09:00 到 18:00（同日）
    m = re.search(
        r'(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*到\s*(\d{1,2}):(\d{2})(?::(\d{2}))?',
        q,
    )
    if m:
        d1 = _parse_ymd(m.group(1))
        h1, mi1 = int(m.group(2)), int(m.group(3))
        s1 = int(m.group(4) or 0)
        h2, mi2 = int(m.group(5)), int(m.group(6))
        s2 = int(m.group(7) or 0)
        if h2 >= 24 or mi2 >= 60 or s2 >= 60 or h1 >= 24 or mi1 >= 60 or s1 >= 60:
            return None
        if not _campaign_date_in_range(d1):
            return None
        return (_build_cli_datetime(d1, h1, mi1, s1), _build_cli_datetime(d1, h2, mi2, s2))

    q_ns = q.replace(' ', '')
    # 2026-04-05当天09:00到18:00
    m = re.search(
        r'(\d{4}-\d{2}-\d{2})当天(\d{1,2}):(\d{2})到(\d{1,2}):(\d{2})',
        q_ns,
    )
    if m:
        d1 = _parse_ymd(m.group(1))
        h1, mi1 = int(m.group(2)), int(m.group(3))
        h2, mi2 = int(m.group(4)), int(m.group(5))
        if h2 >= 24 or mi2 >= 60 or h1 >= 24 or mi1 >= 60:
            return None
        if not _campaign_date_in_range(d1):
            return None
        return (_build_cli_datetime(d1, h1, mi1, 0), _build_cli_datetime(d1, h2, mi2, 0))

    # 整日：2026-04-05 全天 / 整日 / 一整天
    m = re.search(r'(\d{4}-\d{2}-\d{2})\s*(?:全天|整日|一整天)', q_ns)
    if not m:
        m = re.search(r'(\d{4}-\d{2}-\d{2})\s*(?:全天|整日|一整天)', q)
    if m:
        d1 = _parse_ymd(m.group(1))
        if not _campaign_date_in_range(d1):
            return None
        return (_build_cli_datetime(d1, 0, 0, 0), _build_cli_datetime(d1, 23, 59, 0))

    # 单独出现的活动窗口内日期 → 按整日（不用 \\b：中文与数字相邻时无词边界）
    m = re.search(r'(?<![0-9])(2026-04-(?:0[1-9]|10))(?![0-9])', q)
    if m:
        d1 = _parse_ymd(m.group(1))
        return (_build_cli_datetime(d1, 0, 0, 0), _build_cli_datetime(d1, 23, 59, 0))

    return None


def normalize_requested_spec(specs: List[Tuple[str, Optional[int]]]):
    picked = []
    for mode, days in specs:
        if mode == 'all':
            picked.append((mode, None))
            continue
        if days is None:
            continue
        days = int(days)
        if days <= 0:
            continue
        if days > 3650:
            days = 3650
        picked.append((mode, days))
    return picked


def is_wash_query(query: str) -> bool:
    q = query.replace(' ', '')
    keys = ['刷单', '刷量', '是否有刷单', '是否有刷量', '刷单行为', '刷量行为']
    for k in keys:
        if k in q:
            return True
    return False


def is_help_query(query: str) -> bool:
    q = query.replace(' ', '')
    keys = ['帮助文档', '帮助手册', '使用方式', '使用方法', '使用说明', '帮助', 'help']
    for k in keys:
        if k in q:
            return True
    return False


_SPOT_BALANCE_RE = re.compile(
    r'(?i)(?:(?P<c1>[A-Za-z0-9]{2,16})\s*余额|余额\s*(?P<c2>[A-Za-z0-9]{2,16}))'
)


def parse_spot_balance_coin(query: str) -> Optional[str]:
    m = _SPOT_BALANCE_RE.search(query.strip())
    if not m:
        return None
    return (m.group('c1') or m.group('c2') or '').upper() or None


def parse_symbol_hold_symbol(query: str) -> Optional[str]:
    """解析「查询 BTC-USDT 当前/最新持仓」类话术，返回合约代码。"""
    q = query.strip()
    if '查询' not in q or '持仓' not in q:
        return None
    m = re.search(
        r'查询\s*([A-Za-z0-9][A-Za-z0-9_.-]{1,47})\s*(?:当前|最新)?\s*持仓(?:情况)?',
        q,
        flags=re.IGNORECASE,
    )
    if not m:
        return None
    sym = (m.group(1) or '').strip()
    if len(sym) < 3:
        return None
    return sym


def is_symbol_hold_query(query: str) -> bool:
    return parse_symbol_hold_symbol(query) is not None


def is_all_position_dist_query(query: str) -> bool:
    q = query.replace(' ', '')
    return '全部交易对持仓分布' in q


def parse_position_dist_symbol(query: str) -> Optional[str]:
    """解析「查询 BTC-USDT 持仓分布」，不与「查询全部交易对持仓分布」冲突。"""
    q = query.strip()
    if is_all_position_dist_query(query):
        return None
    if '查询' not in q or '持仓分布' not in q:
        return None
    m = re.search(
        r'查询\s*([A-Za-z0-9][A-Za-z0-9_.-]{1,47})\s*持仓分布',
        q,
        flags=re.IGNORECASE,
    )
    if not m:
        return None
    sym = (m.group(1) or '').strip()
    if len(sym) < 3:
        return None
    return sym


def is_pkl_user_group_similarity_query(query: str) -> bool:
    """PKL 离线：用户组两两交易重合/相似度；需与 is_pkl_query 区分。"""
    q = query.replace(' ', '')
    if (
        '相似度' not in q
        and '重合' not in q
        and '交易相似' not in q
        and '交易行为近似' not in q
    ):
        return False
    if '用户组' in q:
        return True
    if 'pkl' in q.lower():
        return True
    # 兼容自然语句：对比/分析 近N天(周/月) 123456和654321 用户的交易相似度
    if re.search(r'(对比|分析)', q) and re.search(r'(?<!\d)\d{6,7}(?!\d)', q):
        return True
    if '用户' in q and re.search(r'(?:近|最近)?\d+\s*(?:个)?(?:[天日周週月])', q):
        return True
    return False


def parse_similarity_user_ids(query: str) -> List[str]:
    """提取 6～7 位用户 ID，顺序去重。"""
    seen = set()
    out: List[str] = []
    for m in re.finditer(r'(?<!\d)(\d{6,7})(?!\d)', query):
        u = m.group(1)
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def parse_similarity_min_rate_pct(query: str) -> float:
    q = query.replace(' ', '')
    for p in [r'超过(\d+(?:\.\d+)?)\s*%', r'大于(\d+(?:\.\d+)?)\s*%', r'高于(\d+(?:\.\d+)?)\s*%']:
        m = re.search(p, q)
        if m:
            v = float(m.group(1))
            return max(0.0, min(100.0, v))
    return 0.0


def parse_similarity_window_days(query: str) -> Optional[int]:
    specs = parse_time_specs(query)
    if not specs:
        return None
    mode, days = specs[0]
    if mode == 'window' and days:
        return max(1, int(days))
    return None


def is_ai_profile_query(query: str) -> bool:
    q = query or ''
    return ('用AI分析' in q) or ('AI分析' in q.upper().replace(' ', ''))


def is_pkl_multi_rank_query(query: str) -> bool:
    """PKL：总收益 / 成交笔数 / 开平仓轮次 各 TopN + calc_trade_stats 详细分析。"""
    q = query.replace(' ', '')
    has_window = (
        re.search(r'近\d+\s*(日|天|周|月)', q) is not None
        or re.search(r'(昨日|昨天)', q) is not None
        or re.search(r'\d{4}-\d{2}-\d{2}\s*到\s*\d{4}-\d{2}-\d{2}', q) is not None
    )
    if not has_window:
        return False
    if '多维度' in q or '各维度' in q:
        return True
    has_profit = ('总收益' in q) or ('盈利' in q and '排名' in q)
    has_trades = ('成交笔数' in q) or ('交易笔数' in q)
    has_rounds = ('轮次' in q) or ('开平' in q)
    if has_profit and has_trades and has_rounds:
        return True
    if has_profit and has_trades and ('各前' in q or '详细分析' in q):
        return True
    return False


def parse_pkl_multi_rank_top_n(query: str) -> int:
    q = query.replace(' ', '')
    m = re.search(r'前(\d+)(?:名|个|位)?', q)
    if m:
        return max(1, min(50, int(m.group(1))))
    return 10


def is_pkl_query(query: str) -> bool:
    q = query.replace(' ', '')
    if is_pkl_multi_rank_query(query):
        return False
    has_window = (
        re.search(r'近\d+\s*(日|天|周|月)', q) is not None
        or re.search(r'(昨日|昨天)', q) is not None
    )
    if not has_window:
        return False
    patterns = [
        r'盈利超过\d+(\.\d+)?\s*(u|usdt)',
        r'盈利(金额)?排名前\d+(名|个|位)?(的用户)?',
        r'单日盈利超过1000.*前\d+名',
        r'交易笔数(累积|累计)排名前\d+名',
    ]
    for p in patterns:
        if re.search(p, q, flags=re.IGNORECASE):
            return True
    return False


def is_detail_query(query: str) -> bool:
    q = query.replace(' ', '')
    return ('交易明细' in q) or ('成交明细' in q)


def has_full_display_flag(query: str) -> bool:
    q = query.replace(' ', '')
    return '全部展示' in q


def strip_full_display_flag(query: str) -> str:
    # 避免“全部展示”里的“全部”被时间解析误判为全量查询
    q = query
    q = q.replace('全部展示', '')
    q = q.replace('全部 显示', '')
    return q.strip()


def build_help_text() -> str:
    lines = [
        '可用查询方式如下：',
        '',
        '1) 交易分析',
        '- 分析123456用户近3天交易数据',
        '- 分析123456用户近2周交易数据',
        '- 分析123456用户近1月交易数据',
        '- 分析123456用户全部交易数据',
        '',
        '2) 刷量排查',
        '- 分析123456是否为刷单用户',
        '- 分析123456是否为刷单用户近7天',
        '- 分析123456是否刷单用户2026-04-05 10:00到12:00（活动窗：2026-04-01～04-10）',
        '',
        '3) 本地PKL数据查询',
        '- 查询近30日 盈利超过1000 usdt用户的id',
        '- 查询近8周 单日盈利超过1000u的用户出现次数，只返回前10名',
        '- 查询近2月 用户交易笔数累积排名前20名，以及对应的盈亏金额',
        '- 分析昨日盈利排名前10的用户（或：昨天）',
        '- 分析近7天盈利排名前20的用户',
        '',
        '3a) PKL 多维度 TopN + 详细交易统计（总收益/成交笔数/开平仓轮次各前N，calc_trade_stats 全指标）',
        '- 查询近7天总收益交易笔数开平仓轮次各前10用户详细分析',
        '- PKL多维度排名 2026-05-01 到 2026-05-07 各前5名',
        '',
        '3b) PKL 用户组交易相似度（离线日切 PKL，全市场腿汇总；时间±1min+同方向）',
        '- PKL用户组交易相似度 1234567 3456789 5566778',
        '- 用户组重合率 1111111,2222222,3333333（可加「同合约」仅匹配相同 symbol）',
        '- 对比近7天与1234567用户成交相似度超过80%的用户',
        '',
        '4) 成交明细查询（仅返回最近100条，不含汇总指标）',
        '- 给出123456用户近3天的交易明细',
        '- 给出123456用户近3天的交易明细 全部展示',
        '- 给出123456用户2026-04-05 09:00到18:00的交易明细（活动窗：2026-04-01～04-10 北京时间）',
        '',
        '时间范围支持：近N天 / N周 / N月 / 全部（全量）；活动窗内可写指定日或时段（2026-04-01～04-10），例如：',
        '- 分析123456用户2026-04-01到2026-04-10交易数据',
        '- 分析123456用户2026-04-05 09:00到18:00交易数据',
        '- 分析123456用户2026-04-05全天交易数据',
        '',
        '5) 带单账户净值/持仓/成交明细查询',
        '- 查询当前所有带单账户的净值/持仓',
        '- 查询当前所有带单账户近1天的成交明细 全部展示',
        '',
        '6) 合约活跃度（近N周/N月，各维度最不活跃交易对；默认 Top5，可加「前N个/前N名/topN」）',
        '- 查询近2周合约活跃度',
        '- 查询近1个月合约活跃度',
        '- 查询近1周合约活跃度前10个（或：前10个用户；口径仍为交易对排行）',
        '',
        '7) 现货活跃度（近N周/N月，最不活跃交易对；默认 Top5，可加「前N个/前N名/topN」）',
        '- 查询近2周现货活跃度',
        '- 查询近1个月现货活跃度',
        '- 查询近1周现货活跃度前10个',
        '',
        '8) 自然月盈利榜（某日历月盈利最多的前N名用户）',
        '- 3月份盈利最多的10个用户交易',
        '- 2026年3月盈利最多前5名',
        '',
        '8b) 滚动周/月盈利榜（上N周/月盈利最多前M名用户，返回ID+盈利金额）',
        '- 查询上1周盈利最多的10个用户',
        '- 查询上2月盈利最多前5名用户',
        '',
        '9) 单合约当前持仓快照（全市场拉仓，排除做市/模拟金；多空人数与 Top3）',
        '- 查询 BTC-USDT 当前持仓情况',
        '- 查询 BTCUSDT 最新持仓',
        '',
        '10) PKL 持仓成本/爆仓分布 + 与 API 对比（需本地日切 PKL + 拉接口，较慢）',
        '- 查询 BTC-USDT 持仓分布',
        '- 查询全部交易对持仓分布',
        '',
        '11) Gemini AI 用户画像（7 模块分析，结论≤500字）',
        '- 用AI分析123456用户交易特征',
        '- 用AI分析123456和654321用户近7天交易行为',
        '- 用AI分析近30天用户交易特征（全用户汇总指标）',
        '- 用AI分析123456用户近3天交易特征 原始回答（仅喂原始提问+逐笔，无格式要求）',
        '',
        '12) 现货账户指定币种余额（WebSea/Gate/Binance，余额为0跳过）',
        '- btc 余额',
        '- 余额 USDT',
    ]
    return '\n'.join(lines)


def is_monthly_profit_top_query(query: str) -> bool:
    """日历月 + 盈利排行；排除「近N月」滚动窗口，避免与 PKL/分析冲突。"""
    q = query.replace(' ', '')
    if '盈利' not in q:
        return False
    if re.search(r'(?:近|上)\d+\s*(?:个)?月', q):
        return False
    if not re.search(r'前\d+|top\d+|最多|最大|\d+个', q, flags=re.IGNORECASE):
        return False
    if re.search(r'(20\d{2})\s*年\s*(\d{1,2})\s*月', q):
        return True
    if re.search(r'(\d{1,2})\s*(?:个)?月份', q):
        return True
    if re.search(r'(?<!近)(?<!上)(\d{1,2})\s*月', q):
        return True
    return False


def parse_monthly_profit_top_params(query: str) -> Optional[Tuple[int, Optional[int], int]]:
    """返回 (month, year|None, top)。year 缺省由脚本用当前北京时间年。"""
    q = query.replace(' ', '')
    if not is_monthly_profit_top_query(query):
        return None
    year = None  # type: Optional[int]
    month = 0
    my = re.search(r'(20\d{2})\s*年\s*(\d{1,2})\s*月', q)
    if my:
        year = int(my.group(1))
        month = int(my.group(2))
    else:
        mm = re.search(r'(\d{1,2})\s*(?:个)?月份', q)
        if not mm:
            mm = re.search(r'(?<!近)(?<!上)(\d{1,2})\s*月', q)
        if not mm:
            return None
        month = int(mm.group(1))
    if month < 1 or month > 12:
        return None
    top = 10
    m_top = re.search(r'前(\d+)', q)
    if m_top:
        top = int(m_top.group(1))
    else:
        m_top = re.search(r'top\s*(\d+)', q, flags=re.IGNORECASE)
        if m_top:
            top = int(m_top.group(1))
        else:
            m_top = re.search(r'(?:最多|最大)(?:的)?(\d+)\s*个', q)
            if m_top:
                top = int(m_top.group(1))
            else:
                m_top = re.search(r'(\d+)\s*个', q)
                if m_top:
                    top = int(m_top.group(1))
    top = max(1, min(top, 50))
    return month, year, top


def is_rolling_profit_top_query(query: str) -> bool:
    """识别「查询上N周/月盈利最多的M个用户」类查询。"""
    q = query.replace(' ', '')
    if '盈利' not in q:
        return False
    if '上' not in q:
        return False
    if re.search(r'上\d+\s*(?:个)?(?:[周週]|月)', q) is None:
        return False
    if re.search(r'前\d+|top\d+|最多|最大|\d+个', q, flags=re.IGNORECASE) is None:
        return False
    return ('查询' in q) or ('用户' in q)


def parse_rolling_profit_top_params(query: str) -> Optional[Tuple[str, int, int]]:
    """返回 (unit, n, top)。unit 为 week/month。"""
    q = query.replace(' ', '')
    if not is_rolling_profit_top_query(query):
        return None
    m_win = re.search(r'上(\d+)\s*(?:个)?([周週月])', q)
    if not m_win:
        return None
    n = int(m_win.group(1))
    if n <= 0:
        return None
    unit_ch = m_win.group(2)
    unit = 'week' if unit_ch in ['周', '週'] else 'month'

    top = 10
    m_top = re.search(r'前(\d+)', q)
    if m_top:
        top = int(m_top.group(1))
    else:
        m_top = re.search(r'top\s*(\d+)', q, flags=re.IGNORECASE)
        if m_top:
            top = int(m_top.group(1))
        else:
            m_top = re.search(r'(?:最多|最大)(?:的)?(\d+)\s*个', q)
            if m_top:
                top = int(m_top.group(1))
            else:
                m_top = re.search(r'(\d+)\s*个', q)
                if m_top:
                    top = int(m_top.group(1))
    top = max(1, min(top, 50))
    return unit, n, top


def is_spot_activity_query(query: str) -> bool:
    q = query.replace(' ', '')
    if '现货活跃度' not in q:
        return False
    return re.search(r'(?:近|最近)?\d+\s*(?:个)?(?:[周週]|月)', q) is not None


def is_contract_activity_query(query: str) -> bool:
    q = query.replace(' ', '')
    if '合约活跃度' not in q:
        return False
    return re.search(r'(?:近|最近)?\d+\s*(?:个)?(?:[周週]|月)', q) is not None


def parse_contract_activity_weeks_months(query: str) -> Optional[Tuple[str, int]]:
    """按语句中先出现的维度解析，返回 ('week'|'month', n)。"""
    q = query.replace(' ', '')
    specs = []  # type: List[Tuple[int, str, int]]
    for m in re.finditer(r'(?:近|最近)?(\d+)\s*(?:个)?月', q):
        n = int(m.group(1))
        if n > 0:
            specs.append((m.start(), 'month', n))
    for m in re.finditer(r'(?:近|最近)?(\d+)\s*(?:个)?[周週]', q):
        n = int(m.group(1))
        if n > 0:
            specs.append((m.start(), 'week', n))
    if not specs:
        return None
    specs.sort(key=lambda x: x[0])
    _, unit, n = specs[0]
    return unit, n


def parse_contract_activity_top_n(query: str) -> int:
    """解析「前N名/前N个/topN」等，供活跃度脚本 --top；默认 5，上限 50。"""
    q = query.replace(' ', '')
    top = 5
    m = re.search(r'前(\d+)\s*(?:名|个|条)(?:用户)?', q)
    if m:
        top = int(m.group(1))
    else:
        m = re.search(r'前(\d+)\s*用户', q)
        if m:
            top = int(m.group(1))
        else:
            m = re.search(r'top\s*(\d+)', q, flags=re.IGNORECASE)
            if m:
                top = int(m.group(1))
    return max(1, min(50, int(top)))


def is_follow_accounts_overview_query(query: str) -> bool:
    q = query.replace(' ', '')
    if '带单账户' not in q and '跟单账户' not in q:
        return False
    keys = ['净值', '持仓', '盈亏', '成交明细', '交易明细', '成交记录']
    for k in keys:
        if k in q:
            return True
    return False


def _follow_query_flags(query: str) -> Tuple[bool, bool, bool]:
    q = query.replace(' ', '')
    wants_net_value = ('净值' in q) or ('盈亏' in q)
    wants_position = ('持仓' in q)
    wants_trades = ('成交明细' in q) or ('交易明细' in q) or ('成交记录' in q)
    return wants_net_value, wants_position, wants_trades


def run_analysis_sync(
    user_id: str,
    mode: str,
    days: Optional[int],
    script_path: str,
    detail: bool = False,
    detail_limit: Optional[int] = None,
    range_start: Optional[str] = None,
    range_end: Optional[str] = None,
):
    cmd = [PYTHON_PATH, script_path, '--user_id', str(user_id), '--print_stdout']
    if range_start and range_end:
        cmd += ['--detail-start', range_start, '--detail-end', range_end]
    if detail:
        cmd += ['--detail', '--detail-limit', str(int(detail_limit if detail_limit is not None else 100))]
    if not (range_start and range_end):
        if (mode or '').lower().strip() == 'all':
            cmd += ['--mode', 'all']
        else:
            cmd += ['--mode', 'window', '--days', str(int(days or 3))]
    return subprocess.run(
        cmd,
        cwd=OPENCLAW_APP_DIR,
        capture_output=True,
        text=True,
        timeout=ANALYSIS_TIMEOUT_SEC,
    )


def run_pkl_query_sync(query: str):
    cmd = [
        PYTHON_PATH,
        PKL_ANALYSIS_SCRIPT,
        '--query-text',
        str(query),
        '--print-stdout',
    ]
    return subprocess.run(
        cmd,
        cwd=OPENCLAW_APP_DIR,
        capture_output=True,
        text=True,
        timeout=ANALYSIS_TIMEOUT_SEC,
    )


def run_pkl_multi_rank_sync(query: str, top_n: int = 10):
    cmd = [
        PYTHON_PATH,
        PKL_MULTI_RANK_SCRIPT,
        '--data-dir',
        PKL_DEALS_DIR,
        '--query-text',
        str(query),
        '--top',
        str(int(top_n)),
        '--print-stdout',
    ]
    return subprocess.run(
        cmd,
        cwd=OPENCLAW_APP_DIR,
        env=_subprocess_env_for_openclaw(),
        capture_output=True,
        text=True,
        timeout=PKL_MULTI_RANK_TIMEOUT_SEC,
    )


def _subprocess_env_for_openclaw() -> dict:
    """子进程 cwd=OPENCLAW_APP_DIR；PYTHONPATH 含应用根，便于 AI_model 与同级脚本 import。"""
    env = os.environ.copy()
    env['UPM_PKL_DEALS_DIR'] = PKL_DEALS_DIR
    env['OPENCLAW_APP_DIR'] = OPENCLAW_APP_DIR
    pp = env.get('PYTHONPATH', '').strip()
    roots = [OPENCLAW_APP_DIR]
    if pp:
        existing = pp.split(os.pathsep)
        env['PYTHONPATH'] = os.pathsep.join(roots + [p for p in existing if p not in roots])
    else:
        env['PYTHONPATH'] = os.pathsep.join(roots)
    return env


def _check_deploy_paths() -> None:
    """启动时记录关键路径；AI 脚本缺失时打 WARN。"""
    if not os.path.isfile(AI_PROFILE_SCRIPT):
        LOG.warning('AI 画像脚本不存在: %s', AI_PROFILE_SCRIPT)
    if not os.path.isdir(PKL_DEALS_DIR):
        LOG.warning('PKL 目录不存在: %s', PKL_DEALS_DIR)


def run_ai_profile_sync(query: str):
    cmd = [
        PYTHON_PATH,
        AI_PROFILE_SCRIPT,
        '--query-text',
        str(query),
        '--data-dir',
        PKL_DEALS_DIR,
        '--print-stdout',
    ]
    return subprocess.run(
        cmd,
        cwd=OPENCLAW_APP_DIR,
        env=_subprocess_env_for_openclaw(),
        capture_output=True,
        text=True,
        timeout=AI_PROFILE_TIMEOUT_SEC,
    )


def run_pkl_similarity_sync(
    users_csv: str,
    same_symbol: bool = False,
    window_days: Optional[int] = None,
    min_rate_pct: float = 0.0,
):
    cmd = [
        PYTHON_PATH,
        PKL_SIMILARITY_SCRIPT,
        '--data-dir',
        POSITION_DIST_DATA_DIR,
        '--print-stdout',
    ]
    if window_days is not None:
        wd = max(1, int(window_days))
        end_d = datetime.now(BJT).date()
        start_d = end_d - timedelta(days=wd - 1)
        cmd += ['--start', start_d.isoformat(), '--end', end_d.isoformat()]
    if users_csv.strip():
        cmd += ['--users', users_csv]
    if min_rate_pct > 0:
        cmd += ['--min-rate-pct', str(float(min_rate_pct))]
    if same_symbol:
        cmd.append('--same-symbol')
    return subprocess.run(
        cmd,
        cwd=OPENCLAW_APP_DIR,
        capture_output=True,
        text=True,
        timeout=ANALYSIS_TIMEOUT_SEC,
    )


def run_contract_activity_sync(unit: str, n: int, top: int = 5):
    top = max(1, min(50, int(top)))
    cmd = [PYTHON_PATH, CONTRACT_ACTIVITY_SCRIPT, '--top', str(top)]
    if unit == 'month':
        cmd += ['--months', str(int(n))]
    else:
        cmd += ['--weeks', str(int(n))]
    return subprocess.run(
        cmd,
        cwd=OPENCLAW_APP_DIR,
        capture_output=True,
        text=True,
        timeout=ANALYSIS_TIMEOUT_SEC,
    )


def run_spot_activity_sync(unit: str, n: int, top: int = 5):
    top = max(1, min(50, int(top)))
    cmd = [PYTHON_PATH, SPOT_ACTIVITY_SCRIPT, '--top', str(top)]
    if unit == 'month':
        cmd += ['--months', str(int(n))]
    else:
        cmd += ['--weeks', str(int(n))]
    return subprocess.run(
        cmd,
        cwd=OPENCLAW_APP_DIR,
        capture_output=True,
        text=True,
        timeout=ANALYSIS_TIMEOUT_SEC,
    )


def run_monthly_profit_top_sync(month: int, year: Optional[int], top: int):
    cmd = [
        PYTHON_PATH,
        MONTHLY_PROFIT_TOP_SCRIPT,
        '--month',
        str(int(month)),
        '--top',
        str(int(top)),
        '--print_stdout',
    ]
    if year is not None:
        cmd += ['--year', str(int(year))]
    return subprocess.run(
        cmd,
        cwd=OPENCLAW_APP_DIR,
        capture_output=True,
        text=True,
        timeout=ANALYSIS_TIMEOUT_SEC,
    )


def run_rolling_profit_top_sync(unit: str, n: int, top: int):
    cmd = [
        PYTHON_PATH,
        MONTHLY_PROFIT_TOP_SCRIPT,
        '--rolling-unit',
        str(unit),
        '--rolling-n',
        str(int(n)),
        '--top',
        str(int(top)),
        '--simple-output',
        '--print_stdout',
    ]
    return subprocess.run(
        cmd,
        cwd=OPENCLAW_APP_DIR,
        capture_output=True,
        text=True,
        timeout=ANALYSIS_TIMEOUT_SEC,
    )


def run_position_dist_sync(all_symbols: bool, symbol: str):
    import tempfile

    fd, tmp_path = tempfile.mkstemp(suffix='.xlsx')
    os.close(fd)
    cmd = [
        PYTHON_PATH,
        POSITION_DIST_SCRIPT,
        '--data-dir',
        POSITION_DIST_DATA_DIR,
        '--output-xlsx',
        tmp_path,
    ]
    if all_symbols:
        cmd.append('--all-symbols')
        mx = (os.environ.get('TG_POSITION_DIST_MAX_SYMBOLS', '') or '').strip()
        if mx.isdigit() and int(mx) > 0:
            cmd.extend(['--max-symbols', mx])
    else:
        cmd.extend(['--symbol', str(symbol or '').strip()])
    try:
        proc = subprocess.run(
            cmd,
            cwd=OPENCLAW_APP_DIR,
            capture_output=True,
            text=True,
            timeout=POSITION_DIST_TIMEOUT_SEC,
        )
        return proc
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def run_symbol_hold_snapshot_sync(symbol: str, top_n: int = 3):
    cmd = [
        PYTHON_PATH,
        SYMBOL_HOLD_SCRIPT,
        '--symbol',
        str(symbol or '').strip(),
        '--top',
        str(int(top_n)),
    ]
    return subprocess.run(
        cmd,
        cwd=OPENCLAW_APP_DIR,
        capture_output=True,
        text=True,
        timeout=SYMBOL_HOLD_TIMEOUT_SEC,
    )


def run_spot_balance_sync(coin: str):
    cmd = [PYTHON_PATH, SPOT_BALANCE_SCRIPT, str(coin)]
    return subprocess.run(
        cmd,
        cwd=OPENCLAW_APP_DIR,
        capture_output=True,
        text=True,
        timeout=SPOT_BALANCE_TIMEOUT_SEC,
    )


def run_follow_accounts_overview_sync(mode: str, days: int, detail_limit: int):
    cmd = [
        PYTHON_PATH,
        FOLLOW_ACCOUNTS_ANALYSIS_SCRIPT,
        '--mode',
        str(mode or 'window'),
        '--days',
        str(int(days or 3)),
        '--detail-limit',
        str(int(detail_limit)),
        '--print_stdout',
    ]
    return subprocess.run(
        cmd,
        cwd=OPENCLAW_APP_DIR,
        capture_output=True,
        text=True,
        timeout=ANALYSIS_TIMEOUT_SEC,
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message or not message.text:
        return

    text = message.text.strip()
    me = await context.bot.get_me()
    mention = '@{}'.format(me.username)

    if message.chat.type in ['group', 'supergroup']:
        if mention not in text:
            return
        query = text.replace(mention, '').strip()
    else:
        query = text

    if is_help_query(query):
        await message.reply_text(build_help_text())
        return

    spot_coin = parse_spot_balance_coin(query)
    if spot_coin:
        chat_id = message.chat_id
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            await message.reply_text('当前群已有任务在执行,请稍后再试。')
            return
        await message.reply_text('正在查询各账户 {} 余额...'.format(spot_coin))
        async with lock:
            try:
                proc = await asyncio.to_thread(run_spot_balance_sync, spot_coin)
            except subprocess.TimeoutExpired:
                await message.reply_text(
                    '余额查询超时（>{}s）,请稍后重试。'.format(SPOT_BALANCE_TIMEOUT_SEC)
                )
                return
            except Exception as e:
                LOG.exception('执行现货余额脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return
            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text(
                    '余额查询失败,exit_code={}\n{}'.format(proc.returncode, err)
                )
                return
            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('余额查询{}:\n{}'.format(suffix, chunk))
        return

    if is_all_position_dist_query(query):
        chat_id = message.chat_id
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            await message.reply_text('当前群已有任务在执行,请稍后再试。')
            return
        await message.reply_text('开始 PKL+API 全部交易对持仓分布（可能较慢）...')
        async with lock:
            try:
                proc = await asyncio.to_thread(run_position_dist_sync, True, '')
            except subprocess.TimeoutExpired:
                await message.reply_text(
                    '持仓分布查询超时（>{}s）,请稍后重试。'.format(POSITION_DIST_TIMEOUT_SEC)
                )
                return
            except Exception as e:
                LOG.exception('执行持仓分布脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return
            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text(
                    '持仓分布失败,exit_code={}\n{}'.format(proc.returncode, err)
                )
                return
            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('持仓分布{}:\n{}'.format(suffix, chunk))
        return

    dist_sym = parse_position_dist_symbol(query)
    if dist_sym:
        chat_id = message.chat_id
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            await message.reply_text('当前群已有任务在执行,请稍后再试。')
            return
        await message.reply_text('开始 PKL+API 持仓分布 {} ...'.format(dist_sym))
        async with lock:
            try:
                proc = await asyncio.to_thread(run_position_dist_sync, False, dist_sym)
            except subprocess.TimeoutExpired:
                await message.reply_text(
                    '持仓分布查询超时（>{}s）,请稍后重试。'.format(POSITION_DIST_TIMEOUT_SEC)
                )
                return
            except Exception as e:
                LOG.exception('执行持仓分布脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return
            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text(
                    '持仓分布失败,exit_code={}\n{}'.format(proc.returncode, err)
                )
                return
            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('持仓分布{}:\n{}'.format(suffix, chunk))
        return

    hold_sym = parse_symbol_hold_symbol(query)
    if hold_sym:
        chat_id = message.chat_id
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            await message.reply_text('当前群已有任务在执行,请稍后再试。')
            return
        await message.reply_text('开始拉取全市场持仓并统计 {} ...'.format(hold_sym))
        async with lock:
            try:
                proc = await asyncio.to_thread(run_symbol_hold_snapshot_sync, hold_sym, 3)
            except subprocess.TimeoutExpired:
                await message.reply_text(
                    '单合约持仓查询超时（>{}s）,请稍后重试。'.format(SYMBOL_HOLD_TIMEOUT_SEC)
                )
                return
            except Exception as e:
                LOG.exception('执行单合约持仓脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return

            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text(
                    '单合约持仓查询失败,exit_code={}\n{}'.format(proc.returncode, err)
                )
                return

            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('单合约持仓{}:\n{}'.format(suffix, chunk))
        return

    if is_spot_activity_query(query):
        wm = parse_contract_activity_weeks_months(query)
        if not wm:
            await message.reply_text('未识别到周期，请使用“近N周”或“近N月”，例如：查询近2周现货活跃度')
            return
        unit, n = wm
        window_desc = '近{}月'.format(n) if unit == 'month' else '近{}周'.format(n)
        top_n = parse_contract_activity_top_n(query)

        chat_id = message.chat_id
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            await message.reply_text('当前群已有任务在执行,请稍后再试。')
            return

        await message.reply_text(
            '开始现货活跃度查询（最不活跃 Top{}）... {}'.format(top_n, window_desc)
        )
        async with lock:
            try:
                proc = await asyncio.to_thread(run_spot_activity_sync, unit, n, top_n)
            except subprocess.TimeoutExpired:
                await message.reply_text('现货活跃度查询超时（>{}s）,请稍后重试。'.format(ANALYSIS_TIMEOUT_SEC))
                return
            except Exception as e:
                LOG.exception('执行现货活跃度脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return

            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text('现货活跃度查询失败,exit_code={}\n{}'.format(proc.returncode, err))
                return

            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('现货活跃度结果{}:\n{}'.format(suffix, chunk))
        return

    if is_contract_activity_query(query):
        wm = parse_contract_activity_weeks_months(query)
        if not wm:
            await message.reply_text('未识别到周期，请使用“近N周”或“近N月”，例如：查询近2周合约活跃度')
            return
        unit, n = wm
        window_desc = '近{}月'.format(n) if unit == 'month' else '近{}周'.format(n)
        top_n = parse_contract_activity_top_n(query)

        chat_id = message.chat_id
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            await message.reply_text('当前群已有任务在执行,请稍后再试。')
            return

        await message.reply_text(
            '开始合约活跃度查询（各榜最不活跃交易对 Top{}）... {}'.format(top_n, window_desc)
        )
        async with lock:
            try:
                proc = await asyncio.to_thread(run_contract_activity_sync, unit, n, top_n)
            except subprocess.TimeoutExpired:
                await message.reply_text('合约活跃度查询超时（>{}s）,请稍后重试。'.format(ANALYSIS_TIMEOUT_SEC))
                return
            except Exception as e:
                LOG.exception('执行合约活跃度脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return

            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text('合约活跃度查询失败,exit_code={}\n{}'.format(proc.returncode, err))
                return

            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('合约活跃度结果{}:\n{}'.format(suffix, chunk))
        return

    if is_follow_accounts_overview_query(query):
        detail_query = strip_full_display_flag(query)
        specs = normalize_requested_spec(parse_time_specs(detail_query))
        wants_net_value, wants_position, wants_trades = _follow_query_flags(query)
        if not specs:
            # 仅查询净值/持仓时，允许不带日期；给一个默认窗口用于内部参数传递
            if wants_trades:
                await message.reply_text('未识别到时间范围,请使用“近N天/N周/N月”')
                return
            specs = [('window', 3)]

        mode, days = specs[0]
        window_desc = '全部/全量' if mode == 'all' else '近{}天'.format(days)
        detail_limit = 0 if has_full_display_flag(query) else 100

        chat_id = message.chat_id
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            await message.reply_text('当前群已有任务在执行,请稍后再试。')
            return

        target_parts = []
        if wants_net_value:
            target_parts.append('净值')
        if wants_position:
            target_parts.append('持仓')
        if wants_trades:
            target_parts.append('成交明细')
        target_desc = '/'.join(target_parts) if target_parts else '净值/持仓/成交明细'
        await message.reply_text('开始带单账户{}查询 ...窗口={}'.format(target_desc, window_desc))
        async with lock:
            try:
                proc = await asyncio.to_thread(
                    run_follow_accounts_overview_sync,
                    mode,
                    days or 3,
                    detail_limit,
                )
            except subprocess.TimeoutExpired:
                await message.reply_text('带单账户查询超时（>{}s）,请稍后重试。'.format(ANALYSIS_TIMEOUT_SEC))
                return
            except Exception as e:
                LOG.exception('执行带单账户查询脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return

            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text('带单账户查询失败,exit_code={}\n{}'.format(proc.returncode, err))
                return

            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('带单账户查询结果{}:\n{}'.format(suffix, chunk))
        return

    if is_detail_query(query):
        user_id = parse_detail_user_id(query)
        if not user_id:
            await message.reply_text('未识别到6～7位用户ID，请在问题中带上用户ID，例如：给我558255用户近1天的交易明细')
            return

        detail_query = strip_full_display_flag(query)
        abs_rng = parse_campaign_bjt_range(detail_query)
        range_s = range_e = None  # type: Optional[str]
        if abs_rng:
            range_s, range_e = abs_rng
            mode, days = 'window', 3
            window_desc = '{} ～ {}'.format(range_s, range_e)
        else:
            specs = normalize_requested_spec(parse_time_specs(detail_query))
            if not specs:
                await message.reply_text(
                    '未识别到时间范围,请使用“近N天/N周/N月”，或活动窗内指定日/时段（2026-04-01～04-10 北京时间）'
                )
                return
            mode, days = specs[0]
            window_desc = '全部/全量' if mode == 'all' else '近{}天'.format(days)
        detail_limit = 0 if has_full_display_flag(query) else 100

        chat_id = message.chat_id
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            await message.reply_text('当前群已有任务在执行,请稍后再试。')
            return

        await message.reply_text('开始成交明细查询 user_id={} 窗口={} ...'.format(user_id, window_desc))
        async with lock:
            try:
                proc = await asyncio.to_thread(
                    run_analysis_sync,
                    user_id,
                    mode,
                    days,
                    ANALYSIS_SCRIPT,
                    True,
                    detail_limit,
                    range_s,
                    range_e,
                )
            except subprocess.TimeoutExpired:
                await message.reply_text('成交明细查询超时（>{}s）,请稍后重试。'.format(ANALYSIS_TIMEOUT_SEC))
                return
            except Exception as e:
                LOG.exception('执行成交明细脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return

            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text('成交明细查询失败,exit_code={}\n{}'.format(proc.returncode, err))
                return

            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('成交明细结果{}:\n{}'.format(suffix, chunk))
        return

    if is_monthly_profit_top_query(query):
        parsed = parse_monthly_profit_top_params(query)
        if not parsed:
            await message.reply_text(
                '未识别到日历月或人数，例如：3月份盈利最多的10个用户交易、2026年3月盈利最多前5名'
            )
            return
        month, year, top_n = parsed
        y_label = str(year) if year is not None else '今年(北京时间)'

        chat_id = message.chat_id
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            await message.reply_text('当前群已有任务在执行,请稍后再试。')
            return

        await message.reply_text(
            '开始自然月盈利榜查询 ... {}-{:02d}（{}） Top{}'.format(
                y_label, month, '北京时间', top_n
            )
        )
        async with lock:
            try:
                proc = await asyncio.to_thread(run_monthly_profit_top_sync, month, year, top_n)
            except subprocess.TimeoutExpired:
                await message.reply_text(
                    '自然月盈利榜查询超时（>{}s）,请稍后重试。'.format(ANALYSIS_TIMEOUT_SEC)
                )
                return
            except Exception as e:
                LOG.exception('执行自然月盈利榜脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return

            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text(
                    '自然月盈利榜查询失败,exit_code={}\n{}'.format(proc.returncode, err)
                )
                return

            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('自然月盈利榜结果{}:\n{}'.format(suffix, chunk))
        return

    if is_rolling_profit_top_query(query):
        parsed = parse_rolling_profit_top_params(query)
        if not parsed:
            await message.reply_text(
                '未识别到滚动窗口或人数，例如：查询上3周盈利最多的10个用户、查询上2月盈利最多前5名用户'
            )
            return
        unit, n, top_n = parsed
        window_desc = '上{}周'.format(n) if unit == 'week' else '上{}月'.format(n)

        chat_id = message.chat_id
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            await message.reply_text('当前群已有任务在执行,请稍后再试。')
            return

        await message.reply_text(
            '开始滚动盈利榜查询 ... {} Top{}（返回用户ID与盈利金额）'.format(window_desc, top_n)
        )
        async with lock:
            try:
                proc = await asyncio.to_thread(run_rolling_profit_top_sync, unit, n, top_n)
            except subprocess.TimeoutExpired:
                await message.reply_text(
                    '滚动盈利榜查询超时（>{}s）,请稍后重试。'.format(ANALYSIS_TIMEOUT_SEC)
                )
                return
            except Exception as e:
                LOG.exception('执行滚动盈利榜脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return

            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text(
                    '滚动盈利榜查询失败,exit_code={}\n{}'.format(proc.returncode, err)
                )
                return

            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('滚动盈利榜结果{}:\n{}'.format(suffix, chunk))
        return

    if is_ai_profile_query(query):
        chat_id = message.chat_id
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            await message.reply_text('当前群已有任务在执行,请稍后再试。')
            return
        await message.reply_text('开始 Gemini AI 用户画像分析（可能需要 1～3 分钟）...')
        async with lock:
            try:
                proc = await asyncio.to_thread(run_ai_profile_sync, query)
            except subprocess.TimeoutExpired:
                await message.reply_text(
                    'AI 用户画像超时（>{}s）,请稍后重试或缩小时间范围。'.format(AI_PROFILE_TIMEOUT_SEC)
                )
                return
            except Exception as e:
                LOG.exception('执行 AI 用户画像脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return

            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text(
                    'AI 用户画像失败,exit_code={}\n{}'.format(proc.returncode, err)
                )
                return

            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('AI 用户画像{}:\n{}'.format(suffix, chunk))
        return

    if is_pkl_user_group_similarity_query(query):
        uids = parse_similarity_user_ids(query)
        q_ns = query.replace(' ', '')
        same_sym = '同合约' in q_ns
        window_days = parse_similarity_window_days(query)
        min_rate_pct = parse_similarity_min_rate_pct(query)
        chat_id = message.chat_id
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            await message.reply_text('当前群已有任务在执行,请稍后再试。')
            return
        if len(uids) < 1:
            await message.reply_text(
                '未识别到基准用户ID。示例：对比近7天与123456用户成交相似度超过80%的用户'
            )
            return
        await message.reply_text(
            '开始 PKL 交易相似度分析（{}，{}，阈值>{}%）{}...'.format(
                '基准用户{}'.format(uids[0]),
                '全部时间' if window_days is None else '近{}天'.format(window_days),
                ('{:.2f}'.format(min_rate_pct)).rstrip('0').rstrip('.'),
                '（同合约）' if same_sym else '',
            )
        )
        async with lock:
            try:
                proc = await asyncio.to_thread(
                    run_pkl_similarity_sync,
                    ','.join(uids),
                    same_sym,
                    window_days,
                    min_rate_pct,
                )
            except subprocess.TimeoutExpired:
                await message.reply_text(
                    'PKL 用户组相似度查询超时（>{}s）,可设 TG_ANALYSIS_TIMEOUT_SEC 或精简用户列表'.format(
                        ANALYSIS_TIMEOUT_SEC
                    )
                )
                return
            except Exception as e:
                LOG.exception('执行 PKL 用户组相似度脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return

            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text(
                    'PKL 用户组相似度失败,exit_code={}\n{}'.format(proc.returncode, err)
                )
                return

            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('PKL 用户组相似度{}:\n{}'.format(suffix, chunk))
        return

    if is_pkl_multi_rank_query(query):
        top_n = parse_pkl_multi_rank_top_n(query)
        chat_id = message.chat_id
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            await message.reply_text('当前群已有任务在执行,请稍后再试。')
            return
        await message.reply_text(
            '开始 PKL 多维度 Top{} 详细分析（三榜+calc_trade_stats，可能需 1～3 分钟）...'.format(top_n)
        )
        async with lock:
            try:
                proc = await asyncio.to_thread(run_pkl_multi_rank_sync, query, top_n)
            except subprocess.TimeoutExpired:
                await message.reply_text(
                    'PKL 多维度分析超时（>{}s）,请缩小时间范围或调大 TG_PKL_MULTI_RANK_TIMEOUT_SEC'.format(
                        PKL_MULTI_RANK_TIMEOUT_SEC
                    )
                )
                return
            except Exception as e:
                LOG.exception('执行 PKL 多维度分析脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return

            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text(
                    'PKL 多维度分析失败,exit_code={}\n{}'.format(proc.returncode, err)
                )
                return

            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('PKL 多维度分析{}:\n{}'.format(suffix, chunk))
        return

    if is_pkl_query(query):
        chat_id = message.chat_id
        lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
        if lock.locked():
            await message.reply_text('当前群已有任务在执行,请稍后再试。')
            return
        await message.reply_text('开始本地PKL数据查询...')
        async with lock:
            try:
                proc = await asyncio.to_thread(run_pkl_query_sync, query)
            except subprocess.TimeoutExpired:
                await message.reply_text('PKL查询超时（>{}s）,请稍后重试。'.format(ANALYSIS_TIMEOUT_SEC))
                return
            except Exception as e:
                LOG.exception('执行PKL查询脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return

            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text('PKL查询失败,exit_code={}\n{}'.format(proc.returncode, err))
                return

            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('PKL查询结果{}:\n{}'.format(suffix, chunk))
        return
    
    user_id = parse_user_id(query)
    if not user_id:
        await message.reply_text('请按格式提问:分析123456用户近n天/n周/n月交易数据,或 分析123456是否为刷单用户')
        return

    is_wash = is_wash_query(query)
    script_path = WASH_ANALYSIS_SCRIPT if is_wash else ANALYSIS_SCRIPT
    abs_rng = parse_campaign_bjt_range(query)
    range_s = range_e = None  # type: Optional[str]
    if abs_rng:
        range_s, range_e = abs_rng
        specs = [('window', 3)]
    else:
        specs = normalize_requested_spec(parse_time_specs(query))
    if not specs:
        if is_wash:
            specs = [('all', None)]
        else:
            await message.reply_text(
                '未识别到时间范围,请使用“近N天/N周/N月”或“全部/全量”，或活动窗内指定日/时段（2026-04-01～04-10 北京时间）'
            )
            return

    action_text = '刷量排查' if is_wash else '交易分析'
    chat_id = message.chat_id
    lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
    if lock.locked():
        await message.reply_text('当前群已有任务在执行,请稍后再试。')
        return

    await message.reply_text('开始{} user_id={} ...'.format(action_text, user_id))
    async with lock:
        for idx, (mode, days) in enumerate(specs, start=1):
            if abs_rng:
                window_desc = '{} ～ {}'.format(range_s, range_e)
            else:
                window_desc = '全部/全量' if mode == 'all' else '近{}天'.format(days)
            await message.reply_text('[{}/{}] {}窗口:{} ...'.format(idx, len(specs), action_text, window_desc))
            try:
                proc = await asyncio.to_thread(
                    run_analysis_sync,
                    user_id,
                    mode,
                    days,
                    script_path,
                    range_start=range_s,
                    range_end=range_e,
                )
            except subprocess.TimeoutExpired:
                await message.reply_text('{}超时（>{}s）,请稍后重试。'.format(action_text, ANALYSIS_TIMEOUT_SEC))
                return
            except Exception as e:
                LOG.exception('执行分析脚本失败: %s', e)
                await message.reply_text('执行失败: {}'.format(e))
                return

            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or '').strip()
                err = err[-1500:] if err else '无错误输出'
                await message.reply_text('{}失败,exit_code={}\n{}'.format(action_text, proc.returncode, err))
                return

            out = (proc.stdout or '').strip()
            for i, chunk in enumerate(split_text(out), start=1):
                suffix = '' if i == 1 else '（续{}）'.format(i)
                await message.reply_text('{}结果{}:\n{}'.format(action_text, suffix, chunk))


def main():
    if not BOT_TOKEN:
        raise RuntimeError('请配置环境变量 TG_BOT_TOKEN')
    _check_deploy_paths()
    LOG.info(
        'openclaw_app_dir=%s ai_model_dir=%s pkl_deals_dir=%s ai_profile_script=%s python=%s',
        OPENCLAW_APP_DIR,
        AI_MODEL_DIR,
        PKL_DEALS_DIR,
        AI_PROFILE_SCRIPT,
        PYTHON_PATH,
    )
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    LOG.info('Bot 正在运行...')
    app.run_polling()


if __name__ == '__main__':
    main()
