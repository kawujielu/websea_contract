# -*- coding: utf-8 -*-
"""
对 CONFIG 中指定的一批 user_id，从日切 PKL 加载成交、逐用户 calc_trade_stats，
将统计结果拼成字符串后调用 Gemini，按交易风格分类。

最终输出：按交易风格分组，如「高频组」「低频交易组」等；
每组内每行 user_id | 总盈亏 | 描述（≤20字）

所有参数写在下方 CONFIG，不从命令行或环境变量读取业务配置。
"""
from __future__ import annotations

import os
import sys
import traceback
import warnings
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
from zoneinfo import ZoneInfo

# =============================================================================
# 配置区
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CODE_DIR = os.path.dirname(SCRIPT_DIR)

# 待分析用户 ID（按需增删）
USER_IDS: Tuple[str, ...] = tuple(
    str(x)
    for x in (
        '632478','640288','575415','573572','613028','584084','585466','515951','643913','491475','501530','533273','640691','610652','574045','619168','640692','674219','679991','572926','642765','537616','538033','614992','670245','573455','674743','489086','543264','596824','598048','608069','619152','632879','642771','644907','558876','617175','633209','643126','643138','646278','670660','514921','537718','578752','583746','594388','598511','599936','630430','647040','674744','485999','491041','553190','558255','599110','601768','611840','624951','624952','629151','638600','647046','1481626','492202','609347','647928','680202','420262','469302','471491','479345','479490','479610','480960','481049','481068','484069','484354','485969','486091','487577','489262','490054','490243','490591','490772','502112','503031','512589','514649','519533','520049','520235','520265','520908','523656','523696','524375','524725','525411','527889','531145','532489','534253','534634','536741','538732','538813','539910','540369','540446','540485','541119','541414','543539','543628','545873','547103','548630','550826','551598','551676','551757','552040','552262','552447','552768','554277','554713','556061','556088','556426','556599','556651','556728','557053','557786','557850','558314','558943','559181','559217','559533','559926','560205','560647','560673','570425','570488','571774','572521','572834','573528','573651','573824','573826','573827','574067','574142','574243','574595','575106','575108','575166','575176','575586','576547','576622','577343','577375','577980','578059','578638','578811','579077','579233','579287','579431','579485','579568','579694','579707','580097','580208','580379','580408','580609','580624','580708','580964','581349','581376','581380','581548','581738','582075','582101','582160','582414','582421','582635','582656','582695','582926','583012','583170','584004','584067','584128','584328','584333','584500','584866','585334','586319','586364','586379','586463','586572','587195','587580','587672','587824','588256','588529','588781','588986','589126','589151','589791','589980','590553','590648','590697','591645','591702','591934','592348','592851','593521','593621','593637','594271','594976','595355','595428','595767','596594','596675','598171','599056','599059','599685','599971','600156','600335','600597','600709','600785','601086','601514','602171','602943','603118','603371','603839','604207','604632','604810','605091','605409','605431','607746','607814','608196','609256','609432','610038','612788','615162','615577','616380','616742','617288','617492','617624','618590','618658','618916','619114','621961','624565','624919','625828','625958','626386','626506','629039','629273','629425','629788','631379','632314','632681','632948','633002','638566','639031','639163','639346','639657','639668','639882','640051','640504','640731','640736','640752','640968','641415','642228','642231','645571','646296','646484','646830','647009','647068','647212','647323','649931','649951','670160','670536','671302','674315','674453','680018','680279','680389','680477','680631','680646','680662','680713','680759','681158','1481243','1481348','1481405','1481457','1481625','1481800','1481830','1481866','1481957','1482124','1482266','1482273','1482277','1482524','1482700','1483480'
    )
)

# 日切 PKL 目录
DATA_DIR = os.path.join(CODE_DIR, "data", "mongo_daily_deals_pkl")

# 统计窗口（北京时间自然日，闭区间，格式 YYYY-MM-DD）
START_DATE = "2026-05-01"
END_DATE = "2026-06-08"

# Gemini
ENABLE_GEMINI = True
GEMINI_MODEL = "models/gemini-2.5-flash"
GOOGLE_API_KEY = ""  # 留空则走 gemini_client 默认
GEMINI_BATCH_SIZE = 100  # 每批发送 Gemini 的用户数

# 输出（stdout 仅打印 Gemini 结果；完整日志含统计明细写入 LOG_DIR）
WRITE_STDOUT = True
LOG_DIR = os.path.join(SCRIPT_DIR, "logs", "user_trade_style")

# =============================================================================

BJT = ZoneInfo("Asia/Shanghai")

for _p in (CODE_DIR, SCRIPT_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from mongdb_order_stats import calc_trade_stats  # noqa: E402
from pkl_user_query_analyzer import (  # noqa: E402
    _pkl_legs_to_calc_trade_stats_df,
    ensure_user_id,
    load_window_df,
)

try:
    from AI_model.gemini_client import generate_content as _gc_generate_content
    from AI_model.gemini_client import get_api_key as _gc_get_api_key

    def get_api_key() -> str:
        k = (GOOGLE_API_KEY or "").strip()
        return k if k else _gc_get_api_key()

    def generate_content(prompt: str, model_name: Optional[str] = None) -> str:
        return _gc_generate_content(prompt, model_name=model_name or GEMINI_MODEL)

except ImportError:

    def get_api_key() -> str:
        return (GOOGLE_API_KEY or "").strip()

    def generate_content(prompt: str, model_name: Optional[str] = None) -> str:
        try:
            import google.generativeai as genai
        except ImportError as e:
            raise RuntimeError(
                "未安装 google-generativeai，请执行: pip install google-generativeai"
            ) from e
        api_key = get_api_key()
        if not api_key:
            raise RuntimeError("请在 CONFIG 中设置 GOOGLE_API_KEY")
        genai.configure(api_key=api_key)
        model_id = (model_name or GEMINI_MODEL).strip()
        model = genai.GenerativeModel(model_id)
        response = model.generate_content(prompt)
        text_out = getattr(response, "text", None)
        if text_out:
            return str(text_out).strip()
        return str(response).strip()


warnings.filterwarnings(
    "ignore",
    message="overflow encountered",
    category=RuntimeWarning,
)

# 与 mongdb_order_stats.calc_trade_stats 中暂时注释掉的指标保持一致
EXCLUDED_STAT_KEYS: frozenset = frozenset(
    {
        "统计指标",
        "taker金额",
        "maker金额",
        "刨除手续费总盈亏",
        "最大持仓时长",
        "最短持仓时长",
        "周均交易次数",
        "月均交易次数",
    }
)
STAT_KEYS: Tuple[str, ...] = tuple(
    k for k in calc_trade_stats(pd.DataFrame()).keys() if k not in EXCLUDED_STAT_KEYS
)

GEMINI_PREAMBLE = (
    "你是一名量化交易分析员。"
    "以下是一批用户在指定时间窗口内的 calc_trade_stats 交易统计。"
    "请先判断每位用户的交易风格，再按风格将用户分组输出，不要编造未出现的字段或结论。"
)

GEMINI_OUTPUT_RULES = (
    "--- 请输出 ---\n"
    "按交易风格将用户分组输出。每个分组先写组名（以「组」结尾，如「高频组」「低频交易组」「"
    "跟单导向组」「波段持仓组」「高杠杆激进组」「无成交数据组」等），组名单独一行；\n"
    "组内每个用户各占一行，格式严格为：\n"
    "user_id | 总盈亏 | 描述\n"
    "说明：\n"
    "1) 总盈亏使用上文该用户统计中的「总盈亏」数值；\n"
    "2) 描述为该用户在本组风格下的简要特征，控制在 20 个汉字（或字符）以内；\n"
    "3) 风格分组须能被上文指标支撑，相近风格可合并为一组，差异明显应拆组；\n"
    "4) 无成交或无有效统计的用户归入「无成交数据组」，总盈亏写 0，描述写「窗口内无成交」；\n"
    "5) 每个 user_id 只能出现在一个组内，须覆盖本批次中的全部用户；\n"
    "6) 只输出分组标题与组内用户行，不要附加总述、分析段落或 Markdown。\n"
    "输出示例：\n"
    "高频组：\n"
    "632588 | 1234.56 | 超短线高换手剥头皮\n"
    "621582 | -320.10 | 日内频繁开平追涨\n"
    "\n"
    "低频交易组：\n"
    "539308 | 8000.00 | 持仓数日低频波段\n"
)


def _format_trade_nature(tn) -> str:
    if isinstance(tn, dict):
        return "自主{} 跟单{} 其他{}".format(
            tn.get("自主交易", 0),
            tn.get("跟单交易", 0),
            tn.get("其他", 0),
        )
    return str(tn)


def _format_stat_value(key: str, val) -> str:
    if key == "交易性质":
        return _format_trade_nature(val)
    if key == "分币对盈亏":
        s = str(val or "").strip()
        return s if s else "(无)"
    if val is None:
        return ""
    return str(val)


def _format_stats_dict(res: Dict) -> str:
    lines = []
    seen = set()
    for k in STAT_KEYS:
        if k in res:
            lines.append("{}: {}".format(k, _format_stat_value(k, res[k])))
            seen.add(k)
    for k in sorted(x for x in res if x not in seen and x not in EXCLUDED_STAT_KEYS):
        lines.append("{}: {}".format(k, _format_stat_value(k, res[k])))
    return "\n".join(lines)


def _resolve_window() -> Tuple[date, date, str]:
    start_s = (START_DATE or "").strip()
    end_s = (END_DATE or "").strip()
    if not start_s or not end_s:
        raise ValueError("请在 CONFIG 中设置 START_DATE 与 END_DATE，格式 YYYY-MM-DD")
    start_d = datetime.strptime(start_s, "%Y-%m-%d").date()
    end_d = datetime.strptime(end_s, "%Y-%m-%d").date()
    if start_d > end_d:
        raise ValueError("START_DATE 不能晚于 END_DATE")
    label = "{} ~ {}（北京时间自然日）".format(start_d.isoformat(), end_d.isoformat())
    return start_d, end_d, label


def _dates_inclusive(start_d: date, end_d: date) -> List[date]:
    out: List[date] = []
    cur = start_d
    while cur <= end_d:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _filter_target_users_raw(df: pd.DataFrame, uids: Tuple[str, ...]) -> pd.DataFrame:
    if df.shape[0] == 0:
        return df
    uset = {str(u) for u in uids}
    t = df["taker_user"].astype(str)
    m = df["maker_user"].astype(str)
    return df[t.isin(uset) | m.isin(uset)].copy()


def _emit_stdout(text: str) -> None:
    if WRITE_STDOUT and text:
        sys.stdout.write(text)
        if not text.endswith("\n"):
            sys.stdout.write("\n")


def _chunk_user_ids(user_ids: Tuple[str, ...], batch_size: int) -> List[Tuple[str, ...]]:
    size = max(1, int(batch_size))
    uids = [str(u) for u in user_ids]
    return [tuple(uids[i : i + size]) for i in range(0, len(uids), size)]


def _filter_bjt_date_range(df: pd.DataFrame, start_d: date, end_d: date) -> pd.DataFrame:
    if df.shape[0] == 0:
        return df
    ts = pd.to_datetime(df["ts_text"], errors="coerce")
    lo = pd.Timestamp(datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0))
    hi = pd.Timestamp(datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59))
    return df[(ts >= lo) & (ts <= hi)].copy()


def calc_users_trade_stats(
    df: pd.DataFrame,
    user_ids: Tuple[str, ...],
) -> Tuple[str, Dict[str, Dict]]:
    """返回 (拼好的统计文本, {uid: stats_dict})。"""
    blocks: List[str] = []
    cache: Dict[str, Dict] = {}
    uid_set = [str(u) for u in user_ids]

    for uid in uid_set:
        header = "=" * 60 + "\nuser_id: {}\n".format(uid) + "=" * 60
        sub = df[df["user_id"].astype(str) == uid].copy()
        if sub.shape[0] == 0:
            block = header + "\n(窗口内无成交数据)"
            cache[uid] = {}
        else:
            s_df = _pkl_legs_to_calc_trade_stats_df(sub, uid)
            if s_df.shape[0] == 0:
                block = header + "\n(无有效成交腿)"
                cache[uid] = {}
            else:
                try:
                    stats = calc_trade_stats(s_df, userid=uid)
                    cache[uid] = stats
                    block = header + "\n" + _format_stats_dict(stats)
                except Exception as e:
                    block = header + "\n统计失败: {}".format(e)
                    cache[uid] = {}
        blocks.append(block)

    meta = [
        "--- 批次元数据 ---",
        "用户数量: {}".format(len(uid_set)),
        "user_id 列表: {}".format(", ".join(uid_set)),
        "",
    ]
    return "\n".join(meta + blocks).rstrip(), cache


def build_gemini_prompt(
    stats_text: str,
    window_label: str,
    batch_idx: int,
    total_batches: int,
    batch_uids: Tuple[str, ...],
) -> str:
    return "\n".join(
        [
            GEMINI_PREAMBLE,
            "",
            "--- 元数据 ---",
            "时间窗口: {}（北京时间自然日）".format(window_label),
            "总用户数: {}".format(len(USER_IDS)),
            "当前批次: {}/{}".format(batch_idx, total_batches),
            "本批用户数: {}".format(len(batch_uids)),
            "",
            "--- 各用户 calc_trade_stats 统计 ---",
            stats_text.strip(),
            "",
            GEMINI_OUTPUT_RULES,
        ]
    )


def _call_gemini_batches(
    df: pd.DataFrame,
    window_label: str,
    log_lines: List[str],
) -> str:
    batches = _chunk_user_ids(USER_IDS, GEMINI_BATCH_SIZE)
    total_batches = len(batches)
    gemini_parts: List[str] = []

    log_lines.append(
        "Gemini 分批: 每批 {} 用户，共 {} 批".format(GEMINI_BATCH_SIZE, total_batches)
    )
    log_lines.append("")

    for batch_idx, batch_uids in enumerate(batches, start=1):
        stats_text, _ = calc_users_trade_stats(df, batch_uids)
        log_lines.append("--- 批次 {}/{} 交易统计（{} 用户）---".format(
            batch_idx, total_batches, len(batch_uids)
        ))
        log_lines.append(stats_text)
        log_lines.append("")

        prompt = build_gemini_prompt(
            stats_text, window_label, batch_idx, total_batches, batch_uids
        )
        try:
            ai_text = generate_content(prompt, GEMINI_MODEL)
            section = "\n".join(
                [
                    "=" * 60,
                    "Gemini 批次 {}/{}（{} 用户）".format(
                        batch_idx, total_batches, len(batch_uids)
                    ),
                    "=" * 60,
                    ai_text.strip(),
                    "",
                ]
            )
            gemini_parts.append(section)
            _emit_stdout(section)
        except Exception as e:
            err = "[ERROR] 批次 {}/{} Gemini 调用失败: {}\n".format(
                batch_idx, total_batches, e
            )
            gemini_parts.append(err)
            log_lines.append(err.rstrip())
            _emit_stdout(err)

    return "\n".join(gemini_parts).rstrip() + ("\n" if gemini_parts else "")


def run_classification() -> Tuple[str, str]:
    start_d, end_d, window_label = _resolve_window()
    day_list = _dates_inclusive(start_d, end_d)
    df_raw, exists, miss = load_window_df(DATA_DIR, day_list)

    header_lines = [
        "=" * 60,
        "批量用户交易风格分类",
        "时间窗口: {}".format(window_label),
        "PKL 目录: {}".format(DATA_DIR),
        "数据覆盖: {}/{} 日".format(len(exists), len(day_list)),
        "缺失日期: {}".format(", ".join(miss) if miss else "无"),
        "用户数: {}".format(len(USER_IDS)),
        "=" * 60,
        "",
    ]

    if df_raw.shape[0] == 0:
        header_lines.append("[ERROR] 窗口内无可用 PKL 数据")
        msg = "\n".join(header_lines) + "\n"
        _emit_stdout(msg)
        return msg, msg

    df = ensure_user_id(df_raw)
    df = _filter_target_users_raw(df, USER_IDS)
    df = _filter_bjt_date_range(df, start_d, end_d)

    if df.shape[0] == 0:
        header_lines.append("[ERROR] 目标用户在窗口内无成交记录")
        msg = "\n".join(header_lines) + "\n"
        _emit_stdout(msg)
        return msg, msg

    if not ENABLE_GEMINI:
        info = "[INFO] CONFIG.ENABLE_GEMINI=False，跳过 Gemini。\n"
        header_lines.append(info.rstrip())
        _emit_stdout(info)
        return info, "\n".join(header_lines).rstrip() + "\n"

    if not get_api_key():
        info = "[INFO] 无可用 GOOGLE_API_KEY，跳过 Gemini。\n"
        header_lines.append(info.rstrip())
        _emit_stdout(info)
        return info, "\n".join(header_lines).rstrip() + "\n"

    gemini_stdout = _call_gemini_batches(df, window_label, header_lines)
    if gemini_stdout.strip():
        header_lines.append("--- Gemini 分批分类结果 ---")
        header_lines.append(gemini_stdout.rstrip())
    full_log = "\n".join(header_lines).rstrip() + "\n"
    return gemini_stdout, full_log


def emit_output(full_text: str) -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(
        LOG_DIR,
        "trade_style_{}.log".format(datetime.now(BJT).strftime("%Y%m%d_%H%M%S")),
    )
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(full_text)


def main() -> int:
    try:
        _gemini_out, full_log = run_classification()
        emit_output(full_log)
        return 0
    except Exception:
        err = traceback.format_exc()
        fatal = "[FATAL]\n" + err
        _emit_stdout(fatal)
        emit_output(fatal)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

