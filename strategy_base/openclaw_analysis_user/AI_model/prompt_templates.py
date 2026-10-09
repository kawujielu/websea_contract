# -*- coding: utf-8 -*-
"""Gemini 分析 Prompt 模板。"""
from __future__ import annotations

SEVEN_MODULE_INSTRUCTION = """
你是一名量化交易用户画像分析助手。请严格基于下方提供的成交与统计数据做客观推断，不要编造数据中不存在的字段；证据不足时请写明「证据不足」。

请按以下 7 个模块输出（每模块用标题行 + 1～3 句要点），最后附一段「总结」（全文总字数不超过 500 字）：

1. 用户交易分类：如高频剥头皮、趋势跟随、资金费率套利、跟单型、刷量型、赌博型等（可多标签）
2. 用户稳定性画像：盈利来源稳定性、持续稳定 alpha、运气型暴赚、单次黑天鹅、高波动赌徒等
3. 用户生命周期：新用户、活跃增长期、策略成熟期、衰退期、爆仓边缘、流失预警等
4. 市场操纵嫌疑：是否表现为每次开仓后 1～2 分钟内常伴随大波动（需结合时间戳与盈亏推断）
5. 实时风险账户：是否长期处于爆仓风险边缘、是否大量单边持仓（结合杠杆、持仓时长、盈亏波动推断）
6. 市场冲击：是否总有大额市价成交特征（结合 taker 占比、单笔名义金额推断）
7. 策略推测：如网格、马丁、套利、跟单等

输出使用简体中文。
""".strip()


def build_time_resolve_prompt(query_text: str, today_bjt: str) -> str:
    return "\n".join(
        [
            "你是时间解析助手。根据用户提问提取查询所用的北京时间自然日区间。",
            "今天（北京时间）是: {}".format(today_bjt),
            "",
            "规则：",
            "- 若提问中有明确或可推断的时间范围，只输出一行：YYYY-MM-DD到YYYY-MM-DD（含首尾日）",
            "- 若完全无法推断时间范围，只输出一行：NONE",
            "- 不要输出任何其它文字、标点或解释",
            "",
            "用户提问：",
            query_text.strip(),
        ]
    )


def build_profile_prompt(original_query: str, data_block: str) -> str:
    return "\n".join(
        [
            SEVEN_MODULE_INSTRUCTION,
            "",
            "--- 用户原始提问 ---",
            original_query.strip(),
            "",
            "--- 数据 ---",
            data_block.strip(),
        ]
    )


def build_raw_answer_prompt(original_query: str, data_block: str) -> str:
    return "\n".join(
        [
            "--- 用户原始提问 ---",
            original_query.strip(),
            "",
            "--- 逐笔成交明细 (CSV) ---",
            data_block.strip() if data_block.strip() else "(无)",
        ]
    )
