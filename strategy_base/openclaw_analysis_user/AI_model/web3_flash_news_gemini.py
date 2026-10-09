# -*- coding: utf-8 -*-
"""
Web3 主流媒体快讯 RSS + Gemini 归纳（参数均在下方 CONFIG，供 crontab 定时执行）。

crontab 示例（每 10 分钟，Ubuntu）:
  */10 * * * * /usr/bin/python3 /path/to/web3_flash_news_gemini.py >> /path/to/logs/cron.stdout 2>&1

依赖: pip install google-generativeai
Telegram: CONFIG 中 ENABLE_TELEGRAM / TG_BOT_TOKEN / TG_CHAT_ID
"""
from __future__ import annotations

import email.utils
import json
import os
import re
import sys
import traceback
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from urllib.error import URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

# =============================================================================
# 配置区（按需修改，勿从命令行传参）
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 时间窗口：优先最近 N 分钟；无条目则放宽到 EXTEND_IF_EMPTY_MINUTES；仍无则 RSS 顺序回退
WINDOW_MINUTES = 10
EXTEND_IF_EMPTY_MINUTES = 60
FALLBACK_PER_SOURCE = 5

# Gemini
ENABLE_GEMINI = True
GEMINI_MODEL = "models/gemini-2.5-flash"
# 留空则尝试从下方 OPENCLAW_CODE_DIRS 导入 AI_model.gemini_client 的 Key
GOOGLE_API_KEY = ""

# 网络
RSS_TIMEOUT_SEC = 20
USER_AGENT = "Mozilla/5.0 (compatible; Web3FlashNewsBot/1.0; +cron)"

# 输出：按北京时间日期追加到 LOG_DIR；cron 可把 stdout 重定向，此处默认也写文件
LOG_DIR = os.path.join(SCRIPT_DIR, "logs", "web3_flash_news")
WRITE_STDOUT = True

# Telegram（cron 每 10 分钟推送 Gemini 归纳；失败仅记日志，不中断写文件）
ENABLE_TELEGRAM = True
TG_BOT_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"
TG_CHAT_ID = 1843312449
TG_MSG_PREFIX = "Web3快讯 "
TG_CHUNK_MAX = 3900
# Telegram 推送时剔除含该短语的段落，仅保留有价值内容
TG_SKIP_PHRASE = "窗口内信息不足，无法判断"

# 复用 gemini_client 时依次尝试的 code 目录（Ubuntu 生产路径放第一位）
OPENCLAW_CODE_DIRS: Tuple[str, ...] = (
    "/home/ubuntu/strategy_base/strategy/openclaw_analysis_user",
    os.path.join(
        os.path.expanduser("~"),
        "OneDrive",
        "websea",
        "AI AGENT",
        "sky量化工作的skills",
        "用户分析",
        "code",
    ),
)

# =============================================================================

BJT = ZoneInfo("Asia/Shanghai")

for _d in OPENCLAW_CODE_DIRS:
    _d = (_d or "").strip()
    if _d and os.path.isdir(_d) and _d not in sys.path:
        sys.path.insert(0, _d)

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
            raise RuntimeError("请在脚本 CONFIG 中设置 GOOGLE_API_KEY")
        genai.configure(api_key=api_key)
        model_id = (model_name or GEMINI_MODEL).strip()
        model = genai.GenerativeModel(model_id)
        response = model.generate_content(prompt)
        text_out = getattr(response, "text", None)
        if text_out:
            return str(text_out).strip()
        return str(response).strip()


GEMINI_PREAMBLE = (
    "你是一名 Web3 与加密市场快讯分析员。"
    "以下是从指定主流媒体 RSS 抓取的、在指定时间窗口内发布的条目原文信息。"
    "请仅基于下列条目做客观归纳，不要编造未出现的媒体、链接、金额或监管结论。"
    "重点关注：监管/执法、交易所/稳定币脱锚、黑客与漏洞、宏观与 ETF、巨鲸与清算、"
    "主流公链与 BTC/ETH/SOL 及 USDT/USDC 等稳定币相关、可能引发短线价格波动的快讯。"
)

MEDIA_RSS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("CoinDesk", ("https://www.coindesk.com/arc/outboundfeeds/rss/",)),
    ("Cointelegraph", ("https://cointelegraph.com/rss",)),
    ("The Block", ("https://www.theblock.co/rss.xml",)),
    ("Decrypt", ("https://decrypt.co/feed",)),
    (
        "Foresight News",
        (
            "https://foresightnews.pro/feed",
            "https://foresightnews.pro/rss.xml",
        ),
    ),
    ("TechFlow 深潮", ("https://techflowpost.substack.com/feed",)),
    (
        "BlockBeats 律动",
        (
            "https://api.theblockbeats.news/v2/rss/newsflash",
            "https://api.theblockbeats.news/v1/open-api/home-xml",
        ),
    ),
)

ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}


@dataclass
class NewsItem:
    source: str
    title: str
    link: str
    published: Optional[datetime]
    summary: str


def _sanitize_xml_bytes(raw: bytes) -> bytes:
    text = raw.decode("utf-8", errors="replace")
    text = re.sub(r"<script\b[^>]*>.*?</script>", "", text, flags=re.I | re.S)
    text = re.sub(r"<script\s*/>", "", text, flags=re.I)
    return text.encode("utf-8")


def _fetch_bytes(url: str, timeout: int) -> bytes:
    req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/rss+xml,*/*"})
    with urlopen(req, timeout=timeout) as resp:
        return _sanitize_xml_bytes(resp.read())


def _parse_datetime(text: str) -> Optional[datetime]:
    text = (text or "").strip()
    if not text:
        return None
    try:
        dt = email.utils.parsedate_to_datetime(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError):
        pass
    for _ in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
        try:
            s = text.replace("Z", "+00:00") if text.endswith("Z") else text
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def _strip_html(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s[:2000]


def _parse_rss_channel(root: ET.Element, source: str) -> List[NewsItem]:
    items: List[NewsItem] = []
    channel = root.find("channel")
    if channel is not None:
        for it in channel.findall("item"):
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            pub_raw = (
                it.findtext("pubDate")
                or it.findtext("{http://purl.org/dc/elements/1.1/}date")
                or ""
            )
            pub = _parse_datetime(pub_raw)
            desc = _strip_html(it.findtext("description") or it.findtext("{*}encoded") or "")
            if title or link:
                items.append(NewsItem(source, title, link, pub, desc))
    for entry in root.findall("atom:entry", ATOM_NS):
        title = (entry.findtext("atom:title", default="", namespaces=ATOM_NS) or "").strip()
        link_el = entry.find("atom:link", ATOM_NS)
        link = ""
        if link_el is not None:
            link = (link_el.get("href") or link_el.text or "").strip()
        pub = _parse_datetime(
            entry.findtext("atom:published", default="", namespaces=ATOM_NS)
            or entry.findtext("atom:updated", default="", namespaces=ATOM_NS)
        )
        summary = _strip_html(
            entry.findtext("atom:summary", default="", namespaces=ATOM_NS)
            or entry.findtext("atom:content", default="", namespaces=ATOM_NS)
        )
        if title or link:
            items.append(NewsItem(source, title, link, pub, summary))
    return items


def fetch_media_items(timeout: int) -> Tuple[List[NewsItem], List[str]]:
    all_items: List[NewsItem] = []
    errors: List[str] = []
    for name, urls in MEDIA_RSS:
        last_err = ""
        for url in urls:
            try:
                raw = _fetch_bytes(url, timeout)
                if not raw.strip():
                    raise ValueError("empty body")
                root = ET.fromstring(raw)
                got = _parse_rss_channel(root, name)
                if got:
                    all_items.extend(got)
                    last_err = ""
                    break
                last_err = "empty feed"
            except (URLError, ET.ParseError, TimeoutError, ValueError) as e:
                last_err = "{} ({})".format(url, e)
        if last_err:
            errors.append("{}: {}".format(name, last_err))
    return all_items, errors


def filter_strict_time_window(
    items: List[NewsItem],
    minutes: int,
    now_utc: Optional[datetime] = None,
) -> List[NewsItem]:
    now_utc = now_utc or datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(minutes=max(1, int(minutes)))
    out: List[NewsItem] = []
    for it in items:
        if it.published is None:
            continue
        if it.published >= cutoff:
            out.append(it)
    out.sort(
        key=lambda x: x.published or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return out


def filter_fallback_by_feed_order(items: List[NewsItem]) -> List[NewsItem]:
    by_source: Dict[str, List[NewsItem]] = {}
    for it in items:
        by_source.setdefault(it.source, []).append(it)
    fallback: List[NewsItem] = []
    for name, _urls in MEDIA_RSS:
        fallback.extend(by_source.get(name, [])[:FALLBACK_PER_SOURCE])
    return fallback


def format_items_for_prompt(
    items: List[NewsItem],
    window_minutes: int,
    now_bjt: datetime,
    filter_mode: str,
) -> str:
    mode_desc = (
        "严格时间窗口（最近 {} 分钟）".format(window_minutes)
        if filter_mode == "time_window"
        else "各源 RSS 最新 {} 条（回退）".format(FALLBACK_PER_SOURCE)
    )
    lines = [
        "--- 抓取元数据 ---",
        "筛选方式: {}".format(mode_desc),
        "截至 {} 北京时间".format(now_bjt.strftime("%Y-%m-%d %H:%M:%S")),
        "条目数: {}".format(len(items)),
        "媒体源: {}".format(", ".join(n for n, _ in MEDIA_RSS)),
        "",
        "--- RSS 条目（标题 / 时间 / 摘要 / 链接）---",
    ]
    if not items:
        lines.append("(未获取到任何条目)")
        return "\n".join(lines)

    for i, it in enumerate(items, start=1):
        pub_bjt = ""
        if it.published:
            pub_bjt = it.published.astimezone(BJT).strftime("%Y-%m-%d %H:%M:%S")
        lines.extend(
            [
                "[{}] 来源: {}".format(i, it.source),
                "标题: {}".format(it.title or "(无标题)"),
                "发布时间(北京时间): {}".format(pub_bjt or "未知"),
                "摘要: {}".format(it.summary or "(无)"),
                "链接: {}".format(it.link or "(无)"),
                "",
            ]
        )
    return "\n".join(lines).rstrip()


def build_gemini_prompt(items_block: str, window_minutes: int) -> str:
    return "\n".join(
        [
            GEMINI_PREAMBLE,
            "",
            items_block,
            "",
            "--- 请输出（中文）---",
            "1) 【快讯列表】按「可能价格影响」从高到低列出 {} 条以内要点，每条注明来源媒体；".format(
                min(15, max(3, window_minutes))
            ),
            "2) 【主流币/稳定币】单独说明对 BTC、ETH、SOL 及 USDT/USDC 等的潜在短线影响方向（利多/利空/不确定）及理由；",
            "3) 【交易提示】若信息不足请明确写「RSS 窗口内信息不足」，勿臆测未列出的新闻；",
            "4) 总字数控制在 800 字以内，条列清晰。",
        ]
    )


def run_report() -> str:
    minutes = WINDOW_MINUTES
    extend_if_empty = EXTEND_IF_EMPTY_MINUTES
    timeout = RSS_TIMEOUT_SEC

    now_utc = datetime.now(timezone.utc)
    now_bjt = now_utc.astimezone(BJT)
    all_items, fetch_errors = fetch_media_items(timeout)

    window_used = max(1, int(minutes))
    picked = filter_strict_time_window(all_items, window_used, now_utc)
    filter_mode = "time_window"
    extended_note = ""

    if not picked and extend_if_empty and int(extend_if_empty) > window_used:
        window_used = int(extend_if_empty)
        picked = filter_strict_time_window(all_items, window_used, now_utc)
        extended_note = "（原 {} 分钟无条目，已放宽到 {} 分钟）".format(minutes, window_used)

    if not picked:
        picked = filter_fallback_by_feed_order(all_items)
        filter_mode = "feed_fallback"
        if not extended_note:
            extended_note = "（启用各源 RSS 最新条目回退）"
        else:
            extended_note += "；并启用 RSS 顺序回退"

    items_block = format_items_for_prompt(picked, window_used, now_bjt, filter_mode)
    header_lines = [
        "=" * 60,
        "Web3 主流媒体快讯抓取",
        "当前时间(北京时间): {}".format(now_bjt.strftime("%Y-%m-%d %H:%M:%S")),
        "筛选窗口: 最近 {} 分钟{}".format(window_used, extended_note),
        "筛选模式: {}".format(filter_mode),
        "RSS 解析总条目(含更早): {}".format(len(all_items)),
        "输出条目: {}".format(len(picked)),
    ]
    if fetch_errors:
        header_lines.append("抓取异常: " + "; ".join(fetch_errors))
    header_lines.append("=" * 60)
    header_lines.append("")
    header_lines.append(items_block)

    report = "\n".join(header_lines)

    if not ENABLE_GEMINI:
        report += "\n\n[INFO] CONFIG.ENABLE_GEMINI=False，跳过 Gemini。\n"
        return report

    if not get_api_key():
        report += "\n\n[INFO] 无可用 GOOGLE_API_KEY，跳过 Gemini。\n"
        return report

    prompt = build_gemini_prompt(items_block, window_used)
    try:
        ai_text = generate_content(prompt, GEMINI_MODEL)
        report += "\n\n" + "=" * 60 + "\n--- Gemini 快讯分析 ---\n\n" + ai_text.strip() + "\n"
    except Exception as e:
        report += "\n\n[ERROR] Gemini 调用失败: {}\n".format(e)
    return report


def _chunk_telegram_text(text: str, max_len: int = TG_CHUNK_MAX) -> List[str]:
    if len(text) <= max_len:
        return [text]
    chunks: List[str] = []
    cur: List[str] = []
    cur_len = 0
    for line in text.splitlines(True):
        if cur_len + len(line) > max_len and cur:
            chunks.append("".join(cur))
            cur = [line]
            cur_len = len(line)
        else:
            cur.append(line)
            cur_len += len(line)
    if cur:
        chunks.append("".join(cur))
    return chunks


def _split_telegram_paragraphs(body: str) -> List[str]:
    body = (body or "").strip()
    if not body:
        return []
    if re.search(r"\n\s*\n", body):
        return [p.strip() for p in re.split(r"\n\s*\n+", body) if p.strip()]
    lines = body.splitlines()
    blocks: List[str] = []
    cur: List[str] = []
    sec_re = re.compile(r"^\s*(\d+\)|【)")
    for line in lines:
        if sec_re.match(line) and cur:
            blocks.append("\n".join(cur).strip())
            cur = [line]
        else:
            cur.append(line)
    if cur:
        blocks.append("\n".join(cur).strip())
    return blocks if len(blocks) > 1 else [body]


def _filter_telegram_body(body: str) -> str:
    kept = [p for p in _split_telegram_paragraphs(body) if TG_SKIP_PHRASE not in p]
    return "\n\n".join(kept).strip()


def _telegram_body_for_send(report: str) -> str:
    marker = "--- Gemini 快讯分析 ---"
    if marker in report:
        body = report.split(marker, 1)[1].strip()
    else:
        body = report.strip()
    body = _filter_telegram_body(body)
    if len(body) > TG_CHUNK_MAX * 3:
        body = body[: TG_CHUNK_MAX * 3] + "\n\n...(内容过长已截断)"
    return body


def send_telegram(report: str) -> bool:
    if not ENABLE_TELEGRAM:
        return False
    token = (TG_BOT_TOKEN or "").strip()
    if not token:
        return False
    body = _telegram_body_for_send(report)
    if not body:
        sys.stderr.write("[telegram] 过滤后无可推送内容，跳过\n")
        return True
    text = (TG_MSG_PREFIX + body).strip()
    url = "https://api.telegram.org/bot{}/sendMessage".format(token)
    ok = True
    for part in _chunk_telegram_text(text):
        payload = urllib.parse.urlencode(
            {
                "chat_id": str(TG_CHAT_ID),
                "text": part,
                "disable_web_page_preview": "true",
            }
        ).encode("utf-8")
        req = Request(url, data=payload, method="POST")
        try:
            with urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            data = json.loads(raw)
            if not data.get("ok"):
                ok = False
                sys.stderr.write("[telegram] API 返回失败: {}\n".format(raw[:500]))
        except (URLError, TimeoutError, json.JSONDecodeError) as e:
            ok = False
            sys.stderr.write("[telegram] 发送失败: {}\n".format(e))
    return ok


def emit_output(text: str) -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(
        LOG_DIR,
        "web3_flash_{}.log".format(datetime.now(BJT).strftime("%Y%m%d")),
    )
    stamp = datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S")
    block = "\n\n########## {} ##########\n{}\n".format(stamp, text.rstrip() + "\n")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(block)
    if WRITE_STDOUT:
        sys.stdout.write(text)
        if not text.endswith("\n"):
            sys.stdout.write("\n")


def main() -> int:
    try:
        report = run_report()
        emit_output(report)
        send_telegram(report)
        return 0
    except Exception:
        err = traceback.format_exc()
        fatal = "[FATAL]\n" + err
        emit_output(fatal)
        send_telegram(fatal)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

