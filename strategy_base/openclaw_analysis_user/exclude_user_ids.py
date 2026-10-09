from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Set

# 与 ToolBoxNew.load_account 使用同一默认路径
ACCOUNT_CONFIG_PATH = os.environ.get(
    "UPM_ACCOUNT_CONFIG_JSON",
    "/home/ubuntu/CCGo/resources/account.config.json",
)


def load_maker_uid_set_from_account_config() -> Set[str]:
    """从 account.config.json 读取做市等业务账户 uid（逻辑对齐 ToolBoxNew.load_account，但不依赖 Contract）。"""
    out: Set[str] = set()
    if not os.path.isfile(ACCOUNT_CONFIG_PATH):
        return out
    try:
        with open(ACCOUNT_CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        for _k, v in data.get("websea", {}).items():
            desc = str(v.get("description", "") or "")
            if "测试" in desc:
                continue
            uid_raw = str(v.get("uid", "") or "")
            id_part = uid_raw.split(",")[0].strip() if uid_raw else ""
            if not id_part:
                id_part = "risk"
            if id_part != "risk":
                out.add(str(id_part))
    except Exception:
        pass
    return out


def load_sim_uid_set_from_env() -> Set[str]:
    raw = (os.environ.get("UPM_SIM_USER_IDS", "") or "").strip()
    if not raw:
        return set()
    return {x.strip() for x in raw.split(",") if x.strip()}


def load_extra_exclude_from_env() -> Set[str]:
    raw = (os.environ.get("UPM_EXTRA_EXCLUDE_USER_IDS", "") or "").strip()
    if not raw:
        return set()
    return {x.strip() for x in raw.split(",") if x.strip()}


def _cache_ttl_sec() -> float:
    return float(os.environ.get("UPM_EXCLUDE_IDS_CACHE_SEC", "3600"))


_cache_set: Set[str] | None = None
_cache_time: float = 0.0


def clear_excluded_user_ids_cache() -> None:
    """清空内存中的排除用户缓存，强制下次 get_excluded_* 重新合并（含重新拉模拟金）。"""
    global _cache_set, _cache_time
    _cache_set = None
    _cache_time = 0.0


async def _fetch_sim_uid_set_via_toolbox() -> Set[str]:
    if os.environ.get("UPM_FETCH_SIM_USERS", "1") != "1":
        return set()
    try:
        from ToolBoxNew import ToolBox
    except ImportError:
        return set()
    try:
        tb = ToolBox()
        users = await tb.get_sim_user()
        return {str(u) for u in (users or [])}
    except Exception:
        return set()


async def get_excluded_user_ids_async() -> Set[str]:
    global _cache_set, _cache_time
    now = time.time()
    if _cache_set is not None and now - _cache_time < _cache_ttl_sec():
        return _cache_set
    makers = load_maker_uid_set_from_account_config()
    merged = makers | load_sim_uid_set_from_env() | load_extra_exclude_from_env()
    merged |= await _fetch_sim_uid_set_via_toolbox()
    _cache_set = merged
    _cache_time = now
    return merged


def get_excluded_user_ids_sync() -> Set[str]:
    """供同步脚本（如 PKL 分析、单笔 on_demand）使用；会按需拉取模拟金列表并带缓存。"""
    global _cache_set, _cache_time
    now = time.time()
    if _cache_set is not None and now - _cache_time < _cache_ttl_sec():
        return _cache_set
    makers = load_maker_uid_set_from_account_config()
    merged = makers | load_sim_uid_set_from_env() | load_extra_exclude_from_env()
    if os.environ.get("UPM_FETCH_SIM_USERS", "1") == "1":
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            try:
                merged |= asyncio.run(_fetch_sim_uid_set_via_toolbox())
            except Exception:
                pass
    _cache_set = merged
    _cache_time = now
    return merged