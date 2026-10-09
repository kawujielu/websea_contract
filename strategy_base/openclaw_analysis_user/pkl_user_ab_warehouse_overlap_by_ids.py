"""ScopeFi 跟单核心逻辑 — scopefi核心逻辑.py

根据 leader 成交通知 (mess) 计算跟单数量，经 HL 取价、精度格式化后，
通过 OpenHub 向 follow 账户下单。单进程批量处理 r_copy_trading_config 中
status=0 的全部配置，不再按 config 起多个子进程。

同目录依赖:
  hl_fetch_price、hl_format_precision、copy_trading_db_cli、
  query_follower_account_cli、query_wallet_position、scopefi下单测试脚本、
  scopefi_rmq_consumer（--rmq 模式）

连接（默认手动 SSH 隧道，见 .env）:
  ssh -i id_ed25519_sky -N -L 1592:localhost:1592 -L 5432:localhost:5432 sky@3.38.144.74
  RABBITMQ_STRATEGY_URL=amqp://sky:***@localhost:1592/strategy
  PostgreSQL / ScopeFi API / OpenHub 经本机转发或公网 URL（.env 配置）

================================================================================
一、运行方式
================================================================================

  # RMQ 实盘跟单（推荐：消费 QUEUE_FULL_FILLED，匹配到的 config 批量跟单）
  python scopefi核心逻辑.py --rmq --live

  # RMQ 模拟下单（dry_run，只组装 OpenHub 请求体，不 POST）
  python scopefi核心逻辑.py --rmq

  # 后台定时任务（无 RMQ：main 每 10s、pos_risk/check_deal_time 每 60s）
  python scopefi核心逻辑.py --live

  # 单次执行风控或闲置解锁（遍历全部 config）
  python scopefi核心逻辑.py --pos-risk
  python scopefi核心逻辑.py --check-deal-time

  # Ubuntu 后台常驻（RMQ 实盘；stdout 追加到同目录 logs/rmq.log）
  cd /path/to/scopefi
  nohup python3 scopefi核心逻辑.py --rmq --live >> logs/rmq.log 2>&1 &

日志（Ubuntu / 任意 OS，路径与 cwd 无关，均为「本脚本同目录/logs/」）:
  logs/runtime.jsonl       启动、pos_risk/check_deal_time/main 定时任务
  logs/rmq_all.jsonl       --rmq 全量成交（含过期跳过）
  logs/leader_push.jsonl   已锁定 leader 的成交子集
  logs/deal_result.jsonl   每条 RMQ 跟单处理结果
  logs/order.jsonl         下单（含 dry_run 与 --live）
  logs/deal_error.jsonl    单 config 跟单异常
  logs/rmq.log             nohup 时 print 输出（可选，非 JSON）

CLI 参数:
  --rmq              订阅 RabbitMQ QUEUE_FULL_FILLED（需 aio_pika）
  --live             实盘下单；不加则 dry_run=True（只打印不下单）
  --pos-risk         仅执行一次 pos_risk（持仓风控）
  --check-deal-time  仅执行一次 check_deal_time（成交闲置解锁）

================================================================================
二、代码逻辑概览
================================================================================

启动阶段 (__main__):
  boot_copy_trading_config_monitor()
    → 立即读库刷新 COPY_TRADING_CONFIG_MONITOR
    → 后台每 60s 再刷新（config 行 + state.leaders + last_deal_ts）

scopefi_trade 实例:
  __init__ 校验 DB/RMQ/OpenHub 连接，启动三条后台线程（--rmq 时关闭 main 循环）:
    - main          每 10s（有 self.mess 时跟单；RMQ 模式由 handle_rmq_message 驱动）
    - pos_risk      每 60s  遍历全部 config 做持仓风控
    - check_deal_time 每 60s  遍历全部 config 做闲置解锁

RMQ 成交流程 (handle_rmq_message → main):
  1. 解析 ORDER_FULL_FILLED：leader 地址、coin、dir、sz、startPosition
  2. _filter_deal_ids：在 monitor 快照中找需跟单的 config
       - lock_leader_ids:   coin 匹配且 leaders 已锁定该成交地址
       - un_lock_leader_ids: leaders=0 且成交地址在 address_pool 内（首次锁定）
  3. 查 leader 权益；批量查各 config 的 follower 权益
  4. 对每个匹配的 config_id:
       _apply_monitor_item → _process_deal_for_config
       按 follow_direction 映射方向 → 按 model + deal_model 算量 → _place_order

跟单模式 (model / follow_mode):
  IMMEDIATE_FULL     按 leader 目标仓位比例跟
  DELTA_FROM_ZERO    从 0 仓位起算增量（首锁要求 startPosition=0）
  INCREASE_ONLY      仅跟 Open，Close 跳过
  REVERSE_REDUCE_ONLY 仅减仓方向跟单

下单量模式 (deal_model / position_mode):
  AUTO_RATIO         按 leader 权益比例缩放 follow 账户
  FIXED_NOTIONAL     按 fixed_amt 固定名义金额

辅助逻辑:
  pos_risk           leader 标记空仓后，若 follow 仍有同 coin 仓则强平
  check_deal_time    各 config 独立 last_deal_ts，超过 inactive_timeout_hours 无下单则
                     将 leaders 置 0 并全平该 config 的 coin 仓位
  _first_lock_leader 首次锁定：写 state.leaders + 平 follow 同 coin 旧仓

last_deal_ts（按 config_id 独立）:
  - 存储: self._last_deal_ts_by_id[id] 与 COPY_TRADING_CONFIG_MONITOR 每项同步
  - 更新: 仅 _place_order 成功路径调用 _touch_last_deal_ts(config_id)
  - 闲置判定: check_deal_time 读取该 id 的 last_deal_ts 与 inactive_timeout_hours 比较

全局缓存 COPY_TRADING_CONFIG_MONITOR:
  每项 = r_copy_trading_config 字段 + leaders(0 或地址列表) + last_deal_ts(10 位秒)
"""
import asyncio
import importlib.util
import sys
import threading
import time
import traceback
from dataclasses import asdict
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

import scopefi_log  # noqa: F401 — 全项目 print 带时间戳

import copy_trading_db_cli as copy_trading_db
import query_follower_account_cli as follower_acct
import query_wallet_position as wallet_query
from copy_trading_db_cli import env_float, env_str

from hl_fetch_price import DEFAULT_HL_INFO_URL, fetch_mid_price
from hl_format_precision import DEFAULT_SLIPPAGE, format_order_fields

_ORDER_SCRIPT = _SCRIPT_DIR / "scopefi下单测试脚本.py"
_order_place_module: Any = None

def _load_order_place_module() -> Any:
    """动态加载 scopefi下单测试脚本.py(中文文件名，缓存单例)。

    从同目录 scopefi下单测试脚本.py 加载模块对象，
    供 OrderPayload、OpenHubClient 等下单类使用。

    Returns:
        已加载的下单脚本模块。

    Raises:
        FileNotFoundError: 候选路径均不存在。
        ImportError: spec 或 loader 无效。
    """
    global _order_place_module
    if _order_place_module is not None:
        return _order_place_module
    script_path = _ORDER_SCRIPT
    if not script_path.is_file():
        raise FileNotFoundError(f"未找到 scopefi下单测试脚本.py: {script_path}")
    spec = importlib.util.spec_from_file_location("_scopefi_order_place", script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载下单模块: {script_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    _order_place_module = mod
    return mod


# r_copy_trading_config 监控缓存（由 refresh_copy_trading_config_monitor 每 60s 刷新）
COPY_TRADING_CONFIG_MONITOR: list[dict[str, Any]] = []


def _parse_last_deal_ts(value: Any) -> int | None:
    """从 monitor 项中解析 last_deal_ts，非法值返回 None。"""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    return None


def _prev_last_deal_ts_map(prev: list[dict[str, Any]]) -> dict[int, int]:
    """从上一轮 snapshot 列表提取 config id -> last_deal_ts。"""
    result: dict[int, int] = {}
    for item in prev:
        cid = item.get("id")
        ts = _parse_last_deal_ts(item.get("last_deal_ts"))
        if cid is not None and ts is not None:
            result[int(cid)] = ts
    return result


def _merge_last_deal_ts_by_id(
    config_ids: set[int],
    prev: list[dict[str, Any]],
) -> dict[int, int]:
    """按 config id 合并 last_deal_ts；新 id 初始化为当前 10 位时间戳。"""
    result = _prev_last_deal_ts_map(prev)
    now = int(time.time())
    for config_id in config_ids:
        if config_id not in result:
            result[config_id] = now
    return result


def _build_monitor_config_item(
    row: copy_trading_db.CopyTradingConfigRow,
    *,
    leaders: list[str] | int,
    last_deal_ts: int,
) -> dict[str, Any]:
    """将 config 行与 state 字段合并为 monitor 快照单项 dict。"""
    item = asdict(row)
    item["leaders"] = leaders
    item["last_deal_ts"] = last_deal_ts
    return item


def refresh_copy_trading_config_monitor(
    *,
    no_tunnel: bool | None = None,
    spawn_status_filter: int | None = 0,
    stop_status_filter: int | None = None,
) -> list[dict[str, Any]]:
    """读库刷新跟单配置监控快照，写入 COPY_TRADING_CONFIG_MONITOR。

    查询 r_copy_trading_config（默认 status=0）与 r_copy_trading_state.leaders，
    合并 last_deal_ts（新 id 用当前时间，已有 id 保留原值）。

    Returns:
        快照 list，每项含 config 全字段 + leaders + last_deal_ts。
    """
    global COPY_TRADING_CONFIG_MONITOR

    copy_trading_db.load_project_env()
    db_no_tunnel = (
        copy_trading_db.default_db_no_tunnel()
        if no_tunnel is None
        else no_tunnel
    )
    copy_trading_db.ensure_ssh_forwards(db_no_tunnel=db_no_tunnel)

    ids_spawn_set, _ids_stop_set = copy_trading_db.fetch_config_id_sets(
        no_tunnel=db_no_tunnel,
        spawn_status_filter=spawn_status_filter,
        stop_status_filter=stop_status_filter,
    )

    prev = COPY_TRADING_CONFIG_MONITOR
    last_deal_ts_by_id = _merge_last_deal_ts_by_id(ids_spawn_set, prev)

    snapshot: list[dict[str, Any]] = []
    for config_id in sorted(ids_spawn_set):
        row = copy_trading_db.fetch_config(config_id, no_tunnel=db_no_tunnel)
        state = copy_trading_db.fetch_state(config_id, no_tunnel=db_no_tunnel)
        if state is None or state.leaders is None:
            leaders: list[str] | int = 0
        else:
            leaders = state.leaders
        snapshot.append(
            _build_monitor_config_item(
                row,
                leaders=leaders,
                last_deal_ts=last_deal_ts_by_id[config_id],
            )
        )
    print(f"164行,刷新跟单配置监控快照,snapshot:{snapshot}")
    COPY_TRADING_CONFIG_MONITOR = snapshot
    return snapshot


CONFIG_MONITOR_INTERVAL_SEC = 60
_config_monitor_stop = threading.Event()
_config_monitor_thread: threading.Thread | None = None


def print_copy_trading_config_monitor(
    snapshot: list[dict[str, Any]] | None = None,
) -> None:
    """格式化打印 COPY_TRADING_CONFIG_MONITOR（或传入的 snapshot）到 stdout。"""
    items = snapshot if snapshot is not None else COPY_TRADING_CONFIG_MONITOR
    if not items:
        print("[config-monitor] 无配置快照")
        return

    print("=" * 60)
    print(f"[config-monitor] 跟单配置监控快照 ({len(items)} 条)")
    print("=" * 60)

    for item in sorted(items, key=lambda x: int(x.get("id") or 0)):
        config_id = item.get("id")
        follow_mode = copy_trading_db.FOLLOW_MODE_MAP.get(
            item.get("follow_mode"), item.get("follow_mode")
        )
        position_mode = copy_trading_db.POSITION_MODE_MAP.get(
            item.get("position_mode"), item.get("position_mode")
        )
        address_pool = item.get("address_pool") or []
        pool_preview = address_pool[:3]
        if len(address_pool) > 3:
            pool_preview = [*pool_preview, "..."]
        deal_ts = int(item.get("last_deal_ts") or time.time())
        deal_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(deal_ts))
        print(f"  --- config id={config_id} ---")
        print(
            f"      follower={item.get('follower')}  coin={item.get('coin')}  "
            f"status={item.get('status')}  leverage={item.get('leverage')}"
        )
        print(
            f"      follow_direction={item.get('follow_direction')}  "
            f"follow_mode={follow_mode}  position_mode={position_mode}  "
            f"fixed_amt={item.get('fixed_order_amount')}"
        )
        print(
            f"      inactive_timeout_hours={item.get('inactive_timeout_hours')}  "
            f"timeout_minutes={item.get('timeout_minutes')}  "
            f"pool_size={len(address_pool)}"
        )
        print(f"      address_pool: {pool_preview}")
        print(f"      last_deal_ts: {deal_ts}  ({deal_str})")
        print(f"      leaders: {item.get('leaders', 0)}")
    print("=" * 60)


def start_copy_trading_config_monitor_loop(
    *,
    no_tunnel: bool | None = None,
    interval_sec: float = CONFIG_MONITOR_INTERVAL_SEC,
) -> None:
    """启动 daemon 线程，每 interval_sec 秒调用 refresh + print。"""
    global _config_monitor_thread

    if _config_monitor_thread is not None and _config_monitor_thread.is_alive():
        return

    def _loop() -> None:
        """后台线程：定时刷新并打印配置监控快照。"""
        while not _config_monitor_stop.wait(interval_sec):
            try:
                snap = refresh_copy_trading_config_monitor(no_tunnel=no_tunnel)
                print_copy_trading_config_monitor(snap)
            except Exception as exc:
                print(f"[config-monitor] 刷新失败: {exc}", file=sys.stderr)

    _config_monitor_stop.clear()
    _config_monitor_thread = threading.Thread(
        target=_loop,
        name="config_monitor",
        daemon=True,
    )
    _config_monitor_thread.start()


def stop_copy_trading_config_monitor_loop() -> None:
    """设置停止事件，结束 config monitor 后台刷新线程。"""
    _config_monitor_stop.set()


def boot_copy_trading_config_monitor(
    *,
    no_tunnel: bool | None = None,
    interval_sec: float = CONFIG_MONITOR_INTERVAL_SEC,
) -> list[dict[str, Any]]:
    """CLI/Rmq 启动时调用：立即刷新一次快照并开启后台定时刷新。"""
    snapshot = refresh_copy_trading_config_monitor(no_tunnel=no_tunnel)
    print_copy_trading_config_monitor(snapshot)
    start_copy_trading_config_monitor_loop(
        no_tunnel=no_tunnel,
        interval_sec=interval_sec,
    )
    return snapshot


class scopefi_trade:
    """ScopeFi 跟单交易执行器（单实例批量处理全部 config）。

    不绑定单一 config_id；每次跟单/风控前从 COPY_TRADING_CONFIG_MONITOR
    加载当前 config 到实例属性（ID、coin、model、follower_wallet 等）。

    典型调用链:
      handle_rmq_message(mess) → main() → _process_deal_for_config() → _place_order()
    """

    def __init__(
        self,
        *,
        db_no_tunnel: bool | None = None,
        periodic_main: bool = True,
    ) -> None:
        """初始化跟单运行时环境（不绑定单一 config，按 monitor 批量处理）。

        Args:
            db_no_tunnel: True=使用本机 ssh -L 转发端口（默认，见 MANUAL_SSH_TUNNEL）；
                False=由程序自动建 PG SSH 隧道（USE_SSH_TUNNEL=1）。
            periodic_main: True 时后台每 60s 执行 main()；--rmq 模式应传 False。

        配置来源:
            COPY_TRADING_CONFIG_MONITOR（r_copy_trading_config 全表快照），
            每次跟单/风控前通过 _apply_monitor_item 加载到实例属性。
        """
        copy_trading_db.load_project_env()
        if db_no_tunnel is None:
            db_no_tunnel = copy_trading_db.default_db_no_tunnel()
        self._db_no_tunnel = db_no_tunnel
        copy_trading_db.print_connection_summary(db_no_tunnel=db_no_tunnel)
        copy_trading_db.ensure_ssh_forwards(db_no_tunnel=db_no_tunnel)
        copy_trading_db.verify_manual_ssh_ports(db_no_tunnel=db_no_tunnel)
        copy_trading_db.test_rabbitmq_connection()
        copy_trading_db.verify_openhub_reachable()

        hl_url = env_str("OPENHUB_HL_INFO_URL", default=DEFAULT_HL_INFO_URL).rstrip("/")
        self.hl_info_url = hl_url or DEFAULT_HL_INFO_URL
        self.slippage_ratio = env_float(
            "DEFAULT_SLIPPAGE_RATIO", default=DEFAULT_SLIPPAGE
        )
        # True=只构建 OpenHub 请求体，不 POST(与 scopefi下单测试脚本 --dry-run 一致)
        self.dry_run = False
        self._leader_pos_empty_by_id: dict[int, bool] = {}
        # 各 config_id 独立的上次下单时间（10 位 Unix 秒），与 monitor 快照同步
        self._last_deal_ts_by_id: dict[int, int] = {}
        self.agg_mess = []

        self.start_parallel_tasks(enable_main_loop=periodic_main)
    
    @staticmethod
    def _flatten_config_ids(ids: list[Any]) -> list[int]:
        """将可能嵌套的 config id 列表展平为 int 列表。"""
        out: list[int] = []
        for x in ids:
            if isinstance(x, list):
                out.extend(scopefi_trade._flatten_config_ids(x))
            else:
                out.append(int(x))
        return out

    @staticmethod
    def _leaders_match(leaders: Any, deal_address: str) -> bool:
        """判断成交 leader 地址是否与 state.leaders 匹配（leaders=0 时 False）。"""
        addr = deal_address.strip().lower()
        if leaders == 0:
            return False
        if isinstance(leaders, list):
            return any(addr == str(a).strip().lower() for a in leaders)
        return str(leaders).strip().lower() == addr

    def _filter_deal_ids(self, deal_address: str, deal_coin: str):
        """根据成交地址与 coin，筛选需跟单的 config id 列表。

        Returns:
            (lock_leader_ids, un_lock_leader_ids):
              - lock_leader_ids: 已锁定且 leaders 含该地址
              - un_lock_leader_ids: 未锁定且地址在 address_pool 内
        """
        lock_leader_ids: list[int] = []
        un_lock_leader_ids: list[int] = []
        coin = deal_coin.strip().upper()
        for item in COPY_TRADING_CONFIG_MONITOR:
            if (
                str(item.get("coin", "")).strip().upper() == coin
                and self._leaders_match(item.get("leaders"), deal_address)
            ):
                lock_leader_ids.append(int(item["id"]))
            if item.get("leaders") == 0 and deal_address in (item.get("address_pool") or []):
                un_lock_leader_ids.append(int(item["id"]))
        return lock_leader_ids, un_lock_leader_ids

    def _get_config_follower_pairs(
        self,
        lock_leader_ids: list[int],
        un_lock_leader_ids: list[int],
    ) -> tuple[list[tuple[int, str]], list[tuple[int, str]]]:
        """按 config id 从 monitor 取 (config_id, follower_wallet) 对。"""
        lock_ids = set(lock_leader_ids)
        unlock_ids = set(un_lock_leader_ids)
        lock_pairs: list[tuple[int, str]] = []
        unlock_pairs: list[tuple[int, str]] = []
        for item in COPY_TRADING_CONFIG_MONITOR:
            cid = int(item["id"])
            follower = str(item.get("follower") or "").strip()
            if not follower:
                continue
            if cid in lock_ids:
                lock_pairs.append((cid, follower))
            if cid in unlock_ids:
                unlock_pairs.append((cid, follower))
        return lock_pairs, unlock_pairs

    async def _batch_fetch_config_assets_async(
        self,
        lock_pairs: list[tuple[int, str]],
        unlock_pairs: list[tuple[int, str]],
    ) -> tuple[list[dict[int, float]], dict[str, dict[str, Any]]]:
        """批量查询 config follower 权益，返回 [{id: asset}, ...] 及 wallet->快照。"""
        id_to_wallet: dict[int, str] = {}
        for cid, wallet in lock_pairs + unlock_pairs:
            id_to_wallet[int(cid)] = wallet.strip()

        if not id_to_wallet:
            return [], {}

        wallets = list(dict.fromkeys(id_to_wallet.values()))
        reports = await wallet_query.fetch_wallet_positions(wallets)
        wallet_snaps: dict[str, dict[str, Any]] = {}
        for report in reports:
            wallet = str(report.get("wallet", "")).strip().lower()
            if wallet:
                wallet_snaps[wallet] = report

        assets_list: list[dict[int, float]] = []
        for cid, wallet in id_to_wallet.items():
            snap = wallet_snaps.get(
                wallet.lower(),
                {"found": False, "wallet": wallet, "account_value": 0.0},
            )
            assets_list.append({cid: follower_acct.follower_equity(snap)})
        return assets_list, wallet_snaps

    def _batch_fetch_config_assets(
        self,
        lock_pairs: list[tuple[int, str]],
        unlock_pairs: list[tuple[int, str]],
    ) -> tuple[list[dict[int, float]], dict[str, dict[str, Any]]]:
        """同步封装：批量查 follower 权益，返回 [{id: equity}, ...] 与 wallet 快照。"""
        return follower_acct.run_async(
            self._batch_fetch_config_assets_async(lock_pairs, unlock_pairs)
        )

    def _asset_for_config_id(
        self, config_assets_list: list[dict[int, float]], config_id: int
    ) -> float:
        """从批量权益结果中取指定 config 的 follower 净值。"""
        for item in config_assets_list:
            if config_id in item:
                return float(item[config_id])
        return 0.0

    @staticmethod
    def _get_monitor_item(config_id: int) -> dict[str, Any] | None:
        """在 COPY_TRADING_CONFIG_MONITOR 中按 id 查找快照项。"""
        for item in COPY_TRADING_CONFIG_MONITOR:
            if int(item.get("id") or 0) == int(config_id):
                return item
        return None

    def _apply_monitor_item(self, item: dict[str, Any]) -> None:
        """将 monitor 快照项映射到实例：ID/coin/model/follower_wallet 等。"""
        row_fields = {
            k: v for k, v in item.items() if k not in ("leaders", "last_deal_ts")
        }
        row = copy_trading_db.CopyTradingConfigRow(**row_fields)
        copy_trading_db.apply_config_to_scopefi(self, row)
        cid = int(item["id"])
        ts = int(item.get("last_deal_ts") or time.time())
        self._last_deal_ts_by_id[cid] = ts

    def _get_last_deal_ts(self, config_id: int | None = None) -> int:
        """读取指定 config 的上次下单时间（优先实例 dict，其次 monitor 快照）。"""
        cid = int(config_id if config_id is not None else self.ID)
        if cid in self._last_deal_ts_by_id:
            return self._last_deal_ts_by_id[cid]
        item = self._get_monitor_item(cid)
        if item is not None:
            ts = int(item.get("last_deal_ts") or time.time())
            self._last_deal_ts_by_id[cid] = ts
            return ts
        ts = int(time.time())
        self._last_deal_ts_by_id[cid] = ts
        return ts

    def _touch_last_deal_ts(self, config_id: int | None = None) -> None:
        """下单后更新指定 config 的 last_deal_ts（实例 dict + monitor 快照）。"""
        cid = int(config_id if config_id is not None else self.ID)
        ts = int(time.time())
        self._last_deal_ts_by_id[cid] = ts
        item = self._get_monitor_item(cid)
        if item is not None:
            item["last_deal_ts"] = ts

    def _get_leader_pos_empty(self, config_id: int | None = None) -> bool:
        """读取指定 config 是否标记为「leader 已空仓」（用于 pos_risk 触发）。"""
        cid = int(config_id if config_id is not None else self.ID)
        return self._leader_pos_empty_by_id.get(cid, False)

    def _set_leader_pos_empty(
        self, value: bool, config_id: int | None = None
    ) -> None:
        """设置指定 config 的 leader 空仓标记（跟单过程中 now_pos==0 时置 True）。"""
        cid = int(config_id if config_id is not None else self.ID)
        self._leader_pos_empty_by_id[cid] = value

    def _get_lock_state(self) -> tuple[bool, str | None]:
        """读取当前策略是否已锁定 leader。

        查询表 r_copy_trading_state.leaders：非空表示已锁定。

        Returns:
            (lock_tag, lock_address): 是否已锁定、已锁定的 leader 地址(小写比较由调用方处理)。
        """
        return copy_trading_db.get_lock_state(self.ID, no_tunnel=self._db_no_tunnel)

    def _lock_leader(self, config_id: int, leader_address: str) -> Any:
        """将 leader 地址写入 state 表，完成「锁定」。

        Args:
            config_id: r_copy_trading_config.id
            leader_address: 本次要跟单的 leader 钱包(0x...)。

        Returns:
            更新后的 CopyTradingStateRow 或 None（见 copy_trading_db_cli.lock_leader）。
        """
        return copy_trading_db.lock_leader(
            config_id, leader_address, no_tunnel=self._db_no_tunnel
        )

    def _fetch_follower_snapshot(self) -> dict[str, Any]:
        """查询 follow 账户 ScopeFi 持仓与权益。

        使用 self.follower_wallet 或 config 行中的 follower 字段，调用
        query_follower_account_cli.fetch_follower_position。

        Returns:
            含 found、account_value、positions、raw 等字段的快照 dict。
        """
        wallet = getattr(self, "follower_wallet", "") or ""
        if not wallet:
            row = getattr(self, "_config_row", None)
            wallet = str(getattr(row, "follower", "") or "").strip()
        if not wallet:
            raise ValueError("follower 钱包地址为空，请检查 r_copy_trading_config.follower")
        return follower_acct.fetch_follower_position(wallet)

    def _follower_equity(self, snapshot: dict[str, Any] | None = None) -> float:
        """返回 follow 账户净值 account_value，用于 AUTO_RATIO 算量。

        Args:
            snapshot: 已查询的持仓快照；为 None 时重新拉取。

        Returns:
            账户净值(float)；未找到持仓时返回 0.0。
        """
        snap = snapshot if snapshot is not None else self._fetch_follower_snapshot()
        return follower_acct.follower_equity(snap)

    def _follower_close_sz(
        self, coin: str, snapshot: dict[str, Any] | None = None
    ) -> float:
        """返回 follow 在指定币种上应平仓的数量(持仓绝对值)。

        用于 leader 目标仓位 now_pos==0 时，按 follow 当前持仓量全平。

        Args:
            coin: 永续币种，如 ETH。
            snapshot: 持仓快照；为 None 时重新查询。

        Returns:
            |szi|，无仓为 0.0。
        """
        snap = snapshot if snapshot is not None else self._fetch_follower_snapshot()
        print(f"198行,获取持仓快照,snap:{snap}")
        return follower_acct.follower_close_sz(snap, coin)

    def _follower_coin_szi(
        self, coin: str, snapshot: dict[str, Any] | None = None
    ) -> float:
        """返回 follow 在指定币种上的 signed 持仓量 szi。

        Args:
            coin: 永续币种。
            snapshot: 持仓快照；为 None 时重新查询。

        Returns:
            正数=多头，负数=空头，无仓=0.0。
        """
        snap = snapshot if snapshot is not None else self._fetch_follower_snapshot()
        return follower_acct.follower_coin_szi(snap, coin)

    def _flatten_follower_coin_if_any(
        self,
        coin: str,
        follow_address: str,
        config_id: int,
        *,
        snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """若 follow 在 coin 上有仓，则下平仓单全平该币种。

        Args:
            coin: 要检查的币种。
            follow_address: 写入订单元数据的 leader 地址。
            config_id: r_copy_trading_config.id
            snapshot: follow 持仓快照，避免重复请求。

        Returns:
            _place_order 的响应 dict；无仓则 None。
        """
        szi = self._follower_coin_szi(coin, snapshot)
        if abs(szi) < 1e-12:
            return None
        direction = "Close Long" if szi > 0 else "Close Short"
        sz = abs(szi)
        price = self._get_coin_price(coin)
        price, sz = self._format_order_precision(coin, price, sz, direction)
        print(
            f"[flatten_follow] config={config_id} follow 存在 {coin} 持仓 szi={szi}, 全平 "
            f"{direction} sz={sz} price={price}",
            flush=True,
        )
        return self._place_order(
            coin, price, sz, direction, follow_address, config_id
        )

    def _first_lock_leader(
        self,
        config_id: int,
        leader_address: str,
        coin: str,
        follower_snap: dict[str, Any],
    ) -> dict[str, Any] | None:
        """首次锁定 leader：写库 + 平掉 follow 同币种旧仓(若有)。

        Args:
            config_id: r_copy_trading_config.id
            leader_address: 待锁定的 leader 钱包。
            coin: 成交消息中的币种，与 follow 待平仓位一致。
            follower_snap: 已拉取的 follow 持仓快照。

        Returns:
            平仓单结果；未平仓则为 None。
        """
        self._lock_leader(config_id, leader_address)
        return self._flatten_follower_coin_if_any(
            coin, leader_address, config_id, snapshot=follower_snap
        )

    def _place_order_after_first_lock(
        self,
        pre_flatten: dict[str, Any] | None,
        follow_result: Any,
    ) -> Any:
        """合并「首次锁定预平仓」与「跟单下单」的返回结构。

        Args:
            pre_flatten: _first_lock_leader 产生的平仓响应，无则为 None。
            follow_result: 本次跟单 _place_order 的返回值或跳过字符串。

        Returns:
            有预平仓时 {"pre_flatten": ..., "follow": ...}，否则仅 follow_result。
        """
        if pre_flatten is not None:
            return {"pre_flatten": pre_flatten, "follow": follow_result}
        return follow_result

    def _fetch_leader_snapshot(self, leader_address: str) -> dict[str, Any]:
        """查询 leader 钱包 ScopeFi 资金与持仓。

        调用 query_wallet_position.fetch_wallet_position（Track API）。

        Args:
            leader_address: leader 钱包 0x 地址。

        Returns:
            与 follow 快照结构类似的 dict（含 account_value、positions 等）。
        """
        wallet = leader_address.strip()
        if not wallet:
            raise ValueError("leader 钱包地址为空")
        return follower_acct.run_async(wallet_query.fetch_wallet_position(wallet))

    def _leader_equity(self, snapshot: dict[str, Any] | None = None) -> float:
        """从 leader 快照提取账户净值，用于比例算量分母。

        Args:
            snapshot: _fetch_leader_snapshot 的返回值。

        Returns:
            account_value；未找到或无效时为 0.0。
        """
        if not snapshot or not snapshot.get("found"):
            return 0.0
        return float(snapshot.get("account_value") or 0.0)

    def _get_coin_price(self, coin: str | None = None) -> float:
        """从 Hyperliquid allMids 获取永续 mid 价。

        Args:
            coin: 币种，如 ETH；为空则用 self.coin。
        
        Returns:
            该币种盘口中间价(float)。

        实现: hl_fetch_price.fetch_mid_price
        """
        c = (coin or self.coin).strip().upper()
        return fetch_mid_price(c, hl_info_url=self.hl_info_url)

    def _format_order_precision(
        self,
        coin: str,
        price: float,
        sz: float,
        direction: str,
    ) -> tuple[float, float]:
        """下单前按 HL 规则格式化价格与数量。

        以 mid 价为基准，按 leader 方向加滑点得到限价，再按 meta.szDecimals
        舍入价格与数量。

        Args:
            coin: 永续币种。
            price: 原始 mid 价(通常来自 _get_coin_price)。
            sz: 原始下单数量。
            direction: HL 方向，如 Open Long、Close Long。

        Returns:
            (formatted_price, formatted_size) 元组。

        实现: hl_format_precision.format_order_fields
        """
        result = format_order_fields(
            coin,
            size=sz,
            mid=price,
            direction=direction,
            slippage_ratio=self.slippage_ratio,
            hl_info_url=self.hl_info_url,
        )
        return result["formatted_price"], result["formatted_size"]

    def _place_order(
        self,
        coin: str,
        price: float,
        sz: float,
        direction: str,
        follow_address: str,
        config_id: int,
    ) -> dict[str, Any]:
        """向 OpenHub 提交一笔跟单限价单。

        Args:
            coin: 永续币种(如 ETH)，用于 asset_id 映射。
            price: 格式化后的限价。
            sz: 格式化后的数量。
            direction: HL 方向(Open/Close + Long/Short)。
            follow_address: 被跟单 leader 地址，写入订单元数据。
            config_id: r_copy_trading_config.id（OpenHub strategyConfigId）

        Returns:
            dry_run 时含 business_status=DRY_RUN、payload、order；
            实盘时含 HTTP 响应、parsed、business_status(SUBMITTED/REJECTED 等)。

        实现: scopefi下单测试脚本 OrderPayload + OpenHubClient.place_order
        """
        self._touch_last_deal_ts(config_id)
        mod = _load_order_place_module()
        settings = mod.load_settings()
        if not self.dry_run:
            settings.validate_for_live()

        order = mod.OrderPayload(
            coin=coin.strip().upper(),
            sz=sz,
            price=price,
            dir=direction,
            leverage=self.level,
            follow_address=follow_address,
        )
        asset_id = settings.resolve_asset_id(order.coin)
        payload = mod.build_place_payload(
            settings,
            order,
            strategy_config_id=config_id,
            asset_id=asset_id,
        )

        if self.dry_run:
            result = {
                "下单参数": {
                    "coin": coin,
                    "price": price,
                    "sz": sz,
                    "direction": direction,
                    "follow_address": follow_address,
                    "config_id": config_id,
                },
                "business_status": "DRY_RUN",
                "business_code": None,
                "asset_id": asset_id,
                "payload": payload,
                "order": order.to_dict(),
            }
            print(f"891行,下单参数result:{result}")
            scopefi_log.log_order(
                coin=coin.strip().upper(),
                price=price,
                sz=sz,
                side=direction,
                config_id=config_id,
                follow_address=follow_address,
                dry_run=True,
                extra={"business_status": "DRY_RUN"},
            )
            return result

        client = mod.OpenHubClient(settings)
        result = client.place_order(order, strategy_config_id=config_id)
        status, code = mod.OpenHubClient.business_status(result.get("parsed"))
        result["business_status"] = status
        result["business_code"] = code
        scopefi_log.log_live_order(
            coin=coin.strip().upper(),
            price=price,
            sz=sz,
            side=direction,
            config_id=config_id,
            follow_address=follow_address,
            extra={"business_status": status, "business_code": code},
        )
        return result

    def handle_rmq_message(self, mess: dict[str, Any]) -> Any:
        """消费 RabbitMQ 推送的一条成交通知并执行跟单逻辑。

        由 scopefi_rmq_consumer 回调；将 mess 写入 self.mess 后转调 main()。

        Args:
            mess: 含 payload.data(user/coin/dir/sz/startPosition) 等字段的消息体。

        Returns:
            与 main() 相同：跳过字符串、{config_id: 结果} 或下单 dict。
        """
        self.agg_mess.append(mess)
        try:
            deal_address = mess["payload"]["data"]["user"]
            deal_coin = mess["payload"]["data"]["coin"]
            lock_ids, unlock_ids = self._filter_deal_ids(deal_address, deal_coin)
            for cid in dict.fromkeys(lock_ids + unlock_ids):
                scopefi_log.log_leader_push_if_locked(
                    mess, cid, no_tunnel=self._db_no_tunnel
                )
        except (KeyError, TypeError):
            pass
        return self.main()

    def _pos_risk_single(self) -> Any:
        """单 config 持仓风控：leader 空仓标记为真且 follow 仍有仓时强平 follow。

        需先 _apply_monitor_item。Returns 跳过字符串或 _place_order 结果。
        """
        if not self._get_leader_pos_empty():
            return "SKIP_LEADER_POS_NOT_EMPTY"
        lock_tag, lock_address = self._get_lock_state()
        if not lock_tag or not lock_address:
            return "SKIP_NOT_LOCKED"
        coin = str(self.coin).strip().upper()
        leader_snap = self._fetch_leader_snapshot(lock_address)
        follower_snap = self._fetch_follower_snapshot()
        leader_szi = follower_acct.follower_coin_szi(leader_snap, coin)
        follow_szi = self._follower_coin_szi(coin, follower_snap)
        if abs(leader_szi) >= 1e-12:
            return "OK_LEADER_HAS_POS"
        if abs(follow_szi) < 1e-12:
            return "OK_FOLLOW_FLAT"
        direction = "Close Long" if follow_szi > 0 else "Close Short"
        sz = abs(follow_szi)
        price = self._get_coin_price(coin)
        price, sz = self._format_order_precision(coin, price, sz, direction)
        cid = int(self.ID)
        print(
            f"[pos_risk] config={cid} leader仓位=0 follow仍有仓 coin={coin} "
            f"leader_szi={leader_szi} follow_szi={follow_szi} -> {direction} sz={sz}",
            flush=True,
        )
        return self._place_order(coin, price, sz, direction, lock_address, cid)

    def pos_risk(self) -> Any:
        """持仓风控：遍历 COPY_TRADING_CONFIG_MONITOR 全部 config，逐条 _pos_risk_single。"""
        if not COPY_TRADING_CONFIG_MONITOR:
            return "SKIP_NO_MONITOR_CONFIG"
        results: dict[int, Any] = {}
        for item in COPY_TRADING_CONFIG_MONITOR:
            self._apply_monitor_item(item)
            results[int(item["id"])] = self._pos_risk_single()
        return results

    def _check_deal_time_single(self) -> Any:
        """单 config 成交闲置检查：超时则 leaders 置 0 并全平 follow 的 coin 仓位。

        使用 self._get_last_deal_ts(self.ID) 与该 config 的 inactive_timeout_hours
        比较。仅 _place_order 会刷新 last_deal_ts；闲置处理本身不算成交。
        """
        cid = int(self.ID)
        now_ts = int(time.time())
        last_ts = self._get_last_deal_ts(cid)
        idle_hours = int(getattr(self, "inactive_timeout_hours", 48))
        threshold_sec = idle_hours * 60 * 60
        elapsed = now_ts - last_ts
        if elapsed <= threshold_sec:
            return (
                f"OK_IDLE_NOT_REACHED elapsed={elapsed}s "
                f"need>{threshold_sec}s ({idle_hours}h) last_deal_ts={last_ts}"
            )

        lock_tag, lock_address = self._get_lock_state()
        coin = str(self.coin).strip().upper()
        follower_snap = self._fetch_follower_snapshot()
        follow_szi = self._follower_coin_szi(coin, follower_snap)
        has_follow_pos = abs(follow_szi) >= 1e-12

        if not lock_tag and not has_follow_pos:
            return (
                f"OK_IDLE_ALREADY_RELEASED config={cid} elapsed={elapsed}s "
                f"leaders=0 follow_flat"
            )

        print(
            f"[check_deal_time] config={cid} 闲置超时 {elapsed}s > {threshold_sec}s "
            f"(last_deal_ts={last_ts})，将 leaders 置 0，并平 follow {coin} 仓位",
            flush=True,
        )

        if lock_tag:
            copy_trading_db.clear_state_leaders(cid, no_tunnel=self._db_no_tunnel)
            monitor_item = self._get_monitor_item(cid)
            if monitor_item is not None:
                monitor_item["leaders"] = 0
            self._set_leader_pos_empty(False, cid)

        follow_wallet = str(getattr(self, "follower_wallet", "") or "").strip()
        meta_addr = lock_address or follow_wallet or "idle_release"
        flat = None
        if has_follow_pos:
            flat = self._flatten_follower_coin_if_any(
                coin,
                meta_addr,
                cid,
                snapshot=follower_snap,
            )
            if flat is None:
                print(
                    f"[check_deal_time] config={cid} follow {coin} 平仓未发起",
                    flush=True,
                )
            else:
                print(
                    f"[check_deal_time] config={cid} follow {coin} 已发起全平",
                    flush=True,
                )
        else:
            print(
                f"[check_deal_time] config={cid} follow {coin} 无仓，仅解锁 leaders",
                flush=True,
            )

        return {
            "status": "IDLE_TIMEOUT_CLEARED_LEADERS",
            "config_id": cid,
            "elapsed_sec": elapsed,
            "threshold_sec": threshold_sec,
            "idle_hours": idle_hours,
            "last_deal_ts": last_ts,
            "follow_flatten": flat,
        }

    def check_deal_time(self) -> Any:
        """成交闲置检查：遍历全部 config，逐条 _check_deal_time_single。"""
        if not COPY_TRADING_CONFIG_MONITOR:
            return "SKIP_NO_MONITOR_CONFIG"
        results: dict[int, Any] = {}
        for item in COPY_TRADING_CONFIG_MONITOR:
            self._apply_monitor_item(item)
            results[int(item["id"])] = self._check_deal_time_single()
        return results

    def start_parallel_tasks(
        self,
        *,
        main_interval_sec: float = 10.0,
        pos_risk_interval_sec: float = 60.0,
        check_deal_interval_sec: float = 60.0,
        enable_main_loop: bool = True,
    ) -> None:
        """启动后台守护线程：main / pos_risk / check_deal_time 定时循环。

        Args:
            main_interval_sec: main 循环间隔（默认 10s；RMQ 模式通常 enable_main_loop=False）
            pos_risk_interval_sec: 持仓风控间隔（默认 60s）
            check_deal_interval_sec: 闲置解锁间隔（默认 60s）
            enable_main_loop: False 时不启动 main 线程（--rmq 模式）
        """
        if getattr(self, "_parallel_tasks_started", False):
            return
        self._parallel_tasks_started = True
        stop = threading.Event()
        self._parallel_stop = stop

        def _loop_main() -> None:
            """后台线程：定时调用 main() 处理 self.mess。"""
            while True:
                try:
                    out = self.main()
                    print(f"[main] {out}", flush=True)
                    scopefi_log.log_runtime(task="main", result=out)
                except Exception as e:
                    print(f"[main] error: {e}", flush=True)
                    scopefi_log.log_runtime(task="main", error=str(e))
                if stop.wait(main_interval_sec):
                    break

        def _loop_pos() -> None:
            """后台线程：定时调用 pos_risk()。"""
            while True:
                try:
                    out = self.pos_risk()
                    print(f"[pos_risk] {out}", flush=True)
                    scopefi_log.log_runtime(task="pos_risk", result=out)
                except Exception as e:
                    print(f"[pos_risk] error: {e}", flush=True)
                    scopefi_log.log_runtime(task="pos_risk", error=str(e))
                if stop.wait(pos_risk_interval_sec):
                    break

        def _loop_deal() -> None:
            """后台线程：定时调用 check_deal_time()。"""
            while True:
                try:
                    out = self.check_deal_time()
                    print(f"[check_deal_time] {out}", flush=True)
                    scopefi_log.log_runtime(task="check_deal_time", result=out)
                except Exception as e:
                    print(f"[check_deal_time] error: {e}", flush=True)
                    scopefi_log.log_runtime(task="check_deal_time", error=str(e))
                if stop.wait(check_deal_interval_sec):
                    break

        if enable_main_loop:
            threading.Thread(target=_loop_main, name="main", daemon=True).start()
        threading.Thread(target=_loop_pos, name="pos_risk", daemon=True).start()
        threading.Thread(target=_loop_deal, name="check_deal_time", daemon=True).start()
        parts = [
            f"pos_risk 每 {pos_risk_interval_sec}s",
            f"check_deal_time 每 {check_deal_interval_sec}s",
        ]
        if enable_main_loop:
            parts.insert(0, f"main 每 {main_interval_sec}s")
        print(f"[parallel] 已启动: {', '.join(parts)}", flush=True)

    def stop_parallel_tasks(self) -> None:
        """停止后台 main / pos_risk / check_deal_time 循环。"""
        stop = getattr(self, "_parallel_stop", None)
        if stop is not None:
            stop.set()

    def main(self):
        """跟单主入口：解析 self.mess，批量匹配 config 并逐条跟单。

        流程概要:
            1. 校验 payload.type == ORDER_FULL_FILLED
            2. _filter_deal_ids 找 lock / unlock 两类 config
            3. 查 leader 权益，批量查各 config 的 follower 权益
            4. 对每个 config_id 调用 _process_deal_for_config

        Returns:
            无匹配 → 跳过字符串；有匹配 → {config_id: 结果}。
        """ 
        self.main_lock = True
        agg_mess = self.agg_mess
        self.agg_mess = []
        # --- 1. 解析 MQ 成交通知 ---
        print(f"成交推送:{agg_mess} {type(agg_mess)}")
        # TODO 先聚合agg_mess数据,若是ORDER_FULL_FILLED则保留,否则跳过；将user相同并且coin相同的数据做聚合，
        # [{'ts_ms': 1781208965524, 'payload': {'type': 'ORDER_FULL_FILLED', 'data': {'user': '0x442d95768dc0d76ccee91a9e05bf99b537bcd064', 'oid': 463404542762, 'dir': 'Open Long', 'coin': 'HYPE', 'sz': 16.54, 'startPosition': 0, 'time': 1780988838406}}, 'local_recv_ms': 1781208965524, 'queue': 'QUEUE_FULL_FILLED', 'exchange': 'FANOUT_EXCHANGE_BASE', 'routing_key': 'FANOUT_ORDER_FULL_FILLED'},...]
        
        payload_type = self.mess["payload"]["type"]
        if payload_type != "ORDER_FULL_FILLED":
            return "SKIP_NOT_ORDER_FULL_FILLED"
        
        # 判断哪些地址需要对冲
        deal_address = self.mess["payload"]["data"]["user"]
        deal_coin = self.mess["payload"]["data"]["coin"]
        # 选出来的地址包括已锁定的对冲地址+未锁定的对冲地址
        lock_leader_ids, un_lock_leader_ids = self._filter_deal_ids(deal_address, deal_coin)
        lock_pairs, unlock_pairs = self._get_config_follower_pairs(
            lock_leader_ids, un_lock_leader_ids
        )
        print(f"1157行,lock_pairs:{lock_pairs}, unlock_pairs:{unlock_pairs}")

        # 查询本次成交地址的权益
        leader_start_pos = self.mess["payload"]["data"]["startPosition"]
        leader_snap = self._fetch_leader_snapshot(deal_address)
        leader_asset = self._leader_equity(leader_snap)

        # 批量查询 lock / unlock 对应 config 的 follower 权益
        config_assets_list, wallet_snaps = self._batch_fetch_config_assets(
            lock_pairs, unlock_pairs
        )
        print(
            f"333行,leader权益:{leader_asset} config_assets_list={config_assets_list} "
            f"wallet={deal_address} found={leader_snap.get('found')}"
        )

        if leader_asset <= 0:
            return "SKIP_LEADER_EQUITY_ZERO"

        config_ids = list(dict.fromkeys(lock_leader_ids + un_lock_leader_ids))
        if not config_ids:
            return "SKIP_NO_MATCHING_CONFIG"

        deal_sz_raw = self.mess["payload"]["data"]["sz"]
        leader_side_raw = self.mess["payload"]["data"]["dir"]
        results: dict[int, Any] = {}
        for cid in config_ids:
            item = self._get_monitor_item(cid)
            if item is None:
                results[cid] = "SKIP_MONITOR_ITEM_MISSING"
                continue
            self._apply_monitor_item(item)
            follow_asset = self._asset_for_config_id(config_assets_list, cid)
            follower_wallet = str(getattr(self, "follower_wallet", "") or "").strip().lower()
            follower_snap = wallet_snaps.get(follower_wallet)
            if follower_snap is None:
                follower_snap = self._fetch_follower_snapshot()
                if follow_asset <= 0:
                    follow_asset = self._follower_equity(follower_snap)
            lock_tag = cid in lock_leader_ids
            print(
                f"[config={cid}] follow权益:{follow_asset} lock_tag={lock_tag}",
                flush=True,
            )
            try:
                results[cid] = self._process_deal_for_config(
                    config_id=cid,
                    deal_address=deal_address,
                    deal_coin=deal_coin,
                    leader_asset=leader_asset,
                    leader_start_pos=leader_start_pos,
                    leader_side=leader_side_raw,
                    deal_sz=deal_sz_raw,
                    follow_asset=follow_asset,
                    follower_snap=follower_snap,
                    lock_tag=lock_tag,
                )
            except Exception as e:
                tb = traceback.format_exc()
                scopefi_log.log_deal_error(
                    config_id=cid,
                    deal_address=deal_address,
                    deal_coin=deal_coin,
                    leader_side=leader_side_raw,
                    deal_sz=deal_sz_raw,
                    lock_tag=lock_tag,
                    follow_asset=follow_asset,
                    error_type=type(e).__name__,
                    error=str(e),
                    traceback=tb,
                    mess=self.mess,
                )
                print(
                    f"[main][config={cid}] _process_deal_for_config 异常，已跳过: "
                    f"{type(e).__name__}: {e}\n{tb}",
                    flush=True,
                )
                results[cid] = {
                    "status": "ERROR_PROCESS_DEAL",
                    "config_id": cid,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }
                continue
        return results

    def _process_deal_for_config(
        self,
        *,
        config_id: int,
        deal_address: str,
        deal_coin: str,
        leader_asset: float,
        leader_start_pos: float,
        leader_side: str,
        deal_sz: float,
        follow_asset: float,
        follower_snap: dict[str, Any],
        lock_tag: bool,
    ) -> Any:
        """对单个 config 执行完整跟单逻辑（需先 _apply_monitor_item）。

        步骤:
          1. follow_direction 映射 leader 方向 → follow 下单方向
          2. 计算 leader 目标仓位 now_pos = startPosition + deal_sz
          3. lock_tag=True:  已锁定，按 model + deal_model 算量下单
          4. lock_tag=False: 首次锁定 (_first_lock_leader) 再按模式首单

        Args:
            config_id: r_copy_trading_config.id，下单时显式传入 _place_order
            deal_address: 成交 leader 钱包
            deal_coin: 成交币种
            leader_asset: leader 账户净值（AUTO_RATIO 分母）
            leader_start_pos: 成交前 leader 持仓
            leader_side: HL 方向（含 Long>Short 反手）
            deal_sz: 本次成交量（方向映射过程中可能被取反）
            follow_asset: follow 账户净值（AUTO_RATIO 分子）
            follower_snap: follow 持仓快照
            lock_tag: True=已锁定该 leader，False=首次从 address_pool 锁定

        Returns:
            跳过字符串、下单 dict，或首次锁定时的 {pre_flatten, follow} 结构。
        """
        # --- 3. 按 follow_direction 将 leader 方向映射为 follow 下单方向 ---
        # 单独处理反手交易
        if leader_side == "Long > Short":
            leader_side = "Open Short"
        elif leader_side == "Short > Long":
            leader_side = "Open Long"
        follow_side = leader_side   # 给一个初始值
        if leader_side == "Open Long":
            if self.follow_direction == 1:
                pass
            elif self.follow_direction == 2:
                follow_side = "Open Short"
        elif leader_side == "Close Long":
            deal_sz = deal_sz*-1
            if self.follow_direction == 1:
                follow_side = "Open Short"
            elif self.follow_direction == 2:
                follow_side = "Open Long"
        elif leader_side == "Open Short":
            deal_sz = deal_sz*-1
            if self.follow_direction == 1:
                pass
            elif self.follow_direction == 2:
                follow_side = "Open Long"
        elif leader_side == "Close Short":
            if self.follow_direction == 1:
                follow_side = "Open Long"
            elif self.follow_direction == 2:
                follow_side = "Open Short"
        now_pos = leader_start_pos+deal_sz
        print(f"414行,now_pos:{now_pos}, leader_start_pos:{leader_start_pos}, deal_sz:{deal_sz}")
        # --- 4. 已锁定 / 首次锁定：按 model + deal_model 算量并下单 ---
        self._set_leader_pos_empty(False, config_id)
        if lock_tag:
            if self.model == "IMMEDIATE_FULL":
                # 第二步:计算是否跟单
                # if https://api.scopefi.ai/api/track/user/position
                if self.deal_model == "AUTO_RATIO":
                    price = self._get_coin_price(deal_coin)
                    # print(f"319行,获取价格,price:{price}")
                    # 说明已经锁定,这一笔只交易本次比例
                    ratio = abs(deal_sz*price/leader_asset)
                    print(f"322行,计算比例,ratio:{ratio} ,deal_sz:{deal_sz} ,price:{price} ,leader_asset:{leader_asset}")
                    if now_pos == 0:
                        sz = self._follower_close_sz(deal_coin, follower_snap)
                        follow_side = "Close Long" if sz > 0 else "Close Short"
                        print(f"IMMEDIATE_FULL AUTO_RATIO 全部平仓数量:{sz} {deal_coin}")
                        self._set_leader_pos_empty(True, config_id)
                    else:
                        follow_amt = ratio * follow_asset
                        sz = follow_amt / price
                    sz = abs(sz)
                    print(f"325行,传入参数,deal_coin:{deal_coin}, price:{price}, sz:{sz}, follow_side:{follow_side}")
                    if sz != 0:
                        price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                        return self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id)
                elif self.deal_model == "FIXED_NOTIONAL":
                    price = self._get_coin_price(deal_coin)
                    # 计算下单量
                    if now_pos == 0:
                        sz = self._follower_close_sz(deal_coin, follower_snap)
                        follow_side = "Close Long" if sz > 0 else "Close Short"
                        print(f"IMMEDIATE_FULL FIXED_NOTIONAL 全部平仓数量:{sz} {deal_coin}")
                        self._set_leader_pos_empty(True, config_id)
                    else:
                        sz = self.fixed_amt/price
                    sz = abs(sz)
                    print(f"564行,传入参数,deal_coin:{deal_coin}, price:{price}, sz:{sz}, follow_side:{follow_side}")
                    if sz != 0:
                        price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                        return self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id)
            elif self.model == "DELTA_FROM_ZERO":
                if self.deal_model == "AUTO_RATIO":
                    price = self._get_coin_price(deal_coin)
                    # 说明已经锁定,这一笔只交易本次比例
                    ratio = abs(deal_sz*price/leader_asset)
                    if now_pos == 0:
                        sz = self._follower_close_sz(deal_coin, follower_snap)
                        follow_side = "Close Long" if sz > 0 else "Close Short"
                        print(f"DELTA_FROM_ZERO AUTO_RATIO 全部平仓数量:{sz} {deal_coin}")
                        self._set_leader_pos_empty(True, config_id)
                    else:
                        follow_amt = ratio * follow_asset
                        sz = follow_amt / price
                    sz = abs(sz)
                    if sz != 0:
                        price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                        return self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id)
                elif self.deal_model == "FIXED_NOTIONAL":
                    price = self._get_coin_price(deal_coin)
                    # 计算下单量
                    if now_pos == 0:
                        sz = self._follower_close_sz(deal_coin, follower_snap)
                        follow_side = "Close Long" if sz > 0 else "Close Short"
                        print(f"DELTA_FROM_ZERO FIXED_NOTIONAL 全部平仓数量:{sz} {deal_coin}")
                        self._set_leader_pos_empty(True, config_id)
                    else:
                        sz = self.fixed_amt/price
                    sz = abs(sz)
                    if sz != 0:
                        price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                        return self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id)
            elif self.model == "INCREASE_ONLY":
                if self.deal_model == "AUTO_RATIO":
                    price = self._get_coin_price(deal_coin)
                    # 说明已经锁定,这一笔只交易本次比例
                    ratio = abs(deal_sz*price/leader_asset)
                    if now_pos == 0:
                        sz = self._follower_close_sz(deal_coin, follower_snap)
                        follow_side = "Close Long" if sz > 0 else "Close Short"
                        print(f"INCREASE_ONLY AUTO_RATIO 全部平仓数量:{sz} {deal_coin}")
                        self._set_leader_pos_empty(True, config_id)
                    else:
                        follow_amt = ratio * follow_asset
                        sz = follow_amt / price
                    sz = abs(sz)
                    if sz != 0:
                        price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                        return self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id)
                elif self.deal_model == "FIXED_NOTIONAL":
                    price = self._get_coin_price(deal_coin)
                    # 计算下单量
                    if now_pos == 0:
                        sz = self._follower_close_sz(deal_coin, follower_snap)
                        follow_side = "Close Long" if sz > 0 else "Close Short"
                        print(f"INCREASE_ONLY FIXED_NOTIONAL 全部平仓数量:{sz} {deal_coin}")
                        self._set_leader_pos_empty(True, config_id)
                    else:
                        sz = self.fixed_amt/price
                    sz = abs(sz)
                    if sz != 0:
                        price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                        return self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id)
            elif self.model == "REVERSE_REDUCE_ONLY":
                if self.deal_model == "AUTO_RATIO":
                    price = self._get_coin_price(deal_coin)
                    # 说明已经锁定,这一笔只交易本次比例
                    ratio = abs(deal_sz*price/leader_asset)
                    follow_amt = ratio * follow_asset
                    sz = follow_amt / price
                    price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                    return self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id)
                elif self.deal_model == "FIXED_NOTIONAL":
                    price = self._get_coin_price(deal_coin)
                    # 计算下单量
                    sz = self.fixed_amt/price
                    price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                    return self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id)
        else:   # 首次锁定leader地址
            if self.model == "IMMEDIATE_FULL":
                pre = self._first_lock_leader(config_id, deal_address, deal_coin, follower_snap)
                if now_pos == 0:
                    if pre is not None:
                        return {"status": "LOCK_ONLY_LEADER_FLAT_FOLLOW", "pre_flatten": pre}
                    return "当前leader持仓为0,只锁定地址,不跟单"
                if self.deal_model == "AUTO_RATIO":
                    if self.follow_direction == 1:
                        follow_side = 'Open Long' if now_pos >= 0 else 'Open Short'
                    elif self.follow_direction == 2:
                        follow_side = 'Open Short' if now_pos >= 0 else 'Open Long'
                    price = self._get_coin_price(deal_coin)
                    ratio = abs(now_pos*price/leader_asset)
                    follow_amt = ratio * follow_asset
                    sz = follow_amt / price
                    print(f"537行,now_pos:{now_pos}, price:{price}, leader_asset:{leader_asset}, ratio:{ratio}, follow_asset:{follow_asset}, follow_amt:{follow_amt}, sz:{sz}, follow_side:{follow_side}")
                    price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                    return self._place_order_after_first_lock(
                        pre,
                        self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id),
                    )
                elif self.deal_model == "FIXED_NOTIONAL":
                    price = self._get_coin_price(deal_coin)
                    if "Close" in leader_side:
                        sz = self.fixed_amt/price
                    elif "Open" in leader_side and leader_start_pos != 0:
                        sz = self.fixed_amt/price*2
                    elif "Open" in leader_side and leader_start_pos == 0:
                        sz = self.fixed_amt/price
                    if self.follow_direction == 1:
                        follow_side = 'Open Long' if now_pos >= 0 else 'Open Short'
                    elif self.follow_direction == 2:
                        follow_side = 'Open Short' if now_pos >= 0 else 'Open Long'
                    print(f"567行,传入参数,self.fixed_amt:{self.fixed_amt}, price:{price}, sz:{sz}")
                    price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                    return self._place_order_after_first_lock(
                        pre,
                        self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id),
                    )
            elif self.model == "DELTA_FROM_ZERO":
                if leader_start_pos != 0:
                    return "DELTA_FROM_ZERO 模式下 leader_start_pos != 0 不锁定leader地址"
                else:
                    print(f"DELTA_FROM_ZERO 模式下 leader_start_pos={leader_start_pos} 锁定leader地址:{deal_address}")
                pre = self._first_lock_leader(config_id, deal_address, deal_coin, follower_snap)
                if self.deal_model == "AUTO_RATIO":
                    price = self._get_coin_price(deal_coin)
                    # 说明已经锁定,这一笔只交易本次比例
                    ratio = abs(deal_sz*price/leader_asset)
                    follow_amt = ratio * follow_asset
                    sz = follow_amt / price
                    price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                    return self._place_order_after_first_lock(
                        pre,
                        self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id),
                    )
                elif self.deal_model == "FIXED_NOTIONAL":
                    price = self._get_coin_price(deal_coin)
                    # 计算下单量
                    sz = self.fixed_amt/price
                    price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                    return self._place_order_after_first_lock(
                        pre,
                        self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id),
                    )
            elif self.model == "INCREASE_ONLY":
                if "Close" in leader_side:
                    return "INCREASE_ONLY 模式下 Close 订单不跟"
                else:
                    print(f"INCREASE_ONLY 模式下 锁定leader地址:{deal_address}")
                if "Open" in leader_side:
                    pre = self._first_lock_leader(config_id, deal_address, deal_coin, follower_snap)
                    if self.deal_model == "AUTO_RATIO":
                        price = self._get_coin_price(deal_coin)
                        # 说明已经锁定,这一笔只交易本次比例
                        ratio = abs(deal_sz*price/leader_asset)
                        follow_amt = ratio * follow_asset
                        sz = follow_amt / price
                        price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                        return self._place_order_after_first_lock(
                            pre,
                            self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id),
                        )
                    elif self.deal_model == "FIXED_NOTIONAL":
                        price = self._get_coin_price(deal_coin)
                        sz = self.fixed_amt / price
                        price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                        return self._place_order_after_first_lock(
                            pre,
                            self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id),
                        )
            elif self.model == "REVERSE_REDUCE_ONLY":
                pre = self._first_lock_leader(config_id, deal_address, deal_coin, follower_snap)
                if self.deal_model == "AUTO_RATIO":
                    if self.follow_direction == 1:
                        follow_side = 'Open Long' if now_pos >= 0 else 'Open Short'
                    elif self.follow_direction == 2:
                        follow_side = 'Open Short' if now_pos >= 0 else 'Open Long'
                    price = self._get_coin_price(deal_coin)
                    # 说明已经锁定,这一笔只交易本次比例
                    ratio = abs(deal_sz*price/leader_asset)
                    follow_amt = ratio * follow_asset
                    sz = follow_amt / price
                    price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                    return self._place_order_after_first_lock(
                        pre,
                        self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id),
                    )
                elif self.deal_model == "FIXED_NOTIONAL":
                    if self.follow_direction == 1:
                        follow_side = 'Open Long' if now_pos >= 0 else 'Open Short'
                    elif self.follow_direction == 2:
                        follow_side = 'Open Short' if now_pos >= 0 else 'Open Long'
                    price = self._get_coin_price(deal_coin)
                    # 计算下单量
                    sz = self.fixed_amt/price
                    price, sz = self._format_order_precision(deal_coin, price, sz, follow_side)
                    return self._place_order_after_first_lock(
                        pre,
                        self._place_order(deal_coin, price, sz, follow_side, deal_address, config_id),
                    )


# CLI 入口：见文件顶部「一、运行方式」
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ScopeFi 跟单（批量处理全部 config）")
    parser.add_argument(
        "--rmq",
        action="store_true",
        help="消费 RabbitMQ QUEUE_FULL_FILLED（需 aio_pika）",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="实盘下单（默认 dry_run）",
    )
    parser.add_argument(
        "--pos-risk",
        action="store_true",
        help="仅执行一次 pos_risk（持仓风控）",
    )
    parser.add_argument(
        "--check-deal-time",
        action="store_true",
        help="仅执行一次 check_deal_time（成交闲置解锁）",
    )
    args = parser.parse_args()
    log_dir = scopefi_log.enable_file_logs()
    print(f"[cli] 日志目录: {log_dir}", flush=True)
    scopefi_log.log_runtime(
        event="cli_start",
        log_dir=str(log_dir),
        rmq=args.rmq,
        live=args.live,
        pos_risk=args.pos_risk,
        check_deal_time=args.check_deal_time,
    )
    print(args, type(args), args.rmq)

    try:
        boot_copy_trading_config_monitor()

        if args.rmq:
            import scopefi_rmq_consumer as rmq_consumer

            def _factory() -> scopefi_trade:
                """为 RMQ 消费者创建 scopefi_trade 实例（每条消息复用同一实例）。"""
                t = scopefi_trade(periodic_main=False)
                if args.live:
                    t.dry_run = False
                return t

            asyncio.run(rmq_consumer.run_scopefi_rmq_consumer(_factory))
        else:
            t = scopefi_trade()
            if args.live:
                t.dry_run = False
            if args.pos_risk:
                print(t.pos_risk())
            elif args.check_deal_time:
                print(t.check_deal_time())
            else:
                print("[cli] main / pos_risk / check_deal_time 已在后台每 60s 运行，Ctrl+C 退出")
                try:
                    while True:
                        time.sleep(3600)
                except KeyboardInterrupt:
                    t.stop_parallel_tasks()
    except KeyboardInterrupt:
        print("\n[cli] 已停止", flush=True)
    finally:
        stop_copy_trading_config_monitor_loop()
