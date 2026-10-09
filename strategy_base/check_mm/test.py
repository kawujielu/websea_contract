"""测试现货/合约 Mongo 是否可连接（独立脚本，不依赖业务模块）。

参考 daily_profit_push_v2_no_mysql.py 的 motor 连接方式。

运行:
  py mongo_conn_test.py
依赖: pip install motor
"""
from __future__ import annotations

import asyncio
import sys
from typing import Optional

import motor.motor_asyncio

# ========== 连接配置 ==========
TARGETS = [
    {
        "name": "w-mongo-contract-quant",
        "uri": "mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/",
        "db": "exchange",
        "coll": "real_contract_deal",
    },
    {
        "name": "w-mongo-spot-quant",
        "uri": "mongodb://spot-ro:PcwDn46F24MHGjRa@10.60.99.86:27018/",
        "db": "exchange",
        "coll": "real_deal",
    },
]
SERVER_SELECTION_TIMEOUT_MS = 8000
# ==============================


async def probe_one(cfg: dict) -> bool:
    name = cfg["name"]
    uri = cfg["uri"]
    # 日志里不打印完整密码
    safe_uri = uri.split("@")[-1] if "@" in uri else uri
    print(f"\n===== {name} =====")
    print(f"host={safe_uri}")
    client: Optional[motor.motor_asyncio.AsyncIOMotorClient] = None
    try:
        client = motor.motor_asyncio.AsyncIOMotorClient(
            uri,
            serverSelectionTimeoutMS=SERVER_SELECTION_TIMEOUT_MS,
        )
        pong = await client.admin.command("ping")
        print(f"ping OK: {pong}")

        dbs = await client.list_database_names()
        print(f"databases: {dbs}")

        db_name = cfg["db"]
        coll_name = cfg["coll"]
        if db_name in dbs:
            col = client[db_name][coll_name]
            n = await col.estimated_document_count()
            print(f"{db_name}.{coll_name} estimated_count={n}")
            sample = await col.find_one()
            if sample is not None:
                keys = list(sample.keys())[:12]
                print(f"sample keys: {keys}")
            else:
                print("sample: (empty collection or no readable docs)")
        else:
            print(f"警告: 库 `{db_name}` 不在可访问列表中（可能无权限或不存在）")
        print(f"结果: {name} 连接成功")
        return True
    except Exception as e:
        print(f"结果: {name} 连接失败: {type(e).__name__}: {e}")
        return False
    finally:
        if client is not None:
            client.close()


async def main() -> int:
    print("开始 Mongo 连通性测试…")
    oks = []
    for cfg in TARGETS:
        oks.append(await probe_one(cfg))
    ok_n = sum(1 for x in oks if x)
    print(f"\n======= 汇总: {ok_n}/{len(TARGETS)} 成功 =======")
    return 0 if all(oks) else 1


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    raise SystemExit(asyncio.run(main()))

