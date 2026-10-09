"""HYPER 策略路径（勿命名为 config.py，会与 strategy_base 的 config 包冲突）。"""
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(os.environ.get("HYPER_ROOT", Path(__file__).resolve().parent))
DATA_DIR = Path(os.environ.get("HYPER_DATA_DIR", ROOT / "parquet_file"))


def _find_strategy_base():
    if os.environ.get("STRATEGY_BASE_PATH"):
        p = Path(os.environ["STRATEGY_BASE_PATH"])
        if (p / "template" / "template_timer.py").is_file():
            return p.resolve()
    for p in [ROOT, *ROOT.parents]:
        if (p / "template" / "template_timer.py").is_file():
            return p.resolve()
    for p in (
        Path("/home/ubuntu/strategy_base"),
        Path("/home/ubuntu/python_files/strategy_base"),
    ):
        if (p / "template" / "template_timer.py").is_file():
            return p.resolve()
    return None


def _find_crypto_center_root():
    if os.environ.get("CRYPTO_CENTER_PATH"):
        p = Path(os.environ["CRYPTO_CENTER_PATH"])
        if (p / "crypto_center" / "client" / "rest" / "okex" / "contract.py").is_file():
            return p.resolve()
    for p in [ROOT, *ROOT.parents, Path("/home/ubuntu"), Path("/home/ubuntu/websea")]:
        if (p / "crypto_center" / "client" / "rest" / "okex" / "contract.py").is_file():
            return p.resolve()
    cc = Path("/home/ubuntu/crypto_center")
    if (cc / "client" / "rest" / "okex" / "contract.py").is_file():
        return cc.resolve()
    return None


STRATEGY_BASE = _find_strategy_base()
if STRATEGY_BASE is None:
    raise FileNotFoundError(
        "未找到 strategy_base（需含 template/template_timer.py）。"
        "请设置: export STRATEGY_BASE_PATH=/home/ubuntu/strategy_base"
    )

STRATEGY_BASE_PATH = str(STRATEGY_BASE)
CRYPTO_CENTER_ROOT = _find_crypto_center_root()

for _p in (ROOT, ROOT / "sub", ROOT / "query", STRATEGY_BASE):
    _s = str(_p)
    if _s not in sys.path:
        sys.path.insert(0, _s)

# append 避免盖住 site-packages 中已安装的 crypto_center
if CRYPTO_CENTER_ROOT is not None:
    _s = str(CRYPTO_CENTER_ROOT)
    if _s not in sys.path:
        sys.path.append(_s)

DATA_DIR.mkdir(parents=True, exist_ok=True)
FILTER_DIR = DATA_DIR / "filter"   # {日期}_filter_address.parquet
SCORE_DIR = DATA_DIR / "score"     # {日期}_address_scores.parquet
FILTER_DIR.mkdir(parents=True, exist_ok=True)
SCORE_DIR.mkdir(parents=True, exist_ok=True)


def latest_parquet(directory, pattern):
    """取目录下日期最新的 parquet（文件名前缀 YYYY-MM-DD_）。"""
    directory = Path(directory)
    files = list(directory.glob(pattern))
    if not files:
        hint = ""
        if "address_scores" in pattern:
            hint = (
                f"\n请先运行 pipeline 生成打分文件:\n"
                f"  cd {ROOT}\n"
                f"  python3 pipeline/filter_addresses_daily.py   # 需先有 active_address.parquet\n"
                f"  python3 pipeline/score_address_deals.py\n"
                f"或设置环境变量 HYPER_DATA_DIR 指向已有 parquet 目录"
            )
        raise FileNotFoundError(f"未找到 parquet: {directory}/{pattern}{hint}")

    def _file_date(path):
        try:
            return datetime.strptime(path.name[:10], "%Y-%m-%d").date()
        except ValueError:
            return None

    dated = [(d, p) for p in files if (d := _file_date(p))]
    if dated:
        return max(dated, key=lambda x: x[0])[1]
    return sorted(files)[-1]
