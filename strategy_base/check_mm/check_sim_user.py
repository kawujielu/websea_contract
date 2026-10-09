
import os
import sys
import asyncio
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "../.."))
sys.path.append(PROJECT_ROOT)
from utils.ToolBoxNew import ToolBox as tbn


def main():
    task = tbn()
    check_id = ['479579']
    sim_user = asyncio.run(task.get_sim_user())
    print(f"模拟金用户:{sim_user}")
    sim_user = list(task.load_account().keys())
    print(f"做市账户:{sim_user}")
    for id in check_id:
        if id in sim_user:
            print(f"{id}是模拟金用户")
        else:
            print(f"{id}不是模拟金用户")


if __name__ == '__main__':
    main()



