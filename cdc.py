# -*- coding: utf-8 -*-
"""仓库根启动器：把 src 加入 sys.path 后转交 cdc.cli。

支持隔离模式运行（推荐，防 PYTHONPATH/当前目录注入）：
    python -I cdc.py run --db ... --url ...
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from cdc.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
