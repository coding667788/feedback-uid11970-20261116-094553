#!/usr/bin/env bash
# 一键运行全部测试（Git Bash / WSL 亦可）。
# 端到端规模可用环境变量 CDC_E2E_TXNS 调整，默认 20000（任务要求规模）。
set -euo pipefail
cd "$(dirname "$0")"
export CDC_E2E_TXNS="${CDC_E2E_TXNS:-20000}"
echo "[1/2] 单元测试（存储/协议/生成器/HTTP 分页）"
python -I tests/test_storage.py
python -I tests/test_generator.py
echo "[2/2] 端到端测试（真实子进程 + 外部进程终止式故障注入，${CDC_E2E_TXNS} 笔事务）"
python -I tests/test_e2e.py
echo
echo "ALL TESTS PASSED"
