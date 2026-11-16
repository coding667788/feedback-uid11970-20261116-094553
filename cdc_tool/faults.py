"""故障注入：在精确同步点硬终止“采集进程自己”。

配置来自环境变量（测试 harness 注入，正常运行不设置即永不触发）：
  CDC_FAULT_POINT  = cursor | dml | postcommit
  CDC_FAULT_FIRE_AT = 第 N 次命中该同步点时终止（>=1）

终止方式是 os._exit()：跳过所有 Python finally/except/缓冲刷新，等价于
进程被 SIGKILL/强杀，业务代码无法捕获。绝不用“抛异常”冒充进程终止。
"""

from __future__ import annotations

import os
import sys

CURSOR = "cursor"        # 事件已接收并写入事务，但接收游标尚未提交
DML = "dml"              # 余额已在事务内写入，但应用事务尚未提交
POSTCOMMIT = "postcommit"  # 应用事务已提交，但客户端尚未确认（ack 前）

EXIT_CODE = 99


def maybe_crash(point: str, context: str) -> None:
    configured = os.environ.get("CDC_FAULT_POINT")
    if configured != point:
        return
    fire_at = int(os.environ.get("CDC_FAULT_FIRE_AT", "1"))
    counter_key = f"CDC_FAULT_COUNT_{point}"
    count = int(os.environ.get(counter_key, "0")) + 1
    os.environ[counter_key] = str(count)
    if count >= fire_at:
        # 直接走内核退出，不抛异常、不执行 finally、不 flush 业务缓冲。
        marker = os.environ.get("CDC_FAULT_MARKER")
        if marker:
            try:
                with open(marker, "a", encoding="utf-8") as handle:
                    handle.write(f"crash point={point} hit={count} context={context}\n")
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError:
                pass
        sys.stderr.write(f"[fault] hard terminate at {point} ({context}) hit={count}\n")
        sys.stderr.flush()
        os._exit(EXIT_CODE)
