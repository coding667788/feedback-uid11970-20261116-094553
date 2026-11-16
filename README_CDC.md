# 本地 CDC 采集归并器（乱序 / 重放 / 崩溃恢复）

接收**乱序、重放、重复**的变更事件流，持续在 SQLite 中生成账户余额镜像。
纯 Python 3.12 标准库实现，**仅需 CPU**，不使用 Docker、外部业务服务或任何
流处理引擎。模拟源、归并器、独立答案、自动化故障测试均在本仓库内交付。

---

## 1. 目录结构

```
.
├── cdc.py                 # 启动器（支持 python -I 隔离运行，防 PYTHONPATH/当前目录注入）
├── cdc.cmd                # Windows 便捷包装：cdc <子命令> ...
├── run_tests.cmd / .sh    # 一键全量测试
├── src/cdc/
│   ├── protocol.py        # 事件线格式 + 不透明游标编解码 + 校验/冲突归一化
│   ├── source_gen.py      # 确定性乱序/重放场景生成（模拟源与独立答案共用，采集器不导入）
│   ├── source_server.py   # 标准库 http.server 模拟源：分页、不透明游标、重复页、END
│   ├── storage.py         # SQLite 双水位存储：接收/应用各自原子提交，冲突检测
│   ├── collector.py       # 采集归并主循环 + A/B/C 崩溃注入停车点
│   ├── oracle.py          # 独立流式标准答案（与采集器零代码/路径共享）
│   └── cli.py             # run / status / snapshot / source / oracle / gen-scenario
└── tests/
    ├── test_storage.py    # 20 个单元测试：原子性、冲突、乱序、双水位、完整性、重启
    ├── test_generator.py  # 6 个单元测试：确定性、分页契约、游标不透明、HTTP 重放
    └── test_e2e.py        # 6 个端到端测试：2 万笔、抽样核对、真实进程终止式故障
```

---

## 2. 环境与前置条件

- Windows 11 原生 **Python 3.12**（开发验证版本 3.12.8），无任何第三方依赖。
- 所有命令在仓库根目录执行。推荐加 `-I`（隔离模式）运行，避免当前目录或
  `PYTHONPATH` 中被植入同名模块（如恶意 `json.py`）。
- 临时/测试产物统一落在仓库内 `.runtime-tmp\`，可随时整体删除。

确认解释器：

```bat
python --version
```

---

## 3. 快速开始（Windows，命令提示符 / PowerShell）

```bat
REM 1) 生成确定性场景参数（默认 2 万笔事务；这里用 1000 笔演示）
python -I cdc.py gen-scenario --out .runtime-tmp\demo\scenario.json --txns 1000

REM 2) 启动本地 HTTP 模拟源（端口 0 = 系统分配，写入 port.json）
python -I cdc.py source --scenario .runtime-tmp\demo\scenario.json ^
        --port 0 --port-file .runtime-tmp\demo\port.json
```

另开一个终端，读取端口并运行采集器：

```bat
for /f %i in ('python -c "import json;print(json.load(open('.runtime-tmp/demo/port.json'))['port'])"') do set PORT=%i

REM 3) 采集归并（可重复执行，断点续跑）
python -I cdc.py run --db .runtime-tmp\demo\mirror.db ^
        --url http://127.0.0.1:%PORT%/events

REM 4) 查看双水位与库内计数
python -I cdc.py status --db .runtime-tmp\demo\mirror.db

REM 5) 单读事务导出“连续水位 + 该水位全量账户状态”
python -I cdc.py snapshot --db .runtime-tmp\demo\mirror.db ^
        --out .runtime-tmp\demo\snap.json

REM 6) 独立流式标准答案（写入隔离目录，采集器无权读取）
python -I cdc.py oracle --scenario .runtime-tmp\demo\scenario.json ^
        --out-dir .runtime-tmp\demo\answers
```

Git Bash 用户把 `^` 续行换成 `\`，或直接用 `./cdc.cmd` 包装。

控制台中文若显示乱码，仅是终端代码页问题；所有 JSON 文件均以 UTF-8 写入。
可先执行 `set PYTHONIOENCODING=utf-8` 改善显示。

---

## 4. 核心设计与正确性保证

### 4.1 两套独立水位，分别原子提交（SQLite WAL）

| 水位 | 持久化字段 | 含义 |
| --- | --- | --- |
| 接收水位 | `meta.recv_cursor` / `recv_done` | 已**可靠接收并整页落盘**的位置，绝不代表已生效 |
| 应用水位 | `meta.applied_watermark` | 已按连续 `commit_seq` **生效**的最大事务序号 |

- **接收侧**：一页内的全部事件 + 该页游标在同一个 `BEGIN IMMEDIATE … COMMIT`
  中提交。崩溃只会看到“整页落盘且游标推进”或“整页都没落盘”，不会出现
  游标先于事件或半页写入。
- **应用侧**：一笔事务的全部余额改动 + `applied_watermark` 推进 + `apply_log`
  记账在同一个事务中提交。读者永远看不到半笔事务，也看不到后序事务。
- `snapshot` 用单条 `BEGIN DEFERRED` 读事务，导出的账户状态与水位严格对应
  同一快照，不会读到撕裂状态。

### 4.2 乱序、缺口与背压

- 只应用 `applied_watermark + 1` 且**已完整**（`op_index` 从 0 连续、
  数量等于 `op_count`）的 COMMITTED 事务；`next_applicable_seq()` 发现缺口
  立即返回，主循环回去继续拉取并持久化后续页。
- 等待缺失事务期间，事件直接写 SQLite，**不在内存堆积整个流**；SQLite 天然
  提供背压，不存在“内存缓冲满而停止获取补齐事件”的路径。
- 事务可跨任意多页；COMMIT 可先于操作到达（早到 COMMIT 的事务保留在调度
  集合中，直到操作补齐才终结）；不同事务交错。

### 4.3 重放、去重与冲突

- 服务端契约：同一游标（页号）永远返回相同页面；已确认页可被旧游标重放。
- 存储侧以部分唯一索引区分身份：
  - 操作：`(txid, op_index)`（PUT/DELETE）
  - 终结：每事务至多一条 COMMIT/ABORT；BEGIN 每事务至多一条
- 身份已存在且内容**逐字节相同** → 计为重复，无副作用；
  身份相同但**内容冲突**（含 COMMIT 与 ABORT 互斥、commit_seq/op_count 不符、
  余额不同）→ 抛 `ConflictError`，整页回滚，进程以非零码停止。
- 重启后由唯一索引去重、由应用水位挡住重复生效，做到**不重复生效、不遗漏**。

### 4.4 终止与完整性

- `DELETE` 不存在账户为空操作；余额允许负数（场景含强制负余额事务）。
- 收到 END 并应用到无法再推进后执行 `final_check_or_raise()`：存在未终结
  事务、COMMIT 操作不全、`commit_seq` 不连续/有缺口、END 未落盘，任一成立
  即明确失败（退出码 3）。

### 4.5 独立答案与隔离性

- `oracle.py` 只导入 `protocol` / `source_gen`，**不导入** `collector` /
  `storage`，也不打开采集器数据库；按 `commit_seq 1..N` 流式逐笔改写账户
  字典并在指定水位写检查点，任何时刻只保留当前账户状态。
- 采集器命令行从不接收答案目录；`test_05` 用 AST 静态核验导入边界，并检查
  采集进程日志不含 `oracle_answers`，满足“测试答案不能被采集器读取”。

---

## 5. 故障注入（真实进程终止，而非“抛个被捕获的异常”）

采集器支持三个**同步停车点**（仅测试用，正常运行不传任何故障参数）：

| 点 | 位置 | 对应需求窗口 |
| --- | --- | --- |
| **A** | 接收事务内：事件已写入、**接收游标尚未 COMMIT** | 接收事件但游标未提交 |
| **B** | 应用事务内：余额已写入、**应用事务尚未 COMMIT** | 余额写入但事务未提交 |
| **C** | 应用事务 COMMIT 返回后、下一轮确认处理前 | 提交成功但客户端尚未确认 |

机制：到达停车点时，采集进程写 `parked_<点>.json` 标记后**阻塞在
`threading.Event` 上**；测试框架从外部用 Windows `TerminateProcess`
（`Popen.kill()`，POSIX 下 SIGKILL）**真正杀掉进程**，并断言退出码非零。
随后用**同一个数据库**重启，核对结果与无故障基线逐账户一致。
没有任何“抛出后被业务代码吞掉”的异常路径。

手动演示单故障点（A）：

```bat
python -I cdc.py run --db .runtime-tmp\crash.db --url http://127.0.0.1:%PORT%/events ^
        --fault A --marker-dir .runtime-tmp\markers
REM 看到 markers\parked_A.json 后，从任务管理器或 taskkill 终止该 python，再原样重启
python -I cdc.py run --db .runtime-tmp\crash.db --url http://127.0.0.1:%PORT%/events
```

---

## 6. 测试

### 6.1 一键全量

```bat
run_tests.cmd
REM 或 Git Bash： CDC_E2E_TXNS=20000 ./run_tests.sh
```

### 6.2 测试构成

- `tests/test_storage.py`（21）：有序/乱序应用、COMMIT 先到、跨页交错、
  缺口期间持续接收、ABORT 不生效、DELETE 空操作/重建/负余额、幂等重放、
  PUT/COMMIT/ABORT 冲突回滚、A/B 钩子回滚后无残留、END 缺操作/缺序号/
  未终结失败、重启续跑。
- `tests/test_generator.py`（6）：确定性逐字节复现、op_index 连续、
  线上事件与台账一致（防“操作未发出”）、重复页、冲突副本、2 万笔序号连续、
  HTTP 分页遍历/同游标重试一致/游标不透明/坏游标 400。
- `tests/test_e2e.py`（6，真实子进程 + 外部终止）：
  1. 无故障基线跑满 2 万笔，多水位快照与流式 oracle 全量核对；
  2. A/B/C 三点分别杀进程后重启，结果 == 基线；
  3. 同一库连续三次崩溃（A、B、C 各一次）后完成，结果 == 基线；
  4. 同身份冲突内容必须非零码停止，库仍可自洽打开，输出含 `fatal`；
  5. AST 级导入边界 + 运行证据，保证答案对采集器隔离；
  6. 采集进程峰值工作集远低于整流驻留量级（断言 < 250 MB）。

### 6.3 本次实际测试结果（Windows 11，Python 3.12.8）

| 套件 | 用例数 | 结果 | 规模/耗时 |
| --- | --- | --- | --- |
| test_storage.py | 21 | 全部 OK | 亚秒级 |
| test_generator.py | 6 | 全部 OK | 约 2 s（含 2 万笔生成自检） |
| test_e2e.py | 6 | 全部 OK | 20000 笔 / 约 72 s |

2 万笔场景实测（默认 seed=20261116）：

- 事件总量约 **9.9 万**，分页约 **510 页**（含约 3% 重复页）；
- 提交事务 18406 笔、中止 1594 笔、早到 COMMIT 上万笔；
- 采集进程**峰值工作集约 30.6 MB**（内存断言上限 250 MB），SQLite 库约 5 MB；
- 强制生命周期账户最终值 777（创建→删除→重建），强制负余额账户 -42；
- 终点快照与 oracle `final.json` 账户字典完全相等，各抽样水位逐账户相等。

复跑命令（可先用小流量快速验证）：

```bat
set CDC_E2E_TXNS=500
python -I tests\test_e2e.py
```

---

## 7. 命令行参考

| 命令 | 关键参数 | 说明 |
| --- | --- | --- |
| `gen-scenario` | `--out --seed --txns --accounts --page-size --duplicate-rate --poison-offset` | 生成确定性场景参数文件 |
| `source` | `--scenario --host --port --port-file` | 启动 HTTP 模拟源，`--port 0` 自动分配 |
| `run` | `--db --url [--fault A,B,C --marker-dir] [--max-pages]` | 采集归并；可重复执行断点续跑 |
| `status` | `--db` | 双水位、事件/事务/账户计数 |
| `snapshot` | `--db [--out]` | 单读事务导出水位 + 全量账户 |
| `oracle` | `--scenario --out-dir [--checkpoints 1,100,...]` | 独立流式标准答案 |

退出码：`0` 正常完成；`2` 协议/冲突致命错误；`3` END 完整性失败；
`10` 因 `--max-pages` 安全阀提前暂停（未到 END，非错误）。

---

## 8. 未完成项 / 已知边界

- HTTP 层仅实现明文本地回环（`127.0.0.1`）与请求重试退避，不含 TLS、认证、
  压缩或断线续传协议；按任务约束面向单机本地场景。
- 故障停车点每个点在单个进程生命周期内只触发一次；多轮反复崩溃由
  `test_03` 通过“重启后重新挂点”覆盖，而非单进程内多次触发。
- 场景生成器为确定性有限流（含明确 END），未实现无限流/动态 schema 变更；
  账户模型为单表整数余额覆盖写，未建模多币种或账户元数据。
- 未提供并发多采集者锁；设计上假定同一数据库同一时刻只有一个采集进程
  （WAL 允许并发只读 `snapshot/status`）。
- 测试中的峰值内存阈值（250 MB）为工程宽松上限，实测约 30.6 MB；若在
  极大型账户基数下运行，可按实际机器调整该断言。
