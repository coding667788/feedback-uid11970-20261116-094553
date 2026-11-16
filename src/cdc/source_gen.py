# -*- coding: utf-8 -*-
"""确定性乱序/重放场景生成。

仅由两个进程使用：
  1) 模拟源服务（source_server）——把生成结果切片成分页事件流；
  2) 独立答案生成器（oracle）——按 commit_seq 规范顺序重放，产出标准答案。

采集归并器（collector/storage）绝不导入本模块，也读不到答案目录，
它只能通过 HTTP 看到事件，符合“测试答案不能被采集器读取”的要求。

内部事件为紧凑元组（wire 转换在 protocol.event_to_wire）：
  ('BEGIN', txid)
  ('PUT',    txid, op_index, account, balance)
  ('DELETE', txid, op_index, account)
  ('COMMIT', txid, commit_seq, op_count)
  ('ABORT',  txid)
  ('END',)
"""
from __future__ import annotations

import json
import random

from . import protocol as p

DEFAULT_PAGE_SIZE = 200
DUPLICATE_RATE = 0.03          # 约 3% 的页是上一页的原样重放
ABORT_RATE = 0.08              # 约 8% 的事务被 ABORT
EARLY_COMMIT_RATE = 0.30       # 约 30% 已提交事务：COMMIT 先于部分操作到达
MAX_OPS = 5
ACTIVE_WINDOW = 40


class ScenarioError(ValueError):
    pass


class _DeterministicRNG(random.Random):
    """random.Random(seed) 本身即确定性；保留子类便于将来替换。"""


def _build_scenario_params(seed: int, txn_count: int, accounts: int,
                           page_size: int, duplicate_rate: float,
                           poison_offset: int | None) -> dict:
    return {
        "seed": int(seed),
        "txn_count": int(txn_count),
        "accounts": int(accounts),
        "page_size": int(page_size),
        "duplicate_rate": float(duplicate_rate),
        "poison_offset": None if poison_offset is None else int(poison_offset),
        "rng_algorithm": "python-random-MT19937",
    }


def generate(seed: int = 20261116, txn_count: int = 20000,
             accounts: int = 64, page_size: int = DEFAULT_PAGE_SIZE,
             duplicate_rate: float = DUPLICATE_RATE,
             poison_offset: int | None = None) -> dict:
    """生成一个完整场景（纯函数：相同参数 -> 逐字节相同结果）。

    返回：
      events:             完整事件元组列表（含结尾 END）
      canonical_txns:     {txid: {state, commit_seq, op_count, ops:[(kind,acct,bal)]}}
      params:             可 JSON 序列化的生成参数
      stats:              计数信息
    """
    if txn_count < 1:
        raise ScenarioError("txn_count 至少为 1")
    if accounts < 2:
        raise ScenarioError("accounts 至少为 2")
    if page_size < 1:
        raise ScenarioError("page_size 至少为 1")

    rng = _DeterministicRNG(seed)
    events: list[tuple] = []
    canonical: dict[int, dict] = {}

    # 强制场景账户：前三笔事务按确定顺序提交，保证“创建->删除->重建”。
    # seq1 创建，seq2 删除（同时写一笔负余额），seq3 重建为 777。
    forced_life_acct = "acct_forced_lifecycle"
    forced_neg_acct = "acct_forced_negative"
    forced_txns = {
        1: {"state": "COMMITTED", "commit_seq": 1, "op_count": 1, "ops": [
            {"kind": p.PUT, "account": forced_life_acct, "balance": 100}]},
        2: {"state": "COMMITTED", "commit_seq": 2, "op_count": 2, "ops": [
            {"kind": p.DELETE, "account": forced_life_acct, "balance": None},
            {"kind": p.PUT, "account": forced_neg_acct, "balance": -42}]},
        3: {"state": "COMMITTED", "commit_seq": 3, "op_count": 1, "ops": [
            {"kind": p.PUT, "account": forced_life_acct, "balance": 777}]},
    }

    commit_seq = 0
    active: dict[int, dict] = {}      # txid -> 运行状态
    next_txid = 1
    aborted = 0
    early_commits = 0

    def emit(ev: tuple) -> None:
        events.append(ev)

    def open_txn() -> int:
        nonlocal next_txid
        txid = next_txid
        next_txid += 1
        n_ops = rng.randint(1, MAX_OPS)
        is_abort = rng.random() < ABORT_RATE
        if is_abort:
            n_ops = rng.randint(0, MAX_OPS)  # 中止事务可以没有操作
        canonical[txid] = {
            "state": "ABORTED" if is_abort else "COMMITTED",
            "commit_seq": None,
            "op_count": n_ops,
            "ops": [],
        }
        active[txid] = {
            "n_ops": n_ops,
            "emitted_ops": 0,
            "is_abort": is_abort,
            "terminal_emitted": False,
            "early_commit": False,
        }
        emit((p.BEGIN, txid))
        return txid

    def emit_op(txid: int) -> None:
        st = active[txid]
        op_index = st["emitted_ops"]

        acct = "acct_%04d" % rng.randrange(accounts)
        if rng.random() < 0.25:
            kind = p.DELETE
            bal = None
        else:
            kind = p.PUT
            # 约 1/8 的 PUT 为负余额，其余可正可零
            if rng.random() < 0.125:
                bal = -rng.randint(1, 10000)
            else:
                bal = rng.randint(0, 100000)

        if kind == p.PUT:
            emit((p.PUT, txid, op_index, acct, bal))
        else:
            emit((p.DELETE, txid, op_index, acct))
        canonical[txid]["ops"].append(
            {"kind": kind, "account": acct, "balance": bal})
        st["emitted_ops"] += 1

    def emit_forced_txn(txid: int) -> None:
        """按 BEGIN -> 操作 -> COMMIT 顺序发出一笔强制事务。"""
        nonlocal commit_seq
        info = forced_txns[txid]
        canonical[txid] = dict(info)
        emit((p.BEGIN, txid))
        for op_index, op in enumerate(info["ops"]):
            if op["kind"] == p.PUT:
                emit((p.PUT, txid, op_index, op["account"], op["balance"]))
            else:
                emit((p.DELETE, txid, op_index, op["account"]))
        commit_seq += 1
        if commit_seq != info["commit_seq"]:
            raise ScenarioError("强制事务序号错乱")
        emit((p.COMMIT, txid, info["commit_seq"], info["op_count"]))

    # 强制事务最先按序进入事件流（乱序/交错由后续随机事务覆盖）
    for forced_txid in (1, 2, 3):
        if txn_count < forced_txid:
            raise ScenarioError("txn_count 至少为 3 以容纳强制生命周期事务")
        emit_forced_txn(forced_txid)
    next_txid = 4

    def emit_terminal(txid: int) -> None:
        nonlocal commit_seq, aborted, early_commits
        st = active[txid]
        st["terminal_emitted"] = True
        if st["is_abort"]:
            emit((p.ABORT, txid))
            aborted += 1
            return
        commit_seq += 1
        canonical[txid]["commit_seq"] = commit_seq
        emit((p.COMMIT, txid, commit_seq, st["n_ops"]))
        if st["emitted_ops"] < st["n_ops"]:
            st["early_commit"] = True
            early_commits += 1

    # 主调度：保持一个交错窗口，事务可跨任意多页，不同事务交错
    while next_txid <= txn_count or active:
        can_open = next_txid <= txn_count and len(active) < ACTIVE_WINDOW
        if can_open and (not active or rng.random() < 0.4):
            open_txn()
            continue

        txid = rng.choice(sorted(active.keys()))
        st = active[txid]
        if st["terminal_emitted"]:
            # COMMIT 已先发出：继续补齐剩余操作（逆序完成的收尾）
            emit_op(txid)
        elif st["is_abort"]:
            # ABORT 随时可终结；若还有操作，一半概率先补操作
            if st["emitted_ops"] < st["n_ops"] and rng.random() < 0.5:
                emit_op(txid)
            else:
                emit_terminal(txid)
        else:
            remaining = st["n_ops"] - st["emitted_ops"]
            if remaining == 0:
                emit_terminal(txid)
            elif rng.random() < EARLY_COMMIT_RATE:
                # COMMIT 先于（部分）操作到达
                emit_terminal(txid)
            else:
                emit_op(txid)
                if st["emitted_ops"] == st["n_ops"] and not st["terminal_emitted"]:
                    emit_terminal(txid)

        if st["terminal_emitted"] and st["emitted_ops"] == st["n_ops"]:
            # 终结事件已发出且操作全部补齐后才能移出活跃集合；
            # 早到 COMMIT 的事务必须留在集合里继续发完剩余操作。
            del active[txid]

    if active:  # 理论不可达，防御性检查
        raise ScenarioError("调度结束仍有未终结事务: %s" % sorted(active))

    emit((p.END,))

    # 自检：commit_seq 连续、op_index 连续、事务完整、线上事件与台账一致
    seqs = sorted(c["commit_seq"] for c in canonical.values()
                  if c["state"] == "COMMITTED")
    if seqs != list(range(1, len(seqs) + 1)):
        raise ScenarioError("commit_seq 不连续")
    wire_ops = sum(1 for e in events if e[0] in (p.PUT, p.DELETE))
    ledger_ops = sum(len(c["ops"]) for c in canonical.values())
    if wire_ops != ledger_ops:
        raise ScenarioError(
            "线上操作事件 %s 与台账操作 %s 不一致（有操作未发出）"
            % (wire_ops, ledger_ops))
    wire_begins = sum(1 for e in events if e[0] == p.BEGIN)
    if wire_begins != txn_count:
        raise ScenarioError("BEGIN 事件数 %s != 事务数 %s"
                            % (wire_begins, txn_count))
    for txid, c in canonical.items():
        if c["state"] == "COMMITTED":
            idxs = [i for i, _ in enumerate(c["ops"])]
            if idxs != list(range(c["op_count"])) or len(c["ops"]) != c["op_count"]:
                raise ScenarioError("事务 %s 操作不完整" % txid)

    # 冲突注入：在 END 之前追加一个“身份相同、内容矛盾”的 PUT 副本，
    # 模拟源端重放出冲突内容。采集器先收到原版、后收到矛盾版，必须报错停止。
    if poison_offset is not None:
        if not 0 <= poison_offset < len(events):
            raise ScenarioError("poison_offset 越界: %s" % poison_offset)
        ev = events[poison_offset]
        if ev[0] != p.PUT:
            raise ScenarioError("poison_offset 必须指向 PUT 事件")
        end_pos = len(events) - 1
        if events[end_pos][0] != p.END:
            raise ScenarioError("事件流必须以 END 结尾才能注入冲突")
        events.insert(end_pos, (p.PUT, ev[1], ev[2], ev[3], ev[4] + 1))

    pages = build_pages(events, page_size, rng, duplicate_rate)
    committed = sum(1 for c in canonical.values() if c["state"] == "COMMITTED")
    stats = {
        "total_events": len(events),
        "total_txns": txn_count,
        "committed_txns": committed,
        "aborted_txns": aborted,
        "early_commit_txns": early_commits,
        "total_pages": len(pages),
        "forced_lifecycle_account": forced_life_acct,
        "poison_offset": poison_offset,
    }
    params = _build_scenario_params(seed, txn_count, accounts, page_size,
                                    duplicate_rate, poison_offset)
    return {"events": events, "canonical_txns": canonical,
            "params": params, "stats": stats, "pages": pages}


def build_pages(events: list[tuple], page_size: int,
                rng: random.Random, duplicate_rate: float) -> list[list[tuple]]:
    """把事件流切成页，并按概率插入“与上一页完全相同”的重复页。

    重复页有自己的页号（新游标），但内容与上一页逐字节相同；
    同一游标永远映射同一页，满足重试幂等。
    """
    chunks: list[list[tuple]] = []
    for i in range(0, len(events), page_size):
        chunks.append(list(events[i:i + page_size]))
    if not chunks:
        chunks = [[]]

    pages: list[list[tuple]] = []
    for idx, chunk in enumerate(chunks):
        # 第一页不重复，避免启动即绕圈；重复页复制相邻前一页
        if idx > 0 and rng.random() < duplicate_rate:
            pages.append(list(pages[-1]))
        pages.append(list(chunk))
    return pages


def write_scenario_file(path: str, scenario: dict) -> None:
    """把场景参数与统计落盘（事件内容不写，由 seed 确定性再生成）。"""
    payload = {"params": scenario["params"], "stats": scenario["stats"]}
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def regenerate(params: dict) -> dict:
    """根据参数重新生成完全相同的场景（服务端重启 / oracle 使用）。"""
    return generate(
        seed=params["seed"],
        txn_count=params["txn_count"],
        accounts=params["accounts"],
        page_size=params["page_size"],
        duplicate_rate=params.get("duplicate_rate", DUPLICATE_RATE),
        poison_offset=params.get("poison_offset"),
    )
