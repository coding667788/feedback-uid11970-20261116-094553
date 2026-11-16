"""确定性事件流生成器。

特性（由固定 seed 复现）：
- 跨账户事务（每笔操作 >=2 个不同账户），余额可负，PUT 为覆盖写；
- DELETE 后再 PUT 的“删除后重建”账户循环（可计数、可断言）；
- 不同事务交错，事务可跨页，COMMIT 可先于操作到达；
- 部分事务“逆序完成”（调度完成顺序与 commit_seq 不一致）；
- 同事件完全重复、整页重复（页面是新游标、内容相同）。

生成器按事务规格流式产出，不依赖采集器；answer.py 使用同一套原始页面
独立推导期望值。
"""

from __future__ import annotations

import random
import collections
from dataclasses import dataclass, field

from .events import ABORT, BEGIN, COMMIT, DELETE, END, PUT, Event


@dataclass
class TxSpec:
    txid: str
    aborted: bool
    commit_seq: int | None
    op_count: int
    operations: list[tuple[str, str, int | None]]  # (type, account, balance)
    commit_first: bool
    reverse: bool


@dataclass
class Page:
    index: int
    events: list[Event]
    is_end: bool


@dataclass
class Dataset:
    pages: list[Page]
    tx_specs: list[TxSpec]
    stats: dict
    seed: int

    def all_events(self) -> list[Event]:
        out: list[Event] = []
        for page in self.pages:
            out.extend(page.events)
        return out


@dataclass
class _OpenTx:
    spec: TxSpec
    begun: bool = False
    terminal_sent: bool = False
    op_pos: int = 0
    reverse_hold: int = 0


@dataclass
class _GeneratorStats:
    total_tx: int = 0
    committed_tx: int = 0
    aborted_tx: int = 0
    total_ops: int = 0
    duplicate_events: int = 0
    duplicate_pages: int = 0
    delete_rebuild_cycles: int = 0
    commit_first_tx: int = 0
    reverse_tx: int = 0
    cross_page_tx: int = 0
    negative_puts: int = 0
    account_count: int = 0


def _build_tx_specs(
    n_tx: int,
    account_count: int,
    abort_rate: float,
    recycle_accounts: int,
    seed: int,
) -> tuple[list[TxSpec], _GeneratorStats]:
    rng = random.Random(seed * 7919 + 13)
    stats = _GeneratorStats()
    stats.account_count = account_count
    specs: list[TxSpec] = []
    next_seq = 1
    alive = {f"acc{i:04d}": True for i in range(account_count)}

    for idx in range(n_tx):
        txid = f"tx{idx:07d}"
        # 保证至少有足够的删除-重建循环：指定的回收账户每轮强制 DELETE 再 PUT，
        # 且承载回收动作的事务强制提交（abort 循环不计入保证数）。
        forced_rebuild = idx < recycle_accounts * 4
        aborted = (not forced_rebuild) and (rng.random() < abort_rate)
        op_count = rng.randint(2, 5)

        accounts: list[str] = []
        while len(set(accounts)) < 2:
            accounts = [f"acc{rng.randrange(account_count):04d}" for _ in range(op_count)]

        operations: list[tuple[str, str, int | None]] = []
        if forced_rebuild:
            recycle_acc = f"acc{idx % recycle_accounts:04d}"
            accounts[0] = recycle_acc
            if alive.get(recycle_acc, False):
                operations.append((DELETE, recycle_acc, None))
                alive[recycle_acc] = False
                # 同一事务内后续再创建，形成删除后重建；若 op 数不够则扩展。
            operations.append((PUT, recycle_acc, rng.randint(-500, 5000)))
            alive[recycle_acc] = True
            stats.delete_rebuild_cycles += 1

        for j, account in enumerate(accounts):
            if any(acc == account for _, acc, _ in operations):
                account = f"acc{(int(account[3:]) + 1) % account_count:04d}"
            kind = DELETE if (not forced_rebuild and rng.random() < 0.12 and alive.get(account, False)) else PUT
            if kind == DELETE:
                operations.append((DELETE, account, None))
                alive[account] = False
            else:
                balance = rng.randint(-1000, 10000)
                if balance < 0:
                    stats.negative_puts += 1
                operations.append((PUT, account, balance))
                alive[account] = True

        # 保证 op 覆盖的账户 >=2（跨账户）
        used = {acc for _, acc, _ in operations}
        if len(used) < 2:
            extra = f"acc{(rng.randrange(account_count)):04d}"
            if extra not in used:
                operations.append((PUT, extra, rng.randint(-1000, 10000)))

        spec = TxSpec(
            txid=txid,
            aborted=aborted,
            commit_seq=None if aborted else next_seq,
            op_count=len(operations),
            operations=operations,
            commit_first=(not aborted) and rng.random() < 0.10,
            reverse=(not aborted) and rng.random() < 0.10,
        )
        if not aborted:
            next_seq += 1
            stats.committed_tx += 1
            if spec.commit_first:
                stats.commit_first_tx += 1
            if spec.reverse:
                stats.reverse_tx += 1
        else:
            stats.aborted_tx += 1
        stats.total_tx += 1
        stats.total_ops += len(operations)
        specs.append(spec)

    return specs, stats


def _emit_terminal(spec: TxSpec) -> Event:
    if spec.aborted:
        return Event(ABORT, spec.txid)
    return Event(COMMIT, spec.txid, commit_seq=spec.commit_seq, op_count=spec.op_count)


def _flatten(specs: list[TxSpec], stats: _GeneratorStats, seed: int, dup_event_rate: float) -> list[Event]:
    rng = random.Random(seed * 104729 + 7)
    events: list[Event] = []
    pending: "collections.deque[_OpenTx]" = collections.deque()
    cursor = 0

    def emit(event: Event) -> None:
        events.append(event)
        if rng.random() < dup_event_rate:
            events.append(event)
            stats.duplicate_events += 1

    while cursor < len(specs) or pending:
        # 每个调度步：以约 60% 概率新开一个事务（若还有），否则轮转推进一个
        # 已打开事务。队列轮转保证不同事务交错，且每个事务最终都会被推进。
        open_new = cursor < len(specs) and (not pending or rng.random() < 0.6)
        if open_new:
            spec = specs[cursor]
            cursor += 1
            open_tx = _OpenTx(spec=spec)
            if spec.reverse:
                open_tx.reverse_hold = rng.randint(2, 4)
            pending.append(open_tx)
            continue

        open_tx = pending.popleft()
        spec = open_tx.spec
        if open_tx.reverse_hold > 0:
            # 逆序完成：挂起递减后放回队尾，让后开的事务先推进/终结。
            open_tx.reverse_hold -= 1
            pending.append(open_tx)
            continue
        if not open_tx.begun:
            emit(Event(BEGIN, spec.txid))
            open_tx.begun = True
            if spec.commit_first:
                # COMMIT/ABORT 先于操作到达。
                emit(_emit_terminal(spec))
                open_tx.terminal_sent = True
            pending.append(open_tx)
            continue
        if open_tx.op_pos < spec.op_count:
            op_type, account, balance = spec.operations[open_tx.op_pos]
            if op_type == PUT:
                emit(Event(PUT, spec.txid, open_tx.op_pos, account, balance))
            else:
                emit(Event(DELETE, spec.txid, open_tx.op_pos, account))
            open_tx.op_pos += 1
            pending.append(open_tx)
            continue
        if not open_tx.terminal_sent:
            emit(_emit_terminal(spec))
            open_tx.terminal_sent = True
        # 操作与终结都已发出，事务完成，不再入队。

    # 统计跨页在分页阶段才能最终确定，这里先返回事件。
    return events


def generate_dataset(
    n_tx: int = 21000,
    seed: int = 20261116,
    account_count: int = 300,
    abort_rate: float = 0.04,
    page_size: int = 500,
    recycle_accounts: int = 24,
    dup_page_rate: float = 0.02,
    dup_event_rate: float = 0.01,
) -> Dataset:
    specs, stats = _build_tx_specs(n_tx, account_count, abort_rate, recycle_accounts, seed)
    flat = _flatten(specs, stats, seed, dup_event_rate)

    pages: list[Page] = []
    page_rng = random.Random(seed * 31 + 5)
    index = 0
    cross_page_tx: set[str] = set()
    seen_tx_on_page: dict[int, set[str]] = {}
    for start in range(0, len(flat), page_size):
        chunk = flat[start : start + page_size]
        page = Page(index=index, events=list(chunk), is_end=False)
        pages.append(page)
        seen_tx_on_page[index] = {event.txid for event in chunk}
        index += 1
        # 整页重复：新游标、相同内容（模拟源重放已确认页面）。
        if page_rng.random() < dup_page_rate and start + page_size < len(flat):
            dup = Page(index=index, events=list(chunk), is_end=False)
            pages.append(dup)
            seen_tx_on_page[index] = {event.txid for event in chunk}
            stats.duplicate_pages += 1
            index += 1

    if pages:
        pages[-1].is_end = True

    # 跨页事务统计
    tx_pages: dict[str, set[int]] = {}
    for pidx, txids in seen_tx_on_page.items():
        for txid in txids:
            tx_pages.setdefault(txid, set()).add(pidx)
    stats.cross_page_tx = sum(1 for p in tx_pages.values() if len(p) > 1)

    return Dataset(
        pages=pages,
        tx_specs=specs,
        stats={
            "total_tx": stats.total_tx,
            "committed_tx": stats.committed_tx,
            "aborted_tx": stats.aborted_tx,
            "total_ops": stats.total_ops,
            "duplicate_events": stats.duplicate_events,
            "duplicate_pages": stats.duplicate_pages,
            "delete_rebuild_cycles": stats.delete_rebuild_cycles,
            "commit_first_tx": stats.commit_first_tx,
            "reverse_tx": stats.reverse_tx,
            "cross_page_tx": stats.cross_page_tx,
            "negative_puts": stats.negative_puts,
            "account_count": stats.account_count,
            "page_count": len(pages),
            "end_present": True,
        },
        seed=seed,
    )
