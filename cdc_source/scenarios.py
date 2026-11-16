"""异常输入场景：用于验证采集器“明确失败、报错停止”。

所有场景最终都带 END，问题在 END 校验或接收阶段暴露。
"""

from __future__ import annotations

import copy

from .events import COMMIT, PUT, Event, canonical_json
from .generator import Dataset, Page, generate_dataset


def _repaginate(flat_events: list[Event], page_size: int, seed: int, stats: dict) -> Dataset:
    pages: list[Page] = []
    index = 0
    for start in range(0, len(flat_events), page_size):
        pages.append(Page(index=index, events=list(flat_events[start : start + page_size]), is_end=False))
        index += 1
    if pages:
        pages[-1].is_end = True
    return Dataset(pages=pages, tx_specs=[], stats=dict(stats), seed=seed)


def _small_dataset(seed: int = 99, n_tx: int = 400) -> Dataset:
    return generate_dataset(n_tx=n_tx, seed=seed, page_size=120)


def conflict_payload(seed: int = 99) -> Dataset:
    """同一身份（txid, op_index）出现不同余额 → 接收阶段必须报错停止。"""
    ds = _small_dataset(seed)
    flat = list(ds.all_events())
    target = next(event for event in flat if event.type == PUT)
    conflicting = Event(PUT, target.txid, target.op_index, target.account, target.balance + 777)
    assert canonical_json(conflicting) != canonical_json(target)
    insert_at = len(flat) - 1
    flat.insert(insert_at, conflicting)
    return _repaginate(flat, 120, seed, ds.stats | {"scenario": "conflict_payload"})


def missing_operation(seed: int = 99) -> Dataset:
    """某已提交事务缺一个操作事件 → END 后必须明确失败。"""
    ds = _small_dataset(seed)
    flat = ds.all_events()
    # 找一个带 COMMIT 且 op_count>=2 的事务，删掉它的最后一个操作。
    commits = {event.txid: event for event in flat if event.type == COMMIT and event.op_count >= 2}
    victim = None
    for event in reversed(flat):
        if event.type == PUT and event.txid in commits:
            victim = event
            break
    assert victim is not None
    filtered = [event for event in flat if not (
        event.type == victim.type
        and event.txid == victim.txid
        and event.op_index == victim.op_index
    )]
    return _repaginate(filtered, 120, seed, ds.stats | {"scenario": "missing_operation"})


def gap_in_commit_seq(seed: int = 99) -> Dataset:
    """整笔已提交事务被移除，commit_seq 出现空洞 → END 后必须明确失败。"""
    ds = _small_dataset(seed)
    flat = ds.all_events()
    commits = [event for event in flat if event.type == COMMIT]
    victim_commit = commits[len(commits) // 2]
    victim_tx = victim_commit.txid
    filtered = [event for event in flat if event.txid != victim_tx]
    return _repaginate(filtered, 120, seed, ds.stats | {"scenario": "gap_in_commit_seq"})


def unterminated_transaction(seed: int = 99) -> Dataset:
    """事务有 BEGIN/操作但缺终结事件 → END 后必须明确失败。"""
    ds = _small_dataset(seed)
    flat = ds.all_events()
    commits = [event for event in flat if event.type == COMMIT]
    victim_tx = commits[len(commits) // 3].txid
    filtered = [
        event
        for event in flat
        if not (event.txid == victim_tx and event.type in (COMMIT, "ABORT"))
    ]
    return _repaginate(filtered, 120, seed, ds.stats | {"scenario": "unterminated_transaction"})


SCENARIOS = {
    "conflict_payload": conflict_payload,
    "missing_operation": missing_operation,
    "gap_in_commit_seq": gap_in_commit_seq,
    "unterminated_transaction": unterminated_transaction,
}
