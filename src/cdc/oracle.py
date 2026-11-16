# -*- coding: utf-8 -*-
"""独立标准答案生成器（oracle）。

独立性保证：
- 只导入 protocol/source_gen（场景的确定性生成规则），
  绝不导入 collector/storage，也不打开采集器的 SQLite 数据库；
- 标准答案写入独立目录，采集器命令行从不接收该路径，测试层再做静态检查，
  确保“测试答案不能被采集器读取”。

流式做法：按 commit_seq 1..N 逐笔取事务、逐笔改写内存中的账户字典，
在指定水位输出检查点。任何时刻只保留当前账户状态，不保留事件流。
DELETE 不存在账户为空操作；余额允许负数。
"""
from __future__ import annotations

import json
import logging
import os

from . import protocol as p
from . import source_gen

log = logging.getLogger("cdc.oracle")


def _apply_txn(accounts: dict[str, int], txn: dict) -> None:
    for op in txn["ops"]:
        if op["kind"] == p.PUT:
            accounts[op["account"]] = op["balance"]
        else:
            accounts.pop(op["account"], None)  # 不存在即空操作


def build_checkpoints(params: dict, checkpoint_seqs: list[int],
                      out_dir: str) -> dict:
    """重放规范顺序并在给定 commit_seq 水位写检查点 JSON。

    每个检查点文件 checkpoint_{seq}.json：
      {"watermark": seq, "account_count": n, "accounts": {...}}
    另写 final.json（最高水位）与 summary.json。
    """
    os.makedirs(out_dir, exist_ok=True)
    scenario = source_gen.regenerate(params)
    canonical = scenario["canonical_txns"]

    by_seq: dict[int, dict] = {}
    for txid, txn in canonical.items():
        if txn["state"] == "COMMITTED":
            if txn["commit_seq"] is None:
                raise RuntimeError("已提交事务缺少 commit_seq: txid=%s" % txid)
            by_seq[txn["commit_seq"]] = txn

    total = len(by_seq)
    if sorted(by_seq) != list(range(1, total + 1)):
        raise RuntimeError("规范流 commit_seq 不连续")
    wanted = sorted(set(checkpoint_seqs))
    bad = [s for s in wanted if s < 0 or s > total]
    if bad:
        raise ValueError("检查点水位越界: %s（总事务 %s）" % (bad, total))

    accounts: dict[str, int] = {}
    wanted_set = set(wanted)
    written: list[str] = []
    for seq in range(1, total + 1):
        _apply_txn(accounts, by_seq[seq])
        if seq in wanted_set:
            payload = {
                "watermark": seq,
                "account_count": len(accounts),
                "accounts": dict(sorted(accounts.items())),
            }
            path = os.path.join(out_dir, "checkpoint_%08d.json" % seq)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
            written.append(os.path.basename(path))

    final_payload = {
        "watermark": total,
        "account_count": len(accounts),
        "accounts": dict(sorted(accounts.items())),
    }
    with open(os.path.join(out_dir, "final.json"), "w", encoding="utf-8") as fh:
        json.dump(final_payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
    summary = {
        "params": params,
        "total_committed": total,
        "checkpoint_seqs": wanted,
        "files": written + ["final.json"],
        "final_account_count": len(accounts),
    }
    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2, sort_keys=True)
    log.info("oracle 完成：%s 笔提交，%s 个检查点 -> %s",
             total, len(wanted), out_dir)
    return summary


def default_checkpoint_seqs(total_committed: int,
                            fractions: tuple[float, ...] = (0.0, 0.25, 0.5,
                                                            0.75, 1.0)
                            ) -> list[int]:
    seqs = {1, total_committed}
    for frac in fractions:
        seq = max(1, min(total_committed, round(total_committed * frac)))
        seqs.add(seq)
    return sorted(seqs)
