"""事件线格式与身份/冲突判定（源端与答案端共用）。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

BEGIN = "BEGIN"
PUT = "PUT"
DELETE = "DELETE"
COMMIT = "COMMIT"
ABORT = "ABORT"
END = "END"

TERMINAL_TYPES = (COMMIT, ABORT)


@dataclass(frozen=True, slots=True)
class Event:
    type: str
    txid: str
    op_index: int | None = None
    account: str | None = None
    balance: int | None = None
    commit_seq: int | None = None
    op_count: int | None = None

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"type": self.type, "txid": self.txid}
        if self.op_index is not None:
            data["op_index"] = self.op_index
        if self.account is not None:
            data["account"] = self.account
        if self.balance is not None:
            data["balance"] = self.balance
        if self.commit_seq is not None:
            data["commit_seq"] = self.commit_seq
        if self.op_count is not None:
            data["op_count"] = self.op_count
        return data


def from_dict(data: dict[str, Any]) -> Event:
    return Event(
        type=data["type"],
        txid=data["txid"],
        op_index=data.get("op_index"),
        account=data.get("account"),
        balance=data.get("balance"),
        commit_seq=data.get("commit_seq"),
        op_count=data.get("op_count"),
    )


def canonical_json(event: Event | dict[str, Any]) -> str:
    """同一身份事件的稳定序列化：字节完全相同即完全重复。"""
    data = event.to_dict() if isinstance(event, Event) else dict(event)
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def event_identity(event: Event | dict[str, Any]) -> tuple:
    """事件身份。PUT/DELETE 以 (txid, op_index) 为身份，类型不同也算冲突。"""
    data = event.to_dict() if isinstance(event, Event) else event
    etype = data["type"]
    if etype in (PUT, DELETE):
        return ("OP", data["txid"], data["op_index"])
    return (etype, data["txid"])
