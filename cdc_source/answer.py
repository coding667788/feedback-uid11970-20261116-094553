"""独立答案：只消费页面事件流，推导任意水位的完整账户状态。

本模块不读取采集器数据库，也不被 cdc_tool 导入。状态仅包含：
- 已应用的账户余额表（当前镜像）；
- 尚未终结的事务缓冲（事件可能乱序、COMMIT 先到）。
不保存整个事件流。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .events import ABORT, BEGIN, COMMIT, DELETE, PUT, Event, event_identity


@dataclass
class _TxBuffer:
    commit_seq: int | None = None
    op_count: int | None = None
    aborted: bool = False
    operations: dict[int, tuple[str, str, int | None]] = field(default_factory=dict)


class ConflictDetected(Exception):
    pass


class ExpectedModel:
    def __init__(self) -> None:
        self.balances: dict[str, int] = {}
        self.open_tx: dict[str, _TxBuffer] = {}
        self.applied_seq = 0
        self.identity_payload: dict[tuple, str] = {}
        from .events import canonical_json

        self._canonical_json = canonical_json

    def _check_identity(self, event: Event) -> None:
        identity = event_identity(event)
        payload = self._canonical_json(event)
        prior = self.identity_payload.get(identity)
        if prior is None:
            self.identity_payload[identity] = payload
        elif prior != payload:
            raise ConflictDetected(f"同身份事件内容冲突: {identity}")

    def handle_event(self, event: Event) -> None:
        self._check_identity(event)
        txid = event.txid
        buf = self.open_tx.get(txid)
        if event.type == BEGIN:
            if buf is None:
                self.open_tx[txid] = _TxBuffer()
            return
        if buf is None:
            buf = _TxBuffer()
            self.open_tx[txid] = buf
        if event.type == PUT:
            buf.operations[event.op_index] = (PUT, event.account, event.balance)
        elif event.type == DELETE:
            buf.operations[event.op_index] = (DELETE, event.account, None)
        elif event.type == COMMIT:
            buf.commit_seq = event.commit_seq
            buf.op_count = event.op_count
        elif event.type == ABORT:
            buf.aborted = True
        self._try_drain()

    def _try_drain(self) -> None:
        # 按连续 commit_seq 应用；只应用操作完整（op_index 0..op_count-1）的事务。
        by_seq: dict[int, str] = {}
        for txid, buf in self.open_tx.items():
            if buf.aborted and buf.commit_seq is None:
                # ABORT 可随时丢弃（不生效）。
                if self._abortable(buf):
                    del self.open_tx[txid]
                    return self._try_drain()
            if buf.commit_seq is not None:
                by_seq[buf.commit_seq] = txid
        target = self.applied_seq + 1
        while target in by_seq:
            txid = by_seq[target]
            buf = self.open_tx[txid]
            if not self._complete(buf):
                return  # 等缺失操作，期间继续接收后续事件
            self._apply(txid, buf)
            del self.open_tx[txid]
            self.applied_seq = target
            target += 1
            by_seq = {b.commit_seq: t for t, b in self.open_tx.items() if b.commit_seq is not None}

    @staticmethod
    def _abortable(buf: _TxBuffer) -> bool:
        return buf.aborted and buf.commit_seq is None

    @staticmethod
    def _complete(buf: _TxBuffer) -> bool:
        if buf.op_count is None:
            return False
        if len(buf.operations) != buf.op_count:
            return False
        return sorted(buf.operations) == list(range(buf.op_count))

    def _apply(self, txid: str, buf: _TxBuffer) -> None:
        for op_index in range(buf.op_count):
            op_type, account, balance = buf.operations[op_index]
            if op_type == PUT:
                self.balances[account] = balance
            else:
                # DELETE 不存在账户为空操作。
                self.balances.pop(account, None)

    def state_at_or_before(self, watermark: int) -> dict[str, int]:
        """已应用水位 == watermark 时的完整账户状态（拷贝）。"""
        if self.applied_seq < watermark:
            raise RuntimeError(f"答案水位 {self.applied_seq} 尚未到达 {watermark}")
        return dict(self.balances)


def build_expected_from_events(events: list[Event]) -> ExpectedModel:
    model = ExpectedModel()
    for event in events:
        model.handle_event(event)
    return model
