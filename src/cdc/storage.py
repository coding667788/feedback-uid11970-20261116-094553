# -*- coding: utf-8 -*-
"""SQLite 持久化：接收水位与应用水位严格分离。

两套各自原子的提交：

1. 接收侧（apply_page）：一页内的全部事件 + 该页接收游标在同一个 SQLite
   事务里提交。崩溃只会看到“整页已落盘且游标推进”或“整页都没落盘”，
   不会出现游标先于事件、或事件落了一半的状态。
2. 应用侧（apply_next）：一笔事务的全部余额改动 + applied_watermark 推进
   在同一个 SQLite 事务里提交。读者永远看不到半笔事务，也看不到后序事务。

游标只代表“已可靠接收”，applied_watermark 才代表“已生效顺序”，两者分别持久化。
未完成事务可以落盘（txns/raw_events），内存中不保存整个事件流。
"""
from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from typing import Any

from . import protocol as p

SCHEMA_VERSION = 1

DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS raw_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    etype      TEXT NOT NULL,
    txid       INTEGER,
    op_index   INTEGER,
    account    TEXT,
    balance    INTEGER,
    commit_seq INTEGER,
    op_count   INTEGER,
    end_flag   INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_op
    ON raw_events(txid, op_index) WHERE etype IN ('PUT','DELETE');
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_terminal
    ON raw_events(txid) WHERE etype IN ('COMMIT','ABORT');
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_begin
    ON raw_events(txid) WHERE etype = 'BEGIN';
CREATE TABLE IF NOT EXISTS txns (
    txid            INTEGER PRIMARY KEY,
    state           TEXT NOT NULL CHECK (state IN ('OPEN','COMMITTED','ABORTED','APPLIED')),
    commit_seq      INTEGER UNIQUE,
    op_count        INTEGER,
    ops_received    INTEGER NOT NULL DEFAULT 0,
    min_op          INTEGER,
    max_op          INTEGER,
    applied         INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS accounts (
    account TEXT PRIMARY KEY,
    balance INTEGER NOT NULL,
    version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS apply_log (
    commit_seq INTEGER PRIMARY KEY,
    txid       INTEGER NOT NULL
);
"""

META_RECV_CURSOR = "recv_cursor"          # 已可靠接收并落盘的下一页游标（可为 null）
META_RECV_DONE = "recv_done"              # END 事件是否已随某页原子落盘 (0/1)
META_APPLIED_WM = "applied_watermark"     # 已连续应用的最大 commit_seq
META_SCHEMA = "schema_version"

STATE_OPEN = "OPEN"
STATE_COMMITTED = "COMMITTED"
STATE_ABORTED = "ABORTED"
STATE_APPLIED = "APPLIED"


class Store:
    """单连接封装。SQLite 连接默认隔离级别下，我们用显式 BEGIN IMMEDIATE 管理事务。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self._init_schema()
        self._integrity_self_check()

    # ------------------------------------------------------------------ 基础
    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _safe_rollback(self) -> None:
        # executescript 等语句失败时事务可能已被 SQLite 隐式关闭
        try:
            self.conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass

    def _init_schema(self) -> None:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            # 逐条执行：executescript 会隐式提交，破坏外层事务边界
            for stmt in (s.strip() for s in DDL.split(";") if s.strip()):
                self.conn.execute(stmt)
            row = self.conn.execute(
                "SELECT value FROM meta WHERE key=?", (META_SCHEMA,)).fetchone()
            if row is None:
                self.conn.execute(
                    "INSERT INTO meta(key,value) VALUES(?,?)",
                    (META_SCHEMA, str(SCHEMA_VERSION)))
                self.conn.execute(
                    "INSERT INTO meta(key,value) VALUES(?,?)",
                    (META_APPLIED_WM, "0"))
                self.conn.execute(
                    "INSERT INTO meta(key,value) VALUES(?,?)",
                    (META_RECV_DONE, "0"))
            self.conn.execute("COMMIT")
        except BaseException:
            self._safe_rollback()
            raise

    def _get_meta(self, key: str) -> str | None:
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return None if row is None else row["value"]

    def _set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))

    @property
    def recv_cursor(self) -> str | None:
        return self._get_meta(META_RECV_CURSOR)

    @property
    def recv_done(self) -> bool:
        return self._get_meta(META_RECV_DONE) == "1"

    @property
    def applied_watermark(self) -> int:
        return int(self._get_meta(META_APPLIED_WM) or "0")

    def _integrity_self_check(self) -> None:
        """打开库时做一次不变量检查：已应用水位与 apply_log/txns 必须一致。"""
        wm = self.applied_watermark
        logged = self.conn.execute(
            "SELECT COUNT(*) c, COALESCE(MAX(commit_seq),0) m FROM apply_log"
        ).fetchone()
        if logged["c"] != wm or logged["m"] != wm:
            raise RuntimeError("数据库不变量被破坏：apply_log 与水位不一致")
        if wm:
            gap = self.conn.execute(
                "SELECT 1 FROM txns WHERE applied=1 AND "
                "(commit_seq IS NULL OR commit_seq>? OR commit_seq<1) LIMIT 1",
                (wm,)).fetchone()
            if gap:
                raise RuntimeError("数据库不变量被破坏：已应用事务序号越界")

    # ------------------------------------------------------------ 接收侧
    def _existing_op(self, txid: int, op_index: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM raw_events WHERE txid=? AND op_index=? "
            "AND etype IN ('PUT','DELETE')", (txid, op_index)).fetchone()

    def _existing_terminal(self, txid: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM raw_events WHERE txid=? "
            "AND etype IN ('COMMIT','ABORT')", (txid,)).fetchone()

    def _existing_begin(self, txid: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM raw_events WHERE txid=? AND etype='BEGIN'",
            (txid,)).fetchone()

    @staticmethod
    def _row_matches(row: sqlite3.Row, ev: dict) -> bool:
        return (row["etype"] == ev["etype"] and row["txid"] == ev["txid"]
                and _same(row["op_index"], ev["op_index"])
                and _same(row["account"], ev["account"])
                and _same(row["balance"], ev["balance"])
                and _same(row["commit_seq"], ev["commit_seq"])
                and _same(row["op_count"], ev["op_count"])
                and row["end_flag"] == ev["end_flag"])

    def _upsert_txn_open(self, txid: int) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO txns(txid,state) VALUES(?,?)",
            (txid, STATE_OPEN))

    def _record_op(self, ev: dict) -> None:
        self._upsert_txn_open(ev["txid"])
        self.conn.execute(
            "UPDATE txns SET ops_received=ops_received+1, "
            "min_op=COALESCE(MIN(min_op,?),?), max_op=COALESCE(MAX(max_op,?),?) "
            "WHERE txid=?",
            (ev["op_index"], ev["op_index"], ev["op_index"], ev["op_index"],
             ev["txid"]))

    def apply_page(self, events: list[dict], next_cursor: str | None,
                   end_received: bool,
                   before_commit_hook: Callable[[str], None] | None = None) -> dict:
        """原子接收一页：所有事件 + 接收游标同事务提交。

        before_commit_hook('recv') 在事件已写入、游标已更新但 COMMIT 尚未
        执行时调用——即故障点 A（事件已接收、游标未提交）。
        返回统计 {new_events, duplicate_events, end_flag}。
        """
        if not events and not end_received:
            raise p.ProtocolError("空页面且非 END，源端分页非法")
        if end_received and not any(e["etype"] == p.END for e in events):
            raise p.ProtocolError("end_received 与页面内容不一致")

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            stats = {"new_events": 0, "duplicate_events": 0, "end_flag": False}
            for ev in events:
                etype = ev["etype"]

                if etype in p.OP_TYPES:
                    row = self._existing_op(ev["txid"], ev["op_index"])
                    if row is not None:
                        if not self._row_matches(row, ev):
                            raise p.ConflictError(
                                "事件身份冲突 txid=%s op_index=%s：库中 %s vs 新事件 %s"
                                % (ev["txid"], ev["op_index"],
                                   dict(row), p.normalize_tuple(ev)))
                        stats["duplicate_events"] += 1
                        continue
                    self.conn.execute(
                        "INSERT INTO raw_events(etype,txid,op_index,account,balance,"
                        "commit_seq,op_count,end_flag) VALUES(?,?,?,?,?,?,?,?)",
                        (etype, ev["txid"], ev["op_index"], ev["account"],
                         ev["balance"], ev["commit_seq"], ev["op_count"],
                         ev["end_flag"]))
                    self._record_op(ev)
                    stats["new_events"] += 1

                elif etype == p.BEGIN:
                    row = self._existing_begin(ev["txid"])
                    if row is not None:
                        if not self._row_matches(row, ev):
                            raise p.ConflictError(
                                "BEGIN 身份冲突 txid=%s" % ev["txid"])
                        stats["duplicate_events"] += 1
                        continue
                    self.conn.execute(
                        "INSERT INTO raw_events(etype,txid,op_index,account,balance,"
                        "commit_seq,op_count,end_flag) VALUES(?,?,?,?,?,?,?,?)",
                        tuple(ev[k] for k in
                              ("etype", "txid", "op_index", "account", "balance",
                               "commit_seq", "op_count", "end_flag")))
                    # 重复 BEGIN 不应回退已有状态
                    self._upsert_txn_open(ev["txid"])
                    stats["new_events"] += 1

                elif etype in p.TERMINAL_TYPES:
                    row = self._existing_terminal(ev["txid"])
                    if row is not None:
                        if row["etype"] != etype or not self._row_matches(row, ev):
                            raise p.ConflictError(
                                "终结事件身份冲突 txid=%s：库中 %s vs 新事件 %s"
                                % (ev["txid"], dict(row), p.normalize_tuple(ev)))
                        stats["duplicate_events"] += 1
                        continue
                    self._upsert_txn_open(ev["txid"])
                    if etype == p.COMMIT:
                        self.conn.execute(
                            "UPDATE txns SET state=?, commit_seq=?, op_count=? "
                            "WHERE txid=?",
                            (STATE_COMMITTED, ev["commit_seq"], ev["op_count"],
                             ev["txid"]))
                    else:
                        self.conn.execute(
                            "UPDATE txns SET state=? WHERE txid=?",
                            (STATE_ABORTED, ev["txid"]))
                    self.conn.execute(
                        "INSERT INTO raw_events(etype,txid,op_index,account,balance,"
                        "commit_seq,op_count,end_flag) VALUES(?,?,?,?,?,?,?,?)",
                        tuple(ev[k] for k in
                              ("etype", "txid", "op_index", "account", "balance",
                               "commit_seq", "op_count", "end_flag")))
                    stats["new_events"] += 1

                elif etype == p.END:
                    stats["end_flag"] = True
                    # END 也作为事件落盘，保证“页与游标一致提交”可核验
                    existing = self.conn.execute(
                        "SELECT 1 FROM raw_events WHERE end_flag=1 LIMIT 1"
                    ).fetchone()
                    if existing is None:
                        self.conn.execute(
                            "INSERT INTO raw_events(etype,txid,op_index,account,"
                            "balance,commit_seq,op_count,end_flag) "
                            "VALUES('END',NULL,NULL,NULL,NULL,NULL,NULL,1)")
                        stats["new_events"] += 1
                    else:
                        stats["duplicate_events"] += 1

                else:  # parse_event 已拦截，防御性分支
                    raise p.ProtocolError("未知事件类型: %r" % (etype,))

            # 游标与事件同事务：游标只代表接收位置，绝不代表应用位置
            if next_cursor is not None:
                self._set_meta(META_RECV_CURSOR, next_cursor)
            if end_received:
                self._set_meta(META_RECV_DONE, "1")

            if before_commit_hook is not None:
                before_commit_hook("recv")
            self.conn.execute("COMMIT")
            return stats
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------ 应用侧
    def next_applicable_seq(self) -> int | None:
        """水位的下一个连续序号，且对应事务已完整可应用；否则 None（等待补齐）。"""
        expected = self.applied_watermark + 1
        row = self.conn.execute(
            "SELECT * FROM txns WHERE commit_seq=?", (expected,)).fetchone()
        if row is None or row["state"] != STATE_COMMITTED:
            return None
        if not self._txn_complete(row):
            return None
        return expected

    def _txn_complete(self, row: sqlite3.Row) -> bool:
        if row["ops_received"] != row["op_count"]:
            return False
        if row["op_count"] == 0:
            return row["min_op"] is None and row["max_op"] is None
        return (row["min_op"] == 0 and row["max_op"] == row["op_count"] - 1
                and row["ops_received"] == row["op_count"])

    def apply_next(self,
                   before_commit_hook: Callable[[str], None] | None = None
                   ) -> dict | None:
        """原子应用水位上的下一笔完整事务；无可应用事务返回 None。

        before_commit_hook('apply') 在余额已写入、水位尚未提交时调用
        ——即故障点 B（余额写入但事务未提交）。
        """
        expected = self.applied_watermark + 1
        row = self.conn.execute(
            "SELECT * FROM txns WHERE commit_seq=?", (expected,)).fetchone()
        if row is None or row["state"] != STATE_COMMITTED:
            return None
        if not self._txn_complete(row):
            return None
        txid = row["txid"]

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            # 二次校验放在事务内，防止与任何写入交错
            row = self.conn.execute(
                "SELECT * FROM txns WHERE txid=?", (txid,)).fetchone()
            if row is None or row["state"] != STATE_COMMITTED \
                    or row["commit_seq"] != expected or not self._txn_complete(row):
                raise RuntimeError("可应用事务在应用瞬间状态变化 txid=%s" % txid)

            ops = self.conn.execute(
                "SELECT etype,op_index,account,balance FROM raw_events "
                "WHERE txid=? AND etype IN ('PUT','DELETE') ORDER BY op_index",
                (txid,)).fetchall()
            if len(ops) != row["op_count"]:
                raise RuntimeError("事务 %s 操作数与 op_count 不符" % txid)
            seen_idx: set[int] = set()
            for op in ops:
                if op["op_index"] in seen_idx:
                    raise RuntimeError("事务 %s op_index 重复 %s"
                                       % (txid, op["op_index"]))
                seen_idx.add(op["op_index"])
            if seen_idx and (min(seen_idx) != 0 or max(seen_idx) != len(ops) - 1):
                raise RuntimeError("事务 %s op_index 不从 0 连续" % txid)

            changes = {"puts": 0, "deletes": 0, "delete_noop": 0}
            for op in ops:
                if op["etype"] == p.PUT:
                    self.conn.execute(
                        "INSERT INTO accounts(account,balance,version) "
                        "VALUES(?,?,1) ON CONFLICT(account) DO UPDATE SET "
                        "balance=excluded.balance, version=accounts.version+1",
                        (op["account"], op["balance"]))
                    changes["puts"] += 1
                else:  # DELETE：不存在账户为空操作
                    exists = self.conn.execute(
                        "SELECT 1 FROM accounts WHERE account=?",
                        (op["account"],)).fetchone()
                    if exists is None:
                        changes["delete_noop"] += 1
                    else:
                        self.conn.execute(
                            "DELETE FROM accounts WHERE account=?",
                            (op["account"],))
                        changes["deletes"] += 1

            self.conn.execute(
                "UPDATE txns SET state=?, applied=1 WHERE txid=?",
                (STATE_APPLIED, txid))
            self.conn.execute(
                "INSERT INTO apply_log(commit_seq,txid) VALUES(?,?)",
                (expected, txid))
            self._set_meta(META_APPLIED_WM, str(expected))

            if before_commit_hook is not None:
                before_commit_hook("apply")
            self.conn.execute("COMMIT")
            return {"commit_seq": expected, "txid": txid, **changes}
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------ 收尾与读
    def final_check_or_raise(self) -> None:
        """END 已接收后调用：缺操作、缺序号、未终结事务一律明确失败。"""
        problems: list[str] = []
        unfinished = self.conn.execute(
            "SELECT txid,state,ops_received,op_count,commit_seq FROM txns "
            "WHERE state IN (?,?)", (STATE_OPEN, STATE_COMMITTED)).fetchall()
        for r in unfinished:
            if r["state"] == STATE_OPEN:
                problems.append("事务 %s 缺少终结事件（OPEN）" % r["txid"])
            else:
                problems.append(
                    "事务 %s 已 COMMIT(seq=%s) 但操作不全：%s/%s"
                    % (r["txid"], r["commit_seq"],
                       r["ops_received"], r["op_count"]))

        wm = self.applied_watermark
        max_seq = self.conn.execute(
            "SELECT COALESCE(MAX(commit_seq),0) m FROM txns WHERE state IN (?,?)",
            (STATE_COMMITTED, STATE_APPLIED)).fetchone()["m"]
        if max_seq != wm:
            problems.append("commit_seq 水位缺口：已应用 %s，源端最大 %s"
                            % (wm, max_seq))

        # 序号连续性与唯一性
        seqs = [r["commit_seq"] for r in self.conn.execute(
            "SELECT commit_seq FROM txns WHERE commit_seq IS NOT NULL "
            "ORDER BY commit_seq")]
        if seqs != list(range(1, len(seqs) + 1)):
            problems.append("commit_seq 不连续或重复: %s" % _summarize_gaps(seqs))

        end_row = self.conn.execute(
            "SELECT 1 FROM raw_events WHERE end_flag=1 LIMIT 1").fetchone()
        if end_row is None:
            problems.append("END 事件未落盘")

        if problems:
            raise RuntimeError("END 后完整性校验失败：\n  - " + "\n  - ".join(problems))

    def snapshot(self) -> dict:
        """一次读事务导出连续水位及该水位下的完整账户状态。"""
        self.conn.execute("BEGIN DEFERRED")
        try:
            wm = self.applied_watermark
            accounts = {
                r["account"]: r["balance"]
                for r in self.conn.execute(
                    "SELECT account,balance FROM accounts ORDER BY account")}
            done = self.recv_done
            self.conn.execute("COMMIT")
        except BaseException:
            self._safe_rollback()
            raise
        return {
            "watermark": wm,
            "end_received": done,
            "account_count": len(accounts),
            "accounts": accounts,
        }

    def status(self) -> dict:
        self.conn.execute("BEGIN DEFERRED")
        try:
            out = {
                "db_path": self.db_path,
                "recv_cursor": self.recv_cursor,
                "recv_done": self.recv_done,
                "applied_watermark": self.applied_watermark,
                "stored_events": self.conn.execute(
                    "SELECT COUNT(*) c FROM raw_events").fetchone()["c"],
                "txns_open": self.conn.execute(
                    "SELECT COUNT(*) c FROM txns WHERE state=?",
                    (STATE_OPEN,)).fetchone()["c"],
                "txns_committed_pending": self.conn.execute(
                    "SELECT COUNT(*) c FROM txns WHERE state=?",
                    (STATE_COMMITTED,)).fetchone()["c"],
                "txns_aborted": self.conn.execute(
                    "SELECT COUNT(*) c FROM txns WHERE state=?",
                    (STATE_ABORTED,)).fetchone()["c"],
                "txns_applied": self.conn.execute(
                    "SELECT COUNT(*) c FROM txns WHERE state=?",
                    (STATE_APPLIED,)).fetchone()["c"],
                "account_count": self.conn.execute(
                    "SELECT COUNT(*) c FROM accounts").fetchone()["c"],
            }
            self.conn.execute("COMMIT")
        except BaseException:
            self._safe_rollback()
            raise
        return out

    def dump_meta_json(self) -> str:
        return json.dumps(self.status(), ensure_ascii=False, indent=2, sort_keys=True)


def _same(a: Any, b: Any) -> bool:
    return a == b


def _summarize_gaps(seqs: list[int]) -> str:
    missing = sorted(set(range(1, len(seqs) + 1)) - set(seqs))
    shown = missing[:10]
    return "缺失前10个=%s" % shown
