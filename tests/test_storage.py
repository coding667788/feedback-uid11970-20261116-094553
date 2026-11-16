# -*- coding: utf-8 -*-
"""存储层与协议单元测试：原子性、冲突、乱序、双水位、完整性失败。"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from cdc import protocol as p          # noqa: E402
from cdc import storage               # noqa: E402


def ev(obj: dict) -> dict:
    return p.parse_event(obj)


class StorageTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "test.db")
        self.store = storage.Store(self.db)

    def tearDown(self) -> None:
        # 先主动 WAL checkpoint 并关闭连接，避免 Windows 上 -wal/-shm
        # 句柄延迟释放导致 TemporaryDirectory 清理失败
        try:
            self.store.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass
        self.store.close()
        self.tmp.cleanup()

    def reopen(self) -> storage.Store:
        self.store.close()
        self.store = storage.Store(self.db)
        return self.store


class TestProtocol(unittest.TestCase):
    def test_cursor_roundtrip_and_garbage(self):
        tok = p.encode_cursor(7)
        self.assertEqual(p.decode_cursor(tok), 7)
        for bad in ("", "nope", "cur_!!!", 123, None):
            with self.assertRaises(p.ProtocolError):
                p.decode_cursor(bad)  # type: ignore[arg-type]

    def test_parse_validation(self):
        self.assertEqual(ev({"type": "PUT", "txid": 1, "op_index": 0,
                             "account": "a", "balance": -5})["balance"], -5)
        for bad in (
            {"type": "PUT", "txid": True, "op_index": 0, "account": "a", "balance": 1},
            {"type": "PUT", "txid": 1, "op_index": -1, "account": "a", "balance": 1},
            {"type": "PUT", "txid": 1, "op_index": 0, "account": "", "balance": 1},
            {"type": "PUT", "txid": 1, "op_index": 0, "account": "a", "balance": 1.5},
            {"type": "COMMIT", "txid": 1, "commit_seq": 0, "op_count": 1},
            {"type": "BEGIN", "txid": 0},
            {"type": "WEIRD"},
            "not-an-object",
        ):
            with self.assertRaises(p.ProtocolError):
                ev(bad)  # type: ignore[arg-type]

    def test_normalize_tuple_distinguishes_content(self):
        a = ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "a", "balance": 1})
        b = ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "a", "balance": 2})
        self.assertNotEqual(p.normalize_tuple(a), p.normalize_tuple(b))
        self.assertEqual(p.normalize_tuple(a), p.normalize_tuple(
            ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "a", "balance": 1})))


class TestApplyHappyPath(StorageTestBase):
    def test_ordered_apply_and_watermark(self):
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "A", "balance": 10}),
            ev({"type": "COMMIT", "txid": 1, "commit_seq": 1, "op_count": 1}),
        ], p.encode_cursor(1), False)
        self.assertEqual(self.store.recv_cursor, p.encode_cursor(1))
        self.assertEqual(self.store.applied_watermark, 0)  # 接收不等于应用
        out = self.store.apply_next()
        self.assertEqual(out["commit_seq"], 1)
        snap = self.store.snapshot()
        self.assertEqual(snap["watermark"], 1)
        self.assertEqual(snap["accounts"], {"A": 10})
        self.assertIsNone(self.store.apply_next())

    def test_out_of_order_waits_then_drains(self):
        # seq2 先完整到达
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 2}),
            ev({"type": "PUT", "txid": 2, "op_index": 0, "account": "B", "balance": 2}),
            ev({"type": "COMMIT", "txid": 2, "commit_seq": 2, "op_count": 1}),
        ], p.encode_cursor(1), False)
        self.assertIsNone(self.store.next_applicable_seq())  # 缺口：等 seq1
        self.assertEqual(self.store.snapshot()["accounts"], {})
        # seq1 到达后两笔连续应用
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "A", "balance": 1}),
            ev({"type": "COMMIT", "txid": 1, "commit_seq": 1, "op_count": 1}),
        ], p.encode_cursor(2), False)
        self.assertEqual(self.store.next_applicable_seq(), 1)
        self.assertIsNotNone(self.store.apply_next())
        self.assertIsNotNone(self.store.apply_next())
        self.assertEqual(self.store.applied_watermark, 2)
        self.assertEqual(self.store.snapshot()["accounts"], {"A": 1, "B": 2})

    def test_commit_before_ops_then_fill(self):
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 9}),
            ev({"type": "COMMIT", "txid": 9, "commit_seq": 1, "op_count": 2}),
        ], p.encode_cursor(1), False)
        self.assertIsNone(self.store.next_applicable_seq())  # 操作未齐
        self.store.apply_page([
            ev({"type": "PUT", "txid": 9, "op_index": 0, "account": "A", "balance": 5}),
            ev({"type": "PUT", "txid": 9, "op_index": 1, "account": "B", "balance": 6}),
        ], p.encode_cursor(2), False)
        self.assertEqual(self.store.next_applicable_seq(), 1)
        out = self.store.apply_next()
        self.assertEqual(out["puts"], 2)
        self.assertEqual(self.store.snapshot()["accounts"], {"A": 5, "B": 6})

    def test_interleaved_txns_across_pages(self):
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "BEGIN", "txid": 2}),
            ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "A", "balance": 1}),
        ], p.encode_cursor(1), False)
        self.store.apply_page([
            ev({"type": "PUT", "txid": 2, "op_index": 0, "account": "B", "balance": 2}),
            ev({"type": "COMMIT", "txid": 1, "commit_seq": 1, "op_count": 1}),
            ev({"type": "COMMIT", "txid": 2, "commit_seq": 2, "op_count": 1}),
        ], p.encode_cursor(2), False)
        self.store.apply_next()
        self.store.apply_next()
        self.assertEqual(self.store.snapshot()["accounts"], {"A": 1, "B": 2})

    def test_keep_fetching_while_gap_open(self):
        """缺口期间继续拉取并持久化后续事件，应用侧零推进、接收侧持续前进。"""
        # 只收到 seq3，seq1/seq2 缺失
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 30}),
            ev({"type": "PUT", "txid": 30, "op_index": 0, "account": "C", "balance": 3}),
            ev({"type": "COMMIT", "txid": 30, "commit_seq": 3, "op_count": 1}),
        ], p.encode_cursor(1), False)
        self.assertIsNone(self.store.next_applicable_seq())
        self.assertEqual(self.store.applied_watermark, 0)
        # 再来 seq4、seq5（模拟缺失事务期间持续到达的后续页），照样落盘
        for seq, txid, acct in ((4, 40, "D"), (5, 50, "E")):
            self.store.apply_page([
                ev({"type": "BEGIN", "txid": txid}),
                ev({"type": "PUT", "txid": txid, "op_index": 0,
                    "account": acct, "balance": seq}),
                ev({"type": "COMMIT", "txid": txid, "commit_seq": seq,
                    "op_count": 1}),
            ], p.encode_cursor(seq - 1), False)
        # 接收游标一路推进到第 4 页，但应用仍卡在水位 0，读者看不到任何后序事务
        self.assertEqual(self.store.recv_cursor, p.encode_cursor(4))
        self.assertEqual(self.store.applied_watermark, 0)
        self.assertEqual(self.store.snapshot()["accounts"], {})
        # 补齐 seq1、seq2 后，五笔事务按序一次性排干
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 10}),
            ev({"type": "PUT", "txid": 10, "op_index": 0, "account": "A", "balance": 1}),
            ev({"type": "COMMIT", "txid": 10, "commit_seq": 1, "op_count": 1}),
            ev({"type": "BEGIN", "txid": 20}),
            ev({"type": "PUT", "txid": 20, "op_index": 0, "account": "B", "balance": 2}),
            ev({"type": "COMMIT", "txid": 20, "commit_seq": 2, "op_count": 1}),
        ], p.encode_cursor(5), False)
        for _ in range(5):
            self.assertIsNotNone(self.store.apply_next())
        self.assertIsNone(self.store.apply_next())
        self.assertEqual(self.store.applied_watermark, 5)
        self.assertEqual(self.store.snapshot()["accounts"],
                         {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5})

    def test_abort_never_applies(self):
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "A", "balance": 9}),
            ev({"type": "ABORT", "txid": 1}),
            ev({"type": "END"}),
        ], None, True)
        self.assertIsNone(self.store.next_applicable_seq())
        self.store.final_check_or_raise()  # 中止事务不要求完整
        self.assertEqual(self.store.snapshot()["accounts"], {})

    def test_delete_noop_recreate_and_negative(self):
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "DELETE", "txid": 1, "op_index": 0, "account": "ghost"}),
            ev({"type": "PUT", "txid": 1, "op_index": 1, "account": "neg", "balance": -99}),
            ev({"type": "COMMIT", "txid": 1, "commit_seq": 1, "op_count": 2}),
        ], p.encode_cursor(1), False)
        out = self.store.apply_next()
        self.assertEqual(out["delete_noop"], 1)
        self.assertEqual(self.store.snapshot()["accounts"], {"neg": -99})
        # 第二笔：先删后建同账户
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 2}),
            ev({"type": "DELETE", "txid": 2, "op_index": 0, "account": "neg"}),
            ev({"type": "PUT", "txid": 2, "op_index": 1, "account": "neg", "balance": 7}),
            ev({"type": "COMMIT", "txid": 2, "commit_seq": 2, "op_count": 2}),
        ], p.encode_cursor(2), False)
        self.store.apply_next()
        self.assertEqual(self.store.snapshot()["accounts"], {"neg": 7})


class TestDuplicatesAndConflicts(StorageTestBase):
    def test_exact_duplicate_page_is_idempotent(self):
        page = [
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "A", "balance": 1}),
            ev({"type": "COMMIT", "txid": 1, "commit_seq": 1, "op_count": 1}),
        ]
        s1 = self.store.apply_page(list(page), p.encode_cursor(1), False)
        s2 = self.store.apply_page(list(page), p.encode_cursor(1), False)
        self.assertEqual((s1["new_events"], s1["duplicate_events"]), (3, 0))
        self.assertEqual((s2["new_events"], s2["duplicate_events"]), (0, 3))
        self.store.apply_next()
        self.reopen()
        # 重启后重放同一页仍无副作用，水位挡住重复生效
        s3 = self.store.apply_page(list(page), p.encode_cursor(1), False)
        self.assertEqual(s3["new_events"], 0)
        self.assertIsNone(self.store.apply_next())
        self.assertEqual(self.store.snapshot()["accounts"], {"A": 1})

    def test_put_content_conflict_raises_and_rolls_back(self):
        good = [
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "A", "balance": 1}),
        ]
        self.store.apply_page(good, p.encode_cursor(1), False)
        mixed = [
            ev({"type": "PUT", "txid": 1, "op_index": 1, "account": "B", "balance": 2}),
            ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "A", "balance": 999}),
        ]
        with self.assertRaises(p.ConflictError):
            self.store.apply_page(mixed, p.encode_cursor(2), False)
        # 整页回滚：B 不存在、游标未推进
        row = self.store.conn.execute(
            "SELECT 1 FROM raw_events WHERE account='B'").fetchone()
        self.assertIsNone(row)
        self.assertEqual(self.store.recv_cursor, p.encode_cursor(1))

    def test_commit_vs_abort_conflict(self):
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "COMMIT", "txid": 1, "commit_seq": 1, "op_count": 0}),
        ], p.encode_cursor(1), False)
        with self.assertRaises(p.ConflictError):
            self.store.apply_page([ev({"type": "ABORT", "txid": 1})],
                                  p.encode_cursor(2), False)

    def test_commit_payload_conflict(self):
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "COMMIT", "txid": 1, "commit_seq": 1, "op_count": 1}),
        ], p.encode_cursor(1), False)
        with self.assertRaises(p.ConflictError):
            self.store.apply_page([
                ev({"type": "COMMIT", "txid": 1, "commit_seq": 1, "op_count": 2})],
                p.encode_cursor(2), False)


class TestAtomicityHooks(StorageTestBase):
    def test_recv_hook_rollback_leaves_nothing(self):
        def boom(_phase):
            raise RuntimeError("simulated crash before recv commit")

        with self.assertRaises(RuntimeError):
            self.store.apply_page([
                ev({"type": "BEGIN", "txid": 1}),
                ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "A", "balance": 1}),
            ], p.encode_cursor(1), False, before_commit_hook=boom)
        self.assertIsNone(self.store.recv_cursor)
        self.assertEqual(self.store.conn.execute(
            "SELECT COUNT(*) c FROM raw_events").fetchone()["c"], 0)
        self.assertEqual(self.store.applied_watermark, 0)

    def test_apply_hook_rollback_leaves_no_balance(self):
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "A", "balance": 1}),
            ev({"type": "COMMIT", "txid": 1, "commit_seq": 1, "op_count": 1}),
        ], p.encode_cursor(1), False)

        def boom(_phase):
            raise RuntimeError("simulated crash before apply commit")

        with self.assertRaises(RuntimeError):
            self.store.apply_next(before_commit_hook=boom)
        self.assertEqual(self.store.applied_watermark, 0)
        self.assertEqual(self.store.snapshot()["accounts"], {})
        # 事务仍可重新应用
        self.assertIsNotNone(self.store.apply_next())
        self.assertEqual(self.store.snapshot()["accounts"], {"A": 1})


class TestFinalCheck(StorageTestBase):
    def _end_page(self, extra=None, cursor=None):
        events = list(extra or []) + [ev({"type": "END"})]
        self.store.apply_page(events, cursor, True)

    def test_missing_ops_fails(self):
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "A", "balance": 1}),
            ev({"type": "COMMIT", "txid": 1, "commit_seq": 1, "op_count": 3}),
            ev({"type": "END"}),
        ], None, True)
        with self.assertRaises(RuntimeError):
            self.store.final_check_or_raise()

    def test_open_txn_fails(self):
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "END"}),
        ], None, True)
        with self.assertRaises(RuntimeError):
            self.store.final_check_or_raise()

    def test_seq_gap_fails(self):
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "COMMIT", "txid": 1, "commit_seq": 2, "op_count": 0}),
            ev({"type": "END"}),
        ], None, True)
        with self.assertRaises(RuntimeError):
            self.store.final_check_or_raise()

    def test_complete_passes(self):
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "A", "balance": 1}),
            ev({"type": "COMMIT", "txid": 1, "commit_seq": 1, "op_count": 1}),
            ev({"type": "END"}),
        ], None, True)
        self.store.apply_next()
        self.store.final_check_or_raise()


class TestRestart(StorageTestBase):
    def test_reopen_continues(self):
        self.store.apply_page([
            ev({"type": "BEGIN", "txid": 1}),
            ev({"type": "PUT", "txid": 1, "op_index": 0, "account": "A", "balance": 1}),
            ev({"type": "COMMIT", "txid": 1, "commit_seq": 1, "op_count": 1}),
        ], p.encode_cursor(1), False)
        self.store.apply_next()
        st2 = self.reopen()
        self.assertEqual(st2.applied_watermark, 1)
        self.assertEqual(st2.recv_cursor, p.encode_cursor(1))
        self.assertEqual(st2.snapshot()["accounts"], {"A": 1})
        self.assertIsNone(st2.apply_next())


if __name__ == "__main__":
    unittest.main(verbosity=2)
