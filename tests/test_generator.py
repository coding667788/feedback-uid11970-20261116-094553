# -*- coding: utf-8 -*-
"""场景生成器与 HTTP 分页契约测试。"""
import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from cdc import protocol as p                       # noqa: E402
from cdc import source_gen, source_server          # noqa: E402


class TestGenerator(unittest.TestCase):
    def test_deterministic_and_well_formed_small(self):
        a = source_gen.generate(seed=42, txn_count=300, accounts=8, page_size=25)
        b = source_gen.regenerate(a["params"])
        self.assertEqual(a["events"], b["events"])
        self.assertEqual(a["pages"], b["pages"])

        # 事件结构与 op_index 连续性
        for txid, txn in a["canonical_txns"].items():
            if txn["state"] == "COMMITTED":
                self.assertEqual([op for op in range(txn["op_count"])],
                                 list(range(txn["op_count"])))
                self.assertEqual(len(txn["ops"]), txn["op_count"])

        # 最后一页含 END，其余页不含
        self.assertEqual(a["events"][-1][0], p.END)
        self.assertTrue(any(e[0] == p.END for e in a["pages"][-1]))
        for page in a["pages"][:-1]:
            self.assertFalse(any(e[0] == p.END for e in page))

        # 至少出现：删除后重建、早到 COMMIT、重复页、负余额、DELETE 空操作素材
        self.assertGreaterEqual(a["stats"]["early_commit_txns"], 1)
        self.assertTrue(any(
            e[0] == p.PUT and e[4] < 0 for e in a["events"]))
        # 强制生命周期：seq1 创建、seq2 删除、seq3 重建；负余额在 seq2
        t1 = a["canonical_txns"][1]
        t2 = a["canonical_txns"][2]
        t3 = a["canonical_txns"][3]
        self.assertEqual(t1["commit_seq"], 1)
        self.assertEqual(t1["ops"][0]["kind"], p.PUT)
        self.assertEqual(t1["ops"][0]["account"], "acct_forced_lifecycle")
        self.assertEqual(t2["commit_seq"], 2)
        self.assertEqual(t2["ops"][0]["kind"], p.DELETE)
        self.assertEqual(t2["ops"][0]["account"], "acct_forced_lifecycle")
        self.assertEqual(t2["ops"][1]["balance"], -42)
        self.assertEqual(t3["commit_seq"], 3)
        self.assertEqual(t3["ops"][0]["kind"], p.PUT)
        self.assertEqual(t3["ops"][0]["account"], "acct_forced_lifecycle")
        self.assertEqual(t3["ops"][0]["balance"], 777)

    def test_duplicate_pages_exist_and_share_content(self):
        scn = source_gen.generate(seed=7, txn_count=500, accounts=8,
                                  page_size=20, duplicate_rate=0.5)
        dupes = 0
        for i in range(1, len(scn["pages"])):
            if scn["pages"][i] == scn["pages"][i - 1]:
                dupes += 1
        self.assertGreater(dupes, 0)

    def test_poison_keeps_original_and_appends_conflicting_copy(self):
        scn = source_gen.generate(seed=3, txn_count=50, accounts=4,
                                  page_size=10, duplicate_rate=0.0)
        put_offsets = [i for i, e in enumerate(scn["events"]) if e[0] == p.PUT]
        target = put_offsets[0]
        original = scn["events"][target]
        poisoned = source_gen.generate(seed=3, txn_count=50, accounts=4,
                                       page_size=10, duplicate_rate=0.0,
                                       poison_offset=target)
        # 原版仍在流中
        self.assertIn(original, poisoned["events"])
        # 另有一个同身份（txid/op_index/account 相同）但余额矛盾的副本
        copies = [e for e in poisoned["events"]
                  if e[0] == p.PUT and e[1] == original[1]
                  and e[2] == original[2]]
        self.assertEqual(len(copies), 2)
        self.assertEqual(len({e[4] for e in copies}), 2)
        self.assertEqual(poisoned["events"][-1][0], p.END)

    def test_commit_seq_contiguous_20k(self):
        scn = source_gen.generate(seed=20261116, txn_count=20000)
        seqs = sorted(c["commit_seq"] for c in scn["canonical_txns"].values()
                      if c["state"] == "COMMITTED")
        self.assertEqual(seqs[0], 1)
        self.assertEqual(seqs[-1], len(seqs))
        self.assertEqual(seqs, list(range(1, len(seqs) + 1)))
        self.assertEqual(scn["stats"]["committed_txns"], len(seqs))


class TestHttpPagination(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        scn = source_gen.generate(seed=11, txn_count=200, accounts=6,
                                  page_size=15, duplicate_rate=0.4)
        httpd, self.source = source_server.make_server("127.0.0.1", 0, scn["params"])
        self.host, self.port = httpd.server_address[:2]
        self.thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        self.thread.start()
        self.httpd = httpd

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)
        self.tmp.cleanup()

    def _post(self, payload):
        req = urllib.request.Request(
            "http://%s:%s/events" % (self.host, self.port),
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def test_walk_replay_and_opaque_cursor(self):
        seen_events = []
        cursor = None
        pages = 0
        while True:
            page = self._post({"cursor": cursor})
            pages += 1
            seen_events.extend(page["events"])
            if page["is_end"]:
                self.assertIsNone(page["next_cursor"])
                break
            self.assertIsNotNone(page["next_cursor"])
            # 同游标重试：逐字节相同
            again = self._post({"cursor": cursor})
            self.assertEqual(again, page)
            cursor = page["next_cursor"]
            self.assertTrue(str(cursor).startswith("cur_"))  # 不透明
        self.assertEqual(seen_events[-1]["type"], p.END)
        self.assertEqual(pages, self.source.page_count())

        # 回退重放已确认页：旧游标仍返回同样内容
        first = self._post({"cursor": None})
        first_again = self._post({"cursor": None})
        self.assertEqual(first, first_again)

    def test_bad_cursor_400(self):
        import urllib.error
        req = urllib.request.Request(
            "http://%s:%s/events" % (self.host, self.port),
            data=json.dumps({"cursor": "cur_bogus"}).encode("utf-8"),
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            urllib.request.urlopen(req, timeout=10)
            self.fail("应返回 400")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 400)


if __name__ == "__main__":
    unittest.main(verbosity=2)
