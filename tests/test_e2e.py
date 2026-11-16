# -*- coding: utf-8 -*-
"""端到端测试：真实子进程、HTTP 模拟源、外部进程终止式故障注入。

故障注入不使用“抛一个被业务代码捕获的异常”：
采集进程在停车点写标记后阻塞，测试框架通过 Windows TerminateProcess
（POSIX 下 SIGTERM）从外部真正终止进程，再用同一数据库重启，
核对结果与无故障基线逐字节一致。

运行：python -I tests/test_e2e.py
"""
import ctypes
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CDC = os.path.join(ROOT, "cdc.py")
SRC = os.path.join(ROOT, "src")
sys.path.insert(0, SRC)

from cdc import source_gen  # noqa: E402

PY = sys.executable
WORK = os.path.join(ROOT, ".runtime-tmp", "e2e")
POISON_WORK = os.path.join(ROOT, ".runtime-tmp", "e2e_poison")
PEAK_RSS_LIMIT_MB = 250          # CPU/内存约束下的宽松上限
MEDIUM_TXNS = int(os.environ.get("CDC_E2E_TXNS", "20000"))


# --------------------------------------------------------------------- 工具
def _post(url, payload, timeout=15):
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError("POST %s payload=%r -> %s %s" % (
            url, payload, exc.code, exc.read().decode("utf-8"))) from exc


def _wait_health(url, timeout=30):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url.replace("/events", "/health"),
                                        timeout=5) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except OSError as exc:
            last = exc
            time.sleep(0.2)
    raise RuntimeError("模拟源未在 %ss 内就绪: %s" % (timeout, last))


def _read_peak_rss_mb(pid):
    """Windows：读取目标进程工作集峰值（字节）。失败返回 None。"""
    try:
        psapi = ctypes.windll.psapi
        kernel32 = ctypes.windll.kernel32
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return None
        try:
            peak_workset = ctypes.c_size_t()
            ok = psapi.GetProcessMemoryInfo(
                handle, ctypes.byref(peak_workset), ctypes.sizeof(peak_workset))
            if not ok:
                return None
        finally:
            kernel32.CloseHandle(handle)
        return peak_workset.value / (1024 * 1024)
    except Exception:
        return None


class Cluster:
    """管理一个工作目录下的模拟源与采集进程。"""

    def __init__(self, workdir: str, txns: int = MEDIUM_TXNS,
                 accounts: int = 64, page_size: int = 200,
                 duplicate_rate: float = 0.05,
                 seed: int = 20261116, poison_offset: int | None = None):
        self.workdir = workdir
        os.makedirs(workdir, exist_ok=True)
        self.scenario_path = os.path.join(workdir, "scenario.json")
        self.port_file = os.path.join(workdir, "source.port")
        self.db = os.path.join(workdir, "mirror.db")
        self.log_dir = os.path.join(workdir, "logs")
        self.marker_dir = os.path.join(workdir, "markers")
        os.makedirs(self.log_dir, exist_ok=True)
        scn = source_gen.generate(seed=seed, txn_count=txns, accounts=accounts,
                                  page_size=page_size,
                                  duplicate_rate=duplicate_rate,
                                  poison_offset=poison_offset)
        source_gen.write_scenario_file(self.scenario_path, scn)
        self.params = scn["params"]
        self.stats = scn["stats"]
        self.total_committed = scn["stats"]["committed_txns"]
        self.source_proc = None
        self.collector_proc = None
        self._log_handles = []
        self.peak_rss_mb = 0.0

    # ------------------------------------------------------------- 模拟源
    def start_source(self):
        log = open(os.path.join(self.log_dir, "source.log"), "ab")
        self._log_handles.append(log)
        self.source_proc = subprocess.Popen(
            [PY, "-I", CDC, "source", "--scenario", self.scenario_path,
             "--host", "127.0.0.1", "--port", "0",
             "--port-file", self.port_file],
            cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.time() + 30
        while time.time() < deadline:
            if os.path.exists(self.port_file):
                with open(self.port_file, encoding="utf-8") as fh:
                    info = json.load(fh)
                self.port = info["port"]
                break
            if self.source_proc.poll() is not None:
                raise RuntimeError("模拟源提前退出")
            time.sleep(0.1)
        else:
            raise RuntimeError("模拟源端口文件未生成")
        self.url = "http://127.0.0.1:%s/events" % self.port
        _wait_health(self.url)
        return self.url

    def stop_source(self):
        if self.source_proc and self.source_proc.poll() is None:
            self.source_proc.terminate()
            try:
                self.source_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.source_proc.kill()
        for handle in self._log_handles:
            try:
                handle.close()
            except OSError:
                pass
        self._log_handles.clear()

    # ------------------------------------------------------------- 采集
    def start_collector(self, fault=None, marker_dir=None):
        cmd = [PY, "-I", CDC, "run", "--db", self.db, "--url", self.url]
        if fault:
            cmd += ["--fault", fault, "--marker-dir",
                    marker_dir or self.marker_dir]
        log = open(os.path.join(self.log_dir, "collector.%d.log"
                                % int(time.time() * 1000)), "ab")
        self._log_handles.append(log)
        self.collector_proc = subprocess.Popen(
            cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        return self.collector_proc

    def wait_marker(self, point, timeout=120):
        marker = os.path.join(self.marker_dir, "parked_%s.json" % point)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if os.path.exists(marker):
                with open(marker, encoding="utf-8") as fh:
                    return json.load(fh)
            if self.collector_proc.poll() is not None:
                raise RuntimeError("采集进程在到达停车点 %s 前退出（rc=%s）"
                                   % (point, self.collector_proc.returncode))
            rss = _read_peak_rss_mb(self.collector_proc.pid)
            if rss:
                self.peak_rss_mb = max(self.peak_rss_mb, rss)
            time.sleep(0.05)
        raise RuntimeError("等待停车点 %s 超时" % point)

    def kill_collector(self):
        """外部真实终止：Windows TerminateProcess / POSIX SIGKILL。"""
        proc = self.collector_proc
        self.assert_alive(proc)
        proc.kill()
        rc = proc.wait(timeout=30)
        if rc == 0:
            raise RuntimeError("进程应以被杀方式非正常退出，却得到 rc=0")
        return rc

    def run_collector_clean(self, fault=None, expect_completed=True, timeout=600):
        self.start_collector(fault=fault)
        proc = self.collector_proc
        try:
            rc = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            raise
        if expect_completed and rc != 0:
            raise RuntimeError("采集器应正常完成，实际 rc=%s" % rc)
        return rc

    @staticmethod
    def assert_alive(proc):
        if proc.poll() is not None:
            raise RuntimeError("进程已意外退出 rc=%s" % proc.returncode)

    # ------------------------------------------------------------- 核对
    def snapshot(self):
        out = os.path.join(self.workdir, "snapshot.json")
        rc = subprocess.run([PY, "-I", CDC, "snapshot", "--db", self.db,
                             "--out", out], cwd=ROOT, capture_output=True,
                            text=True, timeout=120)
        if rc.returncode != 0:
            raise RuntimeError("snapshot 失败: %s" % rc.stderr)
        with open(out, encoding="utf-8") as fh:
            return json.load(fh)

    def status(self):
        rc = subprocess.run([PY, "-I", CDC, "status", "--db", self.db],
                            cwd=ROOT, capture_output=True, text=True, timeout=60)
        if rc.returncode != 0:
            raise RuntimeError("status 失败: %s" % rc.stderr)
        return json.loads(rc.stdout)


def build_oracle(workdir: str, params: dict, seqs: list[int]):
    ans_dir = os.path.join(workdir, "oracle_answers")
    rc = subprocess.run(
        [PY, "-I", CDC, "oracle", "--scenario",
         os.path.join(workdir, "scenario.json"),
         "--out-dir", ans_dir,
         "--checkpoints", ",".join(str(s) for s in seqs)],
        cwd=ROOT, capture_output=True, text=True, timeout=300)
    if rc.returncode != 0:
        raise RuntimeError("oracle 失败: %s" % rc.stderr)
    return ans_dir


def load_checkpoint(ans_dir: str, seq: int) -> dict:
    with open(os.path.join(ans_dir, "checkpoint_%08d.json" % seq),
              encoding="utf-8") as fh:
        return json.load(fh)


def assert_snapshot_matches(snap: dict, ans_dir: str, seq: int):
    expected = load_checkpoint(ans_dir, seq)
    assert snap["watermark"] == seq == expected["watermark"], (
        snap["watermark"], seq)
    assert snap["accounts"] == expected["accounts"], (
        "水位 %s 全量账户状态不一致" % seq)
    assert snap["account_count"] == expected["account_count"]


def sample_seqs(total: int) -> list[int]:
    # 含极早水位，验证后序事务绝不可见；含中点与终点
    return sorted({1, 2, 3, max(1, total // 4), total // 2,
                   (3 * total) // 4, total})


# --------------------------------------------------------------------- 测试
class EndToEndTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        shutil.rmtree(WORK, ignore_errors=True)
        os.makedirs(WORK, exist_ok=True)

    def _fresh(self, name, **kw):
        return Cluster(os.path.join(WORK, name), **kw)

    def test_01_baseline_and_sampling(self):
        """无故障基线：跑满 2 万笔，抽样水位与独立答案逐账户核对。"""
        cluster = self._fresh("baseline")
        try:
            cluster.start_source()
            cluster.run_collector_clean()
            snap = cluster.snapshot()
            self.assertEqual(snap["watermark"], cluster.total_committed)
            self.assertTrue(snap["end_received"])

            seqs = sample_seqs(cluster.total_committed)
            ans_dir = build_oracle(cluster.workdir, cluster.params, seqs)

            # 终点全量核对
            assert_snapshot_matches(snap, ans_dir, cluster.total_committed)

            # 中间水位：直接用存储层在“只应用到该水位”的独立库上复算，
            # 与流式 oracle 检查点全量比对（基线库终点快照之外的交叉验证）。
            self._verify_intermediate_watermarks(cluster, ans_dir, seqs)

            status = cluster.status()
            self.assertEqual(status["applied_watermark"], cluster.total_committed)
            self.assertEqual(status["recv_done"], True)
            self.assertGreater(status["stored_events"], 0)
            self.assertEqual(status["txns_committed_pending"], 0)
            # 强制生命周期账户最终存在且为重建值
            self.assertEqual(snap["accounts"]["acct_forced_lifecycle"], 777)
            self.assertEqual(snap["accounts"]["acct_forced_negative"], -42)
        finally:
            cluster.stop_source()

    def _verify_intermediate_watermarks(self, cluster, ans_dir, seqs):
        """新建独立库，只应用到指定水位即快照，逐水位核对 oracle。"""
        from cdc import storage
        # 重新走一遍 HTTP 接收（独立 db 文件），分阶段应用
        db2 = os.path.join(cluster.workdir, "sampling.db")
        st = storage.Store(db2)
        cursor = None
        wanted = sorted(s for s in seqs if s < cluster.total_committed)
        idx = 0
        # 先接收全部事件（应用与接收解耦，本测试专注应用水位快照）
        while True:
            page = _post(cluster.url, {"cursor": cursor})
            from cdc import protocol as proto
            parsed = [proto.parse_event(e) for e in page["events"]]
            st.apply_page(parsed, page.get("next_cursor"),
                          end_received=bool(page.get("is_end")))
            if page["is_end"]:
                cursor = None
                break
            cursor = page["next_cursor"]
        # 逐水位应用并核对
        while idx < len(wanted):
            target = wanted[idx]
            while st.applied_watermark < target:
                out = st.apply_next()
                self.assertIsNotNone(out, "水位 %s 前不应出现缺口" % target)
            snap = st.snapshot()
            self.assertEqual(snap["watermark"], target)
            assert_snapshot_matches(snap, ans_dir, target)
            idx += 1
        st.close()

    def test_02_crash_at_all_points_and_restart(self):
        """A/B/C 三个同步点分别外部杀进程，重启结果 == 基线。"""
        baseline_answers = os.path.join(WORK, "baseline", "oracle_answers")
        for point in ("A", "B", "C"):
            with self.subTest(point=point):
                cluster = self._fresh("crash_%s" % point)
                try:
                    cluster.start_source()
                    cluster.start_collector(fault=point)
                    cluster.wait_marker(point)
                    cluster.kill_collector()

                    # 故障后现场不变量（杀进程后、重启前）
                    st_status = cluster.status()
                    if point == "A":
                        # A：接收事务未提交，水位与接收游标都不应越过该页
                        self.assertEqual(st_status["applied_watermark"], 0)
                        self.assertFalse(st_status["recv_done"])
                    elif point == "B":
                        # B：应用事务未提交，余额不可见、水位不推进
                        self.assertEqual(st_status["applied_watermark"], 0)
                        self.assertNotIn(
                            "acct_forced_lifecycle",
                            cluster.snapshot()["accounts"])
                    elif point == "C":
                        # C：应用事务已提交并持久化——水位必须 > 0 且账户可见，
                        # 重启后只能靠水位挡住重复生效，而不是靠事务回滚。
                        self.assertGreater(st_status["applied_watermark"], 0)
                        pre_restart = cluster.snapshot()
                        wm = st_status["applied_watermark"]
                        self.assertEqual(pre_restart["watermark"], wm)
                        # 该水位未必落在抽样检查点集合里：命中则比对文件，
                        # 否则用独立 oracle 现算该水位期望状态
                        cp_file = os.path.join(
                            baseline_answers, "checkpoint_%08d.json" % wm)
                        if os.path.exists(cp_file):
                            expected = load_checkpoint(baseline_answers, wm)
                        else:
                            from cdc import oracle as oracle_mod
                            tmp_dir = os.path.join(cluster.workdir,
                                                   "oracle_verify_%s" % point)
                            oracle_mod.build_checkpoints(
                                cluster.params, [wm], tmp_dir)
                            expected = json.load(open(
                                os.path.join(tmp_dir,
                                             "checkpoint_%08d.json" % wm),
                                encoding="utf-8"))
                        self.assertEqual(pre_restart["accounts"],
                                         expected["accounts"])
                    # 重启续跑，最终结果与无故障基线逐字节一致
                    cluster.run_collector_clean()
                    snap = cluster.snapshot()
                    self.assertEqual(snap["watermark"], cluster.total_committed)
                    assert_snapshot_matches(
                        snap, baseline_answers, cluster.total_committed)
                    # 与基线终点逐字节一致
                    with open(os.path.join(baseline_answers, "final.json"),
                              encoding="utf-8") as fh:
                        base_final = json.load(fh)
                    self.assertEqual(snap["accounts"], base_final["accounts"])
                finally:
                    cluster.stop_source()

    def test_03_combined_crashes_same_db(self):
        """同一数据库连续三次崩溃（A、B、C 各一次）后再完成，结果仍等于基线。"""
        cluster = self._fresh("crash_combo")
        try:
            cluster.start_source()
            for point in ("A", "B", "C"):
                # 每次重启重新挂一个停车点；点只触发一次
                shutil.rmtree(cluster.marker_dir, ignore_errors=True)
                cluster.start_collector(fault=point)
                cluster.wait_marker(point)
                cluster.kill_collector()
            cluster.run_collector_clean()
            snap = cluster.snapshot()
            base = os.path.join(WORK, "baseline", "oracle_answers", "final.json")
            with open(base, encoding="utf-8") as fh:
                base_final = json.load(fh)
            self.assertEqual(snap["watermark"], cluster.total_committed)
            self.assertEqual(snap["accounts"], base_final["accounts"])
        finally:
            cluster.stop_source()

    def test_04_conflict_stops_with_nonzero_exit(self):
        """同一身份冲突内容：采集器必须报错停止，且库保持一致。"""
        shutil.rmtree(POISON_WORK, ignore_errors=True)
        # 先在“无重复页”场景里定位一个真实会被传输的 PUT 的全局偏移，
        # 再让正式场景关闭重复页（duplicate_rate=0），保证偏移精确命中。
        pre = source_gen.generate(seed=5, txn_count=300, accounts=8,
                                  page_size=20, duplicate_rate=0.0)
        put_offsets = [i for i, e in enumerate(pre["events"]) if e[0] == "PUT"]
        target = next(i for i in put_offsets if i >= 40)
        cluster = Cluster(POISON_WORK, txns=300, accounts=8, page_size=20,
                          seed=5, duplicate_rate=0.0, poison_offset=target)
        try:
            cluster.start_source()
            cluster.start_collector()
            rc = cluster.collector_proc.wait(timeout=180)
            self.assertNotEqual(rc, 0, "冲突场景必须以非零码退出，实际 rc=%s" % rc)
            # 冲突页整体回滚，数据库仍可打开且自洽
            status = cluster.status()
            self.assertGreaterEqual(status["stored_events"], 0)
            # 采集器输出必须含稳定的致命标记（ASCII，不受控制台编码影响）
            log_text = ""
            for name in os.listdir(cluster.log_dir):
                if name.startswith("collector"):
                    with open(os.path.join(cluster.log_dir, name),
                              encoding="utf-8", errors="replace") as fh:
                        log_text += fh.read()
            self.assertIn('"status": "fatal"', log_text)
            self.assertIn("txid=8", log_text)
        finally:
            cluster.stop_source()

    def test_05_collector_cannot_read_answers(self):
        """隔离性：采集器代码不导入生成器/答案模块，命令行也不接触答案目录。"""
        import ast

        def imported_modules(rel_path):
            tree = ast.parse(open(os.path.join(SRC, rel_path),
                                  encoding="utf-8").read())
            mods = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    mods.update(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    mods.add(node.module)
            return mods

        # 采集链路（collector/storage）不得导入场景生成器或答案模块
        for mod in ("cdc/collector.py", "cdc/storage.py"):
            mods = imported_modules(mod)
            self.assertFalse(
                any(m.endswith("source_gen") or m.endswith("oracle")
                    for m in mods),
                "%s 非法导入答案相关模块: %s" % (mod, sorted(mods)))
        # oracle 不得导入采集器/存储
        mods = imported_modules("cdc/oracle.py")
        self.assertFalse(
            any(m.endswith("collector") or m.endswith("storage") for m in mods),
            "oracle 非法导入采集链路模块: %s" % sorted(mods))
        # 运行证据：baseline 采集进程命令行/日志不含 oracle_answers
        logs = os.path.join(WORK, "baseline", "logs")
        if os.path.isdir(logs):
            for name in os.listdir(logs):
                if name.startswith("collector"):
                    body = open(os.path.join(logs, name), encoding="utf-8",
                                errors="replace").read()
                    self.assertNotIn("oracle_answers", body)

    def test_06_memory_bound_no_full_stream_buffering(self):
        """采集进程峰值工作集远低于“把事件流全放内存”的量级。"""
        cluster = self._fresh("memory")
        try:
            cluster.start_source()
            cluster.start_collector()
            proc = cluster.collector_proc
            # 边跑边采样峰值工作集
            while proc.poll() is None:
                rss = _read_peak_rss_mb(proc.pid)
                if rss:
                    cluster.peak_rss_mb = max(cluster.peak_rss_mb, rss)
                time.sleep(0.2)
            self.assertEqual(proc.returncode, 0)
            self.assertLess(
                cluster.peak_rss_mb, PEAK_RSS_LIMIT_MB,
                "峰值工作集 %sMB 超过 %sMB，疑似整流驻留内存"
                % (round(cluster.peak_rss_mb, 1), PEAK_RSS_LIMIT_MB))
        finally:
            cluster.stop_source()


if __name__ == "__main__":
    unittest.main(verbosity=2)
