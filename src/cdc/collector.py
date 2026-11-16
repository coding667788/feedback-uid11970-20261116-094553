# -*- coding: utf-8 -*-
"""采集归并主循环。

核心保证：
- 拉取 -> 校验 -> 整页事件与接收游标原子落盘；内存不保存整个事件流，
  页内事件解析后立即交给 SQLite，处理完即释放。
- 每页落盘后尽可能推进应用水位；只能按连续 commit_seq 应用完整事务。
- 等待缺失事务期间继续拉取后续页并持久化（由存储层天然背压，无内存堆积）。
- 崩溃恢复完全依赖 SQLite：游标、事件、水位都在库里，重启接着跑，
  重复事件按身份去重，已应用事务由水位挡住，绝不重复生效。

故障注入（不是“抛个被业务捕获的异常”）：
  停车点写标记文件后阻塞在 Event 上，由外部测试进程 TerminateProcess/
  SIGTERM 真正杀掉本进程。标记目录与启用的停车点均通过命令行显式传入，
  正常运行不传任何故障参数，行为完全不变。
    A = 事件已写入、接收游标尚未提交（storage.apply_page 提交前）
    B = 余额已写入、应用事务尚未提交（storage.apply_next 提交前）
    C = 应用事务已提交、下一轮确认处理尚未开始（提交返回后的窗口）
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

from . import protocol as p
from . import storage

log = logging.getLogger("cdc.collector")

MAX_RETRIES = 20
RETRY_BASE_SECONDS = 0.2


class FatalCollectorError(RuntimeError):
    """必须停止的采集器致命错误（冲突、协议错误、完整性失败）。"""


@dataclass
class FaultHooks:
    """停车点配置：每个点只触发一次，触发后停车等待外部进程终止。"""
    marker_dir: str
    enabled: set[str] = field(default_factory=set)
    fired: set[str] = field(default_factory=set)
    _park: threading.Event | None = None

    @classmethod
    def parse(cls, spec: str | None, marker_dir: str | None) -> "FaultHooks | None":
        if not spec:
            return None
        if not marker_dir:
            raise FatalCollectorError("启用故障注入必须同时提供 --marker-dir")
        points = {s.strip().upper() for s in spec.split(",") if s.strip()}
        unknown = points - {"A", "B", "C"}
        if unknown:
            raise FatalCollectorError("未知故障点: %s" % ",".join(sorted(unknown)))
        return cls(marker_dir=marker_dir, enabled=points)

    def hook(self, point: str) -> None:
        point = point.upper()
        if point not in self.enabled or point in self.fired:
            return
        self.fired.add(point)
        os.makedirs(self.marker_dir, exist_ok=True)
        marker = os.path.join(self.marker_dir, "parked_%s.json" % point)
        with open(marker, "w", encoding="utf-8") as fh:
            json.dump({"point": point, "pid": os.getpid(),
                       "time": time.strftime("%Y-%m-%dT%H:%M:%S")},
                      fh, ensure_ascii=False)
        log.warning("故障停车点 %s 已到达，pid=%s，等待外部终止…", point, os.getpid())
        # 真正阻塞：业务循环不会继续，靠外部杀进程来模拟崩溃。
        # 不使用可被业务代码吞掉的异常；Event.wait 对 SIGTERM/terminate 无捕获。
        self._park = threading.Event()
        while True:
            self._park.wait(timeout=3600)
            # 没有“放行”路径；唤醒只可能来自解释器关闭，继续等待直至被杀
            if not threading.main_thread().is_alive():
                return


def _http_post_json(url: str, payload: dict, timeout: float) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/json; charset=utf-8"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
    return json.loads(body.decode("utf-8"))


def fetch_page(url: str, cursor: str | None, timeout: float = 30.0) -> dict:
    """带退避重试地抓一页。同游标重试天然幂等（服务端契约）。"""
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            page = _http_post_json(url, {"cursor": cursor}, timeout)
            if not isinstance(page, dict) or "events" not in page:
                raise FatalCollectorError("服务端页面格式非法: %r" % page)
            return page
        except (urllib.error.URLError, TimeoutError, OSError,
                json.JSONDecodeError) as exc:
            last_exc = exc
            delay = RETRY_BASE_SECONDS * (2 ** min(attempt, 6))
            log.warning("抓页失败(cursor=%s) 第%s次：%s；%.1fs 后重试",
                        cursor, attempt + 1, exc, delay)
            time.sleep(delay)
    raise FatalCollectorError("抓页持续失败，放弃：%s" % last_exc)


class Collector:
    def __init__(self, db_path: str, source_url: str,
                 fault_hooks: FaultHooks | None = None,
                 max_pages: int | None = None):
        self.store = storage.Store(db_path)
        self.source_url = source_url
        self.fault_hooks = fault_hooks
        self.max_pages = max_pages  # 测试用安全阀，正常运行为 None
        self.pages_processed = 0
        self.events_new = 0
        self.events_duplicate = 0
        self.txns_applied = 0

    def close(self) -> None:
        self.store.close()

    def _hook(self, point: str) -> None:
        if self.fault_hooks is not None:
            self.fault_hooks.hook(point)

    def run(self) -> dict:
        try:
            cursor = self.store.recv_cursor  # 重启：从已确认接收位置继续
            end_received = self.store.recv_done
            log.info("启动：recv_cursor=%s applied_watermark=%s recv_done=%s",
                     cursor, self.store.applied_watermark, end_received)

            while not end_received:
                page = fetch_page(self.source_url, cursor)
                wire_events = page["events"]
                next_cursor = page.get("next_cursor")
                is_end = bool(page.get("is_end"))
                parsed = [p.parse_event(e) for e in wire_events]
                page_has_end = any(e["etype"] == p.END for e in parsed)
                if is_end != page_has_end:
                    raise FatalCollectorError(
                        "页面 is_end 标记与 END 事件不一致")
                if page_has_end and next_cursor is not None:
                    raise FatalCollectorError("END 页不应再带 next_cursor")

                stats = self.store.apply_page(
                    parsed, next_cursor, end_received=page_has_end,
                    before_commit_hook=self._recv_hook)
                self.pages_processed += 1
                self.events_new += stats["new_events"]
                self.events_duplicate += stats["duplicate_events"]
                if page_has_end:
                    end_received = True
                cursor = next_cursor
                log.info("接收第%s页：新事件%s 重复%s 下一游标=%s END=%s",
                         self.pages_processed, stats["new_events"],
                         stats["duplicate_events"], next_cursor, page_has_end)

                self._drain_apply()

                if self.max_pages is not None and self.pages_processed >= self.max_pages:
                    log.info("达到 max_pages=%s 安全阀，暂停（未 END）", self.max_pages)
                    break

            if end_received:
                self._drain_apply()
                # 故障点 C：应用事务已提交、客户端尚未做确认处理。
                # 放在最终校验前：此时 kill 重启，水位挡住重复生效。
                self._hook("C")
                self.store.final_check_or_raise()
                log.info("采集完成并通过 END 完整性校验")

            return {
                "pages_processed": self.pages_processed,
                "events_new": self.events_new,
                "events_duplicate": self.events_duplicate,
                "txns_applied": self.txns_applied,
                "applied_watermark": self.store.applied_watermark,
                "recv_done": self.store.recv_done,
                "status": "completed" if self.store.recv_done else "paused",
            }
        finally:
            self.close()

    def _recv_hook(self, _phase: str) -> None:
        # storage 在接收事务提交前回调 -> 故障点 A
        self._hook("A")

    def _apply_hook(self, _phase: str) -> None:
        # storage 在应用事务提交前回调 -> 故障点 B
        self._hook("B")

    def _drain_apply(self) -> None:
        """尽可能推进连续应用水位；缺事务时立即返回，继续回去拉页。"""
        while True:
            seq = self.store.next_applicable_seq()
            if seq is None:
                return
            out = self.store.apply_next(before_commit_hook=self._apply_hook)
            if out is None:
                return
            self.txns_applied += 1
            if self.txns_applied % 500 == 0 or out["commit_seq"] <= 5:
                log.info("应用事务 seq=%s txid=%s 变更=%s",
                         out["commit_seq"], out["txid"], out)
