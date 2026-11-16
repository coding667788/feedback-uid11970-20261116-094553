# -*- coding: utf-8 -*-
"""本地 HTTP 模拟源（仅标准库 http.server，无第三方依赖）。

GET  /health                       -> {"ok": true, ...}
POST /events  body: {"cursor": null|"cur_..."}
     同一游标（页号）永远返回相同页面：内容与 next_cursor 都是确定性的。
     已确认页面可被重放：客户端回退到旧游标请求，服务端照常返回该页。

页号对客户端完全不透明（base64 令牌，见 protocol.encode_cursor）。
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import protocol as p
from . import source_gen


class EventSource:
    """持有一个确定性场景，按页号提供不可变页面。"""

    def __init__(self, params: dict):
        scenario = source_gen.regenerate(params)
        self.params = scenario["params"]
        self.stats = scenario["stats"]
        self.pages: list[list[tuple]] = scenario["pages"]
        self._lock = threading.Lock()
        self.request_count = 0

    def page_count(self) -> int:
        return len(self.pages)

    def serve(self, cursor_token: str | None) -> dict:
        with self._lock:
            self.request_count += 1
        if cursor_token is None:
            idx = 0
        else:
            idx = p.decode_cursor(cursor_token)  # 非法令牌 -> ProtocolError -> 400

        if idx >= len(self.pages):
            # 越过最后一页继续请求：返回 END 之后的稳定空页，便于客户端容错
            return {
                "events": [],
                "next_cursor": None,
                "is_end": True,
                "page_index_hint_for_tests": idx,
            }

        events = [p.event_to_wire(ev) for ev in self.pages[idx]]
        is_last = idx == len(self.pages) - 1
        is_end = is_last and any(ev[0] == p.END for ev in self.pages[idx])
        next_cursor = None if is_last else p.encode_cursor(idx + 1)
        return {
            "events": events,
            "next_cursor": next_cursor,
            "is_end": is_end,
            "page_index_hint_for_tests": idx,
        }


def make_server(host: str, port: int, params: dict) -> tuple[ThreadingHTTPServer, EventSource]:
    source = EventSource(params)

    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt: str, *args: object) -> None:
            # 测试期保持安静；调试时可设环境变量 CDC_HTTP_LOG=1
            import os
            if os.environ.get("CDC_HTTP_LOG"):
                super().log_message(fmt, *args)

        def _send_json(self, code: int, payload: dict) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 (stdlib 命名)
            if self.path.split("?")[0] == "/health":
                self._send_json(200, {"ok": True, "stats": source.stats,
                                      "pages": source.page_count()})
            else:
                self._send_json(404, {"error": "not found", "path": self.path})

        def do_POST(self) -> None:  # noqa: N802
            if self.path.split("?")[0] != "/events":
                self._send_json(404, {"error": "not found", "path": self.path})
                return
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            try:
                req = json.loads(raw.decode("utf-8")) if raw else {}
                if not isinstance(req, dict):
                    raise ValueError("请求体必须是 JSON 对象")
                token = req.get("cursor")
                if token is not None and not isinstance(token, str):
                    raise p.ProtocolError("cursor 必须为字符串或 null")
                payload = source.serve(token)
            except (p.ProtocolError, ValueError, UnicodeDecodeError) as exc:
                self._send_json(400, {"error": "bad request", "detail": str(exc)})
                return
            self._send_json(200, payload)

    httpd = ThreadingHTTPServer((host, port), _Handler)
    return httpd, source


def serve_forever(host: str, port: int, params: dict) -> None:
    httpd, _source = make_server(host, port, params)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
