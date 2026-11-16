"""本地 HTTP 模拟源。

- GET  /v1/pages?cursor=<opaque>  分页拉取；相同游标永远返回相同内容；
- POST /v1/ack   {cursor}        记录客户端已确认游标（源可重放已确认页）；
- GET  /v1/stats                 数据集统计；
- GET  /healthz                  存活探针。

游标是 HMAC 签名的不透明 token，采集器无法也不应解析其内容。
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .generator import Dataset, generate_dataset
from .scenarios import SCENARIOS


def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64d(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


class CursorCodec:
    def __init__(self, secret: bytes) -> None:
        self._secret = secret

    def encode(self, page_index: int) -> str:
        body = json.dumps({"v": 1, "i": page_index}, separators=(",", ":")).encode()
        sig = hmac.new(self._secret, body, hashlib.sha256).digest()
        return _b64e(body) + "." + _b64e(sig)

    def decode(self, token: str) -> int:
        try:
            body_b64, sig_b64 = token.split(".", 1)
            body = _b64d(body_b64)
            sig = _b64d(sig_b64)
        except (ValueError, base64.binascii.Error) as exc:  # type: ignore[attr-defined]
            raise ValueError("非法游标") from exc
        expected = hmac.new(self._secret, body, hashlib.sha256).digest()
        if not hmac.compare_digest(sig, expected):
            raise ValueError("游标签名不匹配")
        data = json.loads(body)
        return int(data["i"])


class SourceState:
    def __init__(self, dataset: Dataset, host: str, port: int) -> None:
        self.dataset = dataset
        self.host = host
        self.port = port
        self.codec = CursorCodec(secret=hashlib.sha256(f"cdc-source-{dataset.seed}".encode()).digest())
        self.ack_lock = threading.Lock()
        self.acked_pages: set[int] = set()
        self.max_ack_index = -1
        self.fetch_counts: dict[int, int] = {}


def make_handler(state: SourceState):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt: str, *args) -> None:  # 静默常规访问日志
            return

        def _send_json(self, status: int, payload: dict) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/healthz":
                self._send_json(200, {"ok": True})
                return
            if parsed.path == "/v1/stats":
                self._send_json(200, state.dataset.stats)
                return
            if parsed.path == "/v1/pages":
                query = parse_qs(parsed.query)
                cursor = query.get("cursor", [""])[0]
                if not cursor:
                    page_index = 0
                else:
                    try:
                        page_index = state.codec.decode(cursor)
                    except ValueError:
                        self._send_json(400, {"error": "invalid cursor"})
                        return
                if page_index < 0 or page_index >= len(state.dataset.pages):
                    self._send_json(404, {"error": "page out of range"})
                    return
                with state.ack_lock:
                    state.fetch_counts[page_index] = state.fetch_counts.get(page_index, 0) + 1
                page = state.dataset.pages[page_index]
                next_cursor = None
                if page_index + 1 < len(state.dataset.pages):
                    next_cursor = state.codec.encode(page_index + 1)
                self._send_json(
                    200,
                    {
                        "page_index": page.index,
                        "events": [event.to_dict() for event in page.events],
                        "next_cursor": next_cursor,
                        "end": page.is_end,
                    },
                )
                return
            self._send_json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/v1/ack":
                self._send_json(404, {"error": "not found"})
                return
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                data = json.loads(raw.decode("utf-8"))
                token = data["cursor"]
                page_index = state.codec.decode(token)
            except (ValueError, KeyError, json.JSONDecodeError):
                self._send_json(400, {"error": "invalid ack"})
                return
            with state.ack_lock:
                state.acked_pages.add(page_index)
                state.max_ack_index = max(state.max_ack_index, page_index)
            self._send_json(200, {"ok": True, "max_ack_index": state.max_ack_index})

    return Handler


def build_state(
    scenario: str | None = None,
    n_tx: int = 21000,
    seed: int = 20261116,
    host: str = "127.0.0.1",
    port: int = 0,
) -> SourceState:
    if scenario:
        if scenario not in SCENARIOS:
            raise SystemExit(f"未知场景 {scenario}，可选: {sorted(SCENARIOS)}")
        dataset = SCENARIOS[scenario]()
    else:
        dataset = generate_dataset(n_tx=n_tx, seed=seed)
    return SourceState(dataset=dataset, host=host, port=port)


def serve(state: SourceState) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((state.host, state.port), make_handler(state))
    state.port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, name="cdc-source-http", daemon=True)
    thread.start()
    return httpd


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="本地 CDC 模拟源 HTTP 服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--n-tx", type=int, default=21000)
    parser.add_argument("--seed", type=int, default=20261116)
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default=None)
    args = parser.parse_args(argv)
    state = build_state(args.scenario, args.n_tx, args.seed, args.host, args.port)
    httpd = serve(state)
    base_url = f"http://{state.host}:{state.port}"
    print(json.dumps({"base_url": base_url, "stats": state.dataset.stats}, ensure_ascii=False), flush=True)
    try:
        thread_event = threading.Event()
        thread_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
