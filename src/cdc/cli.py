# -*- coding: utf-8 -*-
"""命令行入口：run / status / snapshot / source / oracle / gen-scenario。"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from . import __version__, collector, oracle, protocol, source_gen, source_server, storage


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S")


def _load_params(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        obj = json.load(fh)
    return obj["params"]


def cmd_gen_scenario(args: argparse.Namespace) -> int:
    scenario = source_gen.generate(
        seed=args.seed, txn_count=args.txns, accounts=args.accounts,
        page_size=args.page_size, duplicate_rate=args.duplicate_rate,
        poison_offset=args.poison_offset)
    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    source_gen.write_scenario_file(args.out, scenario)
    print(json.dumps({"scenario_file": os.path.abspath(args.out),
                      "params": scenario["params"],
                      "stats": scenario["stats"]},
                     ensure_ascii=False, indent=2))
    return 0


def cmd_source(args: argparse.Namespace) -> int:
    params = _load_params(args.scenario)
    httpd, source = source_server.make_server(args.host, args.port, params)
    actual_host, actual_port = httpd.server_address[:2]
    if args.port_file:
        with open(args.port_file, "w", encoding="utf-8") as fh:
            json.dump({"host": actual_host, "port": actual_port,
                       "pid": os.getpid()}, fh)
    print(json.dumps({"event": "source_started", "host": actual_host,
                      "port": actual_port, "pid": os.getpid(),
                      "stats": source.stats}, ensure_ascii=False), flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    hooks = collector.FaultHooks.parse(args.fault, args.marker_dir)
    c = collector.Collector(
        db_path=args.db, source_url=args.url, fault_hooks=hooks,
        max_pages=args.max_pages)
    try:
        result = c.run()
    except (protocol.ConflictError, protocol.ProtocolError) as exc:
        logging.getLogger("cdc").error("致命错误，采集停止：%s", exc)
        print(json.dumps({"status": "fatal", "error": str(exc)},
                         ensure_ascii=False), flush=True)
        return 2
    except RuntimeError as exc:  # 完整性失败等
        logging.getLogger("cdc").error("完整性失败，采集停止：%s", exc)
        print(json.dumps({"status": "fatal", "error": str(exc)},
                         ensure_ascii=False), flush=True)
        return 3
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    return 0 if result["status"] == "completed" else 10


def cmd_status(args: argparse.Namespace) -> int:
    with storage.Store(args.db) as st:
        print(st.dump_meta_json())
    return 0


def cmd_snapshot(args: argparse.Namespace) -> int:
    with storage.Store(args.db) as st:
        snap = st.snapshot()
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".",
                    exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(snap, fh, ensure_ascii=False, indent=2, sort_keys=True)
        print(json.dumps({k: v for k, v in snap.items() if k != "accounts"},
                         ensure_ascii=False, indent=2))
        print("已导出 -> %s（账户 %s 个）" % (os.path.abspath(args.out),
                                            snap["account_count"]))
    else:
        print(json.dumps(snap, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def cmd_oracle(args: argparse.Namespace) -> int:
    params = _load_params(args.scenario)
    if args.checkpoints:
        seqs = sorted({int(s) for s in args.checkpoints.split(",") if s.strip()})
    else:
        scenario = source_gen.regenerate(params)
        total = sum(1 for c in scenario["canonical_txns"].values()
                    if c["state"] == "COMMITTED")
        seqs = oracle.default_checkpoint_seqs(total)
    summary = oracle.build_checkpoints(params, seqs, args.out_dir)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cdc",
        description="本地 CDC 采集归并器：乱序/重放变更流 -> SQLite 余额镜像")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    g = sub.add_parser("gen-scenario", help="生成确定性场景参数文件")
    g.add_argument("--out", required=True)
    g.add_argument("--seed", type=int, default=20261116)
    g.add_argument("--txns", type=int, default=20000)
    g.add_argument("--accounts", type=int, default=64)
    g.add_argument("--page-size", type=int, default=source_gen.DEFAULT_PAGE_SIZE)
    g.add_argument("--duplicate-rate", type=float, default=source_gen.DUPLICATE_RATE)
    g.add_argument("--poison-offset", type=int, default=None,
                   help="冲突注入：改写该偏移处 PUT 事件的余额（测试用）")
    g.set_defaults(func=cmd_gen_scenario)

    s = sub.add_parser("source", help="启动本地 HTTP 模拟源")
    s.add_argument("--scenario", required=True)
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=0, help="0 表示由系统分配端口")
    s.add_argument("--port-file", default=None)
    s.set_defaults(func=cmd_source)

    r = sub.add_parser("run", help="运行采集归并（可重复执行，断点续跑）")
    r.add_argument("--db", required=True)
    r.add_argument("--url", required=True, help="如 http://127.0.0.1:8000/events")
    r.add_argument("--fault", default=None, help="故障停车点，如 A / B,C（测试用）")
    r.add_argument("--marker-dir", default=None)
    r.add_argument("--max-pages", type=int, default=None,
                   help="安全阀：最多处理多少页（测试用）")
    r.set_defaults(func=cmd_run)

    st = sub.add_parser("status", help="查看双水位与库内计数")
    st.add_argument("--db", required=True)
    st.set_defaults(func=cmd_status)

    sn = sub.add_parser("snapshot", help="单读事务导出水位与全量账户状态")
    sn.add_argument("--db", required=True)
    sn.add_argument("--out", default=None)
    sn.set_defaults(func=cmd_snapshot)

    o = sub.add_parser("oracle", help="独立流式标准答案（写入隔离目录）")
    o.add_argument("--scenario", required=True)
    o.add_argument("--out-dir", required=True)
    o.add_argument("--checkpoints", default=None,
                   help="逗号分隔的 commit_seq 水位；默认 0/25/50/75/100%%")
    o.set_defaults(func=cmd_oracle)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(getattr(args, "verbose", False))
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
