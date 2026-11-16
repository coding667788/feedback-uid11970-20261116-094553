# -*- coding: utf-8 -*-
"""事件线协议与不透明游标。

事件类型（线上 JSON 的 type 字段）：
  BEGIN(txid)
  PUT(txid, op_index, account, balance)
  DELETE(txid, op_index, account)
  COMMIT(txid, commit_seq, op_count)
  ABORT(txid)
  END()

游标对客户端不透明：客户端只做原样透传，绝不解析。
同一游标重复请求必须返回相同页面（见 source_server）。
"""
from __future__ import annotations

import base64
import json

BEGIN = "BEGIN"
PUT = "PUT"
DELETE = "DELETE"
COMMIT = "COMMIT"
ABORT = "ABORT"
END = "END"

TERMINAL_TYPES = (COMMIT, ABORT)
OP_TYPES = (PUT, DELETE)
ALL_TYPES = (BEGIN, PUT, DELETE, COMMIT, ABORT, END)


class ProtocolError(ValueError):
    """事件线本身非法（缺字段、类型错误等），属于必须停止的致命错误。"""


class ConflictError(RuntimeError):
    """同一事件身份出现不同内容：源端矛盾，必须报错停止。"""


def encode_cursor(page_index: int) -> str:
    """页序号 -> 不透明游标令牌（仅服务端编解码）。"""
    raw = json.dumps({"v": 1, "p": int(page_index)}, separators=(",", ":")).encode("utf-8")
    return "cur_" + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(token: str) -> int:
    """不透明游标令牌 -> 页序号；非法令牌抛 ProtocolError。"""
    if not isinstance(token, str) or not token.startswith("cur_"):
        raise ProtocolError("非法游标: %r" % (token,))
    body = token[4:]
    body += "=" * (-len(body) % 4)
    try:
        raw = base64.urlsafe_b64decode(body.encode("ascii"))
        obj = json.loads(raw.decode("utf-8"))
        page_index = obj["p"]
        if not isinstance(page_index, int) or isinstance(page_index, bool) or page_index < 0:
            raise ProtocolError("游标页码非法: %r" % (token,))
        return page_index
    except (ValueError, KeyError, UnicodeDecodeError) as exc:
        raise ProtocolError("非法游标: %r (%s)" % (token, exc)) from exc


def event_to_wire(ev: tuple) -> dict:
    """内部紧凑元组 -> 线上 JSON 对象。

    元组形态见 source_gen，存储层只依赖 wire 字典，不依赖元组顺序。
    """
    etype = ev[0]
    if etype == BEGIN:
        return {"type": BEGIN, "txid": ev[1]}
    if etype == PUT:
        return {"type": PUT, "txid": ev[1], "op_index": ev[2],
                "account": ev[3], "balance": ev[4]}
    if etype == DELETE:
        return {"type": DELETE, "txid": ev[1], "op_index": ev[2],
                "account": ev[3]}
    if etype == COMMIT:
        return {"type": COMMIT, "txid": ev[1], "commit_seq": ev[2],
                "op_count": ev[3]}
    if etype == ABORT:
        return {"type": ABORT, "txid": ev[1]}
    if etype == END:
        return {"type": END}
    raise ProtocolError("未知事件类型: %r" % (etype,))


def _require_int(obj: dict, key: str, *, nonneg=False, positive=False) -> int:
    val = obj.get(key)
    # bool 是 int 的子类，余额/序号绝不接受布尔值
    if not isinstance(val, int) or isinstance(val, bool):
        raise ProtocolError("事件字段 %s 必须为整数: %r" % (key, obj))
    if nonneg and val < 0:
        raise ProtocolError("事件字段 %s 不能为负: %r" % (key, obj))
    if positive and val <= 0:
        raise ProtocolError("事件字段 %s 必须为正: %r" % (key, obj))
    return val


def parse_event(obj: dict) -> dict:
    """校验线上事件并归一化为存储层使用的字典。非法即 ProtocolError。"""
    if not isinstance(obj, dict):
        raise ProtocolError("事件必须是 JSON 对象: %r" % (obj,))
    etype = obj.get("type")
    if etype not in ALL_TYPES:
        raise ProtocolError("未知事件类型: %r" % (obj,))

    if etype == BEGIN:
        return {"etype": BEGIN, "txid": _require_int(obj, "txid", positive=True),
                "op_index": None, "account": None, "balance": None,
                "commit_seq": None, "op_count": None, "end_flag": 0}

    if etype in OP_TYPES:
        txid = _require_int(obj, "txid", positive=True)
        op_index = _require_int(obj, "op_index", nonneg=True)
        account = obj.get("account")
        if not isinstance(account, str) or not account:
            raise ProtocolError("account 必须为非空字符串: %r" % (obj,))
        out = {"etype": etype, "txid": txid, "op_index": op_index,
               "account": account, "balance": None, "commit_seq": None,
               "op_count": None, "end_flag": 0}
        if etype == PUT:
            out["balance"] = _require_int(obj, "balance")  # 允许负数
        return out

    if etype == COMMIT:
        return {"etype": COMMIT, "txid": _require_int(obj, "txid", positive=True),
                "op_index": None, "account": None, "balance": None,
                "commit_seq": _require_int(obj, "commit_seq", positive=True),
                "op_count": _require_int(obj, "op_count", nonneg=True),
                "end_flag": 0}

    if etype == ABORT:
        return {"etype": ABORT, "txid": _require_int(obj, "txid", positive=True),
                "op_index": None, "account": None, "balance": None,
                "commit_seq": None, "op_count": None, "end_flag": 0}

    # END
    return {"etype": END, "txid": None, "op_index": None, "account": None,
            "balance": None, "commit_seq": None, "op_count": None, "end_flag": 1}


def normalize_tuple(ev: dict) -> tuple:
    """事件的身份+内容归一化元组，用于逐字节冲突比对。"""
    return (ev["etype"], ev["txid"], ev["op_index"], ev["account"],
            ev["balance"], ev["commit_seq"], ev["op_count"], ev["end_flag"])
