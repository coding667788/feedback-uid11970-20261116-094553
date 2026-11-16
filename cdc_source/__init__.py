"""本地 CDC 模拟源：确定性乱序/重放事件流 + HTTP 分页服务 + 独立答案。

本包只属于“源端/测试端”，cdc_tool 包禁止导入本包（由 tests 静态校验保证），
因此测试答案对采集器不可达。
"""

from .events import Event, canonical_json, event_identity

__all__ = ["Event", "canonical_json", "event_identity"]
