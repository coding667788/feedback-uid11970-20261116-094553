# -*- coding: utf-8 -*-
"""本地 CDC 采集归并器（纯标准库，Python 3.12，Windows 原生）。

模块职责：
- protocol:     事件线格式与不透明游标编解码
- source_gen:   确定性乱序/重放场景生成（仅模拟源与独立答案使用）
- storage:      SQLite 存储：接收水位与应用水位分离，各自原子提交
- source_server: 本地 HTTP 模拟源
- collector:    采集归并主循环与崩溃注入停车点
- oracle:       独立流式标准答案生成
- cli:          命令行入口
"""

__version__ = "1.0.0"
