@echo off
REM 一键运行全部测试（单元 + 端到端故障注入）。
REM 端到端规模可用环境变量 CDC_E2E_TXNS 调整，默认 20000（任务要求规模）。
setlocal
cd /d "%~dp0"
if "%CDC_E2E_TXNS%"=="" set CDC_E2E_TXNS=20000
echo [1/2] 单元测试（存储/协议/生成器/HTTP 分页）
python -I tests\test_storage.py || goto :fail
python -I tests\test_generator.py || goto :fail
echo [2/2] 端到端测试（真实子进程 + 外部进程终止式故障注入，%CDC_E2E_TXNS% 笔事务）
python -I tests\test_e2e.py || goto :fail
echo.
echo ALL TESTS PASSED
exit /b 0
:fail
echo.
echo TESTS FAILED
exit /b 1
