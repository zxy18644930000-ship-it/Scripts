#!/bin/bash
# 价格之和工作台看门狗 - 由cron触发，确保工作台持续运行
# 工作台是纯展示页面，全天候运行即可

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
PYTHON="/usr/bin/python3"
WORKBENCH="$SCRIPT_DIR/price_sum_workbench.py"
PID_FILE="$SCRIPT_DIR/.workbench.pid"
LOG_FILE="$LOG_DIR/workbench.log"
PORT=8052

mkdir -p "$LOG_DIR"

is_running() {
    # 检查PID文件
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE" 2>/dev/null)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "$pid"
            return 0
        fi
    fi
    # PID文件无效，用端口检测兜底
    local pid=$(lsof -ti :$PORT 2>/dev/null | head -1)
    if [ -n "$pid" ]; then
        echo "$pid" > "$PID_FILE"
        echo "$pid"
        return 0
    fi
    return 1
}

PID=$(is_running)
if [ $? -eq 0 ]; then
    exit 0
fi

# 不在运行，拉起
nohup $PYTHON "$WORKBENCH" >> "$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" > "$PID_FILE"
sleep 2

if kill -0 "$NEW_PID" 2>/dev/null; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 工作台已启动 PID=$NEW_PID" >> "$LOG_FILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 工作台启动失败!" >> "$LOG_FILE"
fi
