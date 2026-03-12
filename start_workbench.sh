#!/bin/bash
# 价格之和工作台看门狗 - 由cron触发，确保工作台持续运行
# 根治重启竞态：用flock保证同一时刻只有一个看门狗在操作

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
PYTHON="/usr/bin/python3"
WORKBENCH="$SCRIPT_DIR/price_sum_workbench.py"
PID_FILE="$SCRIPT_DIR/.workbench.pid"
LOCK_FILE="$SCRIPT_DIR/.workbench.lock"
LOG_FILE="$LOG_DIR/workbench.log"
PORT=8052

mkdir -p "$LOG_DIR"

# flock 防止并发（cron + 手动重启同时触发）
exec 200>"$LOCK_FILE"
flock -n 200 || { echo "[$(date '+%F %T')] 另一个看门狗正在运行，跳过" >> "$LOG_FILE"; exit 0; }

is_running() {
    # 1) PID文件检查
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE" 2>/dev/null)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            # 再确认这个PID确实在监听端口（防止PID复用）
            if lsof -ti :$PORT 2>/dev/null | grep -q "^${pid}$"; then
                echo "$pid"
                return 0
            fi
        fi
    fi
    # 2) 端口兜底
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

# 确保端口彻底释放
lsof -ti :$PORT 2>/dev/null | xargs kill -9 2>/dev/null
sleep 2

# 二次确认端口已释放
if lsof -ti :$PORT 2>/dev/null; then
    echo "[$(date '+%F %T')] 端口$PORT仍被占用，放弃启动" >> "$LOG_FILE"
    exit 1
fi

# 拉起新实例
nohup $PYTHON "$WORKBENCH" >> "$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" > "$PID_FILE"
sleep 3

if kill -0 "$NEW_PID" 2>/dev/null && lsof -ti :$PORT 2>/dev/null | grep -q "^${NEW_PID}$"; then
    echo "[$(date '+%F %T')] 工作台已启动 PID=$NEW_PID" >> "$LOG_FILE"
else
    echo "[$(date '+%F %T')] 工作台启动失败!" >> "$LOG_FILE"
fi
