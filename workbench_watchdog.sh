#!/bin/bash
# 交易系统守护脚本
# 每2分钟由launchd调用，检测工作台+CTP采集器健康并自动重启

LOG="/tmp/workbench_watchdog.log"
PYTHON="/usr/bin/python3"

log_msg() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1" >> "$LOG"
}

# ============================================================
# 一、价格之和工作台 (port 8052)
# ============================================================
WORKBENCH="/Users/zhangxiaoyu/Scripts/price_sum_workbench.py"
WB_LOG="/tmp/price_sum_workbench.log"
PORT=8052

restart_wb() {
    log_msg "[工作台-RESTART] 原因: $1"
    pkill -f "price_sum_workbench.py" 2>/dev/null
    sleep 3
    cd /Users/zhangxiaoyu/Scripts
    echo "=== $(date '+%Y-%m-%d %H:%M:%S') 守护脚本启动工作台 ===" >> "$WB_LOG"
    nohup $PYTHON "$WORKBENCH" >> "$WB_LOG" 2>&1 &
    log_msg "[工作台-OK] 新PID: $!"
}

WB_PID=$(pgrep -f "price_sum_workbench.py")
if [ -z "$WB_PID" ]; then
    restart_wb "进程不存在"
else
    # 检查RSS（僵尸检测）
    RSS=$(ps -p "$WB_PID" -o rss= 2>/dev/null | tr -d ' ')
    if [ -n "$RSS" ] && [ "$RSS" -lt 5000 ]; then
        restart_wb "RSS异常低(${RSS}KB)"
    else
        # 检查HTTP响应
        HTTP=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "http://localhost:${PORT}/" 2>/dev/null)
        if [ "$HTTP" != "200" ]; then
            restart_wb "HTTP无响应(code=$HTTP)"
        fi
    fi
fi

# ============================================================
# 二、CTP数据采集器
# ============================================================
COLLECTOR="/Users/zhangxiaoyu/Scripts/ctp_data_collector.py"

restart_ctp() {
    log_msg "[CTP-RESTART] 原因: $1"
    pkill -f "ctp_data_collector.py" 2>/dev/null
    sleep 3
    cd /Users/zhangxiaoyu/Scripts
    echo "=== $(date '+%Y-%m-%d %H:%M:%S') 守护脚本启动CTP采集器 ===" >> /tmp/ctp_collector.log
    nohup $PYTHON "$COLLECTOR" --all-options >> /tmp/ctp_collector.log 2>&1 &
    log_msg "[CTP-OK] 新PID: $!"
}

# 判断是否在交易时段（与ctp_data_collector.py逻辑一致）
is_trading_time() {
    local weekday=$(date +%u)  # 1=周一 ... 7=周日
    local hour=$(date +%H | sed 's/^0//')
    local min=$(date +%M | sed 's/^0//')
    local t=$((hour * 60 + min))

    # 周日全天不交易
    [ "$weekday" -eq 7 ] && return 1

    # 周六只有凌晨段(周五夜盘延续)
    if [ "$weekday" -eq 6 ]; then
        [ "$t" -le 155 ] && return 0  # 02:35 (2:30+5min buffer)
        return 1
    fi

    # 周一凌晨不交易
    if [ "$weekday" -eq 1 ] && [ "$t" -lt 180 ]; then
        return 1
    fi

    # 交易时段（含15min提前+5min延后）
    # 夜盘前半: 20:45 - 23:59
    [ "$t" -ge 1245 ] && [ "$t" -le 1439 ] && return 0
    # 夜盘后半: 00:00 - 02:35
    [ "$t" -le 155 ] && return 0
    # 早盘: 08:45 - 11:35
    [ "$t" -ge 525 ] && [ "$t" -le 695 ] && return 0
    # 午盘: 13:15 - 15:05
    [ "$t" -ge 795 ] && [ "$t" -le 905 ] && return 0

    return 1
}

# 只在交易时段内守护CTP采集器
if is_trading_time; then
    CTP_PID=$(pgrep -f "ctp_data_collector.py")
    if [ -z "$CTP_PID" ]; then
        restart_ctp "交易时段内进程不存在"
    else
        # 检查数据库是否有最近5分钟的数据（采集器活着但不写数据=卡死）
        FRESH=$($PYTHON -c "
import sqlite3, os
from datetime import datetime, timedelta
conn = sqlite3.connect(os.path.expanduser('~/.vntrader/database.db'))
c = conn.cursor()
c.execute('SELECT MAX(datetime) FROM dbbardata')
r = c.fetchone()[0]
conn.close()
if r:
    latest = datetime.strptime(r, '%Y-%m-%d %H:%M:%S')
    if (datetime.now() - latest).total_seconds() > 300:
        print('STALE')
    else:
        print('OK')
else:
    print('STALE')
" 2>/dev/null)
        if [ "$FRESH" = "STALE" ]; then
            restart_ctp "数据超过5分钟未更新"
        fi
    fi
fi
