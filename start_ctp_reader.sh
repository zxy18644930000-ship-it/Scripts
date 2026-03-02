#!/bin/bash
# ============================================================================
# CTP数据收集程序 - 守护进程看门狗
# 由cron定时触发，确保daemon进程持续运行
#
# 工作模式:
#   1. 检查 ctp_data_collector.py --daemon 是否在运行
#   2. 如果在运行 → 记录健康状态，退出
#   3. 如果不在运行 → 自动拉起daemon
#   4. 附带: 交易日历判断、磁盘空间监控、日志清理
#
# Cron触发时间 (即健康检查时间):
#   20:55 (夜盘前) | 08:55 (早盘前) | 13:25 (午盘前)
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
PYTHON="/usr/bin/python3"
CTP_COLLECTOR="$SCRIPT_DIR/ctp_data_collector.py"

# Daemon模式参数
DAEMON_ARGS="--all-options --daemon"
# PID文件（用于快速检测daemon状态）
PID_FILE="$SCRIPT_DIR/.ctp_daemon.pid"

# 日历配置文件
HOLIDAYS_FILE="$SCRIPT_DIR/trading_holidays_2026.conf"
NO_NIGHT_FILE="$SCRIPT_DIR/no_night_session.conf"

# 磁盘监控阈值
DISK_WARN_GB=20          # 剩余空间低于20GB时警告
DISK_CRITICAL_GB=10      # 剩余空间低于10GB时紧急警告
DB_WARN_MB=500           # 数据库超过500MB时提醒迁移

# vnpy数据库路径
VNPY_DB="$HOME/.vntrader/database.db"

# 外置硬盘备份目标（自动检测 /Volumes 下的外置盘）
EXTERNAL_BACKUP_DIR=""

# ============================================================================
# 工具函数
# ============================================================================

log() {
    echo "[$(date '+%H:%M:%S')] $1" >> "$LOG_FILE"
}

notify() {
    # macOS 系统通知
    local title="$1"
    local message="$2"
    osascript -e "display notification \"$message\" with title \"$title\" sound name \"Glass\"" 2>/dev/null
}

alert_dialog() {
    # macOS 弹窗警告（紧急情况）
    local title="$1"
    local message="$2"
    osascript -e "display dialog \"$message\" with title \"$title\" buttons {\"好的\"} default button 1 with icon caution" 2>/dev/null &
}

# 从配置文件读取日期列表（去掉注释和空行）
load_dates() {
    local file="$1"
    if [ -f "$file" ]; then
        grep -v '^#' "$file" | grep -v '^$' | awk '{print $1}'
    fi
}

# 检查日期是否在列表中
date_in_list() {
    local check_date="$1"
    local file="$2"
    load_dates "$file" | grep -q "^${check_date}$"
    return $?
}

# 获取下一个自然日（用于夜盘判断）
next_weekday() {
    local today="$1"
    local dow=$(date -j -f "%Y-%m-%d" "$today" "+%u" 2>/dev/null)
    if [ "$dow" = "5" ]; then
        # 周五 → 下周一
        date -j -v+3d -f "%Y-%m-%d" "$today" "+%Y-%m-%d" 2>/dev/null
    else
        # 其他 → 明天
        date -j -v+1d -f "%Y-%m-%d" "$today" "+%Y-%m-%d" 2>/dev/null
    fi
}

# 检查某天是否为交易日（非周末且非节假日）
is_trading_day() {
    local check_date="$1"
    local dow=$(date -j -f "%Y-%m-%d" "$check_date" "+%u" 2>/dev/null)
    # 周末不是交易日
    if [ "$dow" = "6" ] || [ "$dow" = "7" ]; then
        return 1
    fi
    # 节假日不是交易日
    if date_in_list "$check_date" "$HOLIDAYS_FILE"; then
        return 1
    fi
    return 0
}

# ============================================================================
# 交易日历判断
# ============================================================================

should_run() {
    local today=$(date "+%Y-%m-%d")

    if [ "$SESSION" = "night" ]; then
        # 夜盘逻辑：
        # 1. 检查今天是否在"无夜盘"名单中
        if date_in_list "$today" "$NO_NIGHT_FILE"; then
            log "跳过: $today 节前无夜盘"
            return 1
        fi
        # 2. 检查下一个工作日是否是交易日
        local next_day=$(next_weekday "$today")
        if ! is_trading_day "$next_day"; then
            log "跳过: 下一交易日 $next_day 休市，今晚无夜盘"
            return 1
        fi
        return 0
    else
        # 早盘/午盘逻辑：今天必须是交易日
        if ! is_trading_day "$today"; then
            log "跳过: $today 非交易日"
            return 1
        fi
        return 0
    fi
}

# ============================================================================
# 磁盘空间监控
# ============================================================================

check_disk_space() {
    # 获取主磁盘剩余空间 (GB)
    local avail_kb=$(df -k "$HOME" | tail -1 | awk '{print $4}')
    local avail_gb=$((avail_kb / 1024 / 1024))

    # 获取数据库文件大小 (MB)
    local db_size_mb=0
    if [ -f "$VNPY_DB" ]; then
        local db_size_bytes=$(stat -f%z "$VNPY_DB" 2>/dev/null || echo 0)
        db_size_mb=$((db_size_bytes / 1024 / 1024))
    fi

    log "磁盘剩余: ${avail_gb}GB | 数据库: ${db_size_mb}MB"

    # 检测外置硬盘
    local external_disk=""
    for vol in /Volumes/*/; do
        # 跳过系统盘
        local vol_name=$(basename "$vol")
        if [ "$vol_name" != "Macintosh HD" ] && [ "$vol_name" != "Macintosh HD - Data" ]; then
            external_disk="$vol"
            break
        fi
    done

    # 紧急警告: 磁盘空间严重不足
    if [ $avail_gb -lt $DISK_CRITICAL_GB ]; then
        local msg="磁盘剩余仅 ${avail_gb}GB！数据库已占用 ${db_size_mb}MB。"
        if [ -n "$external_disk" ]; then
            msg="${msg}\n\n检测到外置硬盘: ${external_disk}\n请立即执行迁移:\ncp ${VNPY_DB} ${external_disk}CTP_Backup/"
        else
            msg="${msg}\n\n请插入移动硬盘并迁移数据库文件:\n${VNPY_DB}"
        fi
        log "紧急警告: 磁盘仅剩 ${avail_gb}GB"
        alert_dialog "CTP数据 - 磁盘空间紧急" "$msg"
        return
    fi

    # 一般警告: 磁盘空间偏低
    if [ $avail_gb -lt $DISK_WARN_GB ]; then
        local msg="磁盘剩余 ${avail_gb}GB，数据库 ${db_size_mb}MB。建议尽快迁移数据到移动硬盘。"
        log "警告: 磁盘剩余 ${avail_gb}GB"
        notify "CTP数据 - 磁盘空间不足" "$msg"
        return
    fi

    # 数据库过大提醒
    if [ $db_size_mb -gt $DB_WARN_MB ]; then
        local msg="数据库已达 ${db_size_mb}MB，建议迁移旧数据到移动硬盘。磁盘剩余 ${avail_gb}GB。"
        log "提醒: 数据库 ${db_size_mb}MB，建议迁移"
        notify "CTP数据 - 数据库较大" "$msg"
    fi
}

# ============================================================================
# 自动清理旧日志（保留30天）
# ============================================================================

cleanup_old_logs() {
    find "$LOG_DIR" -name "ctp_*.log" -mtime +30 -delete 2>/dev/null
    find "$LOG_DIR" -name "collector_*.log" -mtime +30 -delete 2>/dev/null
}

# ============================================================================
# 守护进程检测
# ============================================================================

is_daemon_running() {
    # 方法1: 检查PID文件
    if [ -f "$PID_FILE" ]; then
        local saved_pid=$(cat "$PID_FILE" 2>/dev/null)
        if [ -n "$saved_pid" ] && kill -0 "$saved_pid" 2>/dev/null; then
            # PID存活，再确认是不是我们的进程
            if ps -p "$saved_pid" -o args= 2>/dev/null | grep -q "ctp_data_collector.*--daemon"; then
                echo "$saved_pid"
                return 0
            fi
        fi
    fi

    # 方法2: PID文件无效，用pgrep兜底
    local pid=$(pgrep -f "ctp_data_collector.py.*--daemon" 2>/dev/null | head -1)
    if [ -n "$pid" ]; then
        # 更新PID文件
        echo "$pid" > "$PID_FILE"
        echo "$pid"
        return 0
    fi

    return 1
}

start_daemon() {
    log "拉起daemon: $PYTHON $CTP_COLLECTOR $DAEMON_ARGS"
    local daemon_log="$LOG_DIR/collector_$(date +%Y%m%d).log"
    # daemon模式下Python自己写日志文件，nohup输出丢弃避免重复
    nohup $PYTHON "$CTP_COLLECTOR" $DAEMON_ARGS > /dev/null 2>&1 &
    local new_pid=$!
    echo "$new_pid" > "$PID_FILE"
    sleep 2

    # 验证是否启动成功
    if kill -0 "$new_pid" 2>/dev/null; then
        log "daemon启动成功 PID=$new_pid 日志=$daemon_log"
        notify "CTP数据采集" "Daemon已启动 PID=$new_pid"
        return 0
    else
        log "daemon启动失败! 检查日志: $daemon_log"
        notify "CTP数据采集 - 异常" "Daemon启动失败，请检查日志"
        return 1
    fi
}

# ============================================================================
# 主流程
# ============================================================================

mkdir -p "$LOG_DIR"

DATE=$(date +%Y%m%d)
SESSION=""

# 判断当前时段（用 10# 强制十进制，避免08/09被当八进制报错）
HOUR=$((10#$(date +%H)))
MINUTE=$((10#$(date +%M)))
CURRENT_TIME=$((HOUR * 60 + MINUTE))

# CURRENT_TIME 是从0:00起的分钟数
# 08:30=510  12:00=720  13:00=780  16:00=960  20:30=1230  03:00=180
if [ $CURRENT_TIME -ge 780 ] && [ $CURRENT_TIME -lt 960 ]; then
    SESSION="afternoon"
elif [ $CURRENT_TIME -ge 510 ] && [ $CURRENT_TIME -lt 720 ]; then
    SESSION="morning"
elif [ $CURRENT_TIME -ge 1230 ] || [ $CURRENT_TIME -lt 180 ]; then
    SESSION="night"
else
    SESSION="manual"
fi

LOG_FILE="$LOG_DIR/ctp_watchdog_${DATE}.log"

log "========================================"
log "看门狗检查 [$SESSION]"
log "========================================"

# 清理旧日志
cleanup_old_logs

# 交易日历判断（手动运行时跳过检查）
if [ "$SESSION" != "manual" ]; then
    if ! should_run; then
        log "今日不运行，退出"
        exit 0
    fi
fi

# 磁盘空间检查
check_disk_space

# 核心逻辑: 检测daemon并按需拉起
DAEMON_PID=$(is_daemon_running)
if [ $? -eq 0 ]; then
    # Daemon正在运行
    log "daemon运行正常 PID=$DAEMON_PID"

    # 检查进程运行时长（信息记录）
    DAEMON_START=$(ps -p "$DAEMON_PID" -o lstart= 2>/dev/null)
    if [ -n "$DAEMON_START" ]; then
        log "  启动时间: $DAEMON_START"
    fi

    # 检查今日日志最近活动
    TODAY_LOG="$LOG_DIR/collector_${DATE}.log"
    if [ -f "$TODAY_LOG" ]; then
        LAST_LINE=$(tail -1 "$TODAY_LOG" 2>/dev/null)
        log "  最近日志: $LAST_LINE"
    fi

    log "无需操作，退出"
    exit 0
else
    # Daemon不在运行，需要拉起
    log "daemon未运行! 准备拉起..."
    notify "CTP数据采集 - 告警" "Daemon进程不存在，正在自动拉起"

    if start_daemon; then
        log "daemon恢复成功"
        exit 0
    else
        log "daemon恢复失败!"
        alert_dialog "CTP数据采集 - 紧急" "Daemon无法启动，请手动检查:\n$PYTHON $CTP_COLLECTOR $DAEMON_ARGS"
        exit 1
    fi
fi
