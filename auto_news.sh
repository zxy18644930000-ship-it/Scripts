#!/bin/bash
# 每小时自动运行新闻抓取脚本，更新 news_cache.md
# 由 cron/launchd 调度，使用 Python 直接抓取（不依赖 claude CLI）

export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:$PATH"

LOG_FILE="$HOME/Scripts/auto_news.log"

echo "$(date '+%Y-%m-%d %H:%M:%S') - 开始抓取新闻" >> "$LOG_FILE"

/usr/bin/python3 "$HOME/Scripts/news_auto_fetch.py" >> "$LOG_FILE" 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') - 抓取完毕" >> "$LOG_FILE"
echo "---" >> "$LOG_FILE"
