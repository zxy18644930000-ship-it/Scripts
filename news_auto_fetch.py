#!/usr/bin/env python3
"""
自动新闻抓取 - 从新浪财经+东方财富获取最新资讯
被 price_sum_workbench.py 的资讯按钮调用（缓存超过3小时时触发）

输出格式与 /news 技能一致，写入 ~/Scripts/news_cache.md
"""

import json
import subprocess
import os
import re
import time
from datetime import datetime

NEWS_CACHE = os.path.expanduser('~/Scripts/news_cache.md')


def _curl_json(url, timeout=10):
    """通过 curl 获取 JSON（绕过 Python SSL 限制）"""
    try:
        result = subprocess.run(
            ['curl', '-sL', '--max-time', str(timeout), url],
            capture_output=True, text=True, timeout=timeout + 5)
        if result.stdout:
            return json.loads(result.stdout)
    except Exception:
        pass
    return None


def fetch_sina_finance():
    """新浪财经滚动新闻"""
    items = []
    data = _curl_json('https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid=2516&k=&num=20&page=1')
    if not data or 'result' not in data:
        return items
    for item in data['result'].get('data', [])[:20]:
        title = item.get('title', '').strip()
        url = item.get('url', '')
        ctime = item.get('ctime', '')
        if title:
            try:
                t = datetime.fromtimestamp(int(ctime)).strftime('%H:%M')
            except Exception:
                t = ''
            items.append({'title': title, 'url': url, 'time': t, 'source': '新浪'})
    return items


def fetch_eastmoney_express():
    """东方财富7x24快讯"""
    items = []
    data = _curl_json(
        'https://np-listapi.eastmoney.com/comm/web/getFastNewsList'
        '?client=web&biz=web_news_col&fastColumn=102&sortEnd=&pageSize=20&req_trace=1')
    if not data:
        return items
    for item in data.get('data', {}).get('fastNewsList', []):
        title = item.get('title', '').strip()
        digest = item.get('digest', '').strip()
        show_time = item.get('showTime', '')
        if title:
            t = show_time[11:16] if len(show_time) > 16 else ''
            text = title
            if digest and digest != title:
                text = f'{title} — {digest[:80]}'
            items.append({'title': text, 'url': '', 'time': t, 'source': '东财'})
    return items


def fetch_eastmoney_commodity():
    """东方财富期货资讯"""
    items = []
    data = _curl_json(
        'https://np-listapi.eastmoney.com/comm/web/getFastNewsList'
        '?client=web&biz=web_news_col&fastColumn=223&sortEnd=&pageSize=15&req_trace=1')
    if not data or not isinstance(data, dict):
        return items
    inner = data.get('data')
    if not inner or not isinstance(inner, dict):
        return items
    for item in inner.get('fastNewsList', []):
        title = item.get('title', '').strip()
        digest = item.get('digest', '').strip()
        show_time = item.get('showTime', '')
        if title:
            t = show_time[11:16] if len(show_time) > 16 else ''
            text = title
            if digest and digest != title:
                text = f'{title} — {digest[:80]}'
            items.append({'title': text, 'url': '', 'time': t, 'source': '东财期货'})
    return items


# 分类关键词
_CATEGORIES = {
    '大宗商品': ['原油', '黄金', '白银', '铜', '铝', '期货', '螺纹', '棕榈', '棉花',
                 '大豆', '铁矿', '焦炭', '天然气', 'oil', 'gold', 'silver', 'commodity',
                 'OPEC', '有色', '黑色', '能化', '农产品', '贵金属'],
    '宏观经济': ['央行', 'GDP', 'CPI', 'PMI', '利率', '降息', '加息', '通胀', '就业',
                 '非农', '美联储', 'Fed', '欧央行', '人民银行', '汇率', '美元',
                 '贸易', '关税', '财政', '国债', '降准'],
    '地缘政治': ['战争', '冲突', '制裁', '军事', '导弹', '海峡', '伊朗', '俄罗斯',
                 '乌克兰', '台海', '朝鲜', '中东'],
    '股市': ['A股', '美股', '港股', '涨停', '跌停', '大盘', '指数', '纳指', '标普',
             '道指', 'IPO', '上证', '深成', '创业板'],
}


def _classify(title):
    """根据标题关键词分类"""
    for cat, keywords in _CATEGORIES.items():
        for kw in keywords:
            if kw in title:
                return cat
    return '其他'


def generate_news():
    """抓取并生成新闻缓存"""
    all_items = []
    all_items.extend(fetch_sina_finance())
    all_items.extend(fetch_eastmoney_express())
    all_items.extend(fetch_eastmoney_commodity())

    if not all_items:
        return False

    # 去重（标题前20字符相同视为重复）
    seen = set()
    unique = []
    for item in all_items:
        key = item['title'][:20]
        if key not in seen:
            seen.add(key)
            unique.append(item)

    # 分类
    categorized = {}
    for item in unique:
        cat = _classify(item['title'])
        categorized.setdefault(cat, []).append(item)

    # 生成 markdown
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    lines = [f'<!-- updated: {now} -->']

    # 按优先级排序类别
    order = ['大宗商品', '宏观经济', '地缘政治', '股市', '其他']
    for cat in order:
        items = categorized.get(cat, [])
        if not items:
            continue
        lines.append(f'\n## {cat}\n')
        for i, item in enumerate(items[:8], 1):
            t = f'[{item["time"]}] ' if item['time'] else ''
            src = f'({item["source"]})' if item['source'] else ''
            lines.append(f'{i}. **{t}{item["title"]}** {src}')

    content = '\n'.join(lines) + '\n'

    with open(NEWS_CACHE, 'w', encoding='utf-8') as f:
        f.write(content)

    return True


if __name__ == '__main__':
    print('正在抓取新闻...')
    ok = generate_news()
    if ok:
        print(f'已更新 {NEWS_CACHE}')
    else:
        print('抓取失败')
