#!/usr/bin/env python3
"""
Gamma Scalp 每日检查引擎

4个指标 + 综合评分，判断当天应该 Gamma Scalping 还是 Strangle Sell。
指标：重大事件(0-2.5) + IV分位(0-2.5) + BB Squeeze(0-2.5) + ATR低位(0-2.5) = 0-10分

复用 gamma_monitor.py 的 IV/DTE 计算，price_sum_workbench.py 的布林带计算。
"""

import json
import sqlite3
import subprocess
import os
import re
import math
import time
import logging
import threading
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Tuple

DB_PATH = os.path.expanduser('~/.vntrader/database.db')
CALENDAR_PATH = os.path.expanduser('~/Scripts/economic_calendar.json')

logger = logging.getLogger(__name__)

# 复用 gamma_monitor 的函数
from gamma_monitor import (
    implied_volatility, _estimate_dte, _extract_product_code,
    _extract_futures_symbol, _extract_strike, _resolve_symbol,
    MULTIPLIERS, CZCE_PRODUCTS
)


# ============ 数据库连接（线程安全） ============

_thread_local = threading.local()


def _get_db():
    conn = getattr(_thread_local, 'conn', None)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            conn = None
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        _thread_local.conn = conn
    return conn


# ============ 指标1：重大事件 (0-2.5) ============
# 自动从 investing.com 抓取经济日历，6小时缓存

# 货币 → 影响的中国期货品种映射
_CURRENCY_AFFECTS = {
    'USD': ['au', 'ag', 'cu', 'sc', 'zn', 'al', 'ni', 'sn', 'pb'],  # 美元事件 → 贵金属+有色+原油
    'CNY': ['all'],   # 中国事件 → 全品种
    'EUR': ['au', 'ag', 'cu', 'zn', 'al'],  # 欧元区 → 贵金属+有色
    'GBP': ['au', 'ag'],
    'JPY': ['au', 'ag'],
}

# 特定事件关键词 → 额外影响品种
_EVENT_KEYWORD_AFFECTS = {
    'crude oil': ['sc', 'pg', 'ta', 'eb', 'fu', 'bu', 'lu'],
    'natural gas': ['pg'],
    'wheat': ['wh'],
    'corn': ['c'],
    'soybean': ['a', 'b', 'm', 'y'],
    'cotton': ['cf'],
    'sugar': ['sr'],
    'palm oil': ['p'],
    'copper': ['cu'],
    'gold': ['au'],
    'silver': ['ag'],
    'iron ore': ['i'],
    'usda': ['cf', 'sr', 'a', 'b', 'm', 'y', 'p', 'c', 'oi'],
}

_cal_cache = {'events': [], 'ts': None}
_CAL_CACHE_TTL = 6 * 3600  # 6小时


def _fetch_investing_calendar() -> List[dict]:
    """从 investing.com 抓取经济日历（通过 curl 绕过 Python SSL 限制）
    返回标准化的事件列表，每个事件: {date, time_bj, name, level, currency, country, affects}
    """
    try:
        result = subprocess.run([
            'curl', '-sL', '--max-time', '15',
            '-H', 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'https://www.investing.com/economic-calendar/'
        ], capture_output=True, text=True, timeout=20)

        if not result.stdout or len(result.stdout) < 10000:
            return []

        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            result.stdout, re.DOTALL)
        if not m:
            return []

        data = json.loads(m.group(1))
        store = data['props']['pageProps']['state']['economicCalendarStore']
        events_by_date = store.get('calendarEventsByDate', {})

        parsed = []
        today = date.today()
        tomorrow = today + timedelta(days=1)

        for dt_str, raw_events in events_by_date.items():
            try:
                ev_date = date.fromisoformat(dt_str)
            except ValueError:
                continue
            if ev_date not in (today, tomorrow):
                continue

            for ev in raw_events:
                try:
                    imp = int(ev.get('importance', 0))
                except (ValueError, TypeError):
                    continue
                if imp < 2:  # 只要 medium(2) 和 high(3)
                    continue

                name = ev.get('event', '') or ev.get('eventLong', '')
                currency = ev.get('currency', '')
                country = ev.get('country', '')
                time_utc = ev.get('time', '')

                # UTC → 北京时间 (UTC+8)
                time_bj = ''
                if time_utc and 'T' in time_utc:
                    try:
                        dt = datetime.fromisoformat(time_utc.replace('Z', '+00:00'))
                        dt_bj = dt + timedelta(hours=8)
                        time_bj = dt_bj.strftime('%H:%M')
                    except Exception:
                        pass

                # 确定影响级别
                level = 'high' if imp == 3 else 'medium'

                # 确定影响品种
                affects = set()
                # 基于货币
                cur_affects = _CURRENCY_AFFECTS.get(currency, [])
                for a in cur_affects:
                    affects.add(a)
                # 基于事件关键词
                name_lower = name.lower()
                for keyword, products in _EVENT_KEYWORD_AFFECTS.items():
                    if keyword in name_lower:
                        for p in products:
                            affects.add(p)

                if not affects:
                    continue  # 与中国期货无关的事件跳过

                day_label = '今日' if ev_date == today else '明日'
                parsed.append({
                    'date': dt_str,
                    'day_label': day_label,
                    'time_bj': time_bj,
                    'name': name,
                    'level': level,
                    'currency': currency,
                    'country': country,
                    'affects': list(affects),
                })

        return parsed

    except Exception as e:
        logger.warning(f'抓取 investing.com 日历失败: {e}')
        return []


def _load_calendar_fallback():
    """回退：读取手动维护的 JSON 日历"""
    if not os.path.exists(CALENDAR_PATH):
        return {'events': [], 'recurring': []}
    with open(CALENDAR_PATH, 'r') as f:
        return json.load(f)


def _get_calendar_events() -> List[dict]:
    """获取经济事件（6小时缓存，失败回退到 JSON 文件）"""
    now = time.time()
    if _cal_cache['ts'] and now - _cal_cache['ts'] < _CAL_CACHE_TTL:
        return _cal_cache['events']

    events = _fetch_investing_calendar()
    if events:
        _cal_cache['events'] = events
        _cal_cache['ts'] = now
        # 同时更新 JSON 缓存文件
        try:
            _save_calendar_cache(events)
        except Exception:
            pass
        return events

    # 回退到 JSON 文件
    cal = _load_calendar_fallback()
    today = date.today()
    tomorrow = today + timedelta(days=1)
    fallback = []

    for ev in cal.get('events', []):
        try:
            ev_date = date.fromisoformat(ev['date'])
        except (ValueError, KeyError):
            continue
        if ev_date not in (today, tomorrow):
            continue
        day_label = '今日' if ev_date == today else '明日'
        fallback.append({
            'date': ev['date'],
            'day_label': day_label,
            'time_bj': ev.get('time', ''),
            'name': ev.get('name', ''),
            'level': ev.get('level', 'medium'),
            'currency': '',
            'country': '',
            'affects': ev.get('affects', ['all']),
        })

    # 周期事件
    for rec in cal.get('recurring', []):
        for check_date, label in [(today, '今日'), (tomorrow, '明日')]:
            if check_date.weekday() == rec.get('weekday'):
                fallback.append({
                    'date': check_date.isoformat(),
                    'day_label': label,
                    'time_bj': '',
                    'name': rec.get('name', ''),
                    'level': rec.get('level', 'medium'),
                    'currency': '',
                    'country': '',
                    'affects': rec.get('affects', ['all']),
                })

    _cal_cache['events'] = fallback
    _cal_cache['ts'] = now
    return fallback


def _save_calendar_cache(events: List[dict]):
    """将抓取结果写回 JSON 缓存（保留 recurring 和 meta）"""
    cal = _load_calendar_fallback()

    # 转换为 JSON 格式
    new_events = []
    seen = set()
    for ev in events:
        key = (ev['date'], ev['name'])
        if key in seen:
            continue
        seen.add(key)
        new_events.append({
            'date': ev['date'],
            'time': ev['time_bj'],
            'name': ev['name'],
            'level': ev['level'],
            'affects': ev['affects'],
            'source': 'investing.com',
        })

    cal['events'] = new_events
    cal['meta'] = cal.get('meta', {})
    cal['meta']['last_auto_update'] = datetime.now().isoformat()
    cal['meta']['description'] = '经济事件日历 - 自动从 investing.com 抓取，6小时刷新'

    with open(CALENDAR_PATH, 'w') as f:
        json.dump(cal, f, ensure_ascii=False, indent=2)


def check_events(product: str) -> Tuple[float, List[str]]:
    """检查今天/明天是否有影响该品种的经济事件
    Returns: (score, [event_descriptions])
    """
    events = _get_calendar_events()
    prod_lower = product.lower()

    matched = []
    best_level = None
    level_order = {'high': 3, 'medium': 2, 'low': 1}

    for ev in events:
        affects = [a.lower() for a in ev.get('affects', [])]
        if 'all' not in affects and prod_lower not in affects:
            continue
        desc = f"{ev['day_label']} {ev.get('time_bj', '')} {ev['name']}"
        if ev.get('country'):
            desc += f" ({ev['country']})"
        matched.append(desc)
        lv = ev.get('level', 'medium')
        if best_level is None or level_order.get(lv, 0) > level_order.get(best_level, 0):
            best_level = lv

    # 周五夜盘硬编码（不依赖JSON）
    today = date.today()
    if today.weekday() == 4:  # Friday
        matched.append('今日 周五夜盘（周末持仓风险）')
        if best_level is None or level_order.get('medium', 0) > level_order.get(best_level, 0):
            best_level = 'medium'

    score_map = {'high': 2.5, 'medium': 1.5, 'low': 0.5}
    score = score_map.get(best_level, 0.0)
    return score, matched


# ============ 指标2：IV分位数 (0-2.5) ============

def check_iv_percentile(futures_sym: str) -> Tuple[float, float, float]:
    """计算ATM隐含波动率在近期历史中的分位数
    Returns: (score, current_iv, percentile)
    """
    db = _get_db()
    cur = db.cursor()
    product = _extract_product_code(futures_sym)
    dte = _estimate_dte(futures_sym)
    T = dte / 365.0

    if T <= 0.001:
        return 0.0, 0.0, 1.0

    # 获取期货最新价格
    cur.execute("""
        SELECT close_price FROM dbbardata
        WHERE symbol=? ORDER BY datetime DESC LIMIT 1
    """, (futures_sym,))
    row = cur.fetchone()
    if not row:
        return 0.0, 0.0, 0.5
    fut_price = row[0]

    # 找ATM期权：行权价最接近期货价格的 Call 和 Put
    pattern = f'{futures_sym}%'
    dash_pattern = f'{futures_sym}-%'
    cur.execute("""
        SELECT DISTINCT symbol FROM dbbardata
        WHERE (symbol LIKE ? OR symbol LIKE ?) AND symbol != ?
    """, (pattern, dash_pattern, futures_sym))
    all_options = [r[0] for r in cur.fetchall()]

    # 分离 Call / Put，找最接近ATM的
    best_call = best_put = None
    best_c_diff = best_p_diff = float('inf')

    for sym in all_options:
        upper = sym.upper().replace('-', '')
        strike = _extract_strike(sym)
        if not strike:
            continue
        diff = abs(strike - fut_price)
        tail = upper[len(futures_sym.upper()):]
        if 'C' in tail and diff < best_c_diff:
            best_c_diff = diff
            best_call = sym
        elif 'P' in tail and diff < best_p_diff:
            best_p_diff = diff
            best_put = sym

    if not best_call and not best_put:
        return 0.0, 0.0, 0.5

    # 获取ATM期权当前IV
    ivs_now = []
    for sym, opt_type in [(best_call, 'c'), (best_put, 'p')]:
        if not sym:
            continue
        cur.execute("""
            SELECT close_price FROM dbbardata
            WHERE symbol=? ORDER BY datetime DESC LIMIT 1
        """, (sym,))
        r = cur.fetchone()
        if r and r[0] > 0:
            strike = _extract_strike(sym)
            iv = implied_volatility(r[0], fut_price, strike, T, opt_type)
            if iv and 0.01 < iv < 5.0:
                ivs_now.append(iv)

    if not ivs_now:
        return 0.0, 0.0, 0.5
    current_iv = sum(ivs_now) / len(ivs_now)

    # 历史IV：从CTP数据库取过去几天的收盘价计算IV
    start_dt = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d %H:%M:%S')
    hist_ivs = []

    for sym, opt_type in [(best_call, 'c'), (best_put, 'p')]:
        if not sym:
            continue
        # 取期权历史收盘（每小时取一个样本减少计算量）
        cur.execute("""
            SELECT datetime, close_price FROM dbbardata
            WHERE symbol=? AND datetime>=? ORDER BY datetime
        """, (sym, start_dt))
        opt_rows = cur.fetchall()
        if not opt_rows:
            continue

        # 同步取期货历史
        cur.execute("""
            SELECT datetime, close_price FROM dbbardata
            WHERE symbol=? AND datetime>=? ORDER BY datetime
        """, (futures_sym, start_dt))
        fut_map = {r[0]: r[1] for r in cur.fetchall()}

        strike = _extract_strike(sym)
        # 每60分钟采样
        for i in range(0, len(opt_rows), 60):
            dt_str, opt_px = opt_rows[i]
            f_px = fut_map.get(dt_str)
            if not f_px or f_px <= 0 or opt_px <= 0:
                continue
            iv = implied_volatility(opt_px, f_px, strike, T, opt_type)
            if iv and 0.01 < iv < 5.0:
                hist_ivs.append(iv)

    if len(hist_ivs) < 3:
        # 数据不足，给中间分位
        return 1.0, current_iv, 0.5

    # 计算分位
    below = sum(1 for h in hist_ivs if h <= current_iv)
    percentile = below / len(hist_ivs)

    # 评分：IV越低越适合买入（Gamma Scalp）
    if percentile < 0.20:
        score = 2.5
    elif percentile < 0.40:
        score = 2.0
    elif percentile < 0.60:
        score = 1.0
    elif percentile < 0.80:
        score = 0.5
    else:
        score = 0.0

    return score, current_iv, percentile


# ============ 指标3：布林带 Squeeze (0-2.5) ============

def _aggregate_5min_ohlc(times, opens, highs, lows, closes):
    """将1分钟数据聚合为5分钟OHLC"""
    if not times:
        return [], [], [], [], []
    t5, o5, h5, l5, c5 = [], [], [], [], []
    cur_bucket = None
    bo, bh, bl, bc = None, None, None, None

    for t, o, h, l, c in zip(times, opens, highs, lows, closes):
        try:
            dt = datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
            bucket = dt.replace(minute=(dt.minute // 5) * 5, second=0)
        except Exception:
            continue
        if bucket != cur_bucket:
            if bc is not None:
                t5.append(cur_bucket)
                o5.append(bo)
                h5.append(bh)
                l5.append(bl)
                c5.append(bc)
            cur_bucket = bucket
            bo, bh, bl = o, h, l
        else:
            if h > bh:
                bh = h
            if l < bl:
                bl = l
        bc = c

    if bc is not None:
        t5.append(cur_bucket)
        o5.append(bo)
        h5.append(bh)
        l5.append(bl)
        c5.append(bc)

    return t5, o5, h5, l5, c5


def check_bb_squeeze(futures_sym: str) -> Tuple[float, float, float]:
    """计算布林带宽度在近期的分位数
    Returns: (score, current_width_pct, percentile)
    """
    db = _get_db()
    cur = db.cursor()

    # 取最近数据（约5-7天，需要足够5分钟K线）
    start_dt = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("""
        SELECT datetime, close_price FROM dbbardata
        WHERE symbol=? AND datetime>=? ORDER BY datetime
    """, (futures_sym, start_dt))
    rows = cur.fetchall()
    if len(rows) < 130:  # 至少需要 26*5=130 根1分钟K线
        return 0.0, 0.0, 0.5

    times = [r[0] for r in rows]
    prices = [r[1] for r in rows]

    # 聚合5分钟收盘价
    closes_5min = []
    cur_bucket = None
    cur_price = None
    for t, p in zip(times, prices):
        try:
            dt = datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
            bucket = dt.replace(minute=(dt.minute // 5) * 5, second=0)
        except Exception:
            continue
        if bucket != cur_bucket:
            if cur_price is not None:
                closes_5min.append(cur_price)
            cur_bucket = bucket
        cur_price = p
    if cur_price is not None:
        closes_5min.append(cur_price)

    if len(closes_5min) < 100:
        return 0.0, 0.0, 0.5

    # 计算滚动BB Width
    period = 26
    widths = []
    for i in range(period, len(closes_5min) + 1):
        window = closes_5min[i - period:i]
        mid = sum(window) / period
        if mid <= 0:
            continue
        std = (sum((x - mid) ** 2 for x in window) / (period - 1)) ** 0.5
        width_pct = 2 * 1.5 * std / mid * 100  # BB Width as percentage
        widths.append(width_pct)

    if len(widths) < 10:
        return 0.0, 0.0, 0.5

    current_width = widths[-1]
    # 只看最近100根的分位
    recent_widths = widths[-100:] if len(widths) > 100 else widths
    below = sum(1 for w in recent_widths if w <= current_width)
    percentile = below / len(recent_widths)

    # 评分：宽度越窄（分位越低）越适合 Gamma Scalp
    if percentile < 0.10:
        score = 2.5
    elif percentile < 0.25:
        score = 2.0
    elif percentile < 0.50:
        score = 1.0
    elif percentile < 0.75:
        score = 0.5
    else:
        score = 0.0

    return score, current_width, percentile


# ============ 指标4：ATR低位 (0-2.5) ============

def check_atr(futures_sym: str) -> Tuple[float, float, float]:
    """计算5分钟ATR(14)在近期的分位数
    Returns: (score, current_atr, percentile)
    """
    db = _get_db()
    cur = db.cursor()

    start_dt = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("""
        SELECT datetime, open_price, high_price, low_price, close_price FROM dbbardata
        WHERE symbol=? AND datetime>=? ORDER BY datetime
    """, (futures_sym, start_dt))
    rows = cur.fetchall()
    if len(rows) < 100:
        return 0.0, 0.0, 0.5

    # 聚合5分钟OHLC
    times = [r[0] for r in rows]
    opens = [r[1] for r in rows]
    highs = [r[2] for r in rows]
    lows = [r[3] for r in rows]
    closes = [r[4] for r in rows]

    t5, o5, h5, l5, c5 = _aggregate_5min_ohlc(times, opens, highs, lows, closes)

    if len(c5) < 30:
        return 0.0, 0.0, 0.5

    # 计算 True Range 序列
    trs = []
    for i in range(1, len(c5)):
        tr = max(h5[i] - l5[i], abs(h5[i] - c5[i - 1]), abs(l5[i] - c5[i - 1]))
        trs.append(tr)

    if len(trs) < 28:
        return 0.0, 0.0, 0.5

    # 滚动ATR(14)
    atr_period = 14
    atrs = []
    atr_val = sum(trs[:atr_period]) / atr_period
    atrs.append(atr_val)
    for i in range(atr_period, len(trs)):
        atr_val = (atr_val * (atr_period - 1) + trs[i]) / atr_period
        atrs.append(atr_val)

    if len(atrs) < 10:
        return 0.0, 0.0, 0.5

    current_atr = atrs[-1]
    recent_atrs = atrs[-100:] if len(atrs) > 100 else atrs
    below = sum(1 for a in recent_atrs if a <= current_atr)
    percentile = below / len(recent_atrs)

    # 评分：ATR越低越适合 Gamma Scalp（波动率收缩，将要爆发）
    if percentile < 0.15:
        score = 2.5
    elif percentile < 0.30:
        score = 2.0
    elif percentile < 0.50:
        score = 1.0
    elif percentile < 0.75:
        score = 0.5
    else:
        score = 0.0

    return score, current_atr, percentile


# ============ 综合评分 ============

ADVICE_LEVELS = [
    (7.0, 'Gamma Scalp', '#00FF88'),
    (5.0, 'Gamma 倾向', '#FFD700'),
    (3.0, '观望', '#888888'),
    (1.5, 'Strangle 倾向', '#FF8800'),
    (0.0, 'Strangle Sell', '#FF4444'),
]


def get_advice(total_score: float, dte: int, iv_percentile: float) -> Tuple[str, str]:
    """根据总分和硬约束返回 (建议文本, 颜色)"""
    # 硬约束覆盖
    if dte < 7:
        return 'Strangle Sell (DTE<7)', '#FF4444'
    if iv_percentile > 0.90:
        return 'Strangle Sell (IV>90%)', '#FF4444'

    for threshold, label, color in ADVICE_LEVELS:
        if total_score >= threshold:
            return label, color
    return 'Strangle Sell', '#FF4444'


# ============ 单品种完整检查 ============

def check_product(futures_sym: str) -> Optional[Dict]:
    """对单个品种执行完整的4指标检查
    Returns: dict with all scores and details, or None if insufficient data
    """
    product = _extract_product_code(futures_sym)
    dte = _estimate_dte(futures_sym)

    # 指标1: 事件
    event_score, event_list = check_events(product)

    # 指标2: IV分位
    iv_score, current_iv, iv_pct = check_iv_percentile(futures_sym)

    # 指标3: BB Squeeze
    bb_score, bb_width, bb_pct = check_bb_squeeze(futures_sym)

    # 指标4: ATR
    atr_score, current_atr, atr_pct = check_atr(futures_sym)

    total = event_score + iv_score + bb_score + atr_score
    advice, color = get_advice(total, dte, iv_pct)

    return {
        'futures_sym': futures_sym,
        'product': product,
        'dte': dte,
        'event_score': event_score,
        'event_list': event_list,
        'iv_score': iv_score,
        'iv_value': current_iv,
        'iv_percentile': iv_pct,
        'bb_score': bb_score,
        'bb_width': bb_width,
        'bb_percentile': bb_pct,
        'atr_score': atr_score,
        'atr_value': current_atr,
        'atr_percentile': atr_pct,
        'total_score': total,
        'advice': advice,
        'advice_color': color,
    }


# ============ 全品种扫描（带缓存）============

_cache = {'results': [], 'events_all': [], 'ts': None}
CACHE_TTL = 60  # 60秒缓存


def scan_all() -> Tuple[List[Dict], List[str]]:
    """扫描所有活跃品种，返回 (results, all_event_descriptions)
    带60秒缓存。
    """
    now = time.time()
    if _cache['ts'] and now - _cache['ts'] < CACHE_TTL:
        return _cache['results'], _cache['events_all']

    db = _get_db()
    cur = db.cursor()

    # 找出所有活跃的期货合约（最近24小时有数据的）
    recent = (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("""
        SELECT DISTINCT symbol FROM dbbardata
        WHERE datetime >= ?
    """, (recent,))
    all_symbols = [r[0] for r in cur.fetchall()]

    # 筛选期货合约（非期权）
    futures_syms = set()
    for sym in all_symbols:
        # 期货格式: ag2604, TA604, p2604 (无C/P)
        upper = sym.upper().replace('-', '')
        m = re.match(r'^([A-Z]+)(\d{3,4})$', upper)
        if m:
            futures_syms.add(sym)

    # 每个品种只保留最近月份
    product_futures = {}
    for sym in futures_syms:
        product = _extract_product_code(sym)
        m = re.search(r'(\d{3,4})$', sym)
        if not m:
            continue
        month = m.group(1)
        if product not in product_futures or month < product_futures[product][1]:
            product_futures[product] = (sym, month)

    results = []
    events_all = set()

    for product, (fut_sym, month) in product_futures.items():
        try:
            r = check_product(fut_sym)
            if r:
                results.append(r)
                for ev in r['event_list']:
                    events_all.add(ev)
        except Exception:
            continue

    results.sort(key=lambda x: x['total_score'], reverse=True)
    events_all = sorted(events_all)

    _cache['results'] = results
    _cache['events_all'] = events_all
    _cache['ts'] = now

    return results, events_all


def invalidate_cache():
    """手动清除缓存"""
    _cache['ts'] = None
