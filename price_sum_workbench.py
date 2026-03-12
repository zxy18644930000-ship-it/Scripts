#!/usr/bin/env python3
"""期权工作台
一个页面管理多个期权对的走势图，支持动态添加/删除，配置自动持久化。
"""

import json
import sqlite3
import subprocess
import os
import re
import math
import time
import threading
from datetime import datetime, timedelta, date
from dash import Dash, dcc, html, ctx, no_update, ALL, MATCH
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go

# Gamma Scalp 每日检查
try:
    from gamma_scalp_checker import scan_all as gs_scan_all
    _HAS_CHECKER = True
except ImportError:
    _HAS_CHECKER = False

# Gamma 风控计算（Volga 预警）
try:
    from gamma_monitor import calculate_pair_greeks as _gm_calc_greeks
    _HAS_GAMMA_MONITOR = True
except ImportError:
    _HAS_GAMMA_MONITOR = False

DB_PATH = os.path.expanduser('~/.vntrader/database.db')
CONFIG_PATH = os.path.expanduser('~/Scripts/price_sum_pairs.json')
ALERT_HISTORY_PATH = os.path.expanduser('~/Scripts/alert_history.json')
ALERT_DB_PATH = os.path.expanduser('~/Scripts/alert_history.db')  # SQLite长期存储
PORT = 8052
REFRESH_MS = 60_000


# ============ 交易顾问 ============

# 品种→交易所映射
_EXCHANGE_MAP = {
    # 郑商所
    'SA': 'CZCE', 'FG': 'CZCE', 'TA': 'CZCE', 'MA': 'CZCE', 'OI': 'CZCE',
    'RM': 'CZCE', 'CF': 'CZCE', 'SR': 'CZCE', 'PK': 'CZCE', 'SM': 'CZCE',
    'SF': 'CZCE', 'SH': 'CZCE', 'UR': 'CZCE', 'PF': 'CZCE',
    'AP': 'CZCE_SP', 'CJ': 'CZCE_SP', 'PX': 'CZCE_SP',
    # 大商所
    'P': 'DCE', 'M': 'DCE', 'Y': 'DCE', 'I': 'DCE', 'JM': 'DCE',
    'JD': 'DCE', 'LH': 'DCE', 'PP': 'DCE', 'L': 'DCE', 'V': 'DCE',
    'EB': 'DCE', 'EG': 'DCE', 'C': 'DCE', 'CS': 'DCE', 'RR': 'DCE',
    # 上期所
    'AG': 'SHFE', 'AU': 'SHFE', 'CU': 'SHFE', 'AL': 'SHFE', 'ZN': 'SHFE',
    'PB': 'SHFE', 'NI': 'SHFE', 'SN': 'SHFE', 'RU': 'SHFE', 'FU': 'SHFE',
    'BU': 'SHFE', 'SP': 'SHFE', 'AO': 'SHFE', 'RB': 'SHFE',
    # 广期所
    'SI': 'GFEX', 'LC': 'GFEX',
    # 能源中心
    'SC': 'INE',
}

# 各交易所期权到期: 交割月前一月的近似日 (旧版,仅作兼容保留)
_EXPIRY_DAY = {
    'CZCE': 11, 'CZCE_SP': -5, 'DCE': 17, 'SHFE': 25, 'GFEX': 6, 'INE': 13,
}

# === 统一到期日计算 (官方规则, 模块级) ===
try:
    import sys as _sys
    if '/Users/zhangxiaoyu/Downloads/trade2026' not in _sys.path:
        _sys.path.insert(0, '/Users/zhangxiaoyu/Downloads/trade2026')
    from infra.expiry.option_expiry_calendar import get_expiry_date as _official_expiry
    from infra.expiry.option_expiry_calendar import get_trading_calendar as _get_cal
    _trading_cal = _get_cal()
    _USE_OFFICIAL_CAL = True
except Exception:
    _USE_OFFICIAL_CAL = False
    _trading_cal = None
    from calendar import monthrange as _mr
    _CZCE_TWO_MONTH = {'AP', 'CJ', 'PX'}
    def _official_expiry(ex, yr, mo, cal=None, product_code=''):
        """简化回退: 不含节假日但规则正确"""
        pc = product_code.upper()
        if ex in ('CZCE', 'CZCE_SP', 'GFEX_CZCE') and pc in _CZCE_TWO_MONTH:
            pm = mo - 2 if mo > 2 else mo + 10
            py = yr if mo > 2 else yr - 1
            tds = [date(py,pm,d) for d in range(1, _mr(py,pm)[1]+1) if date(py,pm,d).weekday()<5]
            return tds[-3] if len(tds)>=3 else None
        pm = mo-1 if mo>1 else 12; py = yr if mo>1 else yr-1
        if ex in ('CZCE', 'CZCE_SP', 'GFEX_CZCE'):
            tds = [date(py,pm,d) for d in range(1, min(16, _mr(py,pm)[1]+1)) if date(py,pm,d).weekday()<5]
            return tds[-3] if len(tds)>=3 else None
        tds = [date(py,pm,d) for d in range(1, _mr(py,pm)[1]+1) if date(py,pm,d).weekday()<5]
        if ex == 'DCE':
            return tds[11] if len(tds)>=12 else None
        elif ex in ('SHFE','INE'):
            n = 5 if ex=='SHFE' else 13
            return tds[-n] if len(tds)>=n else None
        elif ex == 'GFEX':
            return tds[4] if len(tds)>=5 else None
        return None


# CTP夜盘数据用交易日日期存储(周五夜盘存为下周一日期)，
# 同一日期下夜盘22:59的datetime>日盘14:01，简单DESC会取到昨晚旧价。
# 修正：按日期DESC + 日盘优先(priority=1) + 时间DESC排序。
_LATEST_PRICE_SQL = """SELECT close_price FROM dbbardata WHERE symbol=?
    ORDER BY datetime DESC
    LIMIT 1"""


def calc_expiry(product, year, month):
    """统一到期日计算入口: 品种代码→交易所→官方规则"""
    pc = product.upper()
    ex = _EXCHANGE_MAP.get(pc, '')
    if not ex:
        return None
    # CZCE_SP 在官方日历中用 CZCE
    cal_ex = 'CZCE' if ex == 'CZCE_SP' else ex
    cal = _trading_cal if _USE_OFFICIAL_CAL else None
    return _official_expiry(cal_ex, year, month, cal, product_code=pc)


def calc_dte(product, month_str):
    """从品种+月份码(3/4位)计算DTE天数"""
    try:
        if len(month_str) == 3:
            year = 2020 + int(month_str[0])
            mon = int(month_str[1:])
        elif len(month_str) == 4:
            year = 2000 + int(month_str[:2])
            mon = int(month_str[2:])
        else:
            return None
    except (ValueError, IndexError):
        return None
    if mon < 1 or mon > 12:
        return None
    exp = calc_expiry(product, year, mon)
    if not exp:
        return None
    dte = (exp - date.today()).days
    return max(dte, 0)

# ============ 智能平仓时机提醒 ============
# 数据来源优先级：
#   1. tick快照文件 (state/tick_snapshot.json) — 交易系统实时写入，含bid/ask/spread
#   2. 1分钟K线 (dbbardata) — 回退方案，用成交量粗估价差

# tick快照文件路径（交易系统每2秒写入一次）
_TICK_SNAPSHOT_PATH = os.path.expanduser('~/Downloads/trade2026/state/tick_snapshot.json')

# 各交易所夜盘/日盘收盘时间  (HH:MM)
_SESSION_CLOSE = {
    'CZCE':    {'night': '23:00', 'day': '15:00'},
    'CZCE_SP': {'night': '23:00', 'day': '15:00'},
    'DCE':     {'night': '23:00', 'day': '15:00'},
    'SHFE':    {'night': '01:00', 'day': '15:00'},
    'GFEX':    {'night': '23:00', 'day': '15:00'},
    'INE':     {'night': '02:30', 'day': '15:00'},
}

# 各品种夜盘收盘时间特例（覆盖交易所默认值）
_PRODUCT_NIGHT_CLOSE = {
    'AG': '02:30', 'AU': '02:30',
    'SC': '02:30',
    'CU': '01:00', 'AL': '01:00', 'ZN': '01:00', 'PB': '01:00',
    'NI': '01:00', 'SN': '01:00',
    'RU': '23:00', 'FU': '23:00', 'BU': '23:00', 'SP': '23:00',
    'AO': '01:00', 'RB': '23:00',
}

# 品种→最小变动价位 (用于把价差换算成tick数)
_PRICE_TICK = {
    'CF': 1, 'SA': 1, 'FG': 1, 'TA': 2, 'SR': 1, 'MA': 1, 'RM': 1, 'PK': 2,
    'P': 2, 'M': 1, 'Y': 2, 'I': 0.5, 'JM': 0.5, 'JD': 1, 'PP': 1, 'L': 5, 'V': 5,
    'AG': 1, 'AU': 0.02, 'CU': 10, 'AL': 5, 'ZN': 5, 'NI': 10, 'SN': 10,
    'RU': 5, 'SP': 2, 'AO': 1, 'SI': 5, 'SC': 0.1,
}

# 分轮配置（品种→出仓手数列表，决定所需轮次）
_EXIT_ROUND_VOLUMES = {
    'CF': [10, 10, 10, 10, 10, 10, 5],   # 65手7轮
    'SA': [5, 5, 5],                       # 15手3轮
}
_DEFAULT_EXIT_ROUNDS = [5, 5, 5]

# 价差(tick)→额外时间(秒) 查表
_SPREAD_EXTRA_TABLE = [(1, 0), (2, 30), (3, 60), (5, 120), (8, 240), (10, 360)]


def _get_session_close_dt(product, now=None):
    """获取当前时段的收盘时间 datetime，如果不在交易时段返回 None"""
    if now is None:
        now = datetime.now()
    t = now.time()
    product = product.upper()
    exchange = _EXCHANGE_MAP.get(product)
    if not exchange:
        return None
    sc = _SESSION_CLOSE.get(exchange, {})
    from datetime import time as _time
    day_close_str = sc.get('day', '15:00')
    night_close_str = _PRODUCT_NIGHT_CLOSE.get(product, sc.get('night', '23:00'))
    dh, dm = map(int, day_close_str.split(':'))
    nh, nm = map(int, night_close_str.split(':'))
    day_close_t = _time(dh, dm)
    night_close_t = _time(nh, nm)
    if _time(9, 0) <= t <= _time(15, 0):
        return datetime.combine(now.date(), day_close_t)
    if t >= _time(21, 0):
        if night_close_t < _time(21, 0):
            return datetime.combine(now.date() + timedelta(days=1), night_close_t)
        else:
            return datetime.combine(now.date(), night_close_t)
    if t <= _time(3, 0):
        if night_close_t <= _time(3, 0):
            return datetime.combine(now.date(), night_close_t)
    return None


def _interpolate_spread_extra(spread_ticks):
    """价差(tick)→额外秒数，线性插值"""
    if spread_ticks <= _SPREAD_EXTRA_TABLE[0][0]:
        return _SPREAD_EXTRA_TABLE[0][1]
    if spread_ticks >= _SPREAD_EXTRA_TABLE[-1][0]:
        return _SPREAD_EXTRA_TABLE[-1][1]
    for i in range(len(_SPREAD_EXTRA_TABLE) - 1):
        lo_t, lo_v = _SPREAD_EXTRA_TABLE[i]
        hi_t, hi_v = _SPREAD_EXTRA_TABLE[i + 1]
        if lo_t <= spread_ticks <= hi_t:
            ratio = (spread_ticks - lo_t) / (hi_t - lo_t)
            return lo_v + ratio * (hi_v - lo_v)
    return 0


def _load_tick_snapshot():
    """
    读取交易系统写入的tick快照文件
    Returns:
        dict or None — 快照数据，超过30秒视为过期返回None
    """
    try:
        if not os.path.exists(_TICK_SNAPSHOT_PATH):
            return None
        with open(_TICK_SNAPSHOT_PATH, 'r') as f:
            snap = json.load(f)
        # 检查时效性（超过30秒认为过期）
        ts_str = snap.get('timestamp')
        if ts_str:
            ts = datetime.fromisoformat(ts_str)
            age = (datetime.now() - ts).total_seconds()
            if age > 30:
                return None
        snap['_age'] = age if ts_str else 999
        return snap
    except Exception:
        return None


def _calc_exit_timing_from_tick(snap, product):
    """
    用真实tick数据（bid/ask/volume）计算平仓时机建议

    Args:
        snap: tick快照 {'call': {bid_price, ask_price, ...}, 'put': {...}, ...}
        product: 品种代码
    Returns:
        dict or None
    """
    now = datetime.now()
    close_dt = _get_session_close_dt(product, now)
    if close_dt is None:
        return None
    secs = (close_dt - now).total_seconds()
    if secs <= 0 or secs > 3600:
        return None

    product = product.upper()
    rounds = _EXIT_ROUND_VOLUMES.get(product, _DEFAULT_EXIT_ROUNDS)
    n_rounds = len(rounds)
    max_round_vol = max(rounds) if rounds else 5
    price_tick = _PRICE_TICK.get(product, 1)

    call_data = snap.get('call', {})
    put_data = snap.get('put', {})

    # 计算两腿价差(tick数)
    def _spread_ticks(leg):
        bid = leg.get('bid_price', 0)
        ask = leg.get('ask_price', 0)
        if bid > 0 and ask > 0 and price_tick > 0:
            return (ask - bid) / price_tick
        return 0

    call_spread = _spread_ticks(call_data)
    put_spread = _spread_ticks(put_data)
    worst_spread = max(call_spread, put_spread)

    # 对手盘深度：平空头 = 买入平仓 → 看ask_volume（做市商卖给我们）
    # 注意：也可能是bid_volume（我们挂限价卖单被动成交）
    # 保守起见取两者中较小的
    call_depth = min(call_data.get('bid_volume', 0), call_data.get('ask_volume', 0))
    put_depth = min(put_data.get('bid_volume', 0), put_data.get('ask_volume', 0))
    worst_depth = min(call_depth, put_depth)

    # 基础执行时间
    base_exec = 2.0 + max(0, n_rounds - 1) * 5.0

    # 价差额外（真实tick数据！）
    spread_extra = _interpolate_spread_extra(worst_spread)

    # 深度额外：对手盘<单轮手数时按比例惩罚
    if worst_depth <= 0:
        depth_extra = n_rounds * 7.5
    elif worst_depth >= max_round_vol:
        depth_extra = 0
    else:
        shortage = (max_round_vol - worst_depth) / max_round_vol
        depth_extra = shortage * n_rounds * 7.5

    # tick频率用快照年龄粗估（快照2秒写一次，如果能读到说明系统活跃）
    snap_age = snap.get('_age', 999)
    if snap_age <= 5:
        freq_extra = 0       # 快照新鲜→行情活跃
    elif snap_age <= 15:
        freq_extra = 30
    else:
        freq_extra = 90

    safety = 30
    total_lead = base_exec + spread_extra + depth_extra + freq_extra + safety

    margin = secs - total_lead
    if margin <= 0:
        urgency, color = 'CRITICAL', '#FF4444'
    elif margin <= 30:
        urgency, color = 'URGENT', '#FF8800'
    elif margin <= 120:
        urgency, color = 'CAUTIOUS', '#FFD700'
    elif margin <= 300:
        urgency, color = 'NORMAL', '#00BFFF'
    else:
        urgency, color = 'RELAXED', '#00FF88'

    start_dt = close_dt - timedelta(seconds=total_lead)

    breakdown = (f"基础{base_exec:.0f}s + 价差{spread_extra:.0f}s({worst_spread:.1f}tick) + "
                 f"深度{depth_extra:.0f}s({worst_depth}手) + 频率{freq_extra:.0f}s + 安全{safety}s")

    return {
        'product': product,
        'close_time': close_dt,
        'seconds_to_close': secs,
        'lead_seconds': total_lead,
        'start_time': start_dt.strftime('%H:%M:%S'),
        'urgency': urgency,
        'urgency_color': color,
        'breakdown': breakdown,
        'n_rounds': n_rounds,
        'call_spread': call_spread,
        'put_spread': put_spread,
        'worst_spread': worst_spread,
        'worst_depth': worst_depth,
        'data_source': 'tick',
        'snap_age': snap_age,
    }


def _calc_exit_timing_fallback(product):
    """
    回退方案：无tick快照时，用1分钟K线成交量粗估
    """
    now = datetime.now()
    close_dt = _get_session_close_dt(product, now)
    if close_dt is None:
        return None
    secs = (close_dt - now).total_seconds()
    if secs <= 0 or secs > 3600:
        return None

    product = product.upper()
    rounds = _EXIT_ROUND_VOLUMES.get(product, _DEFAULT_EXIT_ROUNDS)
    n_rounds = len(rounds)
    base_exec = 2.0 + max(0, n_rounds - 1) * 5.0

    # 无tick数据时用固定偏保守的估计
    spread_extra = 60    # 假设~3tick
    depth_extra = 0
    freq_extra = 45      # 假设偏低频
    safety = 30

    total_lead = base_exec + spread_extra + depth_extra + freq_extra + safety
    margin = secs - total_lead
    if margin <= 0:
        urgency, color = 'CRITICAL', '#FF4444'
    elif margin <= 30:
        urgency, color = 'URGENT', '#FF8800'
    elif margin <= 120:
        urgency, color = 'CAUTIOUS', '#FFD700'
    elif margin <= 300:
        urgency, color = 'NORMAL', '#00BFFF'
    else:
        urgency, color = 'RELAXED', '#00FF88'

    start_dt = close_dt - timedelta(seconds=total_lead)
    breakdown = (f"基础{base_exec:.0f}s + 价差{spread_extra:.0f}s(估) + "
                 f"频率{freq_extra:.0f}s(估) + 安全{safety}s [无tick数据]")

    return {
        'product': product,
        'close_time': close_dt,
        'seconds_to_close': secs,
        'lead_seconds': total_lead,
        'start_time': start_dt.strftime('%H:%M:%S'),
        'urgency': urgency,
        'urgency_color': color,
        'breakdown': breakdown,
        'n_rounds': n_rounds,
        'data_source': 'fallback',
    }


def _get_held_products():
    """从策略状态文件获取当前有持仓的品种集合"""
    positions = _load_strategy_positions()
    return {p['product'].upper() for p in positions if p.get('product')}


def _build_exit_timing_banner(manual_pairs):
    """
    仅为实际持仓品种构建平仓时机提醒横幅。
    无持仓时不计算、不显示。
    """
    # 只对有持仓的品种提醒
    held = _get_held_products()
    if not held:
        return None

    alerts = []
    snap = _load_tick_snapshot()

    for pair in (manual_pairs or []):
        call_sym = pair[0]
        parsed = _parse_contract(call_sym)
        if not parsed:
            continue
        product = parsed[0].upper()

        # 没有该品种持仓就跳过
        if product not in held:
            continue

        if snap and snap.get('product', '').upper() == product:
            info = _calc_exit_timing_from_tick(snap, product)
        else:
            info = _calc_exit_timing_fallback(product)

        if info is not None:
            alerts.append(info)

    if not alerts:
        return None

    # 按紧迫度排序
    priority = {'CRITICAL': 0, 'URGENT': 1, 'CAUTIOUS': 2, 'NORMAL': 3, 'RELAXED': 4}
    alerts.sort(key=lambda a: priority.get(a['urgency'], 5))
    top_color = alerts[0]['urgency_color']

    items = []
    for a in alerts:
        mins = int(a['seconds_to_close'] // 60)
        secs = int(a['seconds_to_close'] % 60)

        row_parts = [
            html.Span(f"{a['product']} ", style={
                'fontWeight': 'bold', 'fontSize': '14px', 'color': '#FFD700'}),
            html.Span(f"距收盘 {mins}分{secs}秒", style={
                'fontSize': '13px', 'color': '#ddd', 'marginRight': '12px'}),
            html.Span(f"建议 {a['start_time']} 开始平仓", style={
                'fontSize': '13px', 'color': a['urgency_color'], 'fontWeight': 'bold',
                'marginRight': '12px'}),
            html.Span(f"[{a['urgency']}]", style={
                'fontSize': '12px', 'color': a['urgency_color'],
                'backgroundColor': f"{a['urgency_color']}22",
                'padding': '1px 6px', 'borderRadius': '3px', 'marginRight': '12px'}),
        ]

        # tick数据时显示真实价差/深度
        if a.get('data_source') == 'tick':
            row_parts.append(html.Span(
                f"价差{a.get('worst_spread', 0):.1f}tick  深度{a.get('worst_depth', 0)}手  "
                f"C={a.get('call_spread', 0):.0f}t P={a.get('put_spread', 0):.0f}t",
                style={'fontSize': '12px', 'color': '#4fc3f7', 'marginRight': '12px'}))

        # 数据源标签
        src = 'TICK' if a.get('data_source') == 'tick' else 'K线估算'
        src_color = '#00FF88' if a.get('data_source') == 'tick' else '#888'
        row_parts.append(html.Span(f"[{src}]", style={
            'fontSize': '10px', 'color': src_color, 'marginRight': '8px'}))

        row_parts.append(html.Span(f"{a['n_rounds']}轮 | {a['breakdown']}", style={
            'fontSize': '11px', 'color': '#888'}))

        items.append(html.Div(row_parts, style={'marginBottom': '4px'}))

    return html.Div([
        html.Div([
            html.Span('⏰ 平仓时机提醒', style={
                'color': top_color, 'fontSize': '15px', 'fontWeight': 'bold'}),
            html.Span(
                '  基于实时tick数据' if any(a.get('data_source') == 'tick' for a in alerts)
                else '  基于K线估算（交易系统未运行）',
                style={'color': '#666', 'fontSize': '11px', 'marginLeft': '8px'}),
        ], style={'marginBottom': '6px'}),
        *items,
    ], style={
        'padding': '12px 20px',
        'backgroundColor': '#1a1a0a',
        'borderTop': f'3px solid {top_color}',
        'borderBottom': f'1px solid {top_color}33',
    })


_DASHBOARD_JSON_PATH = os.path.expanduser('~/Downloads/trade2026/state/dashboard.json')
_STRATEGY_STATE_DIR = os.path.expanduser('~/state')


def _load_dashboard():
    """读取dashboard.json，5分钟内视为有效"""
    try:
        if not os.path.exists(_DASHBOARD_JSON_PATH):
            return None
        with open(_DASHBOARD_JSON_PATH, 'r') as f:
            data = json.load(f)
        ts_str = data.get('timestamp')
        if ts_str:
            ts = datetime.fromisoformat(ts_str)
            age = (datetime.now() - ts).total_seconds()
            if age > 300:
                return None
            data['_age'] = age
        else:
            data['_age'] = 999
        return data
    except Exception:
        return None


def _load_strategy_positions():
    """
    从策略状态文件(~/state/*_state.json)读取所有持仓。
    如果state文件无持仓，兜底从dashboard.json读取CTP真实持仓（4小时内有效）。
    Returns:
        list of dict: [{symbol, exchange, direction, volume, avg_price, pnl, product}, ...]
    """
    positions = []
    try:
        if not os.path.isdir(_STRATEGY_STATE_DIR):
            return positions
        for fname in os.listdir(_STRATEGY_STATE_DIR):
            if not fname.endswith('_state.json'):
                continue
            fpath = os.path.join(_STRATEGY_STATE_DIR, fname)
            file_age = time.time() - os.path.getmtime(fpath)
            # 超过24小时一律跳过
            if file_age > 86400:
                continue
            try:
                with open(fpath, 'r') as f:
                    state = json.load(f)
                product = state.get('product_code', '')
                # 策略未运行且文件超过4小时 → 跳过（防止幽灵持仓）
                if file_age > 14400 and product and not _is_strategy_running(product):
                    continue
                extra = state.get('extra', {})
                real_call_px = extra.get('entry_call_price')
                real_put_px = extra.get('entry_put_price')
                call_sym = state.get('call_symbol', '').split('.')[0]
                put_sym = state.get('put_symbol', '').split('.')[0]
                for p in state.get('positions', []):
                    if p.get('volume', 0) > 0:
                        sym = p.get('symbol', '')
                        avg = p.get('avg_price', 0)
                        # 用实际成交价覆盖CTP结算价
                        if sym and real_call_px and sym == call_sym:
                            avg = real_call_px
                        elif sym and real_put_px and sym == put_sym:
                            avg = real_put_px
                        positions.append({
                            'symbol': sym,
                            'exchange': p.get('exchange', ''),
                            'direction': p.get('direction', ''),
                            'volume': p.get('volume', 0),
                            'avg_price': avg,
                            'pnl': p.get('pnl', 0),
                            'product': product,
                        })
            except Exception:
                continue
    except Exception:
        pass

    # 兜底：如果state文件无持仓，从dashboard.json读取CTP真实持仓（4小时内有效）
    if not positions:
        try:
            if os.path.exists(_DASHBOARD_JSON_PATH):
                dash_age = time.time() - os.path.getmtime(_DASHBOARD_JSON_PATH)
                if dash_age < 14400:  # 4小时内
                    with open(_DASHBOARD_JSON_PATH, 'r') as f:
                        dash = json.load(f)
                    for dp in dash.get('positions', []):
                        if dp.get('volume', 0) > 0:
                            sym = dp.get('symbol', '')
                            # 从合约代码推断品种（如CF605C17000→CF）
                            prod = ''
                            import re
                            m = re.match(r'^([A-Za-z]{1,3})', sym)
                            if m:
                                prod = m.group(1).upper()
                            positions.append({
                                'symbol': sym,
                                'exchange': dp.get('exchange', ''),
                                'direction': dp.get('direction', ''),
                                'volume': dp.get('volume', 0),
                                'avg_price': dp.get('avg_price', 0),
                                'pnl': dp.get('pnl', 0),
                                'product': prod,
                            })
        except Exception:
            pass
    return positions


def _load_commodity_config(product):
    """从trade2026读取品种交易参数"""
    try:
        import sys
        if '/Users/zhangxiaoyu/Downloads/trade2026' not in sys.path:
            sys.path.insert(0, '/Users/zhangxiaoyu/Downloads/trade2026')
        from infra.config.commodity_config import COMMODITY_CONFIGS
        return COMMODITY_CONFIGS.get(product.upper(), {})
    except Exception:
        return {}


def _load_full_strategy_states():
    """读取所有活跃的策略状态文件（含extra信息），兜底dashboard.json"""
    states = []
    try:
        if not os.path.isdir(_STRATEGY_STATE_DIR):
            return states
        for fname in os.listdir(_STRATEGY_STATE_DIR):
            if not fname.endswith('_state.json'):
                continue
            fpath = os.path.join(_STRATEGY_STATE_DIR, fname)
            file_age = time.time() - os.path.getmtime(fpath)
            if file_age > 86400:
                continue
            try:
                with open(fpath, 'r') as f:
                    state = json.load(f)
                if state.get('positions'):
                    product = state.get('product_code', '')
                    # 策略未运行且文件超过4小时 → 跳过
                    if file_age > 14400 and product and not _is_strategy_running(product):
                        continue
                    states.append(state)
            except Exception:
                continue
    except Exception:
        pass

    # 兜底：state文件无持仓时，从dashboard.json构造虚拟state
    if not states:
        try:
            if os.path.exists(_DASHBOARD_JSON_PATH):
                dash_age = time.time() - os.path.getmtime(_DASHBOARD_JSON_PATH)
                if dash_age < 14400:  # 4小时内
                    with open(_DASHBOARD_JSON_PATH, 'r') as f:
                        dash = json.load(f)
                    dash_positions = [p for p in dash.get('positions', []) if p.get('volume', 0) > 0]
                    if dash_positions:
                        states.append({
                            'product_code': '',
                            'positions': dash_positions,
                            'call_symbol': '',
                            'put_symbol': '',
                            'extra': {},
                        })
        except Exception:
            pass
    return states


def _build_account_bar():
    """
    从策略状态文件和dashboard.json读取持仓、系统参数和状态，渲染顶部状态栏。
    第一行：持仓 + 账户 + CTP状态
    第二行：策略运行参数（止损比例、止盈模式、强平时间、进仓成本等）
    """
    row1_parts = []
    row2_parts = []

    # 从策略状态文件读持仓
    strategy_states = _load_full_strategy_states()
    positions = []
    for state in strategy_states:
        product = state.get('product_code', '')
        extra = state.get('extra', {})
        # 实际成交价（优先级高于CTP持仓价，后者会被结算价调整）
        real_call_px = extra.get('entry_call_price')
        real_put_px = extra.get('entry_put_price')
        call_sym = state.get('call_symbol', '').split('.')[0]  # "CF605C17000.CZCE" → "CF605C17000"
        put_sym = state.get('put_symbol', '').split('.')[0]
        for p in state.get('positions', []):
            if p.get('volume', 0) > 0:
                p['product'] = product
                # 用实际成交价覆盖CTP结算价
                sym = p.get('symbol', '')
                if sym and real_call_px and sym == call_sym:
                    p['avg_price'] = real_call_px
                elif sym and real_put_px and sym == put_sym:
                    p['avg_price'] = real_put_px
                positions.append(p)

    # 提前加载dashboard（持仓现价 + 后面的账户资金都需要）
    dash_data = _load_dashboard()
    # 即使5分钟超时，仍尝试读取位置现价（4小时内有效）
    if not dash_data and os.path.exists(_DASHBOARD_JSON_PATH):
        _dash_age = time.time() - os.path.getmtime(_DASHBOARD_JSON_PATH)
        if _dash_age < 14400:
            try:
                with open(_DASHBOARD_JSON_PATH, 'r') as f:
                    dash_data = json.load(f)
                dash_data['_stale'] = True  # 标记为非实时
            except Exception:
                pass

    if positions:
        # 获取期权最新价格用于盈亏计算
        # 优先级: 1. dashboard.json现价 2. vnpy DB K线
        _pos_syms = [p.get('symbol', '') for p in positions if p.get('symbol')]
        _pos_px = {}

        # 1. 从dashboard.json读现价
        if dash_data:
            for dp in dash_data.get('positions', []):
                _sym = dp.get('symbol', '')
                _cpx = dp.get('current_price', 0)
                if _sym in _pos_syms and _cpx > 0:
                    _pos_px[_sym] = _cpx

        # 2. 兜底：vnpy DB（跳过结算bar，取倒数第二条）
        _missing = [s for s in _pos_syms if s and s not in _pos_px]
        if _missing:
            try:
                _db = get_db()
                _cur = _db.cursor()
                for _s in _missing:
                    # 取最近2条bar，如果最新一条是结算价（与前一条间隔>10分钟），用前一条
                    _cur.execute(
                        'SELECT datetime, close_price FROM dbbardata WHERE symbol=? '
                        'ORDER BY datetime DESC LIMIT 2', (_s,))
                    _rows = _cur.fetchall()
                    if _rows and len(_rows) >= 2:
                        from datetime import datetime as _dtcls
                        _t0 = _dtcls.fromisoformat(_rows[0][0])
                        _t1 = _dtcls.fromisoformat(_rows[1][0])
                        gap = (_t0 - _t1).total_seconds()
                        if gap > 600 and _rows[1][1] > 0:
                            # 间隔超10分钟，最新bar是结算价，用前一条
                            _pos_px[_s] = _rows[1][1]
                        elif _rows[0][1] > 0:
                            _pos_px[_s] = _rows[0][1]
                    elif _rows and _rows[0][1] > 0:
                        _pos_px[_s] = _rows[0][1]
            except Exception:
                pass

        row1_parts.append(html.Span('持仓 ', style={'color': '#888', 'fontSize': '12px', 'marginRight': '8px'}))
        total_pnl = 0
        for p in positions:
            sym = p.get('symbol', '?')
            direction = p.get('direction', '')
            vol = p.get('volume', 0)
            avg = p.get('avg_price', 0)
            is_short = 'SHORT' in str(direction).upper()
            dir_label = '空' if is_short else '多'
            dir_color = '#FF8800' if is_short else '#00BFFF'

            # 盈亏计算: 卖方盈亏 = (开仓价 - 现价) × 乘数 × 手数
            prod = p.get('product', '').upper()
            mult = _MULTIPLIER.get(prod, 10)
            latest = _pos_px.get(sym)
            pnl_parts = []
            if latest is not None and avg > 0:
                if is_short:
                    pnl = (avg - latest) * mult * vol
                else:
                    pnl = (latest - avg) * mult * vol
                total_pnl += pnl
                pnl_color = '#00e676' if pnl >= 0 else '#f44336'
                pnl_parts = [
                    html.Span(f' 现{latest:.0f}', style={'color': '#aaa', 'fontSize': '11px'}),
                    html.Span(f' {pnl:+,.0f}', style={'color': pnl_color, 'fontSize': '12px', 'fontWeight': 'bold'}),
                ]

            row1_parts.append(html.Span([
                html.Span(f'{sym}', style={'color': '#4fc3f7', 'fontSize': '13px', 'fontWeight': 'bold'}),
                html.Span(f' {dir_label}{vol}手', style={'color': dir_color, 'fontSize': '12px'}),
                html.Span(f' @{avg:.0f}', style={'color': '#888', 'fontSize': '11px'}),
                *pnl_parts,
            ], style={'marginRight': '18px'}))

        # 总盈亏汇总
        if total_pnl != 0:
            pnl_sum_color = '#00e676' if total_pnl >= 0 else '#f44336'
            row1_parts.append(html.Span([
                html.Span('合计 ', style={'color': '#888', 'fontSize': '11px'}),
                html.Span(f'{total_pnl:+,.0f}', style={'color': pnl_sum_color, 'fontSize': '14px', 'fontWeight': 'bold'}),
            ], style={'marginRight': '12px'}))

    # 从dashboard.json读账户资金和系统状态（已在上方加载）
    if dash_data and not dash_data.get('_stale'):
        acc = dash_data.get('account')
        if acc:
            if positions:
                row1_parts.append(html.Span('│', style={'color': '#333', 'margin': '0 12px'}))
            balance = acc.get('balance', 0)
            available = acc.get('available', 0)
            frozen = acc.get('frozen', 0)
            row1_parts.append(html.Span([
                html.Span('权益 ', style={'color': '#888', 'fontSize': '12px'}),
                html.Span(f'{balance:,.0f}', style={'color': '#FFD700', 'fontSize': '14px', 'fontWeight': 'bold'}),
            ], style={'marginRight': '20px'}))
            row1_parts.append(html.Span([
                html.Span('可用 ', style={'color': '#888', 'fontSize': '12px'}),
                html.Span(f'{available:,.0f}', style={'color': '#00FF88', 'fontSize': '14px', 'fontWeight': 'bold'}),
            ], style={'marginRight': '20px'}))
            if frozen > 0:
                row1_parts.append(html.Span([
                    html.Span('冻结 ', style={'color': '#888', 'fontSize': '12px'}),
                    html.Span(f'{frozen:,.0f}', style={'color': '#FF8800', 'fontSize': '13px'}),
                ], style={'marginRight': '20px'}))

        # CTP状态
        health = dash_data.get('health', {})
        ctp_ok = health.get('ctp_connected', False)
        status_color = '#00FF88' if ctp_ok else '#FF4444'
        status_text = 'CTP在线' if ctp_ok else 'CTP离线'
        if row1_parts:
            row1_parts.append(html.Span('│', style={'color': '#333', 'margin': '0 10px'}))
        row1_parts.append(html.Span([
            html.Span('● ', style={'color': status_color, 'fontSize': '10px'}),
            html.Span(status_text, style={'color': status_color, 'fontSize': '12px'}),
        ]))

    # 操作按钮已移至交易行（买入平仓）

    # ===== 第二行：策略运行参数 =====
    for state in strategy_states:
        product = state.get('product_code', '')
        if not product:
            continue
        cfg = _load_commodity_config(product)
        tp = cfg.get('trading_params', {})
        extra = state.get('extra', {})

        tag_style = {'fontSize': '11px', 'padding': '1px 6px', 'borderRadius': '3px', 'marginRight': '10px'}

        # 品种标识
        row2_parts.append(html.Span(f'{product}', style={
            'color': '#FFD700', 'fontSize': '12px', 'fontWeight': 'bold', 'marginRight': '10px'}))

        # 止损: 高/低 >= X
        sl_ratio = tp.get('stop_loss_ratio', 2.0)
        row2_parts.append(html.Span(f'止损≥{sl_ratio}倍', style={
            **tag_style, 'color': '#FF6B6B', 'backgroundColor': '#FF6B6B22'}))

        # 止盈模式
        tp_mode = tp.get('take_profit_mode', 'dte_adaptive')
        if tp_mode == 'fixed_pct':
            tp_pct = tp.get('take_profit_fixed_pct', 0.07)
            tp_text = f'止盈{tp_pct:.0%}/天'
        else:
            tp_mult = tp.get('take_profit_mult', 1.0)
            tp_text = f'止盈DTE×{tp_mult}'
        row2_parts.append(html.Span(tp_text, style={
            **tag_style, 'color': '#00FF88', 'backgroundColor': '#00FF8822'}))

        # 进仓成本
        entry_total = extra.get('entry_total')
        if entry_total:
            row2_parts.append(html.Span(f'成本{entry_total:.0f}', style={
                **tag_style, 'color': '#4fc3f7', 'backgroundColor': '#4fc3f722'}))

        # 进仓时间
        entry_time = extra.get('entry_time')
        if entry_time:
            try:
                et = datetime.fromisoformat(entry_time)
                days_held = (datetime.now() - et).days
                row2_parts.append(html.Span(f'持{days_held}天', style={
                    **tag_style, 'color': '#CE93D8', 'backgroundColor': '#CE93D822'}))
            except Exception:
                pass

        # 止盈目标（fixed_pct模式）
        if tp_mode == 'fixed_pct' and entry_total and entry_time:
            try:
                et = datetime.fromisoformat(entry_time)
                days_held = max(1, (datetime.now() - et).days)
                tp_pct = tp.get('take_profit_fixed_pct', 0.07)
                target = entry_total * tp_pct * days_held
                row2_parts.append(html.Span(f'目标盈{target:.0f}', style={
                    **tag_style, 'color': '#FFD700', 'backgroundColor': '#FFD70022'}))
            except Exception:
                pass

        # 强平时间标签已移除（由下方"时间强平"按钮控制状态显示）

        # 目标手数
        target_lots = tp.get('target_lots', 0)
        if target_lots:
            row2_parts.append(html.Span(f'目标{target_lots}手', style={
                **tag_style, 'color': '#aaa', 'backgroundColor': '#aaaaaa22'}))

        # 进仓模式
        exec_mode = tp.get('execution_mode', 'smart')
        if exec_mode == 'sum_target':
            st = tp.get('sum_target', {})
            target_sum = st.get('target_sum', 0)
            row2_parts.append(html.Span(f'进仓Sum≥{target_sum}', style={
                **tag_style, 'color': '#4fc3f7', 'backgroundColor': '#4fc3f722'}))

        # 交易模式
        mode = state.get('trading_mode', '')
        if mode:
            mode_colors = {'auto': '#00FF88', 'day': '#4fc3f7', 'night': '#CE93D8'}
            row2_parts.append(html.Span(f'模式:{mode}', style={
                **tag_style, 'color': mode_colors.get(mode, '#aaa'),
                'backgroundColor': f'{mode_colors.get(mode, "#aaa")}22'}))

        row2_parts.append(html.Span('  ', style={'marginRight': '20px'}))

    if not row1_parts and not row2_parts:
        return html.Div()

    rows = []
    if row1_parts:
        rows.append(html.Div(row1_parts))
    if row2_parts:
        rows.append(html.Div(row2_parts, style={'marginTop': '4px'}))

    return html.Div(rows, style={
        'padding': '8px 20px',
        'backgroundColor': '#0d1117',
        'borderBottom': '1px solid #1a1a3e',
        'overflow': 'hidden',
    })


# 品种卖出风险画像 (基于B021回测: MAE=持仓过程中Sum最大逆向偏移)
_PRODUCT_RISK = {
    'SA': {'mae': 27,  'entry': (20, 60), 'exit': (0, 3),  'stars': 3, 'tag': '温和'},
    'CF': {'mae': 128, 'entry': (30, 60), 'exit': (0, 7),  'stars': 0, 'tag': '剧烈'},
    'FG': {'mae': 35,  'entry': (25, 60), 'exit': (0, 5),  'stars': 3, 'tag': '温和'},
    'TA': {'mae': 40,  'entry': (25, 60), 'exit': (0, 5),  'stars': 2, 'tag': '适中'},
    'P':  {'mae': 45,  'entry': (25, 60), 'exit': (0, 7),  'stars': 2, 'tag': '适中'},
    'AG': {'mae': 80,  'entry': (30, 60), 'exit': (0, 7),  'stars': 1, 'tag': '较高'},
    'AU': {'mae': 60,  'entry': (30, 60), 'exit': (0, 7),  'stars': 1, 'tag': '较高'},
    'CU': {'mae': 50,  'entry': (25, 60), 'exit': (0, 7),  'stars': 2, 'tag': '适中'},
    'M':  {'mae': 35,  'entry': (25, 60), 'exit': (0, 5),  'stars': 3, 'tag': '温和'},
    'SR': {'mae': 30,  'entry': (25, 60), 'exit': (0, 5),  'stars': 3, 'tag': '温和'},
    'RM': {'mae': 30,  'entry': (25, 60), 'exit': (0, 5),  'stars': 3, 'tag': '温和'},
}


def _estimate_dte(product, month_code):
    """从品种+合约月份估算DTE(日历天, 近似值)"""
    # 解析月份: '604' → 2026年4月, '2604' → 2026年4月
    mc = month_code.lstrip('0')
    if len(month_code) <= 3:
        yr = 2020 + int(month_code[0])
        mn = int(month_code[1:])
    elif len(month_code) == 4:
        yr = 2000 + int(month_code[:2])
        mn = int(month_code[2:])
    else:
        return None, None

    if mn < 1 or mn > 12:
        return None, None

    exchange = _EXCHANGE_MAP.get(product.upper())
    if not exchange:
        return None, None

    # 到期在交割月的前一个月
    exp_mn = mn - 1
    exp_yr = yr
    if exp_mn < 1:
        exp_mn = 12
        exp_yr -= 1

    approx_day = _EXPIRY_DAY.get(exchange, 15)

    # CZCE特殊品种(AP/CJ): 前两个月
    if exchange == 'CZCE_SP':
        exp_mn -= 1
        if exp_mn < 1:
            exp_mn = 12
            exp_yr -= 1
        approx_day = 25

    try:
        exp_date = date(exp_yr, exp_mn, min(abs(approx_day), 28))
        dte = (exp_date - date.today()).days
        return dte, exchange.replace('_SP', '')
    except (ValueError, OverflowError):
        return None, None


def _make_advisory_spans(call_sym, put_sym=None):
    """为期权对生成交易建议，返回 html.Span 列表（嵌入标题行，不新建 div）"""
    try:
        parsed = _parse_contract(call_sym)
        if not parsed:
            return []
        product, month, _, _ = parsed
        dte_result = _estimate_dte(product, month)
        # 兼容两种返回格式: int 或 (int, str)
        if isinstance(dte_result, tuple):
            dte, exchange = dte_result
        else:
            dte = dte_result
            exchange = _EXCHANGE_MAP.get(product.upper(), '?')
        if dte is None:
            return []

        risk = _PRODUCT_RISK.get(product.upper())
        spans = []

        # DTE + 交易所
        dte_color = '#FF4444' if dte <= 7 else '#FFD700' if dte <= 14 else '#aaa'
        spans.append(html.Span(f' DTE≈{dte}天({exchange}) ',
            style={'color': dte_color, 'fontSize': '11px', 'marginLeft': '12px'}))

        # 风险星级
        if risk:
            stars = '\u2605' * risk['stars'] + '\u2606' * (3 - risk['stars'])
            spans.append(html.Span(f'{stars}浮亏{risk["mae"]}%',
                style={'color': '#888', 'fontSize': '11px', 'marginLeft': '4px'}))

        # 策略建议 badge
        def _badge(name, text, color):
            return html.Span(f' {name}:{text} ',
                style={'color': '#000', 'backgroundColor': color, 'fontSize': '10px',
                       'fontWeight': 'bold', 'padding': '1px 4px', 'borderRadius': '3px',
                       'marginLeft': '6px'})

        r = risk or {'entry': (25, 60), 'exit': (0, 7)}
        ent_lo, ent_hi = r['entry']
        ext_lo, ext_hi = r['exit']

        if dte > ent_hi + 10:
            spans.append(_badge('卖', f'等DTE≤{ent_hi}', '#555'))
        elif dte >= ent_lo:
            spans.append(_badge('卖', f'可开仓', '#00FF88'))
        elif dte > ext_hi:
            spans.append(_badge('卖', 'Theta收割', '#FFD700'))
        elif dte > ext_lo:
            spans.append(_badge('卖', '准备平仓', '#00BFFF'))
        else:
            spans.append(_badge('卖', '到期平仓', '#FF6B6B'))

        today_wd = date.today().weekday()
        if dte <= 7:
            if today_wd == 4:
                spans.append(_badge('买', '周五最优89%', '#00FF88'))
            else:
                spans.append(_badge('买', '可做75%', '#FFD700'))
            spans.append(_badge('Scalp', '高Gamma', '#00FF88'))
        elif dte <= 14:
            spans.append(_badge('买', '胜率偏低', '#888'))
            spans.append(_badge('Scalp', '可做', '#FFD700'))

        # Volga 预警 badge（需要 put_sym 和 gamma_monitor）
        if put_sym and _HAS_GAMMA_MONITOR:
            try:
                pg = _gm_calc_greeks(call_sym, put_sym)
                if pg and pg.net_vega > 1e-10:
                    vv = pg.volga_vega_ratio
                    if vv > 30:
                        vcolor, vtxt = '#FF4444', f'V/V:{vv:.0f} 爆发!'
                    elif vv > 15:
                        vcolor, vtxt = '#FF8800', f'V/V:{vv:.0f} 偏高'
                    else:
                        vcolor, vtxt = '#00FF88', f'V/V:{vv:.0f}'
                    spans.append(html.Span(f' {vtxt} ',
                        style={'color': vcolor, 'fontSize': '10px', 'fontWeight': 'bold',
                               'marginLeft': '6px', 'border': f'1px solid {vcolor}',
                               'padding': '1px 4px', 'borderRadius': '3px'}))
            except Exception:
                pass

        return spans
    except Exception:
        return []


# ============ 配置持久化 ============

def load_config():
    """加载已保存的期权对列表"""
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return []


def save_config(pairs):
    """保存期权对列表"""
    with open(CONFIG_PATH, 'w') as f:
        json.dump(pairs, f, ensure_ascii=False, indent=2)


_TRADE_STATE_PATH = os.path.expanduser('~/Scripts/trade_state.json')


def _load_trade_state():
    """加载交易行选中状态"""
    try:
        if os.path.exists(_TRADE_STATE_PATH):
            with open(_TRADE_STATE_PATH, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_trade_state(state):
    """保存交易行选中状态"""
    try:
        with open(_TRADE_STATE_PATH, 'w') as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception:
        pass


# ============ 数据库连接（线程安全） ============
# 每个线程独立持有连接，避免多线程共享导致 SQLite SIGSEGV
_thread_local = threading.local()


def get_db():
    """获取当前线程的数据库连接（线程隔离，异常时自动重建）"""
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
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=10000")
        _thread_local.conn = conn
    return conn


# ============ 数据加载 ============

def get_trading_day_start():
    """获取数据加载起始时间：往前推10天，覆盖约7个交易日的历史数据"""
    now = datetime.now()
    d = (now - timedelta(days=10)).strftime('%Y-%m-%d')
    return f'{d} 21:00:00'


def _get_default_view_start():
    """获取默认显示起始时间：上一个夜盘开盘（21:00），即当前交易日"""
    now = datetime.now()
    if now.hour < 5:
        d = (now - timedelta(days=2)).strftime('%Y-%m-%d')
    elif now.weekday() == 0:
        d = (now - timedelta(days=3)).strftime('%Y-%m-%d')
    else:
        d = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    return f'{d} 21:00:00'


def _extract_futures_symbol(option_sym):
    """从期权 symbol 提取期货 symbol: ag2604C37600 -> ag2604, p2604-C-9000 -> p2604"""
    m = re.match(r'([a-zA-Z]+\d{3,4})[-CP]', option_sym)
    return m.group(1) if m else None


def _resolve_symbol(sym, cur):
    """自动匹配数据库中的实际 symbol 格式（处理大商所/广期所短横线格式）"""
    cur.execute("SELECT 1 FROM dbbardata WHERE symbol=? LIMIT 1", (sym,))
    if cur.fetchone():
        return sym
    # 尝试大商所格式: p2604C9000 -> p2604-C-9000
    m = re.match(r'([a-zA-Z]+\d{3,4})([CP])(\d+)', sym)
    if m:
        dash_sym = f'{m.group(1)}-{m.group(2)}-{m.group(3)}'
        cur.execute("SELECT 1 FROM dbbardata WHERE symbol=? LIMIT 1", (dash_sym,))
        if cur.fetchone():
            return dash_sym
    return sym


def load_pair_data(call_sym, put_sym):
    """加载一个期权对的数据（含期货价格）"""
    db = get_db()
    cur = db.cursor()
    day_start = get_trading_day_start()

    # 自动匹配数据库中的实际格式
    call_sym = _resolve_symbol(call_sym, cur)
    put_sym = _resolve_symbol(put_sym, cur)

    call_data = {}
    put_data = {}
    futures_data = {}

    for sym, store in [(call_sym, call_data), (put_sym, put_data)]:
        cur.execute("""
            SELECT datetime, close_price FROM dbbardata
            WHERE symbol=? AND datetime>=? ORDER BY datetime
        """, (sym, day_start))
        for dt_str, px in cur.fetchall():
            store[dt_str] = px

    # 加载期货价格
    futures_sym = _extract_futures_symbol(call_sym)
    if futures_sym:
        cur.execute("""
            SELECT datetime, close_price FROM dbbardata
            WHERE symbol=? AND datetime>=? ORDER BY datetime
        """, (futures_sym, day_start))
        for dt_str, px in cur.fetchall():
            futures_data[dt_str] = px

    def _night_before_day(dt_str):
        """排序键：夜盘减1天让它排在对应日盘之前，但当前夜盘不减（排在图表最右边）"""
        try:
            dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
            if dt.hour >= 20 or dt.hour < 5:
                # 当前正在进行的夜盘数据不减1天，让它排在今天日盘之后（图表最右边）
                if now.hour >= 20:
                    # 夜盘前半段(20:xx-23:59)：今天日期的夜盘数据是当前实时数据
                    if dt_str[:10] == now.strftime('%Y-%m-%d') and dt.hour >= 20:
                        return dt
                elif now.hour < 5:
                    # 夜盘后半段(0:00-4:59)：昨天20:xx+ 或 今天0:xx-4:xx 都是当前夜盘
                    yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
                    if (dt_str[:10] == yesterday and dt.hour >= 20) or \
                       (dt_str[:10] == now.strftime('%Y-%m-%d') and dt.hour < 5):
                        return dt
                dt -= timedelta(days=1)
            return dt
        except Exception:
            return datetime.min

    # 过滤异常时间戳数据：
    # 1. 15:05~20:55 之间不可能有交易（收盘后、夜盘开盘前），属于CTP异常写入
    # 2. 比当前时间还晚的"未来数据"（如周五夜盘被CTP标记为周一日期）
    now = datetime.now()
    def _is_bad_timestamp(dt_str):
        try:
            dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
            h = dt.hour
            m = dt.minute
            # 15:05~20:55 之间是非交易时段，过滤掉
            if (h == 15 and m >= 5) or (16 <= h <= 19) or (h == 20 and m < 55):
                return True
            # 过滤掉未来数据
            if dt > now + timedelta(minutes=5):
                return True
        except Exception:
            pass
        return False

    all_times = sorted(
        (t for t in set(call_data.keys()) | set(put_data.keys()) | set(futures_data.keys())
         if not _is_bad_timestamp(t)),
        key=_night_before_day
    )
    times, call_prices, put_prices, sum_prices, fut_prices = [], [], [], [], []
    last_c, last_p, last_f = None, None, None

    for t in all_times:
        c = call_data.get(t, last_c)
        p = put_data.get(t, last_p)
        f = futures_data.get(t, last_f)
        if c is not None:
            last_c = c
        if p is not None:
            last_p = p
        if f is not None:
            last_f = f
        if last_c is not None and last_p is not None:
            times.append(t)
            call_prices.append(last_c)
            put_prices.append(last_p)
            sum_prices.append(last_c + last_p)
            fut_prices.append(last_f)  # 可能为 None

    return times, call_prices, put_prices, sum_prices, fut_prices, futures_sym


def _aggregate_5min(times, prices):
    """将1分钟数据聚合为5分钟K线收盘价"""
    if not times:
        return []
    closes = []
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
                closes.append(cur_price)
            cur_bucket = bucket
        cur_price = p
    if cur_price is not None:
        closes.append(cur_price)
    return closes


def _calc_bollinger(closes_5min, period=26, multiplier=1.5):
    """计算布林带（基于5分钟K线收盘价），返回 (upper, middle, lower) 或 None"""
    if len(closes_5min) < period:
        return None
    recent = closes_5min[-period:]
    mid = sum(recent) / period
    variance = sum((x - mid) ** 2 for x in recent) / (period - 1)
    std = variance ** 0.5
    return mid + multiplier * std, mid, mid - multiplier * std


def _check_double_rise(times, call_prices, put_prices, sum_prices, price_tick=1):
    """检测价格之和突破布林上轨（与trade2026布林策略统一）
    条件：价格之和突破布林上轨(5分钟K线, 26周期, 1.5σ)
    不强制要求两腿同涨，由布林带自适应判断异常。
    """
    if len(times) < 2 or not call_prices or not put_prices:
        return {'alert': False}

    # 当前交易日起点（用于计算涨跌幅展示）
    session_start = _get_default_view_start()
    open_idx = None
    for i, t in enumerate(times):
        if t >= session_start:
            open_idx = i
            break
    if open_idx is None:
        return {'alert': False}

    c_open = call_prices[open_idx]
    p_open = put_prices[open_idx]
    c_now = call_prices[-1]
    p_now = put_prices[-1]
    s_now = sum_prices[-1]

    if c_open <= 0 or p_open <= 0:
        return {'alert': False}

    # 两腿失衡过滤：比例超过2倍的不报警（失衡组合无交易价值）
    leg_ratio = max(c_now, p_now) / min(c_now, p_now) if min(c_now, p_now) > 0 else 999
    if leg_ratio > 2:
        return {'alert': False}

    c_chg = (c_now - c_open) / c_open
    p_chg = (p_now - p_open) / p_open

    # 布林通道判断：价格之和的5分钟K线是否突破上轨（26周期, 1.5σ，与trade2026一致）
    closes_5min = _aggregate_5min(times, sum_prices)
    boll = _calc_bollinger(closes_5min, period=26, multiplier=1.5)
    if boll is None:
        return {'alert': False}

    upper, middle, lower = boll
    if s_now > upper:
        # 噪音过滤：突破幅度不到2个tick的忽略（盘口跳动不是真双涨）
        breach_ticks = (s_now - upper) / price_tick if price_tick > 0 else 999
        if breach_ticks < 2:
            return {'alert': False, 'boll_middle': middle}
        breach_pct = (s_now - middle) / middle if middle > 0 else 0
        return {
            'alert': True,
            'call_chg': c_chg,
            'put_chg': p_chg,
            'sum_chg': breach_pct,
            'boll_upper': upper,
            'boll_middle': middle,
            'method': 'bollinger',
        }
    return {'alert': False, 'boll_middle': middle}


# ============ 预警历史记录 & 回归追踪 ============
# 设计原则：
#   1. 预警触发时立即写入文件（不丢数据）
#   2. 回归和未回归都永久记录（无幸存者偏差）
#   3. 重启时从文件恢复活跃预警（不丢失未回归样本）
#   4. 双写：JSON（兼容）+ SQLite（长期分析/回测）
# 存储位置：
#   JSON: ~/Scripts/alert_history.json（工作台实时读写）
#   SQLite: ~/Scripts/alert_history.db（长期归档，方便SQL查询和回测）

import sqlite3 as _alert_sqlite3

def _init_alert_db():
    """初始化 SQLite 预警数据库"""
    try:
        conn = _alert_sqlite3.connect(ALERT_DB_PATH)
        conn.execute('''CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            call_sym TEXT NOT NULL,
            put_sym TEXT NOT NULL,
            futures_sym TEXT,
            alert_time TEXT NOT NULL,
            alert_sum REAL,
            boll_upper REAL,
            boll_middle REAL,
            alert_deviation REAL,
            call_chg REAL,
            put_chg REAL,
            peak_sum REAL,
            peak_time TEXT,
            peak_deviation REAL,
            last_sum REAL,
            last_update TEXT,
            resolved INTEGER DEFAULT 0,
            resolve_time TEXT,
            resolve_sum REAL,
            resolve_minutes REAL
        )''')
        conn.execute('''CREATE INDEX IF NOT EXISTS idx_alerts_time
                        ON alerts(alert_time)''')
        conn.execute('''CREATE INDEX IF NOT EXISTS idx_alerts_product
                        ON alerts(futures_sym)''')
        conn.execute('''CREATE INDEX IF NOT EXISTS idx_alerts_resolved
                        ON alerts(resolved)''')
        conn.commit()
        conn.close()
    except Exception as e:
        print(f'[alert_db] 初始化失败: {e}')

_init_alert_db()


def _alert_db_insert(rec):
    """插入一条预警到 SQLite"""
    try:
        conn = _alert_sqlite3.connect(ALERT_DB_PATH)
        conn.execute('''INSERT INTO alerts
            (call_sym, put_sym, futures_sym, alert_time, alert_sum,
             boll_upper, boll_middle, alert_deviation, call_chg, put_chg,
             peak_sum, peak_time, peak_deviation, last_sum, last_update,
             resolved, resolve_time, resolve_sum, resolve_minutes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
            rec.get('call_sym'), rec.get('put_sym'), rec.get('futures_sym'),
            rec.get('alert_time'), rec.get('alert_sum'),
            rec.get('boll_upper'), rec.get('boll_middle'), rec.get('alert_deviation'),
            rec.get('call_chg'), rec.get('put_chg'),
            rec.get('peak_sum'), rec.get('peak_time'), rec.get('peak_deviation'),
            rec.get('last_sum'), rec.get('last_update'),
            1 if rec.get('resolved') else 0,
            rec.get('resolve_time'), rec.get('resolve_sum'), rec.get('resolve_minutes'),
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f'[alert_db] 插入失败: {e}')


def _alert_db_update(call_sym, put_sym, alert_time, updates):
    """更新 SQLite 中的预警记录"""
    try:
        conn = _alert_sqlite3.connect(ALERT_DB_PATH)
        set_clauses = []
        values = []
        for k, v in updates.items():
            if k == 'resolved':
                set_clauses.append('resolved = ?')
                values.append(1 if v else 0)
            else:
                set_clauses.append(f'{k} = ?')
                values.append(v)
        values.extend([call_sym, put_sym, alert_time])
        conn.execute(f'''UPDATE alerts SET {", ".join(set_clauses)}
                        WHERE call_sym = ? AND put_sym = ? AND alert_time = ?''',
                     values)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f'[alert_db] 更新失败: {e}')


def _migrate_json_to_db():
    """将现有 JSON 历史迁移到 SQLite（幂等，只迁移不存在的记录）"""
    history = _load_alert_history()
    if not history:
        return
    try:
        conn = _alert_sqlite3.connect(ALERT_DB_PATH)
        existing = conn.execute('SELECT COUNT(*) FROM alerts').fetchone()[0]
        if existing >= len(history):
            conn.close()
            return  # 已迁移过
        for rec in history:
            # 检查是否已存在
            row = conn.execute(
                'SELECT id FROM alerts WHERE call_sym=? AND put_sym=? AND alert_time=?',
                (rec.get('call_sym'), rec.get('put_sym'), rec.get('alert_time'))
            ).fetchone()
            if row:
                continue
            conn.execute('''INSERT INTO alerts
                (call_sym, put_sym, futures_sym, alert_time, alert_sum,
                 boll_upper, boll_middle, alert_deviation, call_chg, put_chg,
                 peak_sum, peak_time, peak_deviation, last_sum, last_update,
                 resolved, resolve_time, resolve_sum, resolve_minutes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
                rec.get('call_sym'), rec.get('put_sym'), rec.get('futures_sym'),
                rec.get('alert_time'), rec.get('alert_sum'),
                rec.get('boll_upper'), rec.get('boll_middle'), rec.get('alert_deviation'),
                rec.get('call_chg'), rec.get('put_chg'),
                rec.get('peak_sum'), rec.get('peak_time'), rec.get('peak_deviation'),
                rec.get('last_sum'), rec.get('last_update'),
                1 if rec.get('resolved') else 0,
                rec.get('resolve_time'), rec.get('resolve_sum'), rec.get('resolve_minutes'),
            ))
        conn.commit()
        conn.close()
        print(f'[alert_db] 已迁移 {len(history)} 条JSON记录到SQLite')
    except Exception as e:
        print(f'[alert_db] 迁移失败: {e}')


def _load_alert_history():
    """加载预警历史"""
    if os.path.exists(ALERT_HISTORY_PATH):
        try:
            with open(ALERT_HISTORY_PATH, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return []


def _save_alert_history(history):
    """保存预警历史"""
    try:
        with open(ALERT_HISTORY_PATH, 'w') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f'[alert_history] 保存失败: {e}')


def _update_record_in_history(key, updates):
    """更新历史文件中指定记录的字段"""
    history = _load_alert_history()
    for rec in history:
        rec_key = f'{rec.get("call_sym", "")}|{rec.get("put_sym", "")}'
        if rec_key == key and not rec.get('resolved'):
            rec.update(updates)
            break
    _save_alert_history(history)


_alert_active = {}  # 内存中跟踪当前活跃的预警 {key: record}


def _init_active_alerts():
    """启动时从文件恢复未回归的活跃预警"""
    global _alert_active
    history = _load_alert_history()
    for rec in history:
        if not rec.get('resolved'):
            key = f'{rec.get("call_sym", "")}|{rec.get("put_sym", "")}'
            _alert_active[key] = rec
    if _alert_active:
        print(f'[alert_history] 恢复 {len(_alert_active)} 个活跃预警')


# 启动时恢复 + 迁移历史数据到SQLite
_init_active_alerts()
_migrate_json_to_db()


def record_alert(call_sym, put_sym, futures_sym, dr_info, sum_now, boll_middle):
    """记录新预警或更新活跃预警的峰值"""
    key = f'{call_sym}|{put_sym}'
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if key in _alert_active:
        # 已有活跃预警，更新峰值
        rec = _alert_active[key]
        if sum_now > rec.get('peak_sum', 0):
            rec['peak_sum'] = sum_now
            rec['peak_time'] = now_str
            rec['peak_deviation'] = (sum_now - boll_middle) / boll_middle if boll_middle > 0 else 0
            rec['last_sum'] = sum_now
            rec['last_update'] = now_str
            # 同步更新文件和SQLite中的峰值
            peak_updates = {
                'peak_sum': rec['peak_sum'], 'peak_time': now_str,
                'peak_deviation': rec['peak_deviation'],
                'last_sum': sum_now, 'last_update': now_str,
            }
            _update_record_in_history(key, peak_updates)
            _alert_db_update(rec['call_sym'], rec['put_sym'], rec['alert_time'], peak_updates)
        return

    # 新预警 → 立即写入文件
    rec = {
        'call_sym': call_sym,
        'put_sym': put_sym,
        'futures_sym': futures_sym or '?',
        'alert_time': now_str,
        'alert_sum': sum_now,
        'boll_upper': dr_info.get('boll_upper', 0),
        'boll_middle': boll_middle,
        'alert_deviation': (sum_now - boll_middle) / boll_middle if boll_middle > 0 else 0,
        'call_chg': dr_info.get('call_chg', 0),
        'put_chg': dr_info.get('put_chg', 0),
        'peak_sum': sum_now,
        'peak_time': now_str,
        'peak_deviation': (sum_now - boll_middle) / boll_middle if boll_middle > 0 else 0,
        'last_sum': sum_now,
        'last_update': now_str,
        'resolved': False,
        'resolve_time': None,
        'resolve_sum': None,
        'resolve_minutes': None,
    }
    _alert_active[key] = rec

    # 立即持久化（JSON + SQLite 双写）
    history = _load_alert_history()
    history.append(rec)
    _save_alert_history(history)
    _alert_db_insert(rec)
    print(f'[alert_history] 新预警(已存盘): {futures_sym} {call_sym}+{put_sym} sum={sum_now:.1f} upper={dr_info.get("boll_upper", 0):.1f}')


def check_alert_resolved(call_sym, put_sym, sum_now, boll_middle):
    """检查活跃预警是否已回归均值（价格之和回到布林中轨附近）"""
    key = f'{call_sym}|{put_sym}'
    if key not in _alert_active:
        return
    rec = _alert_active[key]
    if rec['resolved']:
        return

    # 更新最后已知价格（无论是否回归）
    rec['last_sum'] = sum_now
    rec['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 回归条件：价格之和 <= 布林中轨（真正回到均值）
    if sum_now <= boll_middle:
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        rec['resolved'] = True
        rec['resolve_time'] = now_str
        rec['resolve_sum'] = sum_now
        # 计算回归耗时（分钟）
        try:
            t0 = datetime.strptime(rec['alert_time'], '%Y-%m-%d %H:%M:%S')
            t1 = datetime.strptime(now_str, '%Y-%m-%d %H:%M:%S')
            rec['resolve_minutes'] = (t1 - t0).total_seconds() / 60
        except Exception:
            rec['resolve_minutes'] = None

        # 更新文件和SQLite中对应记录
        resolve_updates = {
            'resolved': True, 'resolve_time': now_str,
            'resolve_sum': sum_now, 'resolve_minutes': rec['resolve_minutes'],
            'last_sum': sum_now, 'last_update': now_str,
        }
        _update_record_in_history(key, resolve_updates)
        _alert_db_update(rec['call_sym'], rec['put_sym'], rec['alert_time'], resolve_updates)
        del _alert_active[key]
        print(f'[alert_history] 回归: {rec["futures_sym"]} {rec["resolve_minutes"]:.0f}分钟 '
              f'sum {rec["alert_sum"]:.1f} -> {sum_now:.1f}')


def get_alert_stats():
    """统计预警数据（含回归+未回归，无幸存者偏差）"""
    history = _load_alert_history()
    if not history:
        return None

    resolved = [r for r in history if r.get('resolved') and r.get('resolve_minutes') is not None]
    unresolved = [r for r in history if not r.get('resolved')]

    stats = {
        'total': len(history),
        'resolved': len(resolved),
        'unresolved': len(unresolved),
        'resolve_rate': len(resolved) / len(history) * 100 if history else 0,
        'active': len(_alert_active),
    }

    # 回归样本统计
    if resolved:
        minutes_list = sorted(r['resolve_minutes'] for r in resolved)
        deviations = [r.get('alert_deviation', 0) * 100 for r in resolved]
        peak_devs = [r.get('peak_deviation', 0) * 100 for r in resolved]
        stats.update({
            'avg_minutes': sum(minutes_list) / len(minutes_list),
            'median_minutes': minutes_list[len(minutes_list) // 2],
            'p25_minutes': minutes_list[int(len(minutes_list) * 0.25)] if len(minutes_list) >= 4 else minutes_list[0],
            'p75_minutes': minutes_list[int(len(minutes_list) * 0.75)] if len(minutes_list) >= 4 else minutes_list[-1],
            'min_minutes': minutes_list[0],
            'max_minutes': minutes_list[-1],
            'avg_deviation': sum(deviations) / len(deviations),
            'avg_peak_deviation': sum(peak_devs) / len(peak_devs),
        })

    # 未回归样本统计
    if unresolved:
        ur_devs = [r.get('peak_deviation', 0) * 100 for r in unresolved]
        stats['unresolved_avg_peak_deviation'] = sum(ur_devs) / len(ur_devs)

    # 按品种分组（全部样本）
    by_product = {}
    for r in history:
        prod = r.get('futures_sym', '?')
        if prod not in by_product:
            by_product[prod] = {'resolved': [], 'unresolved': []}
        if r.get('resolved'):
            by_product[prod]['resolved'].append(r)
        else:
            by_product[prod]['unresolved'].append(r)

    stats['by_product'] = {prod: {
        'total': len(data['resolved']) + len(data['unresolved']),
        'resolved': len(data['resolved']),
        'unresolved': len(data['unresolved']),
        'resolve_rate': len(data['resolved']) / (len(data['resolved']) + len(data['unresolved'])) * 100,
        'avg_min': (sum(r['resolve_minutes'] for r in data['resolved']) / len(data['resolved']))
                   if data['resolved'] else None,
        'median_min': (sorted(r['resolve_minutes'] for r in data['resolved'])[len(data['resolved']) // 2])
                      if data['resolved'] else None,
    } for prod, data in by_product.items()}

    # 最近记录（全部，不只回归的）
    stats['recent'] = history[-30:][::-1]
    return stats


def build_figure(call_sym, put_sym, call_coeff=1.0, put_coeff=1.0):
    """为一个期权对构建图表（双Y轴：左=期权价格，右=期货价格）"""
    times, call_prices, put_prices, _, fut_prices, futures_sym = \
        load_pair_data(call_sym, put_sym)

    # 应用系数
    if call_coeff != 1.0:
        call_prices = [p * call_coeff for p in call_prices]
    if put_coeff != 1.0:
        put_prices = [p * put_coeff for p in put_prices]
    sum_prices = [c + p for c, p in zip(call_prices, put_prices)]

    weighted = call_coeff != 1.0 or put_coeff != 1.0
    call_label = f'{call_sym}×{call_coeff:g}' if call_coeff != 1.0 else call_sym
    put_label = f'{put_sym}×{put_coeff:g}' if put_coeff != 1.0 else put_sym
    sum_label = '价格之和(加权)' if weighted else '价格之和'

    fig = go.Figure()

    # 左Y轴: 期权价格
    fig.add_trace(go.Scatter(
        x=times, y=sum_prices,
        name=sum_label,
        line=dict(color='#FFD700', width=3),
        yaxis='y',
        hovertemplate=sum_label + ': %{y:.1f}<extra></extra>'
    ))

    fig.add_trace(go.Scatter(
        x=times, y=call_prices,
        name=call_label,
        line=dict(color='#00BFFF', width=1.5, dash='dot'),
        yaxis='y',
        hovertemplate=call_label + ': %{y:.1f}<extra></extra>'
    ))

    fig.add_trace(go.Scatter(
        x=times, y=put_prices,
        name=put_label,
        line=dict(color='#FF6B6B', width=1.5, dash='dot'),
        yaxis='y',
        hovertemplate=put_label + ': %{y:.1f}<extra></extra>'
    ))

    # 右Y轴: 期货价格
    has_futures = any(p is not None for p in fut_prices)
    if has_futures and futures_sym:
        fig.add_trace(go.Scatter(
            x=times, y=fut_prices,
            name=futures_sym,
            line=dict(color='#00FF88', width=2),
            yaxis='y2',
            hovertemplate=futures_sym + ': %{y:.0f}<extra></extra>'
        ))

    # 计算默认显示范围（当前交易日）
    default_view_start = _get_default_view_start()
    default_start_idx = 0
    if times:
        for idx, t in enumerate(times):
            if t >= default_view_start:
                default_start_idx = idx
                break

    # 生成刻度标签（每30个点≈30分钟一个，覆盖全部数据供平移时使用）
    tick_vals, tick_text = [], []
    if times:
        n = len(times)
        step = 30
        prev_date = ''
        for i in range(0, n, step):
            tick_vals.append(times[i])
            try:
                t = datetime.strptime(times[i], '%Y-%m-%d %H:%M:%S')
                d = t.strftime('%m-%d')
                label = (d + ' ' if d != prev_date else '') + t.strftime('%H:%M')
                prev_date = d
            except Exception:
                label = times[i][-8:-3]
            tick_text.append(label)

    layout_kwargs = dict(
        template='plotly_dark',
        paper_bgcolor='#1a1a2e',
        plot_bgcolor='#16213e',
        height=400,
        hovermode='x',
        dragmode='pan',  # 拖拽平移，左右拖动查看历史数据
        uirevision='constant',  # 保持用户的平移/缩放状态不被刷新重置
        margin=dict(l=60, r=70, t=40, b=60),
        xaxis=dict(
            type='category',
            tickmode='array',
            tickvals=tick_vals,
            ticktext=tick_text,
            gridcolor='#2a2a4a',
            showspikes=True, spikemode='across', spikesnap='cursor',
            spikethickness=1, spikecolor='#888', spikedash='solid',
            range=[default_start_idx - 0.5, len(times) - 0.5] if times else None,
        ),
        yaxis=dict(
            title='期权价格',
            title_font=dict(color='#FFD700'),
            tickfont=dict(color='#ddd'),
            gridcolor='#2a2a4a',
            nticks=10,  # 保证足够密的刻度，低价期权也能清晰显示
            showspikes=True, spikemode='across',
            spikethickness=1, spikecolor='#555', spikedash='dot',
        ),
        legend=dict(
            orientation='h', x=0.5, xanchor='center', y=-0.18,
            font=dict(color='#ddd', size=11)
        ),
    )

    if has_futures:
        layout_kwargs['yaxis2'] = dict(
            title='期货价格',
            title_font=dict(color='#00FF88'),
            tickfont=dict(color='#00FF88'),
            overlaying='y',
            side='right',
            showgrid=False,
        )

    fig.update_layout(**layout_kwargs)

    latest_sum = sum_prices[-1] if sum_prices else None
    call_last = call_prices[-1] if call_prices else 0
    put_last = put_prices[-1] if put_prices else 0
    leg_ratio = max(call_last, put_last) / min(call_last, put_last) if min(call_last, put_last) > 0 else 999
    product = _extract_product(call_sym)
    tick = _PRICE_TICK.get(product, 1)
    dr = _check_double_rise(times, call_prices, put_prices, sum_prices, price_tick=tick)
    return fig, {'sum': latest_sum, 'futures_sym': futures_sym, 'double_rise': dr,
                 'call_last': call_last, 'put_last': put_last, 'leg_ratio': leg_ratio}


# ============ 解析输入 ============

_ZHENGZHOU_PRODUCTS = {'SA', 'FG', 'TA', 'MA', 'OI', 'RM', 'AP',
                       'CF', 'CJ', 'SR', 'PK', 'SM', 'SF', 'SH', 'UR', 'PF'}
_DCE_PRODUCTS = {'P', 'EB', 'I', 'LG', 'LH', 'PG', 'V', 'Y', 'L', 'PP', 'M', 'C', 'CS', 'A', 'B', 'JD', 'J', 'JM', 'FB', 'BB', 'RR', 'EG'}


def normalize_symbol(text):
    """规范化单个期权合约代码，大商所自动加连字符
    'AG2604C37600' -> 'ag2604C37600'
    'p2604C9000' -> 'p2604-C-9000' (DCE自动加-)
    'p2604-C-9000' -> 'p2604-C-9000' (已有-保持)
    """
    text = text.strip().upper().replace('_', '').replace(' ', '')
    if not text:
        return None
    # 先去掉连字符统一解析
    clean = text.replace('-', '')
    m = re.match(r'([A-Z]{1,4})(\d{3,4})([CP])(\d+)$', clean)
    if not m:
        return None
    prod, month, cp, strike = m.group(1), m.group(2), m.group(3), m.group(4)
    # 大小写规则
    if prod not in _ZHENGZHOU_PRODUCTS:
        prod = prod.lower()
    # 大商所期权格式: p2604-C-9500
    if prod.upper() in _DCE_PRODUCTS:
        return f'{prod}{month}-{cp}-{strike}'
    return f'{prod}{month}{cp}{strike}'


# ============ 自动选对 ============

def _parse_contract(sym):
    """解析合约代码 -> (product, month, opt_type, strike) 或 None"""
    # DCE格式: p2604-C-9000
    m = re.match(r'^([a-zA-Z]+)(\d{3,4})-([CP])-(\d+)$', sym)
    if m:
        return m.group(1).upper(), m.group(2), m.group(3), int(m.group(4))
    # 标准期权: ag2604C37600, TA604C12000
    m = re.match(r'^([a-zA-Z]+)(\d{3,4})([CP])(\d+)$', sym)
    if m:
        return m.group(1).upper(), m.group(2), m.group(3), int(m.group(4))
    # 期货: ag2604, TA604, l2604F
    m = re.match(r'^([a-zA-Z]+)(\d{3,4})F?$', sym)
    if m:
        return m.group(1).upper(), m.group(2), None, None
    return None


# 品种行权价间隔打分参数 (optimal_min, optimal_max, max_reasonable)
_auto_cache = {'pairs': [], 'ts': None}
_v6_recommended_pairs = []  # 今日计划精选的OTM宽跨推荐，由toggle_plan填充


def _score_pair(cp, pp, cs, ps, fp, cv, pv, strike_params=None):
    """计算期权对得分 (0-10)
    核心原则：两腿权利金价格越接近越好 + 瓶颈腿成交额
    """
    import math as _m
    if cp <= 0 or pp <= 0 or fp <= 0:
        return 0
    # OTM过滤
    c_otm = (cs - fp) / fp
    p_otm = (fp - ps) / fp
    if c_otm < 0.02 or p_otm < 0.02:
        return 0  # 太接近ATM
    if c_otm > 0.15 or p_otm > 0.15:
        return 0  # 太深度虚值
    # 1. 价格对称性 (50%) — 两腿权利金越接近越好
    ratio = max(cp, pp) / min(cp, pp)
    if ratio > 2.5:
        return 0  # 差距太大，淘汰
    price_sym = max(0, 1.0 - (ratio - 1.0) * 0.6)  # 1.0→1.0, 1.5→0.7, 2.0→0.4
    # 2. 瓶颈腿成交额 (30%) — 价格×成交量，log scale
    min_turnover = min(cp * cv, pp * pv)
    vol_score = max(0, min((_m.log10(max(min_turnover, 10)) - 3.5) / 1.5, 1.0))
    # 3. 单腿最低权利金 (20%) — 两腿都要有足够的权利金收入
    min_px_score = 1.0 if min(cp, pp) >= 4 else 0.5 if min(cp, pp) >= 2 else 0.1
    return (price_sym * 0.50 + vol_score * 0.30 + min_px_score * 0.20) * 10


def auto_select_pairs():
    """自动为每个品种选出得分最高的期权对 (5分钟缓存)"""
    now = datetime.now()
    if _auto_cache['ts'] and (now - _auto_cache['ts']).total_seconds() < 300:
        return _auto_cache['pairs']

    db = get_db()
    cur = db.cursor()
    recent = (now - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')

    # 获取每个合约最新价格和成交量
    cur.execute("""
        SELECT b.symbol, b.close_price, b.volume
        FROM dbbardata b
        INNER JOIN (
            SELECT symbol, MAX(datetime) as max_dt
            FROM dbbardata WHERE datetime >= ?
            GROUP BY symbol
        ) m ON b.symbol = m.symbol AND b.datetime = m.max_dt
    """, (recent,))
    latest = {}
    for sym, price, vol in cur.fetchall():
        latest[sym] = (price, vol or 0)

    # 解析并分组: {(product, month): {futures, calls, puts}}
    groups = {}
    for sym, (price, vol) in latest.items():
        parsed = _parse_contract(sym)
        if not parsed:
            continue
        prod, month, opt_type, strike = parsed
        key = (prod, month)
        if key not in groups:
            groups[key] = {'futures': None, 'calls': [], 'puts': []}
        if opt_type is None:
            groups[key]['futures'] = (sym, price)
        elif opt_type == 'C':
            groups[key]['calls'].append((sym, price, vol, strike))
        elif opt_type == 'P':
            groups[key]['puts'].append((sym, price, vol, strike))

    # 每个品种选最近月份的最优期权对
    product_months = {}
    for (prod, month), data in groups.items():
        if not data['futures'] or not data['calls'] or not data['puts']:
            continue
        product_months.setdefault(prod, []).append((month, data))

    result = []
    for prod, months in product_months.items():
        months.sort(key=lambda x: x[0])
        month, data = months[0]
        fp_sym, fp = data['futures']
        otm_calls = [(s, p, v, k) for s, p, v, k in data['calls'] if k > fp and p > 0]
        otm_puts = [(s, p, v, k) for s, p, v, k in data['puts'] if k < fp and p > 0]
        if not otm_calls or not otm_puts:
            continue

        # 按虚值度排序，只搜索前10档
        otm_calls.sort(key=lambda x: x[3] - fp)
        otm_puts.sort(key=lambda x: fp - x[3])

        best = None
        for c_sym, c_px, c_vol, c_k in otm_calls[:10]:
            for p_sym, p_px, p_vol, p_k in otm_puts[:10]:
                if c_px + p_px < 8 or min(c_px, p_px) < 2:
                    continue
                score = _score_pair(c_px, p_px, c_k, p_k, fp, c_vol, p_vol)
                if score > 0 and (best is None or score > best[0]):
                    best = (score, c_sym, p_sym, c_px + p_px)

        if best:
            score, c_sym, p_sym, psum = best
            # 数据量检查：至少需要130条1分钟数据（≈26根5分钟K线）才能算布林带
            min_bars = 130
            cur.execute("SELECT COUNT(*) FROM dbbardata WHERE symbol=?", (c_sym,))
            c_cnt = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM dbbardata WHERE symbol=?", (p_sym,))
            p_cnt = cur.fetchone()[0]
            if min(c_cnt, p_cnt) < min_bars:
                continue  # 数据不足，跳过
            result.append({
                'call': c_sym, 'put': p_sym,
                'product': prod, 'month': month,
                'score': score, 'price_sum': psum,
                'futures_sym': fp_sym,
            })

    result.sort(key=lambda x: x['score'], reverse=True)
    _auto_cache['pairs'] = result
    _auto_cache['ts'] = now
    return result


# ============ VRP 扫描器 ============

import numpy as np
from scipy.stats import norm as _norm

# B023 Tier评级 (品种→Sharpe)
_TIER_SHARPE = {
    'JD':1.028,'SN':0.966,'PS':0.904,'OI':0.808,'LG':0.783,'LH':0.737,
    'AO':0.660,'AP':0.651,'PK':0.634,'SR':0.633,'CJ':0.628,'SM':0.578,'BR':0.532,
    'FG':0.477,'SA':0.470,'A':0.456,'LC':0.445,'PF':0.409,'C':0.403,'SI':0.387,
    'NI':0.381,'CF':0.366,'Y':0.366,'AU':0.349,'SH':0.318,'MA':0.310,'CS':0.305,
    'PX':0.301,'ZC':0.301,
    'SF':0.289,'UR':0.271,'CU':0.267,'M':0.257,'ZN':0.248,'TA':0.246,
    'AG':0.223,'B':0.217,'PB':0.197,'RU':0.157,'SC':0.147,'RM':0.125,'AL':0.106,'EG':0.101,
    'RB':0.097,'EB':0.080,'PP':0.011,'V':-0.003,'P':-0.004,
}

# 合约乘数
_MULTIPLIER = {
    'AG':15,'AU':1000,'CU':5,'AL':5,'ZN':5,'PB':5,'NI':1,'SN':1,'RU':10,'FU':10,
    'BU':10,'SP':5,'AO':20,'RB':10,'BR':5,
    'P':10,'M':10,'Y':10,'I':100,'JM':60,'JD':10,'LH':16,'PP':5,'L':5,'V':5,
    'EB':5,'EG':10,'C':10,'CS':10,'B':10,'LG':20,
    'CF':5,'SA':20,'FG':20,'TA':5,'MA':10,'SR':10,'OI':10,'RM':10,'SM':5,'SF':5,
    'PK':5,'SH':30,'UR':20,'PF':5,'AP':10,'CJ':5,'PX':5,'ZC':100,
    'SI':5,'LC':1,'PS':5,
    'SC':1000,'BZ':100,
}

# 保证金率（各品种期货保证金比例，用于期权卖出保证金计算）
_MARGIN_RATE = {
    'AG': 0.12, 'AU': 0.10, 'CU': 0.12, 'AL': 0.10, 'ZN': 0.10, 'PB': 0.10,
    'NI': 0.12, 'SN': 0.12, 'RU': 0.12, 'FU': 0.12, 'BU': 0.12, 'SP': 0.10,
    'AO': 0.12, 'RB': 0.10, 'BR': 0.10,
    'P': 0.10, 'M': 0.10, 'Y': 0.10, 'I': 0.13, 'JM': 0.12, 'JD': 0.10,
    'LH': 0.12, 'PP': 0.10, 'L': 0.10, 'V': 0.10, 'EB': 0.12, 'EG': 0.10,
    'C': 0.10, 'CS': 0.10, 'B': 0.10, 'LG': 0.12,
    'CF': 0.10, 'SA': 0.10, 'FG': 0.10, 'TA': 0.10, 'MA': 0.10, 'SR': 0.10,
    'OI': 0.10, 'RM': 0.10, 'SM': 0.10, 'SF': 0.10, 'PK': 0.10, 'SH': 0.10,
    'UR': 0.10, 'PF': 0.10, 'AP': 0.10, 'CJ': 0.10, 'PX': 0.10, 'ZC': 0.10,
    'SI': 0.12, 'LC': 0.14, 'PS': 0.10,
    'SC': 0.13, 'BZ': 0.12,
}


def _calc_strangle_margin(call_px, put_px, call_k, put_k, fut_px, mult, mgn_rate):
    """计算卖出宽跨式保证金（中国期货期权标准公式）

    单腿卖出保证金 = 权利金 + max(期货保证金 - 虚值额/2, 期货保证金/2)
    宽跨式保证金 = max(Call保证金, Put保证金) + min(Call权利金, Put权利金)
    """
    fut_margin = fut_px * mult * mgn_rate  # 标的期货保证金
    # Call虚值额 = max(行权价 - 期货价, 0) × 合约乘数
    otm_c = max(call_k - fut_px, 0) * mult
    # Put虚值额 = max(期货价 - 行权价, 0) × 合约乘数
    otm_p = max(fut_px - put_k, 0) * mult
    premium_c = call_px * mult
    premium_p = put_px * mult
    call_margin = premium_c + max(fut_margin - otm_c / 2, fut_margin / 2)
    put_margin = premium_p + max(fut_margin - otm_p / 2, fut_margin / 2)
    strangle_margin = max(call_margin, put_margin) + min(premium_c, premium_p)
    return strangle_margin


# === v6 日内宽跨参数 (B033/B035/B037回测, 模块级共享) ===
_DTE_RANGES = [('DTE≤7', 0, 7), ('DTE8-14', 8, 14), ('DTE15-30', 15, 30),
               ('DTE31-60', 31, 60), ('DTE>60', 61, 999)]

_ENTRY_LABELS = {'night_0': 'N21:00', 'night_30': 'N21:30', 'night_60': 'N22:00',
                 'day_0': 'D09:00', 'day_30': 'D09:30'}

_V6_CONFIG = {
    'A':  {'name': '豆一',   'ex': 'DCE',  'months': list(range(1,13)),
           'DTE8-14':  {'entry': 'night_30','tp': 0.8, 'sl': 999,  'sharpe': 14.41, 'wr': 82.8, 'n': 29},
           'DTE15-30': {'entry': 'day_0',   'tp': 0.5, 'sl': 3.0,  'sharpe': 21.64, 'wr': 87.0, 'n': 23},
           'DTE31-60': {'entry': 'night_0', 'tp': 0.8, 'sl': 999,  'sharpe': 15.36, 'wr': 89.5, 'n': 114},
           'DTE>60':   {'entry': 'day_30',  'tp': 1.0, 'sl': 2.0,  'sharpe': 25.72, 'wr': 87.5, 'n': 24}},
    'AG': {'name': '白银',   'ex': 'SHFE', 'months': list(range(1,13)),
           'DTE15-30': {'entry': 'night_30','tp': 0.5, 'sl': 4.0,  'sharpe': 5.88,  'wr': 73.9, 'n': 69},
           'DTE31-60': {'entry': 'night_60','tp': 0.5, 'sl': 3.5,  'sharpe': 8.73,  'wr': 86.1, 'n': 122},
           'DTE>60':   {'entry': 'night_30','tp': 0.5, 'sl': 2.5,  'sharpe': 21.86, 'wr': 97.0, 'n': 66}},
    'AL': {'name': '铝',     'ex': 'SHFE', 'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'day_0',   'tp': 0.5, 'sl': 999,  'sharpe': 9.16,  'wr': 85.5, 'n': 62},
           'DTE8-14':  {'entry': 'night_0', 'tp': 0.3, 'sl': 999,  'sharpe': 17.42, 'wr': 92.3, 'n': 39},
           'DTE15-30': {'entry': 'night_0', 'tp': 0.5, 'sl': 999,  'sharpe': 17.19, 'wr': 88.3, 'n': 111},
           'DTE31-60': {'entry': 'night_30','tp': 0.5, 'sl': 3.5,  'sharpe': 26.99, 'wr': 90.9, 'n': 22}},
    'AP': {'name': '苹果',   'ex': 'CZCE', 'months': [1,3,5,10,11],
           'DTE15-30': {'entry': 'day_30',  'tp': 0.3, 'sl': 999,  'sharpe': 21.21, 'wr': 100., 'n': 41},
           'DTE31-60': {'entry': 'day_30',  'tp': 0.3, 'sl': 3.0,  'sharpe': 19.75, 'wr': 95.8, 'n': 24}},
    'AU': {'name': '黄金',   'ex': 'SHFE', 'months': list(range(1,13)),
           'DTE15-30': {'entry': 'night_60','tp': 0.5, 'sl': 3.5,  'sharpe': 13.49, 'wr': 87.5, 'n': 88},
           'DTE31-60': {'entry': 'night_60','tp': 0.3, 'sl': 3.5,  'sharpe': 20.91, 'wr': 95.4, 'n': 197},
           'DTE>60':   {'entry': 'night_30','tp': 0.5, 'sl': 3.5,  'sharpe': 8.86,  'wr': 89.4, 'n': 207}},
    'B':  {'name': '豆二',   'ex': 'DCE',  'months': list(range(1,13)),
           'DTE15-30': {'entry': 'day_0',   'tp': 0.5, 'sl': 2.0,  'sharpe': 15.99, 'wr': 90.2, 'n': 51},
           'DTE31-60': {'entry': 'day_0',   'tp': 1.0, 'sl': 2.0,  'sharpe': 9.96,  'wr': 84.2, 'n': 95},
           'DTE>60':   {'entry': 'night_0', 'tp': 1.0, 'sl': 2.5,  'sharpe': 27.39, 'wr': 92.5, 'n': 40}},
    'BR': {'name': '丁橡',   'ex': 'SHFE', 'months': list(range(1,13)),
           'DTE8-14':  {'entry': 'night_30','tp': 0.5, 'sl': 999,  'sharpe': 16.56, 'wr': 83.3, 'n': 54},
           'DTE15-30': {'entry': 'day_30',  'tp': 0.5, 'sl': 5.0,  'sharpe': 20.84, 'wr': 94.1, 'n': 51},
           'DTE31-60': {'entry': 'night_0', 'tp': 0.8, 'sl': 4.0,  'sharpe': 11.9,  'wr': 81.8, 'n': 110},
           'DTE>60':   {'entry': 'night_0', 'tp': 1.5, 'sl': 2.0,  'sharpe': 19.68, 'wr': 88.9, 'n': 36}},
    'C':  {'name': '玉米',   'ex': 'DCE',  'months': [1,3,5,7,9,11],
           'DTE15-30': {'entry': 'night_0', 'tp': 1.2, 'sl': 999,  'sharpe': 21.86, 'wr': 82.8, 'n': 29},
           'DTE31-60': {'entry': 'night_0', 'tp': 1.5, 'sl': 3.5,  'sharpe': 8.88,  'wr': 68.0, 'n': 97}},
    'CF': {'name': '棉花',   'ex': 'CZCE', 'months': [1,3,5,7,9,11],
           'DTE8-14':  {'entry': 'day_30',  'tp': 0.5, 'sl': 999,  'sharpe': 20.58, 'wr': 92.3, 'n': 39},
           'DTE15-30': {'entry': 'day_30',  'tp': 0.3, 'sl': 5.0,  'sharpe': 15.13, 'wr': 93.5, 'n': 107},
           'DTE31-60': {'entry': 'day_30',  'tp': 0.3, 'sl': 3.0,  'sharpe': 12.36, 'wr': 96.1, 'n': 178},
           'DTE>60':   {'entry': 'night_0', 'tp': 0.3, 'sl': 5.0,  'sharpe': 12.22, 'wr': 93.0, 'n': 227}},
    'CJ': {'name': '红枣',   'ex': 'CZCE', 'months': [1,3,5,7,9,11],
           'DTE15-30': {'entry': 'day_30',  'tp': 1.2, 'sl': 999,  'sharpe': 24.31, 'wr': 93.9, 'n': 33},
           'DTE31-60': {'entry': 'day_30',  'tp': 1.2, 'sl': 3.0,  'sharpe': 12.46, 'wr': 87.3, 'n': 71}},
    'CS': {'name': '淀粉',   'ex': 'DCE',  'months': [1,3,5,7,9,11],
           'DTE31-60': {'entry': 'day_0',   'tp': 2.0, 'sl': 3.0,  'sharpe': 10.37, 'wr': 75.7, 'n': 74}},
    'CU': {'name': '铜',     'ex': 'SHFE', 'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'day_0',   'tp': 0.3, 'sl': 999,  'sharpe': 7.54,  'wr': 82.7, 'n': 98},
           'DTE8-14':  {'entry': 'day_0',   'tp': 0.3, 'sl': 2.5,  'sharpe': 6.52,  'wr': 85.1, 'n': 154},
           'DTE15-30': {'entry': 'night_30','tp': 0.3, 'sl': 999,  'sharpe': 8.75,  'wr': 89.8, 'n': 167}},
    'EB': {'name': '苯乙烯', 'ex': 'DCE',  'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'night_30','tp': 0.3, 'sl': 999,  'sharpe': 6.13,  'wr': 65.4, 'n': 52},
           'DTE8-14':  {'entry': 'night_30','tp': 0.3, 'sl': 999,  'sharpe': 17.14, 'wr': 94.1, 'n': 68},
           'DTE15-30': {'entry': 'night_60','tp': 0.3, 'sl': 999,  'sharpe': 13.88, 'wr': 95.6, 'n': 226}},
    'EG': {'name': '乙二醇', 'ex': 'DCE',  'months': list(range(1,13)),
           'DTE15-30': {'entry': 'night_30','tp': 0.8, 'sl': 999,  'sharpe': 12.28, 'wr': 82.3, 'n': 68},
           'DTE31-60': {'entry': 'night_60','tp': 0.8, 'sl': 5.0,  'sharpe': 17.78, 'wr': 85.7, 'n': 70}},
    'FG': {'name': '玻璃',   'ex': 'CZCE', 'months': [1,3,5,7,9,11],
           'DTE15-30': {'entry': 'night_30','tp': 0.8, 'sl': 3.5,  'sharpe': 17.0,  'wr': 75.0, 'n': 24}},
    'JD': {'name': '鸡蛋',   'ex': 'DCE',  'months': list(range(1,13)),
           'DTE15-30': {'entry': 'day_30',  'tp': 0.5, 'sl': 2.0,  'sharpe': 14.3,  'wr': 90.2, 'n': 51},
           'DTE31-60': {'entry': 'day_30',  'tp': 1.0, 'sl': 3.0,  'sharpe': 15.45, 'wr': 86.2, 'n': 58}},
    'L':  {'name': '塑料',   'ex': 'DCE',  'months': list(range(1,13)),
           'DTE15-30': {'entry': 'night_30','tp': 0.3, 'sl': 999,  'sharpe': 17.73, 'wr': 95.3, 'n': 64},
           'DTE31-60': {'entry': 'night_30','tp': 0.3, 'sl': 999,  'sharpe': 11.3,  'wr': 92.0, 'n': 75}},
    'LC': {'name': '碳酸锂', 'ex': 'GFEX', 'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'day_30',  'tp': 0.3, 'sl': 999,  'sharpe': 8.09,  'wr': 82.9, 'n': 35},
           'DTE15-30': {'entry': 'day_30',  'tp': 0.5, 'sl': 2.5,  'sharpe': 13.44, 'wr': 88.6, 'n': 35},
           'DTE31-60': {'entry': 'day_30',  'tp': 0.8, 'sl': 2.0,  'sharpe': 26.78, 'wr': 95.6, 'n': 68}},
    'LH': {'name': '生猪',   'ex': 'DCE',  'months': [1,3,5,7,9,11],
           'DTE15-30': {'entry': 'day_30',  'tp': 0.8, 'sl': 3.0,  'sharpe': 19.33, 'wr': 89.7, 'n': 29},
           'DTE31-60': {'entry': 'day_30',  'tp': 0.8, 'sl': 2.0,  'sharpe': 19.74, 'wr': 95.0, 'n': 20}},
    'M':  {'name': '豆粕',   'ex': 'DCE',  'months': [1,3,5,7,8,9,11,12],
           'DTE15-30': {'entry': 'day_30',  'tp': 0.5, 'sl': 3.5,  'sharpe': 13.58, 'wr': 84.6, 'n': 26},
           'DTE31-60': {'entry': 'night_60','tp': 0.5, 'sl': 5.0,  'sharpe': 20.51, 'wr': 93.3, 'n': 30}},
    'MA': {'name': '甲醇',   'ex': 'CZCE', 'months': list(range(1,13)),
           'DTE15-30': {'entry': 'night_30','tp': 0.5, 'sl': 3.5,  'sharpe': 28.21, 'wr': 92.6, 'n': 27}},
    'NI': {'name': '镍',     'ex': 'SHFE', 'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'day_0',   'tp': 0.3, 'sl': 999,  'sharpe': 31.18, 'wr': 96.5, 'n': 29},
           'DTE8-14':  {'entry': 'night_30','tp': 0.3, 'sl': 999,  'sharpe': 13.15, 'wr': 88.9, 'n': 45},
           'DTE15-30': {'entry': 'night_60','tp': 0.3, 'sl': 5.0,  'sharpe': 17.51, 'wr': 93.3, 'n': 89},
           'DTE31-60': {'entry': 'night_0', 'tp': 0.8, 'sl': 2.5,  'sharpe': 21.11, 'wr': 95.0, 'n': 40}},
    'OI': {'name': '菜油',   'ex': 'CZCE', 'months': [1,3,5,7,9,11],
           'DTE≤7':    {'entry': 'day_30',  'tp': 0.3, 'sl': 5.0,  'sharpe': 13.02, 'wr': 83.9, 'n': 31},
           'DTE8-14':  {'entry': 'night_30','tp': 0.3, 'sl': 999,  'sharpe': 21.78, 'wr': 92.5, 'n': 40},
           'DTE15-30': {'entry': 'night_60','tp': 0.3, 'sl': 999,  'sharpe': 15.84, 'wr': 94.1, 'n': 101},
           'DTE31-60': {'entry': 'day_30',  'tp': 0.3, 'sl': 2.5,  'sharpe': 15.27, 'wr': 95.4, 'n': 109},
           'DTE>60':   {'entry': 'night_0', 'tp': 1.5, 'sl': 999,  'sharpe': 15.72, 'wr': 84.5, 'n': 330}},
    'P':  {'name': '棕榈油', 'ex': 'DCE',  'months': list(range(1,13)),
           'DTE15-30': {'entry': 'night_30','tp': 0.5, 'sl': 999,  'sharpe': 14.54, 'wr': 89.5, 'n': 95},
           'DTE31-60': {'entry': 'night_60','tp': 0.3, 'sl': 3.0,  'sharpe': 25.0,  'wr': 95.8, 'n': 24}},
    'PB': {'name': '铅',     'ex': 'SHFE', 'months': list(range(1,13)),
           'DTE8-14':  {'entry': 'night_0', 'tp': 0.8, 'sl': 999,  'sharpe': 11.47, 'wr': 69.6, 'n': 46},
           'DTE15-30': {'entry': 'day_0',   'tp': 0.8, 'sl': 999,  'sharpe': 15.36, 'wr': 86.7, 'n': 45},
           'DTE31-60': {'entry': 'night_0', 'tp': 1.0, 'sl': 3.0,  'sharpe': 17.66, 'wr': 86.7, 'n': 30}},
    'PF': {'name': '短纤',   'ex': 'CZCE', 'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'day_0',   'tp': 0.3, 'sl': 999,  'sharpe': 16.71, 'wr': 95.0, 'n': 20},
           'DTE8-14':  {'entry': 'night_30','tp': 0.5, 'sl': 999,  'sharpe': 32.5,  'wr': 97.0, 'n': 33},
           'DTE15-30': {'entry': 'day_0',   'tp': 0.5, 'sl': 2.5,  'sharpe': 23.31, 'wr': 96.7, 'n': 30}},
    'PG': {'name': 'LPG',    'ex': 'DCE',  'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'night_30','tp': 0.3, 'sl': 999,  'sharpe': 10.1,  'wr': 76.9, 'n': 65},
           'DTE8-14':  {'entry': 'night_30','tp': 0.3, 'sl': 999,  'sharpe': 17.84, 'wr': 91.5, 'n': 59},
           'DTE15-30': {'entry': 'night_0', 'tp': 0.3, 'sl': 999,  'sharpe': 13.82, 'wr': 95.4, 'n': 131},
           'DTE31-60': {'entry': 'day_30',  'tp': 0.5, 'sl': 3.5,  'sharpe': 20.41, 'wr': 92.6, 'n': 54}},
    'PK': {'name': '花生',   'ex': 'CZCE', 'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'day_0',   'tp': 0.5, 'sl': 999,  'sharpe': 17.06, 'wr': 86.2, 'n': 29},
           'DTE8-14':  {'entry': 'day_30',  'tp': 0.5, 'sl': 3.5,  'sharpe': 18.66, 'wr': 81.6, 'n': 38},
           'DTE15-30': {'entry': 'day_0',   'tp': 0.5, 'sl': 999,  'sharpe': 9.81,  'wr': 87.3, 'n': 55},
           'DTE31-60': {'entry': 'day_0',   'tp': 0.5, 'sl': 2.0,  'sharpe': 18.61, 'wr': 97.1, 'n': 35}},
    'PP': {'name': '聚丙烯', 'ex': 'DCE',  'months': list(range(1,13)),
           'DTE8-14':  {'entry': 'day_30',  'tp': 0.5, 'sl': 5.0,  'sharpe': 20.33, 'wr': 89.5, 'n': 38},
           'DTE15-30': {'entry': 'night_30','tp': 0.3, 'sl': 5.0,  'sharpe': 22.05, 'wr': 98.4, 'n': 64},
           'DTE31-60': {'entry': 'night_30','tp': 0.3, 'sl': 999,  'sharpe': 12.32, 'wr': 96.0, 'n': 75}},
    'PX': {'name': '对二甲苯','ex': 'CZCE','months': list(range(1,13)),
           'DTE15-30': {'entry': 'day_30',  'tp': 0.8, 'sl': 3.0,  'sharpe': 33.68, 'wr': 85.7, 'n': 21},
           'DTE31-60': {'entry': 'night_0', 'tp': 1.0, 'sl': 999,  'sharpe': 18.76, 'wr': 86.4, 'n': 88}},
    'RB': {'name': '螺纹钢', 'ex': 'SHFE', 'months': list(range(1,13)),
           'DTE15-30': {'entry': 'day_30',  'tp': 0.8, 'sl': 2.0,  'sharpe': 11.55, 'wr': 77.1, 'n': 35},
           'DTE31-60': {'entry': 'night_30','tp': 0.8, 'sl': 4.0,  'sharpe': 15.43, 'wr': 83.0, 'n': 94},
           'DTE>60':   {'entry': 'day_30',  'tp': 0.8, 'sl': 2.0,  'sharpe': 23.23, 'wr': 92.1, 'n': 38}},
    'RM': {'name': '菜粕',   'ex': 'CZCE', 'months': [1,3,5,7,9,11],
           'DTE8-14':  {'entry': 'night_30','tp': 0.5, 'sl': 999,  'sharpe': 14.03, 'wr': 80.0, 'n': 30},
           'DTE15-30': {'entry': 'night_30','tp': 0.5, 'sl': 4.0,  'sharpe': 25.62, 'wr': 91.1, 'n': 56},
           'DTE31-60': {'entry': 'night_30','tp': 0.5, 'sl': 999,  'sharpe': 30.19, 'wr': 100., 'n': 25}},
    'RU': {'name': '橡胶',   'ex': 'SHFE', 'months': [1,3,4,5,6,7,8,9,10,11],
           'DTE15-30': {'entry': 'night_60','tp': 0.8, 'sl': 4.0,  'sharpe': 13.03, 'wr': 80.0, 'n': 40},
           'DTE31-60': {'entry': 'night_0', 'tp': 0.8, 'sl': 5.0,  'sharpe': 13.32, 'wr': 85.7, 'n': 84},
           'DTE>60':   {'entry': 'day_30',  'tp': 0.8, 'sl': 2.0,  'sharpe': 40.34, 'wr': 97.1, 'n': 34}},
    'SA': {'name': '纯碱',   'ex': 'CZCE', 'months': list(range(1,13)),
           'DTE8-14':  {'entry': 'night_0', 'tp': 0.8, 'sl': 999,  'sharpe': 12.26, 'wr': 76.2, 'n': 21},
           'DTE31-60': {'entry': 'night_30','tp': 0.8, 'sl': 3.0,  'sharpe': 11.83, 'wr': 82.0, 'n': 50}},
    'SC': {'name': '原油',   'ex': 'INE',  'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'day_30',  'tp': 0.3, 'sl': 999,  'sharpe': 6.88,  'wr': 69.4, 'n': 134},
           'DTE8-14':  {'entry': 'day_30',  'tp': 0.3, 'sl': 3.0,  'sharpe': 8.32,  'wr': 78.0, 'n': 91},
           'DTE15-30': {'entry': 'night_30','tp': 0.3, 'sl': 5.0,  'sharpe': 8.18,  'wr': 81.1, 'n': 264}},
    'SF': {'name': '硅铁',   'ex': 'CZCE', 'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'day_30',  'tp': 0.3, 'sl': 2.5,  'sharpe': 10.47, 'wr': 71.4, 'n': 28},
           'DTE8-14':  {'entry': 'day_30',  'tp': 0.5, 'sl': 2.5,  'sharpe': 7.11,  'wr': 72.4, 'n': 29},
           'DTE15-30': {'entry': 'day_30',  'tp': 0.8, 'sl': 2.5,  'sharpe': 5.64,  'wr': 83.6, 'n': 122},
           'DTE31-60': {'entry': 'day_30',  'tp': 0.8, 'sl': 2.0,  'sharpe': 13.03, 'wr': 87.9, 'n': 66}},
    'SH': {'name': '烧碱',   'ex': 'CZCE', 'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'night_30','tp': 0.3, 'sl': 999,  'sharpe': 11.5,  'wr': 68.2, 'n': 22},
           'DTE8-14':  {'entry': 'day_30',  'tp': 0.5, 'sl': 5.0,  'sharpe': 23.38, 'wr': 88.0, 'n': 25},
           'DTE15-30': {'entry': 'day_30',  'tp': 0.5, 'sl': 3.0,  'sharpe': 43.64, 'wr': 96.7, 'n': 30},
           'DTE31-60': {'entry': 'night_0', 'tp': 0.8, 'sl': 4.0,  'sharpe': 17.2,  'wr': 90.2, 'n': 61}},
    'SI': {'name': '工业硅', 'ex': 'GFEX', 'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'day_30',  'tp': 0.5, 'sl': 3.0,  'sharpe': 5.41,  'wr': 62.1, 'n': 95},
           'DTE15-30': {'entry': 'day_0',   'tp': 0.5, 'sl': 2.0,  'sharpe': 12.09, 'wr': 90.9, 'n': 44},
           'DTE31-60': {'entry': 'day_30',  'tp': 0.8, 'sl': 2.5,  'sharpe': 18.46, 'wr': 95.0, 'n': 40}},
    'SM': {'name': '锰硅',   'ex': 'CZCE', 'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'day_30',  'tp': 0.3, 'sl': 999,  'sharpe': 13.14, 'wr': 86.4, 'n': 22},
           'DTE15-30': {'entry': 'day_0',   'tp': 0.8, 'sl': 3.0,  'sharpe': 8.51,  'wr': 80.4, 'n': 51},
           'DTE31-60': {'entry': 'day_0',   'tp': 0.8, 'sl': 2.5,  'sharpe': 14.22, 'wr': 93.5, 'n': 46}},
    'SN': {'name': '锡',     'ex': 'SHFE', 'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'day_0',   'tp': 0.3, 'sl': 999,  'sharpe': 20.97, 'wr': 90.0, 'n': 50},
           'DTE8-14':  {'entry': 'day_30',  'tp': 0.3, 'sl': 4.0,  'sharpe': 19.33, 'wr': 90.0, 'n': 50},
           'DTE15-30': {'entry': 'day_0',   'tp': 0.5, 'sl': 4.0,  'sharpe': 11.36, 'wr': 94.2, 'n': 104}},
    'SR': {'name': '白糖',   'ex': 'CZCE', 'months': [1,3,5,7,9,11],
           'DTE15-30': {'entry': 'night_60','tp': 0.5, 'sl': 999,  'sharpe': 9.26,  'wr': 84.9, 'n': 73},
           'DTE31-60': {'entry': 'night_30','tp': 0.8, 'sl': 4.0,  'sharpe': 9.98,  'wr': 80.2, 'n': 172}},
    'TA': {'name': 'PTA',    'ex': 'CZCE', 'months': list(range(1,13)),
           'DTE8-14':  {'entry': 'day_30',  'tp': 0.5, 'sl': 2.5,  'sharpe': 11.2,  'wr': 69.0, 'n': 29},
           'DTE15-30': {'entry': 'night_30','tp': 0.5, 'sl': 4.0,  'sharpe': 14.76, 'wr': 90.3, 'n': 31},
           'DTE31-60': {'entry': 'night_60','tp': 0.5, 'sl': 5.0,  'sharpe': 14.7,  'wr': 93.8, 'n': 32},
           'DTE>60':   {'entry': 'night_30','tp': 1.0, 'sl': 5.0,  'sharpe': 13.28, 'wr': 89.5, 'n': 133}},
    'UR': {'name': '尿素',   'ex': 'CZCE', 'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'day_0',   'tp': 0.5, 'sl': 3.5,  'sharpe': 16.35, 'wr': 85.7, 'n': 21},
           'DTE8-14':  {'entry': 'day_0',   'tp': 1.0, 'sl': 3.5,  'sharpe': 19.36, 'wr': 81.0, 'n': 21},
           'DTE15-30': {'entry': 'day_0',   'tp': 0.8, 'sl': 2.0,  'sharpe': 18.97, 'wr': 90.0, 'n': 30}},
    'V':  {'name': 'PVC',    'ex': 'DCE',  'months': list(range(1,13)),
           'DTE8-14':  {'entry': 'day_30',  'tp': 0.3, 'sl': 999,  'sharpe': 34.55, 'wr': 100., 'n': 26},
           'DTE15-30': {'entry': 'night_30','tp': 0.3, 'sl': 999,  'sharpe': 25.03, 'wr': 100., 'n': 77},
           'DTE31-60': {'entry': 'night_0', 'tp': 0.3, 'sl': 5.0,  'sharpe': 9.97,  'wr': 95.7, 'n': 93}},
    'Y':  {'name': '豆油',   'ex': 'DCE',  'months': [1,3,5,7,9,11],
           'DTE15-30': {'entry': 'night_60','tp': 0.5, 'sl': 999,  'sharpe': 21.39, 'wr': 93.3, 'n': 30},
           'DTE>60':   {'entry': 'night_30','tp': 1.0, 'sl': 3.0,  'sharpe': 13.4,  'wr': 92.0, 'n': 138}},
    'ZC': {'name': '动力煤', 'ex': 'CZCE', 'months': list(range(1,13)),
           'DTE8-14':  {'entry': 'night_30','tp': 0.5, 'sl': 999,  'sharpe': 17.73, 'wr': 90.9, 'n': 22},
           'DTE31-60': {'entry': 'day_0',   'tp': 0.3, 'sl': 2.5,  'sharpe': 14.28, 'wr': 89.7, 'n': 39}},
    'ZN': {'name': '锌',     'ex': 'SHFE', 'months': list(range(1,13)),
           'DTE≤7':    {'entry': 'day_30',  'tp': 0.3, 'sl': 5.0,  'sharpe': 11.58, 'wr': 81.5, 'n': 27},
           'DTE8-14':  {'entry': 'day_30',  'tp': 0.3, 'sl': 3.0,  'sharpe': 16.33, 'wr': 87.5, 'n': 32},
           'DTE15-30': {'entry': 'night_60','tp': 0.3, 'sl': 999,  'sharpe': 43.49, 'wr': 95.8, 'n': 24}},
}

# 无夜盘品种（用于今日计划的时段过滤）
_NO_NIGHT_SESSION = {
    # 郑商所无夜盘
    'AP', 'CJ', 'PX', 'PK', 'SM', 'SF',
    # 大商所无夜盘
    'JD', 'LH', 'CS', 'RR',
    # 广期所无夜盘
    'LC',
}


def _get_session_context():
    """根据当前时间返回 (session_label, filter_night_only)
    session_label: 面板标题用的文字
    filter_night_only: True=只显示有夜盘品种, False=显示全部"""
    h = datetime.now().hour
    m = datetime.now().minute
    hm = h * 100 + m
    if 900 <= hm <= 1500:
        return '日盘交易计划', False
    elif 1501 <= hm <= 2059:
        return '今晚夜盘计划 (21:00开盘)', True
    elif 2100 <= hm <= 2359:
        return '夜盘交易计划 (进行中)', True
    elif 0 <= hm <= 230:
        return '夜盘交易计划 (进行中)', True
    else:
        # 02:31 ~ 08:59 → 下一场是日盘
        return '日盘交易计划 (09:00开盘)', False


# VRP缓存（5分钟有效）
_vrp_cache = {'data': None, 'ts': 0}
_VRP_CACHE_SEC = 300


def _bs_iv_from_price(S, K, T, price, cp='C', r=0.03, max_iter=50):
    """从期权价格反推隐含波动率（二分法）"""
    if T <= 0 or price <= 0 or S <= 0 or K <= 0:
        return None
    lo, hi = 0.01, 3.0
    for _ in range(max_iter):
        mid = (lo + hi) / 2
        d1 = (math.log(S / K) + (r + 0.5 * mid**2) * T) / (mid * math.sqrt(T))
        d2 = d1 - mid * math.sqrt(T)
        if cp == 'C':
            bs_price = S * _norm.cdf(d1) - K * math.exp(-r * T) * _norm.cdf(d2)
        else:
            bs_price = K * math.exp(-r * T) * _norm.cdf(-d2) - S * _norm.cdf(-d1)
        if bs_price > price:
            hi = mid
        else:
            lo = mid
        if abs(hi - lo) < 0.0001:
            break
    iv = (lo + hi) / 2
    return iv if 0.02 < iv < 2.5 else None


def _calc_rv(close_prices, annualize_factor=252*6*60):
    """从1分钟收盘价序列计算年化实现波动率"""
    if len(close_prices) < 60:
        return None
    prices = np.array(close_prices, dtype=float)
    returns = np.diff(np.log(prices))
    returns = returns[np.isfinite(returns)]
    if len(returns) < 30:
        return None
    rv = np.std(returns) * np.sqrt(annualize_factor)
    return rv if 0.01 < rv < 5.0 else None


def _estimate_dte(product, month_str):
    """估算DTE天数 — 委托给统一的 calc_dte"""
    return calc_dte(product, month_str)


def _parse_futures_symbol(sym):
    """从期货symbol提取品种和月份: ag2604→(AG,2604), SA604→(SA,604)"""
    m = re.match(r'([a-zA-Z]+)(\d{3,4})$', sym)
    if not m:
        return None, None
    return m.group(1).upper(), m.group(2)


def scan_vrp():
    """全市场VRP扫描，返回排序后的品种列表"""
    now = time.time()
    if _vrp_cache['data'] is not None and (now - _vrp_cache['ts']) < _VRP_CACHE_SEC:
        return _vrp_cache['data']

    db = get_db()
    cur = db.cursor()

    # 1. 获取所有期货合约
    cur.execute("""SELECT DISTINCT symbol FROM dbbardata
                   WHERE symbol NOT LIKE '%C%' AND symbol NOT LIKE '%P%'
                   AND symbol NOT LIKE '%-C-%' AND symbol NOT LIKE '%-P-%'""")
    all_futures = [r[0] for r in cur.fetchall()]

    # 按品种分组
    products = {}
    for f in all_futures:
        prod, month = _parse_futures_symbol(f)
        if not prod or prod not in _EXCHANGE_MAP:
            continue
        # 跳过带后缀的合约（如l2604F）
        if not re.match(r'^[a-zA-Z]+\d{3,4}$', f):
            continue
        products.setdefault(prod, []).append((f, month))

    results = []
    day_start = get_trading_day_start()

    for prod, contracts in products.items():
        try:
            # 选主力合约：近期数据量最大
            best_sym, best_month, best_count = None, None, 0
            for f_sym, month in contracts:
                cur.execute("SELECT COUNT(*) FROM dbbardata WHERE symbol=? AND datetime>=?",
                            (f_sym, day_start))
                cnt = cur.fetchone()[0]
                if cnt > best_count:
                    best_count = cnt
                    best_sym = f_sym
                    best_month = month

            if best_sym is None or best_count < 10:
                continue

            # 期货最新价格
            cur.execute(_LATEST_PRICE_SQL, (best_sym,))
            row = cur.fetchone()
            if not row:
                continue
            futures_price = row[0]

            # RV：近N天1分钟收盘价
            cur.execute("SELECT close_price FROM dbbardata WHERE symbol=? AND datetime>=? ORDER BY datetime",
                        (best_sym, day_start))
            fut_closes = [r[0] for r in cur.fetchall()]
            rv = _calc_rv(fut_closes)
            if rv is None:
                continue

            # DTE
            dte = _estimate_dte(prod, best_month)
            if dte is None or dte <= 0:
                continue
            T = dte / 365.0

            # 构造期权symbol前缀
            is_czce = _EXCHANGE_MAP.get(prod, '') in ('CZCE', 'CZCE_SP')
            if is_czce:
                opt_prefix = f'{prod.upper()}{best_month}'
            else:
                opt_prefix = f'{prod.lower()}{best_month}'

            # 查找Call期权
            cur.execute("SELECT DISTINCT symbol FROM dbbardata WHERE symbol LIKE ? AND datetime>=?",
                        (f'{opt_prefix}C%', day_start))
            call_syms = [r[0] for r in cur.fetchall()]

            # dash格式（大商所/广期所）
            if not call_syms:
                cur.execute("SELECT DISTINCT symbol FROM dbbardata WHERE symbol LIKE ? AND datetime>=?",
                            (f'{opt_prefix}-C-%', day_start))
                call_syms = [r[0] for r in cur.fetchall()]

            if not call_syms:
                continue

            # 解析行权价，找ATM
            atm_call, atm_strike = None, None
            min_diff = float('inf')
            for cs in call_syms:
                m = re.search(r'[CP][-]?(\d+)$', cs)
                if not m:
                    continue
                strike = int(m.group(1))
                diff = abs(strike - futures_price)
                if diff < min_diff:
                    min_diff = diff
                    atm_call = cs
                    atm_strike = strike

            if atm_call is None:
                continue

            # 对应Put
            if '-C-' in atm_call:
                atm_put = atm_call.replace('-C-', '-P-')
            else:
                atm_put = re.sub(r'C(\d+)$', f'P{atm_strike}', atm_call)

            # ATM Call 最新价
            cur.execute(_LATEST_PRICE_SQL, (atm_call,))
            row = cur.fetchone()
            if not row or row[0] <= 0:
                continue
            call_price = row[0]

            # ATM Put 最新价
            cur.execute(_LATEST_PRICE_SQL, (atm_put,))
            row = cur.fetchone()
            put_price = row[0] if row and row[0] > 0 else None

            # IV（Call + Put 均值）
            iv_c = _bs_iv_from_price(futures_price, atm_strike, T, call_price, 'C')
            iv_p = _bs_iv_from_price(futures_price, atm_strike, T, put_price, 'P') if put_price else None

            if iv_c is not None and iv_p is not None:
                iv = (iv_c + iv_p) / 2
            elif iv_c is not None:
                iv = iv_c
            elif iv_p is not None:
                iv = iv_p
            else:
                continue

            vrp = iv - rv

            # Tier评分
            tier_sharpe = _TIER_SHARPE.get(prod, 0.0)
            if tier_sharpe >= 0.5:
                tier_label, tier_score = 'T1', 1.0
            elif tier_sharpe >= 0.3:
                tier_label, tier_score = 'T2', 0.7
            elif tier_sharpe >= 0.1:
                tier_label, tier_score = 'T3', 0.4
            else:
                tier_label, tier_score = 'T4', 0.1

            # DTE评分
            if dte <= 7:
                dte_score, dte_label = 1.0, f'{dte}天★'
            elif dte <= 14:
                dte_score, dte_label = 0.7, f'{dte}天★'
            elif dte <= 30:
                dte_score, dte_label = 0.3, f'{dte}天'
            else:
                dte_score, dte_label = 0.1, f'{dte}天'

            # 流动性评分
            liq_score = min(best_count / 3000, 1.0)

            # 跨式权利金
            straddle_price = call_price + (put_price if put_price else call_price)
            multiplier = _MULTIPLIER.get(prod, 10)

            results.append({
                'product': prod,
                'futures_sym': best_sym,
                'futures_price': futures_price,
                'atm_strike': atm_strike,
                'call_price': call_price,
                'put_price': put_price,
                'straddle': straddle_price,
                'straddle_value': straddle_price * multiplier,
                'iv': iv, 'rv': rv, 'vrp': vrp,
                'dte': dte, 'dte_score': dte_score, 'dte_label': dte_label,
                'tier_sharpe': tier_sharpe, 'tier_label': tier_label, 'tier_score': tier_score,
                'liq_score': liq_score,
            })
        except Exception:
            continue

    if not results:
        _vrp_cache['data'] = []
        _vrp_cache['ts'] = now
        return []

    # VRP百分位
    vrps = sorted([r['vrp'] for r in results])
    for r in results:
        rank = sum(1 for v in vrps if v <= r['vrp'])
        r['vrp_pct'] = rank / len(vrps)

    # 综合评分：VRP百分位30% + Tier30% + DTE20% + 流动性20%
    for r in results:
        r['score'] = (r['vrp_pct'] * 0.30 +
                      r['tier_score'] * 0.30 +
                      r['dte_score'] * 0.20 +
                      r['liq_score'] * 0.20)

    results.sort(key=lambda x: x['score'], reverse=True)
    _vrp_cache['data'] = results
    _vrp_cache['ts'] = now
    return results


def _build_vrp_panel():
    """构建VRP扫描面板HTML"""
    try:
        results = scan_vrp()
    except Exception as e:
        return html.Div(f'VRP扫描出错: {e}', style={'color': '#FF6B6B', 'padding': '20px'})

    if not results:
        return html.Div('暂无数据（非交易时段或数据不足）',
                        style={'color': '#888', 'padding': '20px', 'textAlign': 'center'})

    header = html.Tr([
        html.Th('#', style={'width': '30px', 'padding': '6px 4px'}),
        html.Th('品种', style={'width': '50px'}),
        html.Th('评分', style={'width': '42px'}),
        html.Th('VRP', style={'width': '62px'}),
        html.Th('IV', style={'width': '48px'}),
        html.Th('RV', style={'width': '48px'}),
        html.Th('DTE', style={'width': '52px'}),
        html.Th('Tier', style={'width': '30px'}),
        html.Th('期货', style={'width': '70px'}),
        html.Th('ATM', style={'width': '60px'}),
        html.Th('跨式', style={'width': '55px'}),
        html.Th('价值', style={'width': '60px'}),
    ], style={'backgroundColor': '#1a1a3e', 'color': '#4fc3f7', 'fontSize': '11px',
              'textAlign': 'left'})

    rows = []
    tier_colors = {'T1': '#00FF88', 'T2': '#4fc3f7', 'T3': '#FFD700', 'T4': '#FF6B6B'}

    for i, r in enumerate(results[:30]):
        vrp_val = r['vrp'] * 100
        if vrp_val >= 10:
            vrp_color = '#00FF88'
        elif vrp_val >= 5:
            vrp_color = '#4fc3f7'
        elif vrp_val >= 0:
            vrp_color = '#FFD700'
        else:
            vrp_color = '#FF6B6B'

        score_pct = r['score'] * 100
        if score_pct >= 70:
            score_color, row_bg = '#00FF88', '#0a2a1a'
        elif score_pct >= 50:
            score_color, row_bg = '#4fc3f7', '#0d1117'
        else:
            score_color, row_bg = '#888', '#0d1117'

        td_s = {'padding': '4px 4px', 'fontSize': '12px'}
        rows.append(html.Tr([
            html.Td(f'{i+1}', style={**td_s, 'textAlign': 'center', 'color': '#555'}),
            html.Td(r['product'], style={**td_s, 'fontWeight': 'bold', 'color': '#fff'}),
            html.Td(f'{score_pct:.0f}', style={**td_s, 'textAlign': 'center', 'color': score_color, 'fontWeight': 'bold'}),
            html.Td(f'{vrp_val:+.1f}%', style={**td_s, 'color': vrp_color, 'fontWeight': 'bold'}),
            html.Td(f'{r["iv"]*100:.1f}%', style={**td_s, 'color': '#aaa'}),
            html.Td(f'{r["rv"]*100:.1f}%', style={**td_s, 'color': '#aaa'}),
            html.Td(r['dte_label'], style={**td_s, 'color': '#FFD700' if r['dte'] <= 14 else '#aaa'}),
            html.Td(r['tier_label'], style={**td_s, 'color': tier_colors.get(r['tier_label'], '#888'), 'textAlign': 'center'}),
            html.Td(f'{r["futures_price"]:.0f}', style={**td_s, 'color': '#aaa', 'textAlign': 'right'}),
            html.Td(f'{r["atm_strike"]}', style={**td_s, 'color': '#aaa', 'textAlign': 'right'}),
            html.Td(f'{r["straddle"]:.0f}', style={**td_s, 'color': '#aaa', 'textAlign': 'right'}),
            html.Td(f'{r["straddle_value"]:.0f}', style={**td_s, 'color': '#FFD700', 'textAlign': 'right'}),
        ], style={'backgroundColor': row_bg, 'borderBottom': '1px solid #1a1a3e'}))

    table = html.Table(
        [html.Thead(header), html.Tbody(rows)],
        style={'width': '100%', 'borderCollapse': 'collapse'}
    )

    top_3 = results[:3]
    summary = '  |  '.join([f'{r["product"]} {r["score"]*100:.0f}分 VRP{r["vrp"]*100:+.1f}%' for r in top_3])
    n_positive = sum(1 for r in results if r['vrp'] > 0)
    avg_vrp = sum(r['vrp'] for r in results) / len(results) * 100

    return html.Div([
        html.Div([
            html.Span('VRP扫描', style={'color': '#00FF88', 'fontSize': '15px', 'fontWeight': 'bold'}),
            html.Span(f'  {len(results)}品种 | VRP>0: {n_positive}个 | 均VRP: {avg_vrp:+.1f}%',
                      style={'color': '#666', 'fontSize': '12px', 'marginLeft': '10px'}),
        ], style={'padding': '10px 15px', 'borderBottom': '1px solid #2a2a4a'}),
        html.Div([
            html.Span(f'TOP3: {summary}', style={'color': '#4fc3f7', 'fontSize': '11px'}),
            html.Span('  |  评分=VRP百分位30%+Tier30%+DTE20%+流动性20%  |  DTE≤14=★',
                      style={'color': '#444', 'fontSize': '10px'}),
        ], style={'padding': '4px 15px'}),
        html.Div(table, style={'padding': '0 15px 10px', 'maxHeight': '500px', 'overflowY': 'auto'}),
    ])


# ============ Gamma/Strangle 每日检查面板 ============

_SCORECARD_MAX = 15  # 最多显示15个品种，避免页面过重


def _build_scorecard_panel():
    """构建 Gamma Scalp 每日检查面板 HTML"""
    if not _HAS_CHECKER:
        return None, []

    try:
        results, events_all = gs_scan_all()
    except Exception:
        return None, []

    if not results:
        return None, []

    # 只保留前N个（已按总分排序）
    total_count = len(results)
    results = results[:_SCORECARD_MAX]

    now_str = datetime.now().strftime('%H:%M:%S')

    # 表头
    header_style = {'color': '#aaa', 'fontSize': '11px', 'padding': '3px 8px',
                     'textAlign': 'center', 'borderBottom': '1px solid #333'}
    cell_style = {'padding': '4px 8px', 'textAlign': 'center', 'fontSize': '13px',
                  'color': '#ddd', 'borderBottom': '1px solid #1a1a3a'}

    thead = html.Tr([
        html.Th('品种', style={**header_style, 'textAlign': 'left'}),
        html.Th('DTE', style=header_style),
        html.Th('事件', style=header_style),
        html.Th('IV%', style=header_style),
        html.Th('Squeeze', style=header_style),
        html.Th('ATR', style=header_style),
        html.Th('总分', style=header_style),
        html.Th('建议', style={**header_style, 'textAlign': 'left'}),
    ])

    rows = []
    for r in results:
        def _sc(s):
            return {**cell_style, 'color': '#00FF88' if s >= 2.0 else '#FFD700' if s >= 1.0 else '#888'}
        rows.append(html.Tr([
            html.Td(f'{r["product"]}{r["futures_sym"][-3:]}',
                     style={**cell_style, 'textAlign': 'left', 'fontWeight': 'bold', 'color': '#fff'}),
            html.Td(str(r['dte']), style={**cell_style, 'color': '#FF4444' if r['dte'] < 7 else '#ddd'}),
            html.Td(f'{r["event_score"]:.1f}', style=_sc(r['event_score'])),
            html.Td(f'{r["iv_score"]:.1f}', style=_sc(r['iv_score'])),
            html.Td(f'{r["bb_score"]:.1f}', style=_sc(r['bb_score'])),
            html.Td(f'{r["atr_score"]:.1f}', style=_sc(r['atr_score'])),
            html.Td(f'{r["total_score"]:.1f}',
                     style={**cell_style, 'fontWeight': 'bold', 'fontSize': '14px',
                            'color': r['advice_color']}),
            html.Td(r['advice'],
                     style={**cell_style, 'textAlign': 'left', 'fontWeight': 'bold',
                            'color': r['advice_color']}),
        ]))

    table = html.Table([html.Thead(thead), html.Tbody(rows)],
                        style={'width': '100%', 'borderCollapse': 'collapse'})

    # 事件摘要
    event_items = []
    for ev in events_all:
        event_items.append(html.Div(f'  {ev}', style={'color': '#aaa', 'fontSize': '12px'}))

    truncated_note = f'  (显示前{_SCORECARD_MAX}/{total_count})' if total_count > _SCORECARD_MAX else ''

    panel = html.Div([
        # 标题（可点击折叠，默认折叠）
        html.Div([
            html.Span('Gamma / Strangle 每日检查',
                       style={'color': '#E0AAFF', 'fontSize': '15px', 'fontWeight': 'bold'}),
            html.Span(f'  更新: {now_str}',
                       style={'color': '#666', 'fontSize': '12px', 'marginLeft': '15px'}),
            html.Span(f'{truncated_note}  点击展开/折叠',
                       style={'color': '#555', 'fontSize': '11px'}),
        ], id='scorecard-header',
           style={'padding': '10px 15px', 'cursor': 'pointer', 'borderBottom': '1px solid #333'}),
        # 内容（默认折叠）
        html.Div([
            table,
            html.Div(event_items, style={'padding': '6px 8px', 'borderTop': '1px solid #333',
                                          'marginTop': '4px'}) if event_items else html.Div(),
        ], id='scorecard-body', style={'display': 'none'}),
    ], style={
        'backgroundColor': '#12122a', 'borderTop': '3px solid #9D4EDD',
        'borderRadius': '4px', 'marginBottom': '8px',
    })

    # sidebar 导航数据（只取前10，sidebar空间有限）
    nav_items = []
    for r in results[:10]:
        nav_items.append({
            'product': f'{r["product"]}{r["futures_sym"][-3:]}',
            'score': r['total_score'],
            'color': r['advice_color'],
        })

    return panel, nav_items


# ============ Dash 应用 ============

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
app = Dash(__name__, suppress_callback_exceptions=True,
           assets_folder=os.path.join(SCRIPT_DIR, 'assets'))

def _build_trade_row(idx):
    """构建一行交易控件（动态ID，支持多行并存）"""
    row_children = []
    if idx == 0:
        row_children.append(html.Button('+', id='add-trade-row-btn', n_clicks=0, style={
            'padding': '2px 8px', 'fontSize': '14px', 'cursor': 'pointer', 'fontWeight': 'bold',
            'backgroundColor': '#2a2a4a', 'color': '#FFD700',
            'border': '1px solid #FFD700', 'borderRadius': '4px', 'marginRight': '6px',
            'lineHeight': '1'}))
        row_children.append(html.Span('交易 ', style={
            'color': '#FFD700', 'fontSize': '13px', 'fontWeight': 'bold', 'marginRight': '10px'}))
    else:
        row_children.append(html.Button('×', id={'type': 'remove-trade-row-btn', 'index': idx}, n_clicks=0, style={
            'padding': '2px 8px', 'fontSize': '14px', 'cursor': 'pointer', 'fontWeight': 'bold',
            'backgroundColor': '#3a1a1e', 'color': '#FF6B6B',
            'border': '1px solid #FF6B6B', 'borderRadius': '4px', 'marginRight': '6px',
            'lineHeight': '1'}))
        row_children.append(html.Span(f'交易{idx+1} ', style={
            'color': '#888', 'fontSize': '13px', 'fontWeight': 'bold', 'marginRight': '10px'}))

    # 第一行：选对 + 加载 + 卖出进仓
    line1 = row_children + [
        dcc.Dropdown(
            id={'type': 'trade-pair-select', 'index': idx},
            options=[], placeholder='选择期权对...',
            className='dark-dropdown',
            style={'width': '320px', 'display': 'inline-block', 'verticalAlign': 'middle'}),
        html.Button('加载', id={'type': 'load-btn', 'index': idx}, n_clicks=0, style={
            'padding': '4px 12px', 'fontSize': '12px', 'cursor': 'pointer',
            'backgroundColor': '#1a3a5e', 'color': '#4fc3f7',
            'border': '1px solid #4fc3f7', 'borderRadius': '4px', 'marginLeft': '8px'}),
        html.Span(id={'type': 'load-status', 'index': idx}, style={
            'fontSize': '12px', 'marginLeft': '6px', 'minWidth': '70px'}),
        html.Span(' 单腿手数 ', style={'color': '#aaa', 'fontSize': '12px', 'marginLeft': '12px', 'marginRight': '4px'}),
        dcc.Input(
            id={'type': 'trade-volume', 'index': idx}, type='number', value=1, min=1, max=500,
            style={'width': '60px', 'padding': '5px 8px', 'fontSize': '13px',
                   'backgroundColor': '#1a1a3e', 'color': '#fff',
                   'border': '1px solid #444', 'borderRadius': '4px', 'textAlign': 'center'}),
        html.Span('Ask≥', id={'type': 'entry-condition-label', 'index': idx},
                  style={'color': '#aaa', 'fontSize': '11px', 'marginLeft': '10px'}),
        dcc.Input(
            id={'type': 'entry-condition-sum', 'index': idx}, type='number', placeholder='不限',
            style={'width': '62px', 'padding': '5px 6px', 'fontSize': '12px',
                   'backgroundColor': '#1a1a3e', 'color': '#FFD700',
                   'border': '1px solid #555', 'borderRadius': '4px', 'textAlign': 'center'}),
        dcc.Dropdown(
            id={'type': 'entry-direction', 'index': idx},
            options=[{'label': '卖出开仓', 'value': 'sell'}, {'label': '买入开仓', 'value': 'buy'}],
            value='sell', clearable=False,
            style={'width': '100px', 'display': 'inline-block', 'marginLeft': '6px',
                   'fontSize': '12px', 'verticalAlign': 'middle'}),
        html.Button('进仓', id={'type': 'trade-entry-btn', 'index': idx}, n_clicks=0, style={
            'padding': '5px 16px', 'fontSize': '13px', 'cursor': 'pointer',
            'backgroundColor': '#1a4a1e', 'color': '#00FF88',
            'border': '1px solid #00FF88', 'borderRadius': '4px', 'marginLeft': '4px',
            'fontWeight': 'bold'}),
        dcc.Checklist(
            id={'type': 'entry-split-toggle', 'index': idx},
            options=[{'label': '分轮', 'value': 'split'}],
            value=[], inline=True,
            style={'display': 'inline-block', 'marginLeft': '6px', 'fontSize': '11px',
                   'verticalAlign': 'middle'},
            inputStyle={'marginRight': '2px', 'cursor': 'pointer'},
            labelStyle={'color': '#aaa', 'fontSize': '11px', 'cursor': 'pointer'}),
        html.Span(id={'type': 'entry-split-plan', 'index': idx}, style={
            'color': '#4fc3f7', 'fontSize': '11px', 'marginLeft': '2px'}),
        html.Span(id={'type': 'trade-entry-status', 'index': idx}, style={
            'color': '#00FF88', 'fontSize': '12px', 'marginLeft': '10px'}),
        html.Span(id={'type': 'trade-entry-result', 'index': idx}, style={
            'color': '#00FF88', 'fontSize': '12px'}),
    ]

    # 第二行：买入平仓 + 紧急停止
    line2 = [
        html.Span('', style={'width': '32px', 'display': 'inline-block'}),  # 左侧对齐占位
        html.Span('平仓 ', style={'color': '#aaa', 'fontSize': '12px', 'marginRight': '4px'}),
        dcc.Input(
            id={'type': 'close-volume', 'index': idx}, type='number', value=1, min=1, max=500,
            style={'width': '60px', 'padding': '5px 8px', 'fontSize': '13px',
                   'backgroundColor': '#1a1a3e', 'color': '#fff',
                   'border': '1px solid #444', 'borderRadius': '4px', 'textAlign': 'center'}),
        html.Span('Bid≤', id={'type': 'close-condition-label', 'index': idx},
                  style={'color': '#aaa', 'fontSize': '11px', 'marginLeft': '10px'}),
        dcc.Input(
            id={'type': 'close-condition-sum', 'index': idx}, type='number', placeholder='不限',
            style={'width': '62px', 'padding': '5px 6px', 'fontSize': '12px',
                   'backgroundColor': '#1a1a3e', 'color': '#FF6B6B',
                   'border': '1px solid #555', 'borderRadius': '4px', 'textAlign': 'center'}),
        dcc.Dropdown(
            id={'type': 'close-direction', 'index': idx},
            options=[{'label': '买入平仓', 'value': 'buy_close'}, {'label': '卖出平仓', 'value': 'sell_close'}],
            value='buy_close', clearable=False,
            style={'width': '100px', 'display': 'inline-block', 'marginLeft': '6px',
                   'fontSize': '12px', 'verticalAlign': 'middle'}),
        html.Button('平仓', id={'type': 'trade-close-btn', 'index': idx}, n_clicks=0, style={
            'padding': '5px 16px', 'fontSize': '13px', 'cursor': 'pointer',
            'backgroundColor': '#4a1a1e', 'color': '#FF6B6B',
            'border': '1px solid #FF6B6B', 'borderRadius': '4px', 'marginLeft': '4px',
            'fontWeight': 'bold'}),
        dcc.Checklist(
            id={'type': 'close-split-toggle', 'index': idx},
            options=[{'label': '分轮', 'value': 'split'}],
            value=[], inline=True,
            style={'display': 'inline-block', 'marginLeft': '6px', 'fontSize': '11px',
                   'verticalAlign': 'middle'},
            inputStyle={'marginRight': '2px', 'cursor': 'pointer'},
            labelStyle={'color': '#aaa', 'fontSize': '11px', 'cursor': 'pointer'}),
        html.Span(id={'type': 'close-split-plan', 'index': idx}, style={
            'color': '#FF6B6B', 'fontSize': '11px', 'marginLeft': '2px'}),
        html.Span(id={'type': 'trade-close-status', 'index': idx}, style={
            'color': '#FF6B6B', 'fontSize': '12px', 'marginLeft': '10px'}),
        html.Span(id={'type': 'trade-close-result', 'index': idx}, style={
            'color': '#FF6B6B', 'fontSize': '12px'}),
        html.Span('│', style={'color': '#333', 'margin': '0 8px'}),
        html.Button('停', id={'type': 'emergency-btn', 'index': idx}, n_clicks=0, style={
            'padding': '3px 10px', 'fontSize': '12px', 'cursor': 'pointer',
            'backgroundColor': '#4a0000', 'color': '#FF6B6B',
            'border': '1px solid #FF6B6B', 'borderRadius': '4px',
            'fontWeight': 'bold'}),
        html.Span(id={'type': 'emergency-status', 'index': idx}, style={
            'fontSize': '11px', 'marginLeft': '6px'}),
        html.Span('│', style={'color': '#333', 'margin': '0 8px'}),
        html.Button('时间强平', id={'type': 'force-close-btn', 'index': idx}, n_clicks=0, style={
            'padding': '3px 10px', 'fontSize': '12px', 'cursor': 'pointer',
            'backgroundColor': '#1a3a1a', 'color': '#00FF88',
            'border': '1px solid #00FF88', 'borderRadius': '4px',
            'fontWeight': 'bold'}),
        html.Span(id={'type': 'force-close-status', 'index': idx}, style={
            'fontSize': '11px', 'marginLeft': '6px'}),
    ]

    return html.Div([
        html.Div(line1, style={'display': 'flex', 'alignItems': 'center', 'flexWrap': 'wrap', 'gap': '2px 0'}),
        html.Div(line2, style={'display': 'flex', 'alignItems': 'center', 'flexWrap': 'wrap',
                                'gap': '2px 0', 'marginTop': '3px'}),
    ], id={'type': 'trade-row', 'index': idx}, style={'marginBottom': '8px'})


def serve_layout():
    """每次页面加载都从文件读取最新配置（而非启动时的静态快照）"""
    return html.Div([
    # 顶部标题栏
    html.Div([
        html.H2('期权工作台', style={'margin': '0', 'color': '#fff', 'display': 'inline-block'}),
        html.Button('资讯', id='news-btn', n_clicks=0, style={
            'float': 'right', 'padding': '6px 16px', 'fontSize': '13px',
            'cursor': 'pointer', 'backgroundColor': '#0a4a6e', 'color': '#4fc3f7',
            'border': '1px solid #4fc3f7', 'borderRadius': '4px', 'marginTop': '3px'}),
        html.Button('预警统计', id='alert-stats-btn', n_clicks=0, style={
            'float': 'right', 'padding': '6px 16px', 'fontSize': '13px',
            'cursor': 'pointer', 'backgroundColor': '#3a3a0a', 'color': '#FFD700',
            'border': '1px solid #FFD700', 'borderRadius': '4px', 'marginTop': '3px',
            'marginRight': '8px'}),
        html.Button('VRP扫描', id='vrp-btn', n_clicks=0, style={
            'float': 'right', 'padding': '6px 16px', 'fontSize': '13px',
            'cursor': 'pointer', 'backgroundColor': '#0a3a0a', 'color': '#00FF88',
            'border': '1px solid #00FF88', 'borderRadius': '4px', 'marginTop': '3px',
            'marginRight': '8px'}),
        html.Button('今日计划', id='plan-btn', n_clicks=0, style={
            'float': 'right', 'padding': '6px 16px', 'fontSize': '13px',
            'cursor': 'pointer', 'backgroundColor': '#4a0a0a', 'color': '#ff9800',
            'border': '1px solid #ff9800', 'borderRadius': '4px', 'marginTop': '3px',
            'marginRight': '8px'}),
        html.Button('价差监控', id='spread-btn', n_clicks=0, style={
            'float': 'right', 'padding': '6px 16px', 'fontSize': '13px',
            'cursor': 'pointer', 'backgroundColor': '#1a0a3a', 'color': '#bb86fc',
            'border': '1px solid #bb86fc', 'borderRadius': '4px', 'marginTop': '3px',
            'marginRight': '8px'}),
    ], style={'backgroundColor': '#1a1a2e', 'padding': '15px 25px',
              'borderBottom': '3px solid #e94560'}),

    # 今日计划面板（默认隐藏）
    html.Div(id='plan-panel', style={'display': 'none'}),

    # VRP扫描面板（默认隐藏）
    html.Div(id='vrp-panel', style={'display': 'none'}),

    # 资讯面板（默认隐藏）
    html.Div(id='news-panel', style={'display': 'none'}),

    # 价差Z-Score监控面板（默认隐藏）
    html.Div(id='spread-panel', style={'display': 'none'}),

    # 预警统计面板（默认隐藏）
    html.Div(id='alert-stats-panel', style={'display': 'none'}),

    # 账户+持仓状态栏（由tick快照动态更新）
    html.Div(id='account-bar'),

    # 交易面板（动态多行，根据持久化状态恢复）
    html.Div([
        html.Div(id='trade-rows-container', children=[
            _build_trade_row(i) for i in range(max(1, len(_load_trade_state().get('selections', [])) or 1))
        ]),
        # 全局紧急停止保留为隐藏占位（回调引用需要）
        html.Button(id='emergency-stop-btn', n_clicks=0, style={'display': 'none'}),
        html.Span(id='emergency-stop-status', style={'display': 'none'}),
        dcc.Store(id='trade-row-count', data=max(1, len(_load_trade_state().get('selections', [])) or 1)),
    ], style={'padding': '8px 20px', 'backgroundColor': '#0d1117',
              'borderBottom': '1px solid #1a1a3e'}),

    # 添加期权对的输入区（品种前缀 + C行权价 + P行权价）
    html.Div([
        dcc.Input(
            id='prefix-input', type='text', placeholder='品种，如 ag2604',
            style={'width': '120px', 'padding': '8px 10px', 'fontSize': '14px',
                   'backgroundColor': '#1a1a3e', 'color': '#fff',
                   'border': '1px solid #e94560', 'borderRadius': '4px'},
            debounce=True,
        ),
        html.Span(' C', style={'color': '#00FF88', 'fontSize': '15px', 'fontWeight': 'bold',
                                'margin': '0 4px 0 10px'}),
        dcc.Input(
            id='leg1-input', type='text', placeholder='行权价',
            style={'width': '90px', 'padding': '8px 10px', 'fontSize': '14px',
                   'backgroundColor': '#1a1a3e', 'color': '#fff',
                   'border': '1px solid #444', 'borderRadius': '4px'},
            debounce=True,
        ),
        html.Span(' × ', style={'color': '#FFD700', 'fontSize': '16px', 'fontWeight': 'bold',
                                  'margin': '0 4px'}),
        dcc.Input(
            id='leg1-coeff', type='text', value='1.0',
            style={'width': '50px', 'padding': '8px 6px', 'fontSize': '14px',
                   'backgroundColor': '#1a1a3e', 'color': '#fff',
                   'border': '1px solid #444', 'borderRadius': '4px', 'textAlign': 'center'},
        ),
        html.Span(' + P', style={'color': '#FF6B6B', 'fontSize': '15px', 'fontWeight': 'bold',
                                   'margin': '0 4px 0 10px'}),
        dcc.Input(
            id='leg2-input', type='text', placeholder='行权价',
            style={'width': '90px', 'padding': '8px 10px', 'fontSize': '14px',
                   'backgroundColor': '#1a1a3e', 'color': '#fff',
                   'border': '1px solid #444', 'borderRadius': '4px'},
            debounce=True,
        ),
        html.Span(' × ', style={'color': '#FFD700', 'fontSize': '16px', 'fontWeight': 'bold',
                                  'margin': '0 4px'}),
        dcc.Input(
            id='leg2-coeff', type='text', value='1.0',
            style={'width': '50px', 'padding': '8px 6px', 'fontSize': '14px',
                   'backgroundColor': '#1a1a3e', 'color': '#fff',
                   'border': '1px solid #444', 'borderRadius': '4px', 'textAlign': 'center'},
        ),
        html.Button('添加', id='add-btn', n_clicks=0, style={
            'padding': '8px 20px', 'fontSize': '14px', 'cursor': 'pointer',
            'backgroundColor': '#e94560', 'color': '#fff', 'border': 'none',
            'borderRadius': '4px', 'marginLeft': '15px'
        }),
        html.Span(id='add-msg', style={'color': '#aaa', 'fontSize': '13px', 'marginLeft': '10px'}),
    ], style={'padding': '15px 25px', 'backgroundColor': '#16213e',
              'display': 'flex', 'alignItems': 'center'}),

    # 图表容器
    html.Div(id='charts-container'),

    # 持久化存储
    dcc.Store(id='pairs-store', data=load_config()),
    dcc.Store(id='scorecard-collapsed', data=True),
    dcc.Store(id='loading-state', data={}),  # {product: start_timestamp}
    dcc.Store(id='trade-state', data=_load_trade_state()),  # 交易行选中状态持久化

    # 定时刷新
    dcc.Interval(id='timer', interval=REFRESH_MS, n_intervals=0),
    dcc.Interval(id='account-timer', interval=5000, n_intervals=0),
    dcc.Interval(id='load-timer', interval=2000, disabled=True),

    # 回到顶部悬浮按钮
    html.Button('TOP', id='back-to-top-btn', n_clicks=0, style={
        'position': 'fixed', 'bottom': '30px', 'right': '30px', 'zIndex': '9999',
        'width': '48px', 'height': '48px', 'borderRadius': '50%',
        'backgroundColor': '#e94560', 'color': '#fff', 'border': 'none',
        'fontSize': '12px', 'fontWeight': 'bold', 'cursor': 'pointer',
        'boxShadow': '0 4px 12px rgba(233,69,96,0.4)',
        'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center',
        'opacity': '0.85', 'transition': 'opacity 0.2s',
    }),

], style={'backgroundColor': '#0f0f23', 'minHeight': '100vh'})


app.layout = serve_layout  # 函数引用（非调用），每次页面加载都执行

# 回到顶部 — clientside callback（纯JS，零延迟）
app.clientside_callback(
    """
    function(n) {
        if (n > 0) { window.scrollTo({top: 0, behavior: 'smooth'}); }
        return window.dash_clientside.no_update;
    }
    """,
    Output('back-to-top-btn', 'style'),
    Input('back-to-top-btn', 'n_clicks'),
    prevent_initial_call=True,
)


@app.callback(
    Output('pairs-store', 'data'),
    Output('add-msg', 'children'),
    Output('leg1-input', 'value'),
    Output('leg2-input', 'value'),
    Input('add-btn', 'n_clicks'),
    Input({'type': 'del-btn', 'index': ALL}, 'n_clicks'),
    State('prefix-input', 'value'),
    State('leg1-input', 'value'),
    State('leg1-coeff', 'value'),
    State('leg2-input', 'value'),
    State('leg2-coeff', 'value'),
    State('pairs-store', 'data'),
    prevent_initial_call=True,
)
def modify_pairs(add_clicks, del_clicks, prefix, sym1, coeff1, sym2, coeff2, pairs):
    triggered = ctx.triggered_id

    # 删除按钮（必须有真实点击，防止定时器重建按钮时误触发）
    if isinstance(triggered, dict) and triggered.get('type') == 'del-btn':
        if not del_clicks or not any(c and c > 0 for c in del_clicks):
            return no_update, '', no_update, no_update
        idx = triggered['index']
        if 0 <= idx < len(pairs):
            removed = pairs.pop(idx)
            save_config(pairs)
            return pairs, f'已删除 {removed[0]}+{removed[1]}', no_update, no_update

    # 添加按钮
    if triggered == 'add-btn':
        if not sym1 or not sym2:
            return no_update, '请输入两腿行权价', no_update, no_update
        prefix = (prefix or '').strip()
        sym1 = sym1.strip()
        sym2 = sym2.strip()
        # 用户只输数字（行权价）→ 自动加C/P
        if re.match(r'^\d+$', sym1):
            sym1 = 'C' + sym1
        if re.match(r'^\d+$', sym2):
            sym2 = 'P' + sym2
        # 简写（如 C37600）→ 拼上品种前缀
        if prefix and re.match(r'^[CP]\d+$', sym1, re.IGNORECASE):
            sym1 = prefix + sym1
        if prefix and re.match(r'^[CP]\d+$', sym2, re.IGNORECASE):
            sym2 = prefix + sym2
        leg1 = normalize_symbol(sym1)
        leg2 = normalize_symbol(sym2)
        if not leg1:
            return no_update, f'期权1格式不对: {sym1}', no_update, no_update
        if not leg2:
            return no_update, f'期权2格式不对: {sym2}', no_update, no_update
        try:
            c1 = float(coeff1) if coeff1 else 1.0
        except (ValueError, TypeError):
            c1 = 1.0
        try:
            c2 = float(coeff2) if coeff2 else 1.0
        except (ValueError, TypeError):
            c2 = 1.0
        if c1 <= 0:
            c1 = 1.0
        if c2 <= 0:
            c2 = 1.0
        # 检查重复（合约+系数都相同才算重复）
        for pair in pairs:
            pc1 = pair[2] if len(pair) > 2 else 1.0
            pc2 = pair[3] if len(pair) > 3 else 1.0
            if pair[0] == leg1 and pair[1] == leg2 and pc1 == c1 and pc2 == c2:
                return no_update, f'{leg1}×{c1:g}+{leg2}×{c2:g} 已存在', no_update, no_update
        # 检查两腿价格比率
        # CTP夜盘数据用交易日日期存储(如周五夜盘存为下周一日期)，
        # 导致 ORDER BY datetime DESC 会优先取到陈旧的夜盘价格而非当前日盘价格。
        # 修复：同日期内日盘(hour<20)优先于夜盘(hour>=20)
        db = get_db()
        cur = db.cursor()
        cur.execute(_LATEST_PRICE_SQL, (leg1,))
        r1 = cur.fetchone()
        cur.execute(_LATEST_PRICE_SQL, (leg2,))
        r2 = cur.fetchone()
        if r1 and r2 and r1[0] > 0 and r2[0] > 0:
            p1, p2 = r1[0] * c1, r2[0] * c2
            add_ratio = max(p1, p2) / min(p1, p2)
            if add_ratio > 3.0:
                return no_update, f'两腿价差过大({p1:.0f} vs {p2:.0f}, {add_ratio:.1f}x)，不适合配对', no_update, no_update
        pairs.insert(0, [leg1, leg2, c1, c2])  # 新添加的排在最前面
        save_config(pairs)
        msg = f'已添加 {leg1}×{c1:g} + {leg2}×{c2:g}'
        if r1 and r2 and r1[0] > 0 and r2[0] > 0:
            p1, p2 = r1[0] * c1, r2[0] * c2
            add_ratio = max(p1, p2) / min(p1, p2)
            if add_ratio > 1.5:
                msg += f'  (⚠ 两腿比率{add_ratio:.1f}x，建议换更平衡的行权价)'
        return pairs, msg, '', ''

    return no_update, '', no_update, no_update


@app.callback(
    Output('pairs-store', 'data', allow_duplicate=True),
    Output('add-msg', 'children', allow_duplicate=True),
    Input({'type': 'adopt-btn', 'index': ALL}, 'n_clicks'),
    State('pairs-store', 'data'),
    prevent_initial_call=True,
)
def adopt_pair(adopt_clicks, pairs):
    """收藏按钮：把自动推荐的期权对加入手动列表"""
    # 防止定时器重建按钮时误触发：必须有真实点击(n_clicks>0)
    if not adopt_clicks or not any(c and c > 0 for c in adopt_clicks):
        return no_update, no_update
    triggered = ctx.triggered_id
    if not isinstance(triggered, dict) or triggered.get('type') != 'adopt-btn':
        return no_update, no_update
    key = triggered.get('index', '')
    if '|' not in key:
        return no_update, no_update
    call_sym, put_sym = key.split('|', 1)
    for p in pairs:
        if p[0] == call_sym and p[1] == put_sym:
            print(f'[adopt_pair] {call_sym}+{put_sym} 已存在于pairs中, 跳过')
            return no_update, f'{call_sym}+{put_sym} 已存在'
    pairs.insert(0, [call_sym, put_sym, 1.0, 1.0])  # 排在最前面
    save_config(pairs)
    print(f'[adopt_pair] 已收藏 {call_sym} + {put_sym}, 当前共{len(pairs)}对')
    return pairs, f'已收藏 {call_sym} + {put_sym}'


_STATE_DIR = os.path.expanduser('~/state')
_TRADE2026 = os.path.expanduser('~/Downloads/trade2026')


def _extract_product(symbol):
    """从合约代码提取品种代码，如 CF509C13000 → CF, ag2604C37600 → AG"""
    m = re.match(r'([a-zA-Z]+)', symbol)
    return m.group(1).upper() if m else ''


# ======== HTTP API 客户端 ========

def _get_strategy_api_url(product):
    """读 state/{product}_api_port 文件，返回 http://127.0.0.1:{port} 或 None"""
    port_file = os.path.join(_STATE_DIR, f'{product.upper()}_api_port')
    try:
        if os.path.exists(port_file):
            port = int(open(port_file).read().strip())
            return f'http://127.0.0.1:{port}'
    except Exception:
        pass
    return None


def _is_strategy_running(product):
    """检查策略是否在运行：优先 HTTP /status，fallback pgrep"""
    # 1) 优先 HTTP API
    url = _get_strategy_api_url(product)
    if url:
        try:
            import requests as _req
            resp = _req.get(f'{url}/status', timeout=2)
            if resp.status_code == 200 and resp.json().get('running', False):
                return True
        except Exception:
            pass
    # 2) Fallback: pgrep（兼容尚未升级的旧策略进程）
    try:
        import subprocess as _sp
        result = _sp.run(['pgrep', '-f', f'main.py live.*--product.*{product}'],
                         capture_output=True, text=True, timeout=3)
        return bool(result.stdout.strip())
    except Exception:
        return False


def _get_strategy_status(product):
    """获取策略详细状态"""
    url = _get_strategy_api_url(product)
    if not url:
        return None
    try:
        import requests as _req
        resp = _req.get(f'{url}/status', timeout=2)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def _send_strategy_command(product, endpoint, payload=None):
    """发送命令到策略 HTTP API，返回 (success, message)。无 API 时 fallback 文件"""
    url = _get_strategy_api_url(product)
    if url:
        try:
            import requests as _req
            resp = _req.post(f'{url}/{endpoint}', json=payload or {}, timeout=5)
        except Exception:
            # 连接失败（策略未启动等），fallback 文件
            return _send_strategy_command_file(product, endpoint, payload)
        # HTTP 已发送成功，不再 fallback 文件（避免重复下单）
        try:
            data = resp.json()
            if resp.status_code == 200 and data.get('ok'):
                return True, data.get('message', '成功')
            return False, data.get('error', f'HTTP {resp.status_code}')
        except Exception as e:
            # POST 已成功但响应解析失败，视为成功（命令已送达）
            return True, f'已发送(响应解析异常: {e})'
    # 无 API URL，用文件信号
    return _send_strategy_command_file(product, endpoint, payload)


def _send_strategy_command_file(product, endpoint, payload=None):
    """文件信号 fallback"""
    payload = payload or {}
    pc = product.upper()
    try:
        os.makedirs(_STATE_DIR, exist_ok=True)
        if endpoint == 'entry':
            sig_path = os.path.join(_STATE_DIR, f'.trigger_entry_{pc}')
            with open(sig_path, 'w') as f:
                json.dump(payload, f, ensure_ascii=False)
            return True, '已写入信号文件(fallback)'
        elif endpoint == 'close':
            sig_path = os.path.join(_STATE_DIR, f'.trigger_close_{pc}')
            with open(sig_path, 'w') as f:
                json.dump(payload, f, ensure_ascii=False)
            return True, '已写入信号文件(fallback)'
        elif endpoint == 'emergency_stop':
            es_path = os.path.join(_STATE_DIR, '.emergency_stop')
            action = (payload or {}).get('action', 'stop')
            if action == 'resume':
                if os.path.exists(es_path):
                    os.unlink(es_path)
                    return True, '已恢复(文件)'
                return True, '已在运行中(文件)'
            else:
                # action == 'stop': 幂等写入
                with open(es_path, 'w') as f:
                    from datetime import datetime as _dt
                    f.write(_dt.now().isoformat())
                return True, '已停止(文件)'
        elif endpoint == 'force_close_toggle':
            return False, '文件信号不支持强平切换，需HTTP API'
        return False, f'未知端点: {endpoint}'
    except Exception as e:
        return False, str(e)


def _stop_strategy(product):
    """强力杀死指定品种的旧策略进程（SIGTERM→等待→SIGKILL）"""
    import subprocess as _sp
    import signal as _sig
    pc = product.upper()
    try:
        result = _sp.run(['pgrep', '-f', f'main.py.*--product.*{pc}'],
                         capture_output=True, text=True, timeout=3)
        pids = [p.strip() for p in result.stdout.strip().split('\n') if p.strip()]
    except Exception:
        pids = []
    if not pids:
        return 0  # 无旧进程
    killed = 0
    for pid in pids:
        try:
            pid_int = int(pid)
            # 1) SIGTERM 优雅关闭
            os.kill(pid_int, _sig.SIGTERM)
            # 2) 等最多5秒
            for _ in range(10):
                time.sleep(0.5)
                try:
                    os.kill(pid_int, 0)  # 检查是否还活着
                except OSError:
                    break  # 已退出
            else:
                # 3) 还没退出，SIGKILL
                try:
                    os.kill(pid_int, _sig.SIGKILL)
                except OSError:
                    pass
            killed += 1
        except (ValueError, OSError):
            pass
    if killed > 0:
        # 等CTP释放旧会话（SIGKILL后需要更久）
        time.sleep(3)
    return killed


def _start_strategy_worker(product):
    """后台线程：先杀旧进程再启动新策略（阻塞操作，勿在回调中直接调用）"""
    import subprocess as _sp
    # 先杀旧进程
    killed = _stop_strategy(product)
    if killed > 0:
        # 被SIGKILL的进程需等CTP释放会话
        time.sleep(25)
    venv_python = os.path.join(_TRADE2026, '.venv/bin/python')
    main_py = os.path.join(_TRADE2026, 'main.py')
    log_file = os.path.join(_STATE_DIR, f'{product.lower()}_auto.log')
    cmd = f'cd {_TRADE2026} && nohup {venv_python} {main_py} live --product {product} --strategy strangle_sell -m auto >> {log_file} 2>&1 &'
    _sp.Popen(cmd, shell=True, cwd=_TRADE2026)


def _start_strategy(product):
    """非阻塞启动策略：后台线程先杀旧进程再启动新的"""
    import threading
    log_file = os.path.join(_STATE_DIR, f'{product.lower()}_auto.log')
    t = threading.Thread(target=_start_strategy_worker, args=(product,), daemon=True)
    t.start()
    return log_file


def _smart_round_volumes(total_volume, mode='entry'):
    """智能分轮：entry=条件单友好少轮次，exit=效率优先拆粗"""
    if total_volume <= 0:
        return []
    if total_volume <= 3:
        return [total_volume]
    if mode == 'exit':
        if total_volume <= 8:
            per = 3
        elif total_volume <= 20:
            per = 5
        else:
            per = 10
    else:
        if total_volume <= 6:
            per = 3
        elif total_volume <= 15:
            per = 5
        elif total_volume <= 30:
            per = 5
        else:
            per = 10
    rounds = []
    rem = total_volume
    while rem > 0:
        rounds.append(min(per, rem))
        rem -= rounds[-1]
    return rounds


# ========== 分轮计划实时显示 ==========
@app.callback(
    Output({'type': 'entry-split-plan', 'index': MATCH}, 'children'),
    Input({'type': 'entry-split-toggle', 'index': MATCH}, 'value'),
    Input({'type': 'trade-volume', 'index': MATCH}, 'value'),
    prevent_initial_call=True,
)
def show_entry_split_plan(toggle, volume):
    if not toggle or 'split' not in toggle or not volume or volume < 1:
        return ''
    rounds = _smart_round_volumes(int(volume), mode='entry')
    if len(rounds) <= 1:
        return html.Span('(不需分轮)', style={'color': '#888'})
    return f'{len(rounds)}轮{rounds}'


@app.callback(
    Output({'type': 'close-split-plan', 'index': MATCH}, 'children'),
    Input({'type': 'close-split-toggle', 'index': MATCH}, 'value'),
    Input({'type': 'close-volume', 'index': MATCH}, 'value'),
    prevent_initial_call=True,
)
def show_close_split_plan(toggle, volume):
    if not toggle or 'split' not in toggle or not volume or volume < 1:
        return ''
    rounds = _smart_round_volumes(int(volume), mode='exit')
    if len(rounds) <= 1:
        return html.Span('(不需分轮)', style={'color': '#888'})
    return f'{len(rounds)}轮{rounds}'


@app.callback(
    Output('emergency-stop-status', 'children'),
    Output({'type': 'trade-entry-status', 'index': ALL}, 'children', allow_duplicate=True),
    Output({'type': 'entry-condition-sum', 'index': ALL}, 'value', allow_duplicate=True),
    Input('emergency-stop-btn', 'n_clicks'),
    State({'type': 'entry-condition-sum', 'index': ALL}, 'value'),
    prevent_initial_call=True,
)
def on_emergency_stop_click(n_clicks, all_min_ask):
    """紧急停止/恢复：自动感知状态，运行中→停止，已停止→恢复。服务端幂等，快速连点安全。"""
    if not n_clicks:
        n_rows = len(all_min_ask) if all_min_ask else 0
        return no_update, [no_update] * n_rows, [no_update] * n_rows
    n_rows = len(all_min_ask) if all_min_ask else 0

    es_path = os.path.join(_STATE_DIR, '.emergency_stop')

    # 先查状态：文件优先（最可靠），HTTP辅助
    # 文件是 ground truth，因为HTTP可能超时/卡住
    any_stopped = os.path.exists(es_path)
    if not any_stopped:
        try:
            for f in os.listdir(_STATE_DIR):
                if f.endswith('_api_port'):
                    product = f.replace('_api_port', '')
                    status = _get_strategy_status(product)
                    if status and status.get('emergency_stopped'):
                        any_stopped = True
                        break
        except Exception:
            pass

    action = 'resume' if any_stopped else 'stop'
    payload = {'action': action}

    # 无论HTTP是否成功，都同步文件状态（文件是 ground truth）
    try:
        os.makedirs(_STATE_DIR, exist_ok=True)
        if action == 'stop':
            with open(es_path, 'w') as f:
                f.write(datetime.now().isoformat())
        else:
            if os.path.exists(es_path):
                os.unlink(es_path)
    except Exception:
        pass

    results = []
    tried_products = set()
    try:
        for f in os.listdir(_STATE_DIR):
            if f.endswith('_api_port'):
                product = f.replace('_api_port', '')
                tried_products.add(product)
                ok, msg = _send_strategy_command(product, 'emergency_stop', payload)
                if ok:
                    results.append(f'{product}:{msg}')
                else:
                    results.append(f'{product}:失败-{msg}')
    except Exception:
        pass

    if not tried_products:
        results.append('已停止(文件)' if action == 'stop' else '已恢复(文件)')

    ts = datetime.now().strftime("%H:%M:%S")
    if not results:
        return (html.Span(f'无运行中的策略 {ts}', style={'color': '#FFD700'}),
                [no_update] * n_rows, [no_update] * n_rows)

    summary = ', '.join(results)
    if action == 'stop':
        status_msg = html.Span(f'已停止！ {summary} ({ts})',
                               style={'color': '#FF4444', 'fontWeight': 'bold'})
        entry_statuses = [html.Span(f'条件单已取消 ({ts})', style={'color': '#FF8800', 'fontSize': '11px'})
                          for _ in range(n_rows)]
        ask_values = [None for _ in range(n_rows)]
        return status_msg, entry_statuses, ask_values
    else:
        status_msg = html.Span(f'已恢复运行 {summary} ({ts})',
                               style={'color': '#00FF88', 'fontWeight': 'bold'})
        return status_msg, [no_update] * n_rows, [no_update] * n_rows


@app.callback(
    Output({'type': 'emergency-status', 'index': MATCH}, 'children'),
    Output({'type': 'trade-entry-status', 'index': MATCH}, 'children', allow_duplicate=True),
    Output({'type': 'entry-condition-sum', 'index': MATCH}, 'value', allow_duplicate=True),
    Output({'type': 'trade-close-status', 'index': MATCH}, 'children'),
    Output({'type': 'close-condition-sum', 'index': MATCH}, 'value'),
    Input({'type': 'emergency-btn', 'index': MATCH}, 'n_clicks'),
    State({'type': 'trade-pair-select', 'index': MATCH}, 'value'),
    prevent_initial_call=True,
)
def on_row_emergency_stop(n_clicks, pair_json):
    """单行紧急停止：只停止当前行选定的品种"""
    no_upd = (no_update,) * 5
    if not n_clicks:
        return no_upd

    if not pair_json:
        return html.Span('请先选择期权对', style={'color': '#FF4444'}), no_update, no_update, no_update, no_update

    try:
        pair = json.loads(pair_json)
        product = _extract_product(pair['call'])
        if not product:
            return html.Span('无法识别品种', style={'color': '#FF4444'}), no_update, no_update, no_update, no_update

        # 感知状态：文件优先（最可靠），HTTP辅助
        es_path = os.path.join(_STATE_DIR, '.emergency_stop')
        is_stopped = os.path.exists(es_path)
        if not is_stopped:
            status = _get_strategy_status(product)
            is_stopped = bool(status and status.get('emergency_stopped'))
        action = 'resume' if is_stopped else 'stop'

        # 同步文件状态
        try:
            if action == 'stop':
                with open(es_path, 'w') as _f:
                    _f.write(datetime.now().isoformat())
            else:
                if os.path.exists(es_path):
                    os.unlink(es_path)
        except Exception:
            pass

        ok, msg = _send_strategy_command(product, 'emergency_stop', {'action': action})
        ts = datetime.now().strftime("%H:%M:%S")
        if ok:
            if action == 'stop':
                cancelled = html.Span(f'条件单已取消 ({ts})',
                                      style={'color': '#FF8800', 'fontSize': '11px'})
                return (html.Span(f'{product}已停止 ({ts})',
                                  style={'color': '#FF4444', 'fontWeight': 'bold'}),
                        cancelled, None, cancelled, None)
            else:
                return (html.Span(f'{product}已恢复 ({ts})',
                                  style={'color': '#00FF88', 'fontWeight': 'bold'}),
                        no_update, no_update, no_update, no_update)
        else:
            return (html.Span(f'{product}操作失败: {msg} ({ts})', style={'color': '#FFD700'}),
                    no_update, no_update, no_update, no_update)
    except Exception as e:
        return html.Span(f'错误: {e}', style={'color': '#FF4444'}), no_update, no_update, no_update, no_update


@app.callback(
    Output({'type': 'force-close-status', 'index': MATCH}, 'children'),
    Output({'type': 'force-close-btn', 'index': MATCH}, 'style'),
    Input({'type': 'force-close-btn', 'index': MATCH}, 'n_clicks'),
    State({'type': 'trade-pair-select', 'index': MATCH}, 'value'),
    prevent_initial_call=True,
)
def on_force_close_toggle(n_clicks, pair_json):
    """切换收盘前强平开关"""
    btn_enabled = {'padding': '3px 10px', 'fontSize': '12px', 'cursor': 'pointer',
                   'backgroundColor': '#1a3a1a', 'color': '#00FF88',
                   'border': '1px solid #00FF88', 'borderRadius': '4px', 'fontWeight': 'bold'}
    btn_disabled = {'padding': '3px 10px', 'fontSize': '12px', 'cursor': 'pointer',
                    'backgroundColor': '#4a3a00', 'color': '#FF8800',
                    'border': '1px solid #FF8800', 'borderRadius': '4px', 'fontWeight': 'bold'}
    if not n_clicks:
        return no_update, no_update
    if not pair_json:
        return html.Span('请先选择期权对', style={'color': '#FF4444'}), no_update
    try:
        pair = json.loads(pair_json)
        product = _extract_product(pair['call'])
        if not product:
            return html.Span('无法识别品种', style={'color': '#FF4444'}), no_update

        ok, msg = _send_strategy_command(product, 'force_close_toggle')
        if ok:
            # 读取配置中的强平时间（02:00:00 是占位符，用 scheduler 默认值）
            cfg = _load_commodity_config(product)
            tp = cfg.get('trading_params', {})
            fc_night = tp.get('force_close_start_night', '22:49:00')
            fc_day = tp.get('force_close_start_day', '14:49:00')
            if not fc_night or fc_night == '02:00:00':
                fc_night = '22:49:00'
            if not fc_day or fc_day == '02:00:00':
                fc_day = '14:49:00'
            fc_text = f'夜{fc_night[:5]}/日{fc_day[:5]}'

            if '启用' in msg:
                return (html.Span(f'{product}强平已启用 {fc_text}', style={'color': '#00FF88'}),
                        btn_enabled)
            else:
                return (html.Span(f'{product}强平已禁用', style={'color': '#FF8800', 'fontWeight': 'bold'}),
                        btn_disabled)
        else:
            return html.Span(f'{product}切换失败: {msg}', style={'color': '#FFD700'}), no_update
    except Exception as e:
        return html.Span(f'错误: {e}', style={'color': '#FF4444'}), no_update


@app.callback(
    Output({'type': 'trade-pair-select', 'index': ALL}, 'options'),
    Output({'type': 'trade-pair-select', 'index': ALL}, 'value'),
    Input('pairs-store', 'data'),
    State('trade-state', 'data'),
)
def update_trade_pair_options(pairs, trade_state):
    """根据工作台期权对列表更新所有交易行下拉选项，并恢复选中值"""
    if not pairs:
        options = []
    else:
        options = []
        for pair in pairs:
            call_sym, put_sym = pair[0], pair[1]
            label = f'{call_sym} + {put_sym}'
            value = json.dumps({'call': call_sym, 'put': put_sym})
            options.append({'label': label, 'value': value})
    from dash import callback_context
    n_outputs = len(callback_context.outputs_list[0])
    # 恢复选中值
    saved_selections = (trade_state or {}).get('selections', [])
    valid_values = {o['value'] for o in options}
    values = []
    for i in range(n_outputs):
        if i < len(saved_selections) and saved_selections[i] in valid_values:
            values.append(saved_selections[i])
        else:
            values.append(no_update)
    return [options] * max(n_outputs, 1), values


@app.callback(
    Output('trade-state', 'data'),
    Input({'type': 'trade-pair-select', 'index': ALL}, 'value'),
    prevent_initial_call=True,
)
def save_trade_selections(all_values):
    """选择配对时自动保存到文件，刷新后恢复"""
    state = {'selections': list(all_values or [])}
    _save_trade_state(state)
    return state


_LOAD_READY_SECONDS = 35  # 策略启动后等待秒数


@app.callback(
    Output('loading-state', 'data'),
    Output('load-timer', 'disabled'),
    Input({'type': 'load-btn', 'index': ALL}, 'n_clicks'),
    State({'type': 'trade-pair-select', 'index': ALL}, 'value'),
    State('loading-state', 'data'),
    prevent_initial_call=True,
)
def on_load_click(all_clicks, all_pairs, loading_state):
    """点击加载：启动策略实例"""
    triggered = ctx.triggered_id
    if not isinstance(triggered, dict) or triggered.get('type') != 'load-btn':
        return no_update, no_update
    idx = triggered['index']
    # 找到对应行的pair
    pair_json = None
    for i, clicks_list in enumerate(all_clicks or []):
        btn_id = ctx.inputs_list[0][i]['id']
        if isinstance(btn_id, dict) and btn_id.get('index') == idx:
            # 找对应的pair
            pair_json = all_pairs[i] if i < len(all_pairs) else None
            break
    if not pair_json:
        return no_update, no_update
    try:
        pair = json.loads(pair_json)
        product = _extract_product(pair['call'])
    except Exception:
        return no_update, no_update
    if not product:
        return no_update, no_update

    # 始终先杀旧进程再启动新的（解决CTP会话残留问题）
    _start_strategy(product)

    loading_state[product] = time.time()
    return loading_state, False  # 启用load-timer


@app.callback(
    Output({'type': 'load-status', 'index': ALL}, 'children'),
    Output('load-timer', 'disabled', allow_duplicate=True),
    Input('load-timer', 'n_intervals'),
    State('loading-state', 'data'),
    State({'type': 'trade-pair-select', 'index': ALL}, 'value'),
    prevent_initial_call=True,
)
def update_load_status(_, loading_state, all_pairs):
    """定时更新加载状态"""
    if not loading_state:
        return [no_update] * len(all_pairs), True

    results = []
    any_loading = False
    for pair_json in (all_pairs or []):
        if not pair_json:
            results.append('')
            continue
        try:
            pair = json.loads(pair_json)
            product = _extract_product(pair['call'])
        except Exception:
            results.append('')
            continue

        if product not in loading_state:
            # 检查是否已有运行中的策略（之前手动启动的）
            if _is_strategy_running(product):
                results.append(html.Span('已就绪', style={'color': '#00FF88', 'fontWeight': 'bold'}))
            else:
                results.append('')
            continue

        elapsed = time.time() - loading_state[product]
        if elapsed >= _LOAD_READY_SECONDS and _is_strategy_running(product):
            results.append(html.Span('已就绪', style={'color': '#00FF88', 'fontWeight': 'bold'}))
        else:
            sec = int(elapsed)
            results.append(html.Span(f'加载中 {sec}s...', style={'color': '#4fc3f7'}))
            any_loading = True

    disable_timer = not any_loading
    return results, disable_timer


@app.callback(
    Output({'type': 'entry-condition-label', 'index': MATCH}, 'children'),
    Input({'type': 'entry-direction', 'index': MATCH}, 'value'),
    prevent_initial_call=True,
)
def update_entry_condition_label(direction):
    """进仓方向变化时更新条件标签：卖出→Ask≥，买入→Bid≤"""
    return 'Bid≤' if direction == 'buy' else 'Ask≥'


@app.callback(
    Output({'type': 'close-condition-label', 'index': MATCH}, 'children'),
    Input({'type': 'close-direction', 'index': MATCH}, 'value'),
    prevent_initial_call=True,
)
def update_close_condition_label(direction):
    """平仓方向变化时更新条件标签：卖出平→Ask≥，买入平→Bid≤"""
    return 'Ask≥' if direction == 'sell_close' else 'Bid≤'


@app.callback(
    Output({'type': 'trade-entry-result', 'index': MATCH}, 'children'),
    Input({'type': 'trade-entry-btn', 'index': MATCH}, 'n_clicks'),
    State({'type': 'trade-pair-select', 'index': MATCH}, 'value'),
    State({'type': 'trade-volume', 'index': MATCH}, 'value'),
    State({'type': 'entry-condition-sum', 'index': MATCH}, 'value'),
    State({'type': 'entry-direction', 'index': MATCH}, 'value'),
    State({'type': 'entry-split-toggle', 'index': MATCH}, 'value'),
    prevent_initial_call=True,
)
def on_trade_entry_click(n_clicks, pair_json, volume, condition_sum, entry_direction, split_toggle):
    """进仓按钮：POST /entry 到策略 HTTP API，支持卖出/买入方向"""
    print(f"[DEBUG 进仓] n_clicks={n_clicks}, entry_direction={entry_direction!r}, volume={volume}, split={split_toggle}")
    if not n_clicks:
        return no_update
    if not pair_json:
        return html.Span('请先选择期权对', style={'color': '#FF4444'})
    if not volume or volume < 1:
        return html.Span('请输入有效手数', style={'color': '#FF4444'})

    try:
        pair = json.loads(pair_json)
        product = _extract_product(pair['call'])
        if not product:
            return html.Span('无法识别品种', style={'color': '#FF4444'})

        # 检查策略是否已就绪（HTTP /status）
        if not _is_strategy_running(product):
            return html.Span(f'请先点击"加载"启动{product}策略', style={'color': '#FF4444'})

        # 检查紧急停止状态（HTTP API 或 fallback 文件）
        status = _get_strategy_status(product)
        _es = (status.get('emergency_stopped') if status else
               os.path.exists(os.path.join(_STATE_DIR, '.emergency_stop')))
        if _es:
            return html.Span('系统已紧急停止，无法进仓', style={'color': '#FF4444'})

        # loading_state检查已移除（Dash 4.0不支持MATCH回调混用普通ID State）

        direction = entry_direction or 'sell'
        dir_label = '买入' if direction == 'buy' else '卖出'
        use_split = split_toggle and 'split' in split_toggle
        rounds = _smart_round_volumes(int(volume)) if use_split else [int(volume)]
        payload = {
            'call': pair['call'],
            'put': pair['put'],
            'volume': int(volume),
            'direction': direction,
            'round_volumes': rounds,
        }
        # 条件进仓：卖出→Ask≥，买入→Bid≤
        if condition_sum and condition_sum > 0:
            if direction == 'buy':
                payload['max_bid_sum'] = float(condition_sum)
            else:
                payload['min_ask_sum'] = float(condition_sum)

        ok, msg = _send_strategy_command(product, 'entry', payload)
        if ok:
            ts = datetime.now().strftime("%H:%M:%S")
            if condition_sum and condition_sum > 0:
                cond_label = f'Bid≤{condition_sum}' if direction == 'buy' else f'Ask≥{condition_sum}'
                return html.Span(
                    f'条件挂单 {cond_label} {dir_label}各{volume}手 '
                    f'{pair["call"]}+{pair["put"]} ({ts})',
                    style={'color': '#FFD700'})
            split_info = f' 分{len(rounds)}轮{rounds}' if len(rounds) > 1 else ''
            return html.Span(
                f'已发送 {dir_label}各{volume}手{split_info} '
                f'{pair["call"]}+{pair["put"]} ({ts})',
                style={'color': '#00FF88'})
        else:
            return html.Span(f'发送失败: {msg}', style={'color': '#FF4444'})
    except Exception as e:
        return html.Span(f'发送失败: {e}', style={'color': '#FF4444'})


@app.callback(
    Output({'type': 'trade-close-result', 'index': MATCH}, 'children'),
    Input({'type': 'trade-close-btn', 'index': MATCH}, 'n_clicks'),
    State({'type': 'trade-pair-select', 'index': MATCH}, 'value'),
    State({'type': 'close-volume', 'index': MATCH}, 'value'),
    State({'type': 'close-condition-sum', 'index': MATCH}, 'value'),
    State({'type': 'close-direction', 'index': MATCH}, 'value'),
    State({'type': 'close-split-toggle', 'index': MATCH}, 'value'),
    prevent_initial_call=True,
)
def on_trade_close_click(n_clicks, pair_json, volume, condition_sum, close_direction, split_toggle):
    """平仓按钮：POST /close 到策略 HTTP API，支持买入/卖出方向"""
    print(f'[CLOSE-DEBUG] n_clicks={n_clicks}, pair_json={pair_json!r}, volume={volume}, direction={close_direction}', flush=True)
    if not n_clicks:
        print('[CLOSE-DEBUG] → no_update (n_clicks falsy)', flush=True)
        return no_update
    if not pair_json:
        return html.Span('请先选择期权对', style={'color': '#FF4444'})
    if not volume or volume < 1:
        return html.Span('请输入有效手数', style={'color': '#FF4444'})

    try:
        pair = json.loads(pair_json)
        product = _extract_product(pair['call'])
        if not product:
            print('[CLOSE-DEBUG] → 无法识别品种', flush=True)
            return html.Span('无法识别品种', style={'color': '#FF4444'})
        running = _is_strategy_running(product)
        print(f'[CLOSE-DEBUG] product={product}, running={running}', flush=True)
        if not running:
            print(f'[CLOSE-DEBUG] → 策略未运行，返回提示', flush=True)
            return html.Span(f'{product}策略未运行，无法平仓', style={'color': '#FF4444'})

        # 检查紧急停止状态（HTTP API 或 fallback 文件）
        status = _get_strategy_status(product)
        _es = (status.get('emergency_stopped') if status else
               os.path.exists(os.path.join(_STATE_DIR, '.emergency_stop')))
        if _es:
            print('[CLOSE-DEBUG] → 系统已紧急停止', flush=True)
            return html.Span('系统已紧急停止', style={'color': '#FF4444'})

        direction = close_direction or 'buy_close'
        dir_label = '卖出' if direction == 'sell_close' else '买入'
        use_split = split_toggle and 'split' in split_toggle
        rounds = _smart_round_volumes(int(volume), mode='exit') if use_split else [int(volume)]
        payload = {
            'call': pair['call'],
            'put': pair['put'],
            'volume': int(volume),
            'direction': direction,
            'round_volumes': rounds,
        }
        # 条件平仓：买入平→Bid≤，卖出平→Ask≥
        if condition_sum and condition_sum > 0:
            if direction == 'sell_close':
                payload['min_ask_sum'] = float(condition_sum)
            else:
                payload['max_bid_sum'] = float(condition_sum)

        ok, msg = _send_strategy_command(product, 'close', payload)
        print(f'[CLOSE-DEBUG] → send_command ok={ok}, msg={msg}', flush=True)
        if ok:
            ts = datetime.now().strftime("%H:%M:%S")
            if condition_sum and condition_sum > 0:
                cond_label = f'Ask≥{condition_sum}' if direction == 'sell_close' else f'Bid≤{condition_sum}'
                return html.Span(
                    f'条件平仓 {cond_label} {dir_label}平仓各{volume}手 '
                    f'{pair["call"]}+{pair["put"]} ({ts})',
                    style={'color': '#FFD700'})
            split_info = f' 分{len(rounds)}轮{rounds}' if len(rounds) > 1 else ''
            return html.Span(
                f'已发送 {dir_label}平仓各{volume}手{split_info} '
                f'({ts})',
                style={'color': '#FF6B6B'})
        else:
            return html.Span(f'发送失败: {msg}', style={'color': '#FF4444'})
    except Exception as e:
        import traceback as _tb
        _tb.print_exc()
        print(f'[CLOSE-DEBUG] → 异常: {e}', flush=True)
        return html.Span(f'发送失败: {e}', style={'color': '#FF4444'})


@app.callback(
    Output('trade-rows-container', 'children'),
    Output('trade-row-count', 'data'),
    Input('add-trade-row-btn', 'n_clicks'),
    Input({'type': 'remove-trade-row-btn', 'index': ALL}, 'n_clicks'),
    State('trade-rows-container', 'children'),
    State('trade-row-count', 'data'),
    prevent_initial_call=True,
)
def manage_trade_rows(add_clicks, remove_clicks, current_rows, row_count):
    """点+添加交易行，点×删除对应行"""
    triggered = ctx.triggered_id
    if triggered == 'add-trade-row-btn':
        new_idx = row_count
        new_rows = (current_rows or []) + [_build_trade_row(new_idx)]
        return new_rows, row_count + 1
    elif isinstance(triggered, dict) and triggered.get('type') == 'remove-trade-row-btn':
        rm_idx = triggered['index']
        kept = []
        for r in (current_rows or []):
            # 获取行的id（可能是dict或Dash组件）
            row_id = r.get('props', {}).get('id', {}) if isinstance(r, dict) else getattr(r, 'id', {})
            if isinstance(row_id, dict) and row_id.get('type') == 'trade-row' and row_id.get('index') == rm_idx:
                continue
            kept.append(r)
        if not kept:
            kept = [_build_trade_row(0)]
        return kept, row_count
    return no_update, no_update


@app.callback(
    Output('account-bar', 'children'),
    Input('account-timer', 'n_intervals'),
)
def update_account_bar(_):
    return _build_account_bar()


@app.callback(
    Output('charts-container', 'children'),
    Input('pairs-store', 'data'),
    Input('timer', 'n_intervals'),
)
def render_charts(pairs, _):
    manual_pairs = pairs or []
    try:
        auto_pairs_raw = auto_select_pairs()
    except Exception:
        auto_pairs_raw = []

    # 排除与手动对重复的品种
    manual_futures = set()
    for p in manual_pairs:
        fs = _extract_futures_symbol(p[0])
        if fs:
            manual_futures.add(fs)
    auto_pairs = [ap for ap in auto_pairs_raw if ap['futures_sym'] not in manual_futures][:20]

    # 注入今日计划精选的OTM宽跨推荐（Top5，排除已有品种）
    # 懒初始化：如果还没点过今日计划，自动触发一次v6匹配
    if not _v6_recommended_pairs:
        try:
            toggle_plan(1)  # 填充 _v6_recommended_pairs
        except Exception:
            pass
    existing_futures = manual_futures | {ap['futures_sym'] for ap in auto_pairs}
    for vp in _v6_recommended_pairs[:5]:
        _d = re.search(r'(\d{3,4})$', vp['contract'])
        if not _d:
            continue
        _digits = _d.group(1)
        if vp['ex'] == 'CZCE':
            fut_sym = f"{vp['prod'].upper()}{_digits[-3:]}"
        else:
            fut_sym = f"{vp['prod'].lower()}{_digits}"
        if fut_sym in existing_futures:
            continue
        auto_pairs.insert(0, {
            'call': vp['call_sym'], 'put': vp['put_sym'],
            'product': vp['prod'], 'month': vp['contract'][-4:],
            'score': vp['composite'], 'price_sum': vp['psum'],
            'futures_sym': fut_sym,
            'v6_tag': f"v6 Sh{vp['sharpe']:.0f} WR{vp['wr']:.0f}%",
        })
        existing_futures.add(fut_sym)

    if not manual_pairs and not auto_pairs:
        return html.Div('暂无数据，等待数据采集...',
                         style={'color': '#666', 'padding': '50px', 'textAlign': 'center',
                                'fontSize': '16px'})

    # ---- 构建所有图表和摘要 ----
    manual_items = []
    for i, pair in enumerate(manual_pairs):
        call_sym, put_sym = pair[0], pair[1]
        cc = pair[2] if len(pair) > 2 else 1.0
        pc = pair[3] if len(pair) > 3 else 1.0
        try:
            fig, info = build_figure(call_sym, put_sym, cc, pc)
        except Exception:
            fig, info = go.Figure(), {'sum': None, 'futures_sym': None, 'double_rise': {'alert': False}}
        manual_items.append((i, pair, fig, info, cc, pc))

    auto_items = []
    for i, ap in enumerate(auto_pairs):
        try:
            fig, info = build_figure(ap['call'], ap['put'])
        except Exception:
            continue
        auto_items.append((i, ap, fig, info))

    # ---- 布林线警报检测：收集所有触发警报的项 ----
    alert_manual = [(idx, pair, fig, info, cc, pc) for idx, pair, fig, info, cc, pc in manual_items
                     if info.get('double_rise', {}).get('alert')]
    alert_auto = [(idx, ap, fig, info) for idx, ap, fig, info in auto_items
                   if info.get('double_rise', {}).get('alert')]
    # 非警报项保持原序
    normal_manual = [(idx, pair, fig, info, cc, pc) for idx, pair, fig, info, cc, pc in manual_items
                      if not info.get('double_rise', {}).get('alert')]
    normal_auto = [(idx, ap, fig, info) for idx, ap, fig, info in auto_items
                    if not info.get('double_rise', {}).get('alert')]

    # ---- 预警历史记录 & 回归追踪 ----
    for idx, pair, fig, info, cc, pc in manual_items:
        call_sym, put_sym = pair[0], pair[1]
        dr = info.get('double_rise', {})
        boll_mid = dr.get('boll_middle', 0)
        s = info.get('sum')
        if dr.get('alert'):
            record_alert(call_sym, put_sym, info.get('futures_sym'), dr, s or 0, boll_mid)
        elif s is not None and boll_mid > 0:
            check_alert_resolved(call_sym, put_sym, s, boll_mid)

    for idx, ap, fig, info in auto_items:
        call_sym, put_sym = ap.get('call', ''), ap.get('put', '')
        dr = info.get('double_rise', {})
        boll_mid = dr.get('boll_middle', 0)
        s = info.get('sum')
        if dr.get('alert'):
            record_alert(call_sym, put_sym, ap.get('product', '?'), dr, s or 0, boll_mid)
        elif s is not None and boll_mid > 0:
            check_alert_resolved(call_sym, put_sym, s, boll_mid)

    # ---- 补充回归检查：不在当前监控列表中的活跃预警，主动从DB查价格 ----
    checked_keys = set()
    for _, pair, _, info, _, _ in manual_items:
        checked_keys.add(f'{pair[0]}|{pair[1]}')
    for _, ap, _, info in auto_items:
        checked_keys.add(f'{ap.get("call","")}|{ap.get("put","")}')

    unchecked = {k: v for k, v in _alert_active.items() if k not in checked_keys and not v.get('resolved')}
    if unchecked:
        try:
            _adb = get_db()
            _acur = _adb.cursor()
            for akey, arec in list(unchecked.items()):
                csym, psym = arec.get('call_sym', ''), arec.get('put_sym', '')
                boll_mid = arec.get('boll_middle', 0)
                if not csym or not psym or boll_mid <= 0:
                    continue
                _acur.execute(_LATEST_PRICE_SQL, (csym,))
                cr = _acur.fetchone()
                _acur.execute(_LATEST_PRICE_SQL, (psym,))
                pr = _acur.fetchone()
                if cr and pr and cr[0] > 0 and pr[0] > 0:
                    cur_sum = cr[0] + pr[0]
                    check_alert_resolved(csym, psym, cur_sum, boll_mid)
        except Exception as _ae:
            print(f'[alert_history] 补充回归检查失败: {_ae}')

    # ---- Gamma/Strangle 每日检查 (暂时禁用排查渲染问题) ----
    scorecard_panel, scorecard_nav_items = None, []

    # 预计算：警报的索引集合（供导航栏和图表区共用）
    alert_manual_indices = {item[0] for item in alert_manual}
    alert_auto_indices = {item[0] for item in alert_auto}
    non_alert_auto = [(idx, ap, fig, info) for idx, ap, fig, info in auto_items
                       if idx not in alert_auto_indices]

    # ---- 左侧导航栏 ----
    nav_style = {'display': 'block', 'padding': '8px 12px', 'textDecoration': 'none',
                 'borderLeft': '3px solid transparent', 'cursor': 'pointer'}
    nav = []

    # 每日检查导航（sidebar顶部）
    if scorecard_nav_items:
        nav.append(html.Div('每日检查', style={
            'padding': '8px 12px', 'color': '#9D4EDD', 'fontSize': '11px',
            'fontWeight': 'bold', 'textTransform': 'uppercase', 'letterSpacing': '1px',
            'borderBottom': '2px solid #9D4EDD'}))
        for item in scorecard_nav_items:
            nav.append(html.A([
                html.Div(item['product'], style={
                    'fontWeight': 'bold', 'fontSize': '13px', 'color': item['color']}),
                html.Div(f'{item["score"]:.1f}分', style={'fontSize': '11px', 'color': '#aaa'}),
            ], href='#scorecard-panel', className='nav-item', style=nav_style))

    # 布林线警报导航
    all_alerts = alert_manual + alert_auto
    if all_alerts:
        nav.append(html.Div('布林线警报', style={
            'padding': '8px 12px', 'color': '#FF4444', 'fontSize': '11px',
            'fontWeight': 'bold', 'textTransform': 'uppercase', 'letterSpacing': '1px',
            'borderBottom': '2px solid #FF4444', 'animation': 'none'}))
        for item in all_alerts:
            if len(item) == 6:  # manual
                idx, pair, fig, info, cc, pc = item
                fs = info.get('futures_sym') or '?'
                dr = info.get('double_rise', {})
                nav.append(html.A([
                    html.Div(f'⚠ {fs}', style={'fontWeight': 'bold', 'fontSize': '13px', 'color': '#FF4444'}),
                    html.Div(f'+{dr.get("sum_chg", 0)*100:.1f}%',
                             style={'fontSize': '11px', 'color': '#FF8888'}),
                ], href=f'#alert-m-{idx}', className='nav-item', style=nav_style))
            else:  # auto
                idx, ap, fig, info = item
                dr = info.get('double_rise', {})
                nav.append(html.A([
                    html.Div(f'⚠ {ap["product"]}', style={'fontWeight': 'bold', 'fontSize': '13px', 'color': '#FF4444'}),
                    html.Div(f'+{dr.get("sum_chg", 0)*100:.1f}%',
                             style={'fontSize': '11px', 'color': '#FF8888'}),
                ], href=f'#alert-a-{idx}', className='nav-item', style=nav_style))

    if normal_manual:
        nav.append(html.Div('自选', style={
            'padding': '8px 12px', 'color': '#e94560', 'fontSize': '11px',
            'fontWeight': 'bold', 'textTransform': 'uppercase', 'letterSpacing': '1px',
            'borderBottom': '1px solid #e94560',
            'marginTop': '6px' if all_alerts else '0'}))
        for idx, pair, fig, info, cc, pc in manual_items:
            fs = info.get('futures_sym') or '?'
            s = info.get('sum')
            is_alert = idx in alert_manual_indices
            color = '#FF4444' if is_alert else '#FFD700'
            anchor = f'alert-m-{idx}' if is_alert else f'm-chart-{idx}'
            nav.append(html.A([
                html.Div(fs, style={'fontWeight': 'bold', 'fontSize': '13px', 'color': color}),
                html.Div(f'{s:.1f}' if s else '--', style={'fontSize': '11px', 'color': '#aaa'}),
            ], href=f'#{anchor}', className='nav-item', style=nav_style))

    if non_alert_auto:
        nav.append(html.Div('智能推荐', style={
            'padding': '8px 12px', 'color': '#00FF88', 'fontSize': '11px',
            'fontWeight': 'bold', 'textTransform': 'uppercase', 'letterSpacing': '1px',
            'borderBottom': '1px solid #00FF88', 'marginTop': '6px'}))
        for idx, ap, fig, info in non_alert_auto:
            tag = ' v6' if ap.get('v6_tag') else ''
            label_color = '#FF69B4' if tag else '#00FF88'
            nav.append(html.A([
                html.Div(f'{ap["product"]}{tag}', style={'fontWeight': 'bold', 'fontSize': '13px', 'color': label_color}),
                html.Div(f'{ap["score"]:.1f}分  {ap["price_sum"]:.0f}',
                         style={'fontSize': '11px', 'color': '#aaa'}),
            ], href=f'#a-chart-{idx}', className='nav-item', style=nav_style))

    sidebar = html.Div(nav, style={
        'width': '110px', 'minWidth': '110px', 'backgroundColor': '#1a1a2e',
        'borderRight': '2px solid #2a2a4a', 'position': 'sticky', 'top': '10px',
        'alignSelf': 'flex-start', 'borderRadius': '4px', 'marginRight': '8px',
        'maxHeight': '95vh', 'overflowY': 'auto'})

    # ---- 图表区 ----
    charts = []
    graph_cfg = {'scrollZoom': True, 'displayModeBar': False}

    # 辅助函数：构建手动图表卡片
    def _manual_card(idx, pair, fig, info, cc, pc, is_alert=False):
        call_sym, put_sym = pair[0], pair[1]
        l1 = f'{call_sym}*{cc:g}' if cc != 1.0 else call_sym
        l2 = f'{put_sym}*{pc:g}' if pc != 1.0 else put_sym
        dr = info.get('double_rise', {})
        header_parts = [
            html.Span(f'{l1}  +  {l2}',
                       style={'color': '#FFD700', 'fontSize': '15px', 'fontWeight': 'bold'}),
        ]
        # 两腿失衡警告
        leg_ratio = info.get('leg_ratio', 1)
        if leg_ratio > 1.5:
            cl = info.get('call_last', 0)
            pl = info.get('put_last', 0)
            severity = '严重失衡' if leg_ratio > 3 else '失衡'
            header_parts.append(html.Span(
                f'  ⚠ {severity} C={cl:.0f} P={pl:.0f} ({leg_ratio:.1f}x) 建议删除换对',
                style={'color': '#FF8800', 'fontSize': '12px', 'marginLeft': '10px',
                       'backgroundColor': 'rgba(255,136,0,0.15)', 'padding': '2px 8px',
                       'borderRadius': '4px'}))
        if is_alert:
            boll_info = f'  突破上轨{dr.get("boll_upper", 0):.0f}' if dr.get('method') == 'bollinger' else ''
            header_parts.append(html.Span(
                f'  布林突破{boll_info}'
                f'  (C +{dr.get("call_chg", 0)*100:.1f}%, P +{dr.get("put_chg", 0)*100:.1f}%)',
                style={'color': '#FF4444', 'fontSize': '13px', 'fontWeight': 'bold', 'marginLeft': '10px'}))
        header_parts.append(html.Button('\u2715', id={'type': 'del-btn', 'index': idx}, n_clicks=0, style={
            'float': 'right', 'backgroundColor': 'transparent', 'color': '#e94560',
            'border': '1px solid #e94560', 'borderRadius': '3px', 'cursor': 'pointer',
            'padding': '2px 8px', 'fontSize': '12px'}))
        if is_alert:
            adopt_id = f'{call_sym}|{put_sym}'
            header_parts.append(html.Button('\u2713', id={'type': 'adopt-btn', 'index': adopt_id}, n_clicks=0, style={
                'float': 'right', 'backgroundColor': 'transparent', 'color': '#00FF88',
                'border': '1px solid #00FF88', 'borderRadius': '3px', 'cursor': 'pointer',
                'padding': '2px 8px', 'fontSize': '14px', 'fontWeight': 'bold', 'marginLeft': '6px'}))
        border_color = '#FF4444' if is_alert else '#2a2a4a'
        div_id = f'alert-m-{idx}' if is_alert else f'm-chart-{idx}'
        header_parts.extend(_make_advisory_spans(call_sym, put_sym))
        card_children = [
            html.Div(header_parts, style={'padding': '10px 20px', 'backgroundColor': '#1a1a2e'}),
            dcc.Graph(figure=fig, config=graph_cfg),
        ]
        return html.Div(card_children,
            id=div_id, style={'marginBottom': '8px', 'borderBottom': f'2px solid {border_color}',
                              'borderLeft': f'3px solid {border_color}' if is_alert else 'none'})

    # 辅助函数：构建自动推荐图表卡片
    def _auto_card(idx, ap, fig, info, is_alert=False):
        dr = info.get('double_rise', {})
        adopt_id = f'{ap["call"]}|{ap["put"]}'
        header_parts = [
            html.Span(f'{ap["call"]}  +  {ap["put"]}',
                       style={'color': '#00FF88', 'fontSize': '14px', 'fontWeight': 'bold'}),
            html.Span(f'  {ap["score"]:.1f}分',
                       style={'color': '#FFD700', 'fontSize': '13px', 'marginLeft': '10px'}),
        ]
        if ap.get('v6_tag'):
            header_parts.append(html.Span(
                f'  {ap["v6_tag"]}',
                style={'color': '#FF69B4', 'fontSize': '12px', 'marginLeft': '8px',
                       'backgroundColor': '#2a1a2a', 'padding': '2px 6px', 'borderRadius': '3px'}))
        if is_alert:
            boll_info = f'  突破上轨{dr.get("boll_upper", 0):.0f}' if dr.get('method') == 'bollinger' else ''
            header_parts.append(html.Span(
                f'  布林突破{boll_info}'
                f'  (C +{dr.get("call_chg", 0)*100:.1f}%, P +{dr.get("put_chg", 0)*100:.1f}%)',
                style={'color': '#FF4444', 'fontSize': '13px', 'fontWeight': 'bold', 'marginLeft': '10px'}))
        header_parts.append(html.Button('\u2713', id={'type': 'adopt-btn', 'index': adopt_id}, n_clicks=0, style={
            'float': 'right', 'backgroundColor': 'transparent', 'color': '#00FF88',
            'border': '1px solid #00FF88', 'borderRadius': '3px', 'cursor': 'pointer',
            'padding': '2px 8px', 'fontSize': '14px', 'fontWeight': 'bold', 'marginLeft': '6px'}))
        border_color = '#FF4444' if is_alert else '#2a2a4a'
        div_id = f'alert-a-{idx}' if is_alert else f'a-chart-{idx}'
        header_parts.extend(_make_advisory_spans(ap['call'], ap.get('put')))
        card_children = [
            html.Div(header_parts, style={'padding': '10px 20px', 'backgroundColor': '#1a1a2e'}),
            dcc.Graph(figure=fig, config=graph_cfg),
        ]
        return html.Div(card_children,
            id=div_id, style={'marginBottom': '8px', 'borderBottom': f'2px solid {border_color}',
                              'borderLeft': f'3px solid {border_color}' if is_alert else 'none'})

    # 0) Gamma/Strangle 每日检查面板（最顶部）
    if scorecard_panel:
        charts.append(html.Div(scorecard_panel, id='scorecard-panel'))

    # 0.5) 智能平仓时机提醒（距收盘<1小时时显示）
    exit_banner = _build_exit_timing_banner(manual_pairs)
    if exit_banner:
        charts.append(exit_banner)

    # 1) 布林线警报区（置顶显示）
    if all_alerts:
        alert_products = []
        for item in all_alerts:
            if len(item) == 6:
                _, _, _, info, _, _ = item
                fs = info.get('futures_sym') or '?'
            else:
                _, ap, _, info = item
                fs = ap.get('product', '?')
            dr = info.get('double_rise', {})
            alert_products.append(f'{fs} +{dr.get("sum_chg", 0)*100:.1f}%')

        charts.append(html.Div([
            html.Span('⚠ 布林线警报  ', style={'color': '#FF4444', 'fontSize': '17px', 'fontWeight': 'bold'}),
            html.Span(' | '.join(alert_products),
                       style={'color': '#FF8888', 'fontSize': '14px'}),
            html.Div('价格之和突破布林上轨(5分钟K线, 26期, 1.5σ) → 可卖出（B021: 93-94%胜率，当日平仓）',
                      style={'color': '#FF8888', 'fontSize': '12px', 'marginTop': '4px', 'opacity': '0.8'}),
        ], style={'padding': '14px 20px', 'backgroundColor': '#3a0a0a',
                  'borderTop': '3px solid #FF4444', 'borderBottom': '3px solid #FF4444'}))

        for item in all_alerts:
            if len(item) == 6:
                pass  # 手动对在自选区统一渲染（带警报样式）
            else:
                charts.append(_auto_card(*item, is_alert=True))

    # 2) 自选区（所有手动对，有警报的标红）
    for idx, pair, fig, info, cc, pc in manual_items:
        is_alert = idx in alert_manual_indices
        charts.append(_manual_card(idx, pair, fig, info, cc, pc, is_alert=is_alert))

    # 3) 智能推荐区（排除已在警报区渲染的）
    if non_alert_auto:
        charts.append(html.Div([
            html.Span('智能推荐', style={'color': '#00FF88', 'fontSize': '15px', 'fontWeight': 'bold'}),
            html.Span('  基于trade2026评分自动选对', style={'color': '#666', 'fontSize': '12px'}),
        ], style={'padding': '12px 20px', 'backgroundColor': '#0a1628',
                  'borderTop': '3px solid #00FF88', 'marginTop': '10px'}))

        for idx, ap, fig, info in non_alert_auto:
            charts.append(_auto_card(idx, ap, fig, info))

    charts_area = html.Div(charts, style={'flex': '1', 'minWidth': '0'})

    charts_layout = html.Div([sidebar, charts_area],
                              style={'display': 'flex', 'alignItems': 'flex-start', 'padding': '8px'})
    return charts_layout


# ============ 价差Z-Score监控 ============
# 强相关品种对，按回测夏普排序（来自B031）
_SPREAD_PAIRS = [
    ('RB', 'HC', '螺纹钢', '热卷', 0.88, 11.55),
    ('RU', 'NR', '天然橡胶', '20号胶', 0.84, 7.82),
    ('J', 'JM', '焦炭', '焦煤', 0.73, 6.86),
    ('TA', 'PF', 'PTA', '短纤', 0.84, 5.61),
    ('PP', 'L', '聚丙烯', 'LLDPE', 0.80, 4.46),
    ('CF', 'CY', '棉花', '棉纱', 0.72, 4.42),
    ('I', 'RB', '铁矿石', '螺纹钢', 0.68, 3.97),
    ('SC', 'LU', '原油', '低硫燃料油', 0.78, 3.07),
    ('M', 'RM', '豆粕', '菜粕', 0.59, 2.25),
    ('CU', 'ZN', '铜', '锌', 0.72, 1.25),
    ('AU', 'AG', '黄金', '白银', 0.76, 0.29),
    ('P', 'Y', '棕榈油', '豆油', 0.82, 0.99),
]


def _get_futures_prices(code, n_minutes=150):
    """从CTP数据库获取期货品种最近N分钟的收盘价"""
    try:
        db = get_db()
        cur = db.cursor()
        # 精确匹配：品种代码+纯数字月份（如 rb2510, AG2506, cu2604）
        # 避免 J 匹配到 JD/JM, P 匹配到 PF/PP/PK, M 匹配到 MA/MO 等
        import re as _re
        code_lower = code.lower()
        code_upper = code.upper()
        cur.execute("SELECT DISTINCT symbol FROM dbbardata ORDER BY symbol")
        all_symbols = [r[0] for r in cur.fetchall()]
        # 期货pattern: 精确品种代码 + 纯数字(3-4位月份) + 可选F后缀
        pat = _re.compile(
            r'^(' + _re.escape(code_lower) + r'|' + _re.escape(code_upper) + r')(\d{3,4})F?$',
            _re.IGNORECASE)
        futures_syms = [s for s in all_symbols if pat.match(s)]

        if not futures_syms:
            return None

        # 取最近交易的合约
        best_sym = None
        best_time = None
        for sym in futures_syms[-5:]:  # 检查最后几个
            cur.execute(
                "SELECT MAX(datetime) FROM dbbardata WHERE symbol = ?",
                (sym,))
            row = cur.fetchone()
            if row and row[0]:
                if best_time is None or row[0] > best_time:
                    best_time = row[0]
                    best_sym = sym

        if not best_sym:
            return None

        # 取最近N分钟数据
        cur.execute(
            "SELECT datetime, close_price FROM dbbardata "
            "WHERE symbol = ? ORDER BY datetime DESC LIMIT ?",
            (best_sym, n_minutes))
        rows = cur.fetchall()
        if len(rows) < 30:
            return None

        import pandas as _pd
        df = _pd.DataFrame(rows, columns=['datetime', 'close'])
        df['datetime'] = _pd.to_datetime(df['datetime'])
        df = df.sort_values('datetime').reset_index(drop=True)
        df['close'] = df['close'].astype(float)
        return df, best_sym

    except Exception:
        return None


def _calc_spread_zscore(code_a, code_b, window=120):
    """计算两个品种的实时Z-Score"""
    result_a = _get_futures_prices(code_a, n_minutes=window + 50)
    result_b = _get_futures_prices(code_b, n_minutes=window + 50)

    if result_a is None or result_b is None:
        return None

    df_a, sym_a = result_a
    df_b, sym_b = result_b

    import pandas as _pd
    merged = _pd.merge(df_a, df_b, on='datetime', suffixes=('_A', '_B'))
    if len(merged) < window:
        return None

    merged['ratio'] = merged['close_A'] / merged['close_B']
    merged['ratio_mean'] = merged['ratio'].rolling(window).mean()
    merged['ratio_std'] = merged['ratio'].rolling(window).std()
    merged['z'] = (merged['ratio'] - merged['ratio_mean']) / merged['ratio_std']

    last = merged.dropna(subset=['z']).iloc[-1] if len(merged.dropna(subset=['z'])) > 0 else None
    if last is None:
        return None

    # 最近的Z-Score趋势（最后10根）
    recent = merged.dropna(subset=['z']).tail(10)
    z_trend = recent['z'].diff().mean() if len(recent) > 1 else 0

    return {
        'z': float(last['z']),
        'ratio': float(last['ratio']),
        'ratio_mean': float(last['ratio_mean']),
        'price_a': float(last['close_A']),
        'price_b': float(last['close_B']),
        'sym_a': sym_a,
        'sym_b': sym_b,
        'z_trend': float(z_trend),
        'n_bars': len(merged),
        'last_time': str(last['datetime']),
    }


def _build_spread_panel():
    """构建价差Z-Score监控面板"""
    rows = []
    n_extreme = 0
    n_ok = 0
    n_fail = 0

    for code_a, code_b, name_a, name_b, corr, sharpe in _SPREAD_PAIRS:
        result = _calc_spread_zscore(code_a, code_b)

        if result is None:
            n_fail += 1
            rows.append(html.Tr([
                html.Td(f'{name_a}↔{name_b}', style={'color': '#666'}),
                html.Td(f'{corr:.2f}', style={'color': '#666', 'textAlign': 'center'}),
                html.Td('无数据', colSpan='6', style={'color': '#444', 'textAlign': 'center'}),
            ], style={'backgroundColor': '#0d1117', 'borderBottom': '1px solid #1a1a3e'}))
            continue

        z = result['z']
        abs_z = abs(z)

        # 颜色编码
        if abs_z >= 2.0:
            z_color = '#f44336'  # 红
            status = '偏离'
            n_extreme += 1
        elif abs_z >= 1.5:
            z_color = '#ff9800'  # 橙
            status = '警戒'
        elif abs_z >= 1.0:
            z_color = '#ffeb3b'  # 黄
            status = '注意'
        else:
            z_color = '#4caf50'  # 绿
            status = '正常'
            n_ok += 1

        # Z趋势箭头
        trend = result['z_trend']
        if trend > 0.05:
            trend_arrow = '↑'
            trend_color = '#f44336' if z > 0 else '#4caf50'  # 偏离加剧/回归
        elif trend < -0.05:
            trend_arrow = '↓'
            trend_color = '#4caf50' if z > 0 else '#f44336'
        else:
            trend_arrow = '→'
            trend_color = '#888'

        # 哪个偏贵
        if z > 0.5:
            bias = f'{name_a}偏贵'
            bias_color = '#ff6b6b'
        elif z < -0.5:
            bias = f'{name_b}偏贵'
            bias_color = '#ff6b6b'
        else:
            bias = '均衡'
            bias_color = '#4caf50'

        row_bg = '#1a0a0a' if abs_z >= 2.0 else '#1a1a0a' if abs_z >= 1.5 else '#0d1117'

        rows.append(html.Tr([
            html.Td(f'{name_a}↔{name_b}', style={
                'color': '#fff', 'fontWeight': 'bold', 'fontSize': '13px'}),
            html.Td(f'{corr:.2f}', style={
                'color': '#888', 'textAlign': 'center', 'fontSize': '12px'}),
            html.Td(f'{z:+.2f}', style={
                'color': z_color, 'fontWeight': 'bold', 'textAlign': 'center',
                'fontSize': '16px'}),
            html.Td(f'{trend_arrow}', style={
                'color': trend_color, 'textAlign': 'center', 'fontSize': '16px'}),
            html.Td(status, style={
                'color': z_color, 'textAlign': 'center', 'fontSize': '12px'}),
            html.Td(bias, style={
                'color': bias_color, 'textAlign': 'center', 'fontSize': '12px'}),
            html.Td(f'{result["price_a"]:.0f}/{result["price_b"]:.0f}', style={
                'color': '#888', 'textAlign': 'center', 'fontSize': '11px'}),
            html.Td(result['last_time'][-8:], style={
                'color': '#555', 'textAlign': 'right', 'fontSize': '11px'}),
        ], style={'backgroundColor': row_bg, 'borderBottom': '1px solid #1a1a3e'}))

    # 表头
    header = html.Tr([
        html.Th('品种对', style={'width': '130px'}),
        html.Th('r', style={'width': '45px', 'textAlign': 'center'}),
        html.Th('Z-Score', style={'width': '80px', 'textAlign': 'center'}),
        html.Th('趋势', style={'width': '40px', 'textAlign': 'center'}),
        html.Th('状态', style={'width': '50px', 'textAlign': 'center'}),
        html.Th('偏向', style={'width': '80px', 'textAlign': 'center'}),
        html.Th('价格A/B', style={'width': '100px', 'textAlign': 'center'}),
        html.Th('时间', style={'width': '70px', 'textAlign': 'right'}),
    ], style={'backgroundColor': '#1a1a3e', 'color': '#bb86fc', 'fontSize': '12px'})

    table = html.Table(
        [html.Thead(header), html.Tbody(rows)],
        style={'width': '100%', 'borderCollapse': 'collapse', 'fontSize': '13px'})

    # 统计摘要
    summary = f'{n_extreme}个偏离  {n_ok}个正常  {n_fail}个无数据'

    panel = html.Div([
        html.Div([
            html.Span('价差Z-Score监控', style={
                'color': '#bb86fc', 'fontSize': '15px', 'fontWeight': 'bold'}),
            html.Span(f'  {summary}', style={'color': '#666', 'fontSize': '12px'}),
            html.Span('  |Z|≥2偏离  1.5警戒  1.0注意  <1正常', style={
                'color': '#444', 'fontSize': '11px', 'marginLeft': '15px'}),
        ], style={'padding': '10px 25px', 'borderBottom': '1px solid #2a2a4a'}),
        html.Div(table, style={'padding': '5px 25px'}),
        html.Div([
            html.Span('滚动窗口120分钟 | B030: Z>2后30分钟90%+回归 | '
                       'B031: 配对交易毛利可观但成本不可行',
                       style={'color': '#444', 'fontSize': '11px'}),
        ], style={'padding': '8px 25px', 'borderTop': '1px solid #1a1a3e'}),
    ])

    return panel


NEWS_CACHE = os.path.expanduser('~/Scripts/news_cache.md')
NEWS_FETCHER = os.path.expanduser('~/Scripts/news_auto_fetch.py')


def _news_cache_age():
    """返回缓存文件的年龄（秒），不存在返回 inf"""
    if not os.path.exists(NEWS_CACHE):
        return float('inf')
    return time.time() - os.path.getmtime(NEWS_CACHE)


def _news_hourly_fetcher():
    """后台线程: 每整点自动运行 news_auto_fetch.py"""
    import subprocess
    while True:
        now = datetime.now()
        # 计算到下一个整点的秒数
        next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        wait = (next_hour - now).total_seconds()
        time.sleep(wait)
        # 整点到了，执行抓取
        try:
            subprocess.run(
                ['/usr/bin/python3', NEWS_FETCHER],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=60)
        except Exception:
            pass


def _render_news_content():
    """读取并渲染新闻缓存内容"""
    if not os.path.exists(NEWS_CACHE):
        return '', '', None

    try:
        with open(NEWS_CACHE, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception:
        return '', '读取失败', None

    # 跳过内嵌时间戳行（不可靠），统一用文件mtime
    if content.startswith('<!-- updated:'):
        content = content.split('\n', 1)[1] if '\n' in content else ''
    # 用文件修改时间作为唯一时间来源
    mtime = os.path.getmtime(NEWS_CACHE)
    updated = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')

    if not content.strip():
        return '', updated, None

    # 分离自动抓取内容和 /news 分析区块
    analysis_marker = '<!-- ANALYSIS -->'
    main_content = content
    analysis_content = ''
    if analysis_marker in content:
        idx = content.index(analysis_marker)
        main_content = content[:idx]
        analysis_content = content[idx + len(analysis_marker):]
        # 去掉结束标记
        end_marker = '<!-- /ANALYSIS -->'
        if end_marker in analysis_content:
            analysis_content = analysis_content[:analysis_content.index(end_marker)]

    lines = []
    for line in main_content.split('\n'):
        stripped = line.strip()
        if stripped.startswith('## '):
            lines.append(html.H3(stripped[3:], style={
                'color': '#4fc3f7', 'margin': '18px 0 8px', 'fontSize': '15px',
                'borderBottom': '1px solid #2a2a4a', 'paddingBottom': '4px'}))
        elif stripped.startswith('**') and stripped.endswith('**'):
            lines.append(html.Div(stripped.strip('*'), style={
                'color': '#FFD700', 'fontWeight': 'bold', 'fontSize': '13px', 'margin': '6px 0'}))
        elif stripped:
            lines.append(html.Div(stripped, style={
                'color': '#ddd', 'fontSize': '13px', 'lineHeight': '1.6', 'margin': '2px 0'}))

    # 渲染 /news 分析区块（交易提示等）
    if analysis_content.strip():
        lines.append(html.Hr(style={'borderColor': '#4fc3f7', 'margin': '16px 0'}))
        for line in analysis_content.split('\n'):
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith('## '):
                lines.append(html.H3(stripped[3:], style={
                    'color': '#ff9800', 'margin': '14px 0 6px', 'fontSize': '15px',
                    'borderBottom': '1px solid #3a2a1a', 'paddingBottom': '4px'}))
            elif stripped.startswith('**') and stripped.endswith('**'):
                lines.append(html.Div(stripped.strip('*'), style={
                    'color': '#FFD700', 'fontWeight': 'bold', 'fontSize': '13px', 'margin': '6px 0'}))
            elif stripped.startswith('- ') or stripped.startswith('* '):
                lines.append(html.Div('• ' + stripped[2:], style={
                    'color': '#e0e0e0', 'fontSize': '13px', 'lineHeight': '1.6',
                    'margin': '2px 0', 'paddingLeft': '12px'}))
            elif any(stripped.startswith(c) for c in ['⚠️', '⚡', '📊', '🔥', '🎯', '🤖']):
                lines.append(html.Div(stripped, style={
                    'color': '#ff9800', 'fontWeight': 'bold', 'fontSize': '13px',
                    'margin': '8px 0', 'padding': '6px 10px',
                    'backgroundColor': 'rgba(255,152,0,0.1)', 'borderRadius': '4px',
                    'borderLeft': '3px solid #ff9800'}))
            else:
                lines.append(html.Div(stripped, style={
                    'color': '#ddd', 'fontSize': '13px', 'lineHeight': '1.6', 'margin': '2px 0'}))

    body = html.Div(lines, style={
        'padding': '10px 25px', 'maxHeight': '70vh', 'overflowY': 'auto'})
    return content, updated, body


@app.callback(
    Output('news-panel', 'children'),
    Output('news-panel', 'style'),
    Input('news-btn', 'n_clicks'),
    prevent_initial_call=True,
)
def toggle_news(n_clicks):
    """切换资讯面板显示/隐藏（数据来源: /news技能 或 整点自动采集）"""
    if not n_clicks or n_clicks % 2 == 0:
        return no_update, {'display': 'none'}

    content, updated, body = _render_news_content()

    if body is None:
        body = html.Div('暂无资讯。数据来源: /news技能手动更新 或 整点自动采集。',
                         style={'color': '#888', 'padding': '30px', 'textAlign': 'center'})

    age = _news_cache_age()
    age_text = ''
    if age < float('inf'):
        hours = int(age // 3600)
        mins = int((age % 3600) // 60)
        if hours > 0:
            age_text = f'{hours}小时{mins}分钟前'
        else:
            age_text = f'{mins}分钟前'

    header_text = f'更新于 {updated}' if updated else ''
    if age_text:
        header_text += f' ({age_text})'

    panel = html.Div([
        html.Div([
            html.Span('每日资讯', style={'color': '#4fc3f7', 'fontSize': '15px', 'fontWeight': 'bold'}),
            html.Span(f'  {header_text}', style={'color': '#666', 'fontSize': '12px'}),
            html.Span('  整点自动更新 | /news手动更新', style={'color': '#444', 'fontSize': '11px'}),
        ], style={'padding': '10px 25px', 'borderBottom': '1px solid #2a2a4a'}),
        body,
    ])

    return panel, {
        'display': 'block', 'backgroundColor': '#111827',
        'borderBottom': '3px solid #4fc3f7', 'marginBottom': '5px',
    }


@app.callback(
    Output('scorecard-body', 'style'),
    Output('scorecard-collapsed', 'data'),
    Input('scorecard-header', 'n_clicks'),
    State('scorecard-collapsed', 'data'),
    prevent_initial_call=True,
)
def toggle_scorecard(n_clicks, collapsed):
    """折叠/展开每日检查面板"""
    new_state = not collapsed
    style = {'display': 'none'} if new_state else {}
    return style, new_state


@app.callback(
    Output('vrp-panel', 'children'),
    Output('vrp-panel', 'style'),
    Input('vrp-btn', 'n_clicks'),
    prevent_initial_call=True,
)
def toggle_vrp(n_clicks):
    """切换VRP扫描面板显示/隐藏"""
    if not n_clicks or n_clicks % 2 == 0:
        return no_update, {'display': 'none'}

    panel = _build_vrp_panel()
    return panel, {
        'display': 'block', 'backgroundColor': '#111827',
        'borderBottom': '3px solid #00FF88', 'marginBottom': '5px',
    }


@app.callback(
    Output('plan-panel', 'children'),
    Output('plan-panel', 'style'),
    Input('plan-btn', 'n_clicks'),
    prevent_initial_call=True,
)
def toggle_plan(n_clicks):
    """今日交易计划面板 — 8维度综合评分实时计算版"""
    if not n_clicks or n_clicks % 2 == 0:
        return no_update, {'display': 'none'}

    import math
    # 从CTP dashboard读取实际账户权益，未连接时回退50万
    _dash = _load_dashboard()
    _acc = _dash.get('account') if _dash else None
    account = _acc.get('balance', 500000) if _acc and _acc.get('balance', 0) > 0 else 500000
    _acc_source = 'CTP' if _acc and _acc.get('balance', 0) > 0 else '默认'
    today_d = datetime.now().date()
    today_str = today_d.strftime('%Y-%m-%d')
    now_time = datetime.now().strftime('%H:%M')
    weekday = datetime.now().weekday()  # 0=Mon, 4=Fri

    # === 1. 遍历v6品种，计算DTE，匹配桶 ===
    matched = []
    for prod, info in _V6_CONFIG.items():
        ex = info['ex']
        months = info.get('months', list(range(1, 13)))
        for mo_off in range(0, 8):
            yr, mo = today_d.year, today_d.month + mo_off
            if mo > 12:
                yr += 1; mo -= 12
            if mo not in months:
                continue
            exp = _official_expiry(ex, yr, mo, _trading_cal, product_code=prod.upper())
            if not exp:
                continue
            dte = (exp - today_d).days
            if dte < 1 or dte > 200:
                continue
            for bname, blo, bhi in _DTE_RANGES:
                if blo <= dte <= bhi and bname in info:
                    params = info[bname]
                    contract = f'{prod}{yr % 100:02d}{mo:02d}'
                    matched.append({
                        'prod': prod, 'name': info['name'], 'ex': ex,
                        'contract': contract, 'dte': dte, 'bucket': bname,
                        'sharpe': params['sharpe'], 'wr': params['wr'],
                        'entry': params['entry'], 'tp': params['tp'],
                        'sl': params['sl'], 'n': params['n'],
                    })
                    break

    # === 2. 查DB: 期货价格 + 期货日K线(用于近期波动计算) ===
    db = get_db()
    cur = db.cursor()
    three_days_ago = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d 00:00:00')
    twenty_days_ago = (datetime.now() - timedelta(days=25)).strftime('%Y-%m-%d 00:00:00')

    # 最新价格
    cur.execute("""SELECT b.symbol, b.close_price FROM dbbardata b
        INNER JOIN (SELECT symbol, MAX(datetime) as max_dt FROM dbbardata
            WHERE datetime >= ? GROUP BY symbol) m
        ON b.symbol = m.symbol AND b.datetime = m.max_dt
    """, (three_days_ago,))
    px_cache = {sym: px for sym, px in cur.fetchall() if px and px > 0}

    # 日级高低收(用于ATR计算) — 从1min聚合日K
    cur.execute("""SELECT symbol, DATE(datetime) as d,
        MAX(close_price) as high, MIN(close_price) as low, MAX(close_price) as close
        FROM dbbardata WHERE datetime >= ? AND close_price > 0
        GROUP BY symbol, DATE(datetime)
        ORDER BY symbol, d""", (twenty_days_ago,))
    daily_bars = {}  # {symbol: [(date, high, low, close), ...]}
    for sym, d, h, l, c in cur.fetchall():
        daily_bars.setdefault(sym, []).append((d, h, l, c))

    # === 3. VRP信号 ===
    vrp_data = {}
    try:
        vrp_results = scan_vrp()
        for i, r in enumerate(vrp_results):
            vrp_data[r['product']] = {
                'rank': i + 1, 'vrp': r['vrp'], 'iv': r['iv'], 'rv': r['rv'],
                'vrp_pct': r.get('vrp_pct', 0),
            }
    except Exception:
        pass

    # === 4. 新闻/经济日历 — 品种事件检测 ===
    _NEWS_KEYWORDS = {
        'AU': ['黄金','gold','贵金属'], 'AG': ['白银','silver','贵金属'],
        'CU': ['铜','copper'], 'AL': ['铝','alumin'], 'ZN': ['锌','zinc'],
        'NI': ['镍','nickel'], 'SN': ['锡','tin'],
        'SC': ['原油','crude','oil','能源'], 'FU': ['燃油','fuel'],
        'P': ['棕榈','palm'], 'Y': ['豆油','soy oil'], 'OI': ['菜油','rapeseed'],
        'M': ['豆粕','soybean meal'], 'RM': ['菜粕','rapeseed meal'],
        'CF': ['棉花','cotton'], 'SR': ['白糖','sugar'],
        'RU': ['橡胶','rubber'], 'SA': ['纯碱','soda ash'],
        'FG': ['玻璃','glass'], 'MA': ['甲醇','methanol'],
        'TA': ['PTA','聚酯'], 'EB': ['苯乙烯','styrene'],
        'I': ['铁矿','iron ore'], 'RB': ['螺纹','rebar'],
        'LC': ['碳酸锂','lithium'], 'SI': ['工业硅','silicon'],
    }
    news_affected = set()  # 今日有新闻提及的品种
    try:
        if os.path.exists(NEWS_CACHE):
            with open(NEWS_CACHE, 'r', encoding='utf-8') as f:
                news_text = f.read().lower()
            for prod, keywords in _NEWS_KEYWORDS.items():
                if any(kw.lower() in news_text for kw in keywords):
                    news_affected.add(prod)
    except Exception:
        pass

    # 经济日历
    eco_affected = set()  # 今日有经济事件影响的品种
    try:
        eco_path = os.path.join(os.path.dirname(__file__), 'economic_calendar.json')
        if os.path.exists(eco_path):
            with open(eco_path) as f:
                eco_cal = json.load(f)
            for evt in eco_cal.get('events', []):
                if evt.get('date') == today_str and evt.get('level') == 'high':
                    affects = evt.get('affects', [])
                    if 'all' in affects:
                        eco_affected.update(_V6_CONFIG.keys())
                    else:
                        eco_affected.update(a.upper() for a in affects)
    except Exception:
        pass

    # === 5. 计算近期波动因子(ATR ratio) ===
    def _calc_atr_ratio(sym):
        """近3天ATR / 20天ATR, <1表示近期平静"""
        bars = daily_bars.get(sym, [])
        if len(bars) < 5:
            return 1.0  # 数据不足，中性
        # 计算True Range (简化: high - low)
        trs = [h - l for _, h, l, c in bars if h > l]
        if len(trs) < 5:
            return 1.0
        atr_3 = sum(trs[-3:]) / min(3, len(trs[-3:]))
        atr_20 = sum(trs) / len(trs)
        if atr_20 <= 0:
            return 1.0
        return atr_3 / atr_20

    # === 6. 综合评分 + 保证金 ===
    picks = []
    for m in matched:
        prod, ex = m['prod'], m['ex']
        _yymm = re.search(r'(\d{3,4})$', m['contract'])
        if not _yymm:
            continue
        digits = _yymm.group(1)
        fut_sym = f'{prod.upper()}{digits[-3:]}' if ex == 'CZCE' else f'{prod.lower()}{digits}'
        fut_px = px_cache.get(fut_sym)
        if not fut_px:
            continue

        # 保证金
        mult = _MULTIPLIER.get(prod, 10)
        mgn_rate = _MARGIN_RATE.get(prod, 0.10)
        margin_per_lot = fut_px * mult * mgn_rate
        if margin_per_lot <= 0:
            continue

        # --- 8维度评分 ---
        base_sharpe = m['sharpe']  # [1] V6 Sharpe (5-43)

        # [2] VRP因子
        vrp_info = vrp_data.get(prod)
        if vrp_info and vrp_info['rank'] <= 3:
            vrp_factor = 1.3
            vrp_label = f"Top{vrp_info['rank']}"
        elif vrp_info and vrp_info['rank'] <= 10:
            vrp_factor = 1.1
            vrp_label = f"#{vrp_info['rank']}"
        elif vrp_info:
            vrp_factor = 1.0
            vrp_label = f"#{vrp_info['rank']}"
        else:
            vrp_factor = 0.9  # 无VRP数据轻微降权
            vrp_label = '-'

        # [3] 新闻/事件因子
        has_news = prod in news_affected
        has_eco = prod in eco_affected
        if has_eco:
            news_factor = 0.5   # 高级经济事件→大幅降权
            news_label = '经济事件'
        elif has_news:
            news_factor = 0.7   # 有新闻提及→适度降权
            news_label = '有新闻'
        else:
            news_factor = 1.0
            news_label = '平静'

        # [4] 近期波动因子
        atr_ratio = _calc_atr_ratio(fut_sym)
        if atr_ratio < 0.8:
            vol_factor = 1.2    # 近期平静→卖方友好
            vol_label = '平静'
        elif atr_ratio <= 1.2:
            vol_factor = 1.0
            vol_label = '正常'
        else:
            vol_factor = 0.7    # 近期波动大→降权
            vol_label = '波动'

        # [5] 流动性(期货日成交量)
        bars = daily_bars.get(fut_sym, [])
        # 无法从聚合日K直接得到日成交量，用bar数近似(每天约240根1min bar)
        recent_bars = len([b for b in bars if b[0] >= (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')])
        if recent_bars >= 600:      # ~2.5天×240bar
            liq_factor = 1.0
            liq_label = '充足'
        elif recent_bars >= 200:
            liq_factor = 0.85
            liq_label = '一般'
        else:
            liq_factor = 0.7
            liq_label = '薄'

        # [6] 周度效应 (B018/B019)
        if weekday == 4 and prod == 'P' and m['dte'] <= 7:
            wday_factor = 1.3   # 周五+P+DTE≤7 特别加分
            wday_label = '周五P★'
        elif weekday == 4:
            wday_factor = 1.05  # 周五略加(周末theta)
            wday_label = '周五'
        else:
            wday_factor = 1.0
            wday_label = ''

        # [7] DTE甜蜜区: 在品种最优桶=1.0, 非最优桶=0.85
        # (每品种可能有多个桶匹配，最高Sharpe的桶为最优)
        _prod_cfg = _V6_CONFIG.get(prod, {})
        prod_best_sharpe = max((_prod_cfg[bn]['sharpe'] for bn, _, _ in _DTE_RANGES
                                if bn in _prod_cfg),
                               default=base_sharpe)
        dte_sweet = 1.0 if base_sharpe >= prod_best_sharpe else 0.85

        # [8] 胜率加权: 高胜率品种更可靠
        wr_factor = 0.85 + 0.15 * (m['wr'] / 100.0)  # 80%→0.97, 95%→0.99, 100%→1.0

        # === 综合分 ===
        composite = (base_sharpe * vrp_factor * news_factor * vol_factor
                     * liq_factor * wday_factor * dte_sweet * wr_factor)

        m.update({
            'fut_px': fut_px, 'fut_sym': fut_sym,
            'margin_per_lot': margin_per_lot,
            'composite': composite,
            'vrp_factor': vrp_factor, 'vrp_label': vrp_label,
            'news_factor': news_factor, 'news_label': news_label,
            'vol_factor': vol_factor, 'vol_label': vol_label, 'atr_ratio': atr_ratio,
            'liq_factor': liq_factor, 'liq_label': liq_label,
            'wday_factor': wday_factor, 'wday_label': wday_label,
            'dte_sweet': dte_sweet, 'wr_factor': wr_factor,
        })
        picks.append(m)

    # 按综合分降序
    picks.sort(key=lambda x: x['composite'], reverse=True)

    # === 时段过滤：只显示当前可交易的品种+入场方式 ===
    session_label, night_only = _get_session_context()
    all_picks_count = len(picks)
    if night_only:
        # 晚间：品种必须有夜盘 AND 入场方式必须是 night_*
        picks = [p for p in picks
                 if p['prod'].upper() not in _NO_NIGHT_SESSION
                 and str(p.get('entry', '')).startswith('night')]
    filtered_count = all_picks_count - len(picks)

    # 标记每品种最优合约
    seen_prods = set()
    for p in picks:
        if p['prod'] not in seen_prods:
            p['is_best'] = True
            seen_prods.add(p['prod'])
        else:
            p['is_best'] = False

    # === 仓位分配（全局预算约束） ===
    budget = account * 0.6  # 总资金60%为可用预算
    per_prod_cap = account * 0.05  # 单品种上限5%
    remaining = budget
    for p in picks:
        if remaining <= 0:
            p['lots'] = 0
            p['total_margin'] = 0
            p['pct'] = 0
            continue
        mgn = p['margin_per_lot']
        if mgn <= 0:
            p['lots'] = 0
            p['total_margin'] = 0
            p['pct'] = 0
            continue
        max_lots_budget = int(remaining / mgn)
        max_lots_cap = max(1, int(per_prod_cap / mgn))
        lots = min(max_lots_budget, max_lots_cap)
        lots = max(lots, 0)
        p['lots'] = lots
        p['total_margin'] = mgn * lots
        p['pct'] = p['total_margin'] / account
        remaining -= p['total_margin']

    # 评分说明
    factor_desc = []
    n_vrp = sum(1 for p in picks if p['vrp_factor'] >= 1.1)
    n_news = sum(1 for p in picks if p['news_factor'] < 1.0)
    n_calm = sum(1 for p in picks if p['vol_factor'] >= 1.2)
    factor_desc.append(f'VRP加分{n_vrp}个')
    if n_news:
        factor_desc.append(f'新闻降权{n_news}个')
    if n_calm:
        factor_desc.append(f'近期平静{n_calm}个')
    if weekday == 4:
        factor_desc.append('周五效应')

    plan_sections = []
    filter_note = f'  已过滤{filtered_count}个非当前时段品种' if filtered_count > 0 else ''

    # === 日内OTM宽跨卖出 — v6参数查表 (引用模块级 _V6_CONFIG) ===
    _v6 = _V6_CONFIG
    _entry_labels = _ENTRY_LABELS
    _dte_ranges = _DTE_RANGES

    # 计算各品种当前DTE → 匹配最优参数
    today_d_v6 = datetime.now().date()
    _get_exp = _official_expiry
    _cal_v6 = _trading_cal

    matched = []  # [(品种code, 中文, 交易所, 合约, DTE, DTE桶, params_dict)]
    for prod, info in _v6.items():
        ex = info['ex']
        months = info.get('months', list(range(1, 13)))
        for mo_off in range(0, 8):
            yr, mo = today_d_v6.year, today_d_v6.month + mo_off
            if mo > 12:
                yr += 1; mo -= 12
            if mo not in months:
                continue
            exp = _get_exp(ex, yr, mo, _cal_v6, product_code=prod.upper())
            if not exp:
                continue
            dte = (exp - today_d_v6).days
            if dte < 1 or dte > 200:
                continue
            # 找匹配的DTE桶
            for bucket_name, blo, bhi in _dte_ranges:
                if blo <= dte <= bhi and bucket_name in info:
                    params = info[bucket_name]
                    contract = f'{prod}{yr % 100:02d}{mo:02d}'
                    matched.append((prod, info['name'], ex, contract, dte, bucket_name,
                                    params['sharpe'], params['wr'], params['entry'],
                                    params['tp'], params['sl'], params['n']))
                    break

    # 按Sharpe降序排列
    matched.sort(key=lambda x: x[6], reverse=True)

    # === 实时行情增强：查DB找ATM期权对，过滤出可交易机会 ===
    db = get_db()
    cur = db.cursor()

    # 批量获取所有期权合约最新价格和成交量
    cur.execute("""SELECT b.symbol, b.close_price, b.volume FROM dbbardata b
        INNER JOIN (SELECT symbol, MAX(datetime) as max_dt FROM dbbardata
            WHERE datetime >= ? AND NOT (
                (CAST(substr(datetime,12,2) AS INTEGER) = 15 AND CAST(substr(datetime,15,2) AS INTEGER) >= 5)
                OR (CAST(substr(datetime,12,2) AS INTEGER) BETWEEN 16 AND 19)
                OR (CAST(substr(datetime,12,2) AS INTEGER) = 20 AND CAST(substr(datetime,15,2) AS INTEGER) < 55))
            GROUP BY symbol) m ON b.symbol = m.symbol AND b.datetime = m.max_dt
    """, ((datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d 00:00:00'),))
    _px_cache = {}
    for sym, px, vol in cur.fetchall():
        _px_cache[sym] = (px, vol or 0)

    actionable = []  # 可交易的机会
    for prod, cn, ex, contract, dte, bucket, sharpe, wr, entry, tp, sl, n in matched:
        # 查期货价格（CZCE用3位月份大写，其他用4位月份小写）
        # contract='CF2604' → yymm='2604'
        _yymm = re.search(r'(\d{3,4})$', contract)
        if not _yymm:
            continue
        _digits = _yymm.group(1)
        if ex == 'CZCE':
            # CZCE: CF604 (1位年+2位月)
            fut_sym = f'{prod.upper()}{_digits[-3:]}'
        else:
            # 其他: ag2604 (小写+4位)
            fut_sym = f'{prod.lower()}{_digits}'
        fut_px_row = _px_cache.get(fut_sym)
        if not fut_px_row or fut_px_row[0] <= 0:
            continue
        fut_px = fut_px_row[0]

        # 找所有该合约的Call和Put
        # 生成月份匹配集合（兼容CZCE 3位 vs 其他4位格式）
        # contract = 'CF2604' → 匹配 '2604' 和 '604'
        contract_mo4 = contract[-4:]  # '2604'
        contract_mo3 = contract[-3:]  # '604'
        valid_months = {contract_mo4, contract_mo3}

        calls, puts = [], []
        for sym, (px, vol) in _px_cache.items():
            parsed = _parse_contract(sym)
            if not parsed:
                continue
            p, mo, opt_type, strike = parsed
            if p != prod.upper():
                continue
            if mo not in valid_months:
                continue
            if px <= 0:
                continue
            if opt_type == 'C':
                calls.append((sym, px, vol, strike))
            elif opt_type == 'P':
                puts.append((sym, px, vol, strike))

        if not calls or not puts:
            continue

        # OTM宽跨：Call行权价>期货价(虚值)，Put行权价<期货价(虚值)
        otm_calls = [(s, p, v, k) for s, p, v, k in calls if k > fut_px and p > 0]
        otm_puts = [(s, p, v, k) for s, p, v, k in puts if k < fut_px and p > 0]

        if not otm_calls or not otm_puts:
            continue

        # 按虚值度排序（离期货价从近到远）
        otm_calls.sort(key=lambda x: x[3] - fut_px)
        otm_puts.sort(key=lambda x: fut_px - x[3])

        # 搜索最优宽跨对（近虚值前10档 × 前10档）
        best_pair = None
        for c_sym, c_px, c_vol, c_k in otm_calls[:10]:
            for p_sym, p_px, p_vol, p_k in otm_puts[:10]:
                if c_px <= 0 or p_px <= 0:
                    continue
                psum = c_px + p_px
                if psum < 4:
                    continue  # 权利金太低，手续费占比过大
                ratio = max(c_px, p_px) / min(c_px, p_px)
                # 行权价虚值度
                c_otm_pct = (c_k - fut_px) / fut_px
                p_otm_pct = (fut_px - p_k) / fut_px
                if c_otm_pct < 0.02 or p_otm_pct < 0.02:
                    continue  # 太接近ATM，不是宽跨
                if c_otm_pct > 0.15 or p_otm_pct > 0.15:
                    continue  # 太深度虚值
                total_vol = c_vol + p_vol
                # === 评分：价格对称 × 成交额 × 最低权利金 ===
                import math as _math
                # 1. 价格对称性：两腿权利金越接近越好
                if ratio > 2.5:
                    continue
                price_sym = max(0, 1.0 - (ratio - 1.0) * 0.6)  # 1.0→1.0, 1.5→0.7, 2.0→0.4
                # 2. 瓶颈腿成交额
                _min_to = min(c_px * c_vol, p_px * p_vol)
                vol_score = max(0, min((_math.log10(max(_min_to, 10)) - 4.0) / 1.1, 1.0))
                # 3. 单腿最低权利金
                min_px_score = 1.0 if min(c_px, p_px) >= 4 else 0.5 if min(c_px, p_px) >= 2 else 0.1
                composite = sharpe * price_sym * (0.15 + 0.85 * vol_score) * min_px_score
                if best_pair is None or composite > best_pair['composite']:
                    best_pair = {
                        'prod': prod, 'cn': cn, 'ex': ex, 'contract': contract,
                        'dte': dte, 'bucket': bucket, 'sharpe': sharpe, 'wr': wr,
                        'entry': entry, 'tp': tp, 'sl': sl, 'n': n,
                        'call_sym': c_sym, 'put_sym': p_sym,
                        'call_px': c_px, 'put_px': p_px,
                        'call_k': c_k, 'put_k': p_k,
                        'call_vol': c_vol, 'put_vol': p_vol,
                        'fut_px': fut_px, 'psum': psum, 'ratio': ratio,
                        'total_vol': total_vol, 'composite': composite,
                    }
        if best_pair:
            # 计算正确的期权卖出保证金
            _mult = _MULTIPLIER.get(best_pair['prod'], 10)
            _mgn_r = _MARGIN_RATE.get(best_pair['prod'], 0.10)
            _margin = _calc_strangle_margin(
                best_pair['call_px'], best_pair['put_px'],
                best_pair['call_k'], best_pair['put_k'],
                best_pair['fut_px'], _mult, _mgn_r)
            best_pair['margin_per_lot'] = _margin
            best_pair['mult'] = _mult
            actionable.append(best_pair)

    # 按综合得分排序，取前20
    actionable.sort(key=lambda x: x['composite'], reverse=True)
    # 时段过滤：与上面的8维度表保持一致（品种+入场方式双重过滤）
    if night_only:
        actionable = [a for a in actionable
                      if a['prod'].upper() not in _NO_NIGHT_SESSION
                      and str(a.get('entry', '')).startswith('night')]
    top_picks = actionable[:20]

    # 计算建议手数和总保证金（全局预算约束）
    _otm_budget = account * 0.6
    _otm_per_cap = account * 0.05
    _otm_remaining = _otm_budget
    _otm_total_margin = 0
    for p in top_picks:
        mgn = p['margin_per_lot']
        if mgn <= 0 or _otm_remaining <= 0:
            p['lots'] = 0
            p['total_margin'] = 0
            continue
        max_budget = int(_otm_remaining / mgn)
        max_cap = max(1, int(_otm_per_cap / mgn))
        p['lots'] = min(max_budget, max_cap)
        p['total_margin'] = mgn * p['lots']
        _otm_remaining -= p['total_margin']
        _otm_total_margin += p['total_margin']

    # 存入全局变量，供智能推荐使用
    global _v6_recommended_pairs
    _v6_recommended_pairs = top_picks

    # === 统一交易计划：合并8D评分 + 实际期权对 + 信念标签 ===
    # 8D分数查询表
    pick_lookup = {}
    for p in picks:
        key = (p['prod'], p['contract'])
        if key not in pick_lookup:
            pick_lookup[key] = p

    # 信念系统标签
    belief_tags = {}  # (prod, contract) -> [tag_strings]
    for p in picks:
        prod_upper = p['prod'].upper()
        key = (p['prod'], p['contract'])
        dte = p.get('dte', 999)
        if weekday == 4 and prod_upper == 'P' and dte <= 7:
            belief_tags.setdefault(key, []).append('B018周五')
        if prod_upper == 'AO' and 5 <= dte <= 19:
            belief_tags.setdefault(key, []).append('B038夜Scalp')
        if prod_upper == 'CF' and 30 <= dte <= 60:
            belief_tags.setdefault(key, []).append('B021')
        if prod_upper == 'SA' and 25 <= dte <= 60:
            belief_tags.setdefault(key, []).append('B021')

    # 合并: actionable + 8D分数 + 信念标签
    unified = []
    for a in top_picks:
        key = (a['prod'], a['contract'])
        pd8 = pick_lookup.get(key, {})
        u = {**a}
        u['composite_8d'] = pd8.get('composite', a.get('sharpe', 0))
        u['vrp_label'] = pd8.get('vrp_label', '-')
        u['vrp_factor'] = pd8.get('vrp_factor', 1.0)
        u['news_label'] = pd8.get('news_label', '平静')
        u['news_factor'] = pd8.get('news_factor', 1.0)
        u['vol_label'] = pd8.get('vol_label', '正常')
        u['vol_factor'] = pd8.get('vol_factor', 1.0)
        u['liq_label'] = pd8.get('liq_label', '-')
        u['wday_label'] = pd8.get('wday_label', '')
        u['belief'] = belief_tags.get(key, [])
        # 手数用8D的(考虑了仓位分配)，若无则用OTM的
        u['lots_final'] = pd8.get('lots', a.get('lots', 1))
        unified.append(u)

    unified.sort(key=lambda x: x['composite_8d'], reverse=True)

    # === 渲染统一交易计划表 ===
    u_hdr = html.Tr([
        html.Th('#', style={'width': '22px', 'padding': '6px 3px'}),
        html.Th('品种', style={'width': '65px', 'padding': '6px 4px'}),
        html.Th('期权对', style={'width': '170px', 'padding': '6px 4px'}),
        html.Th('C价', style={'width': '42px', 'textAlign': 'right', 'padding': '6px 3px'}),
        html.Th('P价', style={'width': '42px', 'textAlign': 'right', 'padding': '6px 3px'}),
        html.Th('权金和', style={'width': '48px', 'textAlign': 'right', 'padding': '6px 3px'}),
        html.Th('DTE', style={'width': '32px', 'textAlign': 'center', 'padding': '6px 3px'}),
        html.Th('综合分', style={'width': '50px', 'textAlign': 'center', 'padding': '6px 3px'}),
        html.Th('VRP', style={'width': '38px', 'textAlign': 'center', 'padding': '6px 3px'}),
        html.Th('环境', style={'width': '65px', 'textAlign': 'center', 'padding': '6px 3px'}),
        html.Th('TP/SL', style={'width': '62px', 'textAlign': 'center', 'padding': '6px 3px'}),
        html.Th('手/保证金', style={'width': '80px', 'textAlign': 'right', 'padding': '6px 3px'}),
        html.Th('信号', style={'width': '60px', 'textAlign': 'center', 'padding': '6px 3px'}),
    ], style={'backgroundColor': '#1a1a3e', 'color': '#4fc3f7', 'fontSize': '11px'})

    u_rows = []
    u_total_margin = 0
    u_total_lots = 0
    for i, u in enumerate(unified):
        dte_c = '#f44336' if u['dte'] <= 7 else '#ff9800' if u['dte'] <= 14 else '#4caf50'
        cs = u['composite_8d']
        cs_c = '#00e676' if cs >= 30 else '#4fc3f7' if cs >= 15 else '#ff9800' if cs >= 8 else '#f44336'
        vrp_c = '#00e676' if u['vrp_factor'] >= 1.3 else '#4fc3f7' if u['vrp_factor'] >= 1.1 else '#888'
        news_c = '#f44336' if u['news_factor'] <= 0.5 else '#ff9800' if u['news_factor'] <= 0.7 else '#4caf50'
        vol_c = '#4caf50' if u['vol_factor'] >= 1.2 else '#888' if u['vol_factor'] >= 1.0 else '#f44336'
        ratio_c = '#00e676' if u['ratio'] <= 1.3 else '#4fc3f7' if u['ratio'] <= 1.8 else '#ff9800'
        tp_l = f'{u["tp"]:.1f}θ' if u['tp'] < 900 else '无'
        sl_l = f'{u["sl"]:.0f}x' if u['sl'] < 900 else '无'
        bg = '#0d1a0d' if i < 5 else '#0d1117'
        rank_c = '#FFD700' if i < 3 else '#00FF88' if i < 5 else '#aaa'

        lots = u['lots_final']
        mgn = u.get('margin_per_lot', 0)
        u_total_margin += mgn * lots
        u_total_lots += lots

        # 环境标签：新闻+波动 组合显示
        env_parts = []
        if u['news_label'] != '平静':
            env_parts.append(u['news_label'])
        if u['vol_label'] != '正常':
            env_parts.append(u['vol_label'])
        if u.get('wday_label'):
            env_parts.append(u['wday_label'])
        env_text = '/'.join(env_parts) if env_parts else '正常'

        # 信号标签
        belief_text = ' '.join(u['belief']) if u['belief'] else ''

        name_text = u.get('cn', u['prod'])

        u_rows.append(html.Tr([
            html.Td(f'{i+1}', style={'color': rank_c, 'fontWeight': 'bold', 'textAlign': 'center', 'padding': '4px 3px'}),
            html.Td(name_text, style={'color': '#fff' if i < 5 else '#aaa', 'fontWeight': 'bold' if i < 5 else 'normal', 'fontSize': '12px', 'padding': '4px'}),
            html.Td([
                html.Span(f'C{u["call_k"]:.0f}', style={'color': '#00BFFF'}),
                html.Span('+', style={'color': '#555'}),
                html.Span(f'P{u["put_k"]:.0f}', style={'color': '#FF6B6B'}),
                html.Span(f' ({(u["call_k"]-u["fut_px"])/u["fut_px"]*100:.0f}%/{(u["fut_px"]-u["put_k"])/u["fut_px"]*100:.0f}%)',
                           style={'color': '#666', 'fontSize': '10px'}),
            ], style={'fontSize': '11px', 'padding': '4px'}),
            html.Td(f'{u["call_px"]:.0f}', style={'textAlign': 'right', 'color': '#00BFFF', 'padding': '4px 3px'}),
            html.Td(f'{u["put_px"]:.0f}', style={'textAlign': 'right', 'color': '#FF6B6B', 'padding': '4px 3px'}),
            html.Td(f'{u["psum"]:.0f}', style={'textAlign': 'right', 'color': '#FFD700', 'fontWeight': 'bold', 'padding': '4px 3px'}),
            html.Td(str(u['dte']), style={'textAlign': 'center', 'color': dte_c, 'fontWeight': 'bold', 'padding': '4px 3px'}),
            html.Td(f'{cs:.1f}', style={'textAlign': 'center', 'color': cs_c, 'fontWeight': 'bold', 'fontSize': '13px', 'padding': '4px 3px'}),
            html.Td(u['vrp_label'], style={'textAlign': 'center', 'color': vrp_c, 'fontSize': '11px', 'padding': '4px 3px'}),
            html.Td(env_text, style={'textAlign': 'center', 'color': news_c if u['news_factor'] < 1.0 else vol_c, 'fontSize': '10px', 'padding': '4px 3px'}),
            html.Td(f'{tp_l}/{sl_l}', style={'textAlign': 'center', 'color': '#888', 'fontSize': '11px', 'padding': '4px 3px'}),
            html.Td(f'{lots}手/{mgn*lots/10000:.1f}万', style={'textAlign': 'right', 'color': '#4fc3f7', 'fontSize': '11px', 'padding': '4px 3px'}),
            html.Td(belief_text, style={'textAlign': 'center', 'color': '#FFD700', 'fontSize': '10px', 'fontWeight': 'bold', 'padding': '4px 3px'}),
        ], style={'backgroundColor': bg, 'borderBottom': '1px solid #1a1a3e'}))

    u_total_pct = u_total_margin / account if account > 0 else 0
    u_summary = html.Tr([
        html.Td(''),
        html.Td(f'{len(unified)}个可交易', style={'color': '#ff9800', 'fontWeight': 'bold', 'fontSize': '11px'}),
        html.Td(''), html.Td(''), html.Td(''), html.Td(''), html.Td(''), html.Td(''),
        html.Td(''), html.Td(''), html.Td(''),
        html.Td(f'{u_total_lots}手/{u_total_margin/10000:.1f}万({u_total_pct:.0%})',
                 style={'textAlign': 'right', 'color': '#ff9800', 'fontWeight': 'bold', 'fontSize': '11px'}),
        html.Td(''),
    ], style={'backgroundColor': '#2a1a0a', 'borderTop': '2px solid #ff9800'})

    u_table = html.Table(
        [html.Thead(u_hdr), html.Tbody(u_rows + [u_summary])],
        style={'width': '100%', 'borderCollapse': 'collapse', 'fontSize': '12px'})

    # 无匹配期权对的品种（在DTE窗口内但无行情数据）
    paired_prods = {(u['prod'], u['contract']) for u in unified}
    no_pair = [p for p in picks if (p['prod'], p['contract']) not in paired_prods and p.get('is_best')]
    no_pair_text = ''
    if no_pair:
        names = [f'{p["name"]}({p["contract"]})' for p in no_pair[:8]]
        no_pair_text = f'  另有{len(no_pair)}品种在窗口内但无期权行情: {", ".join(names)}'

    if unified:
        plan_sections.extend([
            html.Div([
                html.Span(session_label, style={'color': '#ff9800', 'fontSize': '15px', 'fontWeight': 'bold'}),
                html.Span(f'  {today_str} {now_time}  账户{account/10000:.1f}万({_acc_source})',
                           style={'color': '#666', 'fontSize': '12px'}),
                html.Span(filter_note, style={'color': '#4fc3f7', 'fontSize': '11px'}) if filter_note else None,
            ], style={'padding': '10px 25px', 'borderBottom': '1px solid #2a2a4a'}),
            html.Div(u_table, style={'padding': '5px 15px', 'overflowX': 'auto'}),
            html.Div([
                html.Span(f'综合分=Sharpe×VRP×新闻×波动×流动性×周度×DTE甜蜜区×胜率 | {" | ".join(factor_desc)}',
                           style={'color': '#555', 'fontSize': '10px'}),
                html.Span(no_pair_text, style={'color': '#666', 'fontSize': '10px'}) if no_pair_text else None,
            ], style={'padding': '6px 25px'}),
        ])
    else:
        plan_sections.append(html.Div('当前无可交易的宽跨机会',
            style={'color': '#888', 'padding': '30px', 'textAlign': 'center'}))

    panel = html.Div(plan_sections)

    # === 知识库信念驱动的智能提醒 (官方到期日历) ===
    today_d = datetime.now().date()
    is_friday = datetime.now().weekday() == 4

    # 到期日计算使用模块级统一函数 calc_expiry / _official_expiry
    _get_expiry = _official_expiry
    _cal = _trading_cal

    # 各品种合约月份 + 交易所映射 + 夜盘标识 (CTP验证)
    # 格式: (交易所, 中文名, 合约月份, DTE下限, DTE上限, Sharpe, 胜率, 参数, 有夜盘)
    _prod_info = {
        # Tier1 (Sharpe≥0.5)
        'jd':  ('DCE',  '鸡蛋',     list(range(1,13)),    3,7,   1.028, '93.9%', 'TP3%/SL0',       False),
        'sn':  ('SHFE', '锡',       list(range(1,13)),    7,10,  0.966, '85.9%', 'TP7%/SL0',       True),
        'ps':  ('GFEX', '瓶片',     list(range(1,13)),    20,25, 0.904, '92.3%', 'TP3%/SL0',       False),
        'OI':  ('CZCE', '菜油',     [1,3,5,7,9,11],      10,15, 0.808, '95.2%', 'TP3%/SL5',       True),
        'lg':  ('DCE',  '液化气',   list(range(1,13)),    50,60, 0.783, '93.3%', 'TP3%/SL5',       False),
        'lh':  ('DCE',  '生猪',     [1,3,5,7,9,11],      3,7,   0.737, '80.0%', 'TPx2/SL2.5',     False),
        'ao':  ('SHFE', '氧化铝',   list(range(1,13)),    7,10,  0.660, '92.3%', 'TP3%/SL0',       True),
        'AP':  ('CZCE', '苹果',     [1,3,5,10,11],       15,20, 0.651, '65.9%', 'TP15%/SL0',      False),
        'PK':  ('CZCE', '花生',     list(range(1,13)),    10,15, 0.634, '81.2%', 'TP3%/SL5',       False),
        'SR':  ('CZCE', '白糖',     [1,3,5,7,9,11],      7,10,  0.633, '90.1%', 'TP3%/SL3',       True),
        'CJ':  ('CZCE', '红枣',     [1,3,5,7,9,11],      15,20, 0.628, '71.8%', 'TP15%/SL0',      False),
        'SM':  ('CZCE', '锰硅',     list(range(1,13)),    7,10,  0.602, '76.6%', 'TPx1.5/SL2.5',   False),
        'br':  ('SHFE', '丁二烯橡胶', list(range(1,13)),  3,7,   0.532, '86.5%', 'TP7%/SL0',       True),
        # B021特殊策略 (DTE长周期入场)
        'CF':  ('CZCE', '棉花',     [1,3,5,7,9,11],      30,60, 0.366, '100%@30-60', 'B021:TP7%/SL2.1', True),
        'SA':  ('CZCE', '纯碱',     list(range(1,13)),    25,60, 0.470, '95-100%', 'B021:不设止盈止损', True),
    }

    alerts = []  # (priority, html_element)
    existing_prods = {p.get('prod', '').upper() for p in picks}

    # --- 扫描所有品种的DTE窗口 ---
    for prod, (ex, cn, months, dte_lo, dte_hi, sharpe, wr, params, has_night) in _prod_info.items():
        # 时段过滤：夜盘时段跳过无夜盘品种
        if night_only and not has_night:
            continue
        for mo_off in range(0, 8):
            yr, mo = today_d.year, today_d.month + mo_off
            if mo > 12:
                yr += 1; mo -= 12
            if mo not in months:
                continue
            exp = _get_expiry(ex, yr, mo, _cal, product_code=prod.upper())
            if not exp:
                continue
            dte = (exp - today_d).days
            if dte < 1 or dte > 70:
                continue
            contract = f'{prod}{yr % 100:02d}{mo:02d}'
            in_plan = prod.upper() in existing_prods
            in_window = dte_lo <= dte <= dte_hi
            # 即将进入: DTE从上方接近窗口(DTE > dte_hi, 1-3天内降至dte_hi)
            approaching = not in_window and (0 < (dte - dte_hi) <= 3)
            # 刚过窗口: DTE已低于下限(窗口已过,不再提醒)
            passed = dte < dte_lo

            # 生成操作建议时间（基于当前时刻）
            def _action_hint(has_night_, is_friday_):
                _h = datetime.now().hour
                if 9 <= _h < 15:
                    # 日盘中 → 现在就可以交易
                    return '现在可交易'
                elif 21 <= _h or _h < 3:
                    # 夜盘中
                    if has_night_:
                        return '现在可交易'
                    else:
                        return '明日日盘9:00卖出'
                else:
                    # 收盘后
                    if has_night_:
                        if is_friday_:
                            return '今晚21:00卖出'
                        return '今晚21:00卖出'
                    else:
                        if is_friday_:
                            return '周一日盘9:00卖出'
                        return '明日日盘9:00卖出'

            night_tag = '' if has_night else ' [无夜盘]'

            # 在最优窗口 + 不在计划中
            if in_window and not in_plan:
                tier = 'Tier1' if sharpe >= 0.5 else 'B021'
                action = _action_hint(has_night, is_friday)
                alerts.append((1, html.Div([
                    html.Span(f'{cn}({ex}.{prod}) ', style={
                        'color': '#ff4444', 'fontWeight': 'bold', 'fontSize': '13px'}),
                    html.Span(f'{contract} DTE={dte}天 到期{exp}', style={
                        'color': '#FFD700', 'fontWeight': 'bold'}),
                    html.Span(f' — {tier}最优区({dte_lo}-{dte_hi}天)', style={
                        'color': '#00FF88'}),
                    html.Span(f' → {action}{night_tag}', style={
                        'color': '#00BFFF', 'fontWeight': 'bold'}),
                    html.Div(f'  Sharpe={sharpe:.3f} 胜率{wr} {params}', style={
                        'color': '#aaa', 'fontSize': '11px', 'marginLeft': '20px'}),
                ], style={'marginBottom': '6px'})))

            # 即将进入窗口 (DTE略高于上限, 1-3天后进入) + Tier1
            elif approaching and not in_plan and sharpe >= 0.5:
                days_to = dte - dte_hi
                entry_d = today_d + timedelta(days=days_to)
                # 进入窗口那天的操作建议
                entry_weekday = entry_d.weekday()  # 0=Mon ... 4=Fri
                if entry_weekday >= 5:  # 周末顺延到周一
                    entry_d = entry_d + timedelta(days=(7 - entry_weekday))
                if has_night and entry_d.weekday() < 5:
                    when = f'{entry_d}(周{["一","二","三","四","五"][entry_d.weekday()]})晚21:00卖出'
                else:
                    when = f'{entry_d}(周{["一","二","三","四","五"][min(entry_d.weekday(),4)]})日盘9:00卖出'
                alerts.append((2, html.Div([
                    html.Span(f'{cn}({ex}.{prod}) ', style={
                        'color': '#ff9800', 'fontWeight': 'bold', 'fontSize': '13px'}),
                    html.Span(f'{contract} DTE={dte}天→最优{dte_lo}-{dte_hi}', style={
                        'color': '#FFD700'}),
                    html.Span(f' — {when}{night_tag}', style={
                        'color': '#4fc3f7'}),
                    html.Div(f'  Sharpe={sharpe:.3f} 胜率{wr}', style={
                        'color': '#888', 'fontSize': '11px', 'marginLeft': '20px'}),
                ], style={'marginBottom': '4px'})))

    # --- B038: AO氧化铝 夜盘后半段Scalp (每个交易日) ---
    ao_lines = []
    for mo_off in range(0, 6):
        yr, mo = today_d.year, today_d.month + mo_off
        if mo > 12:
            yr += 1; mo -= 12
        exp = _get_expiry('SHFE', yr, mo, _cal, product_code='AO')
        if not exp:
            continue
        dte = (exp - today_d).days
        contract = f'ao{yr % 100:02d}{mo:02d}'
        if 0 < dte <= 30:
            if 5 <= dte <= 19:
                ao_lines.append(html.Div(
                    f'  {contract} DTE={dte}天 到期{exp} — ★可交易: 胜率83-100%/均PnL+1.5~7.5%',
                    style={'color': '#00FF88', 'fontSize': '12px', 'fontWeight': 'bold'}))
            elif dte <= 4:
                ao_lines.append(html.Div(
                    f'  {contract} DTE={dte}天 到期{exp} — DTE过短,波动大,谨慎',
                    style={'color': '#ff9800', 'fontSize': '12px'}))
            else:
                ao_lines.append(html.Div(
                    f'  {contract} DTE={dte}天 到期{exp} — DTE偏长: θ衰减慢,TP触发率仍可但收益偏低',
                    style={'color': '#aaa', 'fontSize': '12px'}))
    if ao_lines:
        alerts.append((1, html.Div([
            html.Span('氧化铝(SHFE.AO) 夜盘后半段Scalp ', style={
                'color': '#ff4444', 'fontWeight': 'bold', 'fontSize': '13px'}),
            html.Span('B038 conf=0.82 | 23:15卖出宽跨→00:50强平 | θ止盈+腿比止损', style={
                'color': '#666', 'fontSize': '11px'}),
            *ao_lines,
            html.Div('策略: 23:15进仓 → TP=权利金÷DTE×0.5 → 无需止损(腿比天然平衡) → 00:50强平 | 入场腿比≤3x', style={
                'color': '#ff9800', 'fontSize': '11px', 'marginTop': '4px'}),
            html.Div('DTE5-10天最优 | 不持仓过夜 | 23:00后流动性下降但AO仍可交易至01:00', style={
                'color': '#888', 'fontSize': '11px'}),
        ], style={'marginBottom': '6px'})))

    # --- B018: 棕榈油周五夜盘Scalp ---
    if is_friday:
        p_lines = []
        for mo_off in range(0, 4):
            yr, mo = today_d.year, today_d.month + mo_off
            if mo > 12:
                yr += 1; mo -= 12
            exp = _get_expiry('DCE', yr, mo, _cal, product_code='P')
            if not exp:
                continue
            dte = (exp - today_d).days
            contract = f'p{yr % 100:02d}{mo:02d}'
            if 0 < dte <= 7:
                p_lines.append(html.Div(
                    f'  {contract} DTE={dte}天 到期{exp} — B018最优区: 89%胜率/+23%PnL',
                    style={'color': '#00FF88', 'fontSize': '12px', 'fontWeight': 'bold'}))
            elif 0 < dte <= 14:
                p_lines.append(html.Div(
                    f'  {contract} DTE={dte}天 到期{exp} — B018可用区: ~75%胜率',
                    style={'color': '#FFD700', 'fontSize': '12px'}))
        if p_lines:
            alerts.append((1, html.Div([
                html.Span('棕榈油(DCE.p) 周五夜盘Scalp ', style={
                    'color': '#ff4444', 'fontWeight': 'bold', 'fontSize': '13px'}),
                html.Span('B018 conf=0.83 | 周五夜盘胜率显著高于周一~四(89% vs 75%)', style={
                    'color': '#666', 'fontSize': '11px'}),
                *p_lines,
                html.Div('策略: 周五21:05卖出→22:55平仓(当晚了结,不持过周末)', style={
                    'color': '#ff9800', 'fontSize': '11px', 'marginTop': '4px'}),
                html.Div('条件: 总权金3-30, 两腿深OTM, ratio 0.5-2.0', style={
                    'color': '#888', 'fontSize': '11px'}),
            ], style={'marginBottom': '6px'})))

    # --- 周五提示: 持仓过周末需承受周一开盘gap风险 ---
    if is_friday:
        low_dte_names = [f"{p.get('cn','')}" for p in picks if p.get('dte', 99) <= 10]
        if low_dte_names:
            alerts.append((5, html.Div([
                html.Span('周五持仓提示: ', style={
                    'color': '#ff9800', 'fontWeight': 'bold', 'fontSize': '12px'}),
                html.Span(f'低DTE品种({",".join(low_dte_names)})若持过周末,周一9:00存在gap风险,但theta也在周末自然衰减',
                           style={'color': '#aaa', 'fontSize': '11px'}),
            ])))

    # 日历来源标注
    cal_note = '官方日历(含节假日+AP/CJ/PX特殊规则)'

    # 组装提醒面板
    if alerts:
        alerts.sort(key=lambda x: x[0])
        alert_items = [a[1] for a in alerts]
        n_critical = sum(1 for a in alerts if a[0] <= 1)
        n_upcoming = sum(1 for a in alerts if a[0] == 2)
        header_parts = []
        if n_critical:
            header_parts.append(f'{n_critical}个窗口内机会')
        if n_upcoming:
            header_parts.append(f'{n_upcoming}个即将进入')
        header_text = f'知识库信念提醒 ({" + ".join(header_parts)})' if header_parts else '知识库信念提醒'

        alert_panel = html.Div([
            html.Div([
                html.Span(header_text, style={
                    'color': '#ff4444' if n_critical else '#FFD700',
                    'fontSize': '14px', 'fontWeight': 'bold'}),
                html.Span(f'  B018/B021/B023 | {cal_note}', style={
                    'color': '#666', 'fontSize': '11px'}),
                html.Span('  [周五]' if is_friday else '', style={
                    'color': '#00FF88', 'fontSize': '12px', 'fontWeight': 'bold'}),
            ], style={'marginBottom': '8px'}),
            *alert_items,
        ], style={'padding': '12px 25px',
                  'borderTop': f'2px solid {"#ff4444" if n_critical else "#FFD700"}',
                  'backgroundColor': '#1a0a0a' if n_critical else '#1a1a0a'})
        panel.children.append(alert_panel)

    return panel, {
        'display': 'block', 'backgroundColor': '#111827',
        'borderBottom': '3px solid #ff9800', 'marginBottom': '5px',
    }


@app.callback(
    Output('spread-panel', 'children'),
    Output('spread-panel', 'style'),
    Input('spread-btn', 'n_clicks'),
    prevent_initial_call=True,
)
def toggle_spread(n_clicks):
    """切换价差Z-Score监控面板"""
    if not n_clicks or n_clicks % 2 == 0:
        return no_update, {'display': 'none'}

    panel = _build_spread_panel()
    return panel, {
        'display': 'block', 'backgroundColor': '#111827',
        'borderBottom': '3px solid #bb86fc', 'marginBottom': '5px',
    }


@app.callback(
    Output('alert-stats-panel', 'children'),
    Output('alert-stats-panel', 'style'),
    Input('alert-stats-btn', 'n_clicks'),
    prevent_initial_call=True,
)
def toggle_alert_stats(n_clicks):
    """切换预警回归统计面板"""
    if not n_clicks or n_clicks % 2 == 0:
        return no_update, {'display': 'none'}

    panel = _build_alert_stats_panel()
    return panel, {
        'display': 'block', 'backgroundColor': '#1a1a0a',
        'borderBottom': '3px solid #FFD700', 'marginBottom': '5px',
    }


def _build_alert_stats_panel():
    """构建预警统计面板（含回归+未回归，无幸存者偏差）"""
    alert_stats = get_alert_stats()
    active_count = len(_alert_active)
    children = []

    # 标题
    if not alert_stats:
        total_n, resolved_n, unresolved_n, rate = 0, 0, 0, 0
    else:
        total_n = alert_stats['total']
        resolved_n = alert_stats['resolved']
        unresolved_n = alert_stats['unresolved']
        rate = alert_stats['resolve_rate']
    children.append(html.Div([
        html.Span('预警统计（全样本）', style={'color': '#FFD700', 'fontSize': '16px', 'fontWeight': 'bold'}),
        html.Span(f'  总计 {total_n} | 回归 {resolved_n} | 未回归 {unresolved_n} | '
                  f'回归率 {rate:.0f}% | 活跃 {active_count}',
                   style={'color': '#888', 'fontSize': '12px', 'marginLeft': '12px'}),
    ], style={'marginBottom': '10px'}))

    if not alert_stats and active_count == 0:
        children.append(html.Div('暂无预警记录。盘中产生布林线预警后会自动记录并持久保存。',
                                  style={'color': '#666', 'fontSize': '13px', 'padding': '20px 0'}))
        return html.Div(children, style={'padding': '16px 24px'})

    if alert_stats:
        # 核心指标卡片
        cards = [
            ('回归率', rate, '%', '#00e676' if rate >= 80 else '#FFD700'),
            ('总样本', total_n, '', '#4fc3f7'),
            ('未回归', unresolved_n, '', '#FF4444' if unresolved_n > 0 else '#888'),
        ]
        if alert_stats.get('avg_minutes') is not None:
            cards += [
                ('平均回归', alert_stats['avg_minutes'], '分钟', '#FFD700'),
                ('中位数', alert_stats['median_minutes'], '分钟', '#FFD700'),
                ('P25', alert_stats['p25_minutes'], '分钟', '#FFD700'),
                ('P75', alert_stats['p75_minutes'], '分钟', '#FFD700'),
                ('最快', alert_stats['min_minutes'], '分钟', '#FFD700'),
                ('最慢', alert_stats['max_minutes'], '分钟', '#FFD700'),
            ]
        if alert_stats.get('avg_deviation') is not None:
            cards += [
                ('平均偏离', alert_stats['avg_deviation'], '%', '#FF8888'),
                ('平均峰值偏离', alert_stats['avg_peak_deviation'], '%', '#FF8888'),
            ]

        card_divs = []
        for label, val, unit, color in cards:
            card_divs.append(html.Div([
                html.Div(f'{val:.0f}{unit}', style={
                    'fontSize': '20px', 'fontWeight': 'bold', 'color': color}),
                html.Div(label, style={'fontSize': '11px', 'color': '#888'}),
            ], style={'textAlign': 'center', 'padding': '10px 16px', 'backgroundColor': '#1a1a2e',
                      'borderRadius': '6px', 'minWidth': '90px'}))
        children.append(html.Div(card_divs, style={
            'display': 'flex', 'gap': '10px', 'flexWrap': 'wrap', 'marginBottom': '14px'}))

        # 品种汇总（一行一个品种）
        if alert_stats.get('by_product'):
            prod_items = sorted(alert_stats['by_product'].items(),
                                key=lambda x: x[1]['total'], reverse=True)
            rows = []
            th_style = {'color': '#666', 'padding': '4px 12px', 'textAlign': 'left'}
            for p, d in prod_items:
                avg_str = f'{d["avg_min"]:.0f}分钟' if d.get('avg_min') is not None else '-'
                med_str = f'{d["median_min"]:.0f}分钟' if d.get('median_min') is not None else '-'
                rate_color = '#00e676' if d['resolve_rate'] >= 80 else '#FFD700' if d['resolve_rate'] >= 50 else '#FF4444'
                rows.append(html.Tr([
                    html.Td(p, style={'color': '#FFD700', 'fontWeight': 'bold', 'padding': '4px 12px'}),
                    html.Td(f'{d["total"]}', style={'color': '#aaa', 'padding': '4px 12px'}),
                    html.Td(f'{d["resolved"]}', style={'color': '#00e676', 'padding': '4px 12px'}),
                    html.Td(f'{d["unresolved"]}', style={'color': '#FF4444' if d["unresolved"] else '#aaa', 'padding': '4px 12px'}),
                    html.Td(f'{d["resolve_rate"]:.0f}%', style={'color': rate_color, 'fontWeight': 'bold', 'padding': '4px 12px'}),
                    html.Td(avg_str, style={'color': '#aaa', 'padding': '4px 12px'}),
                    html.Td(med_str, style={'color': '#aaa', 'padding': '4px 12px'}),
                ]))
            children.append(html.Div([
                html.Div('品种汇总', style={'color': '#FFD700', 'fontSize': '13px', 'fontWeight': 'bold',
                                         'marginBottom': '6px'}),
                html.Table([
                    html.Thead(html.Tr([
                        html.Th('品种', style=th_style), html.Th('总计', style=th_style),
                        html.Th('回归', style=th_style), html.Th('未回归', style=th_style),
                        html.Th('回归率', style=th_style), html.Th('平均', style=th_style),
                        html.Th('中位', style=th_style),
                    ])),
                    html.Tbody(rows),
                ], style={'fontSize': '12px'}),
            ], style={'marginBottom': '14px'}))

        # 全部记录明细（每条带期权对）
        if alert_stats.get('recent'):
            rec_rows = []
            for r in alert_stats['recent'][:30]:
                is_resolved = r.get('resolved', False)
                if is_resolved:
                    status = html.Span(f'{r.get("resolve_minutes", 0):.0f}分钟',
                                       style={'color': '#00e676', 'fontWeight': 'bold'})
                    sum_text = f'{r["alert_sum"]:.0f} -> {r.get("resolve_sum", 0):.0f}'
                else:
                    status = html.Span('未回归', style={'color': '#FF4444', 'fontWeight': 'bold'})
                    last = r.get('last_sum', r['alert_sum'])
                    sum_text = f'{r["alert_sum"]:.0f} -> {last:.0f}'
                pair_text = f'{r.get("call_sym", "?")} + {r.get("put_sym", "?")}'
                rec_rows.append(html.Tr([
                    html.Td(r['futures_sym'], style={'color': '#FFD700', 'padding': '3px 10px'}),
                    html.Td(pair_text, style={'color': '#8888ff', 'padding': '3px 10px', 'fontSize': '11px'}),
                    html.Td(r['alert_time'][-14:], style={'color': '#aaa', 'padding': '3px 10px', 'fontSize': '11px'}),
                    html.Td(sum_text, style={'color': '#aaa', 'padding': '3px 10px'}),
                    html.Td(f'+{r.get("peak_deviation", 0)*100:.1f}%',
                             style={'color': '#FF8888', 'padding': '3px 10px'}),
                    html.Td(status, style={'padding': '3px 10px'}),
                ]))
            children.append(html.Div([
                html.Div('最近记录', style={'color': '#FFD700', 'fontSize': '13px', 'fontWeight': 'bold',
                                         'marginBottom': '6px'}),
                html.Table([
                    html.Thead(html.Tr([
                        html.Th('品种', style={'color': '#666', 'padding': '3px 10px', 'textAlign': 'left'}),
                        html.Th('期权对', style={'color': '#666', 'padding': '3px 10px', 'textAlign': 'left'}),
                        html.Th('时间', style={'color': '#666', 'padding': '3px 10px', 'textAlign': 'left'}),
                        html.Th('Sum变化', style={'color': '#666', 'padding': '3px 10px', 'textAlign': 'left'}),
                        html.Th('峰值偏离', style={'color': '#666', 'padding': '3px 10px', 'textAlign': 'left'}),
                        html.Th('结果', style={'color': '#666', 'padding': '3px 10px', 'textAlign': 'left'}),
                    ])),
                    html.Tbody(rec_rows),
                ], style={'fontSize': '12px'}),
            ], style={'marginBottom': '14px'}))

    # 当前活跃预警
    if _alert_active:
        active_rows = []
        now = datetime.now()
        for key, rec in _alert_active.items():
            try:
                t0 = datetime.strptime(rec['alert_time'], '%Y-%m-%d %H:%M:%S')
                elapsed = f'{(now - t0).total_seconds() / 60:.0f}分钟'
            except Exception:
                elapsed = '?'
            pair_text = f'{rec.get("call_sym", "?")} + {rec.get("put_sym", "?")}'
            active_rows.append(html.Tr([
                html.Td(rec['futures_sym'], style={'color': '#FF4444', 'fontWeight': 'bold', 'padding': '3px 10px'}),
                html.Td(pair_text, style={'color': '#8888ff', 'padding': '3px 10px', 'fontSize': '11px'}),
                html.Td(rec['alert_time'][-8:], style={'color': '#FF8888', 'padding': '3px 10px'}),
                html.Td(elapsed, style={'color': '#FF8888', 'padding': '3px 10px'}),
                html.Td(f'{rec["alert_sum"]:.0f}', style={'color': '#aaa', 'padding': '3px 10px'}),
                html.Td(f'{rec["peak_sum"]:.0f}', style={'color': '#FF8888', 'padding': '3px 10px'}),
                html.Td(f'+{rec.get("alert_deviation", 0)*100:.1f}%',
                         style={'color': '#FF8888', 'padding': '3px 10px'}),
            ]))
        children.append(html.Div([
            html.Div(f'当前活跃预警 ({len(_alert_active)})', style={
                'color': '#FF4444', 'fontSize': '13px', 'fontWeight': 'bold', 'marginBottom': '6px'}),
            html.Table([
                html.Thead(html.Tr([
                    html.Th('品种', style={'color': '#666', 'padding': '3px 10px', 'textAlign': 'left'}),
                    html.Th('期权对', style={'color': '#666', 'padding': '3px 10px', 'textAlign': 'left'}),
                    html.Th('触发', style={'color': '#666', 'padding': '3px 10px', 'textAlign': 'left'}),
                    html.Th('已持续', style={'color': '#666', 'padding': '3px 10px', 'textAlign': 'left'}),
                    html.Th('触发Sum', style={'color': '#666', 'padding': '3px 10px', 'textAlign': 'left'}),
                    html.Th('峰值Sum', style={'color': '#666', 'padding': '3px 10px', 'textAlign': 'left'}),
                    html.Th('偏离', style={'color': '#666', 'padding': '3px 10px', 'textAlign': 'left'}),
                ])),
                html.Tbody(active_rows),
            ], style={'fontSize': '12px'}),
        ]))

    return html.Div(children, style={'padding': '16px 24px'})


@app.server.route('/diag')
def diag_page():
    """诊断页面：检查浏览器是否能正常与 Dash 通信"""
    return """
    <html><body style="background:#111;color:#0f0;font-family:monospace;padding:20px">
    <h2>Dash 诊断</h2>
    <div id="result">测试中...</div>
    <script>
    async function test() {
        var el = document.getElementById('result');
        var log = [];

        // Test 1: 基本连接
        try {
            var r = await fetch('/_dash-layout');
            log.push('Layout: ' + r.status + ' size=' + (await r.clone().text()).length);
        } catch(e) { log.push('Layout ERROR: ' + e); }

        // Test 2: 小回调
        try {
            var r2 = await fetch('/_dash-update-component', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    output: 'charts-container.children',
                    outputs: {id:'charts-container', property:'children'},
                    inputs: [{id:'pairs-store',property:'data',value:[]},
                             {id:'timer',property:'n_intervals',value:1}],
                    changedPropIds: ['timer.n_intervals']
                })
            });
            var txt = await r2.text();
            log.push('Callback: ' + r2.status + ' size=' + txt.length);
            if (txt.length < 100) log.push('Body: ' + txt);
        } catch(e) { log.push('Callback ERROR: ' + e); }

        el.innerHTML = log.join('<br>');
    }
    test();
    </script></body></html>
    """


if __name__ == '__main__':
    pairs = load_config()
    print(f'期权工作台: http://localhost:{PORT}')
    print(f'已加载 {len(pairs)} 个期权对')
    for pair in pairs:
        c, p = pair[0], pair[1]
        cc = pair[2] if len(pair) > 2 else 1.0
        pc = pair[3] if len(pair) > 3 else 1.0
        coeff_info = f' (C×{cc:g}, P×{pc:g})' if cc != 1.0 or pc != 1.0 else ''
        print(f'  {c} + {p}{coeff_info}')

    # 启动资讯整点自动采集线程
    _news_thread = threading.Thread(target=_news_hourly_fetcher, daemon=True)
    _news_thread.start()
    print('资讯自动采集: 每整点更新')

    app.run(host='0.0.0.0', port=PORT, debug=False)
