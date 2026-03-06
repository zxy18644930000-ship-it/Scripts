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

DB_PATH = os.path.expanduser('~/.vntrader/database.db')
CONFIG_PATH = os.path.expanduser('~/Scripts/price_sum_pairs.json')
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

# 各交易所期权到期: 交割月前一月的近似日 (不含节假日调整)
_EXPIRY_DAY = {
    'CZCE': 11, 'CZCE_SP': -5, 'DCE': 17, 'SHFE': 25, 'GFEX': 6, 'INE': 13,
}

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
    这些文件由交易系统每60秒自动保存，不需要重启交易系统。
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
            # 跳过超过5分钟没更新的文件
            if time.time() - os.path.getmtime(fpath) > 300:
                continue
            try:
                with open(fpath, 'r') as f:
                    state = json.load(f)
                product = state.get('product_code', '')
                for p in state.get('positions', []):
                    if p.get('volume', 0) > 0:
                        positions.append({
                            'symbol': p.get('symbol', ''),
                            'exchange': p.get('exchange', ''),
                            'direction': p.get('direction', ''),
                            'volume': p.get('volume', 0),
                            'avg_price': p.get('avg_price', 0),
                            'pnl': p.get('pnl', 0),
                            'product': product,
                        })
            except Exception:
                continue
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
    """读取所有活跃的策略状态文件（含extra信息）"""
    states = []
    try:
        if not os.path.isdir(_STRATEGY_STATE_DIR):
            return states
        for fname in os.listdir(_STRATEGY_STATE_DIR):
            if not fname.endswith('_state.json'):
                continue
            fpath = os.path.join(_STRATEGY_STATE_DIR, fname)
            if time.time() - os.path.getmtime(fpath) > 300:
                continue
            try:
                with open(fpath, 'r') as f:
                    state = json.load(f)
                if state.get('positions'):
                    states.append(state)
            except Exception:
                continue
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
        for p in state.get('positions', []):
            if p.get('volume', 0) > 0:
                p['product'] = product
                positions.append(p)

    if positions:
        row1_parts.append(html.Span('持仓 ', style={'color': '#888', 'fontSize': '12px', 'marginRight': '8px'}))
        for p in positions:
            sym = p.get('symbol', '?')
            direction = p.get('direction', '')
            vol = p.get('volume', 0)
            avg = p.get('avg_price', 0)
            dir_label = '空' if 'SHORT' in str(direction).upper() else '多'
            dir_color = '#FF8800' if 'SHORT' in str(direction).upper() else '#00BFFF'
            row1_parts.append(html.Span([
                html.Span(f'{sym}', style={'color': '#4fc3f7', 'fontSize': '13px', 'fontWeight': 'bold'}),
                html.Span(f' {dir_label}{vol}手', style={'color': dir_color, 'fontSize': '12px'}),
                html.Span(f' @{avg:.0f}', style={'color': '#888', 'fontSize': '11px'}),
            ], style={'marginRight': '18px'}))

    # 从dashboard.json读账户资金和系统状态
    dash_data = _load_dashboard()
    if dash_data:
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

        # 强平时间
        fc_night = tp.get('force_close_start_night', '')
        fc_day = tp.get('force_close_start_day', '')
        fc_parts = []
        if fc_night and fc_night != '02:00:00':
            fc_parts.append(f'夜{fc_night[:5]}')
        if fc_day and fc_day != '02:00:00':
            fc_parts.append(f'日{fc_day[:5]}')
        if fc_parts:
            row2_parts.append(html.Span(f'强平{"/".join(fc_parts)}', style={
                **tag_style, 'color': '#FF8800', 'backgroundColor': '#FF880022'}))
        else:
            row2_parts.append(html.Span('强平禁用', style={
                **tag_style, 'color': '#666', 'backgroundColor': '#66666622'}))

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


def _make_advisory_spans(call_sym):
    """为期权对生成交易建议，返回 html.Span 列表（嵌入标题行，不新建 div）"""
    try:
        parsed = _parse_contract(call_sym)
        if not parsed:
            return []
        product, month, _, _ = parsed
        dte, exchange = _estimate_dte(product, month)
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

    all_times = sorted(set(call_data.keys()) | set(put_data.keys()) | set(futures_data.keys()))
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


def _check_double_rise(times, call_prices, put_prices, sum_prices):
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
    return {'alert': False}


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
    dr = _check_double_rise(times, call_prices, put_prices, sum_prices)
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
_STRIKE_PARAMS = {
    'P': (0.08, 0.14, 0.15), 'TA': (0.05, 0.10, 0.12),
    'AG': (0.05, 0.09, 0.12), 'EB': (0.08, 0.14, 0.15),
    'CF': (0.08, 0.14, 0.15), 'SA': (0.02, 0.04, 0.10),
    'SP': (0.02, 0.04, 0.06), 'AO': (0.02, 0.04, 0.06),
    'FG': (0.02, 0.04, 0.06),
}
_DEFAULT_STRIKE_PARAMS = (0.08, 0.14, 0.15)

_auto_cache = {'pairs': [], 'ts': None}


def _score_pair(cp, pp, cs, ps, fp, cv, pv, strike_params=None):
    """计算期权对得分 (0-10)
    70% 价格接近度 + 15% 行权价间隔 + 15% 流动性（阈值制，>50手/分钟满分）
    """
    if cp <= 0 or pp <= 0 or fp <= 0:
        return 0
    ratio = max(cp, pp) / min(cp, pp)
    if ratio <= 1.3:
        price_score = 1.0
    elif ratio < 1.5:
        price_score = 1.0 - (ratio - 1.3) / 0.2 * 0.5  # 1.3→1.0, 1.5→0.5
    else:
        return 0  # ratio>=1.5 直接淘汰
    spacing_pct = abs(cs - ps) / fp
    opt_min, opt_max, max_r = strike_params or _DEFAULT_STRIKE_PARAMS
    if spacing_pct <= opt_min * 0.3:
        spacing_score = 0.0
    elif spacing_pct < opt_min:
        spacing_score = (spacing_pct - opt_min * 0.3) / (opt_min * 0.7)
    elif spacing_pct <= opt_max:
        spacing_score = 1.0
    elif spacing_pct < max_r:
        spacing_score = 1.0 - (spacing_pct - opt_max) / (max_r - opt_max)
    else:
        spacing_score = 0.0
    avg_vol = (cv + pv) / 2
    liq_score = min(1.0, avg_vol / 50)  # 阈值制：50手/分钟即满分
    return (price_score * 0.70 + spacing_score * 0.15 + liq_score * 0.15) * 10


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
        otm_calls = [(s, p, v, k) for s, p, v, k in data['calls'] if k > fp]
        otm_puts = [(s, p, v, k) for s, p, v, k in data['puts'] if k < fp]
        if not otm_calls or not otm_puts:
            continue
        params = _STRIKE_PARAMS.get(prod, _DEFAULT_STRIKE_PARAMS)

        best = None
        for c_sym, c_px, c_vol, c_k in otm_calls:
            for p_sym, p_px, p_vol, p_k in otm_puts:
                if c_px + p_px < 8 or min(c_px, p_px) < 2:
                    continue
                spacing = abs(c_k - p_k) / fp if fp > 0 else 0
                if spacing < 0.02:
                    continue
                score = _score_pair(c_px, p_px, c_k, p_k, fp, c_vol, p_vol, params)
                if score > 0 and (best is None or score > best[0]):
                    best = (score, c_sym, p_sym, c_px + p_px)

        if best:
            score, c_sym, p_sym, psum = best
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
    """估算DTE天数"""
    product_upper = product.upper()
    exchange = _EXCHANGE_MAP.get(product_upper, '')

    try:
        if len(month_str) == 3:
            # CZCE格式: 604 → 2026-04
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

    import calendar
    exp_day = _EXPIRY_DAY.get(exchange, 15)
    if exp_day < 0:
        # 倒数第N天 → 月末往前推
        exp_month = mon - 1 if mon > 1 else 12
        exp_year = year if mon > 1 else year - 1
        last_day = calendar.monthrange(exp_year, exp_month)[1]
        exp_day = last_day + exp_day
    else:
        exp_month = mon - 1 if mon > 1 else 12
        exp_year = year if mon > 1 else year - 1

    try:
        exp_date = date(exp_year, exp_month, min(exp_day, 28))
    except ValueError:
        return None

    dte = (exp_date - date.today()).days
    return max(dte, 0)


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
            cur.execute("SELECT close_price FROM dbbardata WHERE symbol=? ORDER BY datetime DESC LIMIT 1",
                        (best_sym,))
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
            cur.execute("SELECT close_price FROM dbbardata WHERE symbol=? ORDER BY datetime DESC LIMIT 1",
                        (atm_call,))
            row = cur.fetchone()
            if not row or row[0] <= 0:
                continue
            call_price = row[0]

            # ATM Put 最新价
            cur.execute("SELECT close_price FROM dbbardata WHERE symbol=? ORDER BY datetime DESC LIMIT 1",
                        (atm_put,))
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
        html.Span('Ask≥', style={'color': '#aaa', 'fontSize': '11px', 'marginLeft': '10px'}),
        dcc.Input(
            id={'type': 'min-ask-sum', 'index': idx}, type='number', placeholder='不限',
            style={'width': '62px', 'padding': '5px 6px', 'fontSize': '12px',
                   'backgroundColor': '#1a1a3e', 'color': '#FFD700',
                   'border': '1px solid #555', 'borderRadius': '4px', 'textAlign': 'center'}),
        html.Button('卖出进仓', id={'type': 'trade-entry-btn', 'index': idx}, n_clicks=0, style={
            'padding': '5px 16px', 'fontSize': '13px', 'cursor': 'pointer',
            'backgroundColor': '#1a4a1e', 'color': '#00FF88',
            'border': '1px solid #00FF88', 'borderRadius': '4px', 'marginLeft': '6px',
            'fontWeight': 'bold'}),
        html.Span(id={'type': 'trade-entry-status', 'index': idx}, style={
            'color': '#00FF88', 'fontSize': '12px', 'marginLeft': '10px'}),
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
        html.Span('Bid≤', style={'color': '#aaa', 'fontSize': '11px', 'marginLeft': '10px'}),
        dcc.Input(
            id={'type': 'max-bid-sum', 'index': idx}, type='number', placeholder='不限',
            style={'width': '62px', 'padding': '5px 6px', 'fontSize': '12px',
                   'backgroundColor': '#1a1a3e', 'color': '#FF6B6B',
                   'border': '1px solid #555', 'borderRadius': '4px', 'textAlign': 'center'}),
        html.Button('买入平仓', id={'type': 'trade-close-btn', 'index': idx}, n_clicks=0, style={
            'padding': '5px 16px', 'fontSize': '13px', 'cursor': 'pointer',
            'backgroundColor': '#4a1a1e', 'color': '#FF6B6B',
            'border': '1px solid #FF6B6B', 'borderRadius': '4px', 'marginLeft': '6px',
            'fontWeight': 'bold'}),
        html.Span(id={'type': 'trade-close-status', 'index': idx}, style={
            'color': '#FF6B6B', 'fontSize': '12px', 'marginLeft': '10px'}),
        html.Span('│', style={'color': '#333', 'margin': '0 8px'}),
        html.Button('停', id={'type': 'emergency-btn', 'index': idx}, n_clicks=0, style={
            'padding': '3px 10px', 'fontSize': '12px', 'cursor': 'pointer',
            'backgroundColor': '#4a0000', 'color': '#FF6B6B',
            'border': '1px solid #FF6B6B', 'borderRadius': '4px',
            'fontWeight': 'bold'}),
        html.Span(id={'type': 'emergency-status', 'index': idx}, style={
            'fontSize': '11px', 'marginLeft': '6px'}),
    ]

    return html.Div([
        html.Div(line1, style={'display': 'flex', 'alignItems': 'center', 'flexWrap': 'wrap', 'gap': '2px 0'}),
        html.Div(line2, style={'display': 'flex', 'alignItems': 'center', 'flexWrap': 'wrap',
                                'gap': '2px 0', 'marginTop': '3px'}),
    ], id={'type': 'trade-row', 'index': idx}, style={'marginBottom': '8px'})


app.layout = html.Div([
    # 顶部标题栏
    html.Div([
        html.H2('期权工作台', style={'margin': '0', 'color': '#fff', 'display': 'inline-block'}),
        html.Button('资讯', id='news-btn', n_clicks=0, style={
            'float': 'right', 'padding': '6px 16px', 'fontSize': '13px',
            'cursor': 'pointer', 'backgroundColor': '#0a4a6e', 'color': '#4fc3f7',
            'border': '1px solid #4fc3f7', 'borderRadius': '4px', 'marginTop': '3px'}),
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
    ], style={'backgroundColor': '#1a1a2e', 'padding': '15px 25px',
              'borderBottom': '3px solid #e94560'}),

    # 今日计划面板（默认隐藏）
    html.Div(id='plan-panel', style={'display': 'none'}),

    # VRP扫描面板（默认隐藏）
    html.Div(id='vrp-panel', style={'display': 'none'}),

    # 资讯面板（默认隐藏）
    html.Div(id='news-panel', style={'display': 'none'}),

    # 账户+持仓状态栏（由tick快照动态更新）
    html.Div(id='account-bar'),

    # 交易面板（动态多行）
    html.Div([
        html.Div(id='trade-rows-container', children=[
            _build_trade_row(0),
        ]),
        # 全局紧急停止保留为隐藏占位（回调引用需要）
        html.Button(id='emergency-stop-btn', n_clicks=0, style={'display': 'none'}),
        html.Span(id='emergency-stop-status', style={'display': 'none'}),
        dcc.Store(id='trade-row-count', data=1),
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

    # 定时刷新
    dcc.Interval(id='timer', interval=REFRESH_MS, n_intervals=0),
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
        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT close_price FROM dbbardata WHERE symbol=? ORDER BY datetime DESC LIMIT 1", (leg1,))
        r1 = cur.fetchone()
        cur.execute("SELECT close_price FROM dbbardata WHERE symbol=? ORDER BY datetime DESC LIMIT 1", (leg2,))
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
            return no_update, f'{call_sym}+{put_sym} 已存在'
    pairs.insert(0, [call_sym, put_sym, 1.0, 1.0])  # 排在最前面
    save_config(pairs)
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
            data = resp.json()
            if resp.status_code == 200 and data.get('ok'):
                return True, data.get('message', '成功')
            return False, data.get('error', f'HTTP {resp.status_code}')
        except Exception:
            pass  # HTTP 失败，fallback 文件
    # Fallback: 写文件信号（兼容旧策略）
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
            if os.path.exists(es_path):
                os.unlink(es_path)
                return True, '已恢复(文件)'
            else:
                with open(es_path, 'w') as f:
                    from datetime import datetime as _dt
                    f.write(_dt.now().isoformat())
                return True, '已停止(文件)'
        return False, f'未知端点: {endpoint}'
    except Exception as e:
        return False, str(e)


def _start_strategy(product):
    """自动启动指定品种的策略实例"""
    import subprocess as _sp
    venv_python = os.path.join(_TRADE2026, '.venv/bin/python')
    main_py = os.path.join(_TRADE2026, 'main.py')
    log_file = os.path.join(_STATE_DIR, f'{product.lower()}_auto.log')
    cmd = f'cd {_TRADE2026} && nohup {venv_python} {main_py} live --product {product} --strategy strangle_sell -m auto >> {log_file} 2>&1 &'
    _sp.Popen(cmd, shell=True, cwd=_TRADE2026)
    return log_file


def _smart_round_volumes(total_volume):
    """智能分轮：1-2手不拆，3-6手每轮1手，7-15手每轮2手，16-30手每轮3手，31+手每轮5手"""
    if total_volume <= 0:
        return []
    if total_volume <= 2:
        return [total_volume]
    if total_volume <= 6:
        per = 1
    elif total_volume <= 15:
        per = 2
    elif total_volume <= 30:
        per = 3
    else:
        per = 5
    rounds = []
    rem = total_volume
    while rem > 0:
        rounds.append(min(per, rem))
        rem -= rounds[-1]
    return rounds




@app.callback(
    Output('emergency-stop-status', 'children'),
    Output({'type': 'trade-entry-status', 'index': ALL}, 'children', allow_duplicate=True),
    Output({'type': 'min-ask-sum', 'index': ALL}, 'value', allow_duplicate=True),
    Input('emergency-stop-btn', 'n_clicks'),
    State({'type': 'min-ask-sum', 'index': ALL}, 'value'),
    prevent_initial_call=True,
)
def on_emergency_stop_click(n_clicks, all_min_ask):
    """紧急停止：POST /emergency_stop 到所有运行中的策略，清除条件单"""
    if not n_clicks:
        n_rows = len(all_min_ask) if all_min_ask else 0
        return no_update, [no_update] * n_rows, [no_update] * n_rows
    n_rows = len(all_min_ask) if all_min_ask else 0
    results = []
    any_stopped = False
    any_resumed = False
    tried_products = set()
    try:
        for f in os.listdir(_STATE_DIR):
            if f.endswith('_api_port'):
                product = f.replace('_api_port', '')
                tried_products.add(product)
                ok, msg = _send_strategy_command(product, 'emergency_stop')
                if ok:
                    if '恢复' in msg:
                        any_resumed = True
                    else:
                        any_stopped = True
                    results.append(f'{product}:{msg}')
                else:
                    results.append(f'{product}:失败-{msg}')
    except Exception:
        pass

    if not tried_products:
        ok, msg = _send_strategy_command_file('ALL', 'emergency_stop')
        if ok:
            if '恢复' in msg:
                any_resumed = True
            else:
                any_stopped = True
            results.append(msg)

    ts = datetime.now().strftime("%H:%M:%S")
    if not results:
        return (html.Span(f'无运行中的策略 {ts}', style={'color': '#FFD700'}),
                [no_update] * n_rows, [no_update] * n_rows)

    summary = ', '.join(results)
    if any_stopped:
        status_msg = html.Span(f'已停止！ {summary} ({ts})',
                               style={'color': '#FF4444', 'fontWeight': 'bold'})
        # 清除所有条件单状态 + 归零 Ask 输入框
        entry_statuses = [html.Span(f'条件单已取消 ({ts})', style={'color': '#FF8800', 'fontSize': '11px'})
                          for _ in range(n_rows)]
        ask_values = [None for _ in range(n_rows)]
        return status_msg, entry_statuses, ask_values
    elif any_resumed:
        status_msg = html.Span(f'已恢复 {summary} ({ts})',
                               style={'color': '#00FF88'})
        return status_msg, [no_update] * n_rows, [no_update] * n_rows

    return (html.Span(f'{summary} ({ts})', style={'color': '#FFD700'}),
            [no_update] * n_rows, [no_update] * n_rows)


@app.callback(
    Output({'type': 'emergency-status', 'index': MATCH}, 'children'),
    Output({'type': 'trade-entry-status', 'index': MATCH}, 'children', allow_duplicate=True),
    Output({'type': 'min-ask-sum', 'index': MATCH}, 'value', allow_duplicate=True),
    Output({'type': 'trade-close-status', 'index': MATCH}, 'children', allow_duplicate=True),
    Output({'type': 'max-bid-sum', 'index': MATCH}, 'value', allow_duplicate=True),
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

        ok, msg = _send_strategy_command(product, 'emergency_stop')
        ts = datetime.now().strftime("%H:%M:%S")
        if ok:
            if '恢复' in msg:
                return (html.Span(f'{product}已恢复 ({ts})', style={'color': '#00FF88'}),
                        no_update, no_update, no_update, no_update)
            else:
                cancelled = html.Span(f'条件单已取消 ({ts})',
                                      style={'color': '#FF8800', 'fontSize': '11px'})
                return (html.Span(f'{product}已停止 ({ts})',
                                  style={'color': '#FF4444', 'fontWeight': 'bold'}),
                        cancelled, None, cancelled, None)
        else:
            return (html.Span(f'{product}停止失败: {msg} ({ts})', style={'color': '#FFD700'}),
                    no_update, no_update, no_update, no_update)
    except Exception as e:
        return html.Span(f'错误: {e}', style={'color': '#FF4444'}), no_update, no_update, no_update, no_update


@app.callback(
    Output({'type': 'trade-pair-select', 'index': ALL}, 'options'),
    Input('pairs-store', 'data'),
)
def update_trade_pair_options(pairs):
    """根据工作台期权对列表更新所有交易行下拉选项"""
    if not pairs:
        options = []
    else:
        options = []
        for pair in pairs:
            call_sym, put_sym = pair[0], pair[1]
            label = f'{call_sym} + {put_sym}'
            value = json.dumps({'call': call_sym, 'put': put_sym})
            options.append({'label': label, 'value': value})
    # 返回列表长度需要匹配ALL的组件数
    from dash import callback_context
    n_outputs = len(callback_context.outputs_list)
    return [options] * max(n_outputs, 1)


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

    # 已就绪的不重复加载
    if product in loading_state:
        elapsed = time.time() - loading_state[product]
        if elapsed >= _LOAD_READY_SECONDS and _is_strategy_running(product):
            return no_update, no_update

    # 启动策略
    if not _is_strategy_running(product):
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
    Output({'type': 'trade-entry-status', 'index': MATCH}, 'children'),
    Input({'type': 'trade-entry-btn', 'index': MATCH}, 'n_clicks'),
    State({'type': 'trade-pair-select', 'index': MATCH}, 'value'),
    State({'type': 'trade-volume', 'index': MATCH}, 'value'),
    State({'type': 'min-ask-sum', 'index': MATCH}, 'value'),
    State('loading-state', 'data'),
    prevent_initial_call=True,
)
def on_trade_entry_click(n_clicks, pair_json, volume, min_ask_sum, loading_state):
    """卖出进仓按钮：POST /entry 到策略 HTTP API"""
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

        # 检查加载时间
        load_time = (loading_state or {}).get(product)
        if load_time and time.time() - load_time < _LOAD_READY_SECONDS:
            remain = int(_LOAD_READY_SECONDS - (time.time() - load_time))
            return html.Span(f'{product}策略加载中，还需{remain}秒', style={'color': '#FFD700'})

        payload = {
            'call': pair['call'],
            'put': pair['put'],
            'volume': int(volume),
            'direction': 'sell',
        }
        # 条件进仓：Ask之和 >= min_ask_sum 时才执行
        if min_ask_sum and min_ask_sum > 0:
            payload['min_ask_sum'] = float(min_ask_sum)

        ok, msg = _send_strategy_command(product, 'entry', payload)
        if ok:
            rounds = _smart_round_volumes(int(volume))
            ts = datetime.now().strftime("%H:%M:%S")
            if min_ask_sum and min_ask_sum > 0:
                return html.Span(
                    f'条件挂单 Ask≥{min_ask_sum} 卖出各{volume}手 '
                    f'{pair["call"]}+{pair["put"]} ({ts})',
                    style={'color': '#FFD700'})
            return html.Span(
                f'已发送 卖出各{volume}手 分{len(rounds)}轮{rounds} '
                f'{pair["call"]}+{pair["put"]} ({ts})',
                style={'color': '#00FF88'})
        else:
            return html.Span(f'发送失败: {msg}', style={'color': '#FF4444'})
    except Exception as e:
        return html.Span(f'发送失败: {e}', style={'color': '#FF4444'})


@app.callback(
    Output({'type': 'trade-close-status', 'index': MATCH}, 'children'),
    Input({'type': 'trade-close-btn', 'index': MATCH}, 'n_clicks'),
    State({'type': 'trade-pair-select', 'index': MATCH}, 'value'),
    State({'type': 'close-volume', 'index': MATCH}, 'value'),
    State({'type': 'max-bid-sum', 'index': MATCH}, 'value'),
    prevent_initial_call=True,
)
def on_trade_close_click(n_clicks, pair_json, volume, max_bid_sum):
    """买入平仓按钮：POST /close 到策略 HTTP API"""
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
        if not _is_strategy_running(product):
            return html.Span(f'{product}策略未运行，无法平仓', style={'color': '#FF4444'})

        # 检查紧急停止状态（HTTP API 或 fallback 文件）
        status = _get_strategy_status(product)
        _es = (status.get('emergency_stopped') if status else
               os.path.exists(os.path.join(_STATE_DIR, '.emergency_stop')))
        if _es:
            return html.Span('系统已紧急停止', style={'color': '#FF4444'})

        payload = {
            'call': pair['call'],
            'put': pair['put'],
            'volume': int(volume),
            'direction': 'buy_close',
        }
        # 条件平仓：Bid之和 <= max_bid_sum 时才执行
        if max_bid_sum and max_bid_sum > 0:
            payload['max_bid_sum'] = float(max_bid_sum)

        ok, msg = _send_strategy_command(product, 'close', payload)
        if ok:
            rounds = _smart_round_volumes(int(volume))
            ts = datetime.now().strftime("%H:%M:%S")
            if max_bid_sum and max_bid_sum > 0:
                return html.Span(
                    f'条件平仓 Bid≤{max_bid_sum} 平仓各{volume}手 '
                    f'{pair["call"]}+{pair["put"]} ({ts})',
                    style={'color': '#FFD700'})
            return html.Span(
                f'已发送 平仓各{volume}手 分{len(rounds)}轮{rounds} '
                f'({datetime.now().strftime("%H:%M:%S")})',
                style={'color': '#FF6B6B'})
        else:
            return html.Span(f'发送失败: {msg}', style={'color': '#FF4444'})
    except Exception as e:
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
    Output('charts-container', 'children'),
    Output('account-bar', 'children'),
    Input('pairs-store', 'data'),
    Input('timer', 'n_intervals'),
)
def render_charts(pairs, _):
    manual_pairs = pairs or []
    account_bar = _build_account_bar()
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

    if not manual_pairs and not auto_pairs:
        return html.Div('暂无数据，等待数据采集...',
                         style={'color': '#666', 'padding': '50px', 'textAlign': 'center',
                                'fontSize': '16px'}), account_bar

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

    # ---- Gamma/Strangle 每日检查 (暂时禁用排查渲染问题) ----
    scorecard_panel, scorecard_nav_items = None, []

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
        for idx, pair, fig, info, cc, pc in normal_manual:
            fs = info.get('futures_sym') or '?'
            s = info.get('sum')
            nav.append(html.A([
                html.Div(fs, style={'fontWeight': 'bold', 'fontSize': '13px', 'color': '#FFD700'}),
                html.Div(f'{s:.1f}' if s else '--', style={'fontSize': '11px', 'color': '#aaa'}),
            ], href=f'#m-chart-{idx}', className='nav-item', style=nav_style))

    if normal_auto:
        nav.append(html.Div('智能推荐', style={
            'padding': '8px 12px', 'color': '#00FF88', 'fontSize': '11px',
            'fontWeight': 'bold', 'textTransform': 'uppercase', 'letterSpacing': '1px',
            'borderBottom': '1px solid #00FF88', 'marginTop': '6px'}))
        for idx, ap, fig, info in normal_auto:
            nav.append(html.A([
                html.Div(ap['product'], style={'fontWeight': 'bold', 'fontSize': '13px', 'color': '#00FF88'}),
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
        header_parts.extend(_make_advisory_spans(call_sym))
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
        header_parts.extend(_make_advisory_spans(ap['call']))
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
                charts.append(_manual_card(*item, is_alert=True))
            else:
                charts.append(_auto_card(*item, is_alert=True))

    # 2) 自选区（非警报）
    for idx, pair, fig, info, cc, pc in normal_manual:
        charts.append(_manual_card(idx, pair, fig, info, cc, pc))

    # 3) 智能推荐区（非警报）
    if normal_auto:
        charts.append(html.Div([
            html.Span('智能推荐', style={'color': '#00FF88', 'fontSize': '15px', 'fontWeight': 'bold'}),
            html.Span('  基于trade2026评分自动选对', style={'color': '#666', 'fontSize': '12px'}),
        ], style={'padding': '12px 20px', 'backgroundColor': '#0a1628',
                  'borderTop': '3px solid #00FF88', 'marginTop': '10px'}))

        for idx, ap, fig, info in normal_auto:
            charts.append(_auto_card(idx, ap, fig, info))

    charts_area = html.Div(charts, style={'flex': '1', 'minWidth': '0'})

    charts_layout = html.Div([sidebar, charts_area],
                              style={'display': 'flex', 'alignItems': 'flex-start', 'padding': '8px'})
    return charts_layout, account_bar


NEWS_CACHE = os.path.expanduser('~/Scripts/news_cache.md')
NEWS_FETCHER = os.path.expanduser('~/Scripts/news_auto_fetch.py')
NEWS_MAX_AGE = 3 * 3600  # 3小时过期


def _news_cache_age():
    """返回缓存文件的年龄（秒），不存在返回 inf"""
    if not os.path.exists(NEWS_CACHE):
        return float('inf')
    return time.time() - os.path.getmtime(NEWS_CACHE)


def _auto_refresh_news():
    """如果缓存超过3小时，后台自动刷新"""
    age = _news_cache_age()
    if age > NEWS_MAX_AGE:
        try:
            import subprocess
            subprocess.Popen(
                ['/usr/bin/python3', NEWS_FETCHER],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass
        return True  # 正在刷新
    return False


def _render_news_content():
    """读取并渲染新闻缓存内容"""
    if not os.path.exists(NEWS_CACHE):
        return '', '', None

    try:
        with open(NEWS_CACHE, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception:
        return '', '读取失败', None

    updated = ''
    if content.startswith('<!-- updated:'):
        first_line = content.split('\n', 1)[0]
        updated = first_line.replace('<!-- updated:', '').replace('-->', '').strip()
        content = content.split('\n', 1)[1] if '\n' in content else ''

    if not content.strip():
        return '', updated, None

    lines = []
    for line in content.split('\n'):
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
    """切换资讯面板显示/隐藏，缓存超过3小时自动刷新"""
    if not n_clicks or n_clicks % 2 == 0:
        return no_update, {'display': 'none'}

    # 检查是否需要刷新
    refreshing = _auto_refresh_news()

    content, updated, body = _render_news_content()

    if body is None:
        if refreshing:
            body = html.Div('正在自动抓取最新资讯，请稍后再点...',
                             style={'color': '#4fc3f7', 'padding': '30px', 'textAlign': 'center'})
        else:
            body = html.Div('暂无资讯缓存。请在 Claude Code 中运行 /news 生成资讯。',
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

    refresh_note = '  (已触发自动刷新)' if refreshing else ''

    panel = html.Div([
        html.Div([
            html.Span('每日资讯', style={'color': '#4fc3f7', 'fontSize': '15px', 'fontWeight': 'bold'}),
            html.Span(f'  {header_text}', style={'color': '#666', 'fontSize': '12px'}),
            html.Span(refresh_note, style={'color': '#00FF88', 'fontSize': '11px'}),
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
    """今日交易计划面板"""
    if not n_clicks or n_clicks % 2 == 0:
        return no_update, {'display': 'none'}

    plan_path = os.path.join(os.path.dirname(__file__), 'trading_plan_data.json')
    try:
        with open(plan_path) as f:
            plan_data = json.load(f)
    except Exception as e:
        return html.Div(f'加载失败: {e}', style={'color': '#f44', 'padding': '20px'}), {
            'display': 'block', 'backgroundColor': '#111827',
            'borderBottom': '3px solid #ff9800', 'marginBottom': '5px'}

    today_str = datetime.now().strftime('%Y-%m-%d')
    day_plan = plan_data.get('days', {}).get(today_str)

    if not day_plan:
        # 找最近的有数据的交易日
        from datetime import timedelta as _td
        for delta in range(1, 8):
            alt = (datetime.now() - _td(days=delta)).strftime('%Y-%m-%d')
            if alt in plan_data.get('days', {}):
                day_plan = plan_data['days'][alt]
                today_str = alt + ' (最近交易日)'
                break

    if not day_plan:
        return html.Div('今日无交易计划数据', style={'color': '#888', 'padding': '20px'}), {
            'display': 'block', 'backgroundColor': '#111827',
            'borderBottom': '3px solid #ff9800', 'marginBottom': '5px'}

    account = plan_data.get('account', 500000)
    picks = day_plan.get('picks', [])

    # 构建表格
    header = html.Tr([
        html.Th('', style={'width': '30px'}),
        html.Th('品种', style={'width': '80px'}),
        html.Th('合约', style={'width': '80px'}),
        html.Th('DTE', style={'width': '50px', 'textAlign': 'center'}),
        html.Th('DTE桶', style={'width': '60px', 'textAlign': 'center'}),
        html.Th('手数', style={'width': '50px', 'textAlign': 'center'}),
        html.Th('保证金', style={'width': '80px', 'textAlign': 'right'}),
        html.Th('占比', style={'width': '50px', 'textAlign': 'center'}),
        html.Th('桶Sharpe', style={'width': '70px', 'textAlign': 'center'}),
        html.Th('桶胜率', style={'width': '60px', 'textAlign': 'center'}),
        html.Th('桶PnL', style={'width': '60px', 'textAlign': 'center'}),
        html.Th('回测Sharpe', style={'width': '80px', 'textAlign': 'center'}),
        html.Th('瓶颈额(万)', style={'width': '80px', 'textAlign': 'right'}),
    ], style={'backgroundColor': '#1a1a3e', 'color': '#4fc3f7', 'fontSize': '12px'})

    rows = []
    for i, p in enumerate(picks):
        is_opt = p.get('is_optimal', False)
        opt_mark = '★' if is_opt else ''
        row_bg = '#1a2a1a' if is_opt else '#0d1117'
        dte = p.get('dte', 0)
        dte_color = '#f44336' if dte <= 5 else '#ff9800' if dte <= 10 else '#4caf50'

        b_sh = p.get('b_sharpe', 0)
        sh_color = '#00e676' if b_sh >= 1.0 else '#4fc3f7' if b_sh >= 0.5 else '#ff9800' if b_sh >= 0 else '#f44336'

        b_wr = p.get('b_wr', 0)
        wr_color = '#00e676' if b_wr >= 0.85 else '#4fc3f7' if b_wr >= 0.7 else '#ff9800'

        b_pnl = p.get('b_pnl', 0)
        pnl_color = '#00e676' if b_pnl > 0.1 else '#4fc3f7' if b_pnl > 0 else '#f44336'

        liq = p.get('liq_wan', 0)
        liq_color = '#00e676' if liq >= 1000 else '#ff9800' if liq >= 100 else '#f44336'

        rows.append(html.Tr([
            html.Td(opt_mark, style={'color': '#ff9800', 'fontWeight': 'bold', 'fontSize': '16px'}),
            html.Td(f"{p.get('ex','')}.{p.get('prod','')}", style={'color': '#fff', 'fontWeight': 'bold'}),
            html.Td(p.get('contract', ''), style={'color': '#aaa'}),
            html.Td(str(dte), style={'textAlign': 'center', 'color': dte_color, 'fontWeight': 'bold'}),
            html.Td(p.get('bucket', ''), style={'textAlign': 'center', 'color': '#aaa'}),
            html.Td(str(p.get('lots', 0)), style={'textAlign': 'center', 'color': '#fff', 'fontWeight': 'bold', 'fontSize': '15px'}),
            html.Td(f"{p.get('margin', 0):,.0f}", style={'textAlign': 'right', 'color': '#4fc3f7'}),
            html.Td(f"{p.get('pct', 0):.0%}", style={'textAlign': 'center', 'color': '#aaa'}),
            html.Td(f"{b_sh:.2f}", style={'textAlign': 'center', 'color': sh_color, 'fontWeight': 'bold'}),
            html.Td(f"{b_wr:.0%}", style={'textAlign': 'center', 'color': wr_color}),
            html.Td(f"{b_pnl:+.1%}", style={'textAlign': 'center', 'color': pnl_color}),
            html.Td(f"{p.get('bt_sharpe', 0):.3f}", style={'textAlign': 'center', 'color': '#aaa'}),
            html.Td(f"{liq:,.0f}", style={'textAlign': 'right', 'color': liq_color}),
        ], style={'backgroundColor': row_bg, 'borderBottom': '1px solid #1a1a3e'}))

    total_mgn = day_plan.get('total_margin', 0)
    n_opt = day_plan.get('n_optimal', 0)

    summary_row = html.Tr([
        html.Td(''),
        html.Td(f'{len(picks)}品种', style={'color': '#ff9800', 'fontWeight': 'bold'}),
        html.Td(f'{n_opt}个最优DTE', style={'color': '#ff9800'}),
        html.Td(''),
        html.Td(''),
        html.Td(''),
        html.Td(f'{total_mgn:,.0f}', style={'textAlign': 'right', 'color': '#ff9800', 'fontWeight': 'bold'}),
        html.Td(f'{day_plan.get("total_pct", 0):.0%}', style={'textAlign': 'center', 'color': '#ff9800'}),
        html.Td(''), html.Td(''), html.Td(''), html.Td(''), html.Td(''),
    ], style={'backgroundColor': '#2a1a0a', 'borderTop': '2px solid #ff9800'})

    table = html.Table(
        [html.Thead(header), html.Tbody(rows + [summary_row])],
        style={'width': '100%', 'borderCollapse': 'collapse', 'fontSize': '13px'})

    panel = html.Div([
        html.Div([
            html.Span('今日交易计划', style={'color': '#ff9800', 'fontSize': '15px', 'fontWeight': 'bold'}),
            html.Span(f'  {today_str}  账户{account/10000:.0f}万  ★=最优DTE窗口',
                       style={'color': '#666', 'fontSize': '12px'}),
        ], style={'padding': '10px 25px', 'borderBottom': '1px solid #2a2a4a'}),
        html.Div(table, style={'padding': '5px 20px', 'overflowX': 'auto'}),
        html.Div([
            html.Span('操作: 21:00夜盘开仓卖出宽跨 → 次日14:50平仓 | ', style={'color': '#666', 'fontSize': '11px'}),
            html.Span('★品种优先重仓, 瓶颈额<100万品种注意滑点', style={'color': '#ff9800', 'fontSize': '11px'}),
        ], style={'padding': '8px 25px'}),
    ])

    return panel, {
        'display': 'block', 'backgroundColor': '#111827',
        'borderBottom': '3px solid #ff9800', 'marginBottom': '5px',
    }


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
    app.run(host='0.0.0.0', port=PORT, debug=False)
