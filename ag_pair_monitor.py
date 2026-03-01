#!/usr/bin/env python3
"""
白银(AG)期权对监控器 - 价格之和底部拐点检测

信号条件（必须同时满足）：
  1. 高低腿交叉：原来的高价腿跌到低于原来的低价腿
  2. 价格之和触底：sum不再创新低（连续2次某腿下跌但sum未破底）

用法: python3 ag_pair_monitor.py [--product ag2604] [--port 8051]
浏览器打开: http://localhost:8051
"""

import sqlite3
import os
import re
import argparse
from datetime import datetime, date, timedelta

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ============ 配置 ============
DB_PATH = os.path.expanduser('~/.vntrader/database.db')
DEFAULT_PRODUCT = 'ag2604'
DEFAULT_PORT = 8051
REFRESH_MS = 60_000          # 60秒刷新
MIN_TURNOVER = 5_000_000     # 每腿最低成交额500万
MAX_RATIO = 2.0              # 高价腿/低价腿 ≤ 2
MIN_OTM_PCT = 4.0            # 最低虚值度4%
SIGNAL_COUNT = 2             # 连续N次确认
MAX_PAIRS = 30               # 最多监控对数


# ============ 数据层 ============

class PairState:
    """单个期权对的状态追踪"""

    def __init__(self, call_sym, put_sym, call_strike, put_strike,
                 init_call_px, init_put_px):
        self.call_sym = call_sym
        self.put_sym = put_sym
        self.call_strike = call_strike
        self.put_strike = put_strike
        self.history = []
        self.sum_min = None
        self.consecutive = 0
        self.signal_fired = False
        self.signal_time = None
        # 交叉检测: 记录初始高价腿
        self.initial_high = 'call' if init_call_px >= init_put_px else 'put'
        self.crossed = False

    @property
    def short_id(self):
        cm = re.search(r'C(\d+)', self.call_sym)
        pm = re.search(r'P(\d+)', self.put_sym)
        return "C%s+P%s" % (cm.group(1) if cm else '?', pm.group(1) if pm else '?')

    def update(self, time_str, call_px, put_px):
        """更新价格，返回是否新触发信号"""
        s = call_px + put_px

        if not self.history:
            self.sum_min = s
            self.history.append((time_str, call_px, put_px, s))
            return False

        _, prev_c, prev_p, _ = self.history[-1]

        # 条件1: 交叉检测
        if self.initial_high == 'call':
            self.crossed = call_px < put_px
        else:
            self.crossed = put_px < call_px

        # 条件2: sum底部检测
        any_leg_declined = (call_px < prev_c) or (put_px < prev_p)

        if s < self.sum_min:
            self.sum_min = s
            self.consecutive = 0
        elif any_leg_declined:
            self.consecutive += 1

        self.history.append((time_str, call_px, put_px, s))
        if len(self.history) > 2000:
            self.history = self.history[-2000:]

        # 信号: 两个条件同时满足
        if (self.crossed
                and self.consecutive >= SIGNAL_COUNT
                and not self.signal_fired):
            self.signal_fired = True
            self.signal_time = time_str
            return True

        return False


def get_trading_day_start():
    """获取当前交易日起始（昨晚21:00夜盘开始，含夜盘+早盘+午盘）"""
    now = datetime.now()
    if now.hour < 5:
        # 凌晨（夜盘中），从昨天21:00开始
        d = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        return '%s 21:00:00' % d
    elif now.hour < 21:
        # 白天盘 或 收盘后等待夜盘（15:00~21:00），查今天交易日数据
        if now.weekday() == 0:
            d = (now - timedelta(days=3)).strftime('%Y-%m-%d')
        else:
            d = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        return '%s 21:00:00' % d
    else:
        # 21:00后夜盘开始，新交易日
        return '%s 21:00:00' % now.strftime('%Y-%m-%d')


def get_futures_price(product):
    """获取期货最新价"""
    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()
    cur.execute(
        "SELECT close_price FROM dbbardata WHERE symbol=? ORDER BY datetime DESC LIMIT 1",
        (product,))
    row = cur.fetchone()
    db.close()
    return row[0] if row else None


def get_latest_price(cur, symbol):
    """获取期权最新价"""
    cur.execute(
        "SELECT close_price FROM dbbardata WHERE symbol=? ORDER BY datetime DESC LIMIT 1",
        (symbol,))
    row = cur.fetchone()
    return row[0] if row else None


def get_recent_data_start():
    """获取最近3个交易日的起始时间（用于成交额筛选）
    覆盖更广的时间窗口，确保前几天有成交但今天暂无成交的合约也能被纳入
    """
    now = datetime.now()
    # 往前推5个日历日，大约覆盖3个交易日
    start = (now - timedelta(days=5)).strftime('%Y-%m-%d')
    return '%s 21:00:00' % start


def select_pairs(product, fut_price):
    """自动筛选满足条件的期权对"""
    # 用当天成交额判断流动性（当天=当前交易日，含夜盘）
    day_start = get_trading_day_start()

    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()

    # 获取有成交额的虚值期权（仅按成交额过滤）
    cur.execute("""
        SELECT symbol, SUM(volume) as vol, SUM(turnover) as to_val,
          (SELECT close_price FROM dbbardata b WHERE b.symbol=a.symbol
           ORDER BY b.datetime DESC LIMIT 1) as last_px
        FROM dbbardata a
        WHERE datetime >= ? AND (symbol LIKE ? OR symbol LIKE ?)
        GROUP BY symbol HAVING to_val >= ?
    """, (day_start, product + 'C%', product + 'P%', MIN_TURNOVER))

    calls, puts = [], []
    for sym, vol, to_val, px in cur.fetchall():
        if not px or px <= 0:
            continue
        cm = re.search(r'C(\d+)', sym)
        pm = re.search(r'P(\d+)', sym)
        if cm:
            strike = int(cm.group(1))
            otm = (strike - fut_price) / fut_price * 100
            if otm >= MIN_OTM_PCT:
                calls.append({
                    'sym': sym, 'strike': strike, 'px': px,
                    'vol': vol or 0, 'turnover': to_val or 0, 'otm': otm
                })
        elif pm:
            strike = int(pm.group(1))
            otm = (fut_price - strike) / fut_price * 100
            if otm >= MIN_OTM_PCT:
                puts.append({
                    'sym': sym, 'strike': strike, 'px': px,
                    'vol': vol or 0, 'turnover': to_val or 0, 'otm': otm
                })

    db.close()

    if not calls or not puts:
        return []

    # 按成交额排序
    calls.sort(key=lambda x: x['turnover'], reverse=True)
    puts.sort(key=lambda x: x['turnover'], reverse=True)

    # 核心原则：成交额大的期权优先配对，每个行权价只出现一次
    # 把所有Call和Put放一起，按成交额从大到小排队，逐个找搭档
    all_options = [('call', c) for c in calls] + [('put', p) for p in puts]
    all_options.sort(key=lambda x: x[1]['turnover'], reverse=True)

    used_calls = set()
    used_puts = set()
    candidates = []

    for side, opt in all_options:
        strike = opt['strike']
        if side == 'call' and strike in used_calls:
            continue
        if side == 'put' and strike in used_puts:
            continue

        # 从对面找最佳搭档：成交额最大且ratio<=MAX_RATIO的
        partners = puts if side == 'call' else calls
        best = None
        best_to = -1
        for partner in partners:
            p_strike = partner['strike']
            if side == 'call' and p_strike in used_puts:
                continue
            if side == 'put' and p_strike in used_calls:
                continue
            hi = max(opt['px'], partner['px'])
            lo = min(opt['px'], partner['px'])
            if lo <= 0:
                continue
            ratio = hi / lo
            if ratio > MAX_RATIO:
                continue
            if partner['turnover'] > best_to:
                best = partner
                best_to = partner['turnover']

        if not best:
            continue

        c = opt if side == 'call' else best
        p = best if side == 'call' else opt
        ratio = max(c['px'], p['px']) / min(c['px'], p['px'])
        candidates.append({
            'call': c, 'put': p,
            'ratio': ratio, 'balance': abs(ratio - 1.0),
            'sum_px': c['px'] + p['px'],
            'total_to': c['turnover'] + p['turnover'],
            'min_to': min(c['turnover'], p['turnover']),
        })
        used_calls.add(c['strike'])
        used_puts.add(p['strike'])
        if len(candidates) >= MAX_PAIRS:
            break

    return candidates


# ============ 全局状态 ============
monitors = []       # List[PairState]
product = DEFAULT_PRODUCT
fut_price_global = 0
alerts = []         # 历史信号记录


def load_history(call_sym, put_sym):
    """从数据库加载当天交易日的K线，按分钟对齐，去除价格不变的重复点"""
    data_start = get_trading_day_start()
    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()

    # 分别查两腿的1分钟K线
    call_data = {}
    cur.execute(
        "SELECT datetime, close_price FROM dbbardata "
        "WHERE symbol=? AND datetime>=? ORDER BY datetime",
        (call_sym, data_start))
    for dt, px in cur.fetchall():
        if px and px > 0:
            call_data[dt] = px

    put_data = {}
    cur.execute(
        "SELECT datetime, close_price FROM dbbardata "
        "WHERE symbol=? AND datetime>=? ORDER BY datetime",
        (put_sym, data_start))
    for dt, px in cur.fetchall():
        if px and px > 0:
            put_data[dt] = px

    db.close()

    # 合并所有时间点（不要求两腿同时有数据，用上一个已知价格填充）
    all_times = sorted(set(call_data.keys()) | set(put_data.keys()))
    if not all_times:
        return []

    result = []
    last_c, last_p = None, None
    prev_c, prev_p = None, None

    for dt in all_times:
        c_px = call_data.get(dt, last_c)
        p_px = put_data.get(dt, last_p)
        if c_px is not None:
            last_c = c_px
        if p_px is not None:
            last_p = p_px

        # 两腿都有价格才记录
        if last_c is None or last_p is None:
            continue

        # 去重：跳过两腿价格都与上一条相同的点
        if last_c == prev_c and last_p == prev_p:
            continue

        try:
            t = datetime.strptime(str(dt)[:19], '%Y-%m-%d %H:%M:%S')
            time_str = t.strftime('%m-%d %H:%M')
        except Exception:
            time_str = str(dt)[-8:-3]

        result.append((time_str, last_c, last_p))
        prev_c, prev_p = last_c, last_p

    return result


def init_monitors(prod):
    """初始化监控列表"""
    global monitors, product, fut_price_global, alerts
    product = prod
    fut_price_global = get_futures_price(product)
    if not fut_price_global:
        return False

    pairs = select_pairs(product, fut_price_global)
    monitors = []
    alerts = []
    for info in pairs:
        c, p = info['call'], info['put']
        m = PairState(c['sym'], p['sym'], c['strike'], p['strike'],
                      c['px'], p['px'])

        # 从数据库加载历史数据
        hist = load_history(c['sym'], p['sym'])
        for time_str, c_px, p_px in hist:
            m.update(time_str, c_px, p_px)

        # 如果没有历史数据，用当前价格作为起点
        if not m.history:
            now_str = datetime.now().strftime('%m-%d %H:%M')
            m.update(now_str, c['px'], p['px'])

        monitors.append(m)
    return len(monitors) > 0


def update_all():
    """更新所有期权对的价格"""
    global fut_price_global
    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()

    # 更新期货价格
    cur.execute(
        "SELECT close_price FROM dbbardata WHERE symbol=? ORDER BY datetime DESC LIMIT 1",
        (product,))
    row = cur.fetchone()
    if row:
        fut_price_global = row[0]

    now_str = datetime.now().strftime('%m-%d %H:%M')
    new_signals = []

    for m in monitors:
        c_px = get_latest_price(cur, m.call_sym)
        p_px = get_latest_price(cur, m.put_sym)
        if c_px is None or p_px is None:
            continue
        is_new = m.update(now_str, c_px, p_px)
        if is_new:
            new_signals.append(m)
            alerts.append({
                'time': now_str,
                'pair': m.short_id,
                'call_sym': m.call_sym,
                'put_sym': m.put_sym,
                'call_px': c_px,
                'put_px': p_px,
                'sum': c_px + p_px,
                'sum_min': m.sum_min,
            })

    db.close()
    return new_signals


# ============ Dash应用 ============

app = dash.Dash(__name__)

app.layout = html.Div([
    # 标题
    html.Div([
        html.H2("白银期权对监控器", style={'margin': '0', 'color': '#fff'}),
        html.Span("价格之和底部拐点 → 买入宽跨信号",
                   style={'color': '#aaa', 'fontSize': '14px'}),
    ], style={
        'backgroundColor': '#1a1a2e', 'padding': '15px 25px',
        'borderBottom': '3px solid #e94560'
    }),

    # 信息栏
    html.Div(id='info-bar', style={
        'padding': '10px 25px', 'backgroundColor': '#16213e',
        'color': '#eee', 'fontSize': '14px'
    }),

    # 信号告警区
    html.Div(id='signal-alert', style={'padding': '0 25px'}),

    # 主表格
    html.Div([
        dash_table.DataTable(
            id='pair-table',
            columns=[
                {'name': '期权对', 'id': 'pair'},
                {'name': 'Call价格', 'id': 'call_px', 'type': 'numeric',
                 'format': {'specifier': '.1f'}},
                {'name': 'Put价格', 'id': 'put_px', 'type': 'numeric',
                 'format': {'specifier': '.1f'}},
                {'name': '价格之和', 'id': 'sum_px', 'type': 'numeric',
                 'format': {'specifier': '.1f'}},
                {'name': '底部', 'id': 'sum_min', 'type': 'numeric',
                 'format': {'specifier': '.1f'}},
                {'name': '变化', 'id': 'arrow'},
                {'name': '交叉', 'id': 'crossed'},
                {'name': 'Hold', 'id': 'hold', 'type': 'numeric'},
                {'name': '状态', 'id': 'status'},
            ],
            style_table={'overflowX': 'auto'},
            style_header={
                'backgroundColor': '#1a1a2e', 'color': '#fff',
                'fontWeight': 'bold', 'textAlign': 'center',
            },
            style_cell={
                'textAlign': 'center', 'padding': '8px 12px',
                'backgroundColor': '#0f3460', 'color': '#eee',
                'border': '1px solid #1a1a2e', 'fontSize': '14px',
            },
            style_data_conditional=[
                {
                    'if': {'filter_query': '{status} = "★信号!"'},
                    'backgroundColor': '#e94560', 'color': '#fff',
                    'fontWeight': 'bold',
                },
                {
                    'if': {'filter_query': '{crossed} = "✕已交叉"'},
                    'backgroundColor': '#533483', 'color': '#fff',
                },
                {
                    'if': {'filter_query': '{hold} > 0'},
                    'backgroundColor': '#1a4a5e',
                },
            ],
        ),
    ], style={'padding': '15px 25px'}),

    # 图表区（点击行查看走势）
    html.Div([
        html.H4("价格走势图（点击上表某行查看）",
                 style={'color': '#aaa', 'margin': '10px 0'}),
        dcc.Graph(id='pair-chart', style={'height': '400px'}),
    ], style={'padding': '0 25px'}),

    # 历史信号记录
    html.Div(id='alert-history', style={'padding': '15px 25px'}),

    # 定时器
    dcc.Interval(id='timer', interval=REFRESH_MS, n_intervals=0),
    dcc.Store(id='selected-pair-idx', data=0),

], style={'backgroundColor': '#0a0a23', 'minHeight': '100vh', 'fontFamily': 'monospace'})


@app.callback(
    [Output('info-bar', 'children'),
     Output('pair-table', 'data'),
     Output('signal-alert', 'children'),
     Output('alert-history', 'children')],
    [Input('timer', 'n_intervals')]
)
def refresh(n):
    # 更新数据
    new_signals = update_all()

    # 信息栏
    info = html.Div([
        html.Span("期货 %s = %.0f" % (product, fut_price_global),
                   style={'marginRight': '30px'}),
        html.Span("监控 %d 对" % len(monitors), style={'marginRight': '30px'}),
        html.Span("刷新: %s" % datetime.now().strftime('%H:%M:%S'),
                   style={'marginRight': '30px'}),
        html.Span("信号: %d" % len(alerts),
                   style={'color': '#e94560' if alerts else '#aaa'}),
    ])

    # 表格数据
    rows = []
    for i, m in enumerate(monitors):
        if not m.history:
            continue
        _, c, p, s = m.history[-1]
        arrow = ''
        if len(m.history) >= 2:
            prev_s = m.history[-2][3]
            if s > prev_s:
                arrow = '↑'
            elif s < prev_s:
                arrow = '↓'
            else:
                arrow = '→'

        status = ''
        if m.signal_fired:
            status = '★信号!'
        elif m.crossed and m.consecutive > 0:
            status = '接近...'
        crossed_str = '✕已交叉' if m.crossed else ''

        rows.append({
            'pair': m.short_id,
            'call_px': c,
            'put_px': p,
            'sum_px': s,
            'sum_min': m.sum_min,
            'arrow': arrow,
            'crossed': crossed_str,
            'hold': m.consecutive,
            'status': status,
        })

    # 信号告警
    alert_div = None
    if new_signals:
        alert_items = []
        for m in new_signals:
            _, c, p, s = m.history[-1]
            hi_label = 'Call→低' if m.initial_high == 'call' else 'Put→低'
            alert_items.append(html.Div([
                html.Div("★ 买入信号! %s" % m.short_id,
                         style={'fontSize': '20px', 'fontWeight': 'bold'}),
                html.Div("Call=%s @ %.1f | Put=%s @ %.1f" % (
                    m.call_sym, c, m.put_sym, p)),
                html.Div("价格之和=%.1f (底部=%.1f) | 高低腿已交叉(%s)" % (
                    s, m.sum_min, hi_label)),
                html.Div("建议: 买入这对期权(做多波动率)",
                         style={'marginTop': '5px', 'color': '#ffd700'}),
            ], style={
                'backgroundColor': '#e94560', 'color': '#fff',
                'padding': '15px', 'borderRadius': '8px',
                'margin': '10px 0', 'fontSize': '14px',
            }))
        alert_div = html.Div(alert_items)

    # 历史信号
    hist_div = None
    if alerts:
        hist_items = [html.H4("历史信号记录", style={'color': '#aaa'})]
        for a in reversed(alerts):
            hist_items.append(html.Div(
                "[%s] %s  C=%.1f P=%.1f Sum=%.1f (底部=%.1f)" % (
                    a['time'], a['pair'], a['call_px'], a['put_px'],
                    a['sum'], a['sum_min']),
                style={'color': '#e94560', 'fontSize': '13px', 'padding': '3px 0'}
            ))
        hist_div = html.Div(hist_items)

    return info, rows, alert_div, hist_div


@app.callback(
    Output('pair-chart', 'figure'),
    [Input('pair-table', 'active_cell'),
     Input('timer', 'n_intervals')],
    [State('pair-table', 'data')]
)
def update_chart(active_cell, n, table_data):
    idx = 0
    if active_cell and active_cell.get('row') is not None:
        idx = active_cell['row']

    if idx >= len(monitors) or not monitors[idx].history:
        return go.Figure()

    m = monitors[idx]
    times = [h[0] for h in m.history]
    call_pxs = [h[1] for h in m.history]
    put_pxs = [h[2] for h in m.history]
    sums = [h[3] for h in m.history]

    fig = make_subplots(specs=[[{"secondary_y": False}]])

    fig.add_trace(go.Scatter(
        x=times, y=sums, name='价格之和',
        line=dict(color='#e94560', width=3),
        hovertemplate='价格之和: %{y:.1f}<extra></extra>',
    ))
    fig.add_trace(go.Scatter(
        x=times, y=call_pxs, name='Call价格',
        line=dict(color='#00d2ff', width=1.5, dash='dot'),
        hovertemplate='Call: %{y:.1f}<extra></extra>',
    ))
    fig.add_trace(go.Scatter(
        x=times, y=put_pxs, name='Put价格',
        line=dict(color='#ffd700', width=1.5, dash='dot'),
        hovertemplate='Put: %{y:.1f}<extra></extra>',
    ))

    if m.sum_min:
        fig.add_hline(y=m.sum_min, line_dash="dash", line_color="#e94560",
                      annotation_text="底部=%.1f" % m.sum_min)

    if m.signal_fired and m.signal_time:
        sig_idx = None
        for i, t in enumerate(times):
            if t == m.signal_time:
                sig_idx = i
                break
        if sig_idx is not None:
            fig.add_trace(go.Scatter(
                x=[times[sig_idx]], y=[sums[sig_idx]],
                mode='markers+text', name='信号',
                marker=dict(size=15, color='#e94560', symbol='star'),
                text=['★信号'], textposition='top center',
                textfont=dict(color='#e94560', size=14),
            ))

    # X轴右边留5分钟空白
    x_range = None
    if times:
        try:
            from datetime import datetime as _dt, timedelta as _td
            last_t = _dt.strptime(times[-1], '%Y-%m-%d %H:%M:%S')
            x_range = [times[0], (last_t + _td(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')]
        except Exception:
            pass

    fig.update_layout(
        title="%s 价格走势" % m.short_id,
        template='plotly_dark',
        paper_bgcolor='#0a0a23',
        plot_bgcolor='#0f3460',
        height=400,
        hovermode='x',
        margin=dict(l=50, r=30, t=50, b=30),
        legend=dict(orientation='h', y=-0.15, x=0.5, xanchor='center'),
        xaxis=dict(
            range=x_range,
            showspikes=True, spikemode='across', spikesnap='cursor',
            spikethickness=1, spikecolor='#888', spikedash='solid',
        ),
        yaxis=dict(
            showspikes=True, spikemode='across',
            spikethickness=1, spikecolor='#555', spikedash='dot',
        ),
    )

    return fig


def main():
    parser = argparse.ArgumentParser(description='白银期权对监控器')
    parser.add_argument('--product', default=DEFAULT_PRODUCT,
                        help='期货合约代码 (默认: ag2604)')
    parser.add_argument('--port', type=int, default=DEFAULT_PORT,
                        help='Web端口 (默认: 8051)')
    args = parser.parse_args()

    print("正在初始化...")
    ok = init_monitors(args.product)
    if not ok:
        print("[ERROR] 初始化失败，确保CTP数据收集器在运行且 %s 有数据" % args.product)
        return

    print("期货 %s = %.0f" % (args.product, fut_price_global))
    print("筛选到 %d 个期权对:" % len(monitors))
    for m in monitors:
        c_px = m.history[0][1] if m.history else 0
        p_px = m.history[0][2] if m.history else 0
        print("  %s  C=%.1f  P=%.1f  Sum=%.1f  初始高价腿=%s" % (
            m.short_id, c_px, p_px, c_px + p_px,
            'Call' if m.initial_high == 'call' else 'Put'))

    print("\n启动Web监控: http://localhost:%d" % args.port)
    app.run(host='0.0.0.0', port=args.port, debug=False)


if __name__ == '__main__':
    main()
