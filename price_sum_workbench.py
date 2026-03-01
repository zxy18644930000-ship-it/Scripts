#!/usr/bin/env python3
"""期权对价格之和工作台
一个页面管理多个期权对的走势图，支持动态添加/删除，配置自动持久化。
"""

import json
import sqlite3
import os
import re
from datetime import datetime, timedelta
from dash import Dash, dcc, html, ctx, no_update, ALL
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go

DB_PATH = os.path.expanduser('~/.vntrader/database.db')
CONFIG_PATH = os.path.expanduser('~/Scripts/price_sum_pairs.json')
PORT = 8052
REFRESH_MS = 60_000


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


# ============ 数据加载 ============

def get_trading_day_start():
    now = datetime.now()
    if now.hour < 5:
        d = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    elif now.hour < 21:
        if now.weekday() == 0:
            d = (now - timedelta(days=3)).strftime('%Y-%m-%d')
        else:
            d = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        d = now.strftime('%Y-%m-%d')
    return f'{d} 21:00:00'


def _extract_futures_symbol(option_sym):
    """从期权 symbol 提取期货 symbol: ag2604C37600 -> ag2604"""
    m = re.match(r'([a-zA-Z]+\d{3,4})[CP]\d+', option_sym)
    return m.group(1) if m else None


def load_pair_data(call_sym, put_sym):
    """加载一个期权对的数据（含期货价格）"""
    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()
    day_start = get_trading_day_start()

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

    db.close()

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


def build_figure(call_sym, put_sym):
    """为一个期权对构建图表（双Y轴：左=期权价格，右=期货价格）"""
    times, call_prices, put_prices, sum_prices, fut_prices, futures_sym = \
        load_pair_data(call_sym, put_sym)

    fig = go.Figure()

    # 左Y轴: 期权价格
    fig.add_trace(go.Scatter(
        x=times, y=sum_prices,
        name='价格之和',
        line=dict(color='#FFD700', width=3),
        yaxis='y',
        hovertemplate='价格之和: %{y:.1f}<extra></extra>'
    ))

    fig.add_trace(go.Scatter(
        x=times, y=call_prices,
        name=call_sym,
        line=dict(color='#00BFFF', width=1.5, dash='dot'),
        yaxis='y',
        hovertemplate=call_sym + ': %{y:.1f}<extra></extra>'
    ))

    fig.add_trace(go.Scatter(
        x=times, y=put_prices,
        name=put_sym,
        line=dict(color='#FF6B6B', width=1.5, dash='dot'),
        yaxis='y',
        hovertemplate=put_sym + ': %{y:.1f}<extra></extra>'
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

    x_range = None
    if times:
        try:
            last_t = datetime.strptime(times[-1], '%Y-%m-%d %H:%M:%S')
            x_range = [times[0], (last_t + timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')]
        except Exception:
            pass

    layout_kwargs = dict(
        template='plotly_dark',
        paper_bgcolor='#1a1a2e',
        plot_bgcolor='#16213e',
        height=400,
        hovermode='x',
        margin=dict(l=60, r=70, t=40, b=60),
        xaxis=dict(
            gridcolor='#2a2a4a',
            range=x_range,
            showspikes=True, spikemode='across', spikesnap='cursor',
            spikethickness=1, spikecolor='#888', spikedash='solid',
        ),
        yaxis=dict(
            title='期权价格',
            title_font=dict(color='#FFD700'),
            tickfont=dict(color='#ddd'),
            gridcolor='#2a2a4a',
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

    return fig


# ============ 解析输入 ============

def parse_pair_input(text):
    """解析用户输入的期权对，返回 (call_sym, put_sym) 或 None"""
    text = text.strip().upper()
    if not text:
        return None

    # 匹配完整合约代码: ag2604C37600
    full_pattern = r'([A-Z]{1,4}\d{3,4}[CP]\d+)'
    matches = re.findall(full_pattern, text)
    if len(matches) >= 2:
        call_sym = put_sym = None
        for m in matches:
            if 'C' in m.split(re.search(r'\d', m).group())[0] or re.search(r'\d+C\d+', m):
                # 更精确: 找 ...C数字 的模式
                pass
            cm = re.search(r'(\w+\d{3,4})C(\d+)', m)
            pm = re.search(r'(\w+\d{3,4})P(\d+)', m)
            if cm:
                call_sym = m.lower() if m[:2] not in ('SA', 'FG', 'TA', 'MA', 'OI', 'RM', 'AP',
                    'CF', 'CJ', 'SR', 'PK', 'SM', 'SF', 'SH', 'UR', 'PF') else m
            elif pm:
                put_sym = m.lower() if m[:2] not in ('SA', 'FG', 'TA', 'MA', 'OI', 'RM', 'AP',
                    'CF', 'CJ', 'SR', 'PK', 'SM', 'SF', 'SH', 'UR', 'PF') else m
        if call_sym and put_sym:
            return (call_sym, put_sym)

    # 简写模式: C37600+P17000 或 C37600 P17000 (默认ag2604)
    simple = re.findall(r'([CP])(\d+)', text)
    if len(simple) >= 2:
        call_strike = put_strike = None
        for side, strike in simple:
            if side == 'C':
                call_strike = strike
            elif side == 'P':
                put_strike = strike
        if call_strike and put_strike:
            return (f'ag2604C{call_strike}', f'ag2604P{put_strike}')

    return None


# ============ Dash 应用 ============

app = Dash(__name__, suppress_callback_exceptions=True)

app.layout = html.Div([
    # 顶部标题栏
    html.Div([
        html.H2('期权价格之和工作台', style={'margin': '0', 'color': '#fff', 'display': 'inline-block'}),
        html.Span('  动态添加/删除期权对，实时走势监控',
                   style={'color': '#aaa', 'fontSize': '14px', 'marginLeft': '15px'}),
    ], style={'backgroundColor': '#1a1a2e', 'padding': '15px 25px',
              'borderBottom': '3px solid #e94560'}),

    # 添加期权对的输入区
    html.Div([
        dcc.Input(
            id='pair-input',
            type='text',
            placeholder='输入期权对，如: C37600+P15000 或 ag2604C37600+ag2604P15000',
            style={
                'width': '450px', 'padding': '8px 12px', 'fontSize': '14px',
                'backgroundColor': '#1a1a3e', 'color': '#fff', 'border': '1px solid #444',
                'borderRadius': '4px', 'marginRight': '10px'
            },
            debounce=True,
        ),
        html.Button('添加', id='add-btn', n_clicks=0, style={
            'padding': '8px 20px', 'fontSize': '14px', 'cursor': 'pointer',
            'backgroundColor': '#e94560', 'color': '#fff', 'border': 'none',
            'borderRadius': '4px', 'marginRight': '10px'
        }),
        html.Span(id='add-msg', style={'color': '#aaa', 'fontSize': '13px'}),
    ], style={'padding': '15px 25px', 'backgroundColor': '#16213e'}),

    # 图表容器
    html.Div(id='charts-container'),

    # 持久化存储
    dcc.Store(id='pairs-store', data=load_config()),

    # 定时刷新
    dcc.Interval(id='timer', interval=REFRESH_MS, n_intervals=0),

], style={'backgroundColor': '#0f0f23', 'minHeight': '100vh'})


@app.callback(
    Output('pairs-store', 'data'),
    Output('add-msg', 'children'),
    Output('pair-input', 'value'),
    Input('add-btn', 'n_clicks'),
    Input({'type': 'del-btn', 'index': ALL}, 'n_clicks'),
    State('pair-input', 'value'),
    State('pairs-store', 'data'),
    prevent_initial_call=True,
)
def modify_pairs(add_clicks, del_clicks, input_val, pairs):
    triggered = ctx.triggered_id

    # 删除按钮（必须确认是真正点击，而非定时器重建按钮导致的误触发）
    if isinstance(triggered, dict) and triggered.get('type') == 'del-btn':
        idx = triggered['index']
        # 检查该按钮的 n_clicks 确实 > 0
        if del_clicks and idx < len(del_clicks) and del_clicks[idx] and del_clicks[idx] > 0:
            if 0 <= idx < len(pairs):
                removed = pairs.pop(idx)
                save_config(pairs)
                return pairs, f'已删除 {removed[0]}+{removed[1]}', no_update

    # 添加按钮
    if triggered == 'add-btn':
        if not input_val:
            return no_update, '请输入期权对', ''
        result = parse_pair_input(input_val)
        if not result:
            return no_update, '格式不对，示例: C37600+P15000', no_update
        call_sym, put_sym = result
        # 检查重复
        for c, p in pairs:
            if c == call_sym and p == put_sym:
                return no_update, f'{call_sym}+{put_sym} 已存在', ''
        pairs.append([call_sym, put_sym])
        save_config(pairs)
        return pairs, f'已添加 {call_sym}+{put_sym}', ''

    return no_update, '', no_update


@app.callback(
    Output('charts-container', 'children'),
    Input('pairs-store', 'data'),
    Input('timer', 'n_intervals'),
)
def render_charts(pairs, _):
    if not pairs:
        return html.Div('暂无期权对，请在上方输入框添加',
                         style={'color': '#666', 'padding': '50px', 'textAlign': 'center',
                                'fontSize': '16px'})

    charts = []
    for i, (call_sym, put_sym) in enumerate(pairs):
        fig = build_figure(call_sym, put_sym)

        chart_div = html.Div([
            # 每个图表的标题栏
            html.Div([
                html.Span(f'{call_sym} + {put_sym}',
                           style={'color': '#FFD700', 'fontSize': '15px', 'fontWeight': 'bold'}),
                html.Button('✕', id={'type': 'del-btn', 'index': i}, n_clicks=0, style={
                    'float': 'right', 'backgroundColor': 'transparent', 'color': '#e94560',
                    'border': '1px solid #e94560', 'borderRadius': '3px', 'cursor': 'pointer',
                    'padding': '2px 8px', 'fontSize': '12px'
                }),
            ], style={'padding': '10px 20px', 'backgroundColor': '#1a1a2e'}),

            dcc.Graph(figure=fig, config={'displayModeBar': False}),

        ], style={'marginBottom': '10px', 'borderBottom': '2px solid #2a2a4a'})

        charts.append(chart_div)

    return charts


if __name__ == '__main__':
    pairs = load_config()
    print(f'价格之和工作台: http://localhost:{PORT}')
    print(f'已加载 {len(pairs)} 个期权对')
    for c, p in pairs:
        print(f'  {c} + {p}')
    app.run(host='0.0.0.0', port=PORT, debug=False)
