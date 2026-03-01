#!/usr/bin/env python3
"""期权对价格之和走势图
用法: python3 ag_pair_chart.py <CALL合约> <PUT合约> [端口]
示例: python3 ag_pair_chart.py ag2604C37600 ag2604P15000 8053
"""

import sys
import sqlite3
import os
from datetime import datetime, timedelta
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go

DB_PATH = os.path.expanduser('~/.vntrader/database.db')

# 从命令行参数读取，有默认值兜底
CALL_SYM = sys.argv[1] if len(sys.argv) > 1 else 'ag2604C37600'
PUT_SYM = sys.argv[2] if len(sys.argv) > 2 else 'ag2604P17000'
PORT = int(sys.argv[3]) if len(sys.argv) > 3 else 8052


def load_data():
    """从数据库加载当天交易日数据"""
    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()

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
    day_start = f'{d} 21:00:00'

    call_data = {}
    put_data = {}
    for sym, store in [(CALL_SYM, call_data), (PUT_SYM, put_data)]:
        cur.execute("""
            SELECT datetime, close_price FROM dbbardata
            WHERE symbol=? AND datetime>=? ORDER BY datetime
        """, (sym, day_start))
        for dt_str, px in cur.fetchall():
            store[dt_str] = px

    db.close()

    all_times = sorted(set(call_data.keys()) | set(put_data.keys()))
    times, call_prices, put_prices, sum_prices = [], [], [], []
    last_c, last_p = None, None

    for t in all_times:
        c = call_data.get(t, last_c)
        p = put_data.get(t, last_p)
        if c is not None:
            last_c = c
        if p is not None:
            last_p = p
        if last_c is not None and last_p is not None:
            times.append(t)
            call_prices.append(last_c)
            put_prices.append(last_p)
            sum_prices.append(last_c + last_p)

    return times, call_prices, put_prices, sum_prices


def build_figure():
    times, call_prices, put_prices, sum_prices = load_data()

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=times, y=sum_prices,
        name='价格之和',
        line=dict(color='#FFD700', width=3),
        hovertemplate='价格之和: %{y:.1f}<extra></extra>'
    ))

    fig.add_trace(go.Scatter(
        x=times, y=call_prices,
        name=CALL_SYM,
        line=dict(color='#00BFFF', width=1.5, dash='dot'),
        hovertemplate=CALL_SYM + ': %{y:.1f}<extra></extra>'
    ))

    fig.add_trace(go.Scatter(
        x=times, y=put_prices,
        name=PUT_SYM,
        line=dict(color='#FF6B6B', width=1.5, dash='dot'),
        hovertemplate=PUT_SYM + ': %{y:.1f}<extra></extra>'
    ))

    # X轴右边留5分钟空白，避免最新数据贴边
    x_range = None
    if times:
        try:
            last_t = datetime.strptime(times[-1], '%Y-%m-%d %H:%M:%S')
            x_range = [times[0], (last_t + timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')]
        except Exception:
            pass

    fig.update_layout(
        title=dict(
            text=f'{CALL_SYM} + {PUT_SYM} 价格走势',
            font=dict(size=18, color='#fff'),
            x=0.5
        ),
        template='plotly_dark',
        paper_bgcolor='#1a1a2e',
        plot_bgcolor='#16213e',
        height=600,
        hovermode='x',
        xaxis=dict(
            title='时间', gridcolor='#2a2a4a',
            range=x_range,
            showspikes=True,
            spikemode='across',
            spikesnap='cursor',
            spikethickness=1,
            spikecolor='#888',
            spikedash='solid',
        ),
        yaxis=dict(
            title='价格',
            tickfont=dict(color='#ddd'),
            gridcolor='#2a2a4a',
            showspikes=True,
            spikemode='across',
            spikethickness=1,
            spikecolor='#555',
            spikedash='dot',
        ),
        legend=dict(
            orientation='h', x=0.5, xanchor='center', y=-0.15,
            font=dict(color='#ddd')
        ),
        margin=dict(l=60, r=60, t=60, b=80)
    )

    return fig


app = Dash(__name__)

app.layout = html.Div([
    html.Div([
        html.H2(f'{CALL_SYM} + {PUT_SYM}', style={'margin': '0', 'color': '#fff'}),
        html.Span('期权对价格走势（1分钟K线）', style={'color': '#aaa', 'fontSize': '14px'}),
    ], style={'backgroundColor': '#1a1a2e', 'padding': '15px 25px',
              'borderBottom': '3px solid #e94560'}),

    dcc.Graph(id='pair-chart', figure=build_figure()),

    dcc.Interval(id='timer', interval=60_000, n_intervals=0),
], style={'backgroundColor': '#0f0f23', 'minHeight': '100vh'})


@app.callback(Output('pair-chart', 'figure'), Input('timer', 'n_intervals'))
def refresh(_):
    return build_figure()


if __name__ == '__main__':
    print(f'启动 {CALL_SYM}+{PUT_SYM} 走势图: http://localhost:{PORT}')
    app.run(host='0.0.0.0', port=PORT, debug=False)
