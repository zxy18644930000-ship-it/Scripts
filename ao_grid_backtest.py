#!/usr/bin/env python3
"""
氧化铝(AO) 夜盘卖出跨式 — Tick中间价网格回测

数据: ~/Downloads/ao/*.zip (SHFE tick CSV, BidPrice1/AskPrice1)
策略: 23点后卖出ATM跨式, 01点前出仓
网格: 进仓时间(23:00~00:50) × 止盈系数(premium/DTE×coeff)
价格: mid = (BidPrice1 + AskPrice1) / 2
"""

import os, re, json, sys, zipfile, glob, tempfile, shutil
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
import time as _time
import http.server, threading, webbrowser

# ===== Config =====
PORT = 8061
OUTPUT_HTML = '/tmp/ao_grid_backtest.html'
ZIP_DIR = os.path.expanduser('~/Downloads/ao')

ENTRY_OFFSETS = list(range(0, 115, 5))  # 23:00后0~110分 → 23:00到00:50
COEFFICIENTS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0, 1.2, 1.5, 2.0]
DEADLINE_SORTKEY = (24 * 3600) + (55 * 60)  # 00:55 = 89700

MIN_DTE, MAX_DTE = 7, 70
MIN_TICKS = 30  # ATM期权23:00后最少tick数

USECOLS = ['TradingDay', 'InstrumentID', 'UpdateTime', 'UpdateMillisec',
           'BidPrice1', 'AskPrice1', 'Volume', 'OpenInterest']


# ===== Expiry =====

def load_expiry_dates():
    path = os.path.expanduser('~/Downloads/期权_parquet/SHFE_meta.parquet')
    if not os.path.exists(path):
        return {}
    meta = pd.read_parquet(path)
    ao = meta[meta['合约代码'].str.contains(r'\.ao', case=False, na=False)]
    if '到期日' not in ao.columns:
        return {}
    exp_map = {}
    for _, row in ao.iterrows():
        m = re.search(r'ao(\d{4})', str(row['合约代码']))
        if m and pd.notna(row['到期日']):
            month = m.group(1)
            if month not in exp_map:
                exp_map[month] = pd.to_datetime(row['到期日']).date()
    return exp_map


def estimate_expiry(month_code):
    y = 2000 + int(month_code[:2])
    m = int(month_code[2:])
    if m == 1:
        return datetime(y - 1, 12, 25).date()
    return datetime(y, m - 1, 25).date()


# ===== Time helpers =====

def time_to_sortkey(update_time, ms=0):
    """UpdateTime → sort key (handle midnight: 00:xx → 24:xx)"""
    h, m, s = map(int, update_time.split(':'))
    if h < 21:
        h += 24
    return h * 3600 + m * 60 + s + ms / 1000


def sortkey_to_hhmm(sk):
    h = int(sk) // 3600
    m = (int(sk) % 3600) // 60
    if h >= 24:
        h -= 24
    return f"{h:02d}:{m:02d}"


# ===== Parse filename =====

def parse_fname(fname):
    """SHFE.ao2502C3300.csv → ('2502', 'C', 3300)"""
    m = re.match(r'SHFE\.ao(\d{4})([CP])(\d+)\.csv', fname)
    if m:
        return m.group(1), m.group(2), int(m.group(3))
    return None, None, None


# ===== Process one zip =====

def process_one_zip(zip_path, expiry_map):
    zip_name = os.path.basename(zip_path).replace('.zip', '')
    print(f"\n📦 {zip_name} ", end='', flush=True)
    t0 = _time.time()

    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmpdir)

        # Parse all files
        file_info = []
        for fname in os.listdir(tmpdir):
            month, cp, strike = parse_fname(fname)
            if month:
                fpath = os.path.join(tmpdir, fname)
                file_info.append({'path': fpath, 'month': month, 'cp': cp,
                                  'strike': strike, 'size': os.path.getsize(fpath)})

        if not file_info:
            print("(空)")
            return []

        fi = pd.DataFrame(file_info)

        # Process each contract month
        for month, mf in fi.groupby('month'):
            exp = expiry_map.get(month, estimate_expiry(month))

            # 找有C+P的行权价, 按文件大小选top-10
            strike_pairs = mf.groupby('strike').filter(
                lambda g: set(g['cp']) == {'C', 'P'})
            if strike_pairs.empty:
                continue

            top_strikes = (strike_pairs.groupby('strike')['size'].sum()
                           .nlargest(10).index.tolist())
            load_files = strike_pairs[strike_pairs['strike'].isin(top_strikes)]

            # 加载tick数据(只保留夜盘)
            dfs = []
            for _, row in load_files.iterrows():
                try:
                    df = pd.read_csv(row['path'], usecols=USECOLS)
                    hour_str = df['UpdateTime'].str[:2]
                    df = df[hour_str.isin(['21', '22', '23', '00'])]
                    if df.empty:
                        continue
                    df = df[(df['BidPrice1'] > 0) & (df['AskPrice1'] > 0)]
                    df['mid'] = (df['BidPrice1'] + df['AskPrice1']) / 2
                    df['strike'] = row['strike']
                    df['cp'] = row['cp']
                    dfs.append(df)
                except Exception:
                    continue

            if not dfs:
                continue

            all_data = pd.concat(dfs, ignore_index=True)
            # 向量化计算sort_key (避免apply逐行, 快100倍)
            _parts = all_data['UpdateTime'].str.split(':', expand=True).astype(int)
            _h = _parts[0].values.copy()
            _h[_h < 21] += 24
            all_data['sort_key'] = (_h * 3600 + _parts[1].values * 60 +
                                    _parts[2].values + all_data['UpdateMillisec'].values / 1000)

            # 逐交易日处理
            for td in sorted(all_data['TradingDay'].unique()):
                td_date = datetime.strptime(td, '%Y-%m-%d').date()
                dte = (exp - td_date).days
                if dte < MIN_DTE or dte > MAX_DTE:
                    continue

                day = all_data[all_data['TradingDay'] == td]

                # ATM: 21:00~21:10的mid, 找|C_mid - P_mid|最小的strike
                early = day[day['sort_key'] < time_to_sortkey('21:10:00')]
                if early.empty:
                    early = day[day['sort_key'] < time_to_sortkey('21:30:00')]
                if early.empty:
                    continue

                pivot = (early.sort_values('sort_key')
                         .groupby(['strike', 'cp'])['mid'].last().unstack())
                if 'C' not in pivot.columns or 'P' not in pivot.columns:
                    continue
                both = pivot[(pivot['C'] > 0) & (pivot['P'] > 0)].copy()
                if both.empty:
                    continue

                both['diff'] = (both['C'] - both['P']).abs()
                atm_strike = int(both['diff'].idxmin())
                F_est = atm_strike + both.loc[atm_strike, 'C'] - both.loc[atm_strike, 'P']

                # 构建ATM mid序列 (forward-fill C/P)
                atm = day[day['strike'] == atm_strike].copy()
                c_tk = (atm[atm['cp'] == 'C'][['sort_key', 'mid']]
                        .rename(columns={'mid': 'C_mid'}).sort_values('sort_key'))
                p_tk = (atm[atm['cp'] == 'P'][['sort_key', 'mid']]
                        .rename(columns={'mid': 'P_mid'}).sort_values('sort_key'))

                # 23:00后的tick数量检查
                entry_start = time_to_sortkey('23:00:00')
                c_after23 = c_tk[c_tk['sort_key'] >= entry_start]
                p_after23 = p_tk[p_tk['sort_key'] >= entry_start]
                if len(c_after23) < MIN_TICKS or len(p_after23) < MIN_TICKS:
                    continue

                # Merge + forward-fill
                all_sk = pd.concat([c_tk[['sort_key']], p_tk[['sort_key']]]) \
                    .drop_duplicates().sort_values('sort_key').reset_index(drop=True)
                merged = all_sk.merge(c_tk, on='sort_key', how='left')
                merged = merged.merge(p_tk, on='sort_key', how='left')
                merged['C_mid'] = merged['C_mid'].ffill()
                merged['P_mid'] = merged['P_mid'].ffill()
                merged = merged.dropna().copy()
                merged['sum_mid'] = merged['C_mid'] + merged['P_mid']

                if len(merged) < 20:
                    continue

                # Grid search
                day_res = grid_search_day(merged, dte, td, month, atm_strike, F_est)
                results.extend(day_res)

            print('.', end='', flush=True)

    elapsed = _time.time() - t0
    print(f" {len(results)}笔 ({elapsed:.1f}s)")
    return results


# ===== Grid search =====

def grid_search_day(series, dte, td, month, atm_strike, F_est):
    results = []
    sk = series['sort_key'].values
    sums = series['sum_mid'].values
    idx_arr = np.arange(len(sk))

    for offset in ENTRY_OFFSETS:
        entry_sk = 82800 + offset * 60  # 23:00 = 82800

        # 第一个 >= entry_sk 的tick
        mask = sk >= entry_sk
        if not mask.any():
            continue
        ei = np.argmax(mask)
        entry_sum = sums[ei]
        if entry_sum <= 0 or dte <= 0:
            continue

        # deadline之前的future ticks
        future_mask = (idx_arr > ei) & (sk <= DEADLINE_SORTKEY)
        if not future_mask.any():
            continue
        fi = idx_arr[future_mask]
        f_sums = sums[fi]
        f_sks = sk[fi]
        pnls = entry_sum - f_sums

        for coeff in COEFFICIENTS:
            target = entry_sum / dte * coeff
            hit = pnls >= target

            if hit.any():
                hi = np.argmax(hit)
                exit_pnl = pnls[hi]
                exit_sk = f_sks[hi]
                reason = 'target'
            else:
                exit_pnl = pnls[-1]
                exit_sk = f_sks[-1]
                reason = 'deadline'

            hold = (exit_sk - sk[ei]) / 60

            results.append({
                'entry_time': sortkey_to_hhmm(sk[ei]),
                'coeff': coeff,
                'pnl': round(float(exit_pnl), 2),
                'pnl_pct': round(float(exit_pnl / entry_sum * 100), 2),
                'entry_sum': round(float(entry_sum), 2),
                'target': round(float(target), 2),
                'exit_reason': reason,
                'holding_min': round(float(hold), 1),
                'dte': dte,
                'trading_date': td,
                'month': month,
                'atm_strike': atm_strike,
                'F_est': round(float(F_est), 1),
            })

    return results


# ===== Aggregate =====

def aggregate_results(results):
    if not results:
        return {}, [], pd.DataFrame()

    rdf = pd.DataFrame(results)

    time_labels = []
    for off in ENTRY_OFFSETS:
        h = 23 + off // 60
        m = off % 60
        if h >= 24: h -= 24
        time_labels.append(f"{h:02d}:{m:02d}")

    grid = {}
    for tl in time_labels:
        for coeff in COEFFICIENTS:
            sub = rdf[(rdf['entry_time'] == tl) & (rdf['coeff'] == coeff)]
            if sub.empty:
                continue
            n = len(sub)
            wins = (sub['pnl'] > 0).sum()
            tgt = (sub['exit_reason'] == 'target').sum()
            losses = sub[sub['pnl'] < 0]['pnl']

            grid[(tl, coeff)] = {
                'n': n,
                'total_pnl': round(sub['pnl'].sum(), 1),
                'avg_pnl': round(sub['pnl'].mean(), 2),
                'avg_pnl_pct': round(sub['pnl_pct'].mean(), 2),
                'win_rate': round(wins / n * 100, 1),
                'target_hit_rate': round(tgt / n * 100, 1),
                'avg_holding_min': round(sub['holding_min'].mean(), 1),
                'max_loss': round(sub['pnl'].min(), 2),
                'max_win': round(sub['pnl'].max(), 2),
                'profit_factor': round(
                    sub[sub['pnl'] > 0]['pnl'].sum() /
                    max(0.01, -losses.sum()), 2),
                'avg_entry_sum': round(sub['entry_sum'].mean(), 1),
                'avg_dte': round(sub['dte'].mean(), 1),
            }

    return grid, time_labels, rdf


# ===== HTML Report =====

def generate_report(grid, time_labels, rdf):
    if not grid:
        return "<h1>无数据</h1>"

    metrics = ['avg_pnl', 'win_rate', 'target_hit_rate', 'avg_pnl_pct', 'profit_factor', 'avg_holding_min']
    heatmap_data = {}
    for metric in metrics:
        data = []
        for ci, coeff in enumerate(COEFFICIENTS):
            for ti, tl in enumerate(time_labels):
                val = grid.get((tl, coeff), {}).get(metric)
                if val is not None:
                    data.append([ti, ci, round(val, 2)])
        heatmap_data[metric] = data

    best_key = max(grid, key=lambda k: grid[k]['avg_pnl'])
    best = grid[best_key]

    overall = {
        'sessions': rdf['trading_date'].nunique(),
        'total_trades': len(rdf),
        'avg_premium': round(rdf['entry_sum'].mean(), 1),
        'avg_dte': round(rdf['dte'].mean(), 1),
        'date_range': f"{rdf['trading_date'].min()} ~ {rdf['trading_date'].max()}",
    }

    # Time slice (best coeff)
    ts = {'times': [], 'avg_pnl': [], 'win_rate': []}
    bc = best_key[1]
    for tl in time_labels:
        c = grid.get((tl, bc))
        ts['times'].append(tl)
        ts['avg_pnl'].append(c['avg_pnl'] if c else 0)
        ts['win_rate'].append(c['win_rate'] if c else 0)

    # Coeff slice (best time)
    cs = {'coeffs': [], 'avg_pnl': [], 'hit_rate': []}
    bt = best_key[0]
    for coeff in COEFFICIENTS:
        c = grid.get((bt, coeff))
        cs['coeffs'].append(coeff)
        cs['avg_pnl'].append(c['avg_pnl'] if c else 0)
        cs['hit_rate'].append(c['target_hit_rate'] if c else 0)

    # PnL distribution
    best_trades = rdf[(rdf['entry_time'] == best_key[0]) & (rdf['coeff'] == best_key[1])]
    if not best_trades.empty:
        pnls = best_trades['pnl'].values
        step = max(1, round((pnls.max() - pnls.min()) / 20))
        bins = np.arange(np.floor(pnls.min() / step) * step,
                         np.ceil(pnls.max() / step) * step + step, step)
        counts, edges = np.histogram(pnls, bins=bins)
        pnl_dist = {'bins': [f"{edges[i]:.0f}" for i in range(len(counts))],
                     'counts': counts.tolist()}
        sorted_bt = best_trades.sort_values('trading_date')
        cum_pnl = {'dates': sorted_bt['trading_date'].tolist(),
                    'values': [round(v, 1) for v in sorted_bt['pnl'].cumsum()]}
    else:
        pnl_dist = {'bins': [], 'counts': []}
        cum_pnl = {'dates': [], 'values': []}

    html = """<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AO氧化铝 Tick网格回测</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
:root{--bg:#0a0e17;--card:#131a2a;--border:#1e2d4a;--text:#c5d0e0;
--dim:#6b7b95;--heading:#e8ecf1;--accent:#4fc3f7;--green:#81c784;
--warn:#ffb74d;--danger:#ef5350}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'PingFang SC',sans-serif;
background:var(--bg);color:var(--text);line-height:1.7;font-size:15px}
.container{max-width:1100px;margin:0 auto;padding:0 20px}
.header{background:linear-gradient(135deg,#0d1a2e,#152238);
border-bottom:1px solid var(--border);padding:28px 0 20px}
.header h1{font-size:24px;color:var(--heading)}
.header .sub{color:var(--dim);font-size:13px;margin-top:4px}
.section{padding:28px 0;border-bottom:1px solid #111827}
h2{font-size:20px;color:var(--heading);margin-bottom:16px}
h3{font-size:16px;color:var(--heading);margin:16px 0 8px}
.card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:20px;margin:12px 0}
.card-green{border-left:3px solid var(--green)}
.grid-4{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}
@media(max-width:700px){.grid-4{grid-template-columns:1fr 1fr}}
.stat{text-align:center}
.stat-val{font-size:28px;font-weight:700;color:var(--accent)}
.stat-label{font-size:12px;color:var(--dim)}
.chart-box{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:16px;margin:16px 0}
.chart{width:100%;height:500px}.chart-sm{height:350px}
.btn-group{display:flex;gap:6px;flex-wrap:wrap;margin:12px 0}
.btn{padding:6px 14px;border-radius:6px;border:1px solid var(--border);
background:var(--card);color:var(--dim);cursor:pointer;font-size:13px;transition:all .2s}
.btn:hover,.btn.active{color:var(--accent);border-color:var(--accent);background:#1a2540}
code{background:#0d1220;color:var(--accent);padding:2px 6px;border-radius:4px;font-size:13px}
.footer{padding:24px 0;text-align:center;color:var(--dim);font-size:12px;border-top:1px solid var(--border)}
</style></head><body>
<div class="header"><div class="container">
<h1>AO氧化铝 夜盘卖出跨式 — Tick中间价网格回测</h1>
<div class="sub">策略: 23点后卖ATM跨式, 01点前出仓 &middot; 价格: (Bid+Ask)/2 中间价 &middot; __RANGE__</div>
</div></div>

<div class="section"><div class="container">
<h2>回测概览</h2>
<div class="grid-4">
<div class="card stat"><div class="stat-val">__SESS__</div><div class="stat-label">夜盘sessions</div></div>
<div class="card stat"><div class="stat-val">__TRADES__</div><div class="stat-label">总交易笔数</div></div>
<div class="card stat"><div class="stat-val">__PREM__</div><div class="stat-label">平均ATM权利金(mid)</div></div>
<div class="card stat"><div class="stat-val">__DTE__天</div><div class="stat-label">平均DTE</div></div>
</div>
<div class="card card-green"><h3>最优参数 (按平均盈利)</h3>
<p>进仓时间: <code>__BT__</code> &nbsp; 止盈系数: <code>__BC__</code></p>
<div class="grid-4" style="margin-top:12px">
<div><span style="color:var(--green);font-size:20px;font-weight:700">__BPNL__</span><br><span style="color:var(--dim);font-size:12px">平均盈利(点)</span></div>
<div><span style="color:var(--green);font-size:20px;font-weight:700">__BWR__%</span><br><span style="color:var(--dim);font-size:12px">胜率</span></div>
<div><span style="color:var(--accent);font-size:20px;font-weight:700">__BPF__</span><br><span style="color:var(--dim);font-size:12px">盈亏比</span></div>
<div><span style="color:var(--warn);font-size:20px;font-weight:700">__BHOLD__分钟</span><br><span style="color:var(--dim);font-size:12px">平均持仓</span></div>
</div></div></div></div>

<div class="section"><div class="container">
<h2>热力图 — 参数网格</h2>
<div class="btn-group">
<button class="btn active" onclick="sw('avg_pnl',this)">平均盈利(点)</button>
<button class="btn" onclick="sw('avg_pnl_pct',this)">平均盈利(%)</button>
<button class="btn" onclick="sw('win_rate',this)">胜率(%)</button>
<button class="btn" onclick="sw('target_hit_rate',this)">止盈触发率(%)</button>
<button class="btn" onclick="sw('profit_factor',this)">盈亏比</button>
<button class="btn" onclick="sw('avg_holding_min',this)">持仓时间(分)</button>
</div>
<div class="chart-box"><div class="chart" id="hm"></div></div>
</div></div>

<div class="section"><div class="container">
<h2>进仓时间切面 (系数=__BC__)</h2>
<div class="chart-box"><div class="chart chart-sm" id="ts"></div></div>
</div></div>

<div class="section"><div class="container">
<h2>系数切面 (时间=__BT__)</h2>
<div class="chart-box"><div class="chart chart-sm" id="cs"></div></div>
</div></div>

<div class="section"><div class="container">
<h2>PnL分布 (最优参数)</h2>
<div class="chart-box"><div class="chart chart-sm" id="pd"></div></div>
</div></div>

<div class="section"><div class="container">
<h2>累计PnL (最优参数)</h2>
<div class="chart-box"><div class="chart chart-sm" id="cp"></div></div>
</div></div>

<div class="footer"><div class="container">
AO氧化铝 Tick中间价网格回测 &middot; SHFE tick数据 &middot; mid=(Bid1+Ask1)/2
</div></div>

<script>
const TL=__TL__,CL=__CL__,HD=__HD__,BT='__BT__',BC=__BCN__,
TS=__TS__,CS=__CS__,PD=__PD__,CP=__CP__;
const ax={axisLine:{lineStyle:{color:'#2a3a5c'}},axisLabel:{color:'#6b7b95',fontSize:12},
splitLine:{lineStyle:{color:'#111827'}}};
const mn={'avg_pnl':'平均盈利(点)','avg_pnl_pct':'平均盈利(%)','win_rate':'胜率(%)',
'target_hit_rate':'止盈触发率(%)','profit_factor':'盈亏比','avg_holding_min':'持仓(分)'};

let hm=echarts.init(document.getElementById('hm'));
function rh(m){
let d=HD[m]||[];let vs=d.map(x=>x[2]);let lo=Math.min(...vs),hi=Math.max(...vs);
hm.setOption({backgroundColor:'transparent',
tooltip:{formatter:p=>'进仓:'+TL[p.data[0]]+'<br>系数:'+CL[p.data[1]]+'<br>'+mn[m]+': <b>'+p.data[2]+'</b>'},
grid:{left:80,right:80,top:30,bottom:60},
xAxis:{type:'category',data:TL,name:'进仓时间',nameLocation:'center',nameGap:35,
nameTextStyle:{color:'#6b7b95'},...ax,axisLabel:{...ax.axisLabel,rotate:45}},
yAxis:{type:'category',data:CL.map(String),name:'止盈系数',nameLocation:'center',nameGap:55,
nameTextStyle:{color:'#6b7b95'},...ax},
visualMap:{min:lo,max:hi,calculable:true,orient:'vertical',right:10,top:'center',
inRange:{color:['#1a237e','#0d47a1','#006064','#004d40','#1b5e20','#827717','#f57f17','#e65100','#bf360c']},
textStyle:{color:'#6b7b95'}},
series:[{type:'heatmap',data:d,label:{show:d.length<400,fontSize:10,color:'#c5d0e0',
formatter:p=>p.data[2].toFixed(1)},emphasis:{itemStyle:{borderColor:'#fff',borderWidth:2}}}]},true)}
function sw(m,el){document.querySelectorAll('.btn').forEach(b=>b.classList.remove('active'));
el.classList.add('active');rh(m)}
rh('avg_pnl');

(function(){let c=echarts.init(document.getElementById('ts'));c.setOption({backgroundColor:'transparent',
title:{text:'固定系数='+BC,left:'center',top:5,textStyle:{color:'#6b7b95',fontSize:14}},
tooltip:{trigger:'axis',backgroundColor:'#1a1f2e',borderColor:'#2a3a5c',textStyle:{color:'#c5d0e0'}},
legend:{data:['平均盈利','胜率'],top:25,textStyle:{color:'#6b7b95'}},
grid:{left:60,right:60,top:60,bottom:50},
xAxis:{type:'category',data:TS.times,...ax,axisLabel:{...ax.axisLabel,rotate:45}},
yAxis:[{type:'value',name:'盈利(点)',...ax},{type:'value',name:'胜率(%)',min:0,max:100,...ax}],
series:[{name:'平均盈利',type:'bar',data:TS.avg_pnl,itemStyle:{color:p=>p.data>=0?'#81c784':'#ef5350'},barWidth:'60%'},
{name:'胜率',type:'line',yAxisIndex:1,data:TS.win_rate,lineStyle:{width:2,color:'#ffb74d'},
itemStyle:{color:'#ffb74d'},symbol:'circle',symbolSize:6}]});
window.addEventListener('resize',()=>c.resize())})();

(function(){let c=echarts.init(document.getElementById('cs'));c.setOption({backgroundColor:'transparent',
title:{text:'固定时间='+BT,left:'center',top:5,textStyle:{color:'#6b7b95',fontSize:14}},
tooltip:{trigger:'axis',backgroundColor:'#1a1f2e',borderColor:'#2a3a5c',textStyle:{color:'#c5d0e0'}},
legend:{data:['平均盈利','止盈触发率'],top:25,textStyle:{color:'#6b7b95'}},
grid:{left:60,right:60,top:60,bottom:40},
xAxis:{type:'category',data:CS.coeffs.map(String),...ax},
yAxis:[{type:'value',name:'盈利(点)',...ax},{type:'value',name:'触发率(%)',min:0,max:100,...ax}],
series:[{name:'平均盈利',type:'bar',data:CS.avg_pnl,itemStyle:{color:p=>p.data>=0?'#4fc3f7':'#ef5350'},barWidth:'60%'},
{name:'止盈触发率',type:'line',yAxisIndex:1,data:CS.hit_rate,lineStyle:{width:2,color:'#ce93d8'},
itemStyle:{color:'#ce93d8'},symbol:'circle',symbolSize:6}]});
window.addEventListener('resize',()=>c.resize())})();

(function(){let c=echarts.init(document.getElementById('pd'));c.setOption({backgroundColor:'transparent',
title:{text:'每笔PnL分布',left:'center',top:5,textStyle:{color:'#6b7b95',fontSize:14}},
grid:{left:60,right:30,top:40,bottom:40},
xAxis:{type:'category',data:PD.bins,...ax,axisLabel:{...ax.axisLabel,rotate:45}},
yAxis:{type:'value',name:'次数',...ax},
series:[{type:'bar',data:PD.counts,itemStyle:{color:p=>parseFloat(PD.bins[p.dataIndex])<0?'#ef5350':'#81c784'}}]});
window.addEventListener('resize',()=>c.resize())})();

(function(){let c=echarts.init(document.getElementById('cp'));c.setOption({backgroundColor:'transparent',
title:{text:'累计PnL',left:'center',top:5,textStyle:{color:'#6b7b95',fontSize:14}},
grid:{left:60,right:30,top:40,bottom:40},
xAxis:{type:'category',data:CP.dates,...ax,axisLabel:{...ax.axisLabel,interval:'auto'}},
yAxis:{type:'value',name:'累计PnL(点)',...ax},
series:[{type:'line',data:CP.values,lineStyle:{width:2,color:'#4fc3f7'},symbol:'none',
areaStyle:{color:new echarts.graphic.LinearGradient(0,0,0,1,[
{offset:0,color:'rgba(79,195,247,0.15)'},{offset:1,color:'rgba(79,195,247,0)'}])}}]});
window.addEventListener('resize',()=>c.resize())})();

window.addEventListener('resize',()=>hm.resize());
</script></body></html>"""

    # Inject
    html = html.replace('__RANGE__', overall['date_range'])
    html = html.replace('__SESS__', str(overall['sessions']))
    html = html.replace('__TRADES__', f"{overall['total_trades']:,}")
    html = html.replace('__PREM__', str(overall['avg_premium']))
    html = html.replace('__DTE__', str(overall['avg_dte']))
    html = html.replace('__BT__', best_key[0])
    html = html.replace('__BC__', str(best_key[1]))
    html = html.replace('__BCN__', str(best_key[1]))
    html = html.replace('__BPNL__', str(best['avg_pnl']))
    html = html.replace('__BWR__', str(best['win_rate']))
    html = html.replace('__BPF__', str(best['profit_factor']))
    html = html.replace('__BHOLD__', str(best['avg_holding_min']))
    html = html.replace('__TL__', json.dumps(time_labels))
    html = html.replace('__CL__', json.dumps(COEFFICIENTS))
    html = html.replace('__HD__', json.dumps(heatmap_data))
    html = html.replace('__TS__', json.dumps(ts))
    html = html.replace('__CS__', json.dumps(cs))
    html = html.replace('__PD__', json.dumps(pnl_dist))
    html = html.replace('__CP__', json.dumps(cum_pnl))

    return html


# ===== Server =====

class H(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        if self.path in ('/', '/index.html'):
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            with open(OUTPUT_HTML, 'rb') as f:
                self.wfile.write(f.read())
        else:
            self.send_error(404)


# ===== Main =====

def main():
    print("=" * 60)
    print("  AO氧化铝 夜盘卖出跨式 — Tick中间价网格回测")
    print("  进仓: 23:00~00:50 | 出仓: 00:55前 | 价格: (Bid+Ask)/2")
    print("=" * 60)

    expiry_map = load_expiry_dates()
    print(f"到期日映射: {len(expiry_map)}个月份")

    zips = sorted(glob.glob(os.path.join(ZIP_DIR, '*.zip')))
    print(f"Zip文件: {len(zips)}个")

    all_results = []
    for zp in zips:
        res = process_one_zip(zp, expiry_map)
        all_results.extend(res)

    if not all_results:
        print("❌ 无有效交易!")
        return

    print(f"\n✅ 总计 {len(all_results):,} 笔交易")

    grid, time_labels, rdf = aggregate_results(all_results)
    print(f"网格单元: {len(grid)}")

    html = generate_report(grid, time_labels, rdf)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"报告: {OUTPUT_HTML}")

    for port in range(PORT, PORT + 10):
        try:
            server = http.server.HTTPServer(('0.0.0.0', port), H)
            print(f"\n🌐 http://localhost:{port}")
            webbrowser.open(f'http://localhost:{port}')
            server.serve_forever()
        except OSError:
            continue


if __name__ == '__main__':
    main()
