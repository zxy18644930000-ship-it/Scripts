"""
白银(AG) 日内多次循环卖出宽跨回测 v3 — 浅虚值版

v2教训：深度虚值价格之和≈65点，止盈目标仅1-2tick = 刷单幻觉
v3改进：浅虚值(OTM 100-300点)，价格之和300-500点，止盈10+tick = 可执行

选对逻辑：
- Call行权价 = ATM + OTM_OFFSET (100/200/300)
- Put行权价  = ATM - OTM_OFFSET (100/200/300)
- 必须两腿都有足够volume (>=30手/天)

tick验证：每笔盈利必须>=5tick才算有效
"""

import os
import gc
import json
import re
import warnings
from collections import defaultdict
from datetime import timedelta

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

warnings.filterwarnings('ignore')

HOME = os.path.expanduser('~')
DATA_PATH = f'{HOME}/Downloads/期权_parquet/SHFE/ag.parquet'
TICK_SIZE = 1  # AG期权最小变动价位
UNIT = 15      # 合约乘数 15kg/手

# ========== 参数 ==========
OTM_OFFSETS = [100, 200, 300]              # 虚值度（点）
TP_COEFFICIENTS = [1.0, 1.5, 2.0, 3.0]    # 更大的TP系数
SL_THRESHOLDS = [3.0, 5.0, 999]           # 腿比止损
REENTRY_PCTS = [0.9, 1.0, 1.1]            # 再入场条件
DTE_RANGE = (15, 60)
MAX_TRADES_PER_DAY = 10
COOLDOWN_MINUTES = 10                       # 冷却期拉长到10分钟
MIN_PROFIT_TICKS = 5                        # 最小有效盈利tick数（用于结果验证）


def estimate_dte(yymm_str, trade_date):
    yy, mm = int(yymm_str[:2]), int(yymm_str[2:])
    ey, em = 2000 + yy, mm - 1
    if em <= 0:
        em, ey = 12, ey - 1
    try:
        exp = pd.Timestamp(ey, em, 25).date()
    except:
        exp = pd.Timestamp(ey, em, 20).date()
    return max((exp - trade_date).days, 1)


def select_shallow_otm_pair(day_df, otm_offset):
    """
    选浅虚值宽跨：
    1. 找主力月份
    2. ATM推算
    3. Call = ATM + offset, Put = ATM - offset
    4. 检查两腿都有volume
    """
    day_df = day_df.copy()
    m = day_df['symbol'].str.extract(r'ag(\d{4})([CP])(\d+)')
    day_df['yymm'], day_df['cp'] = m[0], m[1]
    day_df['strike'] = pd.to_numeric(m[2])
    day_df = day_df.dropna(subset=['yymm'])
    day_df['strike'] = day_df['strike'].astype(int)

    vol_by_m = day_df.groupby('yymm')['volume'].sum()
    if vol_by_m.empty:
        return None
    main_m = vol_by_m.idxmax()
    mdf = day_df[day_df['yymm'] == main_m]

    # ATM
    first = mdf[mdf['datetime'] == mdf['datetime'].min()]
    cp = first[first['cp'] == 'C'].set_index('strike')['close']
    pp = first[first['cp'] == 'P'].set_index('strike')['close']
    common = sorted(set(cp.index) & set(pp.index))
    if len(common) < 3:
        return None
    atm = min(common, key=lambda k: abs(cp.get(k, 999) - pp.get(k, 999)))

    call_k = atm + otm_offset
    put_k = atm - otm_offset

    call_sym = f'SHFE.ag{main_m}C{call_k}'
    put_sym = f'SHFE.ag{main_m}P{put_k}'

    # 检查两腿都有数据和volume
    c_data = mdf[mdf['symbol'] == call_sym]
    p_data = mdf[mdf['symbol'] == put_sym]
    if c_data.empty or p_data.empty:
        return None
    if c_data['volume'].sum() < 30 or p_data['volume'].sum() < 30:
        return None

    # 验证价格合理
    c_avg = c_data['close'].mean()
    p_avg = p_data['close'].mean()
    if c_avg < 10 or p_avg < 10:
        return None

    return call_sym, put_sym, call_k, put_k, main_m, atm, c_avg, p_avg


def build_minute_sum(day_df, call_sym, put_sym):
    c = day_df[day_df['symbol'] == call_sym][['datetime', 'close']].rename(
        columns={'close': 'call'}).set_index('datetime')
    p = day_df[day_df['symbol'] == put_sym][['datetime', 'close']].rename(
        columns={'close': 'put'}).set_index('datetime')
    mg = c.join(p, how='inner').sort_index()
    if mg.empty:
        return mg
    mg['psum'] = mg['call'] + mg['put']
    return mg


def simulate_day(mdata, dte, tp_coeff, sl_thresh, reentry_pct):
    if len(mdata) < 10:
        return []

    trades = []
    in_pos = False
    entry_sum = entry_time = entry_c = entry_p = 0
    count = 0
    cooldown = None
    tp_tgt = 0

    idx = mdata.index
    vals = mdata[['call', 'put', 'psum']].values
    end_t = idx[-1]
    close_cut = end_t - pd.Timedelta(minutes=5)
    no_entry_cut = end_t - pd.Timedelta(minutes=20)  # 收盘前20min不入场
    warm = idx[0] + pd.Timedelta(minutes=5)

    for i in range(len(idx)):
        ts = idx[i]
        cv, pv, sv = vals[i]

        if not in_pos:
            if cooldown and ts < cooldown:
                continue
            if count >= MAX_TRADES_PER_DAY or ts >= no_entry_cut:
                continue

            if count == 0:
                if ts <= warm:
                    continue
                enter = True
            else:
                enter = sv >= entry_sum * reentry_pct

            if enter:
                in_pos = True
                entry_sum, entry_time, entry_c, entry_p = sv, ts, cv, pv
                tp_tgt = entry_sum / dte * tp_coeff
        else:
            profit = entry_sum - sv
            tp_hit = profit >= tp_tgt

            hi, lo = max(cv, pv), min(cv, pv)
            lr = hi / lo if lo > 0 else 999
            sl_hit = lr >= sl_thresh
            fc = ts >= close_cut

            if tp_hit or sl_hit or fc:
                reason = 'TP' if tp_hit else ('SL' if sl_hit else 'FC')
                ticks = profit / TICK_SIZE
                trades.append({
                    'entry_t': str(entry_time),
                    'exit_t': str(ts),
                    'entry_sum': round(entry_sum, 1),
                    'exit_sum': round(sv, 1),
                    'profit': round(profit, 1),
                    'ticks': round(ticks, 1),
                    'profit_yuan': round(profit * UNIT, 0),
                    'hold_min': round((ts - entry_time).total_seconds() / 60, 1),
                    'reason': reason,
                    'entry_c': round(entry_c, 1),
                    'entry_p': round(entry_p, 1),
                    'lr': round(lr, 2),
                })
                in_pos = False
                count += 1
                cooldown = ts + pd.Timedelta(minutes=COOLDOWN_MINUTES)

    return trades


def main():
    print("=" * 70)
    print("AG 日内多次循环宽跨 v3 — 浅虚值 + tick验证")
    print("=" * 70)
    print(f"虚值度: {OTM_OFFSETS}点 | TP: {TP_COEFFICIENTS}")
    print(f"SL: {SL_THRESHOLDS} | Reentry: {REENTRY_PCTS}")
    print(f"冷却期: {COOLDOWN_MINUTES}min | 最小有效盈利: {MIN_PROFIT_TICKS}tick")
    print()

    pf = pq.ParquetFile(DATA_PATH)

    # Pass 1: 扫描日期
    print("Pass 1: 扫描日期...")
    dates = set()
    for b in pf.iter_batches(batch_size=1000000, columns=['datetime']):
        dates.update(b.to_pandas()['datetime'].dt.date.unique())
    dates = sorted(dates)
    print(f"  {len(dates)}个交易日")
    gc.collect()

    # Pass 2: 逐天处理
    print("\nPass 2: 回测...")
    bufs = {}
    results = defaultdict(list)  # (otm, tp, sl, re) -> trades
    n_proc = 0
    n_valid = 0

    for batch in pf.iter_batches(batch_size=500000, columns=['datetime', 'symbol', 'close', 'volume']):
        df = batch.to_pandas()
        df['date'] = df['datetime'].dt.date
        for d, grp in df.groupby('date'):
            bufs.setdefault(d, []).append(grp.drop(columns=['date']))

        cur_max = df['date'].max()
        safe = cur_max - timedelta(days=2)
        ready = [d for d in sorted(bufs.keys()) if d <= safe]

        for d in ready:
            day_df = pd.concat(bufs.pop(d), ignore_index=True)
            valid = _process_day(day_df, d, results)
            n_valid += valid
            n_proc += 1
            if n_proc % 50 == 0:
                print(f"  {n_proc}/{len(dates)} (有效{n_valid})...")
                gc.collect()
        del df
        gc.collect()

    for d in sorted(bufs.keys()):
        day_df = pd.concat(bufs.pop(d), ignore_index=True)
        n_valid += _process_day(day_df, d, results)
        n_proc += 1
    gc.collect()

    print(f"\n完成: {n_proc}天, {n_valid}有效")
    summarize(results, n_valid, n_proc)


def _process_day(day_df, trade_date, results):
    valid = 0
    for otm in OTM_OFFSETS:
        pair = select_shallow_otm_pair(day_df, otm)
        if pair is None:
            continue
        call_sym, put_sym, ck, pk, yymm, atm, c_avg, p_avg = pair
        dte = estimate_dte(yymm, trade_date)
        if dte < DTE_RANGE[0] or dte > DTE_RANGE[1]:
            continue

        mdata = build_minute_sum(day_df, call_sym, put_sym)
        if len(mdata) < 20:
            continue

        for tp in TP_COEFFICIENTS:
            # 预检：止盈目标够不够5tick
            avg_sum = mdata['psum'].iloc[:5].mean()
            tp_target = avg_sum / dte * tp
            if tp_target < MIN_PROFIT_TICKS:
                continue  # 跳过不合格参数

            for sl in SL_THRESHOLDS:
                for re in REENTRY_PCTS:
                    trades = simulate_day(mdata, dte, tp, sl, re)
                    if trades:
                        for t in trades:
                            t['date'] = str(trade_date)
                            t['dte'] = dte
                            t['otm'] = otm
                            t['atm'] = atm
                            t['avg_sum'] = round(avg_sum, 1)
                        results[(otm, tp, sl, re)].extend(trades)
        valid = 1
    return valid


def summarize(results, n_valid, n_total):
    rows = []
    for (otm, tp, sl, re), trades in results.items():
        if not trades:
            continue
        df = pd.DataFrame(trades)
        n = len(df)
        nd = df['date'].nunique()
        daily = df.groupby('date').agg(cnt=('profit', 'count'), pnl=('profit', 'sum'))
        multi = (daily['cnt'] > 1).sum()
        wins = df[df['profit'] > 0]

        # tick分布
        win_ticks = wins['ticks'].values if len(wins) > 0 else [0]
        pcts = np.percentile(win_ticks, [25, 50, 75]) if len(win_ticks) > 0 else [0, 0, 0]

        # 过滤<=3tick后
        filtered = df[(df['ticks'] > 3) | (df['profit'] <= 0)]
        f_pnl = filtered['profit'].sum() if len(filtered) > 0 else 0

        rows.append({
            'otm': otm, 'tp': tp, 'sl': sl, 're': re,
            'trades': n, 'days': nd,
            'avg_daily': round(daily['cnt'].mean(), 2),
            'multi_pct': round(multi / nd * 100, 1) if nd > 0 else 0,
            'wr': round((df['profit'] > 0).mean() * 100, 1),
            'total_pnl': round(df['profit'].sum(), 0),
            'daily_pnl': round(daily['pnl'].mean(), 1),
            'max_loss': round(daily['pnl'].min(), 0),
            'avg_hold': round(df['hold_min'].mean(), 1),
            'tick_p25': round(pcts[0], 1),
            'tick_p50': round(pcts[1], 1),
            'tick_p75': round(pcts[2], 1),
            'pnl_filtered': round(f_pnl, 0),  # 过滤小tick后PnL
            'pnl_retain': round(f_pnl / df['profit'].sum() * 100, 0) if df['profit'].sum() != 0 else 0,
            'avg_sum': round(df['avg_sum'].mean(), 0),  # 平均价格之和
            'tp_n': (df['reason'] == 'TP').sum(),
            'sl_n': (df['reason'] == 'SL').sum(),
            'fc_n': (df['reason'] == 'FC').sum(),
            'avg_yuan': round(df['profit_yuan'].mean(), 0),  # 平均每笔盈利(元)
        })

    if not rows:
        print("没有交易！")
        return

    sdf = pd.DataFrame(rows).sort_values('pnl_filtered', ascending=False)

    print("\n" + "=" * 140)
    print(f"结果总览 ({n_valid}有效天, 按过滤后PnL排序)")
    print("=" * 140)
    print(f"{'OTM':>4} {'TP':>4} {'SL':>5} {'Re':>4} | {'笔数':>5} {'天':>4} {'日均':>4} {'多次%':>5} "
          f"{'WR%':>5} {'总PnL':>7} {'日PnL':>6} {'最大亏':>6} | "
          f"{'P25':>5} {'P50':>5} {'P75':>5} tick | {'过滤PnL':>7} {'保留%':>5} | "
          f"{'均元/笔':>7} {'均Sum':>5}")
    print("-" * 140)

    for _, r in sdf.head(30).iterrows():
        sl_s = "NoSL" if r['sl'] >= 999 else f"{r['sl']}x"
        pc = '\033[32m' if r['pnl_filtered'] > 0 else '\033[31m'
        print(f"{r['otm']:>4} {r['tp']:>4} {sl_s:>5} {r['re']:>4} | "
              f"{r['trades']:>5} {r['days']:>4} {r['avg_daily']:>4} {r['multi_pct']:>5} "
              f"{r['wr']:>5} {r['total_pnl']:>7} {r['daily_pnl']:>6} {r['max_loss']:>6} | "
              f"{r['tick_p25']:>5} {r['tick_p50']:>5} {r['tick_p75']:>5} tick | "
              f"{pc}{r['pnl_filtered']:>7}\033[0m {r['pnl_retain']:>5}% | "
              f"{r['avg_yuan']:>7} {r['avg_sum']:>5}")

    # 关键对比：多次 vs 单次
    print("\n" + "=" * 140)
    print("多次 vs 单次对比 (过滤后PnL)")
    print("=" * 140)
    for otm in OTM_OFFSETS:
        for tp in [1.5, 2.0, 3.0]:
            for sl in [3.0, 999]:
                sl_s = "NoSL" if sl >= 999 else f"SL{sl}x"
                line = f"  OTM{otm}/TP{tp}/{sl_s}: "
                items = []
                for re in [1.1, 1.0, 0.9]:
                    key = (otm, tp, sl, re)
                    t = results.get(key, [])
                    if t:
                        tdf = pd.DataFrame(t)
                        # 过滤后
                        filt = tdf[(tdf['ticks'] > 3) | (tdf['profit'] <= 0)]
                        nd = tdf['date'].nunique()
                        avg_d = len(tdf) / nd
                        f_pnl = filt['profit'].sum()
                        items.append(f"Re{re}({avg_d:.1f}笔/天 PnL={f_pnl:.0f})")
                if items:
                    print(line + " | ".join(items))

    # 保存
    out_json = f'{HOME}/Scripts/ag_multi_trade_v3_results.json'
    save = {
        'version': 'v3_shallow_otm',
        'summary': sdf.to_dict('records'),
        'params': {
            'otm_offsets': OTM_OFFSETS, 'tp': TP_COEFFICIENTS,
            'sl': SL_THRESHOLDS, 're': REENTRY_PCTS,
            'cooldown': COOLDOWN_MINUTES, 'min_ticks': MIN_PROFIT_TICKS,
        },
        'days': {'valid': n_valid, 'total': n_total},
    }
    # top 5 详细trades
    top_keys = [(r['otm'], r['tp'], r['sl'], r['re']) for _, r in sdf.head(5).iterrows()]
    save['top_details'] = {f"otm{k[0]}_tp{k[1]}_sl{k[2]}_re{k[3]}": results[k] for k in top_keys if k in results}

    with open(out_json, 'w') as f:
        json.dump(save, f, indent=2, default=str)
    print(f"\nJSON: {out_json}")

    # HTML
    generate_html(sdf, results, n_valid)


def generate_html(sdf, results, n_valid):
    out = f'{HOME}/Scripts/ag_multi_trade_v3_report.html'
    top30 = sdf.head(30).to_dict('records')

    # 各OTM档位的最优参数对比
    otm_compare = []
    for otm in OTM_OFFSETS:
        sub = sdf[sdf['otm'] == otm]
        if not sub.empty:
            best = sub.iloc[0]
            otm_compare.append({
                'otm': otm, 'avg_sum': best['avg_sum'],
                'best_pnl': best['pnl_filtered'],
                'best_params': f"TP{best['tp']}/SL{best['sl']}/Re{best['re']}",
                'wr': best['wr'], 'avg_daily': best['avg_daily'],
            })

    # 再入场对比数据
    re_compare = defaultdict(dict)
    for _, r in sdf.iterrows():
        key = f"OTM{r['otm']}/TP{r['tp']}/SL{r['sl']}"
        re_compare[key][r['re']] = r['pnl_filtered']

    # 挑几个有代表性的
    re_labels = []
    re_series = {0.9: [], 1.0: [], 1.1: []}
    for key in list(re_compare.keys())[:8]:
        re_labels.append(key)
        for rv in [0.9, 1.0, 1.1]:
            re_series[rv].append(re_compare[key].get(rv, 0))

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>AG 日内多次宽跨 v3 — 浅虚值</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
*{{box-sizing:border-box}}
body{{font-family:-apple-system,sans-serif;background:#0a0a1a;color:#e0e0e0;margin:0;padding:20px}}
.hdr{{text-align:center;padding:20px;background:linear-gradient(135deg,#1a1a3e,#2d1b4e);border-radius:12px;margin-bottom:20px}}
.hdr h1{{color:#ffd700;margin:0;font-size:24px}} .hdr p{{color:#aaa;margin:4px 0;font-size:13px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(350px,1fr));gap:15px;margin-bottom:20px}}
.card{{background:#1a1a2e;border-radius:10px;padding:16px;border:1px solid #333}}
.card h3{{color:#ffd700;margin:0 0 10px;font-size:15px}}
.m{{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #222;font-size:13px}}
.ml{{color:#888}} .mv{{font-weight:bold}}
.pos{{color:#00ff88}} .neg{{color:#ff4444}} .warn{{color:#ffaa00}}
.ch{{width:100%;height:380px}}
table{{width:100%;border-collapse:collapse;font-size:11px}}
th{{background:#2a2a4a;color:#ffd700;padding:7px 5px;text-align:right;position:sticky;top:0}}
th:first-child,th:nth-child(2),th:nth-child(3),th:nth-child(4){{text-align:left}}
td{{padding:5px;border-bottom:1px solid #222;text-align:right}}
td:first-child,td:nth-child(2),td:nth-child(3),td:nth-child(4){{text-align:left}}
tr:hover{{background:#1a1a3e}}
.hl{{background:#1a2a1a!important}}
</style></head><body>

<div class="hdr">
<h1>AG 日内多次循环宽跨 v3 — 浅虚值 + tick验证</h1>
<p>浅虚值OTM {OTM_OFFSETS}点 | TP {TP_COEFFICIENTS} | 冷却{COOLDOWN_MINUTES}min | 最小有效盈利{MIN_PROFIT_TICKS}tick | {n_valid}有效天</p>
<p>v2教训: 深度虚值80%盈利仅1-2tick=刷单 → v3用浅虚值(价格之和300-500), 每笔10+tick</p>
</div>

<div class="grid">
<div class="card">
<h3>各OTM档位最优结果</h3>"""

    for item in otm_compare:
        pc = 'pos' if item['best_pnl'] > 0 else 'neg'
        html += f"""
<div class="m"><span class="ml">OTM {item['otm']}点 (均Sum={item['avg_sum']})</span>
<span class="mv {pc}">过滤PnL={item['best_pnl']:.0f} [{item['best_params']}] WR{item['wr']}% {item['avg_daily']}笔/天</span></div>"""

    html += f"""
<div class="m" style="border-top:2px solid #ffd700;margin-top:8px;padding-top:8px">
<span class="ml">核心指标</span><span class="mv warn">过滤后PnL = 扣除<=3tick小盈利后的真实收益</span></div>
</div>

<div class="card">
<h3>多次 vs 单次 再入场对比</h3>
<div id="re_chart" class="ch" style="height:300px"></div>
</div>
</div>

<div class="card" style="margin-bottom:20px">
<h3>Top 30 参数 (按过滤后PnL排序)</h3>
<div id="top_chart" class="ch"></div>
</div>

<div class="card" style="margin-bottom:20px">
<h3>全部结果明细 — 关注"过滤PnL"和"保留%"列</h3>
<div style="overflow:auto;max-height:600px">
<table>
<tr><th>OTM</th><th>TP</th><th>SL</th><th>Re</th>
<th>笔数</th><th>天</th><th>日均</th><th>多次%</th><th>WR%</th>
<th>总PnL</th><th>日PnL</th><th>最大亏</th>
<th>P25</th><th>P50</th><th>P75</th>
<th>过滤PnL</th><th>保留%</th><th>均元/笔</th><th>均Sum</th></tr>"""

    for _, r in sdf.iterrows():
        sl_s = "NoSL" if r['sl'] >= 999 else f"{r['sl']}x"
        pc = "pos" if r['pnl_filtered'] > 0 else "neg"
        hl = "hl" if r['pnl_retain'] >= 60 and r['pnl_filtered'] > 0 else ""
        html += f"""<tr class="{hl}">
<td>{r['otm']}</td><td>{r['tp']}</td><td>{sl_s}</td><td>{r['re']}</td>
<td>{r['trades']}</td><td>{r['days']}</td><td>{r['avg_daily']}</td><td>{r['multi_pct']}</td><td>{r['wr']}</td>
<td>{r['total_pnl']:.0f}</td><td>{r['daily_pnl']}</td><td>{r['max_loss']:.0f}</td>
<td>{r['tick_p25']}</td><td>{r['tick_p50']}</td><td>{r['tick_p75']}</td>
<td class="{pc}">{r['pnl_filtered']:.0f}</td><td class="{pc}">{r['pnl_retain']:.0f}</td>
<td>{r['avg_yuan']:.0f}</td><td>{r['avg_sum']:.0f}</td></tr>"""

    # Chart data
    t30_labels = json.dumps([f"OTM{r['otm']}/TP{r['tp']}/SL{r['sl']}/Re{r['re']}" for r in top30])
    t30_raw = json.dumps([r['total_pnl'] for r in top30])
    t30_filt = json.dumps([r['pnl_filtered'] for r in top30])

    html += f"""</table></div></div>
<script>
var c1=echarts.init(document.getElementById('top_chart'));
c1.setOption({{tooltip:{{trigger:'axis'}},legend:{{data:['原始PnL','过滤后PnL'],textStyle:{{color:'#aaa'}}}},
xAxis:{{type:'category',data:{t30_labels},axisLabel:{{rotate:55,fontSize:8}}}},
yAxis:{{type:'value',name:'PnL'}},
series:[
{{name:'原始PnL',type:'bar',data:{t30_raw},itemStyle:{{color:'#555'}}}},
{{name:'过滤后PnL',type:'bar',data:{t30_filt},itemStyle:{{color:'#00ff88'}}}}
]}});

var c2=echarts.init(document.getElementById('re_chart'));
c2.setOption({{tooltip:{{trigger:'axis'}},legend:{{textStyle:{{color:'#aaa'}}}},
xAxis:{{type:'category',data:{json.dumps(re_labels)},axisLabel:{{rotate:30,fontSize:9}}}},
yAxis:{{type:'value',name:'过滤后PnL'}},
series:[
{{name:'Re0.9(积极)',type:'bar',data:{json.dumps(re_series[0.9])},itemStyle:{{color:'#ff6b6b'}}}},
{{name:'Re1.0(中性)',type:'bar',data:{json.dumps(re_series[1.0])},itemStyle:{{color:'#ffd700'}}}},
{{name:'Re1.1(保守)',type:'bar',data:{json.dumps(re_series[1.1])},itemStyle:{{color:'#888'}}}}
]}});

window.addEventListener('resize',()=>{{c1.resize();c2.resize();}});
</script></body></html>"""

    with open(out, 'w') as f:
        f.write(html)
    print(f"HTML: {out}")


if __name__ == '__main__':
    main()
