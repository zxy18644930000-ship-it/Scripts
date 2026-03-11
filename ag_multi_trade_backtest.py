"""
白银(AG) 日内多次循环卖出宽跨回测 v2

核心逻辑：
1. 每个交易日，找主力合约月份
2. 选深度虚值的Call+Put（OTM >= 500点，按volume排序）
3. 卖出宽跨，监控价格之和(分钟级)
4. 止盈后出仓，等价格之和回升到入场水平附近再次进仓
5. 统计一天能循环几次，对比单次交易收益

内存优化：两遍扫描，第一遍只提取日期，第二遍按天过滤加载
止盈止损：使用用户原创公式
  - TP = 总权利金 / DTE × 系数
  - SL = 高价腿 / 低价腿 ≥ 阈值
"""

import os
import gc
import json
import re
import warnings
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

warnings.filterwarnings('ignore')

HOME = os.path.expanduser('~')
DATA_PATH = f'{HOME}/Downloads/期权_parquet/SHFE/ag.parquet'

# ========== 参数网格 ==========
TP_COEFFICIENTS = [0.3, 0.5, 0.8, 1.0, 1.5]
SL_THRESHOLDS = [2.0, 3.0, 5.0, 999]    # 999=不止损
REENTRY_PCTS = [0.8, 0.9, 1.0, 1.1]
DTE_RANGE = (15, 60)
MAX_TRADES_PER_DAY = 10
COOLDOWN_MINUTES = 5
MIN_OTM_POINTS = 400   # 最小虚值度（行权价距ATM的距离）


def estimate_dte(yymm_str, trade_date):
    """上期所到期日：交割月前一月倒数第5个交易日，简化为25号"""
    yy = int(yymm_str[:2])
    mm = int(yymm_str[2:])
    exp_year = 2000 + yy
    exp_month = mm - 1
    if exp_month <= 0:
        exp_month = 12
        exp_year -= 1
    try:
        exp_date = pd.Timestamp(exp_year, exp_month, 25).date()
    except:
        exp_date = pd.Timestamp(exp_year, exp_month, 20).date()
    return max((exp_date - trade_date).days, 1)


def select_deep_otm_strangle(day_df):
    """
    选择深度虚值宽跨对：
    1. 找主力月份（volume最大）
    2. 推算ATM
    3. Call选OTM >= 400点中volume最大的
    4. Put选OTM >= 400点中volume最大的
    """
    day_df = day_df.copy()
    m = day_df['symbol'].str.extract(r'ag(\d{4})([CP])(\d+)')
    day_df['yymm'] = m[0]
    day_df['cp'] = m[1]
    day_df['strike'] = pd.to_numeric(m[2])
    day_df = day_df.dropna(subset=['yymm'])
    day_df['strike'] = day_df['strike'].astype(int)

    # 主力月份
    vol_by_month = day_df.groupby('yymm')['volume'].sum()
    if vol_by_month.empty:
        return None
    main_month = vol_by_month.idxmax()
    mdf = day_df[day_df['yymm'] == main_month]

    # 推算ATM
    first_min = mdf['datetime'].min()
    first = mdf[mdf['datetime'] == first_min]
    calls_p = first[first['cp'] == 'C'].set_index('strike')['close']
    puts_p = first[first['cp'] == 'P'].set_index('strike')['close']
    common = sorted(set(calls_p.index) & set(puts_p.index))
    if len(common) < 5:
        return None

    atm = min(common, key=lambda k: abs(calls_p.get(k, 999) - puts_p.get(k, 999)))

    # 深度虚值Call: 行权价 >= ATM + MIN_OTM_POINTS, volume最大
    deep_calls = mdf[(mdf['cp'] == 'C') & (mdf['strike'] >= atm + MIN_OTM_POINTS)]
    deep_puts = mdf[(mdf['cp'] == 'P') & (mdf['strike'] <= atm - MIN_OTM_POINTS)]

    if deep_calls.empty or deep_puts.empty:
        return None

    # 按volume排序选最活跃的
    call_vol = deep_calls.groupby('strike')['volume'].sum()
    put_vol = deep_puts.groupby('strike')['volume'].sum()

    # 过滤掉volume太小的（至少50手/天）
    call_vol = call_vol[call_vol >= 50]
    put_vol = put_vol[put_vol >= 50]

    if call_vol.empty or put_vol.empty:
        return None

    call_strike = call_vol.idxmax()
    put_strike = put_vol.idxmax()

    # 确保期权价格合理（不能太低，至少5点）
    call_sym = f'SHFE.ag{main_month}C{call_strike}'
    put_sym = f'SHFE.ag{main_month}P{put_strike}'

    call_avg = mdf[(mdf['symbol'] == call_sym)]['close'].mean()
    put_avg = mdf[(mdf['symbol'] == put_sym)]['close'].mean()
    if call_avg < 3 or put_avg < 3:
        return None

    return call_sym, put_sym, call_strike, put_strike, main_month, atm


def build_minute_price_sum(day_df, call_sym, put_sym):
    """构建分钟级价格之和序列"""
    call_data = day_df[day_df['symbol'] == call_sym][['datetime', 'close']].rename(
        columns={'close': 'call'}).set_index('datetime')
    put_data = day_df[day_df['symbol'] == put_sym][['datetime', 'close']].rename(
        columns={'close': 'put'}).set_index('datetime')
    merged = call_data.join(put_data, how='inner').sort_index()
    if merged.empty:
        return pd.DataFrame()
    merged['price_sum'] = merged['call'] + merged['put']
    return merged


def simulate_multi_trade_day(minute_data, dte, tp_coeff, sl_threshold, reentry_pct):
    """模拟一天内多次循环交易"""
    if len(minute_data) < 10:
        return []

    trades = []
    in_position = False
    entry_price_sum = 0
    entry_time = None
    entry_call = entry_put = 0
    trade_count = 0
    cooldown_until = None
    tp_target = 0

    idx = minute_data.index
    vals = minute_data[['call', 'put', 'price_sum']].values
    start_time = idx[0]
    end_time = idx[-1]
    close_cutoff = end_time - pd.Timedelta(minutes=5)
    warmup_end = start_time + pd.Timedelta(minutes=5)

    for i in range(len(idx)):
        ts = idx[i]
        c_val, p_val, s_val = vals[i]

        if not in_position:
            if cooldown_until and ts < cooldown_until:
                continue
            if trade_count >= MAX_TRADES_PER_DAY:
                continue
            # 不要在收盘前15分钟入场
            if ts >= end_time - pd.Timedelta(minutes=15):
                continue

            if trade_count == 0:
                if ts <= warmup_end:
                    continue
                should_enter = True
            else:
                should_enter = s_val >= entry_price_sum * reentry_pct

            if should_enter:
                in_position = True
                entry_price_sum = s_val
                entry_time = ts
                entry_call = c_val
                entry_put = p_val
                tp_target = entry_price_sum / dte * tp_coeff
        else:
            profit = entry_price_sum - s_val
            tp_hit = profit >= tp_target

            high_leg = max(c_val, p_val)
            low_leg = min(c_val, p_val)
            leg_ratio = high_leg / low_leg if low_leg > 0 else 999
            sl_hit = leg_ratio >= sl_threshold

            force_close = ts >= close_cutoff

            if tp_hit or sl_hit or force_close:
                reason = 'TP' if tp_hit else ('SL' if sl_hit else 'FC')
                trades.append({
                    'entry_time': str(entry_time),
                    'exit_time': str(ts),
                    'entry_sum': round(entry_price_sum, 2),
                    'exit_sum': round(s_val, 2),
                    'profit': round(profit, 2),
                    'profit_pct': round(profit / entry_price_sum * 100, 2),
                    'hold_min': round((ts - entry_time).total_seconds() / 60, 1),
                    'reason': reason,
                    'entry_call': round(entry_call, 1),
                    'entry_put': round(entry_put, 1),
                    'exit_leg_ratio': round(leg_ratio, 2),
                })
                in_position = False
                trade_count += 1
                cooldown_until = ts + pd.Timedelta(minutes=COOLDOWN_MINUTES)

    return trades


def main():
    print("=" * 70)
    print("白银(AG) 日内多次循环卖出宽跨回测 v2 — 深度虚值版")
    print("=" * 70)
    print(f"数据: {DATA_PATH}")
    print(f"深度虚值门槛: OTM >= {MIN_OTM_POINTS}点")
    print(f"TP系数: {TP_COEFFICIENTS}")
    print(f"SL阈值: {SL_THRESHOLDS}")
    print(f"再入场: {REENTRY_PCTS}")
    print(f"DTE: {DTE_RANGE}")
    print()

    pf = pq.ParquetFile(DATA_PATH)

    # ========== 第1遍：提取所有唯一日期 ==========
    print("Pass 1: 扫描交易日...")
    all_dates = set()
    for batch in pf.iter_batches(batch_size=1000000, columns=['datetime']):
        dates = batch.to_pandas()['datetime'].dt.date.unique()
        all_dates.update(dates)
    all_dates = sorted(all_dates)
    print(f"  共 {len(all_dates)} 个交易日 ({all_dates[0]} ~ {all_dates[-1]})")
    gc.collect()

    # ========== 第2遍：逐天处理 ==========
    print("\nPass 2: 逐天回测...")

    # 预先按天收集数据
    # 为了避免多次遍历整个文件，一次遍历把所有天的数据都收集好
    # 但不全部存在内存中——用生成器式的处理
    # 策略：遍历batches，按天聚合，当一天的数据"确认完整"后立刻处理并释放

    day_buffers = {}  # date -> list of DataFrame chunks
    all_results = defaultdict(list)
    days_processed = 0
    days_valid = 0
    last_seen_date = None
    pair_info = {}  # 记录每天选了什么对

    for batch in pf.iter_batches(batch_size=500000, columns=['datetime', 'symbol', 'close', 'volume']):
        df = batch.to_pandas()
        df['date'] = df['datetime'].dt.date

        for d, grp in df.groupby('date'):
            if d not in day_buffers:
                day_buffers[d] = []
            day_buffers[d].append(grp.drop(columns=['date']))

        # 获取当前batch中最新日期
        current_max_date = df['date'].max()

        # 处理所有"安全完成"的天（比当前最新日期早2天以上的）
        safe_cutoff = current_max_date - timedelta(days=2)
        dates_to_process = [d for d in sorted(day_buffers.keys()) if d <= safe_cutoff]

        for d in dates_to_process:
            chunks = day_buffers.pop(d)
            day_df = pd.concat(chunks, ignore_index=True)
            del chunks

            result = _process_one_day(day_df, d)
            if result is not None:
                for key, trades in result.items():
                    all_results[key].extend(trades)
                days_valid += 1

            days_processed += 1
            if days_processed % 50 == 0:
                print(f"  {days_processed}/{len(all_dates)} 天 (有效{days_valid})...")
                gc.collect()

        del df
        gc.collect()

    # 处理剩余的天
    for d in sorted(day_buffers.keys()):
        chunks = day_buffers.pop(d)
        day_df = pd.concat(chunks, ignore_index=True)
        del chunks

        result = _process_one_day(day_df, d)
        if result is not None:
            for key, trades in result.items():
                all_results[key].extend(trades)
            days_valid += 1
        days_processed += 1

    del day_buffers
    gc.collect()

    print(f"\n处理完成: {days_processed}天扫描, {days_valid}天有效交易")

    # ========== 汇总 ==========
    summarize_and_report(all_results, days_valid, days_processed)


def _process_one_day(day_df, trade_date):
    """处理单天，返回 {(tp, sl, re): [trades]} 或 None"""
    pair = select_deep_otm_strangle(day_df)
    if pair is None:
        return None

    call_sym, put_sym, call_strike, put_strike, yymm, atm = pair
    dte = estimate_dte(yymm, trade_date)
    if dte < DTE_RANGE[0] or dte > DTE_RANGE[1]:
        return None

    minute_data = build_minute_price_sum(day_df, call_sym, put_sym)
    if len(minute_data) < 20:
        return None

    results = {}
    for tp in TP_COEFFICIENTS:
        for sl in SL_THRESHOLDS:
            for re in REENTRY_PCTS:
                trades = simulate_multi_trade_day(minute_data, dte, tp, sl, re)
                if trades:
                    for t in trades:
                        t['date'] = str(trade_date)
                        t['dte'] = dte
                        t['call_sym'] = call_sym
                        t['put_sym'] = put_sym
                        t['atm'] = atm
                        t['call_strike'] = call_strike
                        t['put_strike'] = put_strike
                    results[(tp, sl, re)] = trades
    return results if results else None


def summarize_and_report(all_results, days_valid, days_total):
    """汇总结果 + 生成HTML"""
    rows = []
    for (tp, sl, re), trades in all_results.items():
        if not trades:
            continue
        df = pd.DataFrame(trades)
        n = len(df)
        n_days = df['date'].nunique()
        daily = df.groupby('date').agg(
            cnt=('profit', 'count'),
            pnl=('profit', 'sum'),
        )
        multi_days = (daily['cnt'] > 1).sum()

        rows.append({
            'tp': tp, 'sl': sl, 're': re,
            'trades': n, 'days': n_days,
            'avg_daily': round(daily['cnt'].mean(), 2),
            'multi_days': multi_days,
            'multi_pct': round(multi_days / n_days * 100, 1) if n_days > 0 else 0,
            'wr': round((df['profit'] > 0).mean() * 100, 1),
            'avg_pnl': round(df['profit'].mean(), 2),
            'total_pnl': round(df['profit'].sum(), 1),
            'daily_pnl': round(daily['pnl'].mean(), 2),
            'max_loss': round(daily['pnl'].min(), 1),
            'avg_hold': round(df['hold_min'].mean(), 1),
            'tp_n': (df['reason'] == 'TP').sum(),
            'sl_n': (df['reason'] == 'SL').sum(),
            'fc_n': (df['reason'] == 'FC').sum(),
        })

    if not rows:
        print("没有产生任何交易！")
        return

    sdf = pd.DataFrame(rows).sort_values('total_pnl', ascending=False)

    # ========== 控制台输出 ==========
    print("\n" + "=" * 130)
    print(f"Top 20 参数组合 (共{len(sdf)}组, {days_valid}有效天/{days_total}总天)")
    print("=" * 130)
    for _, r in sdf.head(20).iterrows():
        sl_s = "NoSL" if r['sl'] >= 999 else f"SL{r['sl']}x"
        print(f"  TP{r['tp']}x/{sl_s}/Re{r['re']}x | "
              f"{r['trades']}笔/{r['days']}天 日均{r['avg_daily']}笔 | "
              f"多次天{r['multi_days']}({r['multi_pct']}%) | "
              f"胜率{r['wr']}% | 日均PnL{r['daily_pnl']} 总PnL{r['total_pnl']} | "
              f"最大日亏{r['max_loss']} | {r['avg_hold']}min | "
              f"TP{r['tp_n']}/SL{r['sl_n']}/FC{r['fc_n']}")

    # ========== 关键对比 ==========
    print("\n" + "=" * 130)
    print("关键对比: 多次交易 vs 单次交易")
    print("=" * 130)

    for tp in [0.5, 0.8, 1.0]:
        for sl in [3.0, 999]:
            sl_s = "NoSL" if sl >= 999 else f"SL{sl}x"
            # Reentry 1.1 = 保守（很难再入场，接近单次交易）
            # Reentry 0.9 = 积极（更容易再入场）
            # Reentry 1.0 = 中性（回到原价）
            for re_label, re_val in [("保守1.1x", 1.1), ("中性1.0x", 1.0), ("积极0.9x", 0.9)]:
                key = (tp, sl, re_val)
                trades = all_results.get(key, [])
                if trades:
                    tdf = pd.DataFrame(trades)
                    d = tdf.groupby('date')['profit'].sum()
                    avg_trades = len(tdf) / tdf['date'].nunique()
                    print(f"  TP{tp}x/{sl_s}/{re_label}: "
                          f"{len(tdf)}笔/{tdf['date'].nunique()}天 "
                          f"日均{avg_trades:.1f}笔 "
                          f"总PnL={d.sum():.0f} 日均PnL={d.mean():.1f}")

    # ========== 保存JSON ==========
    out_json = f'{HOME}/Scripts/ag_multi_trade_results.json'
    save = {
        'version': 'v2_deep_otm',
        'min_otm_points': MIN_OTM_POINTS,
        'summary': sdf.to_dict('records'),
        'params': {
            'tp': TP_COEFFICIENTS, 'sl': SL_THRESHOLDS, 're': REENTRY_PCTS,
            'dte': list(DTE_RANGE), 'max_trades': MAX_TRADES_PER_DAY,
            'cooldown': COOLDOWN_MINUTES,
        },
        'days_valid': days_valid, 'days_total': days_total,
    }
    # 只保存top参数的详细trades（避免文件太大）
    top_keys = [(r['tp'], r['sl'], r['re']) for _, r in sdf.head(10).iterrows()]
    details = {}
    for k in top_keys:
        trades = all_results.get(k, [])
        if trades:
            details[f"tp{k[0]}_sl{k[1]}_re{k[2]}"] = trades
    save['top_details'] = details

    with open(out_json, 'w') as f:
        json.dump(save, f, indent=2, default=str)
    print(f"\nJSON: {out_json}")

    # ========== HTML报告 ==========
    generate_html(sdf, all_results, days_valid)


def generate_html(sdf, all_results, days_valid):
    """生成交互式HTML"""
    out = f'{HOME}/Scripts/ag_multi_trade_report.html'
    top20 = sdf.head(20).to_dict('records')

    # 找一个代表性参数的日度分布
    best_key = (sdf.iloc[0]['tp'], sdf.iloc[0]['sl'], sdf.iloc[0]['re'])
    best_trades = all_results.get(best_key, [])
    daily_counts = {}
    daily_pnls = []
    if best_trades:
        bdf = pd.DataFrame(best_trades)
        cnt = bdf.groupby('date').size().value_counts().sort_index()
        daily_counts = {str(k): int(v) for k, v in cnt.items()}
        daily_pnl = bdf.groupby('date')['profit'].sum()
        daily_pnls = [round(v, 1) for v in daily_pnl.values.tolist()]

    # 多次 vs 单次对比数据
    compare_data = []
    for tp in [0.5, 0.8, 1.0]:
        for sl in [999, 3.0]:
            sl_s = "NoSL" if sl >= 999 else f"SL{sl}x"
            label = f"TP{tp}/{sl_s}"
            vals = []
            for re in [1.1, 1.0, 0.9, 0.8]:
                t = all_results.get((tp, sl, re), [])
                if t:
                    tdf = pd.DataFrame(t)
                    vals.append({
                        're': re,
                        'total_pnl': round(tdf['profit'].sum(), 0),
                        'avg_trades': round(len(tdf) / tdf['date'].nunique(), 1),
                    })
            if vals:
                compare_data.append({'label': label, 'vals': vals})

    # ECharts数据
    t20_labels = json.dumps([f"TP{r['tp']}/SL{r['sl']}/Re{r['re']}" for r in top20])
    t20_pnl = json.dumps([r['total_pnl'] for r in top20])
    t20_wr = json.dumps([r['wr'] for r in top20])
    t20_daily = json.dumps([r['avg_daily'] for r in top20])

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>AG 日内多次循环宽跨回测 v2</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
*{{box-sizing:border-box}}
body{{font-family:-apple-system,sans-serif;background:#0a0a1a;color:#e0e0e0;margin:0;padding:20px}}
.hdr{{text-align:center;padding:20px;background:linear-gradient(135deg,#1a1a3e,#2d1b4e);border-radius:12px;margin-bottom:20px}}
.hdr h1{{color:#ffd700;margin:0;font-size:26px}} .hdr p{{color:#aaa;margin:4px 0;font-size:14px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:15px;margin-bottom:20px}}
.card{{background:#1a1a2e;border-radius:10px;padding:18px;border:1px solid #333}}
.card h3{{color:#ffd700;margin:0 0 12px 0;font-size:16px}}
.m{{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #222;font-size:14px}}
.ml{{color:#888}} .mv{{font-weight:bold}}
.pos{{color:#00ff88}} .neg{{color:#ff4444}}
.ch{{width:100%;height:380px}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
th{{background:#2a2a4a;color:#ffd700;padding:8px 6px;text-align:left;position:sticky;top:0}}
td{{padding:6px;border-bottom:1px solid #222}}
tr:hover{{background:#1a1a3e}}
.hl{{background:#2d1b4e!important}}
</style></head><body>

<div class="hdr">
<h1>AG 日内多次循环卖出宽跨回测 v2 — 深度虚值</h1>
<p>SHFE白银期权 1分钟K线 | OTM>={MIN_OTM_POINTS}点 | DTE {DTE_RANGE[0]}-{DTE_RANGE[1]} | 冷却{COOLDOWN_MINUTES}min | {days_valid}有效天</p>
<p>TP=权利金/DTE×系数 | SL=高价腿/低价腿≥阈值 | 再入场=价格之和回到X倍</p>
</div>

<div class="grid">
<div class="card">
<h3>核心发现</h3>
<div class="m"><span class="ml">有效交易天</span><span class="mv">{days_valid}</span></div>
<div class="m"><span class="ml">参数组合</span><span class="mv">{len(sdf)}</span></div>
<div class="m"><span class="ml">最佳日均交易</span><span class="mv">{sdf['avg_daily'].max():.1f}笔</span></div>
<div class="m"><span class="ml">最高多次天占比</span><span class="mv">{sdf['multi_pct'].max():.0f}%</span></div>
<div class="m"><span class="ml">最高胜率</span><span class="mv">{sdf['wr'].max():.1f}%</span></div>
<div class="m"><span class="ml">最高总PnL</span><span class="mv pos">{sdf['total_pnl'].max():.0f}</span></div>
<div class="m"><span class="ml">最佳日均PnL</span><span class="mv pos">{sdf['daily_pnl'].max():.1f}</span></div>
</div>

<div class="card">
<h3>最佳参数-每天交易次数分布</h3>
<div id="cnt_chart" class="ch" style="height:260px"></div>
</div>
</div>

<div class="grid">
<div class="card">
<h3>Top 20 总PnL + 日均交易次数</h3>
<div id="top_chart" class="ch"></div>
</div>
<div class="card">
<h3>再入场阈值对比 (同参数不同reentry)</h3>
<div id="cmp_chart" class="ch"></div>
</div>
</div>

<div class="card" style="margin-bottom:20px">
<h3>最佳参数-日度PnL分布</h3>
<div id="daily_chart" class="ch" style="height:300px"></div>
</div>

<div class="card" style="margin-bottom:20px">
<h3>全部参数结果</h3>
<div style="overflow:auto;max-height:500px">
<table>
<tr><th>TP</th><th>SL</th><th>Re</th><th>笔数</th><th>天</th><th>日均</th>
<th>多次%</th><th>胜率</th><th>日均PnL</th><th>总PnL</th><th>最大日亏</th>
<th>持仓min</th><th>TP/SL/FC</th></tr>"""

    for _, r in sdf.iterrows():
        sl_s = "NoSL" if r['sl'] >= 999 else f"{r['sl']}x"
        pc = "pos" if r['total_pnl'] > 0 else "neg"
        hl = "hl" if r['avg_daily'] > 1.5 else ""
        html += f"""<tr class="{hl}">
<td>{r['tp']}</td><td>{sl_s}</td><td>{r['re']}</td>
<td>{r['trades']}</td><td>{r['days']}</td><td>{r['avg_daily']}</td>
<td>{r['multi_pct']}</td><td>{r['wr']}</td>
<td class="{pc}">{r['daily_pnl']}</td><td class="{pc}">{r['total_pnl']}</td>
<td class="neg">{r['max_loss']}</td><td>{r['avg_hold']}</td>
<td>{r['tp_n']}/{r['sl_n']}/{r['fc_n']}</td></tr>"""

    # 对比数据
    cmp_labels = []
    cmp_series = defaultdict(list)
    for item in compare_data:
        cmp_labels.append(item['label'])
        re_map = {v['re']: v for v in item['vals']}
        for re in [1.1, 1.0, 0.9, 0.8]:
            v = re_map.get(re, {})
            cmp_series[f"Re{re}x"].append(v.get('total_pnl', 0))

    cmp_series_js = []
    colors = ['#888888', '#ffd700', '#00ff88', '#ff6b6b']
    for i, (name, data) in enumerate(cmp_series.items()):
        cmp_series_js.append(f"{{name:'{name}',type:'bar',data:{json.dumps(data)},itemStyle:{{color:'{colors[i]}'}}}}")

    html += f"""</table></div></div>

<script>
// 交易次数分布
var c1=echarts.init(document.getElementById('cnt_chart'));
c1.setOption({{tooltip:{{}},xAxis:{{type:'category',data:{json.dumps(list(daily_counts.keys()))},name:'次/天'}},
yAxis:{{type:'value',name:'天数'}},
series:[{{type:'bar',data:{json.dumps(list(daily_counts.values()))},itemStyle:{{color:'#ffd700'}}}}]}});

// Top20
var c2=echarts.init(document.getElementById('top_chart'));
c2.setOption({{tooltip:{{trigger:'axis'}},legend:{{data:['总PnL','胜率%','日均笔数'],textStyle:{{color:'#aaa'}}}},
xAxis:{{type:'category',data:{t20_labels},axisLabel:{{rotate:45,fontSize:9}}}},
yAxis:[{{type:'value',name:'PnL',axisLabel:{{color:'#00ff88'}}}},
       {{type:'value',name:'%/笔',axisLabel:{{color:'#ffd700'}}}}],
series:[
{{name:'总PnL',type:'bar',data:{t20_pnl},itemStyle:{{color:'#00ff88'}}}},
{{name:'胜率%',type:'line',yAxisIndex:1,data:{t20_wr},lineStyle:{{color:'#ff6b6b'}},itemStyle:{{color:'#ff6b6b'}}}},
{{name:'日均笔数',type:'line',yAxisIndex:1,data:{t20_daily},lineStyle:{{color:'#ffd700'}},itemStyle:{{color:'#ffd700'}}}}
]}});

// 对比
var c3=echarts.init(document.getElementById('cmp_chart'));
c3.setOption({{tooltip:{{trigger:'axis'}},legend:{{textStyle:{{color:'#aaa'}}}},
xAxis:{{type:'category',data:{json.dumps(cmp_labels)},axisLabel:{{rotate:20}}}},
yAxis:{{type:'value',name:'总PnL'}},
series:[{','.join(cmp_series_js)}]}});

// 日度PnL
var c4=echarts.init(document.getElementById('daily_chart'));
c4.setOption({{tooltip:{{}},xAxis:{{type:'category',data:Array.from({{length:{len(daily_pnls)}}},(_,i)=>i+1),name:'第N天'}},
yAxis:{{type:'value',name:'日PnL'}},
visualMap:{{show:false,dimension:1,pieces:[{{lt:0,color:'#ff4444'}},{{gte:0,color:'#00ff88'}}]}},
series:[{{type:'bar',data:{json.dumps(daily_pnls)}}}]}});

window.addEventListener('resize',()=>{{c1.resize();c2.resize();c3.resize();c4.resize();}});
</script></body></html>"""

    with open(out, 'w') as f:
        f.write(html)
    print(f"HTML: {out}")


if __name__ == '__main__':
    main()
