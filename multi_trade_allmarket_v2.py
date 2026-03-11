"""
全市场日内多次循环卖出宽跨回测 v2 — 加入DTE分桶

v1: OTM% × TP × SL × Re = 108组/品种, DTE固定15-60
v2: OTM% × TP × SL × Re × DTE桶 = 432组/品种, DTE作为搜索维度

DTE桶: (0,7], (7,15], (15,30], (30,60]
预期: 不同品种最优DTE不同, B037已证明DTE是关键隐藏维度

用法: python3 multi_trade_allmarket_v2.py [品种列表]
  python3 multi_trade_allmarket_v2.py              # 跑全部
  python3 multi_trade_allmarket_v2.py CF cu pg     # 只跑指定品种
"""

import os, sys, gc, json, re, warnings, traceback
from collections import defaultdict, Counter
from datetime import timedelta, date

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

warnings.filterwarnings('ignore')

HOME = os.path.expanduser('~')
BASE = f'{HOME}/Downloads/期权_parquet'
OUT_DIR = f'{HOME}/Scripts/multi_trade_results_v2'
os.makedirs(OUT_DIR, exist_ok=True)

# ========== 网格参数 ==========
OTM_PCTS = [0.03, 0.05, 0.07]
TP_COEFFS = [1.0, 1.5, 2.0, 3.0]
SL_THRESHOLDS = [3.0, 5.0, 999]
REENTRY_PCTS = [0.9, 1.0, 1.1]
DTE_BUCKETS = [(1, 7), (7, 15), (15, 30), (30, 60)]  # v2新增
MAX_TRADES = 10
COOLDOWN_MIN = 10
MIN_PROFIT_TICKS = 5

EXPIRY_OFFSETS = {
    'CZCE': 25, 'DCE': 25, 'SHFE': 25, 'INE': 25, 'GFEX': 25, 'CFFEX': 20,
}

# ========== Symbol解析 ==========
SYMBOL_PATTERNS = {
    'CZCE': re.compile(r'CZCE\.(\w+?)(\d{3,4})([CP])(\d+)'),
    'DCE': re.compile(r'DCE\.(\w+?)(\d{4})-([CP])-(\d+)'),
    'SHFE': re.compile(r'SHFE\.(\w+?)(\d{4})([CP])(\d+)'),
    'INE': re.compile(r'INE\.(\w+?)(\d{4})([CP])(\d+)'),
    'GFEX': re.compile(r'GFEX\.(\w+?)(\d{4})-([CP])-(\d+)'),
}


def parse_symbol(sym, exchange):
    pat = SYMBOL_PATTERNS.get(exchange)
    if not pat:
        return None
    m = pat.match(sym)
    if not m:
        return None
    return m.group(2), m.group(3), int(m.group(4))


def make_symbol(exchange, product, yymm, cp, strike):
    if exchange == 'CZCE':
        return f'{exchange}.{product}{yymm}{cp}{strike}'
    elif exchange in ('DCE', 'GFEX'):
        return f'{exchange}.{product}{yymm}-{cp}-{strike}'
    else:
        return f'{exchange}.{product}{yymm}{cp}{strike}'


def estimate_dte(yymm, trade_date, exchange):
    yy = int(yymm[:2]) if len(yymm) >= 2 else 0
    if len(yymm) == 3:
        yy = int(yymm[0])
        mm = int(yymm[1:])
        yy = yy + 2020 if yy <= 5 else yy + 2010
    else:
        mm = int(yymm[2:])
        yy = 2000 + yy

    em = mm - 1
    ey = yy
    if em <= 0:
        em, ey = 12, ey - 1
    day = EXPIRY_OFFSETS.get(exchange, 25)
    try:
        exp = pd.Timestamp(ey, em, day).date()
    except:
        exp = pd.Timestamp(ey, em, 20).date()
    return max((exp - trade_date).days, 1)


def get_dte_bucket(dte):
    """返回DTE所属桶, 或None(不在任何桶)"""
    for lo, hi in DTE_BUCKETS:
        if lo < dte <= hi:
            return (lo, hi)
    return None


def detect_strike_step(strikes):
    s = sorted(set(strikes))
    if len(s) < 3:
        return s[1] - s[0] if len(s) == 2 else 100
    diffs = [s[i+1] - s[i] for i in range(len(s)-1)]
    cnt = Counter(diffs)
    return cnt.most_common(1)[0][0]


def parse_day_once(day_df, exchange):
    """解析一次symbol，返回带yymm/cp/strike列的df（性能关键）"""
    syms = day_df['symbol'].unique()
    sym_map = {}
    pat = SYMBOL_PATTERNS.get(exchange)
    if not pat:
        return None
    for s in syms:
        m = pat.match(s)
        if m:
            sym_map[s] = (m.group(2), m.group(3), int(m.group(4)))
    if not sym_map:
        return None
    day_df = day_df[day_df['symbol'].isin(sym_map)].copy()
    day_df['yymm'] = day_df['symbol'].map(lambda s: sym_map[s][0])
    day_df['cp'] = day_df['symbol'].map(lambda s: sym_map[s][1])
    day_df['strike'] = day_df['symbol'].map(lambda s: sym_map[s][2])
    return day_df


def select_all_pairs(parsed_df, exchange, product, trade_date):
    """
    一次性返回所有月份×OTM组合的对，以及DTE信息。
    返回: [(otm_pct, dte, bucket, call_sym, put_sym, yymm, mdata_key), ...]
    """
    vol_by_m = parsed_df.groupby('yymm')['volume'].sum()
    if vol_by_m.empty:
        return []

    all_pairs = []
    for yymm in vol_by_m.index:
        mdf = parsed_df[parsed_df['yymm'] == yymm]
        if mdf['volume'].sum() < 50:
            continue

        dte = estimate_dte(yymm, trade_date, exchange)
        bucket = get_dte_bucket(dte)
        if bucket is None:
            continue

        # ATM
        first_t = mdf['datetime'].min()
        first = mdf[mdf['datetime'] == first_t]
        cp = first[first['cp'] == 'C'].set_index('strike')['close']
        pp = first[first['cp'] == 'P'].set_index('strike')['close']
        common = sorted(set(cp.index) & set(pp.index))
        if len(common) < 3:
            continue
        atm = min(common, key=lambda k: abs(cp.get(k, 9999) - pp.get(k, 9999)))
        step = detect_strike_step(common)

        for otm_pct in OTM_PCTS:
            target_otm = atm * otm_pct
            n_steps = max(1, round(target_otm / step))
            call_k = atm + n_steps * step
            put_k = atm - n_steps * step

            call_sym = make_symbol(exchange, product, yymm, 'C', call_k)
            put_sym = make_symbol(exchange, product, yymm, 'P', put_k)

            c_data = mdf[mdf['symbol'] == call_sym]
            p_data = mdf[mdf['symbol'] == put_sym]
            if c_data.empty or p_data.empty:
                continue
            if c_data['volume'].sum() < 10 or p_data['volume'].sum() < 10:
                continue
            c_avg = c_data['close'].mean()
            p_avg = p_data['close'].mean()
            if c_avg < 1 or p_avg < 1:
                continue

            all_pairs.append((otm_pct, dte, bucket, call_sym, put_sym, yymm))

    return all_pairs


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


def simulate_day(mdata, dte, tp_coeff, sl_thresh, reentry_pct, tick_size):
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
    no_entry = end_t - pd.Timedelta(minutes=20)
    warm = idx[0] + pd.Timedelta(minutes=5)

    for i in range(len(idx)):
        ts = idx[i]
        cv, pv, sv = vals[i]

        if not in_pos:
            if cooldown and ts < cooldown:
                continue
            if count >= MAX_TRADES or ts >= no_entry:
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
                ticks = profit / tick_size if tick_size > 0 else profit
                trades.append({
                    'profit': round(profit, 2),
                    'ticks': round(ticks, 1),
                    'hold_min': round((ts - entry_time).total_seconds() / 60, 1),
                    'reason': reason,
                    'entry_sum': round(entry_sum, 2),
                })
                in_pos = False
                count += 1
                cooldown = ts + pd.Timedelta(minutes=COOLDOWN_MIN)
    return trades


def detect_tick_size(day_df):
    prices = day_df['close'].dropna().unique()
    if len(prices) < 10:
        return 1
    prices = sorted(prices)
    diffs = [round(prices[i+1] - prices[i], 6) for i in range(min(200, len(prices)-1))]
    diffs = [d for d in diffs if d > 0]
    if not diffs:
        return 1
    cnt = Counter([round(d, 4) for d in diffs])
    return cnt.most_common(1)[0][0]


def process_day(day_df, trade_date, exchange, product, tick_size, results):
    """v2: 解析一次symbol, 遍历所有月份×OTM, DTE作为分桶维度"""
    parsed_df = parse_day_once(day_df, exchange)
    if parsed_df is None or parsed_df.empty:
        return 0

    all_pairs = select_all_pairs(parsed_df, exchange, product, trade_date)
    if not all_pairs:
        return 0

    # 缓存mdata避免同一(call,put)重复构建
    mdata_cache = {}
    valid = 0

    for otm_pct, dte, bucket, call_sym, put_sym, yymm in all_pairs:
        cache_key = (call_sym, put_sym)
        if cache_key not in mdata_cache:
            mdata_cache[cache_key] = build_minute_sum(day_df, call_sym, put_sym)
        mdata = mdata_cache[cache_key]
        if len(mdata) < 20:
            continue

        entry_est = mdata['psum'].iloc[:5].mean()

        for tp in TP_COEFFS:
            tp_target = entry_est / dte * tp
            if tp_target / tick_size < MIN_PROFIT_TICKS:
                continue

            for sl in SL_THRESHOLDS:
                for re_pct in REENTRY_PCTS:
                    trades = simulate_day(mdata, dte, tp, sl, re_pct, tick_size)
                    if trades:
                        for t in trades:
                            t['date'] = str(trade_date)
                            t['otm_pct'] = otm_pct
                            t['dte'] = dte
                            t['dte_bucket'] = f"{bucket[0]}-{bucket[1]}"
                        results[(otm_pct, tp, sl, re_pct, bucket)].extend(trades)
        valid = 1
    return valid


def summarize_product(results, exchange, product, tick_size, n_valid):
    rows = []
    for (otm_pct, tp, sl, re, dte_bkt), trades in results.items():
        if not trades:
            continue
        df = pd.DataFrame(trades)
        n = len(df)
        nd = df['date'].nunique()
        daily = df.groupby('date').agg(cnt=('profit', 'count'), pnl=('profit', 'sum'))
        multi = (daily['cnt'] > 1).sum()
        wins = df[df['profit'] > 0]
        win_ticks = wins['ticks'].values if len(wins) > 0 else [0]
        pcts = np.percentile(win_ticks, [25, 50, 75]) if len(win_ticks) > 2 else [0, 0, 0]

        filt = df[(df['ticks'] > 3) | (df['profit'] <= 0)]
        f_pnl = filt['profit'].sum()
        raw_pnl = df['profit'].sum()

        rows.append({
            'otm_pct': otm_pct, 'tp': tp, 'sl': sl, 're': re,
            'dte_lo': dte_bkt[0], 'dte_hi': dte_bkt[1],
            'dte_bucket': f"{dte_bkt[0]}-{dte_bkt[1]}",
            'trades': n, 'days': nd,
            'avg_daily': round(daily['cnt'].mean(), 2),
            'multi_pct': round(multi / nd * 100, 1) if nd > 0 else 0,
            'wr': round((df['profit'] > 0).mean() * 100, 1),
            'total_pnl': round(raw_pnl, 1),
            'daily_pnl': round(daily['pnl'].mean(), 2),
            'max_loss': round(daily['pnl'].min(), 1),
            'tick_p25': round(pcts[0], 1),
            'tick_p50': round(pcts[1], 1),
            'tick_p75': round(pcts[2], 1),
            'pnl_filtered': round(f_pnl, 1),
            'pnl_retain': round(f_pnl / raw_pnl * 100, 0) if raw_pnl != 0 else 0,
            'tp_n': int((df['reason'] == 'TP').sum()),
            'sl_n': int((df['reason'] == 'SL').sum()),
            'fc_n': int((df['reason'] == 'FC').sum()),
        })

    if not rows:
        return None

    sdf = pd.DataFrame(rows).sort_values('pnl_filtered', ascending=False)

    # 最优DTE桶对比
    print(f"  DTE桶对比 (最优参数下):")
    best = sdf.iloc[0]
    same_params = sdf[(sdf['otm_pct'] == best['otm_pct']) &
                      (sdf['tp'] == best['tp']) &
                      (sdf['sl'] == best['sl']) &
                      (sdf['re'] == best['re'])]
    if len(same_params) > 0:
        for _, r in same_params.iterrows():
            print(f"    DTE {r['dte_bucket']:>5}: {r['trades']}笔/{r['days']}天 "
                  f"WR{r['wr']}% P50={r['tick_p50']}tick 过滤PnL={r['pnl_filtered']}")

    # 各DTE桶的最优
    print(f"  各DTE桶最优:")
    for bkt in DTE_BUCKETS:
        bkt_rows = sdf[(sdf['dte_lo'] == bkt[0]) & (sdf['dte_hi'] == bkt[1])]
        if bkt_rows.empty:
            print(f"    DTE {bkt[0]:>2}-{bkt[1]:>2}: 无数据")
            continue
        b = bkt_rows.iloc[0]
        sl_s = "NoSL" if b['sl'] >= 999 else f"SL{b['sl']}x"
        print(f"    DTE {bkt[0]:>2}-{bkt[1]:>2}: OTM{b['otm_pct']*100:.0f}%/TP{b['tp']}/{sl_s}/Re{b['re']} "
              f"| {b['trades']}笔 WR{b['wr']}% P50={b['tick_p50']}tick PnL={b['pnl_filtered']}")

    # 全局Top 5
    print(f"  全局Top 5:")
    for _, r in sdf.head(5).iterrows():
        sl_s = "NoSL" if r['sl'] >= 999 else f"SL{r['sl']}x"
        print(f"    DTE{r['dte_bucket']}/OTM{r['otm_pct']*100:.0f}%/TP{r['tp']}/{sl_s}/Re{r['re']} | "
              f"{r['trades']}笔/{r['days']}天 日均{r['avg_daily']}笔 | "
              f"WR{r['wr']}% P50={r['tick_p50']}tick | 过滤PnL={r['pnl_filtered']} (保留{r['pnl_retain']}%)")

    # 按DTE桶汇总最优
    dte_bests = {}
    for bkt in DTE_BUCKETS:
        bkt_rows = sdf[(sdf['dte_lo'] == bkt[0]) & (sdf['dte_hi'] == bkt[1])]
        if not bkt_rows.empty:
            dte_bests[f"{bkt[0]}-{bkt[1]}"] = bkt_rows.iloc[0].to_dict()

    return {
        'exchange': exchange,
        'product': product,
        'tick_size': tick_size,
        'valid_days': n_valid,
        'summary': sdf.to_dict('records'),
        'best': sdf.iloc[0].to_dict() if len(sdf) > 0 else {},
        'dte_bests': dte_bests,  # v2新增: 各DTE桶最优
    }


def run_product(exchange, product, path):
    print(f"\n{'='*60}")
    print(f"  {exchange}.{product}")
    print(f"{'='*60}")

    try:
        pf = pq.ParquetFile(path)
    except Exception as e:
        print(f"  文件损坏: {e}")
        return None

    # 一次性读取4列（内存可控：通常<1GB），避免流式处理的日期重复bug
    print(f"  读取数据...", end="", flush=True)
    chunks = []
    for batch in pf.iter_batches(batch_size=1000000, columns=['datetime', 'symbol', 'close', 'volume']):
        chunks.append(batch.to_pandas())
    all_df = pd.concat(chunks, ignore_index=True)
    del chunks
    gc.collect()
    all_df['date'] = all_df['datetime'].dt.date
    dates = sorted(all_df['date'].unique())
    print(f" {len(all_df)}行 / {len(dates)}天 ({dates[0]}~{dates[-1]})")

    tick_size = detect_tick_size(all_df.head(5000))
    print(f"  tick_size={tick_size}")

    results = defaultdict(list)
    n_proc = n_valid = 0
    # groupby一次，避免重复过滤
    grouped = all_df.groupby('date')
    del all_df
    gc.collect()

    for d, day_df in grouped:
        day_df = day_df.drop(columns=['date'])
        v = process_day(day_df, d, exchange, product, tick_size, results)
        n_valid += v
        n_proc += 1
        if n_proc % 200 == 0:
            print(f"  {n_proc}/{len(dates)} (有效{n_valid})...")
    del grouped
    gc.collect()

    print(f"  完成: {n_proc}天/{n_valid}有效")

    if not results:
        print(f"  无交易结果")
        return None

    summary = summarize_product(results, exchange, product, tick_size, n_valid)
    return summary


def main():
    print("=" * 60)
    print("全市场日内多次循环宽跨回测 v2 — DTE分桶")
    print(f"OTM%: {[f'{x*100:.0f}%' for x in OTM_PCTS]}")
    print(f"TP: {TP_COEFFS} | SL: {SL_THRESHOLDS}")
    print(f"Re: {REENTRY_PCTS}")
    print(f"DTE桶: {DTE_BUCKETS}")
    print(f"网格: {len(OTM_PCTS)}×{len(TP_COEFFS)}×{len(SL_THRESHOLDS)}×{len(REENTRY_PCTS)}×{len(DTE_BUCKETS)} "
          f"= {len(OTM_PCTS)*len(TP_COEFFS)*len(SL_THRESHOLDS)*len(REENTRY_PCTS)*len(DTE_BUCKETS)}组/品种")
    print("=" * 60)

    all_products = []
    for exchange in ['CZCE', 'DCE', 'SHFE', 'INE', 'GFEX']:
        edir = f'{BASE}/{exchange}'
        if not os.path.isdir(edir):
            continue
        for f in sorted(os.listdir(edir)):
            if not f.endswith('.parquet'):
                continue
            prod = f.replace('.parquet', '')
            path = f'{edir}/{f}'
            out_file = f'{OUT_DIR}/{exchange}_{prod}.json'
            if os.path.exists(out_file):
                print(f"  跳过 {exchange}.{prod} (已完成)")
                continue
            all_products.append((exchange, prod, path))

    if len(sys.argv) > 1:
        targets = [a for a in sys.argv[1:]]
        # 支持大小写匹配
        all_products = [(e, p, path) for e, p, path in all_products
                        if p in targets or p.upper() in [t.upper() for t in targets]
                        or p.lower() in [t.lower() for t in targets]]
        # 强制重跑指定品种（删除已有结果）
        for e, p, path in all_products:
            out_file = f'{OUT_DIR}/{e}_{p}.json'
            if os.path.exists(out_file):
                os.remove(out_file)
                print(f"  删除旧结果: {out_file}")

    print(f"\n待回测: {len(all_products)}个品种")
    all_summaries = []

    for i, (exchange, product, path) in enumerate(all_products):
        print(f"\n[{i+1}/{len(all_products)}]", end="")
        try:
            result = run_product(exchange, product, path)
            if result:
                out_file = f'{OUT_DIR}/{exchange}_{product}.json'
                with open(out_file, 'w') as f:
                    json.dump(result, f, indent=2, default=str)
                all_summaries.append(result)
        except Exception as e:
            print(f"  ERROR: {e}")
            traceback.print_exc()
        gc.collect()

    # 汇总
    if all_summaries:
        generate_summary(all_summaries)


def generate_summary(all_summaries):
    """生成全市场汇总"""
    print("\n" + "=" * 80)
    print("全市场汇总 v2 (DTE分桶)")
    print("=" * 80)

    rankings = []
    for s in all_summaries:
        b = s.get('best', {})
        if not b:
            continue
        rankings.append({
            'product': f"{s['exchange']}.{s['product']}",
            'exchange': s['exchange'],
            'prod': s['product'],
            'tick_size': s['tick_size'],
            'valid_days': s['valid_days'],
            'best_otm': b.get('otm_pct', 0),
            'best_tp': b.get('tp', 0),
            'best_sl': b.get('sl', 999),
            'best_re': b.get('re', 1.0),
            'best_dte': b.get('dte_bucket', '?'),
            'pnl_filtered': b.get('pnl_filtered', 0),
            'pnl_retain': b.get('pnl_retain', 0),
            'wr': b.get('wr', 0),
            'avg_daily': b.get('avg_daily', 0),
            'tick_p50': b.get('tick_p50', 0),
            'total_trades': b.get('trades', 0),
            'multi_pct': b.get('multi_pct', 0),
            'dte_bests': s.get('dte_bests', {}),
        })

    rankings.sort(key=lambda x: x['pnl_filtered'], reverse=True)

    # Tier分级
    tier1 = [r for r in rankings if r['pnl_filtered'] >= 2000]
    tier2 = [r for r in rankings if 500 <= r['pnl_filtered'] < 2000]
    tier3 = [r for r in rankings if r['pnl_filtered'] < 500]

    print(f"\nTier 1 ({len(tier1)}个): {[r['product'] for r in tier1]}")
    print(f"Tier 2 ({len(tier2)}个): {[r['product'] for r in tier2]}")
    print(f"Tier 3 ({len(tier3)}个): {[r['product'] for r in tier3]}")

    print(f"\n{'品种':>12} {'DTE桶':>6} {'参数':>25} {'过滤PnL':>8} {'WR%':>5} {'日均':>5} {'P50t':>5} {'保留%':>5}")
    print("-" * 85)
    for r in rankings[:30]:
        sl_s = "No" if r['best_sl'] >= 999 else f"{r['best_sl']}"
        params = f"OTM{r['best_otm']*100:.0f}%/TP{r['best_tp']}/SL{sl_s}/Re{r['best_re']}"
        print(f"{r['product']:>12} {r['best_dte']:>6} {params:>25} "
              f"{r['pnl_filtered']:>+8.0f} {r['wr']:>5.1f} {r['avg_daily']:>5.2f} "
              f"{r['tick_p50']:>5.1f} {r['pnl_retain']:>5.0f}")

    # DTE桶统计
    print(f"\nDTE桶最优分布:")
    dte_cnt = Counter()
    for r in rankings:
        dte_cnt[r['best_dte']] += 1
    for k, v in dte_cnt.most_common():
        print(f"  DTE {k}: {v}个品种 ({v/len(rankings)*100:.0f}%)")

    # v1 vs v2 对比（如果v1结果存在）
    v1_dir = f'{HOME}/Scripts/multi_trade_results'
    v1_sum = f'{v1_dir}/_ALL_SUMMARY.json'
    if os.path.exists(v1_sum):
        with open(v1_sum) as f:
            v1 = json.load(f)
        v1_map = {r['prod']: r['pnl_filtered'] for r in v1.get('all_rankings', [])}
        print(f"\nv1 vs v2 对比:")
        improved = 0
        for r in rankings:
            v1_pnl = v1_map.get(r['prod'], 0)
            v2_pnl = r['pnl_filtered']
            if v1_pnl > 0:
                chg = (v2_pnl - v1_pnl) / v1_pnl * 100
                if chg > 5:
                    improved += 1
                    print(f"  {r['prod']:>6}: v1={v1_pnl:.0f} → v2={v2_pnl:.0f} ({chg:+.0f}%)")
        print(f"  改善品种: {improved}/{len(v1_map)}")

    summary_file = f'{OUT_DIR}/_ALL_SUMMARY.json'
    with open(summary_file, 'w') as f:
        json.dump({
            'date': str(date.today()),
            'total_products': len(rankings),
            'tier1': [r['product'] for r in tier1],
            'tier2': [r['product'] for r in tier2],
            'all_rankings': rankings,
            'params': {
                'otm_pcts': OTM_PCTS, 'tp': TP_COEFFS,
                'sl': SL_THRESHOLDS, 're': REENTRY_PCTS,
                'dte_buckets': DTE_BUCKETS,
                'cooldown': COOLDOWN_MIN, 'min_ticks': MIN_PROFIT_TICKS,
            },
        }, f, indent=2, ensure_ascii=False, default=str)
    print(f"\n汇总: {summary_file}")


if __name__ == '__main__':
    main()
