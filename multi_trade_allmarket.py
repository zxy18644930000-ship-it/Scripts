"""
全市场日内多次循环卖出宽跨回测 — 浅虚值 + tick验证

逐品种跑，每个品种跑完保存结果+释放内存。
OTM用百分比虚值度（适配不同品种价格）。
自动适配 CZCE/DCE/SHFE/INE/GFEX/CFFEX symbol格式。

用法: python3 multi_trade_allmarket.py [品种列表]
  python3 multi_trade_allmarket.py              # 跑全部
  python3 multi_trade_allmarket.py CF SA MA     # 只跑指定品种
"""

import os, sys, gc, json, re, warnings, traceback
from collections import defaultdict
from datetime import timedelta, date

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

warnings.filterwarnings('ignore')

HOME = os.path.expanduser('~')
BASE = f'{HOME}/Downloads/期权_parquet'
OUT_DIR = f'{HOME}/Scripts/multi_trade_results'
os.makedirs(OUT_DIR, exist_ok=True)

# ========== 网格参数 ==========
OTM_PCTS = [0.03, 0.05, 0.07]       # 虚值度百分比
TP_COEFFS = [1.0, 1.5, 2.0, 3.0]
SL_THRESHOLDS = [3.0, 5.0, 999]
REENTRY_PCTS = [0.9, 1.0, 1.1]
DTE_RANGE = (15, 60)
MAX_TRADES = 10
COOLDOWN_MIN = 10
MIN_PROFIT_TICKS = 5  # 验证用

# 各交易所到期日估算（月份-1的第N天或倒数第N天）
# 简化为25号
EXPIRY_OFFSETS = {
    'CZCE': 25, 'DCE': 25, 'SHFE': 25, 'INE': 25, 'GFEX': 25, 'CFFEX': 20,
}

# ========== Symbol解析 ==========
# CZCE: CZCE.CF011C13000 (yymm无分隔)
# DCE:  DCE.m2011-C-2600 (有分隔符)
# SHFE: SHFE.ag2303C4100 (无分隔)
# INE:  INE.sc2109C390
# GFEX: GFEX.lc2401-C-188000 (有分隔)
# CFFEX: 跳过（股指期权结构不同）

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
    return m.group(2), m.group(3), int(m.group(4))  # yymm, CP, strike


def make_symbol(exchange, product, yymm, cp, strike):
    """构造symbol字符串"""
    if exchange == 'CZCE':
        return f'{exchange}.{product}{yymm}{cp}{strike}'
    elif exchange in ('DCE', 'GFEX'):
        return f'{exchange}.{product}{yymm}-{cp}-{strike}'
    else:  # SHFE, INE
        return f'{exchange}.{product}{yymm}{cp}{strike}'


def estimate_dte(yymm, trade_date, exchange):
    yy, mm = int(yymm[:2]), int(yymm[2:]) if len(yymm) == 4 else int(yymm[1:])
    # CZCE用3位yymm(如011=2001年1月或2021年1月)，其他用4位
    if len(yymm) == 3:
        yy = int(yymm[0])
        mm = int(yymm[1:])
        # 推断世纪
        if yy <= 5:
            yy += 2020
        else:
            yy += 2010
    else:
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


def detect_strike_step(strikes):
    """自动检测行权价间距"""
    s = sorted(set(strikes))
    if len(s) < 3:
        return s[1] - s[0] if len(s) == 2 else 100
    diffs = [s[i+1] - s[i] for i in range(len(s)-1)]
    # 取最常见的间距
    from collections import Counter
    cnt = Counter(diffs)
    return cnt.most_common(1)[0][0]


def select_pair(day_df, exchange, product, otm_pct):
    """
    选浅虚值宽跨对（百分比虚值度）
    """
    day_df = day_df.copy()
    parsed = day_df['symbol'].apply(lambda s: parse_symbol(s, exchange))
    day_df['yymm'] = parsed.apply(lambda x: x[0] if x else None)
    day_df['cp'] = parsed.apply(lambda x: x[1] if x else None)
    day_df['strike'] = parsed.apply(lambda x: x[2] if x else None)
    day_df = day_df.dropna(subset=['yymm'])
    day_df['strike'] = day_df['strike'].astype(int)

    # 主力月份
    vol_by_m = day_df.groupby('yymm')['volume'].sum()
    if vol_by_m.empty:
        return None
    main_m = vol_by_m.idxmax()
    mdf = day_df[day_df['yymm'] == main_m]

    # ATM
    first_t = mdf['datetime'].min()
    first = mdf[mdf['datetime'] == first_t]
    cp = first[first['cp'] == 'C'].set_index('strike')['close']
    pp = first[first['cp'] == 'P'].set_index('strike')['close']
    common = sorted(set(cp.index) & set(pp.index))
    if len(common) < 3:
        return None
    atm = min(common, key=lambda k: abs(cp.get(k, 9999) - pp.get(k, 9999)))

    # 行权价间距
    step = detect_strike_step(common)

    # 目标虚值度
    target_otm = atm * otm_pct
    # 找最接近target_otm的行权价档位
    n_steps = max(1, round(target_otm / step))
    call_k = atm + n_steps * step
    put_k = atm - n_steps * step

    call_sym = make_symbol(exchange, product, main_m, 'C', call_k)
    put_sym = make_symbol(exchange, product, main_m, 'P', put_k)

    c_data = mdf[mdf['symbol'] == call_sym]
    p_data = mdf[mdf['symbol'] == put_sym]
    if c_data.empty or p_data.empty:
        return None
    if c_data['volume'].sum() < 10 or p_data['volume'].sum() < 10:
        return None
    c_avg = c_data['close'].mean()
    p_avg = p_data['close'].mean()
    if c_avg < 1 or p_avg < 1:
        return None

    return call_sym, put_sym, call_k, put_k, main_m, atm, c_avg + p_avg


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
    """从价格数据推断tick size"""
    prices = day_df['close'].dropna().unique()
    if len(prices) < 10:
        return 1
    prices = sorted(prices)
    diffs = [round(prices[i+1] - prices[i], 6) for i in range(min(200, len(prices)-1))]
    diffs = [d for d in diffs if d > 0]
    if not diffs:
        return 1
    from collections import Counter
    cnt = Counter([round(d, 4) for d in diffs])
    return cnt.most_common(1)[0][0]


def run_product(exchange, product, path):
    """回测单个品种"""
    print(f"\n{'='*60}")
    print(f"  {exchange}.{product}")
    print(f"{'='*60}")

    try:
        pf = pq.ParquetFile(path)
    except Exception as e:
        print(f"  文件损坏: {e}")
        return None

    # Pass 1: 日期
    dates = set()
    for b in pf.iter_batches(batch_size=1000000, columns=['datetime']):
        dates.update(b.to_pandas()['datetime'].dt.date.unique())
    dates = sorted(dates)
    print(f"  {len(dates)}交易日 ({dates[0]}~{dates[-1]})")
    gc.collect()

    # 检测tick size
    sample_batch = next(pf.iter_batches(batch_size=5000, columns=['close']))
    tick_size = detect_tick_size(sample_batch.to_pandas())
    print(f"  tick_size={tick_size}")

    # Pass 2: 逐天
    bufs = {}
    results = defaultdict(list)
    n_proc = n_valid = 0

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
            v = process_day(day_df, d, exchange, product, tick_size, results)
            n_valid += v
            n_proc += 1
            if n_proc % 100 == 0:
                print(f"  {n_proc}/{len(dates)} (有效{n_valid})...")
                gc.collect()
        del df
        gc.collect()

    for d in sorted(bufs.keys()):
        day_df = pd.concat(bufs.pop(d), ignore_index=True)
        n_valid += process_day(day_df, d, exchange, product, tick_size, results)
        n_proc += 1
    del bufs
    gc.collect()

    print(f"  完成: {n_proc}天/{n_valid}有效")

    if not results:
        print(f"  无交易结果")
        return None

    # 汇总
    summary = summarize_product(results, exchange, product, tick_size, n_valid)
    return summary


def process_day(day_df, trade_date, exchange, product, tick_size, results):
    valid = 0
    for otm_pct in OTM_PCTS:
        pair = select_pair(day_df, exchange, product, otm_pct)
        if pair is None:
            continue
        call_sym, put_sym, ck, pk, yymm, atm, avg_sum = pair
        dte = estimate_dte(yymm, trade_date, exchange)
        if dte < DTE_RANGE[0] or dte > DTE_RANGE[1]:
            continue

        mdata = build_minute_sum(day_df, call_sym, put_sym)
        if len(mdata) < 20:
            continue

        for tp in TP_COEFFS:
            # 预检：止盈目标>=5tick
            entry_est = mdata['psum'].iloc[:5].mean()
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
                        results[(otm_pct, tp, sl, re_pct)].extend(trades)
        valid = 1
    return valid


def summarize_product(results, exchange, product, tick_size, n_valid):
    rows = []
    for (otm_pct, tp, sl, re), trades in results.items():
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

        # 过滤<=3tick
        filt = df[(df['ticks'] > 3) | (df['profit'] <= 0)]
        f_pnl = filt['profit'].sum()
        raw_pnl = df['profit'].sum()

        rows.append({
            'otm_pct': otm_pct, 'tp': tp, 'sl': sl, 're': re,
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
            'tp_n': (df['reason'] == 'TP').sum(),
            'sl_n': (df['reason'] == 'SL').sum(),
            'fc_n': (df['reason'] == 'FC').sum(),
        })

    sdf = pd.DataFrame(rows).sort_values('pnl_filtered', ascending=False)

    # 打印top 5
    print(f"  Top 5 (过滤后PnL):")
    for _, r in sdf.head(5).iterrows():
        sl_s = "NoSL" if r['sl'] >= 999 else f"SL{r['sl']}x"
        print(f"    OTM{r['otm_pct']*100:.0f}%/TP{r['tp']}/{sl_s}/Re{r['re']} | "
              f"{r['trades']}笔/{r['days']}天 日均{r['avg_daily']}笔 | "
              f"WR{r['wr']}% | P50={r['tick_p50']}tick | "
              f"过滤PnL={r['pnl_filtered']} (保留{r['pnl_retain']}%)")

    # 多次vs单次
    best_otm = sdf.iloc[0]['otm_pct'] if len(sdf) > 0 else 0.05
    best_tp = sdf.iloc[0]['tp'] if len(sdf) > 0 else 1.5
    best_sl = sdf.iloc[0]['sl'] if len(sdf) > 0 else 999
    print(f"  多次vs单次 (最优OTM/TP/SL):")
    for re_val, label in [(1.1, "保守"), (1.0, "中性"), (0.9, "积极")]:
        key = (best_otm, best_tp, best_sl, re_val)
        t = results.get(key, [])
        if t:
            tdf = pd.DataFrame(t)
            filt = tdf[(tdf['ticks'] > 3) | (tdf['profit'] <= 0)]
            nd = tdf['date'].nunique()
            print(f"    {label}(Re{re_val}): {len(tdf)/nd:.1f}笔/天 过滤PnL={filt['profit'].sum():.0f}")

    return {
        'exchange': exchange,
        'product': product,
        'tick_size': tick_size,
        'valid_days': n_valid,
        'summary': sdf.to_dict('records'),
        'best': sdf.iloc[0].to_dict() if len(sdf) > 0 else {},
    }


def main():
    print("=" * 60)
    print("全市场日内多次循环宽跨回测")
    print(f"OTM%: {[f'{x*100:.0f}%' for x in OTM_PCTS]}")
    print(f"TP: {TP_COEFFS} | SL: {SL_THRESHOLDS}")
    print(f"Re: {REENTRY_PCTS} | 冷却{COOLDOWN_MIN}min")
    print("=" * 60)

    # 收集所有品种
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
            # 跳过已完成的
            out_file = f'{OUT_DIR}/{exchange}_{prod}.json'
            if os.path.exists(out_file):
                print(f"  跳过 {exchange}.{prod} (已完成)")
                continue
            all_products.append((exchange, prod, path))

    # 过滤指定品种
    if len(sys.argv) > 1:
        targets = [a.upper() for a in sys.argv[1:]]
        all_products = [(e, p, path) for e, p, path in all_products
                        if p.upper() in targets or p in targets]

    print(f"\n待回测: {len(all_products)}个品种")
    all_summaries = []

    for i, (exchange, product, path) in enumerate(all_products):
        print(f"\n[{i+1}/{len(all_products)}]", end="")
        try:
            result = run_product(exchange, product, path)
            if result:
                # 即时保存
                out_file = f'{OUT_DIR}/{exchange}_{product}.json'
                with open(out_file, 'w') as f:
                    json.dump(result, f, indent=2, default=str)
                all_summaries.append(result)
        except Exception as e:
            print(f"  ERROR: {e}")
            traceback.print_exc()
        gc.collect()

    # 全市场汇总
    if all_summaries:
        print("\n" + "=" * 80)
        print("全市场汇总")
        print("=" * 80)

        # 按过滤后PnL排序
        rankings = []
        for s in all_summaries:
            b = s.get('best', {})
            if b:
                rankings.append({
                    'product': f"{s['exchange']}.{s['product']}",
                    'valid_days': s['valid_days'],
                    'best_params': f"OTM{b.get('otm_pct',0)*100:.0f}%/TP{b.get('tp','?')}/SL{b.get('sl','?')}/Re{b.get('re','?')}",
                    'pnl_filtered': b.get('pnl_filtered', 0),
                    'wr': b.get('wr', 0),
                    'avg_daily': b.get('avg_daily', 0),
                    'tick_p50': b.get('tick_p50', 0),
                    'pnl_retain': b.get('pnl_retain', 0),
                })

        rankings.sort(key=lambda x: x['pnl_filtered'], reverse=True)
        print(f"\n{'品种':>12} {'有效天':>5} {'最优参数':>30} {'过滤PnL':>8} {'WR%':>5} {'日均笔':>5} {'P50tick':>7} {'保留%':>5}")
        print("-" * 100)
        for r in rankings:
            pc = '+' if r['pnl_filtered'] > 0 else ''
            print(f"{r['product']:>12} {r['valid_days']:>5} {r['best_params']:>30} "
                  f"{pc}{r['pnl_filtered']:>7.0f} {r['wr']:>5.1f} {r['avg_daily']:>5.2f} "
                  f"{r['tick_p50']:>7.1f} {r['pnl_retain']:>5.0f}")

        # 保存汇总
        summary_file = f'{OUT_DIR}/_ALL_SUMMARY.json'
        with open(summary_file, 'w') as f:
            json.dump({
                'rankings': rankings,
                'total_products': len(all_summaries),
                'date': str(date.today()),
                'params': {
                    'otm_pcts': OTM_PCTS, 'tp': TP_COEFFS,
                    'sl': SL_THRESHOLDS, 're': REENTRY_PCTS,
                    'cooldown': COOLDOWN_MIN, 'min_ticks': MIN_PROFIT_TICKS,
                },
            }, f, indent=2, ensure_ascii=False)
        print(f"\n汇总: {summary_file}")


if __name__ == '__main__':
    main()
