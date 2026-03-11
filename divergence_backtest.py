"""
腿背离策略回测 v1

核心信号: 高价腿方向 ≠ Sum方向 + IV(ATM)不动 → 真背离
策略A: 卖出(均值回归) — 背离后Sum会回落
策略B: 买入(动量) — 背离后Sum继续涨
影响力权重: 初始=高腿/低腿价格比, 动态=滚动贡献度
IV代理: ATM跨式价格(纯vega, 日内theta可忽略)

用法: python3 divergence_backtest.py [品种] [交易所]
  python3 divergence_backtest.py cu SHFE
  python3 divergence_backtest.py CF CZCE
"""

import os, sys, gc, json, re, warnings
from collections import defaultdict, Counter
from datetime import timedelta
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

warnings.filterwarnings('ignore')

HOME = os.path.expanduser('~')
BASE = f'{HOME}/Downloads/期权_parquet'
OUT_DIR = f'{HOME}/Scripts/divergence_results'
os.makedirs(OUT_DIR, exist_ok=True)

# ========== 参数 ==========
# 背离检测
LOOKBACK_WINDOWS = [5, 10, 20]     # 滚动窗口(分钟), 网格搜索
COMP_THRESHOLDS = [0.8, 1.0, 1.2]  # 补偿比阈值: 低腿补偿/高腿变化
IV_THRESHOLD = 0.005                # ATM sum变化率 < 0.5% 视为IV不动
MIN_SUM_MOVE = 0.003               # Sum最小变动率(过滤噪声)

# 交易
HOLD_LIMITS = [10, 20, 30, 60]     # 最大持仓分钟数
TP_TICKS_MULT = [0.5, 1.0, 1.5]   # TP = 背离幅度 × 倍数
SL_TICKS_MULT = [1.0, 2.0]        # SL = 背离幅度 × 倍数
COOLDOWN_MIN = 5                   # 信号冷却

# 虚值度
OTM_PCT = 0.03                     # 3% OTM
DTE_RANGE = (15, 60)

# Symbol解析
SYMBOL_PATTERNS = {
    'CZCE': re.compile(r'CZCE\.(\w+?)(\d{3,4})([CP])(\d+)'),
    'DCE': re.compile(r'DCE\.(\w+?)(\d{4})-([CP])-(\d+)'),
    'SHFE': re.compile(r'SHFE\.(\w+?)(\d{4})([CP])(\d+)'),
    'INE': re.compile(r'INE\.(\w+?)(\d{4})([CP])(\d+)'),
    'GFEX': re.compile(r'GFEX\.(\w+?)(\d{4})-([CP])-(\d+)'),
}
EXPIRY_OFFSETS = {'CZCE': 25, 'DCE': 25, 'SHFE': 25, 'INE': 25, 'GFEX': 25}


def make_symbol(exchange, product, yymm, cp, strike):
    if exchange == 'CZCE':
        return f'{exchange}.{product}{yymm}{cp}{strike}'
    elif exchange in ('DCE', 'GFEX'):
        return f'{exchange}.{product}{yymm}-{cp}-{strike}'
    else:
        return f'{exchange}.{product}{yymm}{cp}{strike}'


def parse_symbols(day_df, exchange):
    """一次性解析所有symbol"""
    pat = SYMBOL_PATTERNS.get(exchange)
    if not pat:
        return None
    syms = day_df['symbol'].unique()
    sym_map = {}
    for s in syms:
        m = pat.match(s)
        if m:
            sym_map[s] = (m.group(2), m.group(3), int(m.group(4)))
    if not sym_map:
        return None
    df = day_df[day_df['symbol'].isin(sym_map)].copy()
    df['yymm'] = df['symbol'].map(lambda s: sym_map[s][0])
    df['cp'] = df['symbol'].map(lambda s: sym_map[s][1])
    df['strike'] = df['symbol'].map(lambda s: sym_map[s][2])
    return df


def estimate_dte(yymm, trade_date, exchange):
    if len(yymm) == 3:
        yy = int(yymm[0])
        mm = int(yymm[1:])
        yy = yy + 2020 if yy <= 5 else yy + 2010
    else:
        yy = 2000 + int(yymm[:2])
        mm = int(yymm[2:])
    em, ey = (mm - 1, yy) if mm > 1 else (12, yy - 1)
    day = EXPIRY_OFFSETS.get(exchange, 25)
    try:
        exp = pd.Timestamp(ey, em, day).date()
    except:
        exp = pd.Timestamp(ey, em, 20).date()
    return max((exp - trade_date).days, 1)


def detect_tick_size(prices):
    prices = sorted(set(prices))
    if len(prices) < 10:
        return 1
    diffs = [round(prices[i+1] - prices[i], 6) for i in range(min(200, len(prices)-1))]
    diffs = [d for d in diffs if d > 0]
    if not diffs:
        return 1
    return Counter([round(d, 4) for d in diffs]).most_common(1)[0][0]


def detect_strike_step(strikes):
    s = sorted(set(strikes))
    if len(s) < 3:
        return s[1] - s[0] if len(s) == 2 else 100
    diffs = [s[i+1] - s[i] for i in range(len(s)-1)]
    return Counter(diffs).most_common(1)[0][0]


def find_pairs_for_day(parsed_df, exchange, product, trade_date):
    """
    找到OTM宽跨 + 相邻OTM对（IV代理）
    IV代理用相邻档OTM而非ATM — 因为交易的是OTM，相邻档更能反映该虚值区域的IV变化
    返回: (otm_call, otm_put, iv_call, iv_put, yymm, dte) or None
    """
    vol_by_m = parsed_df.groupby('yymm')['volume'].sum()
    if vol_by_m.empty:
        return None
    main_m = vol_by_m.idxmax()
    mdf = parsed_df[parsed_df['yymm'] == main_m]

    dte = estimate_dte(main_m, trade_date, exchange)
    if dte < DTE_RANGE[0] or dte > DTE_RANGE[1]:
        return None

    # ATM
    first_t = mdf['datetime'].min()
    first = mdf[mdf['datetime'] == first_t]
    cp = first[first['cp'] == 'C'].set_index('strike')['close']
    pp = first[first['cp'] == 'P'].set_index('strike')['close']
    common = sorted(set(cp.index) & set(pp.index))
    if len(common) < 5:
        return None
    atm_k = min(common, key=lambda k: abs(cp.get(k, 9999) - pp.get(k, 9999)))
    step = detect_strike_step(common)

    # OTM pair (交易用)
    n_steps = max(1, round(atm_k * OTM_PCT / step))
    otm_call_k = atm_k + n_steps * step
    otm_put_k = atm_k - n_steps * step

    # IV代理pair: 相邻一档OTM (虚值度相近, 能反映该区域IV)
    # 直接从common列表找实际存在的相邻行权价，不依赖step（处理非均匀间距）
    common_set = set(common)

    # Call侧: 优先找比otm_call_k更虚一档(更高), 否则浅一档(更低但不等于ATM)
    call_higher = sorted([k for k in common if k > otm_call_k])
    call_lower = sorted([k for k in common if atm_k < k < otm_call_k], reverse=True)
    iv_call_k = call_higher[0] if call_higher else (call_lower[0] if call_lower else otm_call_k)

    # Put侧: 优先找比otm_put_k更虚一档(更低), 否则浅一档(更高但不等于ATM)
    put_lower = sorted([k for k in common if k < otm_put_k], reverse=True)
    put_higher = sorted([k for k in common if otm_put_k < k < atm_k])
    iv_put_k = put_lower[0] if put_lower else (put_higher[0] if put_higher else otm_put_k)

    # 构造symbol
    otm_c = make_symbol(exchange, product, main_m, 'C', otm_call_k)
    otm_p = make_symbol(exchange, product, main_m, 'P', otm_put_k)
    iv_c = make_symbol(exchange, product, main_m, 'C', iv_call_k)
    iv_p = make_symbol(exchange, product, main_m, 'P', iv_put_k)

    # 验证OTM对必须存在
    for sym in [otm_c, otm_p]:
        sdf = mdf[mdf['symbol'] == sym]
        if sdf.empty or sdf['volume'].sum() < 5:
            return None

    # IV代理对: 有最好, 没有也能跑(降级为不用IV过滤)
    iv_c_ok = not mdf[mdf['symbol'] == iv_c].empty
    iv_p_ok = not mdf[mdf['symbol'] == iv_p].empty
    if not iv_c_ok or not iv_p_ok:
        iv_c = otm_c  # 降级: 用自身作为IV代理(IV过滤自动失效)
        iv_p = otm_p

    return otm_c, otm_p, iv_c, iv_p, main_m, dte


def build_minute_data(day_df, otm_c, otm_p, iv_c, iv_p):
    """
    构建对齐的1分钟数据 (tick-ready接口: 换数据源即可)
    otm_call, otm_put, otm_sum (交易对)
    iv_call, iv_put, iv_sum (IV代理 — 相邻档OTM)
    """
    def get_series(sym, name):
        return day_df[day_df['symbol'] == sym][['datetime', 'close']].rename(
            columns={'close': name}).set_index('datetime')

    oc = get_series(otm_c, 'otm_c')
    op = get_series(otm_p, 'otm_p')
    ic = get_series(iv_c, 'iv_c')
    ip = get_series(iv_p, 'iv_p')

    mg = oc.join(op, how='inner').join(ic, how='inner').join(ip, how='inner').sort_index()
    if len(mg) < 30:
        return None

    mg['otm_sum'] = mg['otm_c'] + mg['otm_p']
    mg['iv_sum'] = mg['iv_c'] + mg['iv_p']  # IV代理: 相邻档OTM pair sum

    return mg


def compute_signals(mg, lookback):
    """
    计算背离信号
    返回带信号列的DataFrame (保留接口兼容tick级)
    """
    n = len(mg)
    if n < lookback + 5:
        return None

    # 变化量
    mg = mg.copy()
    mg['d_otm_c'] = mg['otm_c'].diff()
    mg['d_otm_p'] = mg['otm_p'].diff()
    mg['d_otm_sum'] = mg['otm_sum'].diff()
    mg['d_iv_sum'] = mg['iv_sum'].diff()

    # 高价腿/低价腿判定（每分钟动态判定）
    mg['high_leg'] = np.where(mg['otm_c'] >= mg['otm_p'], 'C', 'P')
    mg['high_price'] = np.maximum(mg['otm_c'], mg['otm_p'])
    mg['low_price'] = np.minimum(mg['otm_c'], mg['otm_p'])

    # 初始影响力权重: 高腿价格 / (高腿+低腿)
    mg['high_weight'] = mg['high_price'] / (mg['high_price'] + mg['low_price'])

    # 高价腿变化量
    mg['d_high'] = np.where(mg['high_leg'] == 'C', mg['d_otm_c'], mg['d_otm_p'])
    mg['d_low'] = np.where(mg['high_leg'] == 'C', mg['d_otm_p'], mg['d_otm_c'])

    # 滚动窗口统计
    mg['sum_chg'] = mg['otm_sum'].diff(lookback)
    mg['high_chg'] = mg['high_price'].diff(lookback)
    mg['low_chg'] = mg['low_price'].diff(lookback)
    mg['atm_chg'] = mg['iv_sum'].diff(lookback)

    # Sum变动率 (过滤噪声)
    mg['sum_chg_pct'] = mg['sum_chg'] / mg['otm_sum'].shift(lookback)

    # IV变动率
    mg['iv_chg_pct'] = mg['atm_chg'] / mg['iv_sum'].shift(lookback)

    # 补偿比: |低腿变化| / |高腿变化| (当方向相反时)
    with np.errstate(divide='ignore', invalid='ignore'):
        mg['comp_ratio'] = np.where(
            mg['high_chg'] != 0,
            np.abs(mg['low_chg']) / np.abs(mg['high_chg']),
            0
        )

    # 滚动实际贡献度
    abs_d_high = mg['d_high'].abs().rolling(lookback).sum()
    abs_d_low = mg['d_low'].abs().rolling(lookback).sum()
    total_abs = abs_d_high + abs_d_low
    mg['actual_high_contrib'] = np.where(total_abs > 0, abs_d_high / total_abs, 0.5)

    # 贡献偏离 = 实际贡献 - 初始权重 (负值=高腿贡献不足)
    mg['contrib_surprise'] = mg['actual_high_contrib'] - mg['high_weight']

    # === 背离信号判定 ===
    # 条件1: Sum和高价腿方向相反
    mg['direction_diverge'] = (mg['sum_chg'] * mg['high_chg']) < 0

    # 条件2: Sum变动幅度足够大
    mg['sum_move_ok'] = mg['sum_chg_pct'].abs() > MIN_SUM_MOVE

    # 条件3: IV不动 (ATM sum变动率小于阈值)
    mg['iv_flat'] = mg['iv_chg_pct'].abs() < IV_THRESHOLD

    # 背离方向: +1=Sum涨高腿跌(做多信号), -1=Sum跌高腿涨(做空信号)
    mg['div_direction'] = np.where(
        mg['direction_diverge'],
        np.sign(mg['sum_chg']),
        0
    ).astype(int)

    return mg


def simulate_trades(mg, comp_thresh, hold_limit, tp_mult, sl_mult, tick_size):
    """
    模拟交易
    当背离信号触发:
      策略A(均值回归): 与Sum方向反向交易 (Sum涨→卖, Sum跌→买)
      策略B(动量): 与Sum方向同向交易 (Sum涨→买, Sum跌→卖)
    """
    trades_a = []  # 均值回归
    trades_b = []  # 动量

    idx = mg.index
    n = len(idx)
    in_pos_a = in_pos_b = False
    entry_a = entry_b = None
    cooldown = None

    for i in range(n):
        ts = idx[i]
        row = mg.iloc[i]

        # 检查是否有有效信号
        if pd.isna(row['div_direction']) or row['div_direction'] == 0:
            pass  # 无信号
        elif not in_pos_a and not in_pos_b:
            if cooldown and ts < cooldown:
                continue

            # 信号条件
            is_diverge = row['direction_diverge']
            sum_ok = row['sum_move_ok']
            iv_ok = row['iv_flat']
            comp_ok = row['comp_ratio'] >= comp_thresh

            if is_diverge and sum_ok and iv_ok and comp_ok:
                div_dir = row['div_direction']
                entry_sum = row['otm_sum']
                entry_high = row['high_price']
                diverge_size = abs(row['sum_chg'])  # 背离幅度

                # TP/SL 基于背离幅度
                tp_pts = diverge_size * tp_mult
                sl_pts = diverge_size * sl_mult

                entry_a = {
                    'time': ts, 'sum': entry_sum, 'dir': -div_dir,  # 反向
                    'tp': tp_pts, 'sl': sl_pts, 'deadline': i + hold_limit,
                    'high_w': row['high_weight'], 'comp': row['comp_ratio'],
                    'iv_chg': row['iv_chg_pct'],
                    'div_size': diverge_size,
                }
                entry_b = {
                    'time': ts, 'sum': entry_sum, 'dir': div_dir,   # 同向
                    'tp': tp_pts, 'sl': sl_pts, 'deadline': i + hold_limit,
                    'high_w': row['high_weight'], 'comp': row['comp_ratio'],
                    'iv_chg': row['iv_chg_pct'],
                    'div_size': diverge_size,
                }
                in_pos_a = in_pos_b = True
                continue

        # 检查持仓退出
        cur_sum = row['otm_sum']

        if in_pos_a:
            # 策略A: 反向交易, dir=-div_dir
            # 如果dir=-1(看跌Sum), profit = entry_sum - cur_sum
            # 如果dir=+1(看涨Sum), profit = cur_sum - entry_sum
            pnl_a = (cur_sum - entry_a['sum']) * entry_a['dir']
            tp_hit = pnl_a >= entry_a['tp']
            sl_hit = pnl_a <= -entry_a['sl']
            timeout = i >= entry_a['deadline']

            if tp_hit or sl_hit or timeout:
                reason = 'TP' if tp_hit else ('SL' if sl_hit else 'TO')
                trades_a.append({
                    'entry_time': str(entry_a['time']),
                    'exit_time': str(ts),
                    'pnl': round(pnl_a, 2),
                    'ticks': round(pnl_a / tick_size, 1) if tick_size > 0 else 0,
                    'hold_min': round((ts - entry_a['time']).total_seconds() / 60, 1),
                    'reason': reason,
                    'high_w': round(entry_a['high_w'], 3),
                    'comp': round(entry_a['comp'], 2),
                    'div_size': round(entry_a['div_size'], 2),
                    'strategy': 'A_revert',
                })
                in_pos_a = False

        if in_pos_b:
            pnl_b = (cur_sum - entry_b['sum']) * entry_b['dir']
            tp_hit = pnl_b >= entry_b['tp']
            sl_hit = pnl_b <= -entry_b['sl']
            timeout = i >= entry_b['deadline']

            if tp_hit or sl_hit or timeout:
                reason = 'TP' if tp_hit else ('SL' if sl_hit else 'TO')
                trades_b.append({
                    'entry_time': str(entry_b['time']),
                    'exit_time': str(ts),
                    'pnl': round(pnl_b, 2),
                    'ticks': round(pnl_b / tick_size, 1) if tick_size > 0 else 0,
                    'hold_min': round((ts - entry_b['time']).total_seconds() / 60, 1),
                    'reason': reason,
                    'high_w': round(entry_b['high_w'], 3),
                    'comp': round(entry_b['comp'], 2),
                    'div_size': round(entry_b['div_size'], 2),
                    'strategy': 'B_momentum',
                })
                in_pos_b = False

        if not in_pos_a and not in_pos_b and (entry_a or entry_b):
            cooldown = ts + pd.Timedelta(minutes=COOLDOWN_MIN)
            entry_a = entry_b = None

    return trades_a, trades_b


def process_day(day_df, trade_date, exchange, product, tick_size):
    """处理单天, 返回所有参数组合的交易"""
    parsed = parse_symbols(day_df, exchange)
    if parsed is None:
        return {}

    result = find_pairs_for_day(parsed, exchange, product, trade_date)
    if result is None:
        return {}

    otm_c, otm_p, atm_c, atm_p, yymm, dte = result
    mg = build_minute_data(day_df, otm_c, otm_p, atm_c, atm_p)
    if mg is None:
        return {}

    day_results = {}

    for lookback in LOOKBACK_WINDOWS:
        sig = compute_signals(mg, lookback)
        if sig is None:
            continue

        # 统计当天背离次数
        n_div = (sig['direction_diverge'] & sig['sum_move_ok'] & sig['iv_flat']).sum()
        if n_div == 0:
            continue

        for comp_th in COMP_THRESHOLDS:
            for hold in HOLD_LIMITS:
                for tp_m in TP_TICKS_MULT:
                    for sl_m in SL_TICKS_MULT:
                        key = (lookback, comp_th, hold, tp_m, sl_m)
                        ta, tb = simulate_trades(sig, comp_th, hold, tp_m, sl_m, tick_size)
                        for t in ta + tb:
                            t['date'] = str(trade_date)
                            t['dte'] = dte
                            t['lookback'] = lookback
                        day_results.setdefault(key, {'A': [], 'B': []})
                        day_results[key]['A'].extend(ta)
                        day_results[key]['B'].extend(tb)

    return day_results


def summarize_results(all_results, exchange, product, tick_size, n_valid, n_total):
    """汇总所有参数组合"""
    rows = []
    for key, strats in all_results.items():
        lookback, comp_th, hold, tp_m, sl_m = key

        for strat_name, trades in strats.items():
            if not trades:
                continue
            df = pd.DataFrame(trades)
            n = len(df)
            if n < 3:
                continue
            nd = df['date'].nunique()
            pnl = df['pnl'].sum()
            wr = (df['pnl'] > 0).mean() * 100
            avg_pnl = df['pnl'].mean()
            avg_ticks = df['ticks'].mean()
            avg_hold = df['hold_min'].mean()
            tp_pct = (df['reason'] == 'TP').mean() * 100
            sl_pct = (df['reason'] == 'SL').mean() * 100
            to_pct = (df['reason'] == 'TO').mean() * 100

            rows.append({
                'strategy': strat_name,
                'lookback': lookback,
                'comp_th': comp_th,
                'hold_limit': hold,
                'tp_mult': tp_m,
                'sl_mult': sl_m,
                'trades': n,
                'days': nd,
                'total_pnl': round(pnl, 1),
                'wr': round(wr, 1),
                'avg_pnl': round(avg_pnl, 2),
                'avg_ticks': round(avg_ticks, 1),
                'avg_hold': round(avg_hold, 1),
                'tp_pct': round(tp_pct, 1),
                'sl_pct': round(sl_pct, 1),
                'to_pct': round(to_pct, 1),
            })

    if not rows:
        print(f"  无有效结果")
        return None

    sdf = pd.DataFrame(rows)

    # A策略最优 vs B策略最优
    for strat in ['A', 'B']:
        sub = sdf[sdf['strategy'] == strat].sort_values('total_pnl', ascending=False)
        label = "均值回归(卖)" if strat == 'A' else "动量(买)"
        print(f"\n  === 策略{strat}: {label} ===")
        if sub.empty:
            print(f"  无交易")
            continue
        print(f"  Top 5:")
        for _, r in sub.head(5).iterrows():
            print(f"    LB{r['lookback']}/Comp{r['comp_th']}/Hold{r['hold_limit']}m/"
                  f"TP{r['tp_mult']}x/SL{r['sl_mult']}x | "
                  f"{r['trades']}笔/{r['days']}天 WR{r['wr']}% "
                  f"PnL={r['total_pnl']} avg={r['avg_ticks']:.1f}t/笔 "
                  f"TP{r['tp_pct']:.0f}%/SL{r['sl_pct']:.0f}%/TO{r['to_pct']:.0f}%")

    # A vs B 总体对比
    a_best = sdf[sdf['strategy'] == 'A'].sort_values('total_pnl', ascending=False)
    b_best = sdf[sdf['strategy'] == 'B'].sort_values('total_pnl', ascending=False)
    a_top = a_best.iloc[0]['total_pnl'] if len(a_best) > 0 else 0
    b_top = b_best.iloc[0]['total_pnl'] if len(b_best) > 0 else 0
    winner = 'A(均值回归)' if a_top > b_top else 'B(动量)'
    print(f"\n  结论: {winner}更优 (A={a_top:.0f} vs B={b_top:.0f})")

    return {
        'exchange': exchange,
        'product': product,
        'tick_size': tick_size,
        'valid_days': n_valid,
        'total_days': n_total,
        'summary': sdf.to_dict('records'),
        'best_A': a_best.iloc[0].to_dict() if len(a_best) > 0 else {},
        'best_B': b_best.iloc[0].to_dict() if len(b_best) > 0 else {},
    }


def run_product(exchange, product):
    path = f'{BASE}/{exchange}/{product}.parquet'
    if not os.path.exists(path):
        print(f"  文件不存在: {path}")
        return None

    print(f"\n{'='*60}")
    print(f"  背离回测: {exchange}.{product}")
    print(f"{'='*60}")

    try:
        pf = pq.ParquetFile(path)
    except Exception as e:
        print(f"  文件损坏: {e}")
        return None

    # 读取全量数据
    print(f"  读取数据...", end="", flush=True)
    chunks = []
    for batch in pf.iter_batches(batch_size=1000000, columns=['datetime', 'symbol', 'close', 'volume']):
        chunks.append(batch.to_pandas())
    all_df = pd.concat(chunks, ignore_index=True)
    del chunks
    gc.collect()
    all_df['date'] = all_df['datetime'].dt.date
    dates = sorted(all_df['date'].unique())
    print(f" {len(all_df)}行 / {len(dates)}天")

    tick_size = detect_tick_size(all_df.head(5000)['close'].dropna().values)
    print(f"  tick_size={tick_size}")

    # 逐天处理
    all_results = defaultdict(lambda: {'A': [], 'B': []})
    n_valid = 0
    grouped = all_df.groupby('date')
    del all_df
    gc.collect()

    for i, (d, day_df) in enumerate(grouped):
        day_df = day_df.drop(columns=['date'])
        day_res = process_day(day_df, d, exchange, product, tick_size)
        if day_res:
            n_valid += 1
            for key, strats in day_res.items():
                all_results[key]['A'].extend(strats['A'])
                all_results[key]['B'].extend(strats['B'])
        if (i + 1) % 200 == 0:
            n_trades = sum(len(v['A']) + len(v['B']) for v in all_results.values())
            print(f"  {i+1}/{len(dates)} (有效{n_valid}, {n_trades}笔)...")

    del grouped
    gc.collect()

    print(f"  完成: {len(dates)}天/{n_valid}有效")

    # 统计背离频率
    total_a = sum(len(v['A']) for v in all_results.values())
    total_b = sum(len(v['B']) for v in all_results.values())
    print(f"  总交易: A={total_a}笔 B={total_b}笔")

    if not all_results or total_a + total_b == 0:
        print(f"  无背离信号")
        return None

    return summarize_results(all_results, exchange, product, tick_size, n_valid, len(dates))


def main():
    print("=" * 60)
    print("腿背离策略回测")
    print(f"窗口: {LOOKBACK_WINDOWS} | 补偿阈值: {COMP_THRESHOLDS}")
    print(f"持仓: {HOLD_LIMITS}min | TP: {TP_TICKS_MULT}x | SL: {SL_TICKS_MULT}x")
    print(f"IV阈值: {IV_THRESHOLD} | Sum最小变动: {MIN_SUM_MOVE}")
    print("=" * 60)

    # 默认品种或从命令行指定
    if len(sys.argv) >= 3:
        targets = [(sys.argv[2], sys.argv[1])]
    elif len(sys.argv) >= 2:
        # 自动匹配交易所
        prod = sys.argv[1]
        ex_map = {'cu': 'SHFE', 'al': 'SHFE', 'zn': 'SHFE', 'ag': 'SHFE', 'au': 'SHFE',
                   'ru': 'SHFE', 'br': 'SHFE', 'ao': 'SHFE', 'pb': 'SHFE', 'rb': 'SHFE',
                   'CF': 'CZCE', 'TA': 'CZCE', 'OI': 'CZCE', 'SM': 'CZCE', 'MA': 'CZCE',
                   'RM': 'CZCE', 'SF': 'CZCE', 'SR': 'CZCE', 'AP': 'CZCE', 'PK': 'CZCE',
                   'SA': 'CZCE', 'PX': 'CZCE', 'ZC': 'CZCE', 'PF': 'CZCE', 'FG': 'CZCE',
                   'p': 'DCE', 'y': 'DCE', 'pg': 'DCE', 'm': 'DCE', 'l': 'DCE',
                   'c': 'DCE', 'a': 'DCE', 'lh': 'DCE', 'eg': 'DCE', 'eb': 'DCE',
                   'ps': 'GFEX', 'si': 'GFEX', 'sc': 'INE'}
        ex = ex_map.get(prod, 'SHFE')
        targets = [(ex, prod)]
    else:
        # 默认: 流动性最好的几个品种
        targets = [('SHFE', 'cu'), ('DCE', 'pg'), ('CZCE', 'CF')]

    for exchange, product in targets:
        try:
            result = run_product(exchange, product)
            if result:
                out_file = f'{OUT_DIR}/{exchange}_{product}.json'
                with open(out_file, 'w') as f:
                    json.dump(result, f, indent=2, default=str, ensure_ascii=False)
                print(f"  保存: {out_file}")
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
        gc.collect()


if __name__ == '__main__':
    main()
