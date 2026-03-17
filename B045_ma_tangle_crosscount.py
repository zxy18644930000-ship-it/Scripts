#!/usr/bin/env python3
"""
B045: MA纠缠度重新定义 — 穿越次数 + 偏向比例 + 40MA等距确认（含网格测试）

改进思路:
  B043/B044用 LB10/T7 (最近10根中>=7根在同侧=趋势) 定义纠缠/趋势,
  窗口太短, 频繁穿越MA的品种被误判为趋势, 几乎选不出纠缠品种.

  新定义（三维判定）:
    1. 穿越次数(CrossCount): 较长窗口内价格穿越MA的次数, 穿越多=纠缠
    2. 偏向比例(Bias): 窗口内在MA同一侧的占比, 偏向大=趋势信号
    3. 40MA等距确认: 价格在40MA上下分布是否均匀, 均匀=围绕40MA震荡=纠缠

  综合决策:
    crossings >= CROSS_THRESHOLD → entangled (频繁穿越=纠缠)
    crossings < CROSS_THRESHOLD AND bias >= 0.8 AND 40MA确认趋势 → trending
    其余 → entangled (保守归入纠缠)

  网格测试: CROSS_THRESHOLD = [2, 3, 4, 5, 6], 对同一数据用不同阈值标记,
  一次加载数据, 多次统计, 找最优穿越阈值.

用法:
  python3 B045_ma_tangle_crosscount.py --product ag
  python3 B045_ma_tangle_crosscount.py --product ag,p,cu --workers 4
  python3 B045_ma_tangle_crosscount.py --all --workers 12
"""

import argparse
import gc
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# ============ 配置 ============
MA_PERIOD = 20
MA_PERIOD_40 = 40
LOOKBACK_1M = 50       # 1m 回看窗口 (~50分钟)
LOOKBACK_5M = 20       # 5m 回看窗口 (~100分钟)
BIAS_THRESHOLD = 0.8   # 偏向比例阈值
MA40_DIST_RATIO = 2.0  # 40MA上下平均距离比阈值
MA40_COUNT_BIAS = 0.7  # 40MA上下K线数偏向阈值

CROSS_THRESHOLD_GRID = [2, 3, 4, 5, 6]

HOLD_PERIODS = [30, 60]
ENTRY_INTERVAL = 30
SESSION_GAP_MINUTES = 30

FUTURES_DIR = '/mnt/d/backtest_data/Futures_parquet/'
OPTIONS_DIR = '/mnt/d/backtest_data/Options_parquet/'
if not os.path.exists('/mnt/d/'):
    FUTURES_DIR = os.path.expanduser('~/Downloads/期货数据_parquet/')
    OPTIONS_DIR = os.path.expanduser('~/Downloads/期权_parquet/')

RESULT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ma_tangle_crosscount_results')

EXCHANGE_MAP = {
    'ag': 'SHFE', 'au': 'SHFE', 'cu': 'SHFE', 'al': 'SHFE', 'zn': 'SHFE',
    'ni': 'SHFE', 'sn': 'SHFE', 'ru': 'SHFE', 'fu': 'SHFE', 'ao': 'SHFE',
    'rb': 'SHFE', 'bu': 'SHFE', 'sp': 'SHFE', 'pb': 'SHFE',
    'p': 'DCE', 'm': 'DCE', 'y': 'DCE', 'i': 'DCE', 'jm': 'DCE',
    'j': 'DCE', 'jd': 'DCE', 'lh': 'DCE', 'pp': 'DCE', 'l': 'DCE',
    'v': 'DCE', 'eb': 'DCE', 'eg': 'DCE', 'c': 'DCE', 'pg': 'DCE',
    'sa': 'CZCE', 'fg': 'CZCE', 'ta': 'CZCE', 'ma': 'CZCE', 'cf': 'CZCE',
    'sr': 'CZCE', 'rm': 'CZCE', 'oi': 'CZCE', 'ur': 'CZCE', 'pf': 'CZCE',
    'sh': 'CZCE', 'pk': 'CZCE', 'sm': 'CZCE', 'sf': 'CZCE', 'px': 'CZCE',
    'ap': 'CZCE', 'cj': 'CZCE',
    'si': 'GFEX', 'lc': 'GFEX',
    'sc': 'INE', 'br': 'INE',
}


# ============ 数据加载 ============

def load_futures(product):
    import pyarrow.parquet as pq
    exchange = EXCHANGE_MAP.get(product.lower(), '')
    if not exchange:
        return pd.DataFrame()
    prod_upper = product.upper()
    dir_c = os.path.join(FUTURES_DIR, exchange, prod_upper)
    if os.path.isdir(dir_c):
        files = sorted([f for f in os.listdir(dir_c) if f.endswith('.parquet')])
        all_dfs = []
        for fname in files:
            contract = fname.replace('.parquet', '')
            suffix = contract[len(prod_upper):]
            if not suffix.isdigit():
                continue
            try:
                df = pq.read_table(os.path.join(dir_c, fname),
                                   columns=['datetime', 'open', 'close', 'high', 'low', 'volume']).to_pandas()
            except Exception:
                continue
            if df.empty:
                continue
            df['symbol'] = contract
            df.rename(columns={'open': 'open_price', 'close': 'close_price',
                               'high': 'high_price', 'low': 'low_price'}, inplace=True)
            all_dfs.append(df)
        if all_dfs:
            result = pd.concat(all_dfs, ignore_index=True)
            del all_dfs; gc.collect()
            return result
    path_a = os.path.join(FUTURES_DIR, exchange, f'{prod_upper}.parquet')
    if os.path.exists(path_a):
        df = pq.read_table(path_a, columns=['datetime', 'symbol', 'open', 'close', 'high', 'low', 'volume']).to_pandas()
        df.rename(columns={'open': 'open_price', 'close': 'close_price',
                           'high': 'high_price', 'low': 'low_price'}, inplace=True)
        pattern = rf'^{prod_upper}\d{{3,4}}$'
        return df[df['symbol'].str.match(pattern)].copy()
    return pd.DataFrame()


def _contract_yymm_for_exchange(contract_yymm, exchange):
    if exchange == 'CZCE':
        return contract_yymm[1:]
    return contract_yymm


def parse_option_symbol(sym, product):
    prod_upper = product.upper()
    prod_lower = product.lower()
    clean = sym.split('.')[-1]
    m = re.match(rf'^(?:{prod_upper}|{prod_lower})(\d{{3,4}})-([CP])-(\d+)$', clean, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2).upper(), int(m.group(3))
    m = re.match(rf'^(?:{prod_upper}|{prod_lower})(\d{{3,4}})([CP])(\d+)$', clean, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2).upper(), int(m.group(3))
    return None


# ============ 新 MA 状态计算（三维判定）============

def _compute_raw_features(close, ma_period, lookback):
    """计算MA穿越次数、偏向比例等原始特征（向量化，每根bar一组值）

    Returns: (crossings, bias, above_ratio, ma) — 每个都是 len(close) 的数组
             前 ma_period+lookback-1 根为 NaN
    """
    n = len(close)
    ma = pd.Series(close).rolling(ma_period, min_periods=ma_period).mean().values

    diff = close - ma
    sign = np.sign(diff)  # +1, 0, -1

    above = (close > ma).astype(np.float64)
    below = (close < ma).astype(np.float64)

    crossings_arr = np.full(n, np.nan)
    bias_arr = np.full(n, np.nan)
    above_ratio_arr = np.full(n, np.nan)

    start = ma_period + lookback - 1
    if start >= n:
        return crossings_arr, bias_arr, above_ratio_arr, ma

    for i in range(start, n):
        window_sign = sign[i - lookback + 1: i + 1]
        sign_diff = np.diff(window_sign)
        cross_count = int(np.count_nonzero(sign_diff != 0))
        crossings_arr[i] = cross_count

        ab_count = float(np.sum(above[i - lookback + 1: i + 1]))
        bl_count = float(np.sum(below[i - lookback + 1: i + 1]))
        total = ab_count + bl_count
        if total > 0:
            ab_ratio = ab_count / total
            bias_arr[i] = max(ab_ratio, 1.0 - ab_ratio)
            above_ratio_arr[i] = ab_ratio
        else:
            bias_arr[i] = 0.5
            above_ratio_arr[i] = 0.5

    return crossings_arr, bias_arr, above_ratio_arr, ma


def _compute_ma40_equidist(close, ma40, lookback):
    """计算40MA等距特征: 上下平均距离比 + 上下K线数偏向

    Returns: (dist_ratio, count_bias) — 各 len(close) 的数组
    """
    n = len(close)
    dist_ratio_arr = np.full(n, np.nan)
    count_bias_arr = np.full(n, np.nan)

    start = 40 + lookback - 1  # 40MA需要40根预热
    if start >= n:
        return dist_ratio_arr, count_bias_arr

    for i in range(start, n):
        if np.isnan(ma40[i]):
            continue
        window_close = close[i - lookback + 1: i + 1]
        window_ma40 = ma40[i - lookback + 1: i + 1]

        valid = ~np.isnan(window_ma40)
        if valid.sum() < lookback * 0.5:
            continue

        wc = window_close[valid]
        wm = window_ma40[valid]
        above_mask = wc > wm
        below_mask = wc < wm
        n_above = int(above_mask.sum())
        n_below = int(below_mask.sum())

        if n_above > 0:
            dist_above = float(np.mean(wc[above_mask] - wm[above_mask]))
        else:
            dist_above = 0.0
        if n_below > 0:
            dist_below = float(np.mean(wm[below_mask] - wc[below_mask]))
        else:
            dist_below = 0.0

        eps = 1e-10
        if dist_above > eps and dist_below > eps:
            dist_ratio_arr[i] = max(dist_above, dist_below) / min(dist_above, dist_below)
        elif dist_above > eps or dist_below > eps:
            dist_ratio_arr[i] = 999.0
        else:
            dist_ratio_arr[i] = 1.0

        total = n_above + n_below
        if total > 0:
            count_bias_arr[i] = max(n_above, n_below) / total
        else:
            count_bias_arr[i] = 0.5

    return dist_ratio_arr, count_bias_arr


def classify_state_grid(crossings, bias, above_ratio, dist_ratio_40, count_bias_40, cross_threshold):
    """根据三维特征 + 给定穿越阈值，判定每根bar的状态

    Returns: state 数组 (U20)
    """
    n = len(crossings)
    state = np.full(n, 'warmup', dtype='U20')

    for i in range(n):
        if np.isnan(crossings[i]) or np.isnan(bias[i]):
            continue

        cx = int(crossings[i])
        bi = bias[i]
        ar = above_ratio[i]
        dr = dist_ratio_40[i] if not np.isnan(dist_ratio_40[i]) else 999.0
        cb = count_bias_40[i] if not np.isnan(count_bias_40[i]) else 1.0

        if cx >= cross_threshold:
            state[i] = 'entangled'
        elif bi >= BIAS_THRESHOLD:
            ma40_confirms_trend = (dr > MA40_DIST_RATIO) or (cb > MA40_COUNT_BIAS)
            if ma40_confirms_trend:
                state[i] = 'trending_up' if ar > 0.5 else 'trending_down'
            else:
                state[i] = 'entangled'
        else:
            state[i] = 'entangled'

    return state


def compute_features_1m(fdf, ma_period, ma_period_40, lookback):
    """计算1m原始特征（只算一次，后续 grid 复用）"""
    close = fdf['close_price'].values
    crossings, bias, above_ratio, ma20 = _compute_raw_features(close, ma_period, lookback)
    ma40 = pd.Series(close).rolling(ma_period_40, min_periods=ma_period_40).mean().values
    dist_ratio_40, count_bias_40 = _compute_ma40_equidist(close, ma40, lookback)
    return crossings, bias, above_ratio, dist_ratio_40, count_bias_40


def compute_features_5m(fdf, ma_period, ma_period_40, lookback):
    """计算5m原始特征（重采样后只算一次）"""
    df = fdf.copy()
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.set_index('datetime')
    r5 = df['close_price'].resample('5min').last().dropna()

    min_needed = ma_period_40 + lookback
    if len(r5) < min_needed:
        n = len(fdf)
        return (np.full(n, np.nan), np.full(n, np.nan), np.full(n, np.nan),
                np.full(n, np.nan), np.full(n, np.nan), r5)

    close_5m = r5.values
    crossings, bias, above_ratio, ma20 = _compute_raw_features(close_5m, ma_period, lookback)
    ma40 = pd.Series(close_5m).rolling(ma_period_40, min_periods=ma_period_40).mean().values
    dist_ratio_40, count_bias_40 = _compute_ma40_equidist(close_5m, ma40, lookback)

    return crossings, bias, above_ratio, dist_ratio_40, count_bias_40, r5


def map_5m_to_1m(arr_5m, r5_index, fdf):
    """将5m级别数组映射回1m级别（forward fill）"""
    s5 = pd.Series(arr_5m, index=r5_index)
    fdf_dt = pd.to_datetime(fdf['datetime'])
    mapped = s5.reindex(fdf_dt, method='ffill')
    return mapped.values


def map_5m_state_to_1m(state_5m, r5_index, fdf):
    """将5m状态字符串数组映射回1m"""
    s5 = pd.Series(state_5m, index=r5_index)
    fdf_dt = pd.to_datetime(fdf['datetime'])
    mapped = s5.reindex(fdf_dt, method='ffill').fillna('warmup')
    return mapped.values


# ============ 核心回测 ============

OTM_MIN = 0.04
OTM_MAX = 0.08
OTM_FALLBACK_MIN = 0.03
OTM_FALLBACK_MAX = 0.10


def find_otm_pair(options_data, underlying_price, product, contract_yymm):
    exchange = EXCHANGE_MAP.get(product.lower(), '')
    opt_yymm = _contract_yymm_for_exchange(contract_yymm, exchange)
    calls = {}
    puts = {}
    for sym, df in options_data.items():
        parsed = parse_option_symbol(sym, product)
        if not parsed:
            continue
        yymm, cp, strike = parsed
        if yymm != opt_yymm:
            continue
        if cp == 'C':
            calls[strike] = sym
        else:
            puts[strike] = sym
    if not calls or not puts:
        return None, None

    def _find_best(otm_lo, otm_hi):
        c_cands = [(s, (s - underlying_price) / underlying_price) for s in calls if otm_lo <= (s - underlying_price) / underlying_price <= otm_hi]
        p_cands = [(s, (underlying_price - s) / underlying_price) for s in puts if otm_lo <= (underlying_price - s) / underlying_price <= otm_hi]
        if not c_cands or not p_cands:
            return None, None
        best_pair, best_score = None, float('inf')
        for cs, co in c_cands:
            for ps, po in p_cands:
                asym = abs(co - po)
                if asym < best_score:
                    best_score = asym
                    best_pair = (calls[cs], puts[ps])
        return best_pair

    result = _find_best(OTM_MIN, OTM_MAX)
    if result and result[0] and result[1]:
        return result
    result = _find_best(OTM_FALLBACK_MIN, OTM_FALLBACK_MAX)
    if result and result[0] and result[1]:
        return result
    return None, None


def backtest_single_contract(futures_df, options_data, product, contract_yymm,
                             hold_periods, entry_interval):
    """单合约回测 — 一次加载数据，对 CROSS_THRESHOLD_GRID 多个阈值分别统计"""
    fdf = futures_df.sort_values('datetime').reset_index(drop=True)
    fdf['datetime'] = pd.to_datetime(fdf['datetime'])

    min_warmup = MA_PERIOD_40 * 5 + LOOKBACK_5M + max(hold_periods)
    if len(fdf) < min_warmup:
        return {}

    # 计算1m特征（只算一次）
    cx_1m, bias_1m, ar_1m, dr40_1m, cb40_1m = compute_features_1m(
        fdf, MA_PERIOD, MA_PERIOD_40, LOOKBACK_1M)

    # 计算5m特征（只算一次）
    cx_5m_raw, bias_5m_raw, ar_5m_raw, dr40_5m_raw, cb40_5m_raw, r5 = compute_features_5m(
        fdf, MA_PERIOD, MA_PERIOD_40, LOOKBACK_5M)

    if len(r5) < MA_PERIOD_40 + LOOKBACK_5M:
        return {}

    # 将5m特征映射回1m（只做一次）
    cx_5m = map_5m_to_1m(cx_5m_raw, r5.index, fdf)
    bias_5m = map_5m_to_1m(bias_5m_raw, r5.index, fdf)
    ar_5m = map_5m_to_1m(ar_5m_raw, r5.index, fdf)
    dr40_5m = map_5m_to_1m(dr40_5m_raw, r5.index, fdf)
    cb40_5m = map_5m_to_1m(cb40_5m_raw, r5.index, fdf)

    # Session 标记
    gaps = fdf['datetime'].diff().dt.total_seconds() / 60
    fdf['session_id'] = (gaps > SESSION_GAP_MINUTES).cumsum()
    dist_to_end = fdf.groupby('session_id').cumcount(ascending=False)
    fdf['dist_to_end'] = dist_to_end

    # 预建期权时间索引
    opt_indexed = {}
    for sym, odf in options_data.items():
        odf = odf.copy()
        odf['datetime'] = pd.to_datetime(odf['datetime'])
        odf = odf.set_index('datetime').sort_index()
        opt_indexed[sym] = odf

    # 找入场点 + 获取期权价格（与阈值无关，只做一次）
    entry_points = []
    last_entry_idx = -entry_interval

    for idx in range(len(fdf)):
        if idx - last_entry_idx < entry_interval:
            continue

        row = fdf.iloc[idx]
        max_hold = max(hold_periods)
        if row['dist_to_end'] < max_hold:
            continue

        # 检查1m特征是否有效
        if np.isnan(cx_1m[idx]) or np.isnan(cx_5m[idx]):
            continue

        entry_time = row['datetime']
        underlying_price = row['close_price']

        call_sym, put_sym = find_otm_pair(opt_indexed, underlying_price, product, contract_yymm)
        if not call_sym or not put_sym:
            continue

        c_parsed = parse_option_symbol(call_sym, product)
        p_parsed = parse_option_symbol(put_sym, product)
        if not c_parsed or not p_parsed:
            continue

        call_df = opt_indexed.get(call_sym)
        put_df = opt_indexed.get(put_sym)
        if call_df is None or put_df is None:
            continue

        try:
            call_entry = call_df.loc[call_df.index.asof(entry_time)]
            put_entry = put_df.loc[put_df.index.asof(entry_time)]
        except (KeyError, ValueError):
            continue

        entry_call_price = call_entry['close']
        entry_put_price = put_entry['close']
        entry_sum = entry_call_price + entry_put_price
        if entry_sum <= 0 or entry_call_price <= 0 or entry_put_price <= 0:
            continue

        # 各持仓期 PnL
        pnls = {}
        for hp in hold_periods:
            exit_idx = idx + hp
            if exit_idx >= len(fdf):
                pnls[hp] = None
                continue
            exit_time = fdf.iloc[exit_idx]['datetime']
            try:
                call_exit = call_df.loc[call_df.index.asof(exit_time)]
                put_exit = put_df.loc[put_df.index.asof(exit_time)]
            except (KeyError, ValueError):
                pnls[hp] = None
                continue
            exit_sum = call_exit['close'] + put_exit['close']
            pnl_pct = (entry_sum - exit_sum) / entry_sum * 100 if entry_sum > 0 else 0
            pnls[hp] = round(pnl_pct, 2)

        entry_points.append({
            'idx': idx,
            'cx_1m': float(cx_1m[idx]),
            'bias_1m': float(bias_1m[idx]),
            'ar_1m': float(ar_1m[idx]),
            'dr40_1m': float(dr40_1m[idx]) if not np.isnan(dr40_1m[idx]) else 999.0,
            'cb40_1m': float(cb40_1m[idx]) if not np.isnan(cb40_1m[idx]) else 1.0,
            'cx_5m': float(cx_5m[idx]),
            'bias_5m': float(bias_5m[idx]) if not np.isnan(bias_5m[idx]) else 0.5,
            'ar_5m': float(ar_5m[idx]) if not np.isnan(ar_5m[idx]) else 0.5,
            'dr40_5m': float(dr40_5m[idx]) if not np.isnan(dr40_5m[idx]) else 999.0,
            'cb40_5m': float(cb40_5m[idx]) if not np.isnan(cb40_5m[idx]) else 1.0,
            'pnls': pnls,
        })
        last_entry_idx = idx

    if not entry_points:
        return {}

    # 对每个 cross_threshold 分别标记状态并统计
    grid_results = {}
    for ct in CROSS_THRESHOLD_GRID:
        trades_by_state = defaultdict(list)  # state -> list of pnl dicts

        for ep in entry_points:
            # 1m 状态
            if ep['cx_1m'] >= ct:
                s1 = 'entangled'
            elif ep['bias_1m'] >= BIAS_THRESHOLD:
                ma40_trend = (ep['dr40_1m'] > MA40_DIST_RATIO) or (ep['cb40_1m'] > MA40_COUNT_BIAS)
                if ma40_trend:
                    s1 = 'trending_up' if ep['ar_1m'] > 0.5 else 'trending_down'
                else:
                    s1 = 'entangled'
            else:
                s1 = 'entangled'

            # 5m 状态
            if ep['cx_5m'] >= ct:
                s5 = 'entangled'
            elif ep['bias_5m'] >= BIAS_THRESHOLD:
                ma40_trend_5 = (ep['dr40_5m'] > MA40_DIST_RATIO) or (ep['cb40_5m'] > MA40_COUNT_BIAS)
                if ma40_trend_5:
                    s5 = 'trending_up' if ep['ar_5m'] > 0.5 else 'trending_down'
                else:
                    s5 = 'entangled'
            else:
                s5 = 'entangled'

            # 合并
            if s1 == 'warmup' or s5 == 'warmup':
                combined = 'warmup'
            elif s1 == 'entangled' and s5 == 'entangled':
                combined = 'both_entangled'
            elif s1.startswith('trending') and s5.startswith('trending'):
                combined = 'both_trending'
            else:
                combined = 'mixed'

            trades_by_state[combined].append(ep['pnls'])

        # 统计
        ct_stats = {'cross_threshold': ct}
        total_entries = len(entry_points)
        state_counts = {s: len(ts) for s, ts in trades_by_state.items()}
        ct_stats['state_distribution'] = state_counts
        ct_stats['entangled_pct'] = round(state_counts.get('both_entangled', 0) / total_entries * 100, 1) if total_entries > 0 else 0

        for hp in hold_periods:
            hp_stats = {}
            for state_name in ['both_entangled', 'mixed', 'both_trending']:
                pnl_list = [t[hp] for t in trades_by_state.get(state_name, []) if t.get(hp) is not None]
                if len(pnl_list) < 5:
                    continue
                pnls = np.array(pnl_list)
                hp_stats[state_name] = {
                    'n': len(pnls),
                    'mean_pnl_pct': round(float(np.mean(pnls)), 3),
                    'median_pnl_pct': round(float(np.median(pnls)), 3),
                    'win_rate': round(float((pnls > 0).mean() * 100), 1),
                    'p25': round(float(np.percentile(pnls, 25)), 3),
                    'p75': round(float(np.percentile(pnls, 75)), 3),
                    'worst': round(float(np.min(pnls)), 3),
                    'best': round(float(np.max(pnls)), 3),
                    'tail_5pct': round(float(np.percentile(pnls, 5)), 3),
                }

            ct_stats[f'hold_{hp}m'] = hp_stats

            ent = hp_stats.get('both_entangled', {})
            trn = hp_stats.get('both_trending', {})
            if ent and trn:
                diff = ent['mean_pnl_pct'] - trn['mean_pnl_pct']
                ct_stats[f'hold_{hp}m_belief'] = {
                    'entangled_better_by': round(diff, 3),
                    'holds': diff > 0,
                }

        grid_results[ct] = ct_stats

    return grid_results


def process_product(product):
    import pyarrow.parquet as pq

    exchange = EXCHANGE_MAP.get(product.lower(), '')
    prod_upper = product.upper()
    print(f'\n{"="*60}')
    print(f'B045 CrossCount: {prod_upper} ({exchange})')
    print(f'{"="*60}')

    fdf_all = load_futures(product)
    if fdf_all.empty:
        print(f'  No futures data')
        return None

    fdf_all['datetime'] = pd.to_datetime(fdf_all['datetime'])
    print(f'  Futures: {len(fdf_all["symbol"].unique())} contracts, {len(fdf_all)} bars')

    opt_base = os.path.join(OPTIONS_DIR, exchange, prod_upper)
    if not os.path.isdir(opt_base):
        opt_base = os.path.join(OPTIONS_DIR, exchange, product.lower())
    if not os.path.isdir(opt_base):
        print(f'  No options directory')
        return None

    month_dirs = sorted([d for d in os.listdir(opt_base) if re.match(r'\d{4}-\d{2}', d)])
    print(f'  Option months: {len(month_dirs)} ({month_dirs[0]}~{month_dirs[-1]})')

    all_grid_results = {ct: defaultdict(list) for ct in CROSS_THRESHOLD_GRID}
    total_entries = 0

    for month_dir in month_dirs:
        month_path = os.path.join(opt_base, month_dir)
        year, mon = month_dir.split('-')
        month_start = pd.Timestamp(f'{year}-{mon}-01')
        if int(mon) == 12:
            month_end = pd.Timestamp(f'{int(year)+1}-01-01')
        else:
            month_end = pd.Timestamp(f'{year}-{int(mon)+1:02d}-01')

        mask = (fdf_all['datetime'] >= month_start) & (fdf_all['datetime'] < month_end)
        fdf_month = fdf_all[mask]
        if fdf_month.empty:
            continue

        vol_by_contract = fdf_month.groupby('symbol')['volume'].sum()
        main_contract = vol_by_contract.idxmax()
        cdf = fdf_month[fdf_month['symbol'] == main_contract].copy()
        if len(cdf) < 300:
            continue

        contract_yymm = main_contract[len(prod_upper):]

        opt_files = [f for f in os.listdir(month_path) if f.endswith('.parquet')]
        opt_yymm = _contract_yymm_for_exchange(contract_yymm, exchange)
        options = {}
        for fname in opt_files:
            sym = fname.replace('.parquet', '')
            clean = sym.split('.')[-1]
            if exchange in ('DCE', 'GFEX'):
                pat = rf'^(?:{prod_upper}|{product.lower()}){opt_yymm}-[CP]-\d+$'
            else:
                pat = rf'^(?:{prod_upper}|{product.lower()}){opt_yymm}[CP]\d+$'
            if not re.match(pat, clean, re.IGNORECASE):
                continue
            try:
                odf = pq.read_table(os.path.join(month_path, fname),
                                    columns=['datetime', 'close', 'volume']).to_pandas()
            except Exception:
                continue
            if odf.empty:
                continue
            odf['symbol'] = clean
            options[clean] = odf

        if not options:
            continue

        print(f'  {month_dir} [{main_contract}]: {len(cdf)} bars, {len(options)} options')

        contract_grid = backtest_single_contract(
            cdf, options, product, contract_yymm,
            HOLD_PERIODS, ENTRY_INTERVAL
        )

        # 合并到全品种 grid 结果
        for ct, ct_stats in contract_grid.items():
            for hp in HOLD_PERIODS:
                hp_key = f'hold_{hp}m'
                for state_name in ['both_entangled', 'mixed', 'both_trending']:
                    s = ct_stats.get(hp_key, {}).get(state_name)
                    if s:
                        all_grid_results[ct][(hp, state_name)].extend(
                            [s['mean_pnl_pct']] * s['n'])

            dist = ct_stats.get('state_distribution', {})
            for state_name, count in dist.items():
                all_grid_results[ct][('count', state_name)] = \
                    all_grid_results[ct].get(('count', state_name), 0) + count

        del cdf, options, fdf_month
        gc.collect()

    # 汇总各阈值的统计
    result = {
        'product': prod_upper,
        'exchange': exchange,
        'params': {
            'ma_period': MA_PERIOD, 'ma_period_40': MA_PERIOD_40,
            'lookback_1m': LOOKBACK_1M, 'lookback_5m': LOOKBACK_5M,
            'bias_threshold': BIAS_THRESHOLD,
            'ma40_dist_ratio': MA40_DIST_RATIO, 'ma40_count_bias': MA40_COUNT_BIAS,
            'cross_threshold_grid': CROSS_THRESHOLD_GRID,
            'hold_periods': HOLD_PERIODS, 'entry_interval': ENTRY_INTERVAL,
        },
    }

    best_ct = None
    best_diff = -float('inf')

    for ct in CROSS_THRESHOLD_GRID:
        gdata = all_grid_results[ct]
        ct_result = {'cross_threshold': ct}

        total = sum(gdata.get(('count', s), 0) for s in ['both_entangled', 'mixed', 'both_trending', 'warmup'])
        ent_count = gdata.get(('count', 'both_entangled'), 0)
        ct_result['total_entries'] = total
        ct_result['entangled_pct'] = round(ent_count / total * 100, 1) if total > 0 else 0
        ct_result['state_distribution'] = {
            s: gdata.get(('count', s), 0) for s in ['both_entangled', 'mixed', 'both_trending']
        }

        for hp in HOLD_PERIODS:
            hp_stats = {}
            for state_name in ['both_entangled', 'mixed', 'both_trending']:
                pnl_list = gdata.get((hp, state_name), [])
                if len(pnl_list) < 5:
                    continue
                pnls = np.array(pnl_list)
                hp_stats[state_name] = {
                    'n': len(pnls),
                    'mean_pnl_pct': round(float(np.mean(pnls)), 3),
                    'median_pnl_pct': round(float(np.median(pnls)), 3),
                    'win_rate': round(float((pnls > 0).mean() * 100), 1),
                    'p25': round(float(np.percentile(pnls, 25)), 3),
                    'p75': round(float(np.percentile(pnls, 75)), 3),
                    'worst': round(float(np.min(pnls)), 3),
                    'best': round(float(np.max(pnls)), 3),
                    'tail_5pct': round(float(np.percentile(pnls, 5)), 3),
                }

            ct_result[f'hold_{hp}m'] = hp_stats

            ent = hp_stats.get('both_entangled', {})
            trn = hp_stats.get('both_trending', {})
            if ent and trn:
                diff = ent['mean_pnl_pct'] - trn['mean_pnl_pct']
                ct_result[f'hold_{hp}m_belief'] = {
                    'entangled_better_by': round(diff, 3),
                    'holds': diff > 0,
                }

        result[f'ct_{ct}'] = ct_result

        # 找最优（以 hold_30m 的 diff 为准）
        belief = ct_result.get('hold_30m_belief', {})
        diff_30 = belief.get('entangled_better_by', -999)
        if diff_30 > best_diff:
            best_diff = diff_30
            best_ct = ct

    result['best_cross_threshold'] = best_ct
    result['best_diff_30m'] = round(best_diff, 3)

    # 打印汇总
    print(f'\n  Grid Summary ({prod_upper}):')
    print(f'  {"CT":>4s} {"Ent%":>6s} {"EntN":>6s} {"EntPnL":>8s} {"TrnN":>6s} {"TrnPnL":>8s} {"Diff":>8s} {"OK":>4s}')
    for ct in CROSS_THRESHOLD_GRID:
        cr = result.get(f'ct_{ct}', {})
        b30 = cr.get('hold_30m', {})
        ent = b30.get('both_entangled', {})
        trn = b30.get('both_trending', {})
        belief = cr.get('hold_30m_belief', {})
        diff = belief.get('entangled_better_by', 0)
        ok = 'Y' if belief.get('holds', False) else 'N'
        marker = ' <<<' if ct == best_ct else ''
        print(f'  {ct:>4d} {cr.get("entangled_pct", 0):>6.1f} {ent.get("n", 0):>6d} '
              f'{ent.get("mean_pnl_pct", 0):>8.3f} {trn.get("n", 0):>6d} '
              f'{trn.get("mean_pnl_pct", 0):>8.3f} {diff:>+8.3f} {ok:>4s}{marker}')

    return result


def _run_one(product):
    try:
        return (product.upper(), process_product(product))
    except Exception as e:
        import traceback
        traceback.print_exc()
        return (product.upper(), None)


# ============ HTML 报告 ============

def generate_report(all_results, output_path):
    from datetime import datetime

    products = sorted(all_results.keys())
    hp = HOLD_PERIODS[0]

    # 热力图数据: 横轴=品种, 纵轴=ct, 值=diff
    heatmap_data = []
    for pi, prod in enumerate(products):
        r = all_results[prod]
        for ct in CROSS_THRESHOLD_GRID:
            cr = r.get(f'ct_{ct}', {})
            b = cr.get(f'hold_{hp}m_belief', {})
            diff = b.get('entangled_better_by', 0)
            heatmap_data.append([pi, CROSS_THRESHOLD_GRID.index(ct), round(diff, 3)])

    # 每品种最优 CT
    best_rows = []
    for prod in products:
        r = all_results[prod]
        best_ct = r.get('best_cross_threshold', '?')
        best_diff = r.get('best_diff_30m', 0)

        best_cr = r.get(f'ct_{best_ct}', {})
        ent_pct = best_cr.get('entangled_pct', 0)
        b30 = best_cr.get(f'hold_{hp}m', {})
        ent = b30.get('both_entangled', {})
        trn = b30.get('both_trending', {})
        belief = best_cr.get(f'hold_{hp}m_belief', {})

        best_rows.append({
            'prod': prod,
            'best_ct': best_ct,
            'ent_pct': ent_pct,
            'ent_n': ent.get('n', 0),
            'ent_pnl': ent.get('mean_pnl_pct', 0),
            'ent_wr': ent.get('win_rate', 0),
            'ent_tail': ent.get('tail_5pct', 0),
            'trn_n': trn.get('n', 0),
            'trn_pnl': trn.get('mean_pnl_pct', 0),
            'trn_wr': trn.get('win_rate', 0),
            'trn_tail': trn.get('tail_5pct', 0),
            'diff': best_diff,
            'holds': belief.get('holds', False),
        })

    best_rows.sort(key=lambda x: -x['diff'])
    n_holds = sum(1 for r in best_rows if r['holds'])
    avg_diff = np.mean([r['diff'] for r in best_rows if r['ent_n'] > 0 and r['trn_n'] > 0]) if best_rows else 0

    # CT 分布统计
    ct_vote = defaultdict(int)
    for r in best_rows:
        ct_vote[r['best_ct']] += 1

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>B045: MA纠缠度重新定义 — 穿越次数网格测试</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
body {{ background: #1a1a2e; color: #ddd; font-family: 'Segoe UI', sans-serif; margin: 0; padding: 20px; }}
.header {{ text-align: center; padding: 20px; border-bottom: 3px solid #4fc3f7; margin-bottom: 20px; }}
.header h1 {{ color: #4fc3f7; margin: 0; }}
.header p {{ color: #888; font-size: 14px; }}
.card-row {{ display: flex; gap: 12px; flex-wrap: wrap; justify-content: center; margin-bottom: 20px; }}
.card {{ background: #16213e; border-radius: 8px; padding: 16px 24px; text-align: center; min-width: 140px; }}
.card .val {{ font-size: 28px; font-weight: bold; }}
.card .label {{ font-size: 12px; color: #888; margin-top: 4px; }}
table {{ width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 13px; }}
th {{ background: #16213e; color: #888; padding: 8px; text-align: left; border-bottom: 2px solid #333; }}
td {{ padding: 6px 8px; border-bottom: 1px solid #2a2a4a; }}
.chart-box {{ background: #16213e; border-radius: 8px; padding: 10px; display: inline-block; margin: 8px; }}
h2 {{ color: #4fc3f7; margin-top: 30px; }}
</style>
</head><body>

<div class="header">
  <h1>B045: MA纠缠度重新定义 — 穿越次数网格测试</h1>
  <p>三维判定: 穿越次数(CrossCount) + 偏向比例(Bias≥{BIAS_THRESHOLD}) + 40MA等距确认(DistRatio≥{MA40_DIST_RATIO})</p>
  <p>MA{MA_PERIOD} + MA{MA_PERIOD_40}确认 | 1m窗口{LOOKBACK_1M} + 5m窗口{LOOKBACK_5M} | CrossThreshold网格: {CROSS_THRESHOLD_GRID}</p>
  <p>持仓{hp}分钟 | 卖出OTM宽跨(4-8%虚值)</p>
  <p>生成: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
</div>

<div class="card-row">
  <div class="card"><div class="val" style="color:#4fc3f7">{len(products)}</div><div class="label">品种数</div></div>
  <div class="card"><div class="val" style="color:#00e676">{n_holds}</div><div class="label">信念成立(最优CT)</div></div>
  <div class="card"><div class="val" style="color:#FF4444">{len(products) - n_holds}</div><div class="label">信念不成立</div></div>
  <div class="card"><div class="val" style="color:#FFD700">{avg_diff:.2f}%</div><div class="label">平均优势</div></div>
  <div class="card"><div class="val" style="color:#FF69B4">{max(ct_vote, key=ct_vote.get, default='?')}</div><div class="label">最优CT众数</div></div>
</div>

<div style="text-align:center">
  <div class="chart-box"><div id="heatmap" style="width:1000px;height:400px"></div></div>
</div>

<h2>各品种最优穿越阈值 (持仓{hp}分钟)</h2>
<table>
<thead><tr>
  <th>#</th><th>品种</th><th>最优CT</th><th>纠缠%</th>
  <th colspan="4" style="color:#00e676">纠缠时卖</th>
  <th colspan="3" style="color:#FF4444">趋势时卖</th>
  <th>优势</th><th>结论</th>
</tr>
<tr>
  <th></th><th></th><th></th><th></th>
  <th>N</th><th>PnL%</th><th>WR%</th><th>Tail5%</th>
  <th>N</th><th>PnL%</th><th>WR%</th>
  <th>差值</th><th></th>
</tr>
</thead>
<tbody>"""

    for i, r in enumerate(best_rows):
        cls = 'color:#00e676' if r['holds'] else 'color:#FF4444'
        verdict = 'Y' if r['holds'] else 'N'
        html += f"""<tr>
  <td>{i+1}</td>
  <td style="font-weight:bold;color:#FFD700">{r['prod']}</td>
  <td style="color:#4fc3f7;font-weight:bold">{r['best_ct']}</td>
  <td>{r['ent_pct']:.1f}%</td>
  <td>{r['ent_n']}</td>
  <td style="color:{'#00e676' if r['ent_pnl']>0 else '#FF4444'}">{r['ent_pnl']:.3f}</td>
  <td>{r['ent_wr']:.1f}</td>
  <td style="color:{'#00e676' if r['ent_tail']>-1 else '#FF4444'}">{r['ent_tail']:.2f}</td>
  <td>{r['trn_n']}</td>
  <td style="color:{'#00e676' if r['trn_pnl']>0 else '#FF4444'}">{r['trn_pnl']:.3f}</td>
  <td>{r['trn_wr']:.1f}</td>
  <td style="font-weight:bold;{cls}">{r['diff']:+.3f}%</td>
  <td style="font-weight:bold;{cls}">{verdict}</td>
</tr>"""

    html += """</tbody></table>"""

    # 穿越阈值投票表
    html += f"""
<h2>穿越阈值投票（各品种最优CT分布）</h2>
<table style="width:auto">
<thead><tr><th>CrossThreshold</th><th>品种数</th><th>占比</th></tr></thead>
<tbody>"""
    for ct in CROSS_THRESHOLD_GRID:
        cnt = ct_vote.get(ct, 0)
        pct = cnt / len(products) * 100 if products else 0
        html += f'<tr><td style="color:#4fc3f7;font-weight:bold">{ct}</td><td>{cnt}</td><td>{pct:.0f}%</td></tr>'
    html += '</tbody></table>'

    # Heatmap chart
    html += """
<script>
var chart = echarts.init(document.getElementById('heatmap'));
chart.setOption(%s);
</script>""" % json.dumps({
        'title': {'text': f'穿越阈值 × 品种 热力图 (纠缠-趋势 PnL差, {hp}m)', 'textStyle': {'color': '#ddd', 'fontSize': 14}},
        'tooltip': {'position': 'top', 'formatter': '{c}'},
        'grid': {'left': 60, 'right': 50, 'top': 50, 'bottom': 80},
        'xAxis': {'type': 'category', 'data': list(products),
                  'axisLabel': {'color': '#888', 'rotate': 45, 'fontSize': 11}},
        'yAxis': {'type': 'category', 'data': [str(ct) for ct in CROSS_THRESHOLD_GRID],
                  'name': 'CrossThreshold', 'axisLabel': {'color': '#888'}},
        'visualMap': {'min': -1.5, 'max': 1.5, 'calculable': True,
                      'orient': 'horizontal', 'left': 'center', 'bottom': 5,
                      'inRange': {'color': ['#FF4444', '#2a2a4a', '#00e676']},
                      'textStyle': {'color': '#888'}},
        'series': [{
            'name': 'PnL Diff', 'type': 'heatmap', 'data': heatmap_data,
            'label': {'show': True, 'color': '#ddd', 'fontSize': 10,
                      'formatter': '{@[2]}'},
        }]
    })

    html += """
<details style="margin-top:20px"><summary style="cursor:pointer;color:#888">原始JSON</summary>
<pre style="background:#16213e;padding:12px;border-radius:6px;overflow-x:auto;font-size:11px">"""
    html += json.dumps(all_results, ensure_ascii=False, indent=2, default=str)
    html += "</pre></details></body></html>"

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'\nReport: {output_path}')


# ============ 主入口 ============

def main():
    parser = argparse.ArgumentParser(description='B045: MA纠缠度重新定义 — 穿越次数网格测试')
    parser.add_argument('--product', default='ag', help='品种代码, 逗号分隔')
    parser.add_argument('--all', action='store_true', help='全品种')
    parser.add_argument('--workers', type=int, default=0, help='并行进程数')
    parser.add_argument('--output', default=None, help='输出目录')
    args = parser.parse_args()

    if args.all:
        products = sorted(EXCHANGE_MAP.keys())
    else:
        products = [p.strip().lower() for p in args.product.split(',')]

    output_dir = args.output or RESULT_DIR
    os.makedirs(output_dir, exist_ok=True)

    print(f'B045: MA纠缠度重新定义 — 穿越次数网格测试')
    print(f'Products: {", ".join(p.upper() for p in products)}')
    print(f'MA{MA_PERIOD} + MA{MA_PERIOD_40} | 1m-LB{LOOKBACK_1M} + 5m-LB{LOOKBACK_5M}')
    print(f'Bias≥{BIAS_THRESHOLD} | MA40 DistRatio≥{MA40_DIST_RATIO} CountBias≥{MA40_COUNT_BIAS}')
    print(f'CrossThreshold Grid: {CROSS_THRESHOLD_GRID}')
    print(f'Hold: {HOLD_PERIODS}min, Entry interval: {ENTRY_INTERVAL}min')

    n_workers = args.workers
    if n_workers <= 0:
        try:
            import psutil
            avail_gb = psutil.virtual_memory().available / (1024**3)
            n_workers = max(1, min(int(avail_gb / 3) - 1, os.cpu_count() - 1, len(products)))
        except ImportError:
            n_workers = min(4, len(products))

    all_results = {}

    if len(products) > 1 and n_workers > 1:
        from concurrent.futures import ProcessPoolExecutor, as_completed
        print(f'\nParallel: {n_workers} workers, {len(products)} products')
        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            futures = {pool.submit(_run_one, p): p for p in products}
            for fut in as_completed(futures):
                prod, result = fut.result()
                if result:
                    all_results[prod] = result
                    with open(os.path.join(output_dir, f'{prod}_crosscount.json'), 'w') as f:
                        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    else:
        for product in products:
            prod, result = _run_one(product)
            if result:
                all_results[prod] = result
                with open(os.path.join(output_dir, f'{prod}_crosscount.json'), 'w') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2, default=str)
            gc.collect()

    if not all_results:
        print('\nNo results')
        return

    summary_path = os.path.join(output_dir, '_DIRECT_SUMMARY.json')
    with open(summary_path, 'w') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)

    report_path = os.path.join(output_dir, 'direct_report.html')
    generate_report(all_results, report_path)

    # 打印最终汇总
    hp = HOLD_PERIODS[0]
    print(f'\n{"="*60}')
    print(f'B045 汇总 (持仓{hp}分钟):')
    print(f'{"Prod":>6s} {"BestCT":>7s} {"Ent%":>6s} {"Diff":>8s} {"OK":>4s}')
    holds = 0
    for prod in sorted(all_results.keys()):
        r = all_results[prod]
        bct = r.get('best_cross_threshold', '?')
        bdiff = r.get('best_diff_30m', 0)
        cr = r.get(f'ct_{bct}', {})
        ent_pct = cr.get('entangled_pct', 0)
        ok = bdiff > 0
        if ok:
            holds += 1
        print(f'{prod:>6s} {bct:>7s} {ent_pct:>6.1f} {bdiff:>+8.3f} {"Y" if ok else "N":>4s}')
    print(f'\n信念成立: {holds}/{len(all_results)}')

    # CT 投票
    ct_vote = defaultdict(int)
    for prod, r in all_results.items():
        bct = r.get('best_cross_threshold')
        if bct is not None:
            ct_vote[bct] += 1
    print(f'\nCT投票: {dict(sorted(ct_vote.items()))}')


if __name__ == '__main__':
    main()
