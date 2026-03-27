#!/usr/bin/env python3
"""
B047: 趋势→纠缠转换瞬间卖出宽跨 — 时段内持仓回测

策略:
  1. 检测MA状态从 trending → entangled/transition 的转换瞬间
  2. 布林线lite条件: price_sum > 双中轨之和 * (1 + boll_lite_pct)
  3. 卖出宽跨，持仓到当前交易时段收盘
  4. 期间止盈止损: 百分比止盈 + 腿比止损

基于B046框架，复用其MA状态计算、数据加载、OTM选对逻辑。

用法:
  python3 B047_transition_session_backtest.py --product ag,y,m
  python3 B047_transition_session_backtest.py --all --workers 4
"""

import argparse
import gc
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from itertools import product as iterproduct

import numpy as np
import pandas as pd

# ============ MA 参数 (MA10替代MA20, 更贴近文华财经实际观察) ============
MA_PERIOD = 10
MA_PERIOD_40 = 40
LOOKBACK_1M = 50
LOOKBACK_5M = 20
CROSS_THRESHOLD = 3
BIAS_THRESHOLD = 0.8
MA40_DIST_RATIO = 2.0
MA40_COUNT_BIAS = 0.7

CONSEC_1M = 8
CONSEC_5M = 4
TRANSITION_WINDOW_1M = 10
TRANSITION_WINDOW_5M = 5

# ============ B047 专有参数 ============
BOLL_PERIOD = 26
BOLL_LITE_PCTS = [0.0, 0.02, 0.05]
TP_PCTS = [0.05, 0.10, 0.15]
SL_RATIO = 2.0
MIN_HOLD_MINUTES = 15

SESSION_GAP_MINUTES = 30
OTM_MIN = 0.04
OTM_MAX = 0.08
OTM_FALLBACK_MIN = 0.03
OTM_FALLBACK_MAX = 0.10

FUTURES_DIR = '/mnt/d/backtest_data/Futures_parquet/'
OPTIONS_DIR = '/mnt/d/backtest_data/Options_parquet/'
if not os.path.exists('/mnt/d/'):
    FUTURES_DIR = os.path.expanduser('~/Downloads/期货数据_parquet/')
    OPTIONS_DIR = os.path.expanduser('~/Downloads/期权_parquet/')

RESULT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'B047_results')
RECENT_CONTRACTS = 0

EXCHANGE_MAP = {
    'ag': 'SHFE', 'au': 'SHFE', 'cu': 'SHFE', 'al': 'SHFE', 'zn': 'SHFE',
    'ni': 'SHFE', 'sn': 'SHFE', 'ru': 'SHFE', 'ao': 'SHFE',
    'rb': 'SHFE', 'pb': 'SHFE', 'ad': 'SHFE', 'br': 'SHFE',
    'p': 'DCE', 'm': 'DCE', 'y': 'DCE', 'i': 'DCE',
    'jd': 'DCE', 'lh': 'DCE', 'pp': 'DCE', 'l': 'DCE',
    'v': 'DCE', 'eb': 'DCE', 'eg': 'DCE', 'c': 'DCE', 'pg': 'DCE',
    'sa': 'CZCE', 'fg': 'CZCE', 'ta': 'CZCE', 'ma': 'CZCE', 'cf': 'CZCE',
    'sr': 'CZCE', 'rm': 'CZCE', 'oi': 'CZCE', 'ur': 'CZCE', 'pf': 'CZCE',
    'sh': 'CZCE', 'pk': 'CZCE', 'sm': 'CZCE', 'sf': 'CZCE', 'px': 'CZCE',
    'ap': 'CZCE', 'cj': 'CZCE',
    'si': 'GFEX', 'lc': 'GFEX',
    'sc': 'INE',
}


# ============ 数据加载 (复用B046) ============

def load_futures(product):
    import pyarrow.parquet as pq
    exchange = EXCHANGE_MAP.get(product.lower(), '')
    if not exchange:
        return pd.DataFrame()
    prod_upper = product.upper()

    path_flat = os.path.join(FUTURES_DIR, exchange, f'{prod_upper}.parquet')
    if os.path.exists(path_flat):
        df = pq.read_table(path_flat,
                           columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'symbol']).to_pandas()
        df.rename(columns={'open': 'open_price', 'close': 'close_price',
                           'high': 'high_price', 'low': 'low_price'}, inplace=True)
        pattern = rf'^{prod_upper}\d{{3,4}}$'
        return df[df['symbol'].str.match(pattern, na=False)].copy()

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
            return pd.concat(all_dfs, ignore_index=True)
    return pd.DataFrame()


def load_options_flat_for_contract(product, contract_yymm, start_dt, end_dt):
    import pyarrow.parquet as pq
    exchange = EXCHANGE_MAP.get(product.lower(), '')
    prod_upper = product.upper()
    prod_lower = product.lower()
    opt_yymm = contract_yymm[1:] if exchange == 'CZCE' else contract_yymm

    path = os.path.join(OPTIONS_DIR, exchange, f'{prod_upper}.parquet')
    if not os.path.exists(path):
        path = os.path.join(OPTIONS_DIR, exchange, f'{prod_lower}.parquet')
    if not os.path.exists(path):
        return {}

    start_dt = pd.to_datetime(start_dt).to_pydatetime()
    end_dt = pd.to_datetime(end_dt).to_pydatetime()
    try:
        table = pq.read_table(
            path,
            columns=['datetime', 'close', 'volume', 'symbol'],
            filters=[('datetime', '>=', start_dt), ('datetime', '<', end_dt)],
        )
    except Exception:
        table = pq.read_table(path, columns=['datetime', 'close', 'volume', 'symbol'])
    df = table.to_pandas()
    if df.empty:
        return {}
    df['datetime'] = pd.to_datetime(df['datetime'])

    if exchange in ('DCE', 'GFEX'):
        pat = rf'^(?:(?:CZCE|SHFE|DCE|INE|GFEX|CFFEX)\.)?(?:{prod_upper}|{prod_lower}){opt_yymm}-[CP]-\d+$'
    else:
        pat = rf'^(?:(?:CZCE|SHFE|DCE|INE|GFEX|CFFEX)\.)?(?:{prod_upper}|{prod_lower}){opt_yymm}[CP]\d+$'
    mask = df['symbol'].str.match(pat, na=False)
    df = df[mask].copy()
    if df.empty:
        return {}

    result = {}
    for sym, grp in df.groupby('symbol', sort=False):
        clean = sym.split('.')[-1] if '.' in sym else sym
        result[clean] = grp[['datetime', 'close', 'volume']].copy()
    return result


def list_option_yymms_flat(product):
    import pyarrow.parquet as pq
    exchange = EXCHANGE_MAP.get(product.lower(), '')
    prod_upper = product.upper()
    prod_lower = product.lower()

    path = os.path.join(OPTIONS_DIR, exchange, f'{prod_upper}.parquet')
    if not os.path.exists(path):
        path = os.path.join(OPTIONS_DIR, exchange, f'{prod_lower}.parquet')
    if not os.path.exists(path):
        return set()

    df = pq.read_table(path, columns=['symbol']).to_pandas()
    yymms = set()
    for sym in df['symbol'].dropna().unique():
        parsed = parse_option_symbol(sym, product)
        if parsed:
            yymms.add(parsed[0])
    return yymms


def load_options_months(product, month_dir_path, contract_yymm):
    import pyarrow.parquet as pq
    exchange = EXCHANGE_MAP.get(product.lower(), '')
    prod_upper = product.upper()
    opt_yymm = contract_yymm[1:] if exchange == 'CZCE' else contract_yymm

    opt_files = [f for f in os.listdir(month_dir_path) if f.endswith('.parquet')]
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
            odf = pq.read_table(os.path.join(month_dir_path, fname),
                                columns=['datetime', 'close', 'volume']).to_pandas()
        except Exception:
            continue
        if odf.empty:
            continue
        odf['symbol'] = clean
        options[clean] = odf
    return options


def parse_option_symbol(sym, product):
    prod_upper = product.upper()
    prod_lower = product.lower()
    clean = sym.split('.')[-1] if '.' in sym else sym
    m = re.match(rf'^(?:(?:CZCE|SHFE|DCE|INE|GFEX|CFFEX)\.)?(?:{prod_upper}|{prod_lower})(\d{{3,4}})-([CP])-(\d+)$', clean, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2).upper(), int(m.group(3))
    m = re.match(rf'^(?:(?:CZCE|SHFE|DCE|INE|GFEX|CFFEX)\.)?(?:{prod_upper}|{prod_lower})(\d{{3,4}})([CP])(\d+)$', clean, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2).upper(), int(m.group(3))
    return None


# ============ MA 状态计算 (复用B046) ============

def _compute_raw_features(close, ma_period, lookback):
    n = len(close)
    ma = pd.Series(close).rolling(ma_period, min_periods=ma_period).mean().values
    diff = close - ma
    sign = np.sign(diff)
    above = (close > ma).astype(np.float64)
    below = (close < ma).astype(np.float64)

    crossings_arr = np.full(n, np.nan)
    bias_arr = np.full(n, np.nan)
    above_ratio_arr = np.full(n, np.nan)
    recent_consec_arr = np.zeros(n, dtype=np.int32)
    dev_growing_arr = np.zeros(n, dtype=np.bool_)
    longest_streak_arr = np.zeros(n, dtype=np.int32)
    longest_ended_arr = np.full(n, 999, dtype=np.int32)
    longest_sign_arr = np.zeros(n, dtype=np.int8)
    post_cx_arr = np.zeros(n, dtype=np.int32)
    post_opp_arr = np.zeros(n, dtype=np.int32)

    start = ma_period + lookback - 1
    if start >= n:
        return (crossings_arr, bias_arr, above_ratio_arr, ma,
                recent_consec_arr, dev_growing_arr,
                longest_streak_arr, longest_ended_arr,
                longest_sign_arr, post_cx_arr, post_opp_arr)

    for i in range(start, n):
        ws = sign[i - lookback + 1: i + 1]
        sd = np.diff(ws)
        cx = int(np.count_nonzero(sd != 0))
        crossings_arr[i] = cx

        ab_c = float(np.sum(above[i - lookback + 1: i + 1]))
        bl_c = float(np.sum(below[i - lookback + 1: i + 1]))
        total = ab_c + bl_c
        if total > 0:
            ar = ab_c / total
            bias_arr[i] = max(ar, 1.0 - ar)
            above_ratio_arr[i] = ar
        else:
            bias_arr[i] = 0.5
            above_ratio_arr[i] = 0.5

        last_s = ws[-1]
        rc = 0
        if last_s != 0:
            for k in range(lookback - 1, -1, -1):
                if ws[k] == last_s:
                    rc += 1
                else:
                    break
        recent_consec_arr[i] = rc

        if rc >= 3:
            nn = min(rc, lookback)
            dev = np.abs(close[i - nn + 1: i + 1] - ma[i - nn + 1: i + 1])
            if len(dev) >= 3:
                slope = np.polyfit(np.arange(len(dev)), dev, 1)[0]
                dev_growing_arr[i] = slope > 0

        runs = []
        rsk = 0
        for k2 in range(1, lookback):
            if ws[k2] != ws[rsk] or ws[k2] == 0:
                if ws[rsk] != 0:
                    runs.append((rsk, k2 - rsk, int(ws[rsk])))
                rsk = k2
        if ws[rsk] != 0:
            runs.append((rsk, lookback - rsk, int(ws[rsk])))

        if runs:
            best = max(runs, key=lambda r: r[1])
            longest_streak_arr[i] = best[1]
            lend = best[0] + best[1]
            longest_ended_arr[i] = lookback - lend
            longest_sign_arr[i] = best[2]
            if lend < lookback:
                post_sec = ws[lend:]
                if len(post_sec) > 1:
                    post_cx_arr[i] = int(np.count_nonzero(np.diff(post_sec) != 0))
                post_opp_arr[i] = int(np.sum(post_sec == -best[2]))

    return (crossings_arr, bias_arr, above_ratio_arr, ma,
            recent_consec_arr, dev_growing_arr,
            longest_streak_arr, longest_ended_arr,
            longest_sign_arr, post_cx_arr, post_opp_arr)


def _compute_ma40_equidist(close, ma40, lookback):
    n = len(close)
    dist_ratio_arr = np.full(n, np.nan)
    count_bias_arr = np.full(n, np.nan)
    start = 40 + lookback - 1
    if start >= n:
        return dist_ratio_arr, count_bias_arr
    for i in range(start, n):
        if np.isnan(ma40[i]):
            continue
        wc = close[i - lookback + 1: i + 1]
        wm = ma40[i - lookback + 1: i + 1]
        valid = ~np.isnan(wm)
        if valid.sum() < lookback * 0.5:
            continue
        wc2, wm2 = wc[valid], wm[valid]
        am, bm = wc2 > wm2, wc2 < wm2
        na, nb = int(am.sum()), int(bm.sum())
        da = float(np.mean(wc2[am] - wm2[am])) if na > 0 else 0.0
        db = float(np.mean(wm2[bm] - wc2[bm])) if nb > 0 else 0.0
        eps = 1e-10
        if da > eps and db > eps:
            dist_ratio_arr[i] = max(da, db) / min(da, db)
        elif da > eps or db > eps:
            dist_ratio_arr[i] = 999.0
        else:
            dist_ratio_arr[i] = 1.0
        total = na + nb
        count_bias_arr[i] = max(na, nb) / total if total > 0 else 0.5
    return dist_ratio_arr, count_bias_arr


def classify_new(cx, bi, ar, dr40, cb40, rc, dg, ls, lsa, lss, pcx, pop,
                 consec_th, trans_win):
    if np.isnan(cx) or np.isnan(bi):
        return 'warmup'
    if rc >= consec_th and dg:
        return 'trending_up' if ar > 0.5 else 'trending_down'
    if ls >= consec_th and 1 <= lsa <= trans_win:
        if lsa <= 2:
            return 'trans_touch'
        if pcx <= 1 and pop >= 2:
            return 'trans_cross'
        if pcx >= 2:
            return 'trans_oscillate'
    if int(cx) >= CROSS_THRESHOLD:
        return 'entangled'
    if bi >= BIAS_THRESHOLD:
        if (dr40 if not np.isnan(dr40) else 999) > MA40_DIST_RATIO or \
           (cb40 if not np.isnan(cb40) else 1.0) > MA40_COUNT_BIAS:
            return 'trending_up' if ar > 0.5 else 'trending_down'
    return 'entangled'


def _is_trending(state):
    return state in ('trending_up', 'trending_down')


def _is_safe(state):
    return state in ('entangled', 'trans_touch', 'trans_cross', 'trans_oscillate')


# ============ 布林线计算 (price_sum上的简化布林线) ============

def compute_boll_middle(values, period):
    """计算布林线中轨 (SMA)"""
    return pd.Series(values).rolling(period, min_periods=period).mean().values


# ============ OTM选对 (复用B046) ============

def find_otm_pair(options_indexed, underlying_price, product, contract_yymm):
    exchange = EXCHANGE_MAP.get(product.lower(), '')
    opt_yymm = contract_yymm[1:] if exchange == 'CZCE' else contract_yymm
    calls, puts = {}, {}
    for sym in options_indexed:
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

    def _find(lo, hi):
        cc = [(s, (s - underlying_price) / underlying_price) for s in calls if lo <= (s - underlying_price) / underlying_price <= hi]
        pp = [(s, (underlying_price - s) / underlying_price) for s in puts if lo <= (underlying_price - s) / underlying_price <= hi]
        if not cc or not pp:
            return None, None
        best, best_sc = None, float('inf')
        for cs, co in cc:
            for ps2, po in pp:
                a = abs(co - po)
                if a < best_sc:
                    best_sc = a
                    best = (calls[cs], puts[ps2])
        return best

    r = _find(OTM_MIN, OTM_MAX)
    if r and r[0] and r[1]:
        return r
    r = _find(OTM_FALLBACK_MIN, OTM_FALLBACK_MAX)
    if r and r[0] and r[1]:
        return r
    return None, None


# ============ 核心回测 ============

def _get_1m_state(idx, cx_1m, bi_1m, ar_1m, dr40_1m, cb40_1m,
                  rc_1m, dg_1m, ls_1m, lsa_1m, lss_1m, pcx_1m, pop_1m):
    return classify_new(cx_1m[idx], bi_1m[idx], ar_1m[idx],
                        dr40_1m[idx], cb40_1m[idx],
                        int(rc_1m[idx]), bool(dg_1m[idx]),
                        int(ls_1m[idx]), int(lsa_1m[idx]),
                        int(lss_1m[idx]), int(pcx_1m[idx]), int(pop_1m[idx]),
                        CONSEC_1M, TRANSITION_WINDOW_1M)


def _get_5m_state(idx, cx_5m, bi_5m, ar_5m, dr40_5m, cb40_5m,
                  rc_5m, dg_5m, ls_5m, lsa_5m, lss_5m, pcx_5m, pop_5m):
    return classify_new(cx_5m[idx], bi_5m[idx], ar_5m[idx],
                        dr40_5m[idx], cb40_5m[idx],
                        int(rc_5m[idx]), bool(dg_5m[idx]),
                        int(ls_5m[idx]), int(lsa_5m[idx]),
                        int(lss_5m[idx]), int(pcx_5m[idx]), int(pop_5m[idx]),
                        CONSEC_5M, TRANSITION_WINDOW_5M)


def backtest_contract(fdf, options_indexed, product, contract_yymm):
    fdf = fdf.sort_values('datetime').reset_index(drop=True)
    fdf['datetime'] = pd.to_datetime(fdf['datetime'])
    close = fdf['close_price'].values
    n = len(fdf)

    min_warmup = MA_PERIOD_40 * 5 + LOOKBACK_5M + 60
    if n < min_warmup:
        return []

    # --- MA 特征计算 (1m) ---
    cx_1m, bi_1m, ar_1m, ma20_1m, rc_1m, dg_1m, ls_1m, lsa_1m, lss_1m, pcx_1m, pop_1m = \
        _compute_raw_features(close, MA_PERIOD, LOOKBACK_1M)
    ma40_1m = pd.Series(close).rolling(MA_PERIOD_40, min_periods=MA_PERIOD_40).mean().values
    dr40_1m, cb40_1m = _compute_ma40_equidist(close, ma40_1m, LOOKBACK_1M)

    # --- MA 特征计算 (5m) ---
    df5 = fdf[['datetime', 'close_price']].copy()
    df5 = df5.set_index('datetime')
    r5 = df5['close_price'].resample('5min').last().dropna()
    r5v = r5.values

    has_5m = len(r5v) >= MA_PERIOD_40 + LOOKBACK_5M
    if has_5m:
        cx_5mr, bi_5mr, ar_5mr, _, rc_5mr, dg_5mr, ls_5mr, lsa_5mr, lss_5mr, pcx_5mr, pop_5mr = \
            _compute_raw_features(r5v, MA_PERIOD, LOOKBACK_5M)
        ma40_5m = pd.Series(r5v).rolling(MA_PERIOD_40, min_periods=MA_PERIOD_40).mean().values
        dr40_5mr, cb40_5mr = _compute_ma40_equidist(r5v, ma40_5m, LOOKBACK_5M)

        def _map5(arr):
            s = pd.Series(arr, index=r5.index)
            return s.reindex(pd.to_datetime(fdf['datetime']), method='ffill').values

        cx_5m = _map5(cx_5mr)
        bi_5m = _map5(bi_5mr)
        ar_5m = _map5(ar_5mr)
        dr40_5m = _map5(dr40_5mr)
        cb40_5m = _map5(cb40_5mr)
        rc_5m = _map5(rc_5mr)
        dg_5m = _map5(dg_5mr.astype(np.float64))
        ls_5m = _map5(ls_5mr.astype(np.float64))
        lsa_5m = _map5(lsa_5mr.astype(np.float64))
        lss_5m = _map5(lss_5mr.astype(np.float64))
        pcx_5m = _map5(pcx_5mr.astype(np.float64))
        pop_5m = _map5(pop_5mr.astype(np.float64))
    else:
        cx_5m = np.full(n, np.nan)
        bi_5m = np.full(n, np.nan)
        ar_5m = np.full(n, 0.5)
        dr40_5m = np.full(n, np.nan)
        cb40_5m = np.full(n, np.nan)
        rc_5m = np.zeros(n)
        dg_5m = np.zeros(n)
        ls_5m = np.zeros(n)
        lsa_5m = np.full(n, 999.0)
        lss_5m = np.zeros(n)
        pcx_5m = np.zeros(n)
        pop_5m = np.zeros(n)

    # --- 时段划分 ---
    gaps = fdf['datetime'].diff().dt.total_seconds() / 60
    fdf['session_id'] = (gaps > SESSION_GAP_MINUTES).cumsum()
    session_groups = fdf.groupby('session_id')
    session_last_idx = session_groups.tail(1).index.values
    session_last_set = set(session_last_idx)
    dist_to_end = session_groups.cumcount(ascending=False)

    # --- 期权数据索引 ---
    opt_idx = {}
    for sym, odf in options_indexed.items():
        odf2 = odf.copy()
        odf2['datetime'] = pd.to_datetime(odf2['datetime'])
        opt_idx[sym] = odf2.set_index('datetime').sort_index()

    # --- 预计算组合状态序列 (用于转换检测) ---
    state_1m = []
    state_5m = []
    for idx in range(n):
        s1 = _get_1m_state(idx, cx_1m, bi_1m, ar_1m, dr40_1m, cb40_1m,
                           rc_1m, dg_1m, ls_1m, lsa_1m, lss_1m, pcx_1m, pop_1m)
        s5 = _get_5m_state(idx, cx_5m, bi_5m, ar_5m, dr40_5m, cb40_5m,
                           rc_5m, dg_5m, ls_5m, lsa_5m, lss_5m, pcx_5m, pop_5m)
        state_1m.append(s1)
        state_5m.append(s5)

    # --- 构建 price_sum 用于布林线 ---
    # 需要先找到 OTM pair，但 pair 随 underlying 价格变化
    # 简化: 在回测中用每个 entry bar 的 pair 的历史 price_sum 计算布林线
    # 更高效的做法: 预先为每个时段找到一个代表性 pair，计算该时段的 price_sum 序列

    entries = []
    in_position = False
    last_entry_session = -1

    for idx in range(1, n):
        if in_position:
            continue

        # 跳过 warmup
        if np.isnan(cx_1m[idx]) or np.isnan(cx_5m[idx]):
            continue

        # 检测转换: 至少一个时间框架从 trending → safe
        s1_prev, s1_curr = state_1m[idx - 1], state_1m[idx]
        s5_prev, s5_curr = state_5m[idx - 1], state_5m[idx]

        transition_detected = False
        # 1m 转换: prev=trending, curr=safe (entangled/trans_*)
        if _is_trending(s1_prev) and _is_safe(s1_curr):
            transition_detected = True
        # 5m 转换: prev=trending, curr=safe
        if _is_trending(s5_prev) and _is_safe(s5_curr):
            transition_detected = True

        if not transition_detected:
            continue

        # 距离时段末尾至少 MIN_HOLD_MINUTES 根 bar (1min bar)
        dte = dist_to_end.iloc[idx]
        if dte < MIN_HOLD_MINUTES:
            continue

        # 同一时段只允许一次进仓
        sid = fdf['session_id'].iloc[idx]
        if sid == last_entry_session:
            continue

        row = fdf.iloc[idx]
        entry_time = row['datetime']
        upx = row['close_price']

        call_sym, put_sym = find_otm_pair(opt_idx, upx, product, contract_yymm)
        if not call_sym or not put_sym:
            continue

        cdf = opt_idx.get(call_sym)
        pdf = opt_idx.get(put_sym)
        if cdf is None or pdf is None:
            continue

        try:
            ce = cdf.loc[cdf.index.asof(entry_time)]
            pe = pdf.loc[pdf.index.asof(entry_time)]
        except (KeyError, ValueError):
            continue

        ecp, epp = float(ce['close']), float(pe['close'])
        entry_sum = ecp + epp
        if entry_sum <= 0 or ecp <= 0 or epp <= 0:
            continue

        # --- 布林线 lite: 计算 price_sum 的历史中轨 ---
        # 向前取 BOLL_PERIOD 根 bar 的 price_sum
        if idx < BOLL_PERIOD:
            continue

        price_sums = []
        valid_boll = True
        for bi in range(idx - BOLL_PERIOD + 1, idx + 1):
            bt = fdf.iloc[bi]['datetime']
            try:
                bc = cdf.loc[cdf.index.asof(bt)]['close']
                bp = pdf.loc[pdf.index.asof(bt)]['close']
            except (KeyError, ValueError):
                valid_boll = False
                break
            price_sums.append(float(bc) + float(bp))
        if not valid_boll or len(price_sums) < BOLL_PERIOD:
            continue

        boll_middle = np.mean(price_sums)

        # --- 向前扫描持仓到时段结束 ---
        session_end_idx = idx
        for j in range(idx + 1, n):
            if fdf['session_id'].iloc[j] != sid:
                break
            session_end_idx = j

        # 对每个参数组合模拟持仓
        for boll_pct in BOLL_LITE_PCTS:
            # 布林线 lite 条件
            if entry_sum <= boll_middle * (1 + boll_pct):
                continue

            for tp_pct in TP_PCTS:
                tp_threshold = entry_sum * tp_pct
                exit_reason = 'session_close'
                exit_idx = session_end_idx
                exit_sum = None

                for j in range(idx + 1, session_end_idx + 1):
                    jt = fdf.iloc[j]['datetime']
                    try:
                        jc = float(cdf.loc[cdf.index.asof(jt)]['close'])
                        jp = float(pdf.loc[pdf.index.asof(jt)]['close'])
                    except (KeyError, ValueError):
                        continue

                    current_sum = jc + jp

                    # 止盈 (卖方: 权利金下跌 = 盈利)
                    pnl = entry_sum - current_sum
                    if pnl >= tp_threshold:
                        exit_reason = 'take_profit'
                        exit_idx = j
                        exit_sum = current_sum
                        break

                    # 止损 (腿比)
                    high_leg = max(jc, jp)
                    low_leg = min(jc, jp)
                    if low_leg > 0 and high_leg / low_leg >= SL_RATIO:
                        exit_reason = 'stop_loss'
                        exit_idx = j
                        exit_sum = current_sum
                        break

                # 时段收盘时获取最终价格
                if exit_sum is None:
                    et = fdf.iloc[exit_idx]['datetime']
                    try:
                        ec = float(cdf.loc[cdf.index.asof(et)]['close'])
                        ep = float(pdf.loc[pdf.index.asof(et)]['close'])
                        exit_sum = ec + ep
                    except (KeyError, ValueError):
                        continue

                hold_minutes = exit_idx - idx
                pnl_pct = round((entry_sum - exit_sum) / entry_sum * 100, 3) if entry_sum > 0 else 0.0

                entries.append({
                    'boll_pct': boll_pct,
                    'tp_pct': tp_pct,
                    'entry_sum': round(entry_sum, 2),
                    'exit_sum': round(exit_sum, 2),
                    'boll_middle': round(boll_middle, 2),
                    'pnl_pct': pnl_pct,
                    'hold_minutes': hold_minutes,
                    'exit_reason': exit_reason,
                    's1_prev': s1_prev,
                    's1_curr': s1_curr,
                    's5_prev': s5_prev,
                    's5_curr': s5_curr,
                })

        last_entry_session = sid

    return entries


def _stat_group(pnl_list):
    if len(pnl_list) < 3:
        return None
    a = np.array(pnl_list)
    return {
        'n': len(a),
        'mean': round(float(np.mean(a)), 3),
        'median': round(float(np.median(a)), 3),
        'win_rate': round(float((a > 0).mean() * 100), 1),
        'tail5': round(float(np.percentile(a, 5)), 3),
        'best': round(float(np.max(a)), 3),
        'worst': round(float(np.min(a)), 3),
        'sharpe': round(float(np.mean(a) / np.std(a)), 3) if np.std(a) > 0 else 0.0,
    }


def process_product(product):
    import pyarrow.parquet as pq
    exchange = EXCHANGE_MAP.get(product.lower(), '')
    prod_upper = product.upper()
    print(f'\n{"="*60}')
    print(f'B047: {prod_upper} ({exchange})')
    print(f'{"="*60}')

    fdf_all = load_futures(product)
    if fdf_all.empty:
        print(f'  No futures data')
        return None

    fdf_all['datetime'] = pd.to_datetime(fdf_all['datetime'])
    contracts = sorted(fdf_all['symbol'].unique())
    if RECENT_CONTRACTS and len(contracts) > RECENT_CONTRACTS:
        contracts = contracts[-RECENT_CONTRACTS:]
    print(f'  Futures: {len(contracts)} contracts, {len(fdf_all)} bars')

    path_opt_upper = os.path.join(OPTIONS_DIR, exchange, f'{prod_upper}.parquet')
    path_opt_lower = os.path.join(OPTIONS_DIR, exchange, f'{product.lower()}.parquet')
    use_flat = os.path.exists(path_opt_upper) or os.path.exists(path_opt_lower)

    if not use_flat:
        opt_base = os.path.join(OPTIONS_DIR, exchange, prod_upper)
        if not os.path.isdir(opt_base):
            opt_base = os.path.join(OPTIONS_DIR, exchange, product.lower())
        if not os.path.isdir(opt_base):
            print(f'  No options data')
            return None
        month_dirs = sorted([d for d in os.listdir(opt_base) if re.match(r'\d{4}-\d{2}', d)])
        print(f'  Options (month dirs): {len(month_dirs)}')

    all_entries = []

    if use_flat:
        option_yymms = list_option_yymms_flat(product)
        # CZCE 期货合约用4位yymm(如2511), 期权用3位(如511)，需兼容
        def _yymm_match(contract_yymm, yymm_set):
            if contract_yymm in yymm_set:
                return True
            if exchange == 'CZCE' and len(contract_yymm) == 4 and contract_yymm[1:] in yymm_set:
                return True
            return False
        contracts = [c for c in contracts if _yymm_match(c[len(prod_upper):], option_yymms)]
        if RECENT_CONTRACTS and len(contracts) > RECENT_CONTRACTS:
            contracts = contracts[-RECENT_CONTRACTS:]
        print('  Options (flat): 按合约月份+时间范围局部读取')
        for full_contract in contracts:
            yymm = full_contract[len(prod_upper):]
            if not yymm.isdigit():
                continue
            cdf = fdf_all[fdf_all['symbol'] == full_contract].copy()
            if len(cdf) < 300:
                continue

            start_dt = cdf['datetime'].min() - pd.Timedelta(days=2)
            end_dt = cdf['datetime'].max() + pd.Timedelta(days=2)
            options = load_options_flat_for_contract(product, yymm, start_dt, end_dt)
            if not options:
                continue

            contract_entries = backtest_contract(cdf, options, product, yymm)
            if contract_entries:
                print(f'    {full_contract}: {len(cdf)} bars, {len(options)} opts → {len(contract_entries)} entries')
                all_entries.extend(contract_entries)
            del cdf, options
            gc.collect()
    else:
        for month_dir in month_dirs:
            month_path = os.path.join(opt_base, month_dir)
            year, mon = month_dir.split('-')
            month_start = pd.Timestamp(f'{year}-{mon}-01')
            month_end = pd.Timestamp(f'{int(year)+1}-01-01') if int(mon) == 12 \
                else pd.Timestamp(f'{year}-{int(mon)+1:02d}-01')

            mask = (fdf_all['datetime'] >= month_start) & (fdf_all['datetime'] < month_end)
            fdf_month = fdf_all[mask]
            if fdf_month.empty:
                continue

            vol_by = fdf_month.groupby('symbol')['volume'].sum()
            main_c = vol_by.idxmax()
            cdf = fdf_month[fdf_month['symbol'] == main_c].copy()
            if len(cdf) < 300:
                continue

            yymm = main_c[len(prod_upper):]
            options = load_options_months(product, month_path, yymm)
            if not options:
                continue

            contract_entries = backtest_contract(cdf, options, product, yymm)
            if contract_entries:
                print(f'    {month_dir} [{main_c}]: {len(cdf)} bars → {len(contract_entries)} entries')
                all_entries.extend(contract_entries)
            del cdf, options
            gc.collect()

    if not all_entries:
        print(f'  No entries')
        return None

    print(f'  Total entries: {len(all_entries)}')

    # --- 汇总统计 ---
    result = {'product': prod_upper, 'exchange': exchange, 'total_entries': len(all_entries)}
    param_results = {}

    for boll_pct in BOLL_LITE_PCTS:
        for tp_pct in TP_PCTS:
            key = f'boll{int(boll_pct*100)}%_tp{int(tp_pct*100)}%'
            subset = [e for e in all_entries if e['boll_pct'] == boll_pct and e['tp_pct'] == tp_pct]
            if not subset:
                continue

            pnls = [e['pnl_pct'] for e in subset]
            stats = _stat_group(pnls)
            if not stats:
                continue

            # 退出原因分布
            reasons = defaultdict(int)
            for e in subset:
                reasons[e['exit_reason']] += 1
            total_n = len(subset)
            reason_pct = {k: round(v / total_n * 100, 1) for k, v in reasons.items()}

            avg_hold = round(np.mean([e['hold_minutes'] for e in subset]), 1)

            param_results[key] = {
                **stats,
                'avg_hold_min': avg_hold,
                'exit_reasons': dict(reason_pct),
                'boll_pct': boll_pct,
                'tp_pct': tp_pct,
            }

    result['param_results'] = param_results

    # 找最优参数组合
    best_key, best_sharpe = None, -999
    for k, v in param_results.items():
        if v['sharpe'] > best_sharpe and v['n'] >= 10:
            best_sharpe = v['sharpe']
            best_key = k
    result['best_params'] = best_key

    # Print summary
    print(f'\n  {"Params":<20s} {"N":>5s} {"Mean":>8s} {"WR":>6s} {"Sharpe":>7s} {"AvgHold":>8s} {"TP%":>5s} {"SL%":>5s} {"Sess%":>6s}')
    print(f'  {"-"*76}')
    for key in sorted(param_results.keys()):
        pr = param_results[key]
        tp_r = pr['exit_reasons'].get('take_profit', 0)
        sl_r = pr['exit_reasons'].get('stop_loss', 0)
        sess_r = pr['exit_reasons'].get('session_close', 0)
        marker = ' ★' if key == best_key else ''
        print(f'  {key:<20s} {pr["n"]:>5d} {pr["mean"]:>+7.3f}% {pr["win_rate"]:>5.1f}% '
              f'{pr["sharpe"]:>7.3f} {pr["avg_hold_min"]:>7.1f}m '
              f'{tp_r:>4.1f}% {sl_r:>4.1f}% {sess_r:>5.1f}%{marker}')

    return result


def _run_one(product):
    try:
        return (product.upper(), process_product(product))
    except Exception as e:
        import traceback
        traceback.print_exc()
        return (product.upper(), None)


def main():
    global RECENT_CONTRACTS
    parser = argparse.ArgumentParser(description='B047: 趋势→纠缠转换瞬间卖出宽跨回测')
    parser.add_argument('--product', default='ag,y,m', help='品种代码')
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--workers', type=int, default=0)
    parser.add_argument('--recent-contracts', type=int, default=0)
    args = parser.parse_args()
    RECENT_CONTRACTS = max(0, args.recent_contracts)

    if args.all:
        products = sorted(EXCHANGE_MAP.keys())
    else:
        products = [p.strip().lower() for p in args.product.split(',')]

    valid = []
    for p in products:
        ex = EXCHANGE_MAP.get(p, '')
        fp = os.path.join(FUTURES_DIR, ex, f'{p.upper()}.parquet')
        fd = os.path.join(FUTURES_DIR, ex, p.upper())
        op = os.path.join(OPTIONS_DIR, ex, f'{p.upper()}.parquet')
        ol = os.path.join(OPTIONS_DIR, ex, f'{p.lower()}.parquet')
        od = os.path.join(OPTIONS_DIR, ex, p.upper())
        has_f = os.path.exists(fp) or os.path.isdir(fd)
        has_o = os.path.exists(op) or os.path.exists(ol) or os.path.isdir(od)
        if has_f and has_o:
            valid.append(p)
        else:
            print(f'Skip {p.upper()}: futures={has_f} options={has_o}')
    products = valid

    os.makedirs(RESULT_DIR, exist_ok=True)
    print(f'B047: 趋势→纠缠转换瞬间卖出宽跨回测')
    print(f'Products: {", ".join(p.upper() for p in products)}')
    print(f'MA: {MA_PERIOD}+{MA_PERIOD_40} | Boll period: {BOLL_PERIOD}')
    print(f'Boll lite %: {BOLL_LITE_PCTS} | TP %: {TP_PCTS} | SL ratio: {SL_RATIO}')
    if RECENT_CONTRACTS:
        print(f'Only recent contracts: {RECENT_CONTRACTS}')

    t0 = time.time()
    all_results = {}

    n_workers = args.workers
    if n_workers <= 0:
        try:
            import psutil
            avail = psutil.virtual_memory().available / (1024**3)
            n_workers = max(1, min(int(avail / 3) - 1, os.cpu_count() - 1, len(products)))
        except ImportError:
            n_workers = min(2, len(products))

    if len(products) > 1 and n_workers > 1:
        from concurrent.futures import ProcessPoolExecutor, as_completed
        print(f'\nParallel: {n_workers} workers')
        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            futs = {pool.submit(_run_one, p): p for p in products}
            for fut in as_completed(futs):
                prod, r = fut.result()
                if r:
                    all_results[prod] = r
                    with open(os.path.join(RESULT_DIR, f'{prod}.json'), 'w') as f:
                        json.dump(r, f, ensure_ascii=False, indent=2, default=str)
    else:
        for p in products:
            prod, r = _run_one(p)
            if r:
                all_results[prod] = r
                with open(os.path.join(RESULT_DIR, f'{prod}.json'), 'w') as f:
                    json.dump(r, f, ensure_ascii=False, indent=2, default=str)
            gc.collect()

    elapsed = time.time() - t0
    print(f'\n{"="*60}')
    print(f'B047 汇总 (耗时 {elapsed:.0f}s):')
    print(f'{"Prod":>6s} {"BestParams":<22s} {"N":>5s} {"Mean":>8s} {"WR":>6s} {"Sharpe":>7s} {"TP%":>5s} {"SL%":>5s} {"Sess%":>6s}')

    for prod in sorted(all_results.keys()):
        r = all_results[prod]
        bp = r.get('best_params')
        if not bp:
            print(f'{prod:>6s} {"(no data)":<22s}')
            continue
        pr = r['param_results'][bp]
        tp_r = pr['exit_reasons'].get('take_profit', 0)
        sl_r = pr['exit_reasons'].get('stop_loss', 0)
        sess_r = pr['exit_reasons'].get('session_close', 0)
        print(f'{prod:>6s} {bp:<22s} {pr["n"]:>5d} {pr["mean"]:>+7.3f}% {pr["win_rate"]:>5.1f}% '
              f'{pr["sharpe"]:>7.3f} {tp_r:>4.1f}% {sl_r:>4.1f}% {sess_r:>5.1f}%')

    with open(os.path.join(RESULT_DIR, '_SUMMARY.json'), 'w') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)
    print(f'\nResults: {RESULT_DIR}')


if __name__ == '__main__':
    main()
