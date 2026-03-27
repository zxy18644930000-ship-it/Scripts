#!/usr/bin/env python3
"""
B046: MA纠缠/趋势判定 — 双向转换检测回测

对比:
  旧算法(B045): crossings>=CT→entangled, bias>=0.8+40MA→trending, 其余→entangled
  新算法(B046): + 纠缠→趋势覆盖(recent_consecutive+deviation_growing)
               + 趋势→纠缠转换(prior_streak刚结束→transitioning)

使用本地单文件 parquet 数据 (~/Downloads/期货数据_parquet/ + ~/Downloads/期权_parquet/)
或远程月目录数据 (/mnt/d/backtest_data/)

用法:
  python3 B046_transition_backtest.py --product al,cu,ni
  python3 B046_transition_backtest.py --all --workers 4
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

import numpy as np
import pandas as pd

# ============ 参数 ============
MA_PERIOD = 20
MA_PERIOD_40 = 40
LOOKBACK_1M = 50
LOOKBACK_5M = 20
CROSS_THRESHOLD = 3
BIAS_THRESHOLD = 0.8
MA40_DIST_RATIO = 2.0
MA40_COUNT_BIAS = 0.7

CONSEC_1M = 12
CONSEC_5M = 6
TRANSITION_WINDOW_1M = 10
TRANSITION_WINDOW_5M = 5

HOLD_PERIODS = [30, 60]
ENTRY_INTERVAL = 30
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

RESULT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'B046_results')
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


# ============ 数据加载（本地单文件 + 远程月目录兼容）============

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
    """按合约月份 + 时间范围局部加载单文件格式期权数据，避免全量读取数千万行。"""
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
            filters=[
                ('datetime', '>=', start_dt),
                ('datetime', '<', end_dt),
            ],
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
    """仅读 symbol 列，快速提取本地单文件期权数据里实际存在的 yymm。"""
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
    """加载月目录格式的期权数据"""
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


# ============ MA 状态计算 ============

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


def classify_old(cx, bi, ar, dr40, cb40):
    """旧算法 B045: 纯三维判定"""
    if np.isnan(cx) or np.isnan(bi):
        return 'warmup'
    if int(cx) >= CROSS_THRESHOLD:
        return 'entangled'
    if bi >= BIAS_THRESHOLD:
        if (dr40 if not np.isnan(dr40) else 999) > MA40_DIST_RATIO or \
           (cb40 if not np.isnan(cb40) else 1.0) > MA40_COUNT_BIAS:
            return 'trending_up' if ar > 0.5 else 'trending_down'
    return 'entangled'


def classify_new(cx, bi, ar, dr40, cb40, rc, dg, ls, lsa, lss, pcx, pop,
                 consec_th, trans_win):
    """新算法 B046: 三维判定 + 双向转换检测(3子类型)"""
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


def combine_old(s1, s5):
    if s1 == 'warmup' or s5 == 'warmup':
        return 'warmup'
    if s1 == 'entangled' and s5 == 'entangled':
        return 'both_entangled'
    if s1.startswith('trending') and s5.startswith('trending'):
        return 'both_trending'
    return 'mixed'


def combine_new(s1, s5):
    if s1 == 'warmup' or s5 == 'warmup':
        return 'warmup'
    safe_set = ('entangled', 'trans_touch', 'trans_cross', 'trans_oscillate')
    if s1 in safe_set and s5 in safe_set:
        has_trans = s1.startswith('trans_') or s5.startswith('trans_')
        return 'transition_safe' if has_trans else 'both_entangled'
    if s1.startswith('trending') and s5.startswith('trending'):
        return 'both_trending'
    return 'mixed'


# ============ 核心回测 ============

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


def backtest_contract(fdf, options_indexed, product, contract_yymm):
    fdf = fdf.sort_values('datetime').reset_index(drop=True)
    fdf['datetime'] = pd.to_datetime(fdf['datetime'])
    close = fdf['close_price'].values

    min_warmup = MA_PERIOD_40 * 5 + LOOKBACK_5M + max(HOLD_PERIODS)
    if len(fdf) < min_warmup:
        return []

    cx_1m, bi_1m, ar_1m, ma20_1m, rc_1m, dg_1m, ls_1m, lsa_1m, lss_1m, pcx_1m, pop_1m = \
        _compute_raw_features(close, MA_PERIOD, LOOKBACK_1M)
    ma40_1m = pd.Series(close).rolling(MA_PERIOD_40, min_periods=MA_PERIOD_40).mean().values
    dr40_1m, cb40_1m = _compute_ma40_equidist(close, ma40_1m, LOOKBACK_1M)

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
        n = len(fdf)
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

    gaps = fdf['datetime'].diff().dt.total_seconds() / 60
    fdf['session_id'] = (gaps > SESSION_GAP_MINUTES).cumsum()
    dist_to_end = fdf.groupby('session_id').cumcount(ascending=False)

    opt_idx = {}
    for sym, odf in options_indexed.items():
        odf2 = odf.copy()
        odf2['datetime'] = pd.to_datetime(odf2['datetime'])
        opt_idx[sym] = odf2.set_index('datetime').sort_index()

    entries = []
    last_entry = -ENTRY_INTERVAL

    for idx in range(len(fdf)):
        if idx - last_entry < ENTRY_INTERVAL:
            continue
        if dist_to_end.iloc[idx] < max(HOLD_PERIODS):
            continue
        if np.isnan(cx_1m[idx]) or np.isnan(cx_5m[idx]):
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

        ecp, epp = ce['close'], pe['close']
        esum = ecp + epp
        if esum <= 0 or ecp <= 0 or epp <= 0:
            continue

        pnls = {}
        for hp in HOLD_PERIODS:
            eidx = idx + hp
            if eidx >= len(fdf):
                pnls[hp] = None
                continue
            et = fdf.iloc[eidx]['datetime']
            try:
                cx_e = cdf.loc[cdf.index.asof(et)]
                px_e = pdf.loc[pdf.index.asof(et)]
            except (KeyError, ValueError):
                pnls[hp] = None
                continue
            xsum = cx_e['close'] + px_e['close']
            pnls[hp] = round((esum - xsum) / esum * 100, 3) if esum > 0 else 0

        # 旧算法状态
        s1_old = classify_old(cx_1m[idx], bi_1m[idx], ar_1m[idx],
                              dr40_1m[idx], cb40_1m[idx])
        s5_old = classify_old(cx_5m[idx], bi_5m[idx], ar_5m[idx],
                              dr40_5m[idx], cb40_5m[idx])
        comb_old = combine_old(s1_old, s5_old)

        # 新算法状态
        s1_new = classify_new(cx_1m[idx], bi_1m[idx], ar_1m[idx],
                              dr40_1m[idx], cb40_1m[idx],
                              int(rc_1m[idx]), bool(dg_1m[idx]),
                              int(ls_1m[idx]), int(lsa_1m[idx]),
                              int(lss_1m[idx]), int(pcx_1m[idx]), int(pop_1m[idx]),
                              CONSEC_1M, TRANSITION_WINDOW_1M)
        s5_new = classify_new(cx_5m[idx], bi_5m[idx], ar_5m[idx],
                              dr40_5m[idx], cb40_5m[idx],
                              int(rc_5m[idx]), bool(dg_5m[idx]),
                              int(ls_5m[idx]), int(lsa_5m[idx]),
                              int(lss_5m[idx]), int(pcx_5m[idx]), int(pop_5m[idx]),
                              CONSEC_5M, TRANSITION_WINDOW_5M)
        comb_new = combine_new(s1_new, s5_new)

        entries.append({
            'pnls': pnls,
            'old_1m': s1_old, 'old_5m': s5_old, 'old_comb': comb_old,
            'new_1m': s1_new, 'new_5m': s5_new, 'new_comb': comb_new,
        })
        last_entry = idx

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
    }


def process_product(product):
    import pyarrow.parquet as pq
    exchange = EXCHANGE_MAP.get(product.lower(), '')
    prod_upper = product.upper()
    print(f'\n{"="*60}')
    print(f'B046: {prod_upper} ({exchange})')
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
        contracts = [c for c in contracts if c[len(prod_upper):] in option_yymms]
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

            entries = backtest_contract(cdf, options, product, yymm)
            if entries:
                print(f'    {full_contract}: {len(cdf)} bars, {len(options)} options → {len(entries)} entries')
                all_entries.extend(entries)
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

            entries = backtest_contract(cdf, options, product, yymm)
            if entries:
                print(f'    {month_dir} [{main_c}]: {len(cdf)} bars → {len(entries)} entries')
                all_entries.extend(entries)
            del cdf, options
            gc.collect()

    if not all_entries:
        print(f'  No entries')
        return None

    print(f'  Total entries: {len(all_entries)}')

    result = {'product': prod_upper, 'exchange': exchange, 'total_entries': len(all_entries)}

    for hp in HOLD_PERIODS:
        hp_result = {}
        for algo_prefix, comb_key in [('old', 'old_comb'), ('new', 'new_comb')]:
            groups = defaultdict(list)
            for e in all_entries:
                pnl = e['pnls'].get(hp)
                if pnl is not None:
                    groups[e[comb_key]].append(pnl)

            algo_stats = {}
            for state, pnl_list in groups.items():
                s = _stat_group(pnl_list)
                if s:
                    algo_stats[state] = s
            hp_result[algo_prefix] = algo_stats

        old_ent = hp_result['old'].get('both_entangled', {})
        old_trn = hp_result['old'].get('both_trending', {})
        new_ent = hp_result['new'].get('both_entangled', {})
        new_trn = hp_result['new'].get('both_trending', {})
        new_trans = hp_result['new'].get('transition_safe', {})

        hp_result['comparison'] = {}
        if old_ent and old_trn:
            hp_result['comparison']['old_diff'] = round(
                old_ent.get('mean', 0) - old_trn.get('mean', 0), 3)
        if new_ent and new_trn:
            hp_result['comparison']['new_diff_ent_vs_trend'] = round(
                new_ent.get('mean', 0) - new_trn.get('mean', 0), 3)
        if new_trans:
            hp_result['comparison']['transition_mean'] = new_trans.get('mean', 0)
            hp_result['comparison']['transition_win_rate'] = new_trans.get('win_rate', 0)
            if new_ent:
                hp_result['comparison']['transition_vs_entangled'] = round(
                    new_trans.get('mean', 0) - new_ent.get('mean', 0), 3)
            if new_trn:
                hp_result['comparison']['transition_vs_trending'] = round(
                    new_trans.get('mean', 0) - new_trn.get('mean', 0), 3)

        old_safe = [e['pnls'][hp] for e in all_entries
                    if e['pnls'].get(hp) is not None and e['old_comb'] == 'both_entangled']
        new_safe = [e['pnls'][hp] for e in all_entries
                    if e['pnls'].get(hp) is not None and e['new_comb'] in ('both_entangled', 'transition_safe')]

        sub_groups = defaultdict(list)
        for e in all_entries:
            pnl = e['pnls'].get(hp)
            if pnl is None or e['new_comb'] != 'transition_safe':
                continue
            for s in (e['new_1m'], e['new_5m']):
                if s.startswith('trans_'):
                    sub_groups[s].append(pnl)
                    break
        hp_result['trans_subtypes'] = {k: _stat_group(v) for k, v in sub_groups.items() if _stat_group(v)}
        old_s = _stat_group(old_safe)
        new_s = _stat_group(new_safe)
        if old_s and new_s:
            hp_result['safe_pool'] = {
                'old_safe_n': old_s['n'], 'old_safe_mean': old_s['mean'], 'old_safe_wr': old_s['win_rate'],
                'new_safe_n': new_s['n'], 'new_safe_mean': new_s['mean'], 'new_safe_wr': new_s['win_rate'],
                'n_gain': new_s['n'] - old_s['n'],
                'mean_change': round(new_s['mean'] - old_s['mean'], 3),
                'wr_change': round(new_s['win_rate'] - old_s['win_rate'], 1),
            }

        result[f'hold_{hp}m'] = hp_result

    # Print summary
    for hp in HOLD_PERIODS:
        hr = result.get(f'hold_{hp}m', {})
        comp = hr.get('comparison', {})
        sp = hr.get('safe_pool', {})
        print(f'\n  === Hold {hp}m ===')
        for algo in ['old', 'new']:
            stats = hr.get(algo, {})
            for state, s in sorted(stats.items()):
                print(f'    {algo:>3s} {state:<20s} n={s["n"]:>5d} mean={s["mean"]:>+7.3f}% '
                      f'WR={s["win_rate"]:>5.1f}% tail5={s["tail5"]:>+7.3f}%')
        if comp:
            print(f'    比较: old_diff={comp.get("old_diff","?")} '
                  f'trans_mean={comp.get("transition_mean","?")} '
                  f'trans_vs_ent={comp.get("transition_vs_entangled","?")} '
                  f'trans_vs_trend={comp.get("transition_vs_trending","?")}')
        if sp:
            print(f'    安全池: old={sp["old_safe_n"]}笔/{sp["old_safe_mean"]:+.3f}%/WR{sp["old_safe_wr"]:.1f}% '
                  f'→ new={sp["new_safe_n"]}笔/{sp["new_safe_mean"]:+.3f}%/WR{sp["new_safe_wr"]:.1f}% '
                  f'(+{sp["n_gain"]}笔, PnL{sp["mean_change"]:+.3f}%, WR{sp["wr_change"]:+.1f}%)')
        subs = hr.get('trans_subtypes', {})
        if subs:
            sub_labels = {'trans_touch': '首触MA', 'trans_cross': '反穿站稳', 'trans_oscillate': '穿梭'}
            for sk in ('trans_touch', 'trans_cross', 'trans_oscillate'):
                ss = subs.get(sk)
                if ss:
                    print(f'      {sub_labels.get(sk, sk):<8s} n={ss["n"]:>5d} mean={ss["mean"]:>+7.3f}% '
                          f'WR={ss["win_rate"]:>5.1f}% tail5={ss["tail5"]:>+7.3f}%')

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
    parser = argparse.ArgumentParser(description='B046: MA双向转换检测回测')
    parser.add_argument('--product', default='al,cu,ni', help='品种代码')
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--workers', type=int, default=0)
    parser.add_argument('--recent-contracts', type=int, default=0, help='仅回测最近N个期货合约')
    args = parser.parse_args()
    RECENT_CONTRACTS = max(0, args.recent_contracts)

    if args.all:
        products = sorted(EXCHANGE_MAP.keys())
    else:
        products = [p.strip().lower() for p in args.product.split(',')]

    # 过滤掉没有数据的品种
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
    print(f'B046: MA双向转换检测回测')
    print(f'Products: {", ".join(p.upper() for p in products)}')
    print(f'Params: MA{MA_PERIOD}+MA{MA_PERIOD_40} | 1m-LB{LOOKBACK_1M}/consec{CONSEC_1M}/trans{TRANSITION_WINDOW_1M}'
          f' | 5m-LB{LOOKBACK_5M}/consec{CONSEC_5M}/trans{TRANSITION_WINDOW_5M}')
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
    print(f'B046 汇总 (耗时 {elapsed:.0f}s):')
    print(f'{"Prod":>6s} {"OldDiff":>8s} {"TransN":>7s} {"TransPnL":>9s} {"TransWR":>8s} '
          f'{"SafeN_old":>9s} {"SafeN_new":>9s} {"ΔPnL":>8s} {"ΔWR":>6s}')

    hp = HOLD_PERIODS[0]
    for prod in sorted(all_results.keys()):
        r = all_results[prod]
        hr = r.get(f'hold_{hp}m', {})
        comp = hr.get('comparison', {})
        sp = hr.get('safe_pool', {})
        od = comp.get('old_diff', '?')
        tn = hr.get('new', {}).get('transition_safe', {}).get('n', 0)
        tm = comp.get('transition_mean', '?')
        tw = comp.get('transition_win_rate', '?')
        son = sp.get('old_safe_n', '?')
        snn = sp.get('new_safe_n', '?')
        dpm = sp.get('mean_change', '?')
        dwr = sp.get('wr_change', '?')
        print(f'{prod:>6s} {str(od):>8s} {tn:>7d} {str(tm):>9s} {str(tw):>8s} '
              f'{str(son):>9s} {str(snn):>9s} {str(dpm):>8s} {str(dwr):>6s}')

    with open(os.path.join(RESULT_DIR, '_SUMMARY.json'), 'w') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)
    print(f'\nResults: {RESULT_DIR}')


if __name__ == '__main__':
    main()
