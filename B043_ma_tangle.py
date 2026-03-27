#!/usr/bin/env python3
"""
B043: MA纠缠度 vs 趋势持续性 — 宽跨卖出安全过滤器验证

信念:
  - 期货1分钟收盘价持续≥8/10根在20MA同侧 → 趋势可能加速 → 不适合卖跨
  - 价格与20MA纠缠(频繁穿越/翻转到另一侧) → 震荡 → 适合卖跨
  - 多级确认: 1分钟+5分钟都纠缠 → 才安全

验证方法:
  1. 每根bar计算"偏离度评分" = max(上方数, 下方数) / 回看数
  2. 分档: 10/10, 9/10, 8/10, 7/10, 6/10, ≤5/10
  3. 测量: 每档未来N根bar的波动幅度(range, 最大逆行, 方向延续)
  4. 多时间框架: 1分钟20MA + 100MA(≈5分钟20MA)
  5. 如果高偏离档 → 未来波动显著更大 → 信念成立

用法:
  python3 B043_ma_tangle.py                    # 本地DB, AG, 60天
  python3 B043_ma_tangle.py --product ag --source parquet  # parquet
  python3 B043_ma_tangle.py --product ag,au,cu --source parquet --all  # 多品种

网格搜索:
  回看窗口(lookback): 5, 8, 10, 12, 15, 20
  阈值比例(threshold_ratio): 0.6, 0.7, 0.8, 0.9, 1.0
  → threshold = int(lookback * ratio)
  → 找到最优参数: 哪个lookback+threshold组合下trending vs entangled的波动差异最大
"""

import argparse
import gc
import json
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# ============ 配置 ============
MA_PERIOD_1M = 20       # 1分钟20MA
MA_PERIOD_5M = 100      # 5分钟20MA ≈ 1分钟100MA
LOOKBACK = 10           # 默认回看10根bar
TREND_THRESHOLD = 8     # 默认≥8/10 = trending
FORWARD_WINDOWS = [10, 30, 60, 120]  # 前瞻窗口(bar数)
SESSION_GAP_MINUTES = 30  # session间隔阈值

# 网格搜索参数空间
GRID_LOOKBACKS = [5, 8, 10, 12, 15, 20]
GRID_THRESHOLD_RATIOS = [0.6, 0.7, 0.8, 0.9, 1.0]
# → threshold = int(lookback * ratio), 例: lookback=10, ratio=0.8 → threshold=8

SCORE_BINS = [
    (10, '10/10'), (9, '9/10'), (8, '8/10'),
    (7, '7/10'), (6, '6/10'), (0, '≤5/10'),
]

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

DB_PATH = os.path.expanduser('~/.vntrader/database.db')
# 期货parquet路径 (期货数据, 非期权数据!)
PARQUET_DIR_REMOTE = '/mnt/d/backtest_data/Futures_parquet/'
PARQUET_DIR_MAC = os.path.expanduser('~/Downloads/期货数据_parquet/')
# 自动检测环境
PARQUET_DIR = PARQUET_DIR_REMOTE if os.path.exists('/mnt/d/') else PARQUET_DIR_MAC
RESULT_DIR = os.path.expanduser('~/Scripts/ma_tangle_results/')


# ============ 数据加载 ============

def load_futures_from_db(product, days=60):
    """从vnpy DB加载期货1分钟K线 (本地小规模)"""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    query = """
        SELECT datetime, symbol, open_price, close_price, high_price, low_price, volume
        FROM dbbardata
        WHERE datetime >= ?
        ORDER BY datetime
    """
    df = pd.read_sql(query, conn, params=(start,))
    conn.close()
    # 期货 symbol: ag2604, ag2501 等 (不含C/P)
    prod_upper = product.upper()
    prod_lower = product.lower()
    pattern = rf'^(?:{prod_upper}|{prod_lower})\d{{3,4}}$'
    mask = df['symbol'].str.match(pattern)
    df = df[mask].copy()
    if df.empty:
        print(f'  [WARN] No futures bars for {product} in DB')
    return df


def load_futures_from_parquet(product):
    """从parquet加载期货1分钟K线 (支持两种格式)

    格式A (期货数据_parquet): 列名open/high/low/close, symbol无前缀且大写 (如AU0806)
                              路径: {EXCHANGE}/{PRODUCT}.parquet
    格式B (期权_parquet或远程): 列名open_price等, symbol有交易所前缀 (如SHFE.ag2412)
                              路径: {EXCHANGE}.parquet (单文件含全品种)
    """
    import pyarrow.parquet as pq

    exchange = EXCHANGE_MAP.get(product.lower(), '')
    if not exchange:
        print(f'  [WARN] Unknown exchange for {product}')
        return pd.DataFrame()

    # 尝试格式C: {EXCHANGE}/{PRODUCT}/ 目录下每个合约一个文件 (远程服务器)
    path_c = os.path.join(PARQUET_DIR, exchange, product.upper())
    if os.path.isdir(path_c):
        return _load_parquet_format_c(path_c, product)

    # 尝试格式A: {EXCHANGE}/{PRODUCT}.parquet (Mac本地期货)
    path_a = os.path.join(PARQUET_DIR, exchange, f'{product.upper()}.parquet')
    if os.path.exists(path_a):
        return _load_parquet_format_a(path_a, product)

    # 尝试格式B: {EXCHANGE}.parquet (单文件全品种)
    path_b = os.path.join(PARQUET_DIR, f'{exchange}.parquet')
    if os.path.exists(path_b):
        return _load_parquet_format_b(path_b, product, exchange)

    # 尝试子目录格式B
    for sub in os.listdir(PARQUET_DIR):
        p = os.path.join(PARQUET_DIR, sub, f'{exchange}.parquet')
        if os.path.exists(p):
            return _load_parquet_format_b(p, product, exchange)

    print(f'  [WARN] Parquet not found for {product} ({exchange})')
    return pd.DataFrame()


def _load_parquet_format_c(dir_path, product):
    """格式C: 每合约一个文件, 无symbol列, 列名open/high/low/close
    路径: {EXCHANGE}/{PRODUCT}/{CONTRACT}.parquet (如 SHFE/AG/AG1209.parquet)
    """
    import pyarrow.parquet as pq

    prod_upper = product.upper()
    files = sorted([f for f in os.listdir(dir_path) if f.endswith('.parquet')])
    if not files:
        print(f'  [WARN] No parquet files in {dir_path}')
        return pd.DataFrame()

    all_dfs = []
    for fname in files:
        contract = fname.replace('.parquet', '')
        # 只要期货合约 (如 AG1209), 跳过期权
        if not contract.upper().startswith(prod_upper):
            continue
        suffix = contract[len(prod_upper):]
        if not suffix.isdigit():
            continue

        fpath = os.path.join(dir_path, fname)
        try:
            table = pq.read_table(fpath, columns=['datetime', 'open', 'close', 'high', 'low', 'volume'])
            cdf = table.to_pandas()
            del table
        except Exception:
            continue
        if cdf.empty:
            continue

        cdf['symbol'] = contract.upper()
        cdf.rename(columns={'open': 'open_price', 'close': 'close_price',
                           'high': 'high_price', 'low': 'low_price'}, inplace=True)
        all_dfs.append(cdf)

    if not all_dfs:
        return pd.DataFrame()

    df = pd.concat(all_dfs, ignore_index=True)
    del all_dfs
    gc.collect()
    print(f'  Format C: {dir_path}, {len(files)} files, {len(df)} bars')
    return df


def _load_parquet_format_a(path, product):
    """格式A: 单品种文件, 列名open/high/low/close, symbol大写无前缀"""
    import pyarrow.parquet as pq

    prod_upper = product.upper()
    try:
        table = pq.read_table(path, columns=['datetime', 'symbol', 'open', 'close', 'high', 'low', 'volume'])
    except Exception as e:
        print(f'  [ERROR] Reading parquet A: {e}')
        return pd.DataFrame()

    df = table.to_pandas()
    del table
    gc.collect()

    if df.empty:
        return df

    # 统一列名
    df.rename(columns={'open': 'open_price', 'close': 'close_price',
                       'high': 'high_price', 'low': 'low_price'}, inplace=True)

    # 期货symbol: AU0806, CU2401 等 (无C/P)
    pattern = rf'^{prod_upper}\d{{3,4}}$'
    mask = df['symbol'].str.match(pattern)
    df = df[mask].copy()
    print(f'  Format A: {path}, {len(df)} bars')
    return df


def _load_parquet_format_b(path, product, exchange):
    """格式B: 交易所单文件, 列名有_price后缀, symbol有交易所前缀"""
    import pyarrow.parquet as pq

    prefix = f'{exchange}.{product.lower()}'
    if exchange == 'CZCE':
        prefix = f'{exchange}.{product.upper()}'

    try:
        table = pq.read_table(
            path,
            columns=['datetime', 'symbol', 'open_price', 'close_price',
                     'high_price', 'low_price', 'volume'],
            filters=[('symbol', '>=', prefix),
                     ('symbol', '<', prefix + chr(127))])
    except Exception as e:
        print(f'  [ERROR] Reading parquet B: {e}')
        return pd.DataFrame()

    df = table.to_pandas()
    del table
    gc.collect()

    if df.empty:
        return df

    # Strip exchange prefix
    df['symbol'] = df['symbol'].str.split('.', n=1).str[-1]
    # 只保留期货(不含C/P/-)
    prod_lower = product.lower()
    prod_upper = product.upper()
    pattern = rf'^(?:{prod_lower}|{prod_upper})\d{{3,4}}$'
    mask = df['symbol'].str.match(pattern)
    df = df[mask].copy()
    print(f'  Format B: {path}, {len(df)} bars')
    return df


# ============ 核心计算 ============

def compute_states(df, ma_period, lookback, threshold):
    """
    计算每根bar的MA偏离状态

    Adds columns: ma, above_count, below_count, score, state
    """
    df = df.sort_values('datetime').reset_index(drop=True)
    close = df['close_price'].values

    # MA
    ma = pd.Series(close).rolling(ma_period, min_periods=ma_period).mean().values

    # Above/below
    above = (close > ma).astype(np.float64)
    below = (close < ma).astype(np.float64)

    # Rolling count
    above_count = pd.Series(above).rolling(lookback, min_periods=lookback).sum().values
    below_count = pd.Series(below).rolling(lookback, min_periods=lookback).sum().values

    max_side = np.fmax(above_count, below_count)
    score = max_side / lookback

    # State
    state = np.full(len(df), '', dtype='U20')
    valid = ~np.isnan(above_count)
    state[valid & (above_count >= threshold)] = 'trending_up'
    state[valid & (below_count >= threshold)] = 'trending_down'
    state[valid & (above_count < threshold) & (below_count < threshold)] = 'entangled'
    state[~valid] = 'warmup'

    df['ma'] = ma
    df['above_count'] = above_count
    df['below_count'] = below_count
    df['max_side'] = max_side
    df['score'] = score
    df['state'] = state

    return df


def compute_forward_volatility(df, forward_windows):
    """计算前瞻波动指标 (向量化)"""
    close = df['close_price'].values
    high = df['high_price'].values
    low = df['low_price'].values
    n = len(close)

    for fw in forward_windows:
        if n <= fw + 1:
            df[f'fwd{fw}_range'] = np.nan
            df[f'fwd{fw}_abs_ret'] = np.nan
            df[f'fwd{fw}_max_adverse'] = np.nan
            df[f'fwd{fw}_dir_ret'] = np.nan
            continue

        # Forward rolling max/min: reverse → rolling → reverse → shift(-1)
        fwd_max = pd.Series(high[::-1]).rolling(fw, min_periods=fw).max().values[::-1]
        fwd_min = pd.Series(low[::-1]).rolling(fw, min_periods=fw).min().values[::-1]

        # Shift: bars i+1 to i+fw (not including bar i)
        fwd_max = np.append(fwd_max[1:], np.nan)
        fwd_min = np.append(fwd_min[1:], np.nan)
        fwd_close = np.append(close[fw:], [np.nan] * fw)

        with np.errstate(divide='ignore', invalid='ignore'):
            safe = close > 0
            df[f'fwd{fw}_range'] = np.where(safe, (fwd_max - fwd_min) / close * 100, np.nan)
            df[f'fwd{fw}_abs_ret'] = np.where(safe, np.abs(fwd_close - close) / close * 100, np.nan)
            df[f'fwd{fw}_max_adverse'] = np.where(
                safe,
                np.maximum(np.abs(fwd_max - close), np.abs(close - fwd_min)) / close * 100,
                np.nan)
            # 方向性收益(有符号, 正=涨)
            df[f'fwd{fw}_dir_ret'] = np.where(safe, (fwd_close - close) / close * 100, np.nan)

    return df


def invalidate_cross_session(df, forward_windows, gap_minutes=30):
    """清除跨session的前瞻数据"""
    dt = pd.to_datetime(df['datetime'])
    gap = dt.diff().dt.total_seconds() / 60
    session_id = (gap > gap_minutes).cumsum().values

    # 每个session内离session末尾的距离
    df['_session_id'] = session_id
    dist_to_end = df.groupby('_session_id').cumcount(ascending=False)

    for fw in forward_windows:
        mask = dist_to_end < fw
        for col in [f'fwd{fw}_range', f'fwd{fw}_abs_ret',
                    f'fwd{fw}_max_adverse', f'fwd{fw}_dir_ret']:
            if col in df.columns:
                df.loc[mask, col] = np.nan

    df.drop(columns=['_session_id'], inplace=True)
    return df


# ============ 统计分析 ============

def analyze_by_state(df, forward_windows):
    """按state分组统计前瞻波动"""
    results = {}
    valid_states = ['trending_up', 'trending_down', 'entangled']
    total = len(df[df['state'].isin(valid_states)])

    for state in valid_states:
        sub = df[df['state'] == state]
        if len(sub) == 0:
            continue
        r = {'count': int(len(sub)), 'pct': round(len(sub) / total * 100, 1) if total else 0}
        for fw in forward_windows:
            col_r = f'fwd{fw}_range'
            col_a = f'fwd{fw}_abs_ret'
            col_m = f'fwd{fw}_max_adverse'
            col_d = f'fwd{fw}_dir_ret'
            s = sub[col_r].dropna()
            if len(s) == 0:
                continue
            r[f'fwd{fw}'] = {
                'n': int(len(s)),
                'range_mean': round(s.mean(), 4),
                'range_p50': round(s.median(), 4),
                'range_p75': round(s.quantile(0.75), 4),
                'range_p90': round(s.quantile(0.90), 4),
                'abs_ret_mean': round(sub[col_a].dropna().mean(), 4),
                'max_adv_mean': round(sub[col_m].dropna().mean(), 4),
                'max_adv_p90': round(sub[col_m].dropna().quantile(0.90), 4),
            }
            # 趋势延续: trending_up → 正收益比例, trending_down → 负收益比例
            d = sub[col_d].dropna()
            if len(d) > 0:
                if state == 'trending_up':
                    r[f'fwd{fw}']['continuation_rate'] = round((d > 0).mean() * 100, 1)
                elif state == 'trending_down':
                    r[f'fwd{fw}']['continuation_rate'] = round((d < 0).mean() * 100, 1)
                else:
                    r[f'fwd{fw}']['continuation_rate'] = None
                r[f'fwd{fw}']['dir_ret_mean'] = round(d.mean(), 4)
        results[state] = r
    return results


def analyze_by_score_bins(df, forward_windows):
    """按偏离度评分分档统计"""
    results = {}
    for threshold, label in SCORE_BINS:
        if threshold > 0:
            sub = df[df['max_side'] == threshold]
        else:
            sub = df[df['max_side'] <= 5]
        if len(sub) == 0:
            continue
        r = {'label': label, 'count': int(len(sub))}
        for fw in forward_windows:
            s = sub[f'fwd{fw}_range'].dropna()
            m = sub[f'fwd{fw}_max_adverse'].dropna()
            if len(s) == 0:
                continue
            r[f'fwd{fw}'] = {
                'n': int(len(s)),
                'range_mean': round(s.mean(), 4),
                'range_p50': round(s.median(), 4),
                'range_p90': round(s.quantile(0.90), 4),
                'max_adv_mean': round(m.mean(), 4),
                'max_adv_p90': round(m.quantile(0.90), 4),
            }
        results[label] = r
    return results


def analyze_multi_tf(df, forward_windows):
    """多时间框架联合分析: 1分钟state + 5分钟state"""
    if 'state_5m' not in df.columns:
        return {}

    combos = [
        ('both_trending', lambda r: r['state'].startswith('trending') and r['state_5m'].startswith('trending')),
        ('1m_trend_5m_entangled', lambda r: r['state'].startswith('trending') and r['state_5m'] == 'entangled'),
        ('1m_entangled_5m_trend', lambda r: r['state'] == 'entangled' and r['state_5m'].startswith('trending')),
        ('both_entangled', lambda r: r['state'] == 'entangled' and r['state_5m'] == 'entangled'),
    ]

    results = {}
    for name, cond in combos:
        mask = df.apply(cond, axis=1)
        sub = df[mask]
        if len(sub) == 0:
            continue
        r = {'count': int(len(sub))}
        for fw in forward_windows:
            s = sub[f'fwd{fw}_range'].dropna()
            m = sub[f'fwd{fw}_max_adverse'].dropna()
            if len(s) == 0:
                continue
            r[f'fwd{fw}'] = {
                'n': int(len(s)),
                'range_mean': round(s.mean(), 4),
                'range_p90': round(s.quantile(0.90), 4),
                'max_adv_mean': round(m.mean(), 4),
                'max_adv_p90': round(m.quantile(0.90), 4),
            }
        results[name] = r
    return results


# ============ 网格搜索 ============

def grid_search_single_contract(cdf_raw, ma_period, forward_windows, gap_minutes):
    """
    对单个合约的bar数据, 遍历lookback×threshold网格, 返回每组参数的核心指标
    cdf_raw: 已排序、含close_price/high_price/low_price的DataFrame (不含MA/state列)
    """
    results = []
    # 预计算前瞻波动(只需算一次, 与lookback/threshold无关)
    cdf_fwd = compute_forward_volatility(cdf_raw.copy(), forward_windows)
    cdf_fwd = invalidate_cross_session(cdf_fwd, forward_windows, gap_minutes)

    for lb in GRID_LOOKBACKS:
        for ratio in GRID_THRESHOLD_RATIOS:
            thr = max(int(lb * ratio), 1)
            if thr > lb:
                continue
            min_bars = max(MA_PERIOD_5M, ma_period) + lb + max(forward_windows) + 20
            if len(cdf_raw) < min_bars:
                continue

            # 计算state (快速, 只需rolling)
            cdf = cdf_fwd.copy()
            close = cdf['close_price'].values
            ma = pd.Series(close).rolling(ma_period, min_periods=ma_period).mean().values
            above = (close > ma).astype(np.float64)
            below = (close < ma).astype(np.float64)
            above_count = pd.Series(above).rolling(lb, min_periods=lb).sum().values
            below_count = pd.Series(below).rolling(lb, min_periods=lb).sum().values

            valid = ~np.isnan(above_count)
            is_trend_up = valid & (above_count >= thr)
            is_trend_dn = valid & (below_count >= thr)
            is_entangled = valid & (~is_trend_up) & (~is_trend_dn)

            # 核心指标: fwd30
            fw = 30
            col = f'fwd{fw}_range'
            if col not in cdf.columns:
                continue
            fwd = cdf[col].values

            t_up_vals = fwd[is_trend_up & ~np.isnan(fwd)]
            t_dn_vals = fwd[is_trend_dn & ~np.isnan(fwd)]
            ent_vals = fwd[is_entangled & ~np.isnan(fwd)]

            if len(ent_vals) < 30 or (len(t_up_vals) < 10 and len(t_dn_vals) < 10):
                continue

            ent_mean = ent_vals.mean()
            t_up_mean = t_up_vals.mean() if len(t_up_vals) > 0 else 0
            t_dn_mean = t_dn_vals.mean() if len(t_dn_vals) > 0 else 0

            ratio_up = t_up_mean / ent_mean if ent_mean > 0 else 0
            ratio_dn = t_dn_mean / ent_mean if ent_mean > 0 else 0

            # 趋势延续率
            fw_dir_col = f'fwd{fw}_dir_ret'
            cont_up = cont_dn = None
            if fw_dir_col in cdf.columns:
                dir_vals = cdf[fw_dir_col].values
                t_up_dir = dir_vals[is_trend_up & ~np.isnan(dir_vals)]
                t_dn_dir = dir_vals[is_trend_dn & ~np.isnan(dir_vals)]
                if len(t_up_dir) > 0:
                    cont_up = round((t_up_dir > 0).mean() * 100, 1)
                if len(t_dn_dir) > 0:
                    cont_dn = round((t_dn_dir < 0).mean() * 100, 1)

            results.append({
                'lookback': lb,
                'threshold': thr,
                'ratio': ratio,
                'n_trend_up': int(is_trend_up.sum()),
                'n_trend_dn': int(is_trend_dn.sum()),
                'n_entangled': int(is_entangled.sum()),
                'ent_range_mean': round(ent_mean, 5),
                'tup_range_mean': round(t_up_mean, 5),
                'tdn_range_mean': round(t_dn_mean, 5),
                'ratio_up': round(ratio_up, 3),
                'ratio_dn': round(ratio_dn, 3),
                'max_ratio': round(max(ratio_up, ratio_dn), 3),
                'avg_ratio': round((ratio_up + ratio_dn) / 2, 3) if ratio_up > 0 and ratio_dn > 0
                             else round(max(ratio_up, ratio_dn), 3),
                'cont_up': cont_up,
                'cont_dn': cont_dn,
                'belief_holds': ratio_up > 1.0 and ratio_dn > 1.0,
            })

    return results


def run_grid_search(product, source='db', days=60):
    """对单品种跑 lookback×threshold 网格搜索"""
    print(f'\n{"="*60}')
    print(f'Grid Search: {product.upper()}')
    print(f'{"="*60}')
    print(f'  Lookbacks: {GRID_LOOKBACKS}')
    print(f'  Threshold ratios: {GRID_THRESHOLD_RATIOS}')

    if source == 'parquet':
        df = load_futures_from_parquet(product)
    else:
        df = load_futures_from_db(product, days=days)

    if df.empty:
        print(f'  No data')
        return None

    contracts = sorted(df['symbol'].unique())
    print(f'  Contracts: {len(contracts)}, Total bars: {len(df)}')

    all_grid = []
    for contract in contracts:
        cdf = df[df['symbol'] == contract].copy()
        cdf = cdf.sort_values('datetime').reset_index(drop=True)
        if len(cdf) < MA_PERIOD_5M + max(GRID_LOOKBACKS) + max(FORWARD_WINDOWS) + 20:
            continue

        grid = grid_search_single_contract(cdf, MA_PERIOD_1M, FORWARD_WINDOWS, SESSION_GAP_MINUTES)
        all_grid.extend(grid)
        del cdf
        gc.collect()

    if not all_grid:
        print(f'  No grid results')
        return None

    # 按 lookback+threshold 聚合所有合约
    from collections import defaultdict
    agg = defaultdict(lambda: {'ratio_ups': [], 'ratio_dns': [], 'cont_ups': [],
                                'cont_dns': [], 'count': 0})
    for g in all_grid:
        key = (g['lookback'], g['threshold'])
        a = agg[key]
        a['ratio_ups'].append(g['ratio_up'])
        a['ratio_dns'].append(g['ratio_dn'])
        if g['cont_up'] is not None:
            a['cont_ups'].append(g['cont_up'])
        if g['cont_dn'] is not None:
            a['cont_dns'].append(g['cont_dn'])
        a['count'] += 1

    grid_summary = []
    for (lb, thr), a in sorted(agg.items()):
        r_up = np.mean(a['ratio_ups']) if a['ratio_ups'] else 0
        r_dn = np.mean(a['ratio_dns']) if a['ratio_dns'] else 0
        c_up = np.mean(a['cont_ups']) if a['cont_ups'] else None
        c_dn = np.mean(a['cont_dns']) if a['cont_dns'] else None
        grid_summary.append({
            'lookback': lb,
            'threshold': thr,
            'ratio': round(thr / lb, 2),
            'n_contracts': a['count'],
            'avg_ratio_up': round(r_up, 3),
            'avg_ratio_dn': round(r_dn, 3),
            'avg_ratio': round((r_up + r_dn) / 2, 3) if r_up > 0 and r_dn > 0
                         else round(max(r_up, r_dn), 3),
            'avg_cont_up': round(c_up, 1) if c_up is not None else None,
            'avg_cont_dn': round(c_dn, 1) if c_dn is not None else None,
            'belief_holds': r_up > 1.0 and r_dn > 1.0,
        })

    # 排序: avg_ratio降序
    grid_summary.sort(key=lambda x: -x['avg_ratio'])

    print(f'\n  --- Grid Search Results (Top 15) ---')
    print(f'  {"LB":>3s} {"THR":>4s} {"Ratio":>6s} {"↑/Ent":>7s} {"↓/Ent":>7s} '
          f'{"Avg":>6s} {"Cont↑":>6s} {"Cont↓":>6s} {"OK":>3s}')
    for g in grid_summary[:15]:
        ok = 'Y' if g['belief_holds'] else 'N'
        c_up = f'{g["avg_cont_up"]:.0f}%' if g['avg_cont_up'] is not None else '-'
        c_dn = f'{g["avg_cont_dn"]:.0f}%' if g['avg_cont_dn'] is not None else '-'
        print(f'  {g["lookback"]:3d} {g["threshold"]:4d} {g["ratio"]:6.2f} '
              f'{g["avg_ratio_up"]:7.3f} {g["avg_ratio_dn"]:7.3f} '
              f'{g["avg_ratio"]:6.3f} {c_up:>6s} {c_dn:>6s} {ok:>3s}')

    best = grid_summary[0] if grid_summary else None
    if best:
        print(f'\n  最优参数: lookback={best["lookback"]}, threshold={best["threshold"]} '
              f'({best["ratio"]:.0%}), avg_ratio={best["avg_ratio"]:.3f}x')

    return {
        'product': product.upper(),
        'grid_summary': grid_summary,
        'best': best,
        'raw_count': len(all_grid),
    }


# ============ 处理单品种 ============

def process_product(product, source='db', days=60):
    """处理单个品种"""
    print(f'\n{"="*60}')
    print(f'Processing: {product.upper()}')
    print(f'{"="*60}')

    # 加载数据
    if source == 'parquet':
        df = load_futures_from_parquet(product)
    else:
        df = load_futures_from_db(product, days=days)

    if df.empty or len(df) < MA_PERIOD_5M + LOOKBACK + max(FORWARD_WINDOWS):
        print(f'  Insufficient data: {len(df)} bars')
        return None

    contracts = sorted(df['symbol'].unique())
    print(f'  Contracts: {len(contracts)}, Total bars: {len(df)}')
    print(f'  Date range: {df["datetime"].min()} ~ {df["datetime"].max()}')

    # 按合约逐个处理
    all_dfs = []
    for contract in contracts:
        cdf = df[df['symbol'] == contract].copy()
        min_bars = MA_PERIOD_5M + LOOKBACK + max(FORWARD_WINDOWS) + 20
        if len(cdf) < min_bars:
            continue

        # 1分钟 20MA state
        cdf = compute_states(cdf, MA_PERIOD_1M, LOOKBACK, TREND_THRESHOLD)

        # 5分钟 proxy: 100MA state
        cdf_5m = compute_states(cdf.copy(), MA_PERIOD_5M, LOOKBACK, TREND_THRESHOLD)
        cdf['state_5m'] = cdf_5m['state'].values
        cdf['score_5m'] = cdf_5m['score'].values

        # 前瞻波动
        cdf = compute_forward_volatility(cdf, FORWARD_WINDOWS)

        # 清除跨session数据
        cdf = invalidate_cross_session(cdf, FORWARD_WINDOWS, SESSION_GAP_MINUTES)

        all_dfs.append(cdf)
        del cdf, cdf_5m
        gc.collect()

    if not all_dfs:
        print(f'  No contracts with sufficient bars')
        return None

    merged = pd.concat(all_dfs, ignore_index=True)
    del all_dfs
    gc.collect()

    print(f'  Merged: {len(merged)} bars across {len(contracts)} contracts')

    # 分析
    state_stats = analyze_by_state(merged, FORWARD_WINDOWS)
    score_stats = analyze_by_score_bins(merged, FORWARD_WINDOWS)
    multi_tf_stats = analyze_multi_tf(merged, FORWARD_WINDOWS)

    # 打印核心结果
    print(f'\n  --- State Analysis (1-min 20MA) ---')
    for state, stats in state_stats.items():
        fw30 = stats.get('fwd30', {})
        cont = fw30.get('continuation_rate', '-')
        print(f'  {state:18s}: n={stats["count"]:7d} ({stats["pct"]:5.1f}%) '
              f'fwd30_range={fw30.get("range_mean", 0):.3f}% '
              f'fwd30_adv_p90={fw30.get("max_adv_p90", 0):.3f}% '
              f'continuation={cont}')

    print(f'\n  --- Score Bins ---')
    for label, stats in score_stats.items():
        fw30 = stats.get('fwd30', {})
        print(f'  {label:8s}: n={stats["count"]:7d} '
              f'fwd30_range={fw30.get("range_mean", 0):.3f}% '
              f'fwd30_adv_p90={fw30.get("max_adv_p90", 0):.3f}%')

    if multi_tf_stats:
        print(f'\n  --- Multi-Timeframe (1min + 5min proxy) ---')
        for combo, stats in multi_tf_stats.items():
            fw30 = stats.get('fwd30', {})
            print(f'  {combo:30s}: n={stats["count"]:7d} '
                  f'fwd30_range={fw30.get("range_mean", 0):.3f}% '
                  f'fwd30_adv_p90={fw30.get("max_adv_p90", 0):.3f}%')

    # 信念验证
    t_up = state_stats.get('trending_up', {}).get('fwd30', {})
    t_dn = state_stats.get('trending_down', {}).get('fwd30', {})
    ent = state_stats.get('entangled', {}).get('fwd30', {})

    if t_up and ent:
        ratio_up = t_up.get('range_mean', 0) / ent.get('range_mean', 1) if ent.get('range_mean') else 0
    else:
        ratio_up = 0
    if t_dn and ent:
        ratio_dn = t_dn.get('range_mean', 0) / ent.get('range_mean', 1) if ent.get('range_mean') else 0
    else:
        ratio_dn = 0

    print(f'\n  === 信念验证 ===')
    print(f'  trending_up fwd30波动 / entangled fwd30波动 = {ratio_up:.2f}x')
    print(f'  trending_down fwd30波动 / entangled fwd30波动 = {ratio_dn:.2f}x')
    if ratio_up > 1.0 and ratio_dn > 1.0:
        print(f'  ✓ 信念成立: trending状态下前瞻波动显著大于entangled')
    elif ratio_up > 1.0 or ratio_dn > 1.0:
        print(f'  △ 部分成立: 单方向趋势有更大波动')
    else:
        print(f'  ✗ 信念不成立: trending并未带来更大前瞻波动')

    result = {
        'product': product.upper(),
        'exchange': EXCHANGE_MAP.get(product.lower(), ''),
        'total_bars': int(len(merged)),
        'contracts': len(contracts),
        'date_range': [str(df['datetime'].min()), str(df['datetime'].max())],
        'config': {
            'ma_period_1m': MA_PERIOD_1M,
            'ma_period_5m_proxy': MA_PERIOD_5M,
            'lookback': LOOKBACK,
            'trend_threshold': TREND_THRESHOLD,
            'forward_windows': FORWARD_WINDOWS,
        },
        'state_analysis': state_stats,
        'score_bins': score_stats,
        'multi_timeframe': multi_tf_stats,
        'belief_validation': {
            'trending_up_vs_entangled_ratio': round(ratio_up, 3),
            'trending_down_vs_entangled_ratio': round(ratio_dn, 3),
            'belief_holds': ratio_up > 1.0 and ratio_dn > 1.0,
        },
    }

    del merged
    gc.collect()
    return result


# ============ HTML报告 ============

def generate_html_report(all_results, output_path):
    """生成可视化HTML报告"""
    products = list(all_results.keys())
    fw_key = 'fwd30'

    # 汇总表格数据
    summary_rows = []
    for prod, r in all_results.items():
        t_up = r['state_analysis'].get('trending_up', {}).get(fw_key, {})
        t_dn = r['state_analysis'].get('trending_down', {}).get(fw_key, {})
        ent = r['state_analysis'].get('entangled', {}).get(fw_key, {})
        bv = r['belief_validation']
        summary_rows.append({
            'product': prod,
            'total_bars': r['total_bars'],
            'trend_up_pct': r['state_analysis'].get('trending_up', {}).get('pct', 0),
            'trend_dn_pct': r['state_analysis'].get('trending_down', {}).get('pct', 0),
            'entangled_pct': r['state_analysis'].get('entangled', {}).get('pct', 0),
            'ent_range': ent.get('range_mean', 0),
            'tup_range': t_up.get('range_mean', 0),
            'tdn_range': t_dn.get('range_mean', 0),
            'ratio_up': bv['trending_up_vs_entangled_ratio'],
            'ratio_dn': bv['trending_down_vs_entangled_ratio'],
            'holds': bv['belief_holds'],
            'tup_cont': t_up.get('continuation_rate', '-'),
            'tdn_cont': t_dn.get('continuation_rate', '-'),
        })

    # Score bins chart data (first product as example)
    first_prod = products[0] if products else None
    score_chart_data = []
    if first_prod:
        sb = all_results[first_prod].get('score_bins', {})
        for label in ['10/10', '9/10', '8/10', '7/10', '6/10', '≤5/10']:
            d = sb.get(label, {}).get(fw_key, {})
            score_chart_data.append({
                'label': label,
                'range_mean': d.get('range_mean', 0),
                'max_adv_p90': d.get('max_adv_p90', 0),
            })

    # Multi-TF data
    mtf_chart_data = []
    if first_prod:
        mt = all_results[first_prod].get('multi_timeframe', {})
        for combo in ['both_trending', '1m_trend_5m_entangled',
                      '1m_entangled_5m_trend', 'both_entangled']:
            d = mt.get(combo, {}).get(fw_key, {})
            mtf_chart_data.append({
                'combo': combo.replace('_', ' '),
                'range_mean': d.get('range_mean', 0),
                'max_adv_p90': d.get('max_adv_p90', 0),
                'n': d.get('n', 0),
            })

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>B043 MA纠缠度 vs 趋势持续性</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
  body {{ background: #1a1a2e; color: #ddd; font-family: 'Segoe UI', sans-serif; margin: 0; padding: 20px; }}
  .header {{ text-align: center; padding: 20px; border-bottom: 3px solid #FF6B00; margin-bottom: 20px; }}
  .header h1 {{ color: #FF6B00; margin: 0; }}
  .header p {{ color: #888; font-size: 14px; }}
  .card-row {{ display: flex; gap: 12px; flex-wrap: wrap; justify-content: center; margin-bottom: 20px; }}
  .card {{ background: #16213e; border-radius: 8px; padding: 16px 24px; text-align: center; min-width: 140px; }}
  .card .val {{ font-size: 28px; font-weight: bold; }}
  .card .label {{ font-size: 12px; color: #888; margin-top: 4px; }}
  table {{ width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 13px; }}
  th {{ background: #16213e; color: #888; padding: 8px; text-align: left; border-bottom: 2px solid #333; }}
  td {{ padding: 8px; border-bottom: 1px solid #2a2a4a; }}
  .chart-container {{ display: flex; flex-wrap: wrap; gap: 16px; justify-content: center; }}
  .chart-box {{ background: #16213e; border-radius: 8px; padding: 10px; }}
  .hold {{ color: #00e676; font-weight: bold; }}
  .fail {{ color: #FF4444; font-weight: bold; }}
  .partial {{ color: #FFD700; font-weight: bold; }}
</style>
</head><body>

<div class="header">
  <h1>B043: MA纠缠度 vs 趋势持续性</h1>
  <p>信念: 价格持续偏离20MA(≥8/10根) → 趋势加速(不卖跨) / 与MA纠缠 → 震荡(可卖跨)</p>
  <p>前瞻窗口: 30根1分钟bar | 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
</div>

<!-- 汇总卡片 -->
<div class="card-row">
  <div class="card">
    <div class="val" style="color:#4fc3f7">{len(products)}</div>
    <div class="label">品种数</div>
  </div>
  <div class="card">
    <div class="val" style="color:#00e676">{sum(1 for r in summary_rows if r['holds'])}</div>
    <div class="label">信念成立</div>
  </div>
  <div class="card">
    <div class="val" style="color:#FF4444">{sum(1 for r in summary_rows if not r['holds'])}</div>
    <div class="label">信念不成立</div>
  </div>
  <div class="card">
    <div class="val" style="color:#FFD700">{np.mean([r['ratio_up'] for r in summary_rows if r['ratio_up'] > 0]):.2f}x</div>
    <div class="label">平均趋势/纠缠波动比</div>
  </div>
</div>

<!-- 品种汇总表 -->
<h2 style="color:#FF6B00">品种汇总 (fwd30)</h2>
<table>
<thead><tr>
  <th>品种</th><th>Bar数</th>
  <th>Trend↑%</th><th>Trend↓%</th><th>纠缠%</th>
  <th>纠缠波动</th><th>趋势↑波动</th><th>趋势↓波动</th>
  <th>↑/纠缠</th><th>↓/纠缠</th>
  <th>↑延续率</th><th>↓延续率</th>
  <th>结论</th>
</tr></thead>
<tbody>"""

    for row in sorted(summary_rows, key=lambda x: -(x['ratio_up'] + x['ratio_dn'])/2):
        cls = 'hold' if row['holds'] else 'fail'
        verdict = '成立' if row['holds'] else '不成立'
        html += f"""<tr>
  <td style="color:#FFD700;font-weight:bold">{row['product']}</td>
  <td>{row['total_bars']:,}</td>
  <td>{row['trend_up_pct']:.1f}%</td>
  <td>{row['trend_dn_pct']:.1f}%</td>
  <td>{row['entangled_pct']:.1f}%</td>
  <td>{row['ent_range']:.3f}%</td>
  <td>{row['tup_range']:.3f}%</td>
  <td>{row['tdn_range']:.3f}%</td>
  <td style="font-weight:bold;color:{'#00e676' if row['ratio_up']>1 else '#FF4444'}">{row['ratio_up']:.2f}x</td>
  <td style="font-weight:bold;color:{'#00e676' if row['ratio_dn']>1 else '#FF4444'}">{row['ratio_dn']:.2f}x</td>
  <td>{row['tup_cont']}</td>
  <td>{row['tdn_cont']}</td>
  <td class="{cls}">{verdict}</td>
</tr>"""

    html += """</tbody></table>

<!-- 图表区 -->
<div class="chart-container">
  <div class="chart-box">
    <div id="score-chart" style="width:600px;height:400px"></div>
  </div>
  <div class="chart-box">
    <div id="mtf-chart" style="width:600px;height:400px"></div>
  </div>
</div>

<script>
// Score bins chart
var scoreChart = echarts.init(document.getElementById('score-chart'));
scoreChart.setOption({
  title: {text: '偏离度评分 vs 前瞻波动 (""" + (first_prod or '') + """)', textStyle: {color: '#ddd', fontSize: 14}},
  tooltip: {trigger: 'axis'},
  legend: {textStyle: {color: '#888'}, bottom: 0},
  xAxis: {type: 'category', data: """ + json.dumps([d['label'] for d in score_chart_data]) + """,
           axisLabel: {color: '#888'}},
  yAxis: {type: 'value', name: '%', axisLabel: {color: '#888'}, splitLine: {lineStyle: {color: '#2a2a4a'}}},
  series: [
    {name: 'Range均值', type: 'bar', data: """ + json.dumps([d['range_mean'] for d in score_chart_data]) + """,
     itemStyle: {color: '#FF6B00'}},
    {name: 'MaxAdv P90', type: 'bar', data: """ + json.dumps([d['max_adv_p90'] for d in score_chart_data]) + """,
     itemStyle: {color: '#FF4444'}}
  ]
});

// Multi-TF chart
var mtfChart = echarts.init(document.getElementById('mtf-chart'));
mtfChart.setOption({
  title: {text: '多时间框架联合分析 (""" + (first_prod or '') + """)', textStyle: {color: '#ddd', fontSize: 14}},
  tooltip: {trigger: 'axis'},
  legend: {textStyle: {color: '#888'}, bottom: 0},
  xAxis: {type: 'category', data: """ + json.dumps([d['combo'] for d in mtf_chart_data]) + """,
           axisLabel: {color: '#888', rotate: 15}},
  yAxis: {type: 'value', name: '%', axisLabel: {color: '#888'}, splitLine: {lineStyle: {color: '#2a2a4a'}}},
  series: [
    {name: 'Range均值', type: 'bar', data: """ + json.dumps([d['range_mean'] for d in mtf_chart_data]) + """,
     itemStyle: {color: '#4fc3f7'}},
    {name: 'MaxAdv P90', type: 'bar', data: """ + json.dumps([d['max_adv_p90'] for d in mtf_chart_data]) + """,
     itemStyle: {color: '#FF4444'}}
  ]
});
</script>

<!-- 详细JSON -->
<details style="margin-top:20px">
<summary style="cursor:pointer;color:#888">展开原始JSON数据</summary>
<pre style="background:#16213e;padding:12px;border-radius:6px;overflow-x:auto;font-size:11px">
""" + json.dumps(all_results, ensure_ascii=False, indent=2, default=str) + """
</pre>
</details>

</body></html>"""

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'\nHTML report: {output_path}')


def generate_grid_html_report(grid_results, output_path):
    """生成网格搜索可视化HTML报告"""
    products = list(grid_results.keys())

    # 跨品种汇总: 每个(lb, thr)的平均ratio
    from collections import defaultdict
    cross_agg = defaultdict(lambda: {'ratios': [], 'cont_ups': [], 'cont_dns': [], 'holds': 0, 'total': 0})
    for prod, gr in grid_results.items():
        for row in gr.get('grid_summary', []):
            key = (row['lookback'], row['threshold'])
            cross_agg[key]['ratios'].append(row['avg_ratio'])
            cross_agg[key]['total'] += 1
            if row['belief_holds']:
                cross_agg[key]['holds'] += 1
            if row.get('avg_cont_up') is not None:
                cross_agg[key]['cont_ups'].append(row['avg_cont_up'])
            if row.get('avg_cont_dn') is not None:
                cross_agg[key]['cont_dns'].append(row['avg_cont_dn'])

    cross_rows = []
    for (lb, thr), a in sorted(cross_agg.items()):
        cross_rows.append({
            'lookback': lb, 'threshold': thr,
            'ratio_pct': f'{thr/lb:.0%}',
            'avg_ratio': round(np.mean(a['ratios']), 3),
            'max_ratio': round(max(a['ratios']), 3),
            'min_ratio': round(min(a['ratios']), 3),
            'hold_rate': f'{a["holds"]}/{a["total"]}',
            'avg_cont_up': round(np.mean(a['cont_ups']), 1) if a['cont_ups'] else None,
            'avg_cont_dn': round(np.mean(a['cont_dns']), 1) if a['cont_dns'] else None,
        })
    cross_rows.sort(key=lambda x: -x['avg_ratio'])

    # Best per product
    best_rows = []
    for prod, gr in sorted(grid_results.items()):
        best = gr.get('best')
        if best:
            best_rows.append({
                'product': prod,
                'lookback': best['lookback'],
                'threshold': best['threshold'],
                'ratio_pct': f'{best["ratio"]:.0%}',
                'avg_ratio': best['avg_ratio'],
                'cont_up': best.get('avg_cont_up'),
                'cont_dn': best.get('avg_cont_dn'),
                'holds': best['belief_holds'],
            })

    # Heatmap data: lookback as Y, threshold_ratio as X, avg_ratio as Z
    heatmap_data = []
    lb_vals = sorted(set(r['lookback'] for r in cross_rows))
    thr_ratio_vals = sorted(set(round(r['threshold']/r['lookback'], 2) for r in cross_rows))
    for r in cross_rows:
        thr_r = round(r['threshold'] / r['lookback'], 2)
        try:
            x_idx = thr_ratio_vals.index(thr_r)
            y_idx = lb_vals.index(r['lookback'])
            heatmap_data.append([x_idx, y_idx, r['avg_ratio']])
        except ValueError:
            pass

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>B043 Grid Search: MA纠缠度最优参数</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
  body {{ background: #1a1a2e; color: #ddd; font-family: 'Segoe UI', sans-serif; margin: 0; padding: 20px; }}
  .header {{ text-align: center; padding: 20px; border-bottom: 3px solid #FF6B00; margin-bottom: 20px; }}
  .header h1 {{ color: #FF6B00; margin: 0; }}
  .header p {{ color: #888; font-size: 14px; }}
  .card-row {{ display: flex; gap: 12px; flex-wrap: wrap; justify-content: center; margin-bottom: 20px; }}
  .card {{ background: #16213e; border-radius: 8px; padding: 16px 24px; text-align: center; min-width: 140px; }}
  .card .val {{ font-size: 28px; font-weight: bold; }}
  .card .label {{ font-size: 12px; color: #888; margin-top: 4px; }}
  table {{ width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 13px; }}
  th {{ background: #16213e; color: #888; padding: 8px; text-align: left; border-bottom: 2px solid #333; }}
  td {{ padding: 8px; border-bottom: 1px solid #2a2a4a; }}
  .chart-container {{ display: flex; flex-wrap: wrap; gap: 16px; justify-content: center; margin: 20px 0; }}
  .chart-box {{ background: #16213e; border-radius: 8px; padding: 10px; }}
  .hold {{ color: #00e676; font-weight: bold; }}
  .fail {{ color: #FF4444; font-weight: bold; }}
  h2 {{ color: #FF6B00; margin-top: 30px; }}
</style>
</head><body>

<div class="header">
  <h1>B043: MA纠缠度网格搜索</h1>
  <p>寻找最优 lookback × threshold 参数组合</p>
  <p>核心指标: trending fwd30_range / entangled fwd30_range (>1.0 = 信念成立)</p>
  <p>品种: {', '.join(products)} | 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
</div>

<div class="card-row">
  <div class="card">
    <div class="val" style="color:#4fc3f7">{len(products)}</div>
    <div class="label">品种数</div>
  </div>
  <div class="card">
    <div class="val" style="color:#FFD700">{cross_rows[0]['lookback'] if cross_rows else '-'}×{cross_rows[0]['threshold'] if cross_rows else '-'}</div>
    <div class="label">全局最优参数</div>
  </div>
  <div class="card">
    <div class="val" style="color:#00e676">{cross_rows[0]['avg_ratio'] if cross_rows else '-'}x</div>
    <div class="label">最优avg_ratio</div>
  </div>
  <div class="card">
    <div class="val" style="color:#FF6B00">{cross_rows[0]['ratio_pct'] if cross_rows else '-'}</div>
    <div class="label">最优阈值比</div>
  </div>
</div>

<!-- Heatmap -->
<div class="chart-container">
  <div class="chart-box">
    <div id="heatmap" style="width:700px;height:450px"></div>
  </div>
  <div class="chart-box">
    <div id="bar-chart" style="width:600px;height:450px"></div>
  </div>
</div>

<!-- 跨品种参数排名 -->
<h2>跨品种参数排名 (avg_ratio降序)</h2>
<table>
<thead><tr>
  <th>Lookback</th><th>Threshold</th><th>阈值比</th>
  <th>Avg Ratio</th><th>Max</th><th>Min</th>
  <th>成立率</th><th>Cont↑</th><th>Cont↓</th>
</tr></thead>
<tbody>"""

    for i, r in enumerate(cross_rows[:25]):
        bg = 'background:#1a3a1a;' if r['avg_ratio'] >= 1.0 else ''
        html += f"""<tr style="{bg}">
  <td>{r['lookback']}</td><td>{r['threshold']}</td><td>{r['ratio_pct']}</td>
  <td style="font-weight:bold;color:{'#00e676' if r['avg_ratio']>=1.0 else '#FF4444'}">{r['avg_ratio']:.3f}x</td>
  <td>{r['max_ratio']:.3f}x</td><td>{r['min_ratio']:.3f}x</td>
  <td>{r['hold_rate']}</td>
  <td>{f'{r["avg_cont_up"]:.0f}%' if r['avg_cont_up'] else '-'}</td>
  <td>{f'{r["avg_cont_dn"]:.0f}%' if r['avg_cont_dn'] else '-'}</td>
</tr>"""

    html += """</tbody></table>

<!-- 各品种最优参数 -->
<h2>各品种最优参数</h2>
<table>
<thead><tr>
  <th>品种</th><th>Lookback</th><th>Threshold</th><th>阈值比</th>
  <th>Avg Ratio</th><th>Cont↑</th><th>Cont↓</th><th>成立</th>
</tr></thead>
<tbody>"""

    for r in best_rows:
        cls = 'hold' if r['holds'] else 'fail'
        html += f"""<tr>
  <td style="color:#FFD700;font-weight:bold">{r['product']}</td>
  <td>{r['lookback']}</td><td>{r['threshold']}</td><td>{r['ratio_pct']}</td>
  <td class="{cls}">{r['avg_ratio']:.3f}x</td>
  <td>{f'{r["cont_up"]:.0f}%' if r['cont_up'] else '-'}</td>
  <td>{f'{r["cont_dn"]:.0f}%' if r['cont_dn'] else '-'}</td>
  <td class="{cls}">{'Y' if r['holds'] else 'N'}</td>
</tr>"""

    html += f"""</tbody></table>

</body></html>"""

    # 构建JS数据（避免f-string内嵌dict冲突）
    bar_data_list = [{'name': f"LB{r['lookback']}x{r['threshold']}", 'value': r['avg_ratio']}
                     for r in cross_rows[:10]]
    vis_min = min(r['avg_ratio'] for r in cross_rows) if cross_rows else 0.8
    vis_max = max(r['avg_ratio'] for r in cross_rows) if cross_rows else 1.2
    js_block = """<script>
var heatmapChart = echarts.init(document.getElementById('heatmap'));
heatmapChart.setOption({
  title: {text: '参数热力图: avg_ratio', textStyle: {color: '#ddd', fontSize: 14}},
  tooltip: {formatter: function(p) { return 'LB=' + %s[p.data[1]] + ', ratio=' + %s[p.data[0]] + '<br>avg_ratio: ' + p.data[2].toFixed(3) + 'x'; }},
  xAxis: {type: 'category', data: %s, name: 'threshold/lookback', axisLabel: {color: '#888'}},
  yAxis: {type: 'category', data: %s, name: 'lookback', axisLabel: {color: '#888'}},
  visualMap: {min: %s, max: %s, orient: 'horizontal', left: 'center', bottom: 0,
    inRange: {color: ['#313695','#4575b4','#74add1','#abd9e9','#fee090','#fdae61','#f46d43','#d73027']},
    textStyle: {color: '#888'}},
  series: [{type: 'heatmap', data: %s,
    label: {show: true, formatter: function(p) { return p.data[2].toFixed(2); }, fontSize: 10, color: '#fff'},
    emphasis: {itemStyle: {shadowBlur: 10}}}]
});
var barData = %s;
var barChart = echarts.init(document.getElementById('bar-chart'));
barChart.setOption({
  title: {text: 'Top 10 参数组合', textStyle: {color: '#ddd', fontSize: 14}},
  tooltip: {},
  xAxis: {type: 'category', data: barData.map(d => d.name), axisLabel: {color: '#888', rotate: 30}},
  yAxis: {type: 'value', name: 'avg_ratio', axisLabel: {color: '#888'}, splitLine: {lineStyle: {color: '#2a2a4a'}}},
  series: [{type: 'bar', data: barData.map(d => d.value),
    itemStyle: {color: function(p) { return p.data >= 1.0 ? '#00e676' : '#FF4444'; }}}]
});
</script>""" % (
        json.dumps(lb_vals), json.dumps(thr_ratio_vals),
        json.dumps([str(v) for v in thr_ratio_vals]),
        json.dumps([str(v) for v in lb_vals]),
        vis_min, vis_max,
        json.dumps(heatmap_data),
        json.dumps(bar_data_list),
    )

    raw_json = json.dumps(grid_results, ensure_ascii=False, indent=2, default=str)
    details_block = '<details style="margin-top:20px"><summary style="cursor:pointer;color:#888">展开原始JSON</summary>'
    details_block += '<pre style="background:#16213e;padding:12px;border-radius:6px;overflow-x:auto;font-size:11px">'
    details_block += raw_json + '</pre></details>'

    # 在 </body> 前插入JS和details
    html = html.replace('</body></html>', js_block + '\n' + details_block + '\n</body></html>')

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'\nGrid HTML report: {output_path}')


# ============ 主入口 ============

def _run_one_product(product, mode, source, days):
    """模块级函数 — 子进程可pickle"""
    try:
        if mode == 'grid':
            return ('grid', product.upper(), run_grid_search(product, source=source, days=days))
        else:
            return ('normal', product.upper(), process_product(product, source=source, days=days))
    except Exception as e:
        import traceback
        traceback.print_exc()
        return ('error', product.upper(), str(e))


def main():
    parser = argparse.ArgumentParser(description='B043: MA纠缠度 vs 趋势持续性')
    parser.add_argument('--product', default='ag', help='品种代码, 逗号分隔 (default: ag)')
    parser.add_argument('--source', choices=['db', 'parquet'], default='db',
                        help='数据源 (default: db)')
    parser.add_argument('--days', type=int, default=60, help='DB模式回看天数 (default: 60)')
    parser.add_argument('--all', action='store_true', help='跑全部品种')
    parser.add_argument('--grid', action='store_true', help='网格搜索lookback×threshold最优参数')
    parser.add_argument('--output', default=None, help='结果输出目录')
    parser.add_argument('--parquet-dir', default=None, help='覆盖parquet数据目录')
    parser.add_argument('--workers', type=int, default=0, help='并行进程数 (0=自动)')
    args = parser.parse_args()

    # 覆盖parquet路径
    global PARQUET_DIR
    if args.parquet_dir:
        PARQUET_DIR = args.parquet_dir

    if args.all:
        products = sorted(EXCHANGE_MAP.keys())
    else:
        products = [p.strip().lower() for p in args.product.split(',')]

    output_dir = args.output or RESULT_DIR
    os.makedirs(output_dir, exist_ok=True)

    print(f'B043 MA纠缠度回测')
    print(f'品种: {", ".join(p.upper() for p in products)}')
    print(f'数据源: {args.source}')
    print(f'配置: 1m-MA{MA_PERIOD_1M}, 5m-MA{MA_PERIOD_5M}, LB{LOOKBACK}, '
          f'Threshold{TREND_THRESHOLD}/{LOOKBACK}')

    all_results = {}
    grid_results = {}

    # 决定并行度
    n_workers = args.workers
    if n_workers <= 0:
        try:
            import psutil
            avail_gb = psutil.virtual_memory().available / (1024**3)
            n_cpu = os.cpu_count() or 4
            est_per_proc = 2  # 估算每进程2GB
            n_workers = max(1, min(int(avail_gb / est_per_proc) - 1, n_cpu - 1, len(products)))
        except ImportError:
            n_workers = min(4, len(products))

    use_parallel = len(products) > 1 and n_workers > 1
    run_mode = 'grid' if args.grid else 'normal'
    run_source = args.source
    run_days = args.days

    if use_parallel:
        from concurrent.futures import ProcessPoolExecutor, as_completed
        print(f'\n并行执行: {n_workers} workers, {len(products)} 品种')
        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            futures = {pool.submit(_run_one_product, p, run_mode, run_source, run_days): p
                       for p in products}
            for fut in as_completed(futures):
                mode, prod, result = fut.result()
                if mode == 'grid' and result:
                    grid_results[prod] = result
                    json_path = os.path.join(output_dir, f'{prod}_grid.json')
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
                elif mode == 'normal' and result:
                    all_results[prod] = result
                    json_path = os.path.join(output_dir, f'{prod}.json')
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
                elif mode == 'error':
                    print(f'  [ERROR] {prod}: {result}')
    else:
        for product in products:
            mode, prod, result = _run_one_product(product, run_mode, run_source, run_days)
            if mode == 'grid' and result:
                grid_results[prod] = result
                json_path = os.path.join(output_dir, f'{prod}_grid.json')
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2, default=str)
            elif mode == 'normal' and result:
                all_results[prod] = result
                json_path = os.path.join(output_dir, f'{prod}.json')
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2, default=str)
            elif mode == 'error':
                print(f'  [ERROR] {prod}: {result}')
            gc.collect()

    # 网格搜索结果汇总
    if args.grid and grid_results:
        grid_all_path = os.path.join(output_dir, '_GRID_SUMMARY.json')
        with open(grid_all_path, 'w', encoding='utf-8') as f:
            json.dump(grid_results, f, ensure_ascii=False, indent=2, default=str)
        print(f'\nGrid summary: {grid_all_path}')

        # 打印跨品种最优参数
        print(f'\n{"="*60}')
        print(f'跨品种最优参数汇总:')
        for prod, gr in grid_results.items():
            best = gr.get('best')
            if best:
                print(f'  {prod}: lookback={best["lookback"]}, threshold={best["threshold"]} '
                      f'({best["ratio"]:.0%}), ratio={best["avg_ratio"]:.3f}x '
                      f'cont↑={best.get("avg_cont_up", "-")} cont↓={best.get("avg_cont_dn", "-")}')

        # 生成网格搜索HTML报告
        grid_html_path = os.path.join(output_dir, 'grid_report.html')
        generate_grid_html_report(grid_results, grid_html_path)
        return

    if not all_results:
        print('\nNo results generated')
        return

    # 汇总JSON
    summary_path = os.path.join(output_dir, '_ALL_SUMMARY.json')
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)
    print(f'\nSummary JSON: {summary_path}')

    # HTML报告
    html_path = os.path.join(output_dir, 'report.html')
    generate_html_report(all_results, html_path)

    # 全品种信念验证汇总
    holds = sum(1 for r in all_results.values() if r['belief_validation']['belief_holds'])
    total = len(all_results)
    print(f'\n{"="*60}')
    print(f'全品种信念验证: {holds}/{total} ({holds/total*100:.0f}%) 品种成立')
    ratios = [max(r['belief_validation']['trending_up_vs_entangled_ratio'],
                  r['belief_validation']['trending_down_vs_entangled_ratio'])
              for r in all_results.values()]
    print(f'趋势/纠缠波动比: 平均{np.mean(ratios):.2f}x, '
          f'中位{np.median(ratios):.2f}x, '
          f'范围[{np.min(ratios):.2f}, {np.max(ratios):.2f}]')


if __name__ == '__main__':
    main()
