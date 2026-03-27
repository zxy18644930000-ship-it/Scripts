#!/usr/bin/env python3
"""
B043 Phase 2: MA纠缠度直接验证 — 用期权PnL而非期货波幅

思路:
  1. 每30分钟选一个入场点
  2. 检查MA纠缠状态 (1分钟20MA + 5分钟20MA双时间框架)
  3. 找ATM跨式期权对, 模拟卖出
  4. 30/60分钟后平仓, 算实际PnL
  5. 对比: 纠缠时卖 vs 趋势时卖 的盈亏差异

数据:
  期货: {EXCHANGE}/{PRODUCT}/{CONTRACT}.parquet (列: datetime,open,high,low,close,volume)
  期权: {EXCHANGE}/{PRODUCT}/{YYYY-MM}/{SYMBOL}.parquet (列同上, 文件名=symbol)

用法:
  python3 B043_ma_tangle_direct.py --product ag
  python3 B043_ma_tangle_direct.py --product ag,p,cu --workers 4
  python3 B043_ma_tangle_direct.py --all --workers 12
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
MA_PERIOD_1M = 20
MA_PERIOD_5M = 20      # 真5分钟K线的20MA
LOOKBACK = 10           # 回看10根
THRESHOLD = 7           # ≥7/10 = trending (实用推荐参数)
HOLD_PERIODS = [30, 60] # 持仓时间(分钟)
ENTRY_INTERVAL = 30     # 每30分钟一个入场点
SESSION_GAP_MINUTES = 30

FUTURES_DIR = '/mnt/d/backtest_data/Futures_parquet/'
OPTIONS_DIR = '/mnt/d/backtest_data/Options_parquet/'
# Mac本地兼容
if not os.path.exists('/mnt/d/'):
    FUTURES_DIR = os.path.expanduser('~/Downloads/期货数据_parquet/')
    OPTIONS_DIR = os.path.expanduser('~/Downloads/期权_parquet/')

RESULT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ma_tangle_direct_results')

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
    """加载期货1分钟K线 — 自动适配格式"""
    import pyarrow.parquet as pq

    exchange = EXCHANGE_MAP.get(product.lower(), '')
    if not exchange:
        return pd.DataFrame()

    prod_upper = product.upper()

    # 格式C: 目录下每合约一文件
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

    # 格式A: 单品种文件
    path_a = os.path.join(FUTURES_DIR, exchange, f'{prod_upper}.parquet')
    if os.path.exists(path_a):
        df = pq.read_table(path_a, columns=['datetime', 'symbol', 'open', 'close', 'high', 'low', 'volume']).to_pandas()
        df.rename(columns={'open': 'open_price', 'close': 'close_price',
                           'high': 'high_price', 'low': 'low_price'}, inplace=True)
        pattern = rf'^{prod_upper}\d{{3,4}}$'
        return df[df['symbol'].str.match(pattern)].copy()

    return pd.DataFrame()


def load_options_for_contract(product, contract_yymm):
    """加载特定合约月份的所有期权bar

    Returns: dict[symbol] -> DataFrame
    """
    import pyarrow.parquet as pq

    exchange = EXCHANGE_MAP.get(product.lower(), '')
    prod_upper = product.upper()
    prod_lower = product.lower()

    # 尝试查找对应月份目录
    # 合约: AG2307 -> yymm=2307 -> 月份目录 2023-07
    yy = contract_yymm[:2]
    mm = contract_yymm[2:]
    year = f'20{yy}'
    month_dir = f'{year}-{mm}'

    opt_base = os.path.join(OPTIONS_DIR, exchange, prod_upper)
    if not os.path.isdir(opt_base):
        # 尝试小写
        opt_base = os.path.join(OPTIONS_DIR, exchange, prod_lower)

    month_path = os.path.join(opt_base, month_dir)
    if not os.path.isdir(month_path):
        return {}

    result = {}
    opt_yymm = _contract_yymm_for_exchange(contract_yymm, exchange)
    files = [f for f in os.listdir(month_path) if f.endswith('.parquet')]
    for fname in files:
        sym = fname.replace('.parquet', '')
        clean = sym.split('.')[-1]  # strip exchange prefix

        # 匹配: 适配所有交易所格式
        if exchange in ('DCE', 'GFEX'):
            pat = rf'^(?:{prod_upper}|{prod_lower}){opt_yymm}-[CP]-\d+$'
        else:
            pat = rf'^(?:{prod_upper}|{prod_lower}){opt_yymm}[CP]\d+$'
        if not re.match(pat, clean, re.IGNORECASE):
            continue

        try:
            df = pq.read_table(os.path.join(month_path, fname),
                               columns=['datetime', 'close', 'volume']).to_pandas()
        except Exception:
            continue
        if df.empty:
            continue

        df['symbol'] = clean
        result[clean] = df

    return result


def _contract_yymm_for_exchange(contract_yymm, exchange):
    """4位yymm转交易所期权格式: CZCE用3位(2402→402), 其他不变"""
    if exchange == 'CZCE':
        return contract_yymm[1:]  # 2402 → 402
    return contract_yymm


def parse_option_symbol(sym, product):
    """解析期权symbol -> (opt_yymm, cp, strike)
    SHFE/INE: ag2307C4050 -> ('2307', 'C', 4050)
    DCE/GFEX: p2402-C-5600 -> ('2402', 'C', 5600)
    CZCE: MA402C2125 -> ('402', 'C', 2125)  # 注意3位
    """
    prod_upper = product.upper()
    prod_lower = product.lower()
    clean = sym.split('.')[-1]  # strip prefix

    # DCE/GFEX dash format: p2402-C-5600
    m = re.match(rf'^(?:{prod_upper}|{prod_lower})(\d{{3,4}})-([CP])-(\d+)$', clean, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2).upper(), int(m.group(3))

    # SHFE/INE/CZCE no-dash: ag2402C5000 or MA402C2125
    m = re.match(rf'^(?:{prod_upper}|{prod_lower})(\d{{3,4}})([CP])(\d+)$', clean, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2).upper(), int(m.group(3))

    return None


# ============ MA状态计算 ============

def compute_ma_states(df, ma_period, lookback, threshold):
    """计算MA偏离状态, 返回添加了state列的df"""
    close = df['close_price'].values
    ma = pd.Series(close).rolling(ma_period, min_periods=ma_period).mean().values
    above = (close > ma).astype(np.float64)
    below = (close < ma).astype(np.float64)
    above_count = pd.Series(above).rolling(lookback, min_periods=lookback).sum().values
    below_count = pd.Series(below).rolling(lookback, min_periods=lookback).sum().values

    state = np.full(len(df), 'warmup', dtype='U20')
    valid = ~np.isnan(above_count)
    state[valid & (above_count >= threshold)] = 'trending_up'
    state[valid & (below_count >= threshold)] = 'trending_down'
    state[valid & (above_count < threshold) & (below_count < threshold)] = 'entangled'

    df = df.copy()
    df['ma_state'] = state
    return df


def compute_5m_states(df_1m, lookback, threshold):
    """从1分钟bar重采样出5分钟bar, 计算20MA状态

    跨session连续计算MA (和实际看盘一致), 不按session重启.
    非交易时段的NaN自然被dropna()移除, MA只看有效bar.
    """
    df = df_1m.copy()
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.set_index('datetime')

    # 整体重采样 — 跨session连续
    r5 = df['close_price'].resample('5min').last().dropna()

    if len(r5) < MA_PERIOD_5M + lookback:
        return np.full(len(df_1m), 'warmup', dtype='U20')

    r5df = pd.DataFrame({'close_price': r5.values}, index=r5.index)
    r5df = compute_ma_states(r5df, MA_PERIOD_5M, lookback, threshold)

    # 将5分钟状态映射回1分钟: forward fill
    df_1m_dt = pd.to_datetime(df_1m['datetime'])
    state_5m = r5df['ma_state'].reindex(df_1m_dt, method='ffill')
    state_5m = state_5m.fillna('warmup')
    return state_5m.values


# ============ 核心回测 ============

OTM_MIN = 0.04   # 虚值度下限 4%
OTM_MAX = 0.08   # 虚值度上限 8%
OTM_FALLBACK_MIN = 0.03  # 回退下限
OTM_FALLBACK_MAX = 0.10  # 回退上限


def find_otm_pair(options_data, underlying_price, product, contract_yymm):
    """找OTM宽跨对: C在标的上方4-8%, P在下方4-8%, 尽量对称

    选对逻辑(遵循用户三维评分):
    1. 虚值度对称性 — 两腿OTM%越接近越好
    2. 虚值度范围 — 4-8%甜点区, 回退到3-10%
    3. 流动性 — 用volume代理, 过滤无量合约
    """
    exchange = EXCHANGE_MAP.get(product.lower(), '')
    opt_yymm = _contract_yymm_for_exchange(contract_yymm, exchange)

    calls = {}  # strike -> sym
    puts = {}   # strike -> sym
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
        c_cands = []
        for strike in calls:
            otm = (strike - underlying_price) / underlying_price
            if otm_lo <= otm <= otm_hi:
                c_cands.append((strike, otm))
        p_cands = []
        for strike in puts:
            otm = (underlying_price - strike) / underlying_price
            if otm_lo <= otm <= otm_hi:
                p_cands.append((strike, otm))
        if not c_cands or not p_cands:
            return None, None
        # 找最对称的配对
        best_pair = None
        best_score = float('inf')
        for c_strike, c_otm in c_cands:
            for p_strike, p_otm in p_cands:
                asym = abs(c_otm - p_otm)
                if asym < best_score:
                    best_score = asym
                    best_pair = (calls[c_strike], puts[p_strike])
        return best_pair

    # 先试4-8%甜点区
    result = _find_best(OTM_MIN, OTM_MAX)
    if result and result[0] and result[1]:
        return result

    # 回退到3-10%
    result = _find_best(OTM_FALLBACK_MIN, OTM_FALLBACK_MAX)
    if result and result[0] and result[1]:
        return result

    return None, None


def backtest_single_contract(futures_df, options_data, product, contract_yymm,
                             lookback, threshold, hold_periods, entry_interval):
    """单合约回测

    Returns: list of trade dicts
    """
    # 期货排序
    fdf = futures_df.sort_values('datetime').reset_index(drop=True)
    fdf['datetime'] = pd.to_datetime(fdf['datetime'])

    if len(fdf) < MA_PERIOD_5M * 5 + lookback + max(hold_periods):
        return []

    # 1分钟 MA状态
    fdf = compute_ma_states(fdf, MA_PERIOD_1M, lookback, threshold)

    # 5分钟 MA状态 (真正重采样)
    state_5m = compute_5m_states(fdf, lookback, threshold)
    fdf['state_5m'] = state_5m

    # 合并状态
    def combined_state(row):
        s1 = row['ma_state']
        s5 = row['state_5m']
        if s1 == 'warmup' or s5 == 'warmup':
            return 'warmup'
        if s1.startswith('trending') and s5.startswith('trending'):
            return 'both_trending'
        if s1 == 'entangled' and s5 == 'entangled':
            return 'both_entangled'
        return 'mixed'

    fdf['combined_state'] = fdf.apply(combined_state, axis=1)

    # Session标记 (不跨session交易)
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

    # 选入场点: 每 entry_interval 分钟一个
    trades = []
    last_entry_idx = -entry_interval

    for idx in range(len(fdf)):
        if idx - last_entry_idx < entry_interval:
            continue

        row = fdf.iloc[idx]
        state = row['combined_state']
        state_1m = row['ma_state']
        if state == 'warmup':
            continue

        # 确保持仓期内不跨session
        max_hold = max(hold_periods)
        if row['dist_to_end'] < max_hold:
            continue

        entry_time = row['datetime']
        underlying_price = row['close_price']

        # 找OTM宽跨对 (4-8%虚值)
        call_sym, put_sym = find_otm_pair(opt_indexed, underlying_price, product, contract_yymm)
        if not call_sym or not put_sym:
            continue

        # 解析行权价算虚值度
        c_parsed = parse_option_symbol(call_sym, product)
        p_parsed = parse_option_symbol(put_sym, product)
        if not c_parsed or not p_parsed:
            continue
        c_strike = c_parsed[2]
        p_strike = p_parsed[2]
        c_otm_pct = (c_strike - underlying_price) / underlying_price * 100
        p_otm_pct = (underlying_price - p_strike) / underlying_price * 100

        # 获取入场时期权价格
        call_df = opt_indexed.get(call_sym)
        put_df = opt_indexed.get(put_sym)
        if call_df is None or put_df is None:
            continue

        # 用最近时间匹配 (容忍1分钟误差)
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

        # 各持仓期的PnL
        trade = {
            'entry_time': str(entry_time),
            'underlying': underlying_price,
            'call_sym': call_sym,
            'put_sym': put_sym,
            'c_strike': c_strike,
            'p_strike': p_strike,
            'c_otm_pct': round(c_otm_pct, 2),
            'p_otm_pct': round(p_otm_pct, 2),
            'entry_call': entry_call_price,
            'entry_put': entry_put_price,
            'entry_sum': entry_sum,
            'state_1m': state_1m,
            'state_5m': row['state_5m'],
            'combined_state': state,
        }

        for hp in hold_periods:
            exit_idx = idx + hp
            if exit_idx >= len(fdf):
                trade[f'pnl_{hp}m'] = None
                continue

            exit_time = fdf.iloc[exit_idx]['datetime']
            try:
                call_exit = call_df.loc[call_df.index.asof(exit_time)]
                put_exit = put_df.loc[put_df.index.asof(exit_time)]
            except (KeyError, ValueError):
                trade[f'pnl_{hp}m'] = None
                continue

            exit_call_price = call_exit['close']
            exit_put_price = put_exit['close']
            exit_sum = exit_call_price + exit_put_price

            # 卖出PnL = 入场权利金 - 出场权利金 (正=赚)
            pnl = entry_sum - exit_sum
            pnl_pct = pnl / entry_sum * 100 if entry_sum > 0 else 0
            trade[f'pnl_{hp}m'] = round(pnl, 2)
            trade[f'pnl_pct_{hp}m'] = round(pnl_pct, 2)

        trades.append(trade)
        last_entry_idx = idx

    return trades


def process_product(product):
    """处理单品种 — 按交易月份遍历"""
    import pyarrow.parquet as pq

    exchange = EXCHANGE_MAP.get(product.lower(), '')
    prod_upper = product.upper()
    print(f'\n{"="*60}')
    print(f'Phase 2 Direct: {prod_upper} ({exchange})')
    print(f'{"="*60}')

    # 加载全部期货
    fdf_all = load_futures(product)
    if fdf_all.empty:
        print(f'  No futures data')
        return None

    fdf_all['datetime'] = pd.to_datetime(fdf_all['datetime'])
    print(f'  Futures: {len(fdf_all["symbol"].unique())} contracts, {len(fdf_all)} bars')

    # 找期权月目录
    opt_base = os.path.join(OPTIONS_DIR, exchange, prod_upper)
    if not os.path.isdir(opt_base):
        opt_base = os.path.join(OPTIONS_DIR, exchange, product.lower())
    if not os.path.isdir(opt_base):
        print(f'  No options directory')
        return None

    month_dirs = sorted([d for d in os.listdir(opt_base) if re.match(r'\d{4}-\d{2}', d)])
    print(f'  Option months: {len(month_dirs)} ({month_dirs[0]}~{month_dirs[-1]})')

    # 按月遍历
    all_trades = []
    for month_dir in month_dirs:
        month_path = os.path.join(opt_base, month_dir)
        # 解析该月的日期范围
        year, mon = month_dir.split('-')
        month_start = pd.Timestamp(f'{year}-{mon}-01')
        if int(mon) == 12:
            month_end = pd.Timestamp(f'{int(year)+1}-01-01')
        else:
            month_end = pd.Timestamp(f'{year}-{int(mon)+1:02d}-01')

        # 筛选该月的期货bar
        mask = (fdf_all['datetime'] >= month_start) & (fdf_all['datetime'] < month_end)
        fdf_month = fdf_all[mask]
        if fdf_month.empty:
            continue

        # 找该月最活跃的期货合约(成交量最大)
        vol_by_contract = fdf_month.groupby('symbol')['volume'].sum()
        main_contract = vol_by_contract.idxmax()
        cdf = fdf_month[fdf_month['symbol'] == main_contract].copy()
        if len(cdf) < 300:
            continue

        contract_yymm = main_contract[len(prod_upper):]

        # 加载该月目录下匹配该合约的期权
        opt_files = [f for f in os.listdir(month_path) if f.endswith('.parquet')]
        opt_yymm = _contract_yymm_for_exchange(contract_yymm, exchange)
        options = {}
        for fname in opt_files:
            sym = fname.replace('.parquet', '')
            clean = sym.split('.')[-1]
            # 匹配该合约月份的C/P — 适配所有交易所格式
            if exchange in ('DCE', 'GFEX'):
                # dash format: p2402-C-5600
                pat = rf'^(?:{prod_upper}|{product.lower()}){opt_yymm}-[CP]-\d+$'
            else:
                # SHFE/INE: ag2402C5000, CZCE: MA402C2125
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

        trades = backtest_single_contract(
            cdf, options, product, contract_yymm,
            LOOKBACK, THRESHOLD, HOLD_PERIODS, ENTRY_INTERVAL
        )
        all_trades.extend(trades)

        del cdf, options, fdf_month
        gc.collect()

    if not all_trades:
        print(f'  No trades generated')
        return None

    # 统计
    df_trades = pd.DataFrame(all_trades)
    print(f'\n  Total trades: {len(df_trades)}')

    result = {'product': prod_upper, 'exchange': exchange, 'total_trades': len(df_trades)}
    result['params'] = {
        'ma_1m': MA_PERIOD_1M, 'ma_5m': MA_PERIOD_5M,
        'lookback': LOOKBACK, 'threshold': THRESHOLD,
        'hold_periods': HOLD_PERIODS, 'entry_interval': ENTRY_INTERVAL,
    }

    # 按状态分组统计
    for hp in HOLD_PERIODS:
        pnl_col = f'pnl_pct_{hp}m'
        if pnl_col not in df_trades.columns:
            continue

        valid = df_trades[df_trades[pnl_col].notna()]
        hp_stats = {}

        for state in ['both_entangled', 'mixed', 'both_trending']:
            sub = valid[valid['combined_state'] == state]
            if len(sub) < 5:
                continue
            pnls = sub[pnl_col].values
            hp_stats[state] = {
                'n': int(len(sub)),
                'mean_pnl_pct': round(np.mean(pnls), 3),
                'median_pnl_pct': round(np.median(pnls), 3),
                'win_rate': round((pnls > 0).mean() * 100, 1),
                'p25': round(np.percentile(pnls, 25), 3),
                'p75': round(np.percentile(pnls, 75), 3),
                'worst': round(np.min(pnls), 3),
                'best': round(np.max(pnls), 3),
            }

        result[f'hold_{hp}m'] = hp_stats

        # 打印
        print(f'\n  --- Hold {hp}m ---')
        print(f'  {"State":>20s} {"N":>6s} {"MeanPnL%":>9s} {"WR%":>6s} {"P25":>7s} {"P75":>7s} {"Worst":>7s}')
        for state in ['both_entangled', 'mixed', 'both_trending']:
            s = hp_stats.get(state)
            if not s:
                continue
            print(f'  {state:>20s} {s["n"]:>6d} {s["mean_pnl_pct"]:>9.3f} {s["win_rate"]:>6.1f} '
                  f'{s["p25"]:>7.3f} {s["p75"]:>7.3f} {s["worst"]:>7.3f}')

        # 信念验证
        ent = hp_stats.get('both_entangled', {})
        trn = hp_stats.get('both_trending', {})
        if ent and trn:
            diff = ent.get('mean_pnl_pct', 0) - trn.get('mean_pnl_pct', 0)
            result[f'hold_{hp}m_belief'] = {
                'entangled_better_by': float(round(diff, 3)),
                'holds': bool(diff > 0),
            }
            if diff > 0:
                print(f'  -> 纠缠时卖跨比趋势时多赚 {diff:.3f}% (信念成立)')
            else:
                print(f'  -> 趋势时卖跨反而更赚 {-diff:.3f}% (信念不成立)')

    result['trades_sample'] = all_trades[:20]  # 保存前20条样例
    del df_trades
    gc.collect()
    return result


def _run_one(product):
    """模块级函数供多进程调用"""
    try:
        return (product.upper(), process_product(product))
    except Exception as e:
        import traceback
        traceback.print_exc()
        return (product.upper(), None)


# ============ HTML报告 ============

def generate_report(all_results, output_path):
    """生成直接验证HTML报告"""
    from datetime import datetime

    products = sorted(all_results.keys())
    hp = HOLD_PERIODS[0]  # 主要看第一个持仓期

    rows = []
    for prod in products:
        r = all_results[prod]
        hp_data = r.get(f'hold_{hp}m', {})
        ent = hp_data.get('both_entangled', {})
        trn = hp_data.get('both_trending', {})
        mix = hp_data.get('mixed', {})
        belief = r.get(f'hold_{hp}m_belief', {})

        rows.append({
            'prod': prod,
            'n_trades': r['total_trades'],
            'ent_n': ent.get('n', 0),
            'ent_pnl': ent.get('mean_pnl_pct', 0),
            'ent_wr': ent.get('win_rate', 0),
            'trn_n': trn.get('n', 0),
            'trn_pnl': trn.get('mean_pnl_pct', 0),
            'trn_wr': trn.get('win_rate', 0),
            'mix_n': mix.get('n', 0),
            'mix_pnl': mix.get('mean_pnl_pct', 0),
            'diff': belief.get('entangled_better_by', 0),
            'holds': belief.get('holds', False),
        })

    rows.sort(key=lambda x: -x['diff'])

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>B043 Phase 2: MA纠缠度直接验证 (期权PnL)</title>
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
  <h1>B043 Phase 2: 直接验证 — 期权卖出PnL</h1>
  <p>卖出OTM宽跨(4-8%虚值), 持仓{hp}分钟, 按MA纠缠状态分组对比PnL</p>
  <p>参数: 1分钟{MA_PERIOD_1M}MA + 5分钟{MA_PERIOD_5M}MA, LB{LOOKBACK}/T{THRESHOLD}</p>
  <p>生成: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
</div>

<div class="card-row">
  <div class="card"><div class="val" style="color:#4fc3f7">{len(products)}</div><div class="label">品种数</div></div>
  <div class="card"><div class="val" style="color:#00e676">{sum(1 for r in rows if r['holds'])}</div><div class="label">信念成立</div></div>
  <div class="card"><div class="val" style="color:#FF4444">{sum(1 for r in rows if not r['holds'])}</div><div class="label">信念不成立</div></div>
  <div class="card"><div class="val" style="color:#FFD700">{np.mean([r['diff'] for r in rows if r['ent_n']>0 and r['trn_n']>0]):.2f}%</div><div class="label">平均优势</div></div>
</div>

<div style="text-align:center">
  <div class="chart-box"><div id="bar-chart" style="width:900px;height:450px"></div></div>
</div>

<h2>全品种对比 (持仓{hp}分钟)</h2>
<table>
<thead><tr>
  <th>#</th><th>品种</th>
  <th colspan="3" style="color:#00e676">纠缠时卖</th>
  <th colspan="3" style="color:#FF4444">趋势时卖</th>
  <th>优势</th><th>结论</th>
</tr>
<tr>
  <th></th><th></th>
  <th>N</th><th>PnL%</th><th>WR%</th>
  <th>N</th><th>PnL%</th><th>WR%</th>
  <th>差值</th><th></th>
</tr>
</thead>
<tbody>"""

    for i, r in enumerate(rows):
        cls = 'color:#00e676' if r['holds'] else 'color:#FF4444'
        verdict = 'Y' if r['holds'] else 'N'
        html += f"""<tr>
  <td>{i+1}</td>
  <td style="font-weight:bold;color:#FFD700">{r['prod']}</td>
  <td>{r['ent_n']}</td>
  <td style="color:{'#00e676' if r['ent_pnl']>0 else '#FF4444'}">{r['ent_pnl']:.3f}</td>
  <td>{r['ent_wr']:.1f}</td>
  <td>{r['trn_n']}</td>
  <td style="color:{'#00e676' if r['trn_pnl']>0 else '#FF4444'}">{r['trn_pnl']:.3f}</td>
  <td>{r['trn_wr']:.1f}</td>
  <td style="font-weight:bold;{cls}">{r['diff']:+.3f}%</td>
  <td style="font-weight:bold;{cls}">{verdict}</td>
</tr>"""

    # Chart data
    chart_prods = [r['prod'] for r in rows if r['ent_n'] > 0 and r['trn_n'] > 0][:25]
    chart_ent = [r['ent_pnl'] for r in rows if r['prod'] in chart_prods]
    chart_trn = [r['trn_pnl'] for r in rows if r['prod'] in chart_prods]

    html += """</tbody></table>"""
    html += """
<script>
var chart = echarts.init(document.getElementById('bar-chart'));
chart.setOption(%s);
</script>""" % json.dumps({
        'title': {'text': '纠缠 vs 趋势 卖跨PnL%%对比', 'textStyle': {'color': '#ddd', 'fontSize': 14}},
        'tooltip': {'trigger': 'axis'},
        'legend': {'data': ['纠缠时卖', '趋势时卖'], 'textStyle': {'color': '#888'}},
        'xAxis': {'type': 'category', 'data': chart_prods, 'axisLabel': {'color': '#888', 'rotate': 30}},
        'yAxis': {'type': 'value', 'name': 'PnL%', 'axisLabel': {'color': '#888'},
                  'splitLine': {'lineStyle': {'color': '#2a2a4a'}}},
        'series': [
            {'name': '纠缠时卖', 'type': 'bar', 'data': chart_ent, 'itemStyle': {'color': '#00e676'}},
            {'name': '趋势时卖', 'type': 'bar', 'data': chart_trn, 'itemStyle': {'color': '#FF4444'}},
        ]
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
    parser = argparse.ArgumentParser(description='B043 Phase 2: MA纠缠度直接验证')
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

    print(f'B043 Phase 2 Direct Validation')
    print(f'Products: {", ".join(p.upper() for p in products)}')
    print(f'Params: 1m-MA{MA_PERIOD_1M} + 5m-MA{MA_PERIOD_5M}, LB{LOOKBACK}/T{THRESHOLD}')
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
                    with open(os.path.join(output_dir, f'{prod}_direct.json'), 'w') as f:
                        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    else:
        for product in products:
            prod, result = _run_one(product)
            if result:
                all_results[prod] = result
                with open(os.path.join(output_dir, f'{prod}_direct.json'), 'w') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2, default=str)
            gc.collect()

    if not all_results:
        print('\nNo results')
        return

    # 汇总
    summary_path = os.path.join(output_dir, '_DIRECT_SUMMARY.json')
    with open(summary_path, 'w') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)

    # HTML
    report_path = os.path.join(output_dir, 'direct_report.html')
    generate_report(all_results, report_path)

    # 打印汇总
    hp = HOLD_PERIODS[0]
    print(f'\n{"="*60}')
    print(f'Phase 2 汇总 (持仓{hp}分钟):')
    holds = 0
    for prod in sorted(all_results.keys()):
        b = all_results[prod].get(f'hold_{hp}m_belief', {})
        diff = b.get('entangled_better_by', 0)
        ok = b.get('holds', False)
        if ok:
            holds += 1
        print(f'  {prod:>5s}: diff={diff:+.3f}% {"Y" if ok else "N"}')
    print(f'\n信念成立: {holds}/{len(all_results)}')


if __name__ == '__main__':
    main()
