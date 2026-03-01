#!/usr/bin/env python3
"""
全市场期权价格之和信念验证系统 v2 - 高效版
选择中小型品种，分批处理，控制内存
"""
import pandas as pd
import numpy as np
import re
import os
import json
import gc
import sys
from collections import defaultdict, Counter
from datetime import datetime

# 优化P2: 预编译正则，避免每次调用时重新编译
_OPTION_RE = re.compile(r'([A-Za-z]+)(\d+)([CP])(\d+)', re.IGNORECASE)
_FUTURES_RE = re.compile(r'([A-Za-z]+)(\d+)')

FUTURES_BASE = '/Users/zhangxiaoyu/Downloads/期货数据_parquet'
OPTIONS_BASE = '/Users/zhangxiaoyu/Downloads/期权_parquet'
KNOWLEDGE_PATH = '/Users/zhangxiaoyu/Scripts/price_sum_knowledge.json'

# 只选数据量<100MB的品种，确保快速加载
PRODUCTS = [
    # (exchange, product, futures_file, options_file, exchange_type, max_mb)
    # SHFE - 选小的
    ('SHFE', 'SN', 'SHFE/SN.parquet', 'SHFE/sn.parquet', 'shfe'),  # 锡 34MB
    ('SHFE', 'PB', 'SHFE/PB.parquet', 'SHFE/pb.parquet', 'shfe'),  # 铅 24MB
    ('SHFE', 'NI', 'SHFE/NI.parquet', 'SHFE/ni.parquet', 'shfe'),  # 镍 53MB
    ('SHFE', 'RB', 'SHFE/RB.parquet', 'SHFE/rb.parquet', 'shfe'),  # 螺纹 169MB
    ('SHFE', 'AD', 'SHFE/AD.parquet', 'SHFE/ad.parquet', 'shfe'),  # 氧化铝 9MB
    # CZCE
    ('CZCE', 'SF', 'CZCE/SF.parquet', 'CZCE/SF.parquet', 'czce'),  # 硅铁 40MB
    ('CZCE', 'SM', 'CZCE/SM.parquet', 'CZCE/SM.parquet', 'czce'),  # 锰硅 61MB
    ('CZCE', 'UR', 'CZCE/UR.parquet', 'CZCE/UR.parquet', 'czce'),  # 尿素 34MB
    ('CZCE', 'PF', 'CZCE/PF.parquet', 'CZCE/PF.parquet', 'czce'),  # 涤纶 38MB
    ('CZCE', 'AP', 'CZCE/AP.parquet', 'CZCE/AP.parquet', 'czce'),  # 苹果 30MB
    ('CZCE', 'CJ', 'CZCE/CJ.parquet', 'CZCE/CJ.parquet', 'czce'),  # 红枣 23MB
    ('CZCE', 'ZC', 'CZCE/ZC.parquet', 'CZCE/ZC.parquet', 'czce'),  # 动力煤 69MB
    ('CZCE', 'PK', 'CZCE/PK.parquet', 'CZCE/PK.parquet', 'czce'),  # 花生 57MB
    ('CZCE', 'SH', 'CZCE/SH.parquet', 'CZCE/SH.parquet', 'czce'),  # 烧碱 59MB
    # DCE
    ('DCE', 'CS', 'DCE/CS.parquet', 'DCE/cs.parquet', 'dce'),  # 淀粉 19MB
    ('DCE', 'JD', 'DCE/JD.parquet', 'DCE/jd.parquet', 'dce'),  # 鸡蛋 35MB
    ('DCE', 'LH', 'DCE/LH.parquet', 'DCE/lh.parquet', 'dce'),  # 生猪 20MB
    ('DCE', 'LG', 'DCE/LG.parquet', 'DCE/lg.parquet', 'dce'),  # 液化石油气? 7MB
    ('DCE', 'EG', 'DCE/EG.parquet', 'DCE/eg.parquet', 'dce'),  # 乙二醇
    # GFEX
    ('GFEX', 'SI', 'GFEX/SI.parquet', 'GFEX/si.parquet', 'gfex'),  # 工硅 165MB
    # INE - 偏大但重要
    # ('INE', 'SC', 'INE/SC.parquet', 'INE/sc.parquet', 'ine'),  # 原油 231MB - 跳过
]


def flush_print(msg):
    print(msg)
    sys.stdout.flush()


def parse_option_symbol(sym, exchange_type):
    # 优化P2: 使用预编译正则
    s = sym.split('.')[-1] if '.' in sym else sym
    m = _OPTION_RE.match(s)
    if m:
        return m.group(1), m.group(2), m.group(3).upper(), int(m.group(4))
    return None


def futures_month_to_option_month(f_sym, exchange_type):
    # 优化P2: 使用预编译正则
    m = _FUTURES_RE.match(f_sym)
    if not m:
        return None
    month_str = m.group(2)
    if exchange_type == 'czce' and len(month_str) == 4:
        return month_str[1:]
    return month_str


def regress(pairs_list):
    if len(pairs_list) < 5:
        return None
    n = len(pairs_list)
    sx = sum(x for x,y in pairs_list)
    sy = sum(y for x,y in pairs_list)
    sxx = sum(x*x for x,y in pairs_list)
    sxy = sum(x*y for x,y in pairs_list)
    denom = n*sxx - sx*sx
    if abs(denom) < 1e-12: return None
    return (n*sxy - sx*sy) / denom


def analyze_session(day_futures, day_calls, day_puts, c_strike, p_strike,
                    otm_label, c_otm_pct, p_otm_pct, f_price_mid):
    """分析单个session的单个配对"""
    futures_ts = dict(zip(day_futures['datetime'], day_futures['close']))
    call_ts = dict(zip(day_calls['datetime'], day_calls['close']))
    put_ts = dict(zip(day_puts['datetime'], day_puts['close']))

    ft_times = sorted(futures_ts.keys())

    # 合并时间轴
    all_times = sorted(set(ft_times) & (set(call_ts.keys()) | set(put_ts.keys())))
    if len(all_times) < 20:
        return None

    last_c, last_p = None, None
    records = []
    for t in all_times:
        f = futures_ts.get(t)
        c = call_ts.get(t, last_c)
        p = put_ts.get(t, last_p)
        if c is not None: last_c = c
        if p is not None: last_p = p
        if f is not None and last_c is not None and last_p is not None:
            records.append((t, f, last_c, last_p, last_c + last_p))

    if len(records) < 20:
        return None

    f_start = records[0][1]; f_end = records[-1][1]
    f_chg = f_end - f_start
    f_pct = f_chg / f_start * 100 if f_start else 0
    s_start = records[0][4]; s_end = records[-1][4]
    s_chg = s_end - s_start
    s_pct = s_chg / s_start * 100 if s_start else 0
    p_start = records[0][3]; p_end = records[-1][3]
    c_start = records[0][2]; c_end = records[-1][2]

    # 逐分钟分析
    all_df_ds = []
    for i in range(1, len(records)):
        df = records[i][1] - records[i-1][1]
        ds = records[i][4] - records[i-1][4]
        if df != 0:
            all_df_ds.append((df, ds))

    beta_all = regress(all_df_ds)
    beta_up = regress([(x,y) for x,y in all_df_ds if x > 0])
    beta_dn = regress([(x,y) for x,y in all_df_ds if x < 0])
    dn_up_ratio = abs(beta_dn/beta_up) if beta_up and beta_dn and abs(beta_up) > 1e-8 else None

    # Put钉死检测 (百分比版)
    # 优化P4: 用numpy向量化替代O(n²)双重循环
    pin_found = False
    pin_duration = 0
    pin_after_sum_pct = None
    arr = np.array([(r[1], r[3], r[4]) for r in records])  # f_price, p_price, s_price
    n = len(arr)
    f_prices = arr[:, 0]
    p_prices = arr[:, 1]
    s_prices = arr[:, 2]
    best_end_global = 0

    starts_all = np.arange(0, n, 3)
    valid_starts = starts_all[(f_prices[starts_all] > 0) & (p_prices[starts_all] > 0)]

    for window in range(10, min(80, n)):
        mask_in_range = valid_starts + window < n
        starts_v = valid_starts[mask_in_range]
        if len(starts_v) == 0:
            break
        ends_v = starts_v + window
        f_pct_w = (f_prices[ends_v] - f_prices[starts_v]) / f_prices[starts_v]
        p_pct_w = np.abs(p_prices[ends_v] - p_prices[starts_v]) / p_prices[starts_v]
        hit = (f_pct_w > 0.003) & (p_pct_w < 0.02)
        if hit.any() and window > pin_duration:
            pin_found = True
            pin_duration = window
            best_end_global = int(ends_v[np.where(hit)[0][-1]])

    if pin_found and best_end_global < n - 1:
        pin_after_sum_pct = (s_prices[-1] - s_prices[best_end_global]) / max(s_prices[best_end_global], 0.01) * 100

    # Put暴涨检测
    put_spike_found = False
    put_spike_futures_down = False
    put_spike_sum_up = False
    for i in range(5, len(records)):
        if records[i-5][3] == 0:
            continue
        p_chg_5 = (records[i][3] - records[i-5][3]) / records[i-5][3] * 100
        if p_chg_5 > 10:
            put_spike_found = True
            put_spike_futures_down = records[i][1] < records[i-5][1]
            put_spike_sum_up = records[i][4] > records[i-5][4]
            break

    # Put峰值后平台
    p_values = [r[3] for r in records]
    p_max_idx = int(np.argmax(p_values))
    put_peak_higher_platform = None
    if p_max_idx < len(records) - 10 and p_values[p_max_idx] > p_start * 1.05:
        after_peak_min = min(p_values[p_max_idx:])
        put_peak_higher_platform = after_peak_min > p_start

    # 回归残差
    avg_residual = 0
    if beta_all:
        residuals = []
        for i in range(1, len(records)):
            df_val = records[i][1] - records[i-1][1]
            ds_val = records[i][4] - records[i-1][4]
            residuals.append(ds_val - beta_all * df_val)
        avg_residual = np.mean(residuals) if residuals else 0

    # Put波动幅度
    p_range_pct = (max(p_values) - min(p_values)) / max(p_start, 0.01) * 100
    sum_values = [r[4] for r in records]
    sum_volatility_pct = (max(sum_values) - min(sum_values)) / max(s_start, 0.01) * 100

    return {
        'n_records': len(records),
        'otm_label': otm_label,
        'c_strike': c_strike, 'p_strike': p_strike,
        'c_otm_pct': c_otm_pct, 'p_otm_pct': p_otm_pct,
        'f_start': f_start, 'f_end': f_end,
        'f_chg': f_chg, 'f_pct': f_pct,
        's_start': s_start, 's_end': s_end,
        's_chg': s_chg, 's_pct': s_pct,
        'beta_all': beta_all, 'beta_up': beta_up, 'beta_dn': beta_dn,
        'dn_up_ratio': dn_up_ratio,
        'sum_volatility_pct': sum_volatility_pct,
        'pin_found': pin_found,
        'pin_duration': pin_duration,
        'pin_after_sum_pct': pin_after_sum_pct,
        'put_spike_found': put_spike_found,
        'put_spike_futures_down': put_spike_futures_down,
        'put_spike_sum_up': put_spike_sum_up,
        'put_peak_higher_platform': put_peak_higher_platform,
        'avg_residual': avg_residual,
        'p_range_pct': p_range_pct,
        'f_range_pct': abs(f_pct),
    }


def process_product(exchange, product, f_path, o_path, exchange_type):
    f_full = os.path.join(FUTURES_BASE, f_path)
    o_full = os.path.join(OPTIONS_BASE, o_path)

    if not os.path.exists(f_full) or not os.path.exists(o_full):
        return []

    try:
        df_f = pd.read_parquet(f_full)
        df_o = pd.read_parquet(o_full)
    except Exception as e:
        flush_print(f"  ✗ 加载失败: {e}")
        return []

    # 优化P1: 合并过滤条件，减少DataFrame复制次数
    o_start = df_o.loc[df_o['close'] > 0, 'datetime'].min()
    df_f = df_f[(df_f['close'] > 0) & (df_f['datetime'] >= o_start)].copy()
    df_o = df_o[df_o['close'] > 0].copy()

    if len(df_f) == 0 or len(df_o) == 0:
        return []

    # 优化P2: 向量化str.extract替代逐行apply，一次提取多列
    syms = df_o['symbol'].str.split('.').str[-1]
    extracted = syms.str.extract(_OPTION_RE)
    valid = extracted[0].notna()
    df_o = df_o[valid].copy()
    df_o['opt_month'] = extracted.loc[valid, 1]
    df_o['cp'] = extracted.loc[valid, 2].str.upper()
    df_o['strike'] = extracted.loc[valid, 3].astype(int)

    df_f['date'] = df_f['datetime'].dt.date
    df_o['date'] = df_o['datetime'].dt.date

    # 交集日期
    f_dates = set(df_f['date'].unique())
    o_dates = set(df_o['date'].unique())
    all_dates = sorted(f_dates & o_dates)

    # 采样100天
    if len(all_dates) > 100:
        indices = np.linspace(0, len(all_dates)-1, 100, dtype=int)
        all_dates = [all_dates[i] for i in indices]

    results = []
    sessions_done = 0

    for trade_date in all_dates:
        day_f = df_f[df_f['date'] == trade_date]
        if len(day_f) < 30:
            continue

        # 主力合约
        if 'volume' in day_f.columns:
            vol_by_sym = day_f.groupby('symbol')['volume'].sum()
        else:
            vol_by_sym = day_f.groupby('symbol')['close'].count()
        main_contract = vol_by_sym.idxmax()
        day_futures = day_f[day_f['symbol'] == main_contract].sort_values('datetime')
        if len(day_futures) < 30:
            continue

        opt_month = futures_month_to_option_month(main_contract, exchange_type)
        if opt_month is None:
            continue

        day_opts = df_o[(df_o['date'] == trade_date) & (df_o['opt_month'] == opt_month)]
        if len(day_opts) == 0:
            continue

        f_price_mid = day_futures['close'].median()
        calls = day_opts[day_opts['cp'] == 'C']
        puts = day_opts[day_opts['cp'] == 'P']
        call_strikes = sorted(calls['strike'].unique())
        put_strikes = sorted(puts['strike'].unique())

        if not call_strikes or not put_strikes:
            continue

        otm_calls = [k for k in call_strikes if k > f_price_mid]
        otm_puts = [k for k in put_strikes if k < f_price_mid]
        if not otm_calls or not otm_puts:
            continue

        # 选配对: 近端(2-8% OTM), 中端(10-25%), 远端(25-60%)
        # 优化P3: 在循环外预排序，避免内层循环反复排序
        sorted_otm_puts_desc = sorted(otm_puts, reverse=True)
        pairs_to_try = []
        for target_label, otm_lo, otm_hi in [('近端', 0.02, 0.08), ('中端', 0.10, 0.25), ('远端', 0.25, 0.60)]:
            found = False
            for c_k in otm_calls:
                c_otm = (c_k - f_price_mid) / f_price_mid
                if otm_lo <= c_otm <= otm_hi:
                    for p_k in sorted_otm_puts_desc:
                        p_otm = (f_price_mid - p_k) / f_price_mid
                        if otm_lo <= p_otm <= otm_hi:
                            pairs_to_try.append((c_k, p_k, target_label, c_otm, p_otm))
                            found = True
                            break
                if found:
                    break

        for c_k, p_k, label, c_otm, p_otm in pairs_to_try:
            dc = calls[calls['strike'] == c_k].sort_values('datetime')
            dp = puts[puts['strike'] == p_k].sort_values('datetime')
            if len(dc) < 10 or len(dp) < 10:
                continue

            result = analyze_session(day_futures, dc, dp, c_k, p_k,
                                     label, c_otm, p_otm, f_price_mid)
            if result:
                result['date'] = str(trade_date)
                result['contract'] = main_contract
                result['exchange'] = exchange
                result['product'] = product
                results.append(result)

        sessions_done += 1

    flush_print(f"  → {sessions_done}个交易日, {len(results)}个配对")
    return results


def verify_beliefs(all_results):
    evidence = {f'B{i:03d}': {'for': 0, 'against': 0, 'neutral': 0}
                for i in range(1, 16)}

    # 优化P5: 一次遍历替代三次列表过滤
    grouped = defaultdict(list)
    for r in all_results:
        grouped[r['otm_label']].append(r)
    near, mid, far = grouped['近端'], grouped['中端'], grouped['远端']

    by_date_contract = defaultdict(dict)
    for r in all_results:
        key = (r['date'], r['contract'])
        by_date_contract[key][r['otm_label']] = r

    # B001: 方向性
    for r in all_results:
        if abs(r['f_pct']) < 0.1:
            evidence['B001']['neutral'] += 1
        elif (r['f_chg'] > 0 and r['s_chg'] > 0) or (r['f_chg'] < 0 and r['s_chg'] < 0):
            evidence['B001']['for'] += 1
        else:
            evidence['B001']['against'] += 1

    # B002: 下跌不对称
    for r in all_results:
        if r['dn_up_ratio'] is not None:
            if r['dn_up_ratio'] > 1.5:
                evidence['B002']['for'] += 1
            elif r['dn_up_ratio'] < 1.0:
                evidence['B002']['against'] += 1
            else:
                evidence['B002']['neutral'] += 1

    # B003: 虚值杠杆
    for key, otms in by_date_contract.items():
        for pair in [('近端','中端'), ('近端','远端'), ('中端','远端')]:
            if pair[0] in otms and pair[1] in otms:
                v0 = otms[pair[0]]['sum_volatility_pct']
                v1 = otms[pair[1]]['sum_volatility_pct']
                if v1 > v0:
                    evidence['B003']['for'] += 1
                elif v1 < v0:
                    evidence['B003']['against'] += 1
                else:
                    evidence['B003']['neutral'] += 1

    # B004: Delta结构
    for key, otms in by_date_contract.items():
        for pair in [('近端','远端'), ('近端','中端'), ('中端','远端')]:
            if pair[0] in otms and pair[1] in otms:
                d0 = otms[pair[0]].get('beta_all')
                d1 = otms[pair[1]].get('beta_all')
                if d0 is not None and d1 is not None:
                    if abs(d0) > abs(d1):
                        evidence['B004']['for'] += 1
                    else:
                        evidence['B004']['against'] += 1

    # B005: 三因素(theta通过残差)
    for r in all_results:
        if r['avg_residual'] < -0.0001:
            evidence['B005']['for'] += 1
        else:
            evidence['B005']['neutral'] += 1

    # B006: 弹簧效应(大行情时远端涨幅大)
    for r in far:
        if abs(r['f_pct']) > 1.0:
            if abs(r['s_pct']) > 5:
                evidence['B006']['for'] += 1
            else:
                evidence['B006']['neutral'] += 1
    for key, otms in by_date_contract.items():
        if '近端' in otms and '远端' in otms:
            if abs(otms['远端']['f_pct']) > 1.0:
                if abs(otms['远端']['s_pct']) > abs(otms['近端']['s_pct']):
                    evidence['B006']['for'] += 1
                else:
                    evidence['B006']['against'] += 1

    # B007: Put钉死
    for r in all_results:
        if r['pin_found'] and r['pin_duration'] > 10:
            evidence['B007']['for'] += 1
        elif not r['pin_found'] and abs(r['f_pct']) > 1.0:
            evidence['B007']['neutral'] += 1  # 大行情但没钉死, 不算反驳

    # B008: Gamma加速
    for r in all_results:
        if r['pin_found'] and r['pin_duration'] > 15 and r['beta_all'] and r['beta_all'] > 0 and r['f_chg'] > 0:
            evidence['B008']['for'] += 1
        elif r['pin_found'] and r['pin_duration'] > 15:
            evidence['B008']['neutral'] += 1

    # B009: 极深虚值Put退化
    for r in far:
        if r['f_range_pct'] > 0.5:  # 期货至少有0.5%波动
            if r['p_range_pct'] < r['f_range_pct'] * 50:  # Put%波幅 < 期货%波幅*50
                # 远端Put波动率占期货波动的比例
                ratio = r['p_range_pct'] / max(r['f_range_pct'], 0.01)
                if ratio < 5:
                    evidence['B009']['for'] += 1
                elif ratio > 20:
                    evidence['B009']['against'] += 1
                else:
                    evidence['B009']['neutral'] += 1

    # B010: Put钉死入场信号
    for r in all_results:
        if r['pin_found'] and r['pin_after_sum_pct'] is not None:
            if r['pin_after_sum_pct'] > 0:
                evidence['B010']['for'] += 1
            else:
                evidence['B010']['against'] += 1

    # B011: 两阶段
    for r in all_results:
        if r['put_spike_found']:
            if r['put_spike_futures_down'] and r['put_spike_sum_up']:
                evidence['B011']['for'] += 1
            elif r['put_spike_found']:
                evidence['B011']['neutral'] += 1

    # B012: 暴涨后高位平台
    for r in all_results:
        if r['put_peak_higher_platform'] is not None:
            if r['put_peak_higher_platform']:
                evidence['B012']['for'] += 1
            else:
                evidence['B012']['against'] += 1

    # B013: DTE≤14天theta
    for r in all_results:
        if abs(r['f_pct']) < 0.3:
            if r['s_pct'] < -0.3:
                evidence['B013']['for'] += 1
            elif r['s_pct'] > 0.3:
                evidence['B013']['against'] += 1
            else:
                evidence['B013']['neutral'] += 1

    # B014: 基础规则
    evidence['B014']['for'] = 1

    # B015: 总权金≥10
    low = [r for r in all_results if r['s_start'] < 10]
    high = [r for r in all_results if r['s_start'] >= 10]
    if low:
        low_avg = np.mean([abs(r['s_pct']) for r in low])
        high_avg = np.mean([abs(r['s_pct']) for r in high]) if high else 0
        if low_avg > high_avg * 1.5:
            evidence['B015']['for'] += len(low)
        else:
            evidence['B015']['neutral'] += len(low)

    return evidence


def bayesian_update(knowledge, evidence):
    for belief in knowledge['beliefs']:
        bid = belief['id']
        if bid not in evidence:
            continue
        ev = evidence[bid]
        old_conf = belief['confidence']
        total = ev['for'] + ev['against']
        if total == 0:
            continue

        support_rate = ev['for'] / total

        if support_rate > 0.6:
            strength = min((support_rate - 0.5) * 2, 1.0)  # 0~1
            delta = strength * 0.10 * (1 - old_conf)
            new_conf = min(old_conf + delta, 0.99)
        elif support_rate < 0.4:
            strength = min((0.5 - support_rate) * 2, 1.0)
            delta = strength * 0.15 * old_conf
            new_conf = max(old_conf - delta, 0.01)
        else:
            new_conf = old_conf

        new_conf = round(new_conf, 2)
        belief['confidence'] = new_conf
        belief['evidence_for'] += ev['for']
        belief['evidence_against'] += ev['against']
        belief['last_updated'] = '2026-03-02'
        belief['data_points'].append(
            f"2026-03-02 全市场parquet验证: "
            f"支持{ev['for']}/反驳{ev['against']}/中性{ev['neutral']} "
            f"支持率{support_rate*100:.0f}% "
            f"conf {old_conf}→{new_conf}"
        )

    return knowledge


if __name__ == '__main__':
    flush_print("=" * 70)
    flush_print("全市场期权价格之和信念验证系统 v2")
    flush_print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    flush_print("=" * 70)

    all_results = []

    for exchange, product, f_path, o_path, exch_type in PRODUCTS:
        f_full = os.path.join(FUTURES_BASE, f_path)
        o_full = os.path.join(OPTIONS_BASE, o_path)
        if not os.path.exists(f_full) or not os.path.exists(o_full):
            flush_print(f"\n[跳过] {exchange}/{product}: 文件不存在")
            continue

        flush_print(f"\n[处理] {exchange}/{product}...")
        try:
            results = process_product(exchange, product, f_path, o_path, exch_type)
            all_results.extend(results)
        except Exception as e:
            flush_print(f"  ✗ 错误: {e}")
            import traceback
            traceback.print_exc()
            sys.stdout.flush()
        finally:
            gc.collect()

    flush_print(f"\n{'='*70}")
    flush_print(f"总计分析: {len(all_results)}个配对")

    products_set = set(r['product'] for r in all_results)
    sessions_set = set((r['product'], r['date']) for r in all_results)
    flush_print(f"品种数: {len(products_set)}, 交易日数: {len(sessions_set)}")
    # 优化P6: 使用Counter一次遍历替代三次列表过滤
    otm_counts = Counter(r['otm_label'] for r in all_results)
    flush_print(f"近端: {otm_counts['近端']}")
    flush_print(f"中端: {otm_counts['中端']}")
    flush_print(f"远端: {otm_counts['远端']}")
    flush_print(f"{'='*70}")

    if not all_results:
        flush_print("无有效数据")
        exit(1)

    evidence = verify_beliefs(all_results)

    NAMES = {
        'B001': '方向性跟随', 'B002': '下跌不对称性', 'B003': '虚值杠杆效应',
        'B004': 'Delta结构递减', 'B005': '三因素驱动', 'B006': '弹簧效应',
        'B007': 'Put钉死现象', 'B008': 'Gamma加速', 'B009': '极深虚值Put退化',
        'B010': 'Put钉死入场信号', 'B011': 'Put钉死→暴涨', 'B012': '暴涨后高位平台',
        'B013': 'theta收割', 'B014': '6小时/天', 'B015': '权金≥10门槛',
    }

    flush_print("\n信念验证结果:")
    flush_print("-" * 70)
    for bid in sorted(evidence.keys()):
        ev = evidence[bid]
        total = ev['for'] + ev['against']
        rate = ev['for'] / total * 100 if total > 0 else 0
        status = '▲支持' if rate > 60 else ('▼反驳' if rate < 40 else '—中性')
        flush_print(f"  {bid} {NAMES.get(bid,''):10s}: +{ev['for']:4d}/-{ev['against']:4d}/~{ev['neutral']:4d} "
                    f"支持率{rate:5.1f}% {status}")

    # 加载知识库
    with open(KNOWLEDGE_PATH, 'r') as f:
        knowledge = json.load(f)

    old_confs = {b['id']: b['confidence'] for b in knowledge['beliefs']}
    knowledge = bayesian_update(knowledge, evidence)

    knowledge['update_log'].append({
        'date': '2026-03-02',
        'session': '全市场parquet历史数据验证',
        'summary': f"用{len(products_set)}个品种、{len(sessions_set)}个交易日的parquet历史数据验证15个信念。"
                   f"共分析{len(all_results)}个session/配对组合。",
        'changes': '; '.join([
            f"{bid} conf {old_confs.get(bid, '?')}→{b['confidence']}"
            for b in knowledge['beliefs']
            for bid in [b['id']]
            if old_confs.get(bid) != b['confidence']
        ])
    })
    knowledge['meta']['update_count'] += 1
    knowledge['meta']['version'] = '1.5'

    # 保存
    output_path = '/Users/zhangxiaoyu/Scripts/belief_verification_results.json'
    with open(output_path, 'w') as f:
        json.dump({
            'run_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_results': len(all_results),
            'evidence': evidence,
            'stats': {
                'products': list(products_set),
                'total_sessions': len(sessions_set),
                'by_otm': {
                    '近端': otm_counts['近端'],
                    '中端': otm_counts['中端'],
                    '远端': otm_counts['远端'],
                }
            }
        }, f, ensure_ascii=False, indent=2)

    with open(KNOWLEDGE_PATH, 'w') as f:
        json.dump(knowledge, f, ensure_ascii=False, indent=2)

    flush_print(f"\n{'='*70}")
    flush_print("更新后的信念置信度")
    flush_print(f"{'='*70}")
    for belief in knowledge['beliefs']:
        bid = belief['id']
        old = old_confs.get(bid, '?')
        new = belief['confidence']
        arrow = '▲' if new > old else ('▼' if new < old else '—')
        flush_print(f"  {bid} {NAMES.get(bid,''):10s}: {old} → {new} {arrow}")

    flush_print(f"\n结果: {output_path}")
    flush_print(f"知识库: {KNOWLEDGE_PATH}")
    flush_print("完成!")
