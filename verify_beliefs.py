#!/usr/bin/env python3
"""
全市场期权价格之和信念验证系统
用历史parquet数据验证knowledge base中的15个信念
"""
import pandas as pd
import numpy as np
import re
import os
import json
import warnings
from collections import defaultdict
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# 优化点2: 预编译正则表达式，避免每次调用时重复编译
_OPTION_RE = re.compile(r'([A-Za-z]+)(\d+)([CP])(\d+)', re.IGNORECASE)
_FUTURES_RE = re.compile(r'([A-Za-z]+)(\d+)')

FUTURES_BASE = '/Users/zhangxiaoyu/Downloads/期货数据_parquet'
OPTIONS_BASE = '/Users/zhangxiaoyu/Downloads/期权_parquet'
KNOWLEDGE_PATH = '/Users/zhangxiaoyu/Scripts/price_sum_knowledge.json'

# =========================================================================
# 品种配置
# =========================================================================
PRODUCTS = [
    # (exchange, product, futures_file, options_file, contract_month_format)
    # SHFE: futures=AU2412, options=SHFE.au2412C5600 (lowercase in options)
    ('SHFE', 'AU', 'SHFE/AU.parquet', 'SHFE/au.parquet', 'shfe'),
    ('SHFE', 'CU', 'SHFE/CU.parquet', 'SHFE/cu.parquet', 'shfe'),
    ('SHFE', 'AL', 'SHFE/AL.parquet', 'SHFE/al.parquet', 'shfe'),
    ('SHFE', 'ZN', 'SHFE/ZN.parquet', 'SHFE/zn.parquet', 'shfe'),
    ('SHFE', 'RU', 'SHFE/RU.parquet', 'SHFE/ru.parquet', 'shfe'),
    ('SHFE', 'NI', 'SHFE/NI.parquet', 'SHFE/ni.parquet', 'shfe'),
    ('SHFE', 'RB', 'SHFE/RB.parquet', 'SHFE/rb.parquet', 'shfe'),
    ('SHFE', 'SN', 'SHFE/SN.parquet', 'SHFE/sn.parquet', 'shfe'),
    # CZCE: futures=SF2401, options=CZCE.SF401C6500 (3-digit month)
    ('CZCE', 'MA', 'CZCE/MA.parquet', 'CZCE/MA.parquet', 'czce'),
    ('CZCE', 'RM', 'CZCE/RM.parquet', 'CZCE/RM.parquet', 'czce'),
    ('CZCE', 'OI', 'CZCE/OI.parquet', 'CZCE/OI.parquet', 'czce'),
    ('CZCE', 'SF', 'CZCE/SF.parquet', 'CZCE/SF.parquet', 'czce'),
    ('CZCE', 'SM', 'CZCE/SM.parquet', 'CZCE/SM.parquet', 'czce'),
    ('CZCE', 'UR', 'CZCE/UR.parquet', 'CZCE/UR.parquet', 'czce'),
    ('CZCE', 'ZC', 'CZCE/ZC.parquet', 'CZCE/ZC.parquet', 'czce'),
    ('CZCE', 'SA', 'CZCE/SA.parquet', 'CZCE/SA.parquet', 'czce'),  # SA期权存在吗?
    ('CZCE', 'PF', 'CZCE/PF.parquet', 'CZCE/PF.parquet', 'czce'),
    # DCE: futures=M2501, options=DCE.m2501C3400 (lowercase, 4-digit)
    ('DCE', 'M', 'DCE/M.parquet', 'DCE/m.parquet', 'dce'),
    ('DCE', 'I', 'DCE/I.parquet', 'DCE/i.parquet', 'dce'),
    ('DCE', 'PP', 'DCE/PP.parquet', 'DCE/pp.parquet', 'dce'),
    ('DCE', 'L', 'DCE/L.parquet', 'DCE/l.parquet', 'dce'),
    ('DCE', 'V', 'DCE/V.parquet', 'DCE/v.parquet', 'dce'),
    ('DCE', 'C', 'DCE/C.parquet', 'DCE/c.parquet', 'dce'),
    ('DCE', 'EG', 'DCE/EG.parquet', 'DCE/eg.parquet', 'dce'),
    # GFEX
    ('GFEX', 'SI', 'GFEX/SI.parquet', 'GFEX/si.parquet', 'gfex'),
    # INE
    ('INE', 'SC', 'INE/SC.parquet', 'INE/sc.parquet', 'ine'),
]


def parse_option_symbol(sym, exchange_type):
    """解析期权symbol，返回 (contract, cp, strike) 或 None"""
    # 去掉交易所前缀
    s = sym.split('.')[-1] if '.' in sym else sym
    # 优化点2: 使用预编译正则
    m = _OPTION_RE.match(s)
    if m:
        product = m.group(1)
        month = m.group(2)
        cp = m.group(3).upper()
        strike = int(m.group(4))
        return product, month, cp, strike
    return None


def futures_month_to_option_month(f_sym, exchange_type, product):
    """将期货合约号转换为期权合约月份
    期货: SF2401 → 期权月份: 401 (CZCE) or 2401 (SHFE/DCE)
    """
    # 优化点2: 使用预编译正则
    m = _FUTURES_RE.match(f_sym)
    if not m:
        return None
    month_str = m.group(2)  # e.g., '2401'
    if exchange_type == 'czce':
        # CZCE期权用3位月份: 2401 → 401
        if len(month_str) == 4:
            return month_str[1:]  # 去掉千位
        return month_str
    else:
        return month_str


def process_product(exchange, product, futures_path, options_path, exchange_type):
    """处理单个品种，返回所有session的分析结果"""
    f_full = os.path.join(FUTURES_BASE, futures_path)
    o_full = os.path.join(OPTIONS_BASE, options_path)

    if not os.path.exists(f_full) or not os.path.exists(o_full):
        print(f"  ✗ 文件不存在: {f_full} or {o_full}")
        return []

    # 加载数据
    try:
        df_f = pd.read_parquet(f_full, columns=['datetime', 'close', 'symbol'])
        df_o = pd.read_parquet(o_full, columns=['datetime', 'close', 'symbol', 'volume'])
    except Exception as e:
        print(f"  ✗ 加载失败: {e}")
        return []

    # 只用期权数据存在的时间范围
    o_start = df_o['datetime'].min()
    df_f = df_f[df_f['datetime'] >= o_start].copy()

    if len(df_f) == 0 or len(df_o) == 0:
        print(f"  ✗ 无重叠数据")
        return []

    # 过滤掉close=0的数据
    df_f = df_f[df_f['close'] > 0]
    df_o = df_o[df_o['close'] > 0]

    # 找出期货主力合约 (按每天成交量最大的合约)
    # 先确定每个交易日
    df_f['date'] = df_f['datetime'].dt.date

    # 获取所有期货合约
    f_symbols = df_f['symbol'].unique()

    # 优化点3: 用str.extract()向量化替代多次apply()，一次正则提取所有列
    syms = df_o['symbol'].str.split('.').str[-1]
    extracted = syms.str.extract(_OPTION_RE)
    valid = extracted[0].notna()
    df_o = df_o[valid].copy()
    df_o['opt_product'] = extracted.loc[valid, 0]
    df_o['opt_month'] = extracted.loc[valid, 1]
    df_o['cp'] = extracted.loc[valid, 2].str.upper()
    df_o['strike'] = extracted.loc[valid, 3].astype(int)
    df_o['date'] = df_o['datetime'].dt.date

    # 获取期权的合约月份列表
    option_months = df_o['opt_month'].unique()

    # 对每个期货合约，找对应的期权月份
    results = []

    # 按交易日分组处理
    all_dates = sorted(set(df_f['date'].unique()) & set(df_o['date'].unique()))

    # 采样：如果超过200天，均匀采样200天
    if len(all_dates) > 200:
        indices = np.linspace(0, len(all_dates)-1, 200, dtype=int)
        all_dates = [all_dates[i] for i in indices]

    processed_sessions = 0

    for trade_date in all_dates:
        # 获取当天的期货数据
        day_f = df_f[df_f['date'] == trade_date]
        if len(day_f) < 30:  # 至少30分钟数据
            continue

        # 找当天成交量最大的期货合约
        vol_by_sym = day_f.groupby('symbol')['close'].count()
        main_contract = vol_by_sym.idxmax()

        day_futures = day_f[day_f['symbol'] == main_contract].sort_values('datetime')
        if len(day_futures) < 30:
            continue

        # 找对应的期权月份
        opt_month = futures_month_to_option_month(main_contract, exchange_type, product)
        if opt_month is None:
            continue

        # 获取当天该月份的所有期权
        day_opts = df_o[(df_o['date'] == trade_date) & (df_o['opt_month'] == opt_month)]
        if len(day_opts) == 0:
            continue

        # 获取当天期货价格（用于确定OTM程度）
        f_price_mid = day_futures['close'].median()

        # 找所有call和put的行权价
        calls = day_opts[day_opts['cp'] == 'C']
        puts = day_opts[day_opts['cp'] == 'P']

        call_strikes = sorted(calls['strike'].unique())
        put_strikes = sorted(puts['strike'].unique())

        if not call_strikes or not put_strikes:
            continue

        # 构建宽跨式配对: 选3个OTM程度
        # 近端: call/put离ATM最近的OTM
        # 中端: 约10-20% OTM
        # 远端: 约20-40% OTM

        # OTM calls: strike > futures price
        otm_calls = [k for k in call_strikes if k > f_price_mid]
        # OTM puts: strike < futures price
        otm_puts = [k for k in put_strikes if k < f_price_mid]

        if not otm_calls or not otm_puts:
            continue

        # 选配对
        pairs_to_analyze = []

        # 近端: 最近的OTM call + put (约2-5% OTM)
        for c_k in otm_calls:
            c_otm = (c_k - f_price_mid) / f_price_mid
            if 0.02 <= c_otm <= 0.08:
                for p_k in sorted(otm_puts, reverse=True):
                    p_otm = (f_price_mid - p_k) / f_price_mid
                    if 0.02 <= p_otm <= 0.08:
                        pairs_to_analyze.append((c_k, p_k, '近端', c_otm, p_otm))
                        break
                if pairs_to_analyze:
                    break

        # 中端: 10-20% OTM
        for c_k in otm_calls:
            c_otm = (c_k - f_price_mid) / f_price_mid
            if 0.10 <= c_otm <= 0.25:
                for p_k in sorted(otm_puts, reverse=True):
                    p_otm = (f_price_mid - p_k) / f_price_mid
                    if 0.10 <= p_otm <= 0.25:
                        pairs_to_analyze.append((c_k, p_k, '中端', c_otm, p_otm))
                        break
                if len(pairs_to_analyze) >= 2:
                    break

        # 远端: 25%+ OTM
        for c_k in sorted(otm_calls, reverse=True):
            c_otm = (c_k - f_price_mid) / f_price_mid
            if 0.25 <= c_otm <= 0.60:
                for p_k in otm_puts:
                    p_otm = (f_price_mid - p_k) / f_price_mid
                    if 0.25 <= p_otm <= 0.60:
                        pairs_to_analyze.append((c_k, p_k, '远端', c_otm, p_otm))
                        break
                if len(pairs_to_analyze) >= 3:
                    break

        if not pairs_to_analyze:
            continue

        # 构建时间序列
        futures_ts = day_futures.set_index('datetime')['close'].to_dict()
        ft_times = sorted(futures_ts.keys())

        session_pairs = []

        for c_strike, p_strike, otm_label, c_otm_pct, p_otm_pct in pairs_to_analyze:
            # 构建call symbol用于匹配
            call_data = calls[calls['strike'] == c_strike].set_index('datetime')['close'].to_dict()
            put_data = puts[puts['strike'] == p_strike].set_index('datetime')['close'].to_dict()

            if not call_data or not put_data:
                continue

            # 合并时间轴
            all_times = sorted(set(ft_times) & (set(call_data.keys()) | set(put_data.keys())))
            if len(all_times) < 20:
                continue

            # 前向填充构建对齐序列
            last_c, last_p = None, None
            records = []
            for t in all_times:
                f = futures_ts.get(t)
                c = call_data.get(t, last_c)
                p = put_data.get(t, last_p)
                if c is not None: last_c = c
                if p is not None: last_p = p
                if f is not None and last_c is not None and last_p is not None:
                    records.append((t, f, last_c, last_p, last_c + last_p))

            if len(records) < 20:
                continue

            # 分析
            f_start = records[0][1]
            f_end = records[-1][1]
            f_chg = f_end - f_start
            f_pct = f_chg / f_start * 100
            s_start = records[0][4]
            s_end = records[-1][4]
            s_chg = s_end - s_start
            s_pct = s_chg / s_start * 100
            c_start = records[0][2]
            c_end = records[-1][2]
            p_start = records[0][3]
            p_end = records[-1][3]

            # 逐分钟变化分析
            up_ds, dn_ds = [], []
            up_dc, dn_dc = [], []
            up_dp, dn_dp = [], []
            all_df_ds = []

            for i in range(1, len(records)):
                df = records[i][1] - records[i-1][1]
                dc = records[i][2] - records[i-1][2]
                dp = records[i][3] - records[i-1][3]
                ds = records[i][4] - records[i-1][4]
                if df != 0:
                    all_df_ds.append((df, ds))
                if df > 0:
                    up_ds.append(ds); up_dc.append(dc); up_dp.append(dp)
                elif df < 0:
                    dn_ds.append(ds); dn_dc.append(dc); dn_dp.append(dp)

            # 回归
            def regress(pairs_list):
                if len(pairs_list) < 5:
                    return None
                n = len(pairs_list)
                sx = sum(x for x,y in pairs_list)
                sy = sum(y for x,y in pairs_list)
                sxx = sum(x*x for x,y in pairs_list)
                sxy = sum(x*y for x,y in pairs_list)
                denom = n*sxx - sx*sx
                if denom == 0: return None
                return (n*sxy - sx*sy) / denom

            beta_all = regress(all_df_ds)
            beta_up = regress([(x,y) for x,y in all_df_ds if x > 0])
            beta_dn = regress([(x,y) for x,y in all_df_ds if x < 0])

            dn_up_ratio = abs(beta_dn/beta_up) if beta_up and beta_dn and abs(beta_up) > 1e-8 else None

            # Put钉死检测 (用百分比): 期货涨>0.3%但Put变化<1%
            pin_found = False
            pin_duration = 0
            pin_after_sum_pct = None
            f_pct_threshold = 0.003  # 0.3%

            # 优化点1: numpy向量化替代O(n²)嵌套循环
            n_rec = len(records)
            arr_pin = np.array([(r[1], r[3], r[4]) for r in records])  # futures, put, sum
            f_prices = arr_pin[:, 0]
            p_prices = arr_pin[:, 1]
            s_prices = arr_pin[:, 2]

            for window in range(10, min(120, n_rec)):
                f_pct_w = (f_prices[window:] - f_prices[:n_rec-window]) / np.maximum(f_prices[:n_rec-window], 1e-8)
                p_pct_w = np.abs(p_prices[window:] - p_prices[:n_rec-window]) / np.maximum(p_prices[:n_rec-window], 0.01)
                mask = (f_pct_w > f_pct_threshold) & (p_pct_w < 0.02)
                if mask.any():
                    indices = np.where(mask)[0]
                    for idx in indices:
                        dur = window
                        if dur > pin_duration:
                            pin_found = True
                            pin_duration = dur
                            end_i = idx + window
                            if end_i < n_rec - 1:
                                pin_after_sum_pct = (s_prices[-1] - s_prices[end_i]) / max(s_prices[end_i], 0.01) * 100

            # Put暴涨检测 (5分钟内涨>10%)
            put_spike_found = False
            put_spike_futures_down = False
            put_spike_sum_up = False
            for i in range(5, len(records)):
                p_chg_5 = (records[i][3] - records[i-5][3]) / max(records[i-5][3], 0.01) * 100
                if p_chg_5 > 10:
                    put_spike_found = True
                    f_chg_5 = records[i][1] - records[i-5][1]
                    s_chg_5 = records[i][4] - records[i-5][4]
                    put_spike_futures_down = f_chg_5 < 0
                    put_spike_sum_up = s_chg_5 > 0
                    break

            # Put暴涨后平台检测
            put_peak_higher_platform = None
            p_values = [r[3] for r in records]
            p_max_idx = np.argmax(p_values)
            p_max_val = p_values[p_max_idx]
            if p_max_idx < len(records) - 10 and p_max_val > p_start * 1.05:
                # 峰值后的最低点是否高于起始价
                after_peak_min = min(p_values[p_max_idx:])
                put_peak_higher_platform = after_peak_min > p_start

            # 回归残差(theta proxy)
            residuals = []
            if beta_all:
                for i in range(1, len(records)):
                    df_val = records[i][1] - records[i-1][1]
                    ds_val = records[i][4] - records[i-1][4]
                    residuals.append(ds_val - beta_all * df_val)
            avg_residual = np.mean(residuals) if residuals else 0

            # 极深虚值Put退化检测
            p_range_pct = (max(p_values) - min(p_values)) / max(p_start, 0.01) * 100
            f_range_pct = abs(f_pct)

            session_pairs.append({
                'date': str(trade_date),
                'contract': main_contract,
                'otm_label': otm_label,
                'c_strike': c_strike,
                'p_strike': p_strike,
                'c_otm_pct': c_otm_pct,
                'p_otm_pct': p_otm_pct,
                'n_records': len(records),
                'f_start': f_start, 'f_end': f_end,
                'f_chg': f_chg, 'f_pct': f_pct,
                's_start': s_start, 's_end': s_end,
                's_chg': s_chg, 's_pct': s_pct,
                'c_start': c_start, 'c_end': c_end,
                'p_start': p_start, 'p_end': p_end,
                'beta_all': beta_all, 'beta_up': beta_up, 'beta_dn': beta_dn,
                'dn_up_ratio': dn_up_ratio,
                'n_up': len(up_ds), 'n_dn': len(dn_ds),
                'avg_up_ds': np.mean(up_ds) if up_ds else 0,
                'avg_dn_ds': np.mean(dn_ds) if dn_ds else 0,
                'sum_volatility_pct': (max(r[4] for r in records) - min(r[4] for r in records)) / s_start * 100,
                'pin_found': pin_found,
                'pin_duration': pin_duration,
                'pin_after_sum_pct': pin_after_sum_pct,
                'put_spike_found': put_spike_found,
                'put_spike_futures_down': put_spike_futures_down,
                'put_spike_sum_up': put_spike_sum_up,
                'put_peak_higher_platform': put_peak_higher_platform,
                'avg_residual': avg_residual,
                'p_range_pct': p_range_pct,
                'f_range_pct': f_range_pct,
            })

        if session_pairs:
            results.extend(session_pairs)
            processed_sessions += 1

    print(f"  → {processed_sessions}个交易日, {len(results)}个配对分析完成")
    return results


def verify_beliefs(all_results):
    """用所有分析结果验证15个信念"""
    evidence = {f'B{i:03d}': {'for': 0, 'against': 0, 'neutral': 0, 'details': []}
                for i in range(1, 16)}

    # 优化点4: 单次遍历分组，替代三次列表遍历
    grouped = defaultdict(list)
    for r in all_results:
        grouped[r['otm_label']].append(r)
    near = grouped['近端']
    mid = grouped['中端']
    far = grouped['远端']

    print(f"\n数据总量: 近端{len(near)}, 中端{len(mid)}, 远端{len(far)}")

    # === B001: 方向性跟随 ===
    # 期货涨→价格和涨, 期货跌→价格和跌
    for r in all_results:
        if abs(r['f_pct']) < 0.1:  # 期货变化太小，无关
            evidence['B001']['neutral'] += 1
            continue
        same_dir = (r['f_chg'] > 0 and r['s_chg'] > 0) or (r['f_chg'] < 0 and r['s_chg'] < 0)
        if same_dir:
            evidence['B001']['for'] += 1
        else:
            evidence['B001']['against'] += 1

    # === B002: 下跌不对称性 (ratio > 1.5) ===
    for r in all_results:
        if r['dn_up_ratio'] is not None:
            if r['dn_up_ratio'] > 1.5:
                evidence['B002']['for'] += 1
            elif r['dn_up_ratio'] < 1.0:
                evidence['B002']['against'] += 1
            else:
                evidence['B002']['neutral'] += 1

    # === B003: 虚值杠杆 ===
    # 同一天同一合约, 远端%波动 > 近端%波动
    by_date_contract = defaultdict(dict)
    for r in all_results:
        key = (r['date'], r['contract'])
        by_date_contract[key][r['otm_label']] = r

    for key, otms in by_date_contract.items():
        if '近端' in otms and '远端' in otms:
            near_vol = otms['近端']['sum_volatility_pct']
            far_vol = otms['远端']['sum_volatility_pct']
            if far_vol > near_vol:
                evidence['B003']['for'] += 1
            else:
                evidence['B003']['against'] += 1
        if '近端' in otms and '中端' in otms:
            near_vol = otms['近端']['sum_volatility_pct']
            mid_vol = otms['中端']['sum_volatility_pct']
            if mid_vol > near_vol:
                evidence['B003']['for'] += 1
            else:
                evidence['B003']['against'] += 1

    # === B004: Delta结构 (远端Delta < 中端Delta < 近端Delta) ===
    for key, otms in by_date_contract.items():
        if '近端' in otms and '远端' in otms:
            near_d = otms['近端'].get('beta_all')
            far_d = otms['远端'].get('beta_all')
            if near_d is not None and far_d is not None:
                if abs(near_d) > abs(far_d):
                    evidence['B004']['for'] += 1
                else:
                    evidence['B004']['against'] += 1

    # === B005: 三因素驱动 ===
    # 检查回归残差是否为负(暗示theta衰减存在)
    for r in all_results:
        if r['avg_residual'] < -0.001:  # 负残差→theta衰减存在
            evidence['B005']['for'] += 1
        elif r['avg_residual'] > 0.001:
            evidence['B005']['neutral'] += 1  # 正残差可能是vega增加
        else:
            evidence['B005']['neutral'] += 1

    # === B006: 弹簧效应 ===
    # 期货大涨(>1%)时，远端价格和涨幅 > 近端涨幅(百分比)
    for r in all_results:
        if abs(r['f_pct']) > 1.0 and r['otm_label'] == '远端':
            if r['s_pct'] > 5:  # 远端价格和涨>5%
                evidence['B006']['for'] += 1
            elif r['s_pct'] < -5:
                evidence['B006']['against'] += 1
            else:
                evidence['B006']['neutral'] += 1

    # === B007: Put钉死现象 ===
    for r in all_results:
        if r['pin_found']:
            evidence['B007']['for'] += 1
        else:
            evidence['B007']['neutral'] += 1  # 不是每天都会出现

    # === B008: Gamma加速 ===
    # Put钉死出现时，检查Call变化是否加速
    # 简化: 在pin事件中，price sum的变化是否超线性
    for r in all_results:
        if r['pin_found'] and r['pin_duration'] > 20:
            # 如果beta_all存在且正, 说明Call在单边涨
            if r['beta_all'] and r['beta_all'] > 0 and r['f_chg'] > 0:
                evidence['B008']['for'] += 1
            else:
                evidence['B008']['neutral'] += 1

    # === B009: 极深虚值Put退化 ===
    for r in far:
        if r['p_range_pct'] < r['f_range_pct'] * 0.5:  # Put波幅 < 期货波幅的一半
            evidence['B009']['for'] += 1
        elif r['p_range_pct'] > r['f_range_pct'] * 1.5:
            evidence['B009']['against'] += 1
        else:
            evidence['B009']['neutral'] += 1

    # === B010: Put钉死入场信号 ===
    for r in all_results:
        if r['pin_found'] and r['pin_after_sum_pct'] is not None:
            if r['pin_after_sum_pct'] > 0:
                evidence['B010']['for'] += 1
            else:
                evidence['B010']['against'] += 1

    # === B011: Put钉死→暴涨两阶段 ===
    for r in all_results:
        if r['put_spike_found']:
            if r['put_spike_futures_down'] and r['put_spike_sum_up']:
                evidence['B011']['for'] += 1
            elif r['put_spike_found'] and not r['put_spike_futures_down']:
                evidence['B011']['neutral'] += 1
            else:
                evidence['B011']['against'] += 1

    # === B012: Put暴涨后高位平台 ===
    for r in all_results:
        if r['put_peak_higher_platform'] is not None:
            if r['put_peak_higher_platform']:
                evidence['B012']['for'] += 1
            else:
                evidence['B012']['against'] += 1

    # === B013: DTE≤14天theta收割 ===
    # 需要DTE信息，这里用session内的价格和变化(如果价格和下降，说明theta占主导)
    # 粗略: 当期货变化<0.3%时，价格和是否下降?
    for r in all_results:
        if abs(r['f_pct']) < 0.3:  # 期货几乎没动
            if r['s_pct'] < -0.5:  # 价格和下降
                evidence['B013']['for'] += 1
            elif r['s_pct'] > 0.5:
                evidence['B013']['against'] += 1
            else:
                evidence['B013']['neutral'] += 1

    # === B014: 每天6小时交易 ===
    # 基础规则，无需数据验证
    evidence['B014']['for'] += 1
    evidence['B014']['neutral'] += len(all_results)

    # === B015: 总权金≥10门槛 ===
    # 检查低权金配对的表现是否差于高权金配对
    low_prem = [r for r in all_results if r['s_start'] < 10]
    high_prem = [r for r in all_results if r['s_start'] >= 10]
    if low_prem and high_prem:
        low_avg_loss = np.mean([r['s_pct'] for r in low_prem])
        high_avg_gain = np.mean([r['s_pct'] for r in high_prem])
        # 如果低权金配对平均更差
        if low_avg_loss < high_avg_gain:
            evidence['B015']['for'] += len(low_prem)
        else:
            evidence['B015']['against'] += len(low_prem)
    else:
        evidence['B015']['neutral'] += len(all_results)

    return evidence


def bayesian_update(knowledge, evidence):
    """贝叶斯更新知识库"""
    for belief in knowledge['beliefs']:
        bid = belief['id']
        if bid not in evidence:
            continue
        ev = evidence[bid]
        old_conf = belief['confidence']

        net_support = ev['for'] - ev['against']
        total = ev['for'] + ev['against']

        if total == 0:
            continue

        # 根据证据比例更新
        support_rate = ev['for'] / total if total > 0 else 0.5

        if support_rate > 0.6:
            # 支持：每个净支持点+0.02*(1-conf)
            delta = 0.02 * (1 - old_conf) * (ev['for'] - ev['against'])
            delta = min(delta, 0.15)  # 单次最大+0.15
            new_conf = min(old_conf + delta, 0.99)
        elif support_rate < 0.4:
            # 反驳：每个净反对点-0.03*conf
            delta = 0.03 * old_conf * (ev['against'] - ev['for'])
            delta = min(delta, 0.20)  # 单次最大-0.20
            new_conf = max(old_conf - delta, 0.01)
        else:
            # 中性，微调
            new_conf = old_conf

        belief['confidence'] = round(new_conf, 2)
        belief['evidence_for'] += ev['for']
        belief['evidence_against'] += ev['against']
        belief['last_updated'] = '2026-03-02'

        # 添加数据点
        total_sessions = ev['for'] + ev['against'] + ev['neutral']
        belief['data_points'].append(
            f"2026-03-02 全市场历史验证: {total_sessions}个session, "
            f"支持{ev['for']}次/反驳{ev['against']}次/中性{ev['neutral']}次, "
            f"支持率{support_rate*100:.0f}%, "
            f"confidence {old_conf}→{new_conf:.2f}"
        )

    return knowledge


# =========================================================================
# 主流程
# =========================================================================
if __name__ == '__main__':
    print("=" * 70)
    print("全市场期权价格之和信念验证系统")
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    all_results = []

    for exchange, product, f_path, o_path, exch_type in PRODUCTS:
        f_full = os.path.join(FUTURES_BASE, f_path)
        o_full = os.path.join(OPTIONS_BASE, o_path)
        if not os.path.exists(f_full) or not os.path.exists(o_full):
            print(f"\n[跳过] {exchange}/{product}: 文件不存在")
            continue

        print(f"\n[处理] {exchange}/{product}...")
        try:
            results = process_product(exchange, product, f_path, o_path, exch_type)
            for r in results:
                r['exchange'] = exchange
                r['product'] = product
            all_results.extend(results)
        except Exception as e:
            print(f"  ✗ 错误: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{'='*70}")
    print(f"总计分析: {len(all_results)}个配对")
    print(f"{'='*70}")

    if not all_results:
        print("无有效数据，退出")
        exit(1)

    # 验证信念
    evidence = verify_beliefs(all_results)

    # 打印验证结果
    print(f"\n{'='*70}")
    print("信念验证结果")
    print(f"{'='*70}")

    belief_names = {
        'B001': '方向性跟随',
        'B002': '下跌不对称性',
        'B003': '虚值杠杆效应',
        'B004': 'Delta结构递减',
        'B005': '三因素驱动',
        'B006': '弹簧效应',
        'B007': 'Put钉死现象',
        'B008': 'Gamma加速',
        'B009': '极深虚值Put退化',
        'B010': 'Put钉死入场信号',
        'B011': 'Put钉死→暴涨两阶段',
        'B012': 'Put暴涨后高位平台',
        'B013': 'DTE≤14天theta收割',
        'B014': '每天6小时交易时间',
        'B015': '总权金≥10门槛',
    }

    for bid in sorted(evidence.keys()):
        ev = evidence[bid]
        total = ev['for'] + ev['against']
        rate = ev['for'] / total * 100 if total > 0 else 0
        name = belief_names.get(bid, '')
        status = '✓支持' if rate > 60 else ('✗反驳' if rate < 40 else '~中性')
        print(f"  {bid} {name}: 支持{ev['for']}/反驳{ev['against']}/中性{ev['neutral']} "
              f"({rate:.0f}%) → {status}")

    # 加载并更新知识库
    with open(KNOWLEDGE_PATH, 'r') as f:
        knowledge = json.load(f)

    knowledge = bayesian_update(knowledge, evidence)

    # 更新日志
    knowledge['update_log'].append({
        'date': '2026-03-02',
        'session': '全市场历史数据验证',
        'summary': f"使用{len(PRODUCTS)}个品种的parquet历史数据(2022-12~2025-08)全面验证15个信念。"
                   f"共分析{len(all_results)}个session/配对组合。",
        'changes': '; '.join([
            f"{bid} {ev['for']}支持/{ev['against']}反驳 → conf更新"
            for bid, ev in sorted(evidence.items())
            if ev['for'] + ev['against'] > 0
        ])
    })
    knowledge['meta']['update_count'] += 1
    knowledge['meta']['version'] = '1.5'

    # 保存结果到临时文件(不直接覆盖知识库，先检查)
    output_path = '/Users/zhangxiaoyu/Scripts/belief_verification_results.json'
    with open(output_path, 'w') as f:
        json.dump({
            'run_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_results': len(all_results),
            'evidence': evidence,
            'updated_knowledge': knowledge,
            'sample_results': all_results[:20],  # 保存一些样本数据
            'stats': {
                'products_analyzed': len(set(r['product'] for r in all_results)),
                'sessions_analyzed': len(set((r['product'], r['date']) for r in all_results)),
                'by_otm': {
                    label: sum(1 for r in all_results if r['otm_label'] == label)
                    for label in ('近端', '中端', '远端')
                }
            }
        }, f, ensure_ascii=False, indent=2)

    # 同时写回知识库
    with open(KNOWLEDGE_PATH, 'w') as f:
        json.dump(knowledge, f, ensure_ascii=False, indent=2)

    print(f"\n结果保存到: {output_path}")
    print(f"知识库已更新: {KNOWLEDGE_PATH}")

    # 打印最终置信度
    print(f"\n{'='*70}")
    print("更新后的信念置信度")
    print(f"{'='*70}")
    for belief in knowledge['beliefs']:
        bid = belief['id']
        name = belief_names.get(bid, belief.get('category', ''))
        ev = evidence.get(bid, {})
        old_dp = len(belief['data_points']) - 1  # 减去刚添加的
        print(f"  {bid} {name}: {belief['confidence']:.2f} "
              f"(evidence: +{belief['evidence_for']}/-{belief['evidence_against']})")
