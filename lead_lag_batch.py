"""
批量分钟级引领性测试：所有强关联品种对的动量拖拽效应（向量化版）

对每个关联对(A,B)，测试两个方向：
1. A涨 + B跌 → B会止跌吗？（对比A横盘时B跌的情况）
2. B涨 + A跌 → A会止跌吗？（对比B横盘时A跌的情况）
"""
import pandas as pd
import numpy as np
import os
import glob
import json
import time
from pathlib import Path

LOCAL_PARQUET = os.path.expanduser("~/Downloads/期货数据_parquet")
USB_CSV = "/Volumes/Lexar/期货数据备份/期货_全部数据"

EXCHANGE_MAP = {
    'CF': 'CZCE', 'SA': 'CZCE', 'TA': 'CZCE', 'FG': 'CZCE', 'SR': 'CZCE',
    'MA': 'CZCE', 'OI': 'CZCE', 'RM': 'CZCE', 'SF': 'CZCE', 'SM': 'CZCE',
    'AP': 'CZCE', 'CJ': 'CZCE', 'PK': 'CZCE', 'CY': 'CZCE', 'PF': 'CZCE',
    'UR': 'CZCE', 'ZC': 'CZCE', 'RS': 'CZCE', 'SH': 'CZCE', 'PX': 'CZCE',
    'P': 'DCE', 'Y': 'DCE', 'M': 'DCE', 'C': 'DCE', 'CS': 'DCE',
    'I': 'DCE', 'J': 'DCE', 'JM': 'DCE', 'JD': 'DCE', 'LH': 'DCE',
    'L': 'DCE', 'PP': 'DCE', 'V': 'DCE', 'EG': 'DCE', 'EB': 'DCE',
    'PG': 'DCE', 'A': 'DCE', 'B': 'DCE', 'SS': 'DCE',
    'CU': 'SHFE', 'AL': 'SHFE', 'ZN': 'SHFE', 'NI': 'SHFE', 'SN': 'SHFE',
    'PB': 'SHFE', 'AU': 'SHFE', 'AG': 'SHFE', 'RB': 'SHFE', 'HC': 'SHFE',
    'RU': 'SHFE', 'BU': 'SHFE', 'FU': 'SHFE', 'BR': 'SHFE', 'AO': 'SHFE',
    'AD': 'SHFE', 'SP': 'SHFE',
    'SC': 'INE', 'NR': 'INE', 'LU': 'INE', 'BC': 'INE', 'EC': 'INE',
    'SI': 'GFEX', 'LC': 'GFEX',
}


def load_main_contract_minutes(code, start_year=2022):
    """加载品种主力合约1分钟K线"""
    exchange = EXCHANGE_MAP.get(code)
    if not exchange:
        return None

    parquet_path = os.path.join(LOCAL_PARQUET, exchange, f"{code}.parquet")
    if os.path.exists(parquet_path):
        df = pd.read_parquet(parquet_path)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df[df['datetime'].dt.year >= start_year]
    else:
        csv_dir = os.path.join(USB_CSV, exchange, code)
        if not os.path.exists(csv_dir):
            return None
        csv_files = sorted(glob.glob(os.path.join(csv_dir, "*.csv")))
        if not csv_files:
            return None
        dfs = []
        for f in csv_files:
            stem = Path(f).stem
            digits = ''.join(c for c in stem if c.isdigit())
            if len(digits) >= 2:
                yr = int(digits[:2]) + 2000 if int(digits[:2]) < 50 else int(digits[:2]) + 1900
                if yr < start_year:
                    continue
            try:
                d = pd.read_csv(f, parse_dates=['datetime'])
                d['symbol'] = stem
                dfs.append(d)
            except Exception:
                continue
        if not dfs:
            return None
        df = pd.concat(dfs, ignore_index=True)
        df['datetime'] = pd.to_datetime(df['datetime'])
        if 'money' in df.columns:
            df = df.rename(columns={'money': 'turnover'})
        if 'open_interest' in df.columns:
            df = df.rename(columns={'open_interest': 'close_oi'})

    if 'volume' not in df.columns or 'symbol' not in df.columns:
        return None
    df = df[df['volume'] > 0].copy()
    if len(df) < 1000:
        return None

    df['date'] = df['datetime'].dt.date
    daily_vol = df.groupby(['date', 'symbol'])['volume'].sum().reset_index()
    idx = daily_vol.groupby('date')['volume'].idxmax()
    main_contracts = daily_vol.loc[idx, ['date', 'symbol']].set_index('date')['symbol']

    result = []
    for date, main_sym in main_contracts.items():
        day_data = df[(df['date'] == date) & (df['symbol'] == main_sym)]
        result.append(day_data)

    out = pd.concat(result, ignore_index=True)
    out = out.sort_values('datetime').reset_index(drop=True)
    return out[['datetime', 'close', 'volume']].copy()


def add_indicators(df):
    """加均线和动量（向量化）"""
    c = df['close'].astype(float)
    df['ma5'] = c.rolling(5).mean()
    df['ma10'] = c.rolling(10).mean()
    df['ma20'] = c.rolling(20).mean()
    df['mom10'] = c.pct_change(10) * 100
    return df


def test_drag_vectorized(leader, follower, look_ahead=30,
                         leader_mom_thr=0.05, follower_mom_thr=-0.02):
    """
    向量化测试拖拽效应。

    返回:
    - up_result: A涨+B跌 → B止跌率对比
    - down_result: A跌+B涨 → B被拖跌率对比
    """
    # 对齐
    merged = pd.merge(leader, follower, on='datetime', suffixes=('_L', '_F'))
    if len(merged) < 2000:
        return None, None

    # 计算follower未来收益
    merged['future_close_F'] = merged['close_F'].shift(-look_ahead)
    merged['future_ret'] = (merged['future_close_F'] - merged['close_F']) / merged['close_F'] * 100
    merged = merged.dropna(subset=['future_ret', 'mom10_L', 'mom10_F',
                                    'ma5_L', 'ma10_L', 'ma20_L'])

    # === 上涨拖拽测试 ===
    # follower在跌
    f_falling = merged['mom10_F'] < follower_mom_thr

    # leader状态
    l_rising = ((merged['mom10_L'] > leader_mom_thr) &
                (merged['close_L'] > merged['ma20_L']) &
                (merged['ma5_L'] > merged['ma10_L']))
    l_flat = merged['mom10_L'].abs() <= leader_mom_thr

    # 实验组: leader涨 + follower跌
    exp_mask = f_falling & l_rising
    # 对照组: leader横盘 + follower跌
    ctrl_mask = f_falling & l_flat

    # 采样（模拟冷却期：每15根取一个）
    exp_indices = merged.index[exp_mask]
    ctrl_indices = merged.index[ctrl_mask]

    exp_sampled = _sample_with_cooldown(exp_indices, cooldown=15)
    ctrl_sampled = _sample_with_cooldown(ctrl_indices, cooldown=15)

    up_result = None
    if len(exp_sampled) >= 30 and len(ctrl_sampled) >= 30:
        exp_ret = merged.loc[exp_sampled, 'future_ret']
        ctrl_ret = merged.loc[ctrl_sampled, 'future_ret']
        exp_stop_rate = (exp_ret >= 0).mean() * 100
        ctrl_stop_rate = (ctrl_ret >= 0).mean() * 100
        up_result = {
            '实验组止跌率': round(exp_stop_rate, 1),
            '对照组止跌率': round(ctrl_stop_rate, 1),
            '拖拽效应': round(exp_stop_rate - ctrl_stop_rate, 1),
            '实验组信号数': len(exp_sampled),
            '对照组信号数': len(ctrl_sampled),
            '实验组均涨幅': round(exp_ret.mean(), 4),
            '对照组均涨幅': round(ctrl_ret.mean(), 4),
            '涨幅差': round(exp_ret.mean() - ctrl_ret.mean(), 4),
        }

    # === 下跌拖拽测试 ===
    f_rising = merged['mom10_F'] > abs(follower_mom_thr)
    l_falling = ((merged['mom10_L'] < -leader_mom_thr) &
                 (merged['close_L'] < merged['ma20_L']) &
                 (merged['ma5_L'] < merged['ma10_L']))

    exp_mask2 = f_rising & l_falling
    ctrl_mask2 = f_rising & l_flat

    exp_indices2 = merged.index[exp_mask2]
    ctrl_indices2 = merged.index[ctrl_mask2]

    exp_sampled2 = _sample_with_cooldown(exp_indices2, cooldown=15)
    ctrl_sampled2 = _sample_with_cooldown(ctrl_indices2, cooldown=15)

    down_result = None
    if len(exp_sampled2) >= 30 and len(ctrl_sampled2) >= 30:
        exp_ret2 = merged.loc[exp_sampled2, 'future_ret']
        ctrl_ret2 = merged.loc[ctrl_sampled2, 'future_ret']
        exp_drag_rate = (exp_ret2 < 0).mean() * 100
        ctrl_drag_rate = (ctrl_ret2 < 0).mean() * 100
        down_result = {
            '实验组被拖跌率': round(exp_drag_rate, 1),
            '对照组被拖跌率': round(ctrl_drag_rate, 1),
            '拖拽效应': round(exp_drag_rate - ctrl_drag_rate, 1),
            '实验组信号数': len(exp_sampled2),
            '对照组信号数': len(ctrl_sampled2),
            '实验组均涨幅': round(exp_ret2.mean(), 4),
            '对照组均涨幅': round(ctrl_ret2.mean(), 4),
            '涨幅差': round(exp_ret2.mean() - ctrl_ret2.mean(), 4),
        }

    return up_result, down_result


def _sample_with_cooldown(indices, cooldown=15):
    """模拟冷却期采样"""
    if len(indices) == 0:
        return []
    sampled = []
    last = -cooldown - 1
    for idx in indices:
        if idx - last > cooldown:
            sampled.append(idx)
            last = idx
    return sampled


def main():
    # 加载相关性表
    with open(os.path.expanduser("~/Scripts/futures_correlation_table.json"), 'r') as f:
        table = json.load(f)

    # 提取去重的品种对（|r|>=0.4）
    seen = set()
    unique_pairs = []
    for item in table['品种列表']:
        code_a = item['代码'].split('/')[0]
        name_a = item['名称']
        for corr in item['关联品种']:
            code_b = corr['代码'].split('/')[0]
            name_b = corr['名称']
            r = corr.get('实测相关系数', corr.get('网上参考值', 0))
            if abs(r) < 0.4:
                continue
            pair_key = tuple(sorted([code_a, code_b]))
            if pair_key in seen:
                continue
            seen.add(pair_key)
            unique_pairs.append((code_a, name_a, code_b, name_b, r,
                                 corr.get('关系类型', '')))

    print(f"需要测试的品种对: {len(unique_pairs)}")

    # 收集所有需要的品种
    all_codes = set()
    for a, _, b, _, _, _ in unique_pairs:
        all_codes.add(a)
        all_codes.add(b)

    print(f"正在加载 {len(all_codes)} 个品种的分钟数据...\n")
    data_cache = {}
    failed = []
    for code in sorted(all_codes):
        t0 = time.time()
        df = load_main_contract_minutes(code, start_year=2022)
        elapsed = time.time() - t0
        if df is not None:
            df = add_indicators(df)
            data_cache[code] = df
            print(f"  {code}: {len(df):>8}根K线 ({elapsed:.1f}s)")
        else:
            failed.append(code)
            print(f"  {code}: 加载失败 ({elapsed:.1f}s)")

    print(f"\n成功加载: {len(data_cache)}/{len(all_codes)}")
    if failed:
        print(f"失败品种: {failed}")

    # === 批量测试 ===
    print("\n" + "=" * 120)
    print("批量动量拖拽效应测试（30分钟窗口）")
    print("=" * 120)

    results = []
    total = sum(1 for a, _, b, _, _, _ in unique_pairs
                if a in data_cache and b in data_cache)
    done = 0

    for code_a, name_a, code_b, name_b, corr_r, rel_type in unique_pairs:
        if code_a not in data_cache or code_b not in data_cache:
            continue

        done += 1
        t0 = time.time()
        df_a = data_cache[code_a]
        df_b = data_cache[code_b]

        # 方向1: A→B
        up1, down1 = test_drag_vectorized(df_a, df_b)
        # 方向2: B→A
        up2, down2 = test_drag_vectorized(df_b, df_a)

        elapsed = time.time() - t0
        print(f"  [{done}/{total}] {code_a}↔{code_b} ({elapsed:.1f}s)"
              f"  A→B上涨{'有' if up1 else '无'}信号"
              f"  B→A上涨{'有' if up2 else '无'}信号")

        row = {
            '品种A': code_a, '名称A': name_a,
            '品种B': code_b, '名称B': name_b,
            '日线相关系数': corr_r, '关系类型': rel_type,
        }

        if up1:
            row['A涨→B止跌_实验'] = up1['实验组止跌率']
            row['A涨→B止跌_对照'] = up1['对照组止跌率']
            row['A涨→B止跌_效应'] = up1['拖拽效应']
            row['A涨→B_信号数'] = up1['实验组信号数']
            row['A涨→B_涨幅差'] = up1['涨幅差']

        if down1:
            row['A跌→B拖跌_实验'] = down1['实验组被拖跌率']
            row['A跌→B拖跌_对照'] = down1['对照组被拖跌率']
            row['A跌→B拖跌_效应'] = down1['拖拽效应']

        if up2:
            row['B涨→A止跌_实验'] = up2['实验组止跌率']
            row['B涨→A止跌_对照'] = up2['对照组止跌率']
            row['B涨→A止跌_效应'] = up2['拖拽效应']
            row['B涨→A_信号数'] = up2['实验组信号数']
            row['B涨→A_涨幅差'] = up2['涨幅差']

        if down2:
            row['B跌→A拖跌_实验'] = down2['实验组被拖跌率']
            row['B跌→A拖跌_对照'] = down2['对照组被拖跌率']
            row['B跌→A拖跌_效应'] = down2['拖拽效应']

        results.append(row)

    # === 输出排名 ===
    print(f"\n完成测试: {len(results)} 个品种对\n")

    print("=" * 120)
    print("【上涨拖拽排名】A涨→B止跌（正值=A的涨势帮B止跌，负值=反而让B跌更多）")
    print(f"{'排名':>4} {'品种对':<28} {'关系':>10} {'r':>5}"
          f" {'实验%':>7} {'对照%':>7} {'效应':>7} {'信号数':>6} {'涨幅差%':>9}")
    print("-" * 120)

    all_up_rows = []
    for row in results:
        if 'A涨→B止跌_效应' in row:
            all_up_rows.append((
                f"{row['品种A']}({row['名称A']})→{row['品种B']}({row['名称B']})",
                row['关系类型'], row['日线相关系数'],
                row['A涨→B止跌_实验'], row['A涨→B止跌_对照'],
                row['A涨→B止跌_效应'], row['A涨→B_信号数'], row['A涨→B_涨幅差'],
            ))
        if 'B涨→A止跌_效应' in row:
            all_up_rows.append((
                f"{row['品种B']}({row['名称B']})→{row['品种A']}({row['名称A']})",
                row['关系类型'], row['日线相关系数'],
                row['B涨→A止跌_实验'], row['B涨→A止跌_对照'],
                row['B涨→A止跌_效应'], row['B涨→A_信号数'], row['B涨→A_涨幅差'],
            ))

    all_up_rows.sort(key=lambda x: x[5], reverse=True)
    for rank, (pair, rel, r, exp, ctrl, eff, n, rdiff) in enumerate(all_up_rows, 1):
        marker = "★" if eff > 3 else ("▲" if eff > 0 else "▼")
        print(f" {rank:>3} {marker} {pair:<26} {rel:>8} {r:>5.2f}"
              f" {exp:>6.1f} {ctrl:>6.1f} {eff:>+6.1f} {n:>5} {rdiff:>+8.4f}")

    # 下跌拖拽排名
    print()
    print("=" * 120)
    print("【下跌拖拽排名】A跌→B被拖跌（正值=A的跌势拖B下来，负值=B反而逆势上涨）")
    print(f"{'排名':>4} {'品种对':<28} {'关系':>10} {'r':>5}"
          f" {'实验%':>7} {'对照%':>7} {'效应':>7}")
    print("-" * 120)

    all_down_rows = []
    for row in results:
        if 'A跌→B拖跌_效应' in row:
            all_down_rows.append((
                f"{row['品种A']}({row['名称A']})↓{row['品种B']}({row['名称B']})",
                row['关系类型'], row['日线相关系数'],
                row['A跌→B拖跌_实验'], row['A跌→B拖跌_对照'],
                row['A跌→B拖跌_效应'],
            ))
        if 'B跌→A拖跌_效应' in row:
            all_down_rows.append((
                f"{row['品种B']}({row['名称B']})↓{row['品种A']}({row['名称A']})",
                row['关系类型'], row['日线相关系数'],
                row['B跌→A拖跌_实验'], row['B跌→A拖跌_对照'],
                row['B跌→A拖跌_效应'],
            ))

    all_down_rows.sort(key=lambda x: x[5], reverse=True)
    for rank, (pair, rel, r, exp, ctrl, eff) in enumerate(all_down_rows, 1):
        marker = "★" if eff > 3 else ("▲" if eff > 0 else "▼")
        print(f" {rank:>3} {marker} {pair:<26} {rel:>8} {r:>5.2f}"
              f" {exp:>6.1f} {ctrl:>6.1f} {eff:>+6.1f}")

    # === 汇总 ===
    print("\n" + "=" * 80)
    print("汇总统计")
    print("-" * 80)

    all_up_eff = [x[5] for x in all_up_rows]
    all_down_eff = [x[5] for x in all_down_rows]

    if all_up_eff:
        pos = sum(1 for e in all_up_eff if e > 0)
        strong_pos = sum(1 for e in all_up_eff if e > 3)
        strong_neg = sum(1 for e in all_up_eff if e < -3)
        print(f"上涨止跌: {len(all_up_eff)}个方向")
        print(f"  正效应(确实帮止跌): {pos} ({pos/len(all_up_eff)*100:.0f}%)"
              f"  其中强效应(>3%): {strong_pos}")
        print(f"  负效应(反而跌更多): {len(all_up_eff)-pos} ({(len(all_up_eff)-pos)/len(all_up_eff)*100:.0f}%)"
              f"  其中强负(<-3%): {strong_neg}")
        print(f"  总体均值: {np.mean(all_up_eff):+.1f}%")

    if all_down_eff:
        pos = sum(1 for e in all_down_eff if e > 0)
        strong_pos = sum(1 for e in all_down_eff if e > 3)
        print(f"\n下跌拖拽: {len(all_down_eff)}个方向")
        print(f"  正效应(确实拖下来): {pos} ({pos/len(all_down_eff)*100:.0f}%)"
              f"  其中强效应(>3%): {strong_pos}")
        print(f"  总体均值: {np.mean(all_down_eff):+.1f}%")

    # === 有价值的品种对 ===
    print("\n" + "=" * 80)
    print("【有交易参考价值的方向】（|效应| > 3%）")
    print("-" * 80)

    valuable = []
    for pair, rel, r, exp, ctrl, eff, n, rdiff in all_up_rows:
        if abs(eff) > 3:
            valuable.append((pair, '上涨止跌', eff, exp, ctrl, n, rel, r))
    for pair, rel, r, exp, ctrl, eff in all_down_rows:
        if abs(eff) > 3:
            valuable.append((pair, '下跌拖拽', eff, exp, ctrl, 0, rel, r))

    valuable.sort(key=lambda x: abs(x[2]), reverse=True)
    for pair, dtype, eff, exp, ctrl, n, rel, r in valuable:
        direction = "正向" if eff > 0 else "反向"
        print(f"  {pair:<30} {dtype}  效应{eff:>+6.1f}%  "
              f"实验{exp:.1f}% 对照{ctrl:.1f}%  {direction}  ({rel})")

    # 保存
    out_path = os.path.expanduser("~/Scripts/lead_lag_batch_results.json")
    with open(out_path, 'w') as f:
        json.dump({
            '测试参数': {
                '数据起始年': 2022,
                'leader动量阈值': '0.05%',
                'follower动量阈值': '-0.02%',
                '观察窗口': '30分钟',
                '冷却期': '15分钟',
            },
            '品种对结果': results,
            '上涨拖拽排名': [
                {'品种对': x[0], '关系': x[1], 'r': x[2],
                 '实验组': x[3], '对照组': x[4], '效应': x[5]}
                for x in all_up_rows
            ],
            '下跌拖拽排名': [
                {'品种对': x[0], '关系': x[1], 'r': x[2],
                 '实验组': x[3], '对照组': x[4], '效应': x[5]}
                for x in all_down_rows
            ],
        }, f, ensure_ascii=False, indent=2)
    print(f"\n完整结果已保存: {out_path}")


if __name__ == '__main__':
    main()
