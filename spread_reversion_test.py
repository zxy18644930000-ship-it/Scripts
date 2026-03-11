"""
分钟级价差回归测试：强相关品种对

核心问题：
当两个强相关品种的价差偏离均值时，价差会不会回归？
回归速度多快？能不能用来做日内配对交易？

两种价差定义：
1. 比值法：A/B 的Z-Score（适合所有品种，不受价格量级影响）
2. 价差法：A - β*B 的Z-Score（β为滚动回归系数，适合同类品种）

测试逻辑：
- 用过去120根K线的滚动窗口计算价差均值和标准差
- 当Z-Score > 2 或 < -2 时触发信号（价差偏离2个标准差）
- 观察后续5/10/15/30/60分钟价差是否回归（Z-Score回到0附近）
"""
import pandas as pd
import numpy as np
import os
import json
import time
from lead_lag_batch import load_main_contract_minutes, add_indicators, EXCHANGE_MAP


def test_spread_reversion(df_a, df_b, window=120, z_threshold=2.0,
                          look_ahead_bars=[5, 10, 15, 30, 60]):
    """
    测试价差回归效应。

    使用比值法：ratio = close_A / close_B
    Z-Score = (ratio - rolling_mean) / rolling_std

    当Z > threshold: 价差偏高（A相对B涨太多）
    当Z < -threshold: 价差偏低（A相对B跌太多）

    观察后续Z-Score是否回归0
    """
    merged = pd.merge(df_a[['datetime', 'close']],
                      df_b[['datetime', 'close']],
                      on='datetime', suffixes=('_A', '_B'))

    if len(merged) < window + 100:
        return None

    # 计算比值和Z-Score
    merged['ratio'] = merged['close_A'] / merged['close_B']
    merged['ratio_mean'] = merged['ratio'].rolling(window).mean()
    merged['ratio_std'] = merged['ratio'].rolling(window).std()
    merged['z_score'] = (merged['ratio'] - merged['ratio_mean']) / merged['ratio_std']

    # 计算未来Z-Score
    for bars in look_ahead_bars:
        merged[f'z_future_{bars}'] = merged['z_score'].shift(-bars)

    merged = merged.dropna()

    # 信号: Z > threshold (A相对贵了) 或 Z < -threshold (A相对便宜了)
    signals_high = []  # Z > 2
    signals_low = []   # Z < -2

    cooldown = 0
    for i in range(len(merged)):
        if cooldown > 0:
            cooldown -= 1
            continue

        row = merged.iloc[i]
        z = row['z_score']

        if abs(z) >= z_threshold:
            signal = {
                '时间': str(row['datetime']),
                'Z值': round(float(z), 3),
                '价格A': round(float(row['close_A']), 2),
                '价格B': round(float(row['close_B']), 2),
                '比值': round(float(row['ratio']), 6),
            }

            for bars in look_ahead_bars:
                future_z = row[f'z_future_{bars}']
                if not pd.isna(future_z):
                    # Z回归程度 = 1 - |future_z| / |z|
                    reversion = 1 - abs(future_z) / abs(z)
                    signal[f'{bars}分钟后Z'] = round(float(future_z), 3)
                    signal[f'{bars}分钟回归度'] = round(float(reversion), 3)
                    # 方向正确 = Z值绝对值减小
                    signal[f'{bars}分钟回归'] = bool(abs(future_z) < abs(z))

            if z > z_threshold:
                signals_high.append(signal)
            else:
                signals_low.append(signal)

            cooldown = 15  # 冷却15分钟

    return signals_high, signals_low


def print_reversion_stats(signals_high, signals_low, name_a, name_b,
                          bars_list=[5, 10, 15, 30, 60]):
    """打印回归统计"""
    all_signals = signals_high + signals_low
    if not all_signals:
        print("  无信号")
        return {}

    df = pd.DataFrame(all_signals)

    print(f"  信号总数: {len(df)} (偏高{len(signals_high)} + 偏低{len(signals_low)})")
    print(f"  平均|Z|: {df['Z值'].abs().mean():.2f}")
    print()

    result = {}
    print(f"  {'窗口':<10} {'回归率':>8} {'平均回归度':>10} {'均Z变化':>10}"
          f" {'中位回归度':>10}")
    print("  " + "-" * 60)

    for bars in bars_list:
        col_back = f'{bars}分钟回归'
        col_degree = f'{bars}分钟回归度'
        col_z = f'{bars}分钟后Z'

        if col_back not in df.columns:
            continue

        valid = df[col_back].dropna()
        if len(valid) == 0:
            continue

        reversion_rate = valid.mean() * 100
        avg_degree = df[col_degree].dropna().mean() * 100
        med_degree = df[col_degree].dropna().median() * 100
        avg_z_before = df['Z值'].abs().mean()
        avg_z_after = df[col_z].dropna().abs().mean()

        print(f"  {bars:>3}分钟后  {reversion_rate:>7.1f}% {avg_degree:>+9.1f}%"
              f" |Z|{avg_z_before:.2f}→{avg_z_after:.2f}"
              f" {med_degree:>+9.1f}%")

        result[f'{bars}分钟'] = {
            '回归率': round(reversion_rate, 1),
            '平均回归度': round(avg_degree, 1),
            '中位回归度': round(med_degree, 1),
        }

    return result


def main():
    # 要测试的品种对（按相关性排序）
    pairs = [
        ('RB', 'HC', '螺纹钢', '热卷', 0.88),
        ('RU', 'NR', '天然橡胶', '20号胶', 0.84),
        ('TA', 'PF', 'PTA', '短纤', 0.84),
        ('P', 'Y', '棕榈油', '豆油', 0.82),
        ('PP', 'L', '聚丙烯', 'LLDPE', 0.80),
        ('SC', 'LU', '原油', '低硫燃料油', 0.78),
        ('FU', 'LU', '燃料油', '低硫燃料油', 0.78),
        ('AU', 'AG', '黄金', '白银', 0.76),
        ('J', 'JM', '焦炭', '焦煤', 0.73),
        ('SC', 'FU', '原油', '燃料油', 0.73),
        ('CU', 'ZN', '铜', '锌', 0.72),
        ('CF', 'CY', '棉花', '棉纱', 0.72),
        ('Y', 'OI', '豆油', '菜油', 0.72),
        ('NI', 'SN', '镍', '锡', 0.50),  # 增加一个中等相关的对照
        ('NR', 'BR', '20号胶', '合成橡胶', 0.68),
        ('I', 'RB', '铁矿石', '螺纹钢', 0.68),
        ('I', 'HC', '铁矿石', '热卷', 0.65),
        ('RB', 'J', '螺纹钢', '焦炭', 0.62),
        ('EG', 'PF', '乙二醇', '短纤', 0.63),
        ('M', 'RM', '豆粕', '菜粕', 0.59),
        ('SA', 'FG', '纯碱', '玻璃', 0.49),
    ]

    print("=" * 100)
    print("分钟级价差回归测试")
    print("当两个强相关品种的价差偏离均值2个标准差时，价差会回归吗？")
    print("=" * 100)

    # 加载数据
    all_codes = set()
    for a, b, _, _, _ in pairs:
        all_codes.add(a)
        all_codes.add(b)

    print(f"\n正在加载 {len(all_codes)} 个品种...")
    cache = {}
    for code in sorted(all_codes):
        t0 = time.time()
        df = load_main_contract_minutes(code, start_year=2022)
        elapsed = time.time() - t0
        if df is not None:
            cache[code] = df
            print(f"  {code}: {len(df)}根K线 ({elapsed:.1f}s)")
        else:
            print(f"  {code}: 失败 ({elapsed:.1f}s)")

    # 测试每对
    print("\n" + "=" * 100)
    all_results = []

    for code_a, code_b, name_a, name_b, daily_r in pairs:
        if code_a not in cache or code_b not in cache:
            continue

        print(f"\n{'='*80}")
        print(f"{name_a}({code_a}) ↔ {name_b}({code_b})  日线r={daily_r}")
        print("-" * 80)

        result = test_spread_reversion(cache[code_a], cache[code_b])
        if result is None:
            print("  数据不足")
            continue

        signals_high, signals_low = result
        stats = print_reversion_stats(signals_high, signals_low, name_a, name_b)

        all_results.append({
            '品种A': code_a, '名称A': name_a,
            '品种B': code_b, '名称B': name_b,
            '日线r': daily_r,
            '信号数_偏高': len(signals_high),
            '信号数_偏低': len(signals_low),
            '统计': stats,
        })

    # === 汇总排名 ===
    print("\n" + "=" * 100)
    print("【价差回归排名】30分钟回归率（从高到低）")
    print("-" * 100)
    print(f"{'排名':>4} {'品种对':<24} {'r':>5} {'信号数':>6}"
          f" {'30分回归率':>10} {'30分回归度':>10} {'60分回归率':>10} {'60分回归度':>10}")
    print("-" * 100)

    sorted_results = sorted(all_results,
                            key=lambda x: x['统计'].get('30分钟', {}).get('回归率', 0),
                            reverse=True)

    for rank, r in enumerate(sorted_results, 1):
        pair = f"{r['名称A']}({r['品种A']})↔{r['名称B']}({r['品种B']})"
        n = r['信号数_偏高'] + r['信号数_偏低']
        s30 = r['统计'].get('30分钟', {})
        s60 = r['统计'].get('60分钟', {})

        r30 = s30.get('回归率', 0)
        d30 = s30.get('平均回归度', 0)
        r60 = s60.get('回归率', 0)
        d60 = s60.get('平均回归度', 0)

        marker = "★" if r30 > 65 else ("▲" if r30 > 55 else "▼")
        print(f" {rank:>3} {marker} {pair:<22} {r['日线r']:>5.2f} {n:>5}"
              f" {r30:>9.1f}% {d30:>+9.1f}% {r60:>9.1f}% {d60:>+9.1f}%")

    # === 对比不同窗口 ===
    print("\n" + "=" * 100)
    print("【各品种对 5/10/15/30/60分钟回归率一览】")
    print("-" * 100)
    print(f"{'品种对':<24} {'r':>5} {'5分钟':>8} {'10分钟':>8}"
          f" {'15分钟':>8} {'30分钟':>8} {'60分钟':>8}")
    print("-" * 100)

    for r in sorted_results:
        pair = f"{r['名称A']}↔{r['名称B']}"
        vals = []
        for t in ['5分钟', '10分钟', '15分钟', '30分钟', '60分钟']:
            v = r['统计'].get(t, {}).get('回归率', 0)
            vals.append(f"{v:>7.1f}%")
        print(f"  {pair:<22} {r['日线r']:>5.2f} {''.join(vals)}")

    # 保存
    out_path = os.path.expanduser("~/Scripts/spread_reversion_results.json")
    with open(out_path, 'w') as f:
        json.dump({
            '测试参数': {
                '滚动窗口': 120,
                'Z阈值': 2.0,
                '冷却期': 15,
                '数据起始年': 2022,
            },
            '品种对结果': all_results,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存: {out_path}")


if __name__ == '__main__':
    main()
