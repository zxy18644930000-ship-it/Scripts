"""
价差信号优化宽跨卖出时机

核心假说：
当CF/CY的比值Z-Score在21:00偏离较大时(|Z|>1.5)，
CF更可能发生均值回归运动 → 期货波动增大 → 宽跨卖出风险增加。
反之，Z接近0时CF处于"公允值"，更稳定，适合卖出宽跨。

测试方法：
1. 计算CF/CY每分钟的比值Z-Score（滚动120窗口）
2. 记录每晚21:00的Z-Score
3. 测量CF从21:00到次日14:50的实际运动（B021入场→出场周期）
4. 按Z-Score分组比较运动幅度
5. 模拟过滤策略：只在Z-Score安全时卖出，对比baseline

扩展到所有有宽跨策略潜力的品种对。
"""
import pandas as pd
import numpy as np
import os
import json
import time
from lead_lag_batch import load_main_contract_minutes, EXCHANGE_MAP


def calc_zscore(df_a, df_b, window=120):
    """
    计算品种A/B的比值Z-Score序列。
    返回merged DataFrame，包含datetime, close_A, close_B, ratio, z_score
    """
    merged = pd.merge(df_a[['datetime', 'close']],
                      df_b[['datetime', 'close']],
                      on='datetime', suffixes=('_A', '_B'))
    merged = merged.sort_values('datetime').reset_index(drop=True)

    merged['ratio'] = merged['close_A'] / merged['close_B']
    merged['ratio_mean'] = merged['ratio'].rolling(window).mean()
    merged['ratio_std'] = merged['ratio'].rolling(window).std()
    merged['z_score'] = (merged['ratio'] - merged['ratio_mean']) / merged['ratio_std']
    merged = merged.dropna(subset=['z_score']).reset_index(drop=True)

    return merged


def extract_overnight_sessions(df_merged, df_target, target_col='close_A'):
    """
    提取每晚21:00→次日14:50的交易周期。

    对每个周期计算：
    - entry_z: 21:00的Z-Score
    - 期货运动指标: 最大涨幅、最大跌幅、区间range、终值回报、实现波动率
    """
    df = df_merged.copy()
    df['hour'] = df['datetime'].dt.hour
    df['minute'] = df['datetime'].dt.minute
    df['date'] = df['datetime'].dt.date

    # 找到21:00附近的K线作为入场点
    # 夜盘开始于21:00
    entry_mask = (df['hour'] == 21) & (df['minute'] <= 5)
    entry_bars = df[entry_mask].copy()

    if len(entry_bars) == 0:
        return []

    # 每天只取第一根21:00 bar
    entry_bars = entry_bars.groupby('date').first().reset_index()

    sessions = []
    for _, entry in entry_bars.iterrows():
        entry_dt = entry['datetime']
        entry_z = entry['z_score']
        entry_price = entry[target_col]

        if pd.isna(entry_z) or pd.isna(entry_price) or entry_price <= 0:
            continue

        # 找次日14:50作为出场
        # 夜盘: 21:00-23:59, 次日0:00-02:30 (部分品种)
        # 日盘: 09:00-11:30, 13:30-15:00
        # 需要找入场后到次日14:50之间的所有bar

        # 获取入场后的数据，最多到次日收盘（约18小时）
        future_bars = df[(df['datetime'] > entry_dt) &
                         (df['datetime'] <= entry_dt + pd.Timedelta(hours=20))]

        if len(future_bars) < 10:
            continue

        # 找14:50附近的出场点（日盘收盘前）
        exit_mask = (future_bars['hour'] == 14) & (future_bars['minute'] >= 45)
        if len(future_bars[exit_mask]) == 0:
            # 也接受15:00
            exit_mask = (future_bars['hour'] == 15) & (future_bars['minute'] == 0)
        if len(future_bars[exit_mask]) == 0:
            continue

        exit_bar = future_bars[exit_mask].iloc[-1]
        exit_dt = exit_bar['datetime']
        exit_price = exit_bar[target_col]

        # 获取入场到出场之间的所有bar
        session_bars = df[(df['datetime'] >= entry_dt) &
                          (df['datetime'] <= exit_dt)]

        if len(session_bars) < 10:
            continue

        prices = session_bars[target_col].values
        entry_p = prices[0]

        # 计算运动指标
        returns_pct = (prices - entry_p) / entry_p * 100
        minute_returns = np.diff(prices) / prices[:-1] * 100

        max_up = returns_pct.max()
        max_down = returns_pct.min()
        total_range = max_up - max_down
        final_return = returns_pct[-1]
        abs_return = abs(final_return)
        realized_vol = np.std(minute_returns) if len(minute_returns) > 0 else 0

        # Z-Score也看出场时刻的值
        exit_z_row = df[df['datetime'] == exit_dt]
        exit_z = float(exit_z_row['z_score'].iloc[0]) if len(exit_z_row) > 0 else np.nan

        sessions.append({
            'entry_date': str(entry['date']),
            'entry_dt': str(entry_dt),
            'exit_dt': str(exit_dt),
            'entry_z': float(entry_z),
            'exit_z': float(exit_z) if not pd.isna(exit_z) else None,
            'entry_price': float(entry_p),
            'exit_price': float(exit_price),
            'n_bars': len(session_bars),
            'max_up%': round(float(max_up), 4),
            'max_down%': round(float(max_down), 4),
            'range%': round(float(total_range), 4),
            'final_return%': round(float(final_return), 4),
            'abs_return%': round(float(abs_return), 4),
            'realized_vol': round(float(realized_vol), 6),
        })

    return sessions


def analyze_by_zscore(sessions, z_buckets=None):
    """
    按Z-Score分组分析。

    分组策略:
    - |Z| < 0.5: 安全区
    - 0.5 ≤ |Z| < 1.0: 正常区
    - 1.0 ≤ |Z| < 1.5: 注意区
    - 1.5 ≤ |Z| < 2.0: 警戒区
    - |Z| ≥ 2.0: 危险区（价差极端偏离）
    """
    if z_buckets is None:
        z_buckets = [
            (0, 0.5, '|Z|<0.5 安全'),
            (0.5, 1.0, '0.5≤|Z|<1.0 正常'),
            (1.0, 1.5, '1.0≤|Z|<1.5 注意'),
            (1.5, 2.0, '1.5≤|Z|<2.0 警戒'),
            (2.0, 99, '|Z|≥2.0 危险'),
        ]

    df = pd.DataFrame(sessions)
    df['abs_z'] = df['entry_z'].abs()

    results = {}
    for lo, hi, label in z_buckets:
        mask = (df['abs_z'] >= lo) & (df['abs_z'] < hi)
        group = df[mask]
        if len(group) == 0:
            continue

        results[label] = {
            'count': len(group),
            'avg_abs_return%': round(float(group['abs_return%'].mean()), 4),
            'median_abs_return%': round(float(group['abs_return%'].median()), 4),
            'avg_range%': round(float(group['range%'].mean()), 4),
            'avg_max_up%': round(float(group['max_up%'].mean()), 4),
            'avg_max_down%': round(float(group['max_down%'].mean()), 4),
            'avg_realized_vol': round(float(group['realized_vol'].mean()), 6),
            'pct_abs_return_gt_1%': round(float((group['abs_return%'] > 1.0).mean() * 100), 1),
            'pct_abs_return_gt_2%': round(float((group['abs_return%'] > 2.0).mean() * 100), 1),
        }

    return results


def simulate_strangle_filter(sessions, z_threshold=1.5):
    """
    模拟过滤策略：
    - baseline: 每天都卖宽跨
    - filtered: 只在 |Z| < threshold 时卖宽跨

    用简化模型：宽跨P&L ∝ theta - gamma * move²
    更直觉的proxy: 使用固定premium（0.5%），如果|move| < premium则盈利

    但更好的方法是直接用B021的经验数据：
    - B021平均止盈7%, 止损约-20%
    - 胜率79%, 所以用 abs_return 作为风险指标
    - 大move(>2%)的交易日通常亏损

    我们直接统计：
    - baseline: 所有交易日的abs_return分布
    - filtered: 排除|Z|>threshold的交易日
    - 比较：大幅运动（>1%, >2%）的比例是否降低
    """
    df = pd.DataFrame(sessions)
    df['abs_z'] = df['entry_z'].abs()

    # Baseline
    n_base = len(df)
    base_avg_move = df['abs_return%'].mean()
    base_avg_range = df['range%'].mean()
    base_big_move_1 = (df['abs_return%'] > 1.0).mean() * 100
    base_big_move_2 = (df['abs_return%'] > 2.0).mean() * 100
    base_avg_vol = df['realized_vol'].mean()

    # Filtered
    filtered = df[df['abs_z'] < z_threshold]
    n_filt = len(filtered)
    if n_filt == 0:
        return None

    filt_avg_move = filtered['abs_return%'].mean()
    filt_avg_range = filtered['range%'].mean()
    filt_big_move_1 = (filtered['abs_return%'] > 1.0).mean() * 100
    filt_big_move_2 = (filtered['abs_return%'] > 2.0).mean() * 100
    filt_avg_vol = filtered['realized_vol'].mean()

    # 被过滤掉的（高Z日）
    excluded = df[df['abs_z'] >= z_threshold]
    n_excl = len(excluded)
    if n_excl > 0:
        excl_avg_move = excluded['abs_return%'].mean()
        excl_avg_range = excluded['range%'].mean()
        excl_big_move_1 = (excluded['abs_return%'] > 1.0).mean() * 100
        excl_big_move_2 = (excluded['abs_return%'] > 2.0).mean() * 100
        excl_avg_vol = excluded['realized_vol'].mean()
    else:
        excl_avg_move = excl_avg_range = excl_big_move_1 = excl_big_move_2 = excl_avg_vol = 0

    return {
        'z_threshold': z_threshold,
        'baseline': {
            'n_days': n_base,
            'avg_abs_return%': round(base_avg_move, 4),
            'avg_range%': round(base_avg_range, 4),
            'big_move_>1%': round(base_big_move_1, 1),
            'big_move_>2%': round(base_big_move_2, 1),
            'avg_realized_vol': round(base_avg_vol, 6),
        },
        'filtered': {
            'n_days': n_filt,
            'pct_kept': round(n_filt / n_base * 100, 1),
            'avg_abs_return%': round(filt_avg_move, 4),
            'avg_range%': round(filt_avg_range, 4),
            'big_move_>1%': round(filt_big_move_1, 1),
            'big_move_>2%': round(filt_big_move_2, 1),
            'avg_realized_vol': round(filt_avg_vol, 6),
        },
        'excluded': {
            'n_days': n_excl,
            'avg_abs_return%': round(excl_avg_move, 4),
            'avg_range%': round(excl_avg_range, 4),
            'big_move_>1%': round(excl_big_move_1, 1),
            'big_move_>2%': round(excl_big_move_2, 1),
            'avg_realized_vol': round(excl_avg_vol, 6),
        },
        'improvement': {
            'abs_return_reduction%': round((1 - filt_avg_move / base_avg_move) * 100, 1) if base_avg_move > 0 else 0,
            'range_reduction%': round((1 - filt_avg_range / base_avg_range) * 100, 1) if base_avg_range > 0 else 0,
            'big_move_1_reduction%': round(base_big_move_1 - filt_big_move_1, 1),
            'big_move_2_reduction%': round(base_big_move_2 - filt_big_move_2, 1),
        }
    }


def strangle_pnl_proxy(sessions, premium_pct=0.5, otm_distance_pct=3.0):
    """
    简化宽跨P&L估算。

    假设:
    - 收取权利金 = premium_pct% (of entry price)
    - OTM距离 = otm_distance_pct% (strike vs futures)
    - 如果期货终值在 [entry*(1-otm), entry*(1+otm)] 内 → 盈利 = premium
    - 如果突破 → 亏损 = |move| - otm_distance + premium (但至少-premium)

    实际是简化的但能看趋势。
    """
    df = pd.DataFrame(sessions)
    df['abs_z'] = df['entry_z'].abs()

    pnl_list = []
    for _, row in df.iterrows():
        final = row['final_return%']  # 可能正可能负
        max_adverse = max(abs(row['max_up%']), abs(row['max_down%']))

        # 如果日内最大波动没超过OTM距离 → 全额盈利
        if max_adverse <= otm_distance_pct:
            pnl = premium_pct
        else:
            # 简化: 亏损 = max_adverse - otm_distance (被穿越的部分)
            pnl = premium_pct - (max_adverse - otm_distance_pct)

        pnl_list.append(pnl)

    df['proxy_pnl'] = pnl_list

    return df


def run_full_analysis(code_a, code_b, name_a, name_b, target='A'):
    """
    对一个品种对运行完整分析。

    target: 'A' 或 'B'，表示我们在哪个品种上卖宽跨
    """
    print(f"\n{'='*90}")
    print(f"价差信号优化宽跨时机: {name_a}({code_a}) ↔ {name_b}({code_b})")
    print(f"宽跨标的: {name_a if target == 'A' else name_b}")
    print(f"{'='*90}")

    # 加载数据
    t0 = time.time()
    df_a = load_main_contract_minutes(code_a, start_year=2022)
    df_b = load_main_contract_minutes(code_b, start_year=2022)
    if df_a is None or df_b is None:
        print("  数据加载失败")
        return None
    print(f"  数据加载: {code_a}={len(df_a)}根, {code_b}={len(df_b)}根 ({time.time()-t0:.1f}s)")

    # 计算Z-Score
    merged = calc_zscore(df_a, df_b)
    if len(merged) < 500:
        print("  合并后数据不足")
        return None
    print(f"  合并后: {len(merged)}根, Z-Score范围: [{merged['z_score'].min():.2f}, {merged['z_score'].max():.2f}]")

    # 提取过夜交易周期
    target_col = 'close_A' if target == 'A' else 'close_B'
    sessions = extract_overnight_sessions(merged, None, target_col=target_col)
    if not sessions:
        print("  未提取到有效交易周期")
        return None
    print(f"  交易周期: {len(sessions)}个夜盘→日盘周期")

    # 按Z-Score分组分析
    print(f"\n  {'='*70}")
    print(f"  一、按Z-Score分组的期货运动统计")
    print(f"  {'='*70}")

    bucket_results = analyze_by_zscore(sessions)

    print(f"\n  {'Z-Score区间':<22} {'天数':>5} {'均|回报|%':>10} {'中位|回报|%':>12}"
          f" {'均range%':>10} {'大波动>1%':>10} {'大波动>2%':>10}")
    print(f"  {'-'*80}")

    for label, stats in bucket_results.items():
        print(f"  {label:<22} {stats['count']:>5}"
              f" {stats['avg_abs_return%']:>9.3f}%"
              f" {stats['median_abs_return%']:>11.3f}%"
              f" {stats['avg_range%']:>9.3f}%"
              f" {stats['pct_abs_return_gt_1%']:>9.1f}%"
              f" {stats['pct_abs_return_gt_2%']:>9.1f}%")

    # 方向性分析: Z>0时(A贵了) vs Z<0时(A便宜了)
    print(f"\n  {'='*70}")
    print(f"  二、方向性分析：Z正/负与期货运动方向")
    print(f"  {'='*70}")

    df_sess = pd.DataFrame(sessions)
    z_pos = df_sess[df_sess['entry_z'] > 0.5]  # A相对贵
    z_neg = df_sess[df_sess['entry_z'] < -0.5]  # A相对便宜
    z_neutral = df_sess[df_sess['entry_z'].abs() <= 0.5]

    for label, group in [('Z>0.5 (A偏贵)', z_pos),
                          ('Z<-0.5 (A偏便宜)', z_neg),
                          ('|Z|≤0.5 (均衡)', z_neutral)]:
        if len(group) == 0:
            continue
        avg_ret = group['final_return%'].mean()
        pct_up = (group['final_return%'] > 0).mean() * 100
        pct_down = (group['final_return%'] < 0).mean() * 100
        print(f"  {label:<22} {len(group):>4}天  均回报{avg_ret:>+.3f}%"
              f"  涨{pct_up:.1f}%/跌{pct_down:.1f}%")

    # 回归效应: Z偏高时A后续倾向下跌(回归), Z偏低时A后续倾向上涨
    print(f"\n  回归含义: Z>0时A偏贵→如果后续A跌(回归), 卖A的Call端有利、Put端不利")
    print(f"  回归含义: Z<0时A偏便宜→如果后续A涨(回归), 卖A的Put端有利、Call端不利")

    # 过滤策略模拟
    print(f"\n  {'='*70}")
    print(f"  三、过滤策略模拟")
    print(f"  {'='*70}")

    for z_thr in [1.0, 1.5, 2.0]:
        sim = simulate_strangle_filter(sessions, z_threshold=z_thr)
        if sim is None:
            continue
        b = sim['baseline']
        f = sim['filtered']
        e = sim['excluded']
        imp = sim['improvement']

        print(f"\n  Z阈值={z_thr}:")
        print(f"    baseline: {b['n_days']}天, 均|move|={b['avg_abs_return%']:.3f}%,"
              f" 大波动>1%={b['big_move_>1%']:.1f}%, >2%={b['big_move_>2%']:.1f}%")
        print(f"    filtered: {f['n_days']}天({f['pct_kept']:.1f}%), 均|move|={f['avg_abs_return%']:.3f}%,"
              f" 大波动>1%={f['big_move_>1%']:.1f}%, >2%={f['big_move_>2%']:.1f}%")
        print(f"    excluded: {e['n_days']}天, 均|move|={e['avg_abs_return%']:.3f}%,"
              f" 大波动>1%={e['big_move_>1%']:.1f}%, >2%={e['big_move_>2%']:.1f}%")
        print(f"    改善: |move|减少{imp['abs_return_reduction%']:.1f}%,"
              f" 大波动>1%减少{imp['big_move_1_reduction%']:.1f}pp,"
              f" >2%减少{imp['big_move_2_reduction%']:.1f}pp")

    # 宽跨P&L proxy
    print(f"\n  {'='*70}")
    print(f"  四、简化宽跨P&L估算 (权利金0.5%, OTM距离3%)")
    print(f"  {'='*70}")

    df_pnl = strangle_pnl_proxy(sessions, premium_pct=0.5, otm_distance_pct=3.0)

    for z_thr in [1.0, 1.5, 2.0]:
        base_pnl = df_pnl['proxy_pnl']
        filt_pnl = df_pnl[df_pnl['abs_z'] < z_thr]['proxy_pnl']
        excl_pnl = df_pnl[df_pnl['abs_z'] >= z_thr]['proxy_pnl']

        if len(filt_pnl) == 0:
            continue

        base_win = (base_pnl > 0).mean() * 100
        filt_win = (filt_pnl > 0).mean() * 100
        excl_win = (excl_pnl > 0).mean() * 100 if len(excl_pnl) > 0 else 0

        base_sharpe = base_pnl.mean() / base_pnl.std() * np.sqrt(252) if base_pnl.std() > 0 else 0
        filt_sharpe = filt_pnl.mean() / filt_pnl.std() * np.sqrt(252) if filt_pnl.std() > 0 else 0
        excl_sharpe = excl_pnl.mean() / excl_pnl.std() * np.sqrt(252) if len(excl_pnl) > 1 and excl_pnl.std() > 0 else 0

        print(f"\n  Z阈值={z_thr}:")
        print(f"    baseline: {len(base_pnl)}笔, 胜率{base_win:.1f}%,"
              f" 均PnL={base_pnl.mean():+.4f}%, 累计={base_pnl.sum():+.2f}%,"
              f" Sharpe={base_sharpe:.2f}")
        print(f"    filtered: {len(filt_pnl)}笔, 胜率{filt_win:.1f}%,"
              f" 均PnL={filt_pnl.mean():+.4f}%, 累计={filt_pnl.sum():+.2f}%,"
              f" Sharpe={filt_sharpe:.2f}")
        if len(excl_pnl) > 0:
            print(f"    excluded: {len(excl_pnl)}笔, 胜率{excl_win:.1f}%,"
                  f" 均PnL={excl_pnl.mean():+.4f}%, 累计={excl_pnl.sum():+.2f}%,"
                  f" Sharpe={excl_sharpe:.2f}")

    # 相关性分析
    print(f"\n  {'='*70}")
    print(f"  五、|Z|与运动幅度的相关性")
    print(f"  {'='*70}")

    corr_z_move = df_sess['entry_z'].abs().corr(df_sess['abs_return%'])
    corr_z_range = df_sess['entry_z'].abs().corr(df_sess['range%'])
    corr_z_vol = df_sess['entry_z'].abs().corr(df_sess['realized_vol'])

    print(f"  Pearson相关系数:")
    print(f"    |Z| vs |回报|: r = {corr_z_move:.4f}")
    print(f"    |Z| vs range:  r = {corr_z_range:.4f}")
    print(f"    |Z| vs 实现vol: r = {corr_z_vol:.4f}")

    # 分位数分析
    q_lo = df_sess[df_sess['entry_z'].abs() < df_sess['entry_z'].abs().quantile(0.25)]
    q_hi = df_sess[df_sess['entry_z'].abs() > df_sess['entry_z'].abs().quantile(0.75)]

    print(f"\n  分位数对比:")
    print(f"    低25%|Z| ({len(q_lo)}天): 均|move|={q_lo['abs_return%'].mean():.3f}%,"
          f" 均range={q_lo['range%'].mean():.3f}%")
    print(f"    高25%|Z| ({len(q_hi)}天): 均|move|={q_hi['abs_return%'].mean():.3f}%,"
          f" 均range={q_hi['range%'].mean():.3f}%")

    ratio = q_hi['abs_return%'].mean() / q_lo['abs_return%'].mean() if q_lo['abs_return%'].mean() > 0 else 0
    print(f"    高/低比: {ratio:.2f}x")

    return {
        'pair': f'{code_a}/{code_b}',
        'target': code_a if target == 'A' else code_b,
        'n_sessions': len(sessions),
        'bucket_analysis': bucket_results,
        'correlation': {
            'z_vs_abs_return': round(float(corr_z_move), 4),
            'z_vs_range': round(float(corr_z_range), 4),
            'z_vs_vol': round(float(corr_z_vol), 4),
        },
        'quantile_ratio': round(float(ratio), 2),
        'sessions_sample': sessions[:5],  # 保存前5个样本
    }


def main():
    # 要测试的品种对及其宽跨标的
    # 选择那些有实际宽跨策略潜力的品种
    test_cases = [
        # (code_A, code_B, name_A, name_B, target, daily_r)
        # CF棉花 - B021已验证的主要品种
        ('CF', 'CY', '棉花', '棉纱', 'A', 0.72),
        # SA纯碱 - B021已验证的品种
        ('SA', 'FG', '纯碱', '玻璃', 'A', 0.49),
        # 第一梯队配对(夏普>5) - 双边都测
        ('RB', 'HC', '螺纹钢', '热卷', 'A', 0.88),
        ('RB', 'HC', '螺纹钢', '热卷', 'B', 0.88),
        ('RU', 'NR', '天然橡胶', '20号胶', 'A', 0.84),
        ('TA', 'PF', 'PTA', '短纤', 'A', 0.84),
        ('J', 'JM', '焦炭', '焦煤', 'A', 0.73),
        ('J', 'JM', '焦炭', '焦煤', 'B', 0.73),
        # 其他有期权的品种
        ('CU', 'ZN', '铜', '锌', 'A', 0.72),
        ('AU', 'AG', '黄金', '白银', 'A', 0.76),
        ('P', 'Y', '棕榈油', '豆油', 'A', 0.82),
        ('M', 'RM', '豆粕', '菜粕', 'A', 0.59),
        ('I', 'RB', '铁矿石', '螺纹钢', 'A', 0.68),
        ('PP', 'L', '聚丙烯', 'LLDPE', 'A', 0.80),
    ]

    print("=" * 110)
    print("价差信号优化宽跨卖出时机 - 系统性测试")
    print("假说: 品种间Z-Score偏离时，标的品种波动增大，不适合卖宽跨")
    print("=" * 110)

    all_results = []
    cache = {}  # 缓存已加载的数据

    for code_a, code_b, name_a, name_b, target, daily_r in test_cases:
        result = run_full_analysis(code_a, code_b, name_a, name_b, target=target)
        if result:
            result['daily_r'] = daily_r
            all_results.append(result)

    # === 汇总 ===
    print("\n" + "=" * 110)
    print("【汇总】各品种对的Z-Score预测能力")
    print("-" * 110)
    print(f"{'品种对':<18} {'标的':>4} {'r':>5} {'天数':>6}"
          f" {'|Z|<0.5均move':>14} {'|Z|>2均move':>14} {'比值':>6}"
          f" {'相关系数':>8} {'高Z过滤后改善':>14}")
    print("-" * 110)

    for r in all_results:
        pair = r['pair']
        tgt = r['target']
        n = r['n_sessions']
        dr = r['daily_r']

        # 安全区和危险区的均move
        safe = r['bucket_analysis'].get('|Z|<0.5 安全', {})
        danger = r['bucket_analysis'].get('|Z|≥2.0 危险', {})

        safe_move = safe.get('avg_abs_return%', 0)
        danger_move = danger.get('avg_abs_return%', 0)
        ratio = danger_move / safe_move if safe_move > 0 else 0

        corr = r['correlation']['z_vs_abs_return']

        print(f"  {pair:<16} {tgt:>4} {dr:>5.2f} {n:>5}"
              f" {safe_move:>13.3f}% {danger_move:>13.3f}% {ratio:>5.1f}x"
              f" {corr:>+7.4f} {r['quantile_ratio']:>5.1f}x Q3/Q1")

    # 保存结果
    out_path = os.path.expanduser("~/Scripts/spread_strangle_timing_results.json")
    with open(out_path, 'w') as f:
        json.dump({
            '测试参数': {
                'Z_Score滚动窗口': 120,
                '数据起始年': 2022,
                '交易周期': '21:00→次日14:50',
            },
            '品种结果': all_results,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存: {out_path}")


if __name__ == '__main__':
    main()
