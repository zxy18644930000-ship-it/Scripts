"""
分钟级引领性测试V2：黄金(AU) → 白银(AG) 动量拖拽效应

核心问题：
当黄金在上涨，但白银在小幅下跌时，
黄金的上涨能不能"刹住"白银的跌势，让它停止下跌、转为横盘或反涨？

测试设计：
- 实验组：黄金上涨趋势中 + 白银正在小跌 → 观察白银后续动量变化
- 对照组：黄金没有明确趋势 + 白银同样在小跌 → 观察白银后续动量变化
- 对比两组的差异，看黄金的趋势是否对白银有"拖拽/刹车"作用

信号条件（上涨方向）：
- 黄金：过去10根K线涨幅 > 0.05%（明确在涨）
- 白银：过去10根K线涨幅 < -0.02%（在小跌）
- 实验组额外条件：黄金 close > MA20 且 MA5 > MA10（趋势确认）

衡量指标：
- 白银后续N分钟的涨幅（是否止跌）
- 白银动量变化 = 后续涨幅 - 之前跌幅（正值=动量改善）
- 止跌率 = 后续涨幅 >= 0 的比例
"""
import pandas as pd
import numpy as np
import os
import glob
import json
from pathlib import Path

LOCAL = os.path.expanduser("~/Downloads/期货数据_parquet/SHFE")
USB = "/Volumes/Lexar/期货数据备份/期货_全部数据/SHFE"


def load_main_contract_minutes(code, start_year=2022):
    """加载品种主力合约的1分钟K线"""
    parquet_path = os.path.join(LOCAL, f"{code}.parquet")

    if os.path.exists(parquet_path):
        print(f"  从本地parquet加载 {code}...")
        df = pd.read_parquet(parquet_path)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df[df['datetime'].dt.year >= start_year]
    else:
        print(f"  从移动硬盘CSV加载 {code}...")
        csv_dir = os.path.join(USB, code)
        csv_files = sorted(glob.glob(os.path.join(csv_dir, "*.csv")))
        dfs = []
        for f in csv_files:
            stem = Path(f).stem
            digits = ''.join(c for c in stem if c.isdigit())
            if len(digits) >= 4:
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
        df = df.rename(columns={'money': 'turnover', 'open_interest': 'close_oi'})

    df = df[df['volume'] > 0].copy()
    df['date'] = df['datetime'].dt.date

    daily_vol = df.groupby(['date', 'symbol'])['volume'].sum().reset_index()
    idx = daily_vol.groupby('date')['volume'].idxmax()
    main_contracts = daily_vol.loc[idx, ['date', 'symbol']].set_index('date')['symbol']

    result = []
    for date, main_sym in main_contracts.items():
        day_data = df[(df['date'] == date) & (df['symbol'] == main_sym)].copy()
        result.append(day_data)

    out = pd.concat(result, ignore_index=True)
    out = out.sort_values('datetime').reset_index(drop=True)
    print(f"  {code}: {len(out)}根K线, {out['datetime'].iloc[0]} ~ {out['datetime'].iloc[-1]}")
    return out[['datetime', 'open', 'high', 'low', 'close', 'volume']].copy()


def add_indicators(df):
    """加均线和短期动量"""
    c = df['close'].astype(float)
    df['ma5'] = c.rolling(5).mean()
    df['ma10'] = c.rolling(10).mean()
    df['ma20'] = c.rolling(20).mean()
    # 过去10根K线的涨幅
    df['mom10'] = c.pct_change(10) * 100
    # 过去5根K线的涨幅
    df['mom5'] = c.pct_change(5) * 100
    return df


def test_drag_effect(leader_df, follower_df,
                     look_ahead_bars=[5, 10, 15, 30, 60],
                     leader_mom_threshold=0.05,
                     follower_mom_threshold=-0.02):
    """
    测试动量拖拽效应。

    实验组：leader在涨（趋势确认）+ follower在跌
    对照组：leader无明确趋势 + follower同样在跌

    返回两组的统计对比
    """
    leader_df = leader_df.set_index('datetime')
    follower_df = follower_df.set_index('datetime')
    common_times = leader_df.index.intersection(follower_df.index)
    leader = leader_df.loc[common_times].copy()
    follower = follower_df.loc[common_times].copy()
    print(f"  对齐后: {len(common_times)}根共同K线")

    experiment_signals = []  # 黄金涨+白银跌
    control_signals = []     # 黄金横盘+白银跌

    cooldown_exp = 0
    cooldown_ctrl = 0

    for i in range(25, len(common_times)):
        dt = common_times[i]
        l = leader.iloc[i]
        f = follower.iloc[i]

        # 白银必须在小跌（过去10分钟涨幅 < threshold）
        if pd.isna(f['mom10']) or f['mom10'] >= follower_mom_threshold:
            if cooldown_exp > 0:
                cooldown_exp -= 1
            if cooldown_ctrl > 0:
                cooldown_ctrl -= 1
            continue

        follower_prior_mom = float(f['mom10'])

        # === 收集后续表现 ===
        future = {}
        for bars in look_ahead_bars:
            if i + bars < len(common_times):
                future_price = follower.iloc[i + bars]['close']
                entry_price = f['close']
                ret = (future_price - entry_price) / entry_price * 100
                future[f'{bars}分钟后涨幅'] = round(float(ret), 4)
                # 动量变化 = 后续涨幅 - 之前跌幅的绝对值方向
                future[f'{bars}分钟止跌'] = bool(future_price >= entry_price)

        # === 判断黄金状态 ===
        leader_rising = (not pd.isna(l['mom10']) and
                         l['mom10'] > leader_mom_threshold and
                         l['close'] > l['ma20'] and
                         l['ma5'] > l['ma10'])

        leader_flat = (not pd.isna(l['mom10']) and
                       abs(l['mom10']) <= leader_mom_threshold)

        if leader_rising and cooldown_exp <= 0:
            experiment_signals.append({
                '时间': str(dt),
                '黄金动量': round(float(l['mom10']), 4),
                '白银动量': round(follower_prior_mom, 4),
                '白银价格': round(float(f['close']), 2),
                **future,
            })
            cooldown_exp = 15  # 冷却15分钟

        elif leader_flat and cooldown_ctrl <= 0:
            control_signals.append({
                '时间': str(dt),
                '黄金动量': round(float(l['mom10']), 4),
                '白银动量': round(follower_prior_mom, 4),
                '白银价格': round(float(f['close']), 2),
                **future,
            })
            cooldown_ctrl = 15

        if cooldown_exp > 0:
            cooldown_exp -= 1
        if cooldown_ctrl > 0:
            cooldown_ctrl -= 1

    return experiment_signals, control_signals


def test_reverse_drag(leader_df, follower_df,
                      look_ahead_bars=[5, 10, 15, 30, 60],
                      leader_mom_threshold=-0.05,
                      follower_mom_threshold=0.02):
    """
    反向测试：黄金在跌 + 白银在小涨 → 白银涨势会不会被拖下来？
    """
    leader_df = leader_df.set_index('datetime')
    follower_df = follower_df.set_index('datetime')
    common_times = leader_df.index.intersection(follower_df.index)
    leader = leader_df.loc[common_times].copy()
    follower = follower_df.loc[common_times].copy()

    experiment_signals = []
    control_signals = []
    cooldown_exp = 0
    cooldown_ctrl = 0

    for i in range(25, len(common_times)):
        dt = common_times[i]
        l = leader.iloc[i]
        f = follower.iloc[i]

        # 白银必须在小涨
        if pd.isna(f['mom10']) or f['mom10'] <= follower_mom_threshold:
            if cooldown_exp > 0:
                cooldown_exp -= 1
            if cooldown_ctrl > 0:
                cooldown_ctrl -= 1
            continue

        follower_prior_mom = float(f['mom10'])

        future = {}
        for bars in look_ahead_bars:
            if i + bars < len(common_times):
                future_price = follower.iloc[i + bars]['close']
                entry_price = f['close']
                ret = (future_price - entry_price) / entry_price * 100
                future[f'{bars}分钟后涨幅'] = round(float(ret), 4)
                future[f'{bars}分钟被拖跌'] = bool(future_price < entry_price)

        leader_falling = (not pd.isna(l['mom10']) and
                          l['mom10'] < leader_mom_threshold and
                          l['close'] < l['ma20'] and
                          l['ma5'] < l['ma10'])

        leader_flat = (not pd.isna(l['mom10']) and
                       abs(l['mom10']) <= abs(leader_mom_threshold))

        if leader_falling and cooldown_exp <= 0:
            experiment_signals.append({
                '时间': str(dt),
                '黄金动量': round(float(l['mom10']), 4),
                '白银动量': round(follower_prior_mom, 4),
                '白银价格': round(float(f['close']), 2),
                **future,
            })
            cooldown_exp = 15

        elif leader_flat and cooldown_ctrl <= 0:
            control_signals.append({
                '时间': str(dt),
                '黄金动量': round(float(l['mom10']), 4),
                '白银动量': round(follower_prior_mom, 4),
                '白银价格': round(float(f['close']), 2),
                **future,
            })
            cooldown_ctrl = 15

        if cooldown_exp > 0:
            cooldown_exp -= 1
        if cooldown_ctrl > 0:
            cooldown_ctrl -= 1

    return experiment_signals, control_signals


def print_comparison(exp_signals, ctrl_signals, direction='上涨拖拽',
                     bars_list=[5, 10, 15, 30, 60]):
    """对比实验组和对照组"""
    if not exp_signals:
        print("  实验组无信号")
        return
    if not ctrl_signals:
        print("  对照组无信号")
        return

    exp_df = pd.DataFrame(exp_signals)
    ctrl_df = pd.DataFrame(ctrl_signals)

    print(f"  实验组（黄金有趋势）: {len(exp_df)}个信号")
    print(f"  对照组（黄金横盘）  : {len(ctrl_df)}个信号")
    print(f"  实验组白银平均前期动量: {exp_df['白银动量'].mean():.4f}%")
    print(f"  对照组白银平均前期动量: {ctrl_df['白银动量'].mean():.4f}%")
    print()

    stop_key = '止跌' if '上涨' in direction else '被拖跌'

    print(f"  {'时间窗口':<12} {'实验组均涨幅':>12} {'对照组均涨幅':>12} {'差值(拖拽效应)':>14}"
          f" {'实验组止跌率':>12} {'对照组止跌率':>12} {'差值':>8}")
    print("  " + "-" * 90)

    for bars in bars_list:
        col_ret = f'{bars}分钟后涨幅'
        col_stop = f'{bars}分钟{stop_key}'

        if col_ret not in exp_df.columns:
            continue

        exp_ret = exp_df[col_ret].dropna()
        ctrl_ret = ctrl_df[col_ret].dropna()

        exp_avg = exp_ret.mean() if len(exp_ret) > 0 else 0
        ctrl_avg = ctrl_ret.mean() if len(ctrl_ret) > 0 else 0
        diff_ret = exp_avg - ctrl_avg

        if col_stop in exp_df.columns and col_stop in ctrl_df.columns:
            exp_stop = exp_df[col_stop].dropna()
            ctrl_stop = ctrl_df[col_stop].dropna()
            exp_rate = exp_stop.sum() / len(exp_stop) * 100 if len(exp_stop) > 0 else 0
            ctrl_rate = ctrl_stop.sum() / len(ctrl_stop) * 100 if len(ctrl_stop) > 0 else 0
            diff_rate = exp_rate - ctrl_rate
        else:
            exp_rate = ctrl_rate = diff_rate = 0

        print(f"  {bars:>3}分钟后    {exp_avg:>+10.4f}%  {ctrl_avg:>+10.4f}%  {diff_ret:>+12.4f}%"
              f"  {exp_rate:>10.1f}%  {ctrl_rate:>10.1f}%  {diff_rate:>+6.1f}%")


def main():
    print("=" * 80)
    print("分钟级引领性测试V2：动量拖拽效应")
    print("黄金(AU)的趋势能不能'刹住'白银(AG)的反向运动？")
    print("=" * 80)

    print("\n正在加载数据（2022年至今）...")
    au = load_main_contract_minutes('AU', start_year=2022)
    ag = load_main_contract_minutes('AG', start_year=2022)

    if au is None or ag is None:
        print("数据加载失败")
        return

    au = add_indicators(au)
    ag = add_indicators(ag)

    # === 测试1：黄金涨 → 能否刹住白银的跌？ ===
    print("\n" + "=" * 80)
    print("测试1：黄金在涨，白银在小跌 → 黄金能刹住白银的跌势吗？")
    print("  实验组: AU过去10分钟涨>0.05% + 趋势确认(>MA20, MA5>MA10)")
    print("          AG过去10分钟跌<-0.02%")
    print("  对照组: AU过去10分钟横盘(|涨幅|≤0.05%)")
    print("          AG同样在跌<-0.02%")
    print("  对比: 实验组的白银是否比对照组更容易止跌？")
    print("-" * 80)
    exp_up, ctrl_up = test_drag_effect(au, ag)
    print_comparison(exp_up, ctrl_up, '上涨拖拽')

    # === 测试2：黄金跌 → 能否拖住白银的涨？ ===
    print("\n" + "=" * 80)
    print("测试2：黄金在跌，白银在小涨 → 黄金能拖住白银的涨势吗？")
    print("  实验组: AU过去10分钟跌<-0.05% + 趋势确认(<MA20, MA5<MA10)")
    print("          AG过去10分钟涨>0.02%")
    print("  对照组: AU过去10分钟横盘(|涨幅|≤0.05%)")
    print("          AG同样在涨>0.02%")
    print("  对比: 实验组的白银涨势是否比对照组更容易被打断？")
    print("-" * 80)
    exp_down, ctrl_down = test_reverse_drag(au, ag)
    print_comparison(exp_down, ctrl_down, '下跌拖拽')

    # === 不同动量阈值的敏感性测试 ===
    print("\n" + "=" * 80)
    print("敏感性分析：不同黄金动量强度下的拖拽效应（看30分钟止跌率差值）")
    print("-" * 80)
    print(f"  {'AU动量阈值':<14} {'实验组信号':>10} {'对照组信号':>10}"
          f" {'实验组30分止跌':>14} {'对照组30分止跌':>14} {'拖拽效应':>10}")
    print("  " + "-" * 78)

    for threshold in [0.03, 0.05, 0.08, 0.10, 0.15, 0.20]:
        exp, ctrl = test_drag_effect(au.copy(), ag.copy(),
                                     look_ahead_bars=[30],
                                     leader_mom_threshold=threshold)
        if exp and ctrl:
            exp_df = pd.DataFrame(exp)
            ctrl_df = pd.DataFrame(ctrl)
            col = '30分钟止跌'
            if col in exp_df.columns and col in ctrl_df.columns:
                exp_rate = exp_df[col].dropna().mean() * 100
                ctrl_rate = ctrl_df[col].dropna().mean() * 100
                diff = exp_rate - ctrl_rate
                print(f"  AU>{threshold:.2f}%      {len(exp_df):>10}  {len(ctrl_df):>10}"
                      f"  {exp_rate:>12.1f}%  {ctrl_rate:>12.1f}%  {diff:>+8.1f}%")

    # 保存结果
    class NumpyEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (np.bool_, bool)):
                return int(obj)
            if isinstance(obj, (np.integer,)):
                return int(obj)
            if isinstance(obj, (np.floating,)):
                return float(obj)
            return super().default(obj)

    out_path = os.path.expanduser("~/Scripts/lead_lag_momentum_AU_AG.json")
    with open(out_path, 'w') as f:
        json.dump({
            '上涨拖拽_实验组': exp_up,
            '上涨拖拽_对照组': ctrl_up,
            '下跌拖拽_实验组': exp_down,
            '下跌拖拽_对照组': ctrl_down,
        }, f, ensure_ascii=False, indent=2, cls=NumpyEncoder)
    print(f"\n结果已保存: {out_path}")


if __name__ == '__main__':
    main()
