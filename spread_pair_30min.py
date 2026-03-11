"""
半小时级别配对交易回测（含手续费+滑点）

与分钟级的核心区别：
- 1分钟K线重采样为30分钟K线
- Z-Score滚动窗口=40根（约5个交易日）
- 最大持仓=16根（8小时）
- 每笔回归利润应更大，看能否覆盖成本
"""
import pandas as pd
import numpy as np
import os
import json
import time
from lead_lag_batch import load_main_contract_minutes, EXCHANGE_MAP
from spread_pair_cost import COST_PARAMS, calc_pair_cost_pct


def resample_to_30min(df):
    """将1分钟K线重采样为30分钟K线"""
    df = df.copy()
    df = df.set_index('datetime')
    # 用close的最后一个值，volume求和
    resampled = df['close'].resample('30min').last()
    resampled = resampled.dropna()
    out = resampled.reset_index()
    out.columns = ['datetime', 'close']
    return out


def backtest_pair_30min(df_a, df_b, code_a, code_b,
                        window=40, z_entry=2.0, z_exit=0.0, z_stop=3.5,
                        max_hold=16, daily_close=True, cost_pct=None):
    """30分钟级配对交易回测"""

    # 重采样
    a30 = resample_to_30min(df_a)
    b30 = resample_to_30min(df_b)

    merged = pd.merge(a30, b30, on='datetime', suffixes=('_A', '_B'))

    if len(merged) < window + 100:
        return None, None

    merged = merged.sort_values('datetime').reset_index(drop=True)
    merged['ratio'] = merged['close_A'] / merged['close_B']
    merged['ratio_mean'] = merged['ratio'].rolling(window).mean()
    merged['ratio_std'] = merged['ratio'].rolling(window).std()
    merged['z'] = (merged['ratio'] - merged['ratio_mean']) / merged['ratio_std']
    merged['date'] = merged['datetime'].dt.date
    merged = merged.dropna(subset=['z']).reset_index(drop=True)

    if cost_pct is None:
        cost_pct = calc_pair_cost_pct(code_a, code_b)

    trades = []
    position = None
    cooldown = 0

    for i in range(len(merged)):
        row = merged.iloc[i]
        z = row['z']
        dt = row['datetime']
        price_a = row['close_A']
        price_b = row['close_B']

        if position is not None:
            hold_bars = i - position['entry_idx']
            cur_date = row['date']

            if position['direction'] == 'short_A_long_B':
                pnl_a = (position['entry_price_A'] - price_a) / position['entry_price_A'] * 100
                pnl_b = (price_b - position['entry_price_B']) / position['entry_price_B'] * 100
            else:
                pnl_a = (price_a - position['entry_price_A']) / position['entry_price_A'] * 100
                pnl_b = (position['entry_price_B'] - price_b) / position['entry_price_B'] * 100

            total_pnl = pnl_a + pnl_b

            if total_pnl > position.get('max_pnl', 0):
                position['max_pnl'] = total_pnl
            if total_pnl < position.get('min_pnl', 0):
                position['min_pnl'] = total_pnl

            exit_reason = None
            if position['direction'] == 'short_A_long_B':
                if z <= z_exit:
                    exit_reason = '回归获利'
                elif z >= z_stop:
                    exit_reason = '止损'
            else:
                if z >= -z_exit:
                    exit_reason = '回归获利'
                elif z <= -z_stop:
                    exit_reason = '止损'

            if hold_bars >= max_hold:
                exit_reason = '超时平仓'

            if daily_close and i + 1 < len(merged):
                next_date = merged.iloc[i + 1]['date']
                if next_date != cur_date:
                    exit_reason = '日内平仓'

            if exit_reason:
                net_pnl = total_pnl - cost_pct
                trades.append({
                    '入场时间': str(position['entry_time']),
                    '出场时间': str(dt),
                    '持仓bars': hold_bars,
                    '持仓分钟': hold_bars * 30,
                    '方向': position['direction'],
                    '入场Z': round(position['entry_z'], 3),
                    '出场Z': round(float(z), 3),
                    '毛盈亏%': round(total_pnl, 4),
                    '成本%': round(cost_pct, 4),
                    '净盈亏%': round(net_pnl, 4),
                    '最大浮盈%': round(position.get('max_pnl', 0), 4),
                    '最大浮亏%': round(position.get('min_pnl', 0), 4),
                    '出场原因': exit_reason,
                })
                position = None
                cooldown = 2  # 冷却2根30分钟bar = 1小时
                continue

        if position is None and cooldown <= 0:
            if z > z_entry:
                position = {
                    'direction': 'short_A_long_B',
                    'entry_time': dt, 'entry_idx': i,
                    'entry_z': float(z),
                    'entry_price_A': float(price_a),
                    'entry_price_B': float(price_b),
                    'max_pnl': 0, 'min_pnl': 0,
                }
            elif z < -z_entry:
                position = {
                    'direction': 'long_A_short_B',
                    'entry_time': dt, 'entry_idx': i,
                    'entry_z': float(z),
                    'entry_price_A': float(price_a),
                    'entry_price_B': float(price_b),
                    'max_pnl': 0, 'min_pnl': 0,
                }

        if cooldown > 0:
            cooldown -= 1

    return trades, len(merged)


def calc_metrics(trades, pnl_col='净盈亏%'):
    if not trades:
        return None
    df = pd.DataFrame(trades)
    n = len(df)
    pnl = df[pnl_col]
    wins = pnl[pnl > 0]
    losses = pnl[pnl <= 0]

    total = pnl.sum()
    avg = pnl.mean()
    win_rate = (pnl > 0).mean() * 100
    avg_win = wins.mean() if len(wins) > 0 else 0
    avg_loss = losses.mean() if len(losses) > 0 else 0
    pf = abs(wins.sum() / losses.sum()) if len(losses) > 0 and losses.sum() != 0 else float('inf')

    cumulative = pnl.cumsum()
    max_dd = (cumulative - cumulative.cummax()).min()

    max_consec = 0
    cur = 0
    for p in pnl:
        if p <= 0:
            cur += 1
            max_consec = max(max_consec, cur)
        else:
            cur = 0

    sharpe = avg / pnl.std() * np.sqrt(252 * 4) if pnl.std() > 0 else 0

    exit_stats = {}
    for reason, group in df.groupby('出场原因'):
        exit_stats[reason] = {
            '次数': len(group),
            '平均盈亏': round(float(group[pnl_col].mean()), 4),
            '胜率': round(float((group[pnl_col] > 0).mean() * 100), 1),
        }

    return {
        '交易次数': n,
        '胜率%': round(win_rate, 1),
        '总盈亏%': round(total, 2),
        '平均盈亏%': round(avg, 4),
        '平均盈利%': round(float(avg_win), 4),
        '平均亏损%': round(float(avg_loss), 4),
        '利润因子': round(pf, 2),
        '最大回撤%': round(max_dd, 2),
        '最大连亏': max_consec,
        '平均持仓分钟': round(df['持仓分钟'].mean(), 0),
        '夏普比率': round(sharpe, 2),
        '按出场原因': exit_stats,
    }


def main():
    pairs = [
        ('RB', 'HC', '螺纹钢', '热卷', 0.88),
        ('RU', 'NR', '天然橡胶', '20号胶', 0.84),
        ('TA', 'PF', 'PTA', '短纤', 0.84),
        ('J', 'JM', '焦炭', '焦煤', 0.73),
        ('PP', 'L', '聚丙烯', 'LLDPE', 0.80),
        ('CF', 'CY', '棉花', '棉纱', 0.72),
        ('SC', 'LU', '原油', '低硫燃料油', 0.78),
        ('I', 'RB', '铁矿石', '螺纹钢', 0.68),
        ('NR', 'BR', '20号胶', '合成橡胶', 0.68),
        ('P', 'Y', '棕榈油', '豆油', 0.82),
        ('AU', 'AG', '黄金', '白银', 0.76),
        ('SC', 'FU', '原油', '燃料油', 0.73),
        ('CU', 'ZN', '铜', '锌', 0.72),
        ('M', 'RM', '豆粕', '菜粕', 0.59),
        ('EG', 'PF', '乙二醇', '短纤', 0.63),
        ('Y', 'OI', '豆油', '菜油', 0.72),
        ('SA', 'FG', '纯碱', '玻璃', 0.49),
    ]

    # 测试不同参数组合
    param_sets = [
        {'window': 20, 'z_entry': 2.0, 'z_stop': 3.5, 'max_hold': 16, 'label': 'W20_Z2.0_快'},
        {'window': 40, 'z_entry': 2.0, 'z_stop': 3.5, 'max_hold': 16, 'label': 'W40_Z2.0_标准'},
        {'window': 40, 'z_entry': 2.5, 'z_stop': 4.0, 'max_hold': 32, 'label': 'W40_Z2.5_宽松'},
        {'window': 80, 'z_entry': 2.0, 'z_stop': 3.5, 'max_hold': 32, 'label': 'W80_Z2.0_长窗口'},
    ]

    print("=" * 120)
    print("30分钟级配对交易回测（含手续费+滑点）")
    print("关键问题：拉长周期后，回归利润能否覆盖成本？")
    print("=" * 120)

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
            print(f"  {code}: {len(df)}根1分钟 ({elapsed:.1f}s)")
        else:
            print(f"  {code}: 失败")

    # ============================================================
    # 先用标准参数(W40)跑所有品种对，看整体情况
    # ============================================================
    print("\n" + "=" * 120)
    print("一、标准参数（窗口40、Z入2.0、止损3.5、最大持仓16根=8小时）")
    print("=" * 120)

    all_results = []
    for code_a, code_b, na, nb, daily_r in pairs:
        if code_a not in cache or code_b not in cache:
            continue

        cost_pct = calc_pair_cost_pct(code_a, code_b)
        trades, n_bars = backtest_pair_30min(
            cache[code_a], cache[code_b], code_a, code_b,
            window=40, z_entry=2.0, z_stop=3.5, max_hold=16,
            cost_pct=cost_pct
        )

        if not trades:
            print(f"  {na}↔{nb}: 无交易")
            continue

        gross = calc_metrics(trades, '毛盈亏%')
        net = calc_metrics(trades, '净盈亏%')

        print(f"\n  {na}({code_a})↔{nb}({code_b})  r={daily_r}  30分K线={n_bars}根  成本={cost_pct:.3f}%/笔")
        print(f"    {'':14} {'毛利':>12} {'净利':>12}")
        print(f"    {'交易次数':<12} {gross['交易次数']:>12}")
        print(f"    {'胜率':<14} {gross['胜率%']:>11.1f}% {net['胜率%']:>11.1f}%")
        print(f"    {'总盈亏':<12} {gross['总盈亏%']:>+11.2f}% {net['总盈亏%']:>+11.2f}%")
        print(f"    {'平均盈亏':<11} {gross['平均盈亏%']:>+11.4f}% {net['平均盈亏%']:>+11.4f}%")
        print(f"    {'利润因子':<11} {gross['利润因子']:>12.2f} {net['利润因子']:>12.2f}")
        print(f"    {'夏普比率':<11} {gross['夏普比率']:>12.2f} {net['夏普比率']:>12.2f}")
        print(f"    {'最大回撤':<11} {gross['最大回撤%']:>+11.2f}% {net['最大回撤%']:>+11.2f}%")
        print(f"    {'均持仓':<13} {gross['平均持仓分钟']:>10.0f}分钟")

        for reason, stats in net['按出场原因'].items():
            print(f"      {reason}: {stats['次数']}笔, 均净PnL{stats['平均盈亏']:+.4f}%, 胜率{stats['胜率']:.1f}%")

        all_results.append({
            '品种A': code_a, '名称A': na,
            '品种B': code_b, '名称B': nb,
            'r': daily_r,
            '成本%': round(cost_pct, 4),
            '30分K线数': n_bars,
            '毛利': {k: v for k, v in gross.items() if k != '按出场原因'},
            '净利': {k: v for k, v in net.items() if k != '按出场原因'},
        })

    # 汇总
    print("\n" + "=" * 120)
    print("【30分钟标准参数汇总】")
    print("-" * 120)
    print(f"{'排名':>3} {'品种对':<20} {'r':>5} {'笔数':>5} {'成本/笔':>8}"
          f" {'毛均PnL':>10} {'净均PnL':>10} {'毛胜率':>7} {'净胜率':>7}"
          f" {'净总盈亏':>10} {'净夏普':>7} {'净回撤':>8} {'OK':>3}")
    print("-" * 120)

    sorted_results = sorted(all_results, key=lambda x: x['净利']['总盈亏%'], reverse=True)
    n_profitable = 0

    for rank, r in enumerate(sorted_results, 1):
        pair = f"{r['名称A']}↔{r['名称B']}"
        g = r['毛利']
        n = r['净利']
        ok = n['总盈亏%'] > 0
        if ok:
            n_profitable += 1
        marker = "✓" if ok else "✗"

        print(f" {rank:>2}  {pair:<18} {r['r']:>5.2f} {g['交易次数']:>5} {r['成本%']:>7.3f}%"
              f" {g['平均盈亏%']:>+9.4f}% {n['平均盈亏%']:>+9.4f}%"
              f" {g['胜率%']:>6.1f}% {n['胜率%']:>6.1f}%"
              f" {n['总盈亏%']:>+9.1f}% {n['夏普比率']:>6.2f} {n['最大回撤%']:>+7.1f}% {marker:>3}")

    total_gross = sum(r['毛利']['总盈亏%'] for r in all_results)
    total_net = sum(r['净利']['总盈亏%'] for r in all_results)
    total_trades = sum(r['毛利']['交易次数'] for r in all_results)
    print("-" * 120)
    print(f"  总计: {total_trades}笔, 毛利{total_gross:+.1f}%, 净利{total_net:+.1f}%,"
          f" 净盈利{n_profitable}/{len(all_results)}")

    # ============================================================
    # 参数敏感性：用前3名品种对测试不同参数
    # ============================================================
    top3_pairs = sorted_results[:3] if len(sorted_results) >= 3 else sorted_results

    print("\n" + "=" * 120)
    print("二、参数敏感性（前3名品种对 × 4组参数）")
    print("=" * 120)

    for r in top3_pairs:
        code_a = r['品种A']
        code_b = r['品种B']
        na = r['名称A']
        nb = r['名称B']
        cost_pct = calc_pair_cost_pct(code_a, code_b)

        print(f"\n  {na}({code_a}) ↔ {nb}({code_b})  成本={cost_pct:.3f}%/笔")
        print(f"  {'参数':<22} {'笔数':>5} {'毛均PnL':>10} {'净均PnL':>10}"
              f" {'毛胜率':>7} {'净胜率':>7} {'净总盈亏':>10} {'净夏普':>7}")
        print(f"  {'-'*85}")

        for ps in param_sets:
            trades, _ = backtest_pair_30min(
                cache[code_a], cache[code_b], code_a, code_b,
                window=ps['window'], z_entry=ps['z_entry'],
                z_stop=ps['z_stop'], max_hold=ps['max_hold'],
                cost_pct=cost_pct
            )
            if not trades:
                print(f"  {ps['label']:<22} 无交易")
                continue

            gross = calc_metrics(trades, '毛盈亏%')
            net = calc_metrics(trades, '净盈亏%')

            print(f"  {ps['label']:<22} {gross['交易次数']:>5}"
                  f" {gross['平均盈亏%']:>+9.4f}% {net['平均盈亏%']:>+9.4f}%"
                  f" {gross['胜率%']:>6.1f}% {net['胜率%']:>6.1f}%"
                  f" {net['总盈亏%']:>+9.1f}% {net['夏普比率']:>6.2f}")

    # ============================================================
    # 对比：1分钟 vs 30分钟 vs 关键指标
    # ============================================================
    print("\n" + "=" * 120)
    print("三、1分钟 vs 30分钟 关键对比")
    print("-" * 120)
    print(f"  {'品种对':<20} {'1分钟毛均PnL':>14} {'30分毛均PnL':>14} {'放大倍数':>8}"
          f" {'成本':>8} {'30分净均PnL':>14} {'能否覆盖':>8}")
    print("-" * 120)

    # 1分钟的数据从已有JSON读取
    try:
        with open(os.path.expanduser("~/Scripts/spread_pair_backtest_results.json")) as f:
            min1_data = json.load(f)
        min1_map = {}
        for item in min1_data['品种对结果']:
            key = f"{item['品种A']}/{item['品种B']}"
            min1_map[key] = item['指标']['平均盈亏%']
    except Exception:
        min1_map = {}

    for r in sorted_results:
        pair = f"{r['名称A']}↔{r['名称B']}"
        key = f"{r['品种A']}/{r['品种B']}"
        min1_avg = min1_map.get(key, 0)
        min30_avg_gross = r['毛利']['平均盈亏%']
        min30_avg_net = r['净利']['平均盈亏%']
        cost = r['成本%']

        ratio = min30_avg_gross / min1_avg if min1_avg != 0 else 0
        covered = "✓" if min30_avg_net > 0 else "✗"

        print(f"  {pair:<18} {min1_avg:>+13.4f}% {min30_avg_gross:>+13.4f}% {ratio:>7.1f}x"
              f" {cost:>7.3f}% {min30_avg_net:>+13.4f}% {covered:>7}")

    # 保存
    out_path = os.path.expanduser("~/Scripts/spread_pair_30min_results.json")
    with open(out_path, 'w') as f:
        json.dump({
            '说明': '30分钟级配对交易回测（含手续费+滑点）',
            '标准参数': {
                '周期': '30分钟',
                '滚动窗口': 40,
                'Z入场': 2.0,
                'Z出场': 0.0,
                'Z止损': 3.5,
                '最大持仓': '16根=8小时',
                '成本模型': '手续费+1tick滑点×4腿',
            },
            '品种结果': all_results,
            '统计': {
                '总交易笔数': total_trades,
                '毛总盈亏%': round(total_gross, 1),
                '净总盈亏%': round(total_net, 1),
                '净盈利品种数': n_profitable,
            }
        }, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存: {out_path}")


if __name__ == '__main__':
    main()
