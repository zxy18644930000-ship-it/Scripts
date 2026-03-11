"""
价差回归配对交易回测

策略逻辑：
- 入场：比值Z-Score > 2 → 做空A+做多B（A相对贵了）
         比值Z-Score < -2 → 做多A+做空B（A相对便宜了）
- 出场条件（先触发哪个就执行哪个）：
  1. Z回归到0（获利了结）
  2. Z反向扩大到3（止损）
  3. 持仓超过60分钟（超时平仓）
  4. 当日收盘前必须平仓（不隔夜）

每笔交易按等金额配比（各投入1单位资金）
盈亏 = A端收益 + B端收益（方向对冲）
"""
import pandas as pd
import numpy as np
import os
import json
import time
from lead_lag_batch import load_main_contract_minutes, EXCHANGE_MAP


def backtest_pair(df_a, df_b, name_a, name_b,
                  window=120, z_entry=2.0, z_exit=0.0, z_stop=3.5,
                  max_hold=60, daily_close=True):
    """
    回测一个品种对的配对交易。

    参数:
    - window: 滚动窗口（计算Z-Score）
    - z_entry: 入场阈值
    - z_exit: 出场阈值（回归目标）
    - z_stop: 止损阈值（Z继续扩大）
    - max_hold: 最大持仓分钟数
    - daily_close: 是否当日收盘前强制平仓
    """
    merged = pd.merge(df_a[['datetime', 'close']],
                      df_b[['datetime', 'close']],
                      on='datetime', suffixes=('_A', '_B'))

    if len(merged) < window + 200:
        return None

    merged = merged.sort_values('datetime').reset_index(drop=True)

    # 计算比值和Z-Score
    merged['ratio'] = merged['close_A'] / merged['close_B']
    merged['ratio_mean'] = merged['ratio'].rolling(window).mean()
    merged['ratio_std'] = merged['ratio'].rolling(window).std()
    merged['z'] = (merged['ratio'] - merged['ratio_mean']) / merged['ratio_std']
    merged['date'] = merged['datetime'].dt.date

    merged = merged.dropna(subset=['z']).reset_index(drop=True)

    # 逐bar回测
    trades = []
    position = None  # None=空仓, dict=持仓信息
    cooldown = 0

    for i in range(len(merged)):
        row = merged.iloc[i]
        z = row['z']
        dt = row['datetime']
        price_a = row['close_A']
        price_b = row['close_B']

        # 有仓位时检查出场
        if position is not None:
            hold_time = i - position['entry_idx']
            cur_date = row['date']

            # 计算当前盈亏
            if position['direction'] == 'short_A_long_B':
                # A端做空收益 + B端做多收益
                pnl_a = (position['entry_price_A'] - price_a) / position['entry_price_A'] * 100
                pnl_b = (price_b - position['entry_price_B']) / position['entry_price_B'] * 100
            else:  # long_A_short_B
                pnl_a = (price_a - position['entry_price_A']) / position['entry_price_A'] * 100
                pnl_b = (position['entry_price_B'] - price_b) / position['entry_price_B'] * 100

            total_pnl = pnl_a + pnl_b

            # 跟踪最大回撤
            if total_pnl > position.get('max_pnl', 0):
                position['max_pnl'] = total_pnl
            if total_pnl < position.get('min_pnl', 0):
                position['min_pnl'] = total_pnl

            # 出场判断
            exit_reason = None

            if position['direction'] == 'short_A_long_B':
                # Z从>2进入，等Z回到exit
                if z <= z_exit:
                    exit_reason = '回归获利'
                elif z >= z_stop:
                    exit_reason = '止损'
            else:  # long_A_short_B
                # Z从<-2进入，等Z回到-exit
                if z >= -z_exit:
                    exit_reason = '回归获利'
                elif z <= -z_stop:
                    exit_reason = '止损'

            if hold_time >= max_hold:
                exit_reason = '超时平仓'

            # 当日最后一根K线（下一根换日了）
            if daily_close and i + 1 < len(merged):
                next_date = merged.iloc[i + 1]['date']
                if next_date != cur_date:
                    exit_reason = '日内平仓'

            if exit_reason:
                trade = {
                    '入场时间': str(position['entry_time']),
                    '出场时间': str(dt),
                    '持仓分钟': hold_time,
                    '方向': position['direction'],
                    '入场Z': round(position['entry_z'], 3),
                    '出场Z': round(float(z), 3),
                    '入场价A': round(position['entry_price_A'], 2),
                    '出场价A': round(float(price_a), 2),
                    '入场价B': round(position['entry_price_B'], 2),
                    '出场价B': round(float(price_b), 2),
                    'A端盈亏%': round(pnl_a, 4),
                    'B端盈亏%': round(pnl_b, 4),
                    '总盈亏%': round(total_pnl, 4),
                    '最大浮盈%': round(position.get('max_pnl', 0), 4),
                    '最大浮亏%': round(position.get('min_pnl', 0), 4),
                    '出场原因': exit_reason,
                }
                trades.append(trade)
                position = None
                cooldown = 10  # 平仓后冷却10分钟
                continue

        # 空仓时检查入场
        if position is None and cooldown <= 0:
            if z > z_entry:
                position = {
                    'direction': 'short_A_long_B',
                    'entry_time': dt,
                    'entry_idx': i,
                    'entry_z': float(z),
                    'entry_price_A': float(price_a),
                    'entry_price_B': float(price_b),
                    'max_pnl': 0,
                    'min_pnl': 0,
                }
            elif z < -z_entry:
                position = {
                    'direction': 'long_A_short_B',
                    'entry_time': dt,
                    'entry_idx': i,
                    'entry_z': float(z),
                    'entry_price_A': float(price_a),
                    'entry_price_B': float(price_b),
                    'max_pnl': 0,
                    'min_pnl': 0,
                }

        if cooldown > 0:
            cooldown -= 1

    return trades


def calc_metrics(trades):
    """计算回测指标"""
    if not trades:
        return None

    df = pd.DataFrame(trades)
    n = len(df)
    wins = df[df['总盈亏%'] > 0]
    losses = df[df['总盈亏%'] <= 0]

    total_pnl = df['总盈亏%'].sum()
    avg_pnl = df['总盈亏%'].mean()
    median_pnl = df['总盈亏%'].median()
    win_rate = len(wins) / n * 100
    avg_win = wins['总盈亏%'].mean() if len(wins) > 0 else 0
    avg_loss = losses['总盈亏%'].mean() if len(losses) > 0 else 0
    profit_factor = abs(wins['总盈亏%'].sum() / losses['总盈亏%'].sum()) if len(losses) > 0 and losses['总盈亏%'].sum() != 0 else float('inf')

    # 最大连亏
    max_consec_loss = 0
    cur_consec = 0
    for pnl in df['总盈亏%']:
        if pnl <= 0:
            cur_consec += 1
            max_consec_loss = max(max_consec_loss, cur_consec)
        else:
            cur_consec = 0

    # 累计净值曲线和最大回撤
    cumulative = df['总盈亏%'].cumsum()
    running_max = cumulative.cummax()
    drawdown = cumulative - running_max
    max_drawdown = drawdown.min()

    # 按出场原因统计
    exit_stats = df.groupby('出场原因').agg(
        次数=('总盈亏%', 'count'),
        平均盈亏=('总盈亏%', 'mean'),
        胜率=('总盈亏%', lambda x: (x > 0).mean() * 100),
    ).to_dict('index')

    avg_hold = df['持仓分钟'].mean()

    # 夏普比率（按交易计算）
    sharpe = df['总盈亏%'].mean() / df['总盈亏%'].std() * np.sqrt(252 * 4) if df['总盈亏%'].std() > 0 else 0

    return {
        '交易次数': n,
        '胜率%': round(win_rate, 1),
        '总盈亏%': round(total_pnl, 2),
        '平均盈亏%': round(avg_pnl, 4),
        '中位盈亏%': round(median_pnl, 4),
        '平均盈利%': round(avg_win, 4),
        '平均亏损%': round(avg_loss, 4),
        '盈亏比': round(avg_win / abs(avg_loss), 2) if avg_loss != 0 else float('inf'),
        '利润因子': round(profit_factor, 2),
        '最大回撤%': round(max_drawdown, 2),
        '最大连亏': max_consec_loss,
        '平均持仓分钟': round(avg_hold, 1),
        '夏普比率': round(sharpe, 2),
        '按出场原因': exit_stats,
    }


def main():
    pairs = [
        ('RB', 'HC', '螺纹钢', '热卷', 0.88),
        ('RU', 'NR', '天然橡胶', '20号胶', 0.84),
        ('TA', 'PF', 'PTA', '短纤', 0.84),
        ('P', 'Y', '棕榈油', '豆油', 0.82),
        ('PP', 'L', '聚丙烯', 'LLDPE', 0.80),
        ('SC', 'LU', '原油', '低硫燃料油', 0.78),
        ('AU', 'AG', '黄金', '白银', 0.76),
        ('J', 'JM', '焦炭', '焦煤', 0.73),
        ('SC', 'FU', '原油', '燃料油', 0.73),
        ('CU', 'ZN', '铜', '锌', 0.72),
        ('CF', 'CY', '棉花', '棉纱', 0.72),
        ('Y', 'OI', '豆油', '菜油', 0.72),
        ('I', 'RB', '铁矿石', '螺纹钢', 0.68),
        ('NR', 'BR', '20号胶', '合成橡胶', 0.68),
        ('EG', 'PF', '乙二醇', '短纤', 0.63),
        ('M', 'RM', '豆粕', '菜粕', 0.59),
        ('SA', 'FG', '纯碱', '玻璃', 0.49),
    ]

    print("=" * 110)
    print("价差回归配对交易回测")
    print("策略: Z>2做空A+做多B, Z<-2做多A+做空B, Z回0获利/Z达3.5止损/60分超时/日内平仓")
    print("=" * 110)

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
            print(f"  {code}: {len(df)}根 ({elapsed:.1f}s)")
        else:
            print(f"  {code}: 失败")

    # 回测每对
    print("\n" + "=" * 110)
    all_results = []

    for code_a, code_b, name_a, name_b, daily_r in pairs:
        if code_a not in cache or code_b not in cache:
            continue

        t0 = time.time()
        trades = backtest_pair(cache[code_a], cache[code_b], name_a, name_b)
        elapsed = time.time() - t0

        if not trades:
            print(f"\n{name_a}↔{name_b}: 无交易")
            continue

        metrics = calc_metrics(trades)
        print(f"\n{'='*80}")
        print(f"{name_a}({code_a}) ↔ {name_b}({code_b})  r={daily_r}  ({elapsed:.1f}s)")
        print("-" * 80)
        print(f"  交易次数: {metrics['交易次数']}")
        print(f"  胜率: {metrics['胜率%']:.1f}%")
        print(f"  总盈亏: {metrics['总盈亏%']:+.2f}%")
        print(f"  平均盈亏: {metrics['平均盈亏%']:+.4f}%")
        print(f"  中位盈亏: {metrics['中位盈亏%']:+.4f}%")
        print(f"  平均盈利: {metrics['平均盈利%']:+.4f}%  平均亏损: {metrics['平均亏损%']:+.4f}%")
        print(f"  盈亏比: {metrics['盈亏比']:.2f}  利润因子: {metrics['利润因子']:.2f}")
        print(f"  最大回撤: {metrics['最大回撤%']:.2f}%  最大连亏: {metrics['最大连亏']}")
        print(f"  平均持仓: {metrics['平均持仓分钟']:.1f}分钟")
        print(f"  夏普比率: {metrics['夏普比率']:.2f}")
        print(f"  按出场原因:")
        for reason, stats in metrics['按出场原因'].items():
            print(f"    {reason}: {int(stats['次数'])}次  "
                  f"平均{stats['平均盈亏']:+.4f}%  胜率{stats['胜率']:.1f}%")

        all_results.append({
            '品种A': code_a, '名称A': name_a,
            '品种B': code_b, '名称B': name_b,
            'r': daily_r,
            '指标': metrics,
            '交易明细': trades[-20:],  # 只存最近20笔
        })

    # === 汇总排名 ===
    print("\n" + "=" * 110)
    print("【回测结果排名】按总盈亏排序")
    print("-" * 110)
    print(f"{'排名':>3} {'品种对':<20} {'r':>5} {'交易数':>6} {'胜率%':>7}"
          f" {'总盈亏%':>9} {'均盈亏%':>9} {'盈亏比':>7} {'利润因子':>8}"
          f" {'最大回撤%':>9} {'均持仓':>6} {'夏普':>6}")
    print("-" * 110)

    sorted_results = sorted(all_results, key=lambda x: x['指标']['总盈亏%'], reverse=True)

    for rank, r in enumerate(sorted_results, 1):
        m = r['指标']
        pair = f"{r['名称A']}↔{r['名称B']}"
        marker = "★" if m['总盈亏%'] > 5 else ("▲" if m['总盈亏%'] > 0 else "▼")
        print(f" {rank:>2} {marker} {pair:<18} {r['r']:>5.2f} {m['交易次数']:>5}"
              f" {m['胜率%']:>6.1f} {m['总盈亏%']:>+8.2f} {m['平均盈亏%']:>+8.4f}"
              f" {m['盈亏比']:>6.2f} {m['利润因子']:>7.2f}"
              f" {m['最大回撤%']:>8.2f} {m['平均持仓分钟']:>5.1f} {m['夏普比率']:>5.2f}")

    # === 按胜率排名 ===
    print("\n" + "=" * 110)
    print("【按胜率排名】")
    print("-" * 110)
    sorted_wr = sorted(all_results, key=lambda x: x['指标']['胜率%'], reverse=True)
    for rank, r in enumerate(sorted_wr, 1):
        m = r['指标']
        pair = f"{r['名称A']}↔{r['名称B']}"
        print(f" {rank:>2}  {pair:<18} 胜率{m['胜率%']:>6.1f}%  "
              f"交易{m['交易次数']:>5}次  总盈亏{m['总盈亏%']:>+8.2f}%  "
              f"盈亏比{m['盈亏比']:>5.2f}  利润因子{m['利润因子']:>6.2f}")

    # === 汇总统计 ===
    print("\n" + "=" * 80)
    print("全局统计")
    print("-" * 80)
    total_trades = sum(r['指标']['交易次数'] for r in all_results)
    total_pnl = sum(r['指标']['总盈亏%'] for r in all_results)
    avg_wr = np.mean([r['指标']['胜率%'] for r in all_results])
    profitable = sum(1 for r in all_results if r['指标']['总盈亏%'] > 0)

    print(f"  品种对总数: {len(all_results)}")
    print(f"  盈利品种对: {profitable}/{len(all_results)}")
    print(f"  总交易次数: {total_trades}")
    print(f"  全局总盈亏: {total_pnl:+.2f}%")
    print(f"  平均胜率: {avg_wr:.1f}%")

    # === 参数敏感性（对表现最好的品种对做不同Z阈值测试）===
    if sorted_results:
        best = sorted_results[0]
        bc_a, bc_b = best['品种A'], best['品种B']
        print(f"\n{'='*80}")
        print(f"参数敏感性分析: {best['名称A']}↔{best['名称B']}")
        print("-" * 80)
        print(f"{'Z入场':>8} {'Z止损':>8} {'超时':>6} {'交易数':>6} {'胜率%':>7}"
              f" {'总盈亏%':>9} {'均盈亏%':>9} {'盈亏比':>7} {'利润因子':>8}")
        print("-" * 80)

        for z_e in [1.5, 2.0, 2.5, 3.0]:
            for z_s in [3.0, 3.5, 4.0, 99.0]:
                for mh in [30, 60, 120]:
                    trades = backtest_pair(cache[bc_a], cache[bc_b],
                                           best['名称A'], best['名称B'],
                                           z_entry=z_e, z_stop=z_s, max_hold=mh)
                    if trades:
                        m = calc_metrics(trades)
                        z_s_label = f"{z_s:.1f}" if z_s < 50 else "无"
                        print(f"  {z_e:>6.1f} {z_s_label:>8} {mh:>5}分"
                              f" {m['交易次数']:>5} {m['胜率%']:>6.1f}"
                              f" {m['总盈亏%']:>+8.2f} {m['平均盈亏%']:>+8.4f}"
                              f" {m['盈亏比']:>6.2f} {m['利润因子']:>7.2f}")

    # 保存
    out_path = os.path.expanduser("~/Scripts/spread_pair_backtest_results.json")

    class NpEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (np.integer,)):
                return int(obj)
            if isinstance(obj, (np.floating,)):
                return float(obj)
            if isinstance(obj, (np.bool_,)):
                return bool(obj)
            return super().default(obj)

    with open(out_path, 'w') as f:
        json.dump({
            '策略参数': {
                '滚动窗口': 120,
                'Z入场': 2.0,
                'Z出场': 0.0,
                'Z止损': 3.5,
                '最大持仓': '60分钟',
                '日内平仓': True,
                '冷却期': '10分钟',
                '数据': '2022年至今1分钟K线',
            },
            '品种对结果': [{k: v for k, v in r.items() if k != '交易明细'}
                        for r in all_results],
            '排名_按盈亏': [
                {'品种对': f"{r['名称A']}↔{r['名称B']}",
                 'r': r['r'],
                 '交易数': r['指标']['交易次数'],
                 '胜率': r['指标']['胜率%'],
                 '总盈亏': r['指标']['总盈亏%'],
                 '盈亏比': r['指标']['盈亏比'],
                 '利润因子': r['指标']['利润因子'],
                 '夏普': r['指标']['夏普比率']}
                for r in sorted_results
            ],
        }, f, ensure_ascii=False, indent=2, cls=NpEncoder)
    print(f"\n结果已保存: {out_path}")


if __name__ == '__main__':
    main()
