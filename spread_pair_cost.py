"""
配对交易加入真实手续费+滑点

成本模型：
每笔配对交易 = 4条腿（开A + 开B + 平A + 平B）
每条腿成本 = 手续费 + 滑点（1 tick）

手续费用交易所标准（不加倍），滑点按1 tick/腿。
"""
import pandas as pd
import numpy as np
import os
import json
import time
from lead_lag_batch import load_main_contract_minutes, EXCHANGE_MAP

# 各品种的成本参数
# tick_size: 最小变动价位
# multiplier: 合约乘数（吨/手等）
# commission: 手续费/手（单边）
# 注意: 有些品种按万分之X收，这里统一折算成每手固定金额（用近似价格）
COST_PARAMS = {
    'RB': {'tick': 1, 'mult': 10, 'comm': 1.0, 'price': 3500},    # 螺纹钢
    'HC': {'tick': 1, 'mult': 10, 'comm': 1.0, 'price': 3500},    # 热卷
    'J':  {'tick': 0.5, 'mult': 100, 'comm': 28.0, 'price': 2000},  # 焦炭 万1.4
    'JM': {'tick': 0.5, 'mult': 60, 'comm': 12.6, 'price': 1500},   # 焦煤 万1.4
    'RU': {'tick': 5, 'mult': 10, 'comm': 3.0, 'price': 15000},    # 天然橡胶
    'NR': {'tick': 5, 'mult': 10, 'comm': 3.0, 'price': 12000},    # 20号胶
    'TA': {'tick': 2, 'mult': 5, 'comm': 3.0, 'price': 5000},     # PTA
    'PF': {'tick': 2, 'mult': 5, 'comm': 3.0, 'price': 7000},     # 短纤
    'PP': {'tick': 1, 'mult': 5, 'comm': 2.4, 'price': 8000},     # 聚丙烯 万0.6
    'L':  {'tick': 1, 'mult': 5, 'comm': 2.4, 'price': 8000},     # LLDPE 万0.6
    'SC': {'tick': 0.1, 'mult': 1000, 'comm': 20.0, 'price': 500},  # 原油
    'LU': {'tick': 1, 'mult': 10, 'comm': 5.0, 'price': 4000},    # 低硫燃料油
    'FU': {'tick': 1, 'mult': 10, 'comm': 2.0, 'price': 3500},    # 燃料油
    'CU': {'tick': 10, 'mult': 5, 'comm': 12.5, 'price': 75000},   # 铜 万0.5
    'ZN': {'tick': 5, 'mult': 5, 'comm': 3.0, 'price': 22000},    # 锌
    'AU': {'tick': 0.02, 'mult': 1000, 'comm': 10.0, 'price': 650}, # 黄金
    'AG': {'tick': 1, 'mult': 15, 'comm': 5.0, 'price': 8000},    # 白银
    'CF': {'tick': 5, 'mult': 5, 'comm': 4.3, 'price': 14000},    # 棉花
    'CY': {'tick': 5, 'mult': 5, 'comm': 4.0, 'price': 25000},    # 棉纱
    'P':  {'tick': 2, 'mult': 10, 'comm': 2.5, 'price': 8000},    # 棕榈油
    'Y':  {'tick': 2, 'mult': 10, 'comm': 2.5, 'price': 8000},    # 豆油
    'OI': {'tick': 1, 'mult': 10, 'comm': 2.0, 'price': 9000},    # 菜油
    'I':  {'tick': 0.5, 'mult': 100, 'comm': 7.0, 'price': 700},   # 铁矿石 万1
    'M':  {'tick': 1, 'mult': 10, 'comm': 1.5, 'price': 3000},    # 豆粕
    'RM': {'tick': 1, 'mult': 10, 'comm': 1.5, 'price': 2500},    # 菜粕
    'EG': {'tick': 1, 'mult': 10, 'comm': 3.0, 'price': 4500},    # 乙二醇
    'BR': {'tick': 5, 'mult': 5, 'comm': 3.0, 'price': 15000},    # 合成橡胶
    'SA': {'tick': 1, 'mult': 20, 'comm': 3.5, 'price': 1500},    # 纯碱
    'FG': {'tick': 1, 'mult': 20, 'comm': 3.0, 'price': 1500},    # 玻璃
}


def calc_pair_cost_pct(code_a, code_b):
    """
    计算一笔配对交易的总成本（占头寸的%）。

    4条腿: 开A + 开B + 平A + 平B
    每条腿: 手续费 + 1 tick滑点

    返回成本%（直接从trade P&L中扣除）。
    """
    pa = COST_PARAMS.get(code_a)
    pb = COST_PARAMS.get(code_b)
    if not pa or not pb:
        return 0.10  # 默认0.10%

    # A腿: 开+平 = 2次交易
    # 每次: 手续费 + 1 tick * multiplier 的滑点
    slip_a = pa['tick'] * pa['mult']  # 1 tick滑点的金额
    cost_a_per_trade = 2 * (pa['comm'] + slip_a)  # 开+平
    notional_a = pa['price'] * pa['mult']
    cost_a_pct = cost_a_per_trade / notional_a * 100

    slip_b = pb['tick'] * pb['mult']
    cost_b_per_trade = 2 * (pb['comm'] + slip_b)
    notional_b = pb['price'] * pb['mult']
    cost_b_pct = cost_b_per_trade / notional_b * 100

    total_pct = cost_a_pct + cost_b_pct
    return total_pct


def backtest_pair_with_cost(df_a, df_b, code_a, code_b,
                            window=120, z_entry=2.0, z_exit=0.0, z_stop=3.5,
                            max_hold=60, daily_close=True,
                            cost_pct=None):
    """带成本的配对交易回测"""
    merged = pd.merge(df_a[['datetime', 'close']],
                      df_b[['datetime', 'close']],
                      on='datetime', suffixes=('_A', '_B'))

    if len(merged) < window + 200:
        return None

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
            hold_time = i - position['entry_idx']
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

            if hold_time >= max_hold:
                exit_reason = '超时平仓'

            if daily_close and i + 1 < len(merged):
                next_date = merged.iloc[i + 1]['date']
                if next_date != cur_date:
                    exit_reason = '日内平仓'

            if exit_reason:
                # 扣除成本
                net_pnl = total_pnl - cost_pct

                trades.append({
                    '入场时间': str(position['entry_time']),
                    '出场时间': str(dt),
                    '持仓分钟': hold_time,
                    '方向': position['direction'],
                    '入场Z': round(position['entry_z'], 3),
                    '出场Z': round(float(z), 3),
                    '毛盈亏%': round(total_pnl, 4),
                    '成本%': round(cost_pct, 4),
                    '净盈亏%': round(net_pnl, 4),
                    '出场原因': exit_reason,
                })
                position = None
                cooldown = 10
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

    return trades


def calc_metrics(trades, pnl_col='净盈亏%'):
    """计算回测指标"""
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

    exit_stats = df.groupby('出场原因').agg(
        次数=(pnl_col, 'count'),
        平均盈亏=(pnl_col, 'mean'),
        胜率=(pnl_col, lambda x: (x > 0).mean() * 100),
    ).to_dict('index')

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
        '平均持仓分钟': round(df['持仓分钟'].mean(), 1),
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

    print("=" * 120)
    print("配对交易回测 —— 含真实手续费+滑点")
    print("成本 = 每腿(手续费 + 1tick滑点) × 4腿")
    print("=" * 120)

    # 先打印各品种对的成本
    print("\n各品种对单笔交易成本估算:")
    print(f"  {'品种对':<22} {'A腿成本':>10} {'B腿成本':>10} {'总成本':>10}")
    print("  " + "-" * 55)

    for code_a, code_b, na, nb, _ in pairs:
        pa = COST_PARAMS.get(code_a, {})
        pb = COST_PARAMS.get(code_b, {})
        if pa and pb:
            slip_a = pa['tick'] * pa['mult']
            cost_a = 2 * (pa['comm'] + slip_a) / (pa['price'] * pa['mult']) * 100
            slip_b = pb['tick'] * pb['mult']
            cost_b = 2 * (pb['comm'] + slip_b) / (pb['price'] * pb['mult']) * 100
            total = cost_a + cost_b
            print(f"  {na}↔{nb:<14} {cost_a:>9.3f}% {cost_b:>9.3f}% {total:>9.3f}%")

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
    print("\n" + "=" * 120)
    all_results = []

    for code_a, code_b, na, nb, daily_r in pairs:
        if code_a not in cache or code_b not in cache:
            continue

        cost_pct = calc_pair_cost_pct(code_a, code_b)

        print(f"\n{'='*100}")
        print(f"{na}({code_a}) ↔ {nb}({code_b})  r={daily_r}  单笔成本={cost_pct:.3f}%")
        print("-" * 100)

        trades = backtest_pair_with_cost(
            cache[code_a], cache[code_b], code_a, code_b,
            cost_pct=cost_pct
        )

        if not trades:
            print("  数据不足")
            continue

        # 毛利指标
        gross = calc_metrics(trades, '毛盈亏%')
        # 净利指标
        net = calc_metrics(trades, '净盈亏%')

        print(f"  交易次数: {gross['交易次数']}")
        print(f"  单笔成本: {cost_pct:.3f}%")
        print()
        print(f"  {'指标':<14} {'毛利(无成本)':>14} {'净利(扣成本)':>14} {'差异':>10}")
        print(f"  {'-'*55}")
        print(f"  {'胜率':<14} {gross['胜率%']:>13.1f}% {net['胜率%']:>13.1f}% {net['胜率%']-gross['胜率%']:>+9.1f}%")
        print(f"  {'总盈亏':<12} {gross['总盈亏%']:>+13.2f}% {net['总盈亏%']:>+13.2f}% {net['总盈亏%']-gross['总盈亏%']:>+9.2f}%")
        print(f"  {'平均盈亏':<11} {gross['平均盈亏%']:>+13.4f}% {net['平均盈亏%']:>+13.4f}% {net['平均盈亏%']-gross['平均盈亏%']:>+9.4f}%")
        print(f"  {'利润因子':<11} {gross['利润因子']:>14.2f} {net['利润因子']:>14.2f}")
        print(f"  {'夏普比率':<11} {gross['夏普比率']:>14.2f} {net['夏普比率']:>14.2f}")
        print(f"  {'最大回撤':<11} {gross['最大回撤%']:>+13.2f}% {net['最大回撤%']:>+13.2f}%")
        print(f"  {'最大连亏':<12} {gross['最大连亏']:>14} {net['最大连亏']:>14}")

        # 按出场原因
        print(f"\n  净利按出场原因:")
        for reason, stats in net['按出场原因'].items():
            print(f"    {reason}: {stats['次数']}笔, 均盈亏{stats['平均盈亏']:+.4f}%, 胜率{stats['胜率']:.1f}%")

        total_cost = cost_pct * gross['交易次数']
        all_results.append({
            '品种A': code_a, '名称A': na,
            '品种B': code_b, '名称B': nb,
            'r': daily_r,
            '单笔成本%': round(cost_pct, 4),
            '交易次数': gross['交易次数'],
            '总成本%': round(total_cost, 2),
            '毛利': {k: v for k, v in gross.items() if k != '按出场原因'},
            '净利': {k: v for k, v in net.items() if k != '按出场原因'},
        })

    # === 汇总表 ===
    print("\n" + "=" * 120)
    print("【汇总】毛利 vs 净利 对比")
    print("-" * 120)
    print(f"{'排名':>3} {'品种对':<20} {'r':>5} {'笔数':>5} {'成本/笔':>8}"
          f" {'毛胜率':>7} {'净胜率':>7} {'毛总盈亏':>10} {'净总盈亏':>10}"
          f" {'毛夏普':>7} {'净夏普':>7} {'净回撤':>8} {'结论':>6}")
    print("-" * 120)

    sorted_results = sorted(all_results,
                            key=lambda x: x['净利']['总盈亏%'],
                            reverse=True)

    n_profitable = 0
    for rank, r in enumerate(sorted_results, 1):
        pair = f"{r['名称A']}↔{r['名称B']}"
        g = r['毛利']
        n = r['净利']
        profitable = n['总盈亏%'] > 0
        if profitable:
            n_profitable += 1
        marker = "✓" if profitable else "✗"

        print(f" {rank:>2}  {pair:<18} {r['r']:>5.2f} {r['交易次数']:>5} {r['单笔成本%']:>7.3f}%"
              f" {g['胜率%']:>6.1f}% {n['胜率%']:>6.1f}%"
              f" {g['总盈亏%']:>+9.1f}% {n['总盈亏%']:>+9.1f}%"
              f" {g['夏普比率']:>6.2f} {n['夏普比率']:>6.2f}"
              f" {n['最大回撤%']:>+7.1f}% {marker:>5}")

    # 统计
    total_gross = sum(r['毛利']['总盈亏%'] for r in all_results)
    total_net = sum(r['净利']['总盈亏%'] for r in all_results)
    total_cost_all = sum(r['总成本%'] for r in all_results)
    total_trades = sum(r['交易次数'] for r in all_results)

    print("-" * 120)
    print(f"  总计: {total_trades}笔交易, 毛利{total_gross:+.1f}%, 总成本{total_cost_all:.1f}%,"
          f" 净利{total_net:+.1f}%, 净盈利品种{n_profitable}/{len(all_results)}")

    # 成本占比分析
    print(f"\n  成本吞噬分析:")
    for r in sorted_results:
        pair = f"{r['名称A']}↔{r['名称B']}"
        gross_total = r['毛利']['总盈亏%']
        cost_total = r['总成本%']
        if gross_total > 0:
            eaten = cost_total / gross_total * 100
            print(f"    {pair:<18} 毛利{gross_total:>+8.1f}% - 成本{cost_total:>7.1f}%"
                  f" = 净利{r['净利']['总盈亏%']:>+8.1f}%  成本吞噬{eaten:.0f}%")
        else:
            print(f"    {pair:<18} 毛利{gross_total:>+8.1f}% - 成本{cost_total:>7.1f}%"
                  f" = 净利{r['净利']['总盈亏%']:>+8.1f}%  本身就亏")

    # 保存
    out_path = os.path.expanduser("~/Scripts/spread_pair_cost_results.json")
    with open(out_path, 'w') as f:
        json.dump({
            '成本模型': '手续费(交易所标准) + 滑点(1tick/腿) × 4腿',
            '品种结果': all_results,
            '统计': {
                '总交易笔数': total_trades,
                '毛总盈亏%': round(total_gross, 1),
                '总成本%': round(total_cost_all, 1),
                '净总盈亏%': round(total_net, 1),
                '净盈利品种数': n_profitable,
                '总品种数': len(all_results),
            }
        }, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存: {out_path}")


if __name__ == '__main__':
    main()
