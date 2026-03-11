"""
单向引领性测试：黄金(AU) → 白银(AG)

逻辑：
1. 黄金走出上涨趋势信号（价格突破MA20且MA5>MA10>MA20）
2. 此时白银还没涨（白银价格 < 白银MA20）
3. 记录下来，然后观察白银接下来30/60/120/240分钟的表现
4. 统计：白银跟涨的概率、平均涨幅

同理测下跌方向：黄金走出下跌趋势，白银会不会跟跌

数据源：本地parquet（SHFE/AU.parquet, SHFE/AG.parquet）
"""
import pandas as pd
import numpy as np
import os

LOCAL = os.path.expanduser("~/Downloads/期货数据_parquet/SHFE")
USB = "/Volumes/Lexar/期货数据备份/期货_全部数据/SHFE"
import glob
from pathlib import Path


def load_daily(code):
    """加载品种主力合约日线（先试本地parquet，再试USB CSV）"""
    parquet_path = os.path.join(LOCAL, f"{code}.parquet")
    if os.path.exists(parquet_path):
        df = pd.read_parquet(parquet_path, columns=['datetime', 'close', 'volume', 'symbol'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['date'] = df['datetime'].dt.date
        df = df[df['volume'] > 0]
        daily = df.groupby(['date', 'symbol']).agg(
            close=('close', 'last'), total_vol=('volume', 'sum')).reset_index()
    else:
        csv_dir = os.path.join(USB, code)
        csv_files = sorted(glob.glob(os.path.join(csv_dir, "*.csv")))
        dfs = []
        for f in csv_files:
            try:
                d = pd.read_csv(f, usecols=['datetime', 'close', 'volume'], parse_dates=['datetime'])
                d['symbol'] = Path(f).stem
                dfs.append(d)
            except Exception:
                continue
        df = pd.concat(dfs, ignore_index=True)
        df['date'] = df['datetime'].dt.date
        df = df[df['volume'] > 0]
        daily = df.groupby(['date', 'symbol']).agg(
            close=('close', 'last'), total_vol=('volume', 'sum')).reset_index()

    # 每日选成交量最大的合约
    idx = daily.groupby('date')['total_vol'].idxmax()
    main = daily.loc[idx, ['date', 'close']].copy()
    main['date'] = pd.to_datetime(main['date'])
    main = main.set_index('date').sort_index()

    # 加均线
    main['ma5'] = main['close'].rolling(5).mean()
    main['ma10'] = main['close'].rolling(10).mean()
    main['ma20'] = main['close'].rolling(20).mean()

    return main


def find_lead_signals(leader, follower, direction='上涨'):
    """
    找引领信号：
    - leader 走出趋势（突破MA20 + 均线多头/空头排列）
    - follower 此时还没走出趋势（还在MA20另一侧）

    返回每个信号点和follower后续表现
    """
    signals = []

    # 对齐日期
    dates = leader.index.intersection(follower.index)
    leader = leader.loc[dates]
    follower = follower.loc[dates]

    for i in range(25, len(dates)):
        date = dates[i]
        prev_date = dates[i - 1]

        l = leader.loc[date]
        l_prev = leader.loc[prev_date]
        f = follower.loc[date]

        if direction == '上涨':
            # 引领者信号：今天突破MA20（昨天还在MA20下方或刚突破）
            leader_trend = (l['close'] > l['ma20'] and
                            l['ma5'] > l['ma10'] > l['ma20'])
            leader_just_started = (l_prev['close'] <= l_prev['ma20'] or
                                    not (l_prev['ma5'] > l_prev['ma10']))

            # 跟随者还没涨
            follower_not_yet = f['close'] < f['ma20']

        else:  # 下跌
            leader_trend = (l['close'] < l['ma20'] and
                            l['ma5'] < l['ma10'] < l['ma20'])
            leader_just_started = (l_prev['close'] >= l_prev['ma20'] or
                                    not (l_prev['ma5'] < l_prev['ma10']))
            follower_not_yet = f['close'] > f['ma20']

        if leader_trend and leader_just_started and follower_not_yet:
            # 记录follower后续N天的表现
            entry_price = f['close']
            future = {}
            for days in [1, 3, 5, 10, 20]:
                if i + days < len(dates):
                    future_date = dates[i + days]
                    future_price = follower.loc[future_date]['close']
                    ret = (future_price - entry_price) / entry_price * 100
                    future[f'{days}天后涨幅'] = round(ret, 3)
                    # 是否跟随了方向
                    if direction == '上涨':
                        future[f'{days}天后跟涨'] = future_price > entry_price
                    else:
                        future[f'{days}天后跟跌'] = future_price < entry_price

            signals.append({
                '日期': str(date.date()),
                '引领者收盘': round(l['close'], 2),
                '跟随者收盘': round(entry_price, 2),
                **future,
            })

    return signals


def print_stats(signals, direction='上涨'):
    """打印统计"""
    if not signals:
        print("  没有找到信号")
        return

    df = pd.DataFrame(signals)
    print(f"  信号总数: {len(df)}")
    print(f"  时间范围: {df['日期'].iloc[0]} ~ {df['日期'].iloc[-1]}")
    print()

    follow_key = '跟涨' if direction == '上涨' else '跟跌'

    for days in [1, 3, 5, 10, 20]:
        col_ret = f'{days}天后涨幅'
        col_follow = f'{days}天后{follow_key}'
        if col_ret in df.columns and col_follow in df.columns:
            valid = df[col_follow].dropna()
            n = len(valid)
            hit = valid.sum()
            rate = hit / n * 100 if n > 0 else 0
            avg_ret = df[col_ret].dropna().mean()
            med_ret = df[col_ret].dropna().median()
            print(f"  {days:>2}天后: {follow_key}率 {hit}/{n} = {rate:.1f}%  "
                  f"平均涨幅 {avg_ret:+.3f}%  中位数 {med_ret:+.3f}%")


def main():
    print("=" * 70)
    print("单向引领性测试：黄金(AU) 对 白银(AG) 的影响")
    print("=" * 70)

    print("\n正在加载数据...")
    au = load_daily('AU')
    ag = load_daily('AG')
    print(f"  黄金: {len(au)}天, {au.index[0].date()} ~ {au.index[-1].date()}")
    print(f"  白银: {len(ag)}天, {ag.index[0].date()} ~ {ag.index[-1].date()}")

    # === 测试1：黄金上涨 → 白银会跟涨吗？ ===
    print("\n" + "=" * 70)
    print("测试1：黄金走出上涨趋势，白银还没涨 → 白银会跟涨吗？")
    print("  条件: AU收盘>MA20 且 MA5>MA10>MA20 (刚形成)")
    print("        AG收盘<MA20 (还没涨)")
    print("-" * 70)
    signals_up = find_lead_signals(au, ag, '上涨')
    print_stats(signals_up, '上涨')

    # === 测试2：黄金下跌 → 白银会跟跌吗？ ===
    print("\n" + "=" * 70)
    print("测试2：黄金走出下跌趋势，白银还没跌 → 白银会跟跌吗？")
    print("  条件: AU收盘<MA20 且 MA5<MA10<MA20 (刚形成)")
    print("        AG收盘>MA20 (还没跌)")
    print("-" * 70)
    signals_down = find_lead_signals(au, ag, '下跌')
    print_stats(signals_down, '下跌')

    # === 打印最近的信号 ===
    print("\n" + "=" * 70)
    print("最近10次上涨引领信号:")
    print("-" * 70)
    if signals_up:
        for s in signals_up[-10:]:
            days5 = s.get('5天后涨幅', '?')
            follow5 = s.get('5天后跟涨', '?')
            days10 = s.get('10天后涨幅', '?')
            follow10 = s.get('10天后跟涨', '?')
            print(f"  {s['日期']}  AU={s['引领者收盘']}  AG={s['跟随者收盘']}  "
                  f"5天后{'+' if follow5 else '-'}{days5}%  "
                  f"10天后{'+' if follow10 else '-'}{days10}%")

    print("\n最近10次下跌引领信号:")
    print("-" * 70)
    if signals_down:
        for s in signals_down[-10:]:
            days5 = s.get('5天后涨幅', '?')
            follow5 = s.get('5天后跟跌', '?')
            days10 = s.get('10天后涨幅', '?')
            follow10 = s.get('10天后跟跌', '?')
            print(f"  {s['日期']}  AU={s['引领者收盘']}  AG={s['跟随者收盘']}  "
                  f"5天后{days5}%{'跟跌' if follow5 else '没跌'}  "
                  f"10天后{days10}%{'跟跌' if follow10 else '没跌'}")

    # 保存完整信号
    output = {
        '黄金上涨引领白银': signals_up,
        '黄金下跌引领白银': signals_down,
    }
    import json
    out_path = os.path.expanduser("~/Scripts/lead_lag_AU_AG.json")
    with open(out_path, 'w') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n完整信号已保存: {out_path}")


if __name__ == '__main__':
    main()
