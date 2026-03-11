"""
分钟级单向引领性测试：黄金(AU) → 白银(AG)

逻辑（模拟真实日内交易）：
1. 黄金在1分钟K线上走出短期上涨趋势（价格突破MA20且MA5>MA10）
2. 此时白银还没涨（白银价格 < 白银MA20）
3. 观察白银接下来5/10/15/30/60分钟的表现
4. 统计：白银跟涨的概率、平均涨幅
"""
import pandas as pd
import numpy as np
import os
import glob
from pathlib import Path

LOCAL = os.path.expanduser("~/Downloads/期货数据_parquet/SHFE")
USB = "/Volumes/Lexar/期货数据备份/期货_全部数据/SHFE"


def load_main_contract_minutes(code, start_year=2022):
    """加载品种主力合约的1分钟K线（只取近几年，控制内存）"""
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
        # 只加载近几年的合约文件
        dfs = []
        for f in csv_files:
            stem = Path(f).stem  # e.g. AG2312
            # 提取年份数字
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

    # 每日找主力合约（当日成交量最大的合约）
    daily_vol = df.groupby(['date', 'symbol'])['volume'].sum().reset_index()
    idx = daily_vol.groupby('date')['volume'].idxmax()
    main_contracts = daily_vol.loc[idx, ['date', 'symbol']].set_index('date')['symbol']

    # 只保留每日主力合约的分钟数据
    result = []
    for date, main_sym in main_contracts.items():
        day_data = df[(df['date'] == date) & (df['symbol'] == main_sym)].copy()
        result.append(day_data)

    out = pd.concat(result, ignore_index=True)
    out = out.sort_values('datetime').reset_index(drop=True)
    print(f"  {code}: {len(out)}根K线, {out['datetime'].iloc[0]} ~ {out['datetime'].iloc[-1]}")
    return out[['datetime', 'open', 'high', 'low', 'close', 'volume']].copy()


def add_ma(df):
    """加均线"""
    c = df['close'].astype(float)
    df['ma5'] = c.rolling(5).mean()
    df['ma10'] = c.rolling(10).mean()
    df['ma20'] = c.rolling(20).mean()
    return df


def find_intraday_lead_signals(leader_df, follower_df, direction='上涨',
                                look_ahead_bars=[5, 10, 15, 30, 60]):
    """
    在分钟级别找引领信号。

    信号条件（上涨）：
    - 引领者：收盘 > MA20 且 MA5 > MA10（短期多头）
    - 且前一根K线不满足这个条件（刚形成趋势）
    - 跟随者：收盘 < MA20（还没涨）

    返回信号列表
    """
    # 按时间对齐（取两者共有的分钟）
    leader_df = leader_df.set_index('datetime')
    follower_df = follower_df.set_index('datetime')
    common_times = leader_df.index.intersection(follower_df.index)
    leader = leader_df.loc[common_times].copy()
    follower = follower_df.loc[common_times].copy()
    print(f"  对齐后: {len(common_times)}根共同K线")

    signals = []
    cooldown = 0  # 冷却期，避免同一波趋势重复触发

    for i in range(25, len(common_times)):
        if cooldown > 0:
            cooldown -= 1
            continue

        dt = common_times[i]
        dt_prev = common_times[i - 1]

        l = leader.iloc[i]
        l_prev = leader.iloc[i - 1]
        f = follower.iloc[i]

        if direction == '上涨':
            # 引领者刚形成上涨趋势
            leader_now = (l['close'] > l['ma20'] and l['ma5'] > l['ma10'])
            leader_prev_no = not (l_prev['close'] > l_prev['ma20'] and l_prev['ma5'] > l_prev['ma10'])
            follower_not_yet = f['close'] < f['ma20']
        else:
            leader_now = (l['close'] < l['ma20'] and l['ma5'] < l['ma10'])
            leader_prev_no = not (l_prev['close'] < l_prev['ma20'] and l_prev['ma5'] < l_prev['ma10'])
            follower_not_yet = f['close'] > f['ma20']

        if leader_now and leader_prev_no and follower_not_yet:
            entry_price = f['close']
            signal = {
                '时间': str(dt),
                '引领者价格': round(float(l['close']), 2),
                '跟随者入场价': round(float(entry_price), 2),
            }

            for bars in look_ahead_bars:
                if i + bars < len(common_times):
                    future_price = follower.iloc[i + bars]['close']
                    ret = (future_price - entry_price) / entry_price * 100
                    if direction == '上涨':
                        followed = bool(future_price > entry_price)
                    else:
                        followed = bool(future_price < entry_price)
                    signal[f'{bars}分钟后涨幅'] = round(float(ret), 4)
                    signal[f'{bars}分钟后跟随'] = followed

            signals.append(signal)
            cooldown = 30  # 触发后冷却30分钟

    return signals


def print_intraday_stats(signals, direction='上涨', bars_list=[5, 10, 15, 30, 60]):
    """打印统计"""
    if not signals:
        print("  没有找到信号")
        return

    df = pd.DataFrame(signals)
    print(f"  信号总数: {len(df)}")
    print(f"  时间范围: {df['时间'].iloc[0][:10]} ~ {df['时间'].iloc[-1][:10]}")
    print()

    label = '跟涨' if direction == '上涨' else '跟跌'
    for bars in bars_list:
        col_ret = f'{bars}分钟后涨幅'
        col_follow = f'{bars}分钟后跟随'
        if col_ret in df.columns:
            valid = df[col_follow].dropna()
            n = len(valid)
            hit = int(valid.sum())
            rate = hit / n * 100 if n > 0 else 0
            avg_ret = df[col_ret].dropna().mean()
            med_ret = df[col_ret].dropna().median()
            print(f"  {bars:>3}分钟后: {label}率 {hit}/{n} = {rate:.1f}%  "
                  f"均涨幅 {avg_ret:+.4f}%  中位数 {med_ret:+.4f}%")


def main():
    print("=" * 70)
    print("分钟级单向引领性测试：黄金(AU) → 白银(AG)")
    print("=" * 70)

    print("\n正在加载数据（2022年至今）...")
    au = load_main_contract_minutes('AU', start_year=2022)
    ag = load_main_contract_minutes('AG', start_year=2022)

    if au is None or ag is None:
        print("数据加载失败")
        return

    # 加均线
    au = add_ma(au)
    ag = add_ma(ag)

    # === 测试1：黄金上涨 → 白银跟涨？ ===
    print("\n" + "=" * 70)
    print("测试1：黄金1分钟级别走出上涨趋势，白银还没涨 → 白银会跟涨吗？")
    print("  条件: AU收盘>MA20 且 MA5>MA10 (刚形成)")
    print("        AG收盘<MA20 (还没涨)")
    print("  冷却: 每次触发后30分钟内不重复")
    print("-" * 70)
    signals_up = find_intraday_lead_signals(au, ag, '上涨')
    print_intraday_stats(signals_up, '上涨')

    # === 测试2：黄金下跌 → 白银跟跌？ ===
    print("\n" + "=" * 70)
    print("测试2：黄金1分钟级别走出下跌趋势，白银还没跌 → 白银会跟跌吗？")
    print("  条件: AU收盘<MA20 且 MA5<MA10 (刚形成)")
    print("        AG收盘>MA20 (还没跌)")
    print("-" * 70)
    signals_down = find_intraday_lead_signals(au, ag, '下跌')
    print_intraday_stats(signals_down, '下跌')

    # 打印最近的信号样例
    print("\n" + "=" * 70)
    print("最近15次上涨引领信号:")
    print("-" * 70)
    for s in (signals_up or [])[-15:]:
        r10 = s.get('10分钟后涨幅', '?')
        f10 = s.get('10分钟后跟随', '?')
        r30 = s.get('30分钟后涨幅', '?')
        f30 = s.get('30分钟后跟随', '?')
        tag10 = '跟涨' if f10 else '没涨'
        tag30 = '跟涨' if f30 else '没涨'
        print(f"  {s['时间'][:16]}  AU={s['引领者价格']}  AG={s['跟随者入场价']}  "
              f"10分钟{r10:+.3f}%{tag10}  30分钟{r30:+.3f}%{tag30}")

    print("\n最近15次下跌引领信号:")
    print("-" * 70)
    for s in (signals_down or [])[-15:]:
        r10 = s.get('10分钟后涨幅', '?')
        f10 = s.get('10分钟后跟随', '?')
        r30 = s.get('30分钟后涨幅', '?')
        f30 = s.get('30分钟后跟随', '?')
        tag10 = '跟跌' if f10 else '没跌'
        tag30 = '跟跌' if f30 else '没跌'
        print(f"  {s['时间'][:16]}  AU={s['引领者价格']}  AG={s['跟随者入场价']}  "
              f"10分钟{r10:+.3f}%{tag10}  30分钟{r30:+.3f}%{tag30}")

    # 保存
    import json

    class BoolEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (np.bool_, bool)):
                return int(obj)
            return super().default(obj)

    out_path = os.path.expanduser("~/Scripts/lead_lag_AU_AG_intraday.json")
    with open(out_path, 'w') as f:
        json.dump({
            '黄金上涨引领白银': signals_up,
            '黄金下跌引领白银': signals_down,
        }, f, ensure_ascii=False, indent=2, cls=BoolEncoder)
    print(f"\n结果已保存: {out_path}")


if __name__ == '__main__':
    main()
