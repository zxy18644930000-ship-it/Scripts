"""
验证期货品种相关性表 - 用历史数据计算日收益率相关系数
数据源: ~/Downloads/期货数据_parquet/ + /Volumes/Lexar/期货数据备份/
"""
import pandas as pd
import numpy as np
import json
import os
import glob
from pathlib import Path

# === 数据路径 ===
LOCAL_PARQUET = os.path.expanduser("~/Downloads/期货数据_parquet")
USB_CSV = "/Volumes/Lexar/期货数据备份/期货_全部数据"

# 品种→交易所映射
EXCHANGE_MAP = {
    'CF': 'CZCE', 'SA': 'CZCE', 'TA': 'CZCE', 'FG': 'CZCE', 'SR': 'CZCE',
    'MA': 'CZCE', 'OI': 'CZCE', 'RM': 'CZCE', 'SF': 'CZCE', 'SM': 'CZCE',
    'AP': 'CZCE', 'CJ': 'CZCE', 'PK': 'CZCE', 'CY': 'CZCE', 'PF': 'CZCE',
    'UR': 'CZCE', 'ZC': 'CZCE', 'RS': 'CZCE',
    'P': 'DCE', 'Y': 'DCE', 'M': 'DCE', 'C': 'DCE', 'CS': 'DCE',
    'I': 'DCE', 'J': 'DCE', 'JM': 'DCE', 'JD': 'DCE', 'LH': 'DCE',
    'L': 'DCE', 'PP': 'DCE', 'V': 'DCE', 'EG': 'DCE', 'EB': 'DCE',
    'PG': 'DCE', 'A': 'DCE', 'B': 'DCE', 'SS': 'DCE',
    'CU': 'SHFE', 'AL': 'SHFE', 'ZN': 'SHFE', 'NI': 'SHFE', 'SN': 'SHFE',
    'PB': 'SHFE', 'AU': 'SHFE', 'AG': 'SHFE', 'RB': 'SHFE', 'HC': 'SHFE',
    'RU': 'SHFE', 'BU': 'SHFE', 'FU': 'SHFE', 'BR': 'SHFE', 'AO': 'SHFE',
    'SC': 'INE', 'NR': 'INE', 'LU': 'INE', 'BC': 'INE',
    'SI': 'GFEX', 'LC': 'GFEX',
}


def load_daily_close(code):
    """加载品种的日线收盘价 (取主力合约近似: 每日所有合约的成交量加权收盘价)"""
    exchange = EXCHANGE_MAP.get(code)
    if not exchange:
        return None

    # 先尝试本地parquet
    parquet_path = os.path.join(LOCAL_PARQUET, exchange, f"{code}.parquet")
    if os.path.exists(parquet_path):
        df = pd.read_parquet(parquet_path, columns=['datetime', 'close', 'volume', 'symbol'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['date'] = df['datetime'].dt.date
        # 用成交量加权得到每日收盘价(近似主力)
        df = df[df['volume'] > 0]
        if df.empty:
            return None
        # 取每日最后一根K线，按成交量选主力合约
        daily = df.groupby(['date', 'symbol']).agg(
            close=('close', 'last'),
            total_vol=('volume', 'sum')
        ).reset_index()
        # 每日选成交量最大的合约作为主力
        idx = daily.groupby('date')['total_vol'].idxmax()
        main = daily.loc[idx, ['date', 'close']].set_index('date')
        main.index = pd.to_datetime(main.index)
        return main['close']

    # 再尝试USB CSV
    csv_dir = os.path.join(USB_CSV, exchange, code)
    if os.path.exists(csv_dir):
        csv_files = sorted(glob.glob(os.path.join(csv_dir, "*.csv")))
        if not csv_files:
            return None
        dfs = []
        for f in csv_files:
            try:
                d = pd.read_csv(f, usecols=['datetime', 'close', 'volume'], parse_dates=['datetime'])
                # 从文件名提取合约代码
                contract = Path(f).stem
                d['symbol'] = contract
                dfs.append(d)
            except Exception:
                continue
        if not dfs:
            return None
        df = pd.concat(dfs, ignore_index=True)
        df['date'] = df['datetime'].dt.date
        df = df[df['volume'] > 0]
        if df.empty:
            return None
        daily = df.groupby(['date', 'symbol']).agg(
            close=('close', 'last'),
            total_vol=('volume', 'sum')
        ).reset_index()
        idx = daily.groupby('date')['total_vol'].idxmax()
        main = daily.loc[idx, ['date', 'close']].set_index('date')
        main.index = pd.to_datetime(main.index)
        return main['close']

    return None


def compute_correlation(s1, s2, min_overlap=120):
    """计算两个价格序列的日收益率Pearson相关系数"""
    # 对齐日期
    df = pd.DataFrame({'a': s1, 'b': s2}).dropna()
    if len(df) < min_overlap:
        return None, 0
    # 日收益率
    ret = df.pct_change().dropna()
    ret = ret.replace([np.inf, -np.inf], np.nan).dropna()
    if len(ret) < min_overlap:
        return None, 0
    r = ret['a'].corr(ret['b'])
    return round(r, 3), len(ret)


def main():
    # 加载相关性表
    with open(os.path.expanduser("~/Scripts/futures_correlation_table.json"), 'r') as f:
        table = json.load(f)

    # 收集所有需要的品种代码
    all_codes = set()
    for item in table['commodities']:
        all_codes.add(item['code'].split('/')[0])  # handle A/B
        for corr in item['correlations']:
            all_codes.add(corr['code'].split('/')[0])

    # 加载所有品种的日线数据
    print(f"正在加载 {len(all_codes)} 个品种的日线数据...")
    price_data = {}
    for code in sorted(all_codes):
        s = load_daily_close(code)
        if s is not None and len(s) > 100:
            price_data[code] = s
            print(f"  {code}: {len(s)} 日, {s.index.min().strftime('%Y-%m')} ~ {s.index.max().strftime('%Y-%m')}")
        else:
            print(f"  {code}: 数据不足或未找到")

    print(f"\n成功加载 {len(price_data)}/{len(all_codes)} 个品种")

    # 验证每对相关性
    print("\n" + "=" * 100)
    print(f"{'主品种':<8} {'关联品种':<8} {'查询r':>8} {'实测r':>8} {'差异':>8} {'重叠天数':>8} {'判定':<12}")
    print("=" * 100)

    results = []
    for item in table['commodities']:
        code_main = item['code'].split('/')[0]
        if code_main not in price_data:
            continue
        for corr in item['correlations']:
            code_sub = corr['code'].split('/')[0]
            if code_sub not in price_data:
                continue
            expected_r = corr['r']
            actual_r, overlap = compute_correlation(price_data[code_main], price_data[code_sub])
            if actual_r is None:
                continue
            diff = actual_r - expected_r
            if abs(diff) <= 0.15:
                verdict = "吻合"
            elif abs(diff) <= 0.25:
                verdict = "偏差中等"
            else:
                verdict = "偏差较大"

            print(f"{item['name']:<6} {corr['name']:<8} {expected_r:>8.2f} {actual_r:>8.3f} {diff:>+8.3f} {overlap:>8d} {verdict:<12}")
            results.append({
                'main': item['code'], 'main_name': item['name'],
                'sub': corr['code'], 'sub_name': corr['name'],
                'expected_r': expected_r, 'actual_r': actual_r,
                'diff': round(diff, 3), 'overlap_days': overlap,
                'verdict': verdict
            })

    # 统计
    print("\n" + "=" * 60)
    total = len(results)
    match = sum(1 for r in results if r['verdict'] == '吻合')
    medium = sum(1 for r in results if r['verdict'] == '偏差中等')
    large = sum(1 for r in results if r['verdict'] == '偏差较大')
    print(f"总计验证: {total} 对")
    print(f"  吻合 (|差异|≤0.15): {match} ({match/total*100:.1f}%)")
    print(f"  偏差中等 (0.15<|差异|≤0.25): {medium} ({medium/total*100:.1f}%)")
    print(f"  偏差较大 (|差异|>0.25): {large} ({large/total*100:.1f}%)")

    # 保存结果
    output = {
        'summary': {
            'total_pairs': total,
            'match': match, 'match_pct': round(match/total*100, 1),
            'medium_diff': medium, 'medium_pct': round(medium/total*100, 1),
            'large_diff': large, 'large_pct': round(large/total*100, 1),
        },
        'details': results
    }
    out_path = os.path.expanduser("~/Scripts/correlation_verification.json")
    with open(out_path, 'w') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存: {out_path}")


if __name__ == '__main__':
    main()
