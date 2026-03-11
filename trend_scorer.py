"""
趋势打分系统 - 用于判断期货短期方向 + 波动率状态，辅助宽跨式期权执行

两个独立维度：
A. 趋势方向分 (trend_score) → 决定先卖哪条腿
   1. 均线位置（MA5/10/20/40/60）
   2. 回调强度（回调到哪条均线止住）
   3. 布林线（中轨斜率 + 价格在带中位置）
   4. MACD（柱状图方向/加速度 + 金叉死叉）
   5. RSI（趋势方向 + 超买超卖）

   6. 成交量确认（量价配合）

B. 波动率状态 (vol_score) → 决定是否适合进场
   1. ATR百分位（当前ATR在过去120根K线中的排名）
   2. 布林带宽度百分位（当前带宽在过去120根中的排名）
   回测验证：ATR低位时未来60min波动比高位小30%+（AL/AU）

输出：趋势得分 + 波动率状态 + 执行建议
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class TrendResult:
    """打分结果"""
    score_1m: float          # 1分钟总分
    score_5m: float          # 5分钟总分
    total: float             # 加权总分
    ma_score: float          # 均线得分
    pullback_score: float    # 回调得分
    boll_score: float        # 布林得分
    macd_score: float        # MACD得分
    rsi_score: float         # RSI得分
    direction: str           # '上涨' / '下跌' / '震荡'
    advice: str              # 执行建议（中文）


# ============================================================
# 技术指标计算
# ============================================================

def calc_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def calc_sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(period).mean()


def calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def calc_macd(series: pd.Series, fast=12, slow=26, signal=9):
    """返回 (macd_line, signal_line, histogram)"""
    ema_fast = calc_ema(series, fast)
    ema_slow = calc_ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = calc_ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def calc_bollinger(series: pd.Series, period=20, std_mult=2.0):
    """返回 (mid, upper, lower)"""
    mid = calc_sma(series, period)
    std = series.rolling(period).std()
    return mid, mid + std_mult * std, mid - std_mult * std


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """给 OHLC DataFrame 添加所有指标列"""
    c = df['close'].astype(float)

    # 均线
    for p in [5, 10, 20, 40, 60]:
        df[f'ma{p}'] = calc_sma(c, p)

    # 布林线
    df['boll_mid'], df['boll_upper'], df['boll_lower'] = calc_bollinger(c, 20, 2.0)

    # MACD (用 Raschke 3-10-16 快参数)
    df['macd'], df['macd_signal'], df['macd_hist'] = calc_macd(c, 3, 10, 16)

    # RSI 14
    df['rsi'] = calc_rsi(c, 14)

    # 辅助列：最近5根K线最高/最低
    df['recent_high'] = df['high'].astype(float).rolling(5).max()
    df['recent_low'] = df['low'].astype(float).rolling(5).min()

    # ATR (14期)
    h = df['high'].astype(float)
    l = df['low'].astype(float)
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    df['atr14'] = tr.rolling(14).mean()
    df['atr_pct'] = df['atr14'] / c * 100  # ATR占价格百分比

    # 布林带宽度
    df['boll_width'] = (df['boll_upper'] - df['boll_lower']) / df['boll_mid'] * 100

    # 百分位排名 (过去120根K线中的位置, 0~100)
    df['atr_rank'] = df['atr_pct'].rolling(120).rank(pct=True) * 100
    df['boll_width_rank'] = df['boll_width'].rolling(120).rank(pct=True) * 100

    # 成交量相关
    vol = df['volume'].astype(float)
    df['vol_ma20'] = calc_sma(vol, 20)  # 20期成交量均线
    df['vol_ratio'] = vol / df['vol_ma20']  # 量比 (当前量/均量)
    # 连续涨跌判定 (用于量价配合)
    df['bar_dir'] = np.sign(c - c.shift(1))  # 1=涨, -1=跌, 0=平
    # 连续3根同方向且量递增的标记
    df['vol_up_3'] = (
        (df['bar_dir'] == 1) & (df['bar_dir'].shift(1) == 1) & (df['bar_dir'].shift(2) == 1) &
        (vol > vol.shift(1)) & (vol.shift(1) > vol.shift(2))
    )
    df['vol_dn_3'] = (
        (df['bar_dir'] == -1) & (df['bar_dir'].shift(1) == -1) & (df['bar_dir'].shift(2) == -1) &
        (vol > vol.shift(1)) & (vol.shift(1) > vol.shift(2))
    )

    # 量价背离: 价格创5周期新高/新低但成交量在缩
    vol_high5 = vol.rolling(5).max()
    df['vol_diverge_up'] = (c == c.rolling(5).max()) & (vol < vol_high5 * 0.7)
    df['vol_diverge_dn'] = (c == c.rolling(5).min()) & (vol < vol_high5 * 0.7)

    return df


def resample_to_5min(df: pd.DataFrame) -> pd.DataFrame:
    """将1分钟数据重采样为5分钟"""
    df5 = df.set_index('datetime').resample('5min').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
    }).dropna()
    df5 = df5.reset_index()
    return add_indicators(df5)


# ============================================================
# 各维度打分函数
# ============================================================

def score_ma_position(price, ma5, ma10, ma20, ma40, ma60) -> float:
    """均线位置得分 [-6, +6]"""
    if any(np.isnan(x) for x in [ma5, ma10, ma20, ma40, ma60]):
        return 0.0
    score = 0.0
    # 价格 vs 各均线
    if price > ma5:  score += 1.0
    if price > ma10: score += 1.0
    if price > ma20: score += 1.0
    if price > ma40: score += 0.5
    if price > ma60: score += 0.5
    if price < ma5:  score -= 1.0
    if price < ma10: score -= 1.0
    if price < ma20: score -= 1.0
    if price < ma40: score -= 0.5
    if price < ma60: score -= 0.5
    # 均线排列
    if ma5 > ma10 > ma20:   score += 1.5
    elif ma5 > ma10:         score += 0.5
    if ma5 < ma10 < ma20:   score -= 1.5
    elif ma5 < ma10:         score -= 0.5
    return score


def score_pullback(price, ma5, ma10, ma20, recent_low, recent_high) -> float:
    """回调强度得分 [-2, +2]"""
    if any(np.isnan(x) for x in [ma5, ma10, ma20, recent_low, recent_high]):
        return 0.0
    score = 0.0
    tol = 0.001  # 容差

    # 上涨中回调
    if price > ma20:
        if recent_low >= ma5 * (1 - tol):
            score += 2.0
        elif recent_low >= ma10 * (1 - tol):
            score += 1.5
        elif recent_low >= ma20 * (1 - tol):
            score += 1.0
        else:
            score -= 0.5

    # 下跌中反弹
    if price < ma20:
        if recent_high <= ma5 * (1 + tol):
            score -= 2.0
        elif recent_high <= ma10 * (1 + tol):
            score -= 1.5
        elif recent_high <= ma20 * (1 + tol):
            score -= 1.0
        else:
            score += 0.5

    return score


def score_bollinger(price, boll_mid, boll_upper, boll_lower,
                    boll_mid_prev10) -> float:
    """布林线得分 [-3, +3]"""
    if any(np.isnan(x) for x in [boll_mid, boll_upper, boll_lower, boll_mid_prev10]):
        return 0.0
    score = 0.0

    # 中轨斜率
    slope_pct = (boll_mid - boll_mid_prev10) / boll_mid_prev10 * 100
    if slope_pct > 0.15:     score += 2.0
    elif slope_pct > 0.05:   score += 1.0
    elif slope_pct > -0.05:  pass
    elif slope_pct > -0.15:  score -= 1.0
    else:                    score -= 2.0

    # 价格在带中位置
    bw = boll_upper - boll_lower
    if bw > 0:
        pos = (price - boll_lower) / bw
        if pos > 0.9:   score += 1.0
        elif pos > 0.7: score += 0.5
        elif pos < 0.1: score -= 1.0
        elif pos < 0.3: score -= 0.5

    return score


def score_macd(hist, hist_prev, macd_line, signal_line) -> float:
    """MACD得分 [-2.5, +2.5]"""
    if any(np.isnan(x) for x in [hist, hist_prev, macd_line, signal_line]):
        return 0.0
    score = 0.0

    if hist > 0:
        score += 0.5
        if hist > hist_prev: score += 1.0
        else:                score -= 0.5
    elif hist < 0:
        score -= 0.5
        if hist < hist_prev: score -= 1.0
        else:                score += 0.5

    # 金叉/死叉
    if macd_line > signal_line and hist_prev <= 0:
        score += 1.0
    elif macd_line < signal_line and hist_prev >= 0:
        score -= 1.0

    return score


def score_rsi(rsi, rsi_prev) -> float:
    """RSI得分 [-2.5, +2.5]"""
    if any(np.isnan(x) for x in [rsi, rsi_prev]):
        return 0.0
    score = 0.0

    if rsi > 50:   score += 0.5
    else:          score -= 0.5

    if rsi > 80:   score -= 1.5
    elif rsi > 70: score -= 0.5
    elif rsi < 20: score += 1.5
    elif rsi < 30: score += 0.5

    if rsi > rsi_prev: score += 0.5
    else:              score -= 0.5

    return score


def score_volatility(atr_rank, boll_width_rank, vol_ratio=1.0,
                     vol_up_3=False, vol_dn_3=False,
                     vol_diverge_up=False, vol_diverge_dn=False) -> dict:
    """
    波动率状态评估 (含成交量信号)。

    基础: ATR百分位(70%) + 布林带宽度百分位(30%)
    成交量修正 (回测验证):
    - 缩量(量比<0.5) → vol_score -10 (更平静，有利进场)
    - 连续放量 → vol_score +15 (波动加剧，避开)
    - 量价背离 → vol_score -8 (趋势衰竭，更平静)

    返回:
      vol_score: 0~100 (0=最平静, 100=最剧烈)
      vol_state: CALM/LOW/NORMAL/ACTIVE/VOLATILE
      entry_ok: 是否适合进场卖宽跨
    """
    if np.isnan(atr_rank) or np.isnan(boll_width_rank):
        return {'vol_score': 50, 'vol_state': 'NORMAL', 'entry_ok': True}

    # 基础分: ATR权重70%, 布林带宽度30%
    vol_score = atr_rank * 0.7 + boll_width_rank * 0.3

    # 成交量修正
    try:
        if not np.isnan(vol_ratio):
            if vol_ratio < 0.5:
                vol_score -= 10  # 缩量=更平静
            elif vol_ratio > 2.0:
                vol_score += 5   # 放量=更活跃
    except (TypeError, ValueError):
        pass

    if vol_up_3 or vol_dn_3:
        vol_score += 15  # 连续放量=波动加剧警告

    if vol_diverge_up or vol_diverge_dn:
        vol_score -= 8   # 量价背离=趋势衰竭，波动将降低

    # 限制范围
    vol_score = max(0, min(100, vol_score))

    if vol_score <= 20:
        state = 'CALM'
        entry_ok = True
    elif vol_score <= 40:
        state = 'LOW'
        entry_ok = True
    elif vol_score <= 60:
        state = 'NORMAL'
        entry_ok = True
    elif vol_score <= 80:
        state = 'ACTIVE'
        entry_ok = False
    else:
        state = 'VOLATILE'
        entry_ok = False

    return {'vol_score': round(vol_score, 1), 'vol_state': state, 'entry_ok': entry_ok}


# ============================================================
# 主打分函数
# ============================================================

def score_single_bar(row, prev_row, boll_mid_prev10) -> dict:
    """对单根K线打分，返回各维度分数"""
    price = float(row['close'])

    ma = score_ma_position(
        price, row['ma5'], row['ma10'], row['ma20'], row['ma40'], row['ma60'])
    pb = score_pullback(
        price, row['ma5'], row['ma10'], row['ma20'],
        row['recent_low'], row['recent_high'])
    bl = score_bollinger(
        price, row['boll_mid'], row['boll_upper'], row['boll_lower'],
        boll_mid_prev10)
    mc = score_macd(
        row['macd_hist'],
        prev_row['macd_hist'] if prev_row is not None else 0,
        row['macd'], row['macd_signal'])
    rs = score_rsi(
        row['rsi'],
        prev_row['rsi'] if prev_row is not None else 50)

    # 趋势总分 (不含成交量 — 回测验证成交量对方向判断无帮助)
    total = ma + pb + bl + mc + rs

    # 辅助函数
    def _get(r, col, default):
        if hasattr(r, 'get'):
            v = r.get(col, default)
        else:
            v = getattr(r, col, default)
        try:
            if np.isnan(v): return default
        except (TypeError, ValueError):
            pass
        return v

    # 波动率评估 (含成交量信号 — 回测验证成交量对波动率预测有效)
    vol = score_volatility(
        atr_rank=_get(row, 'atr_rank', 50),
        boll_width_rank=_get(row, 'boll_width_rank', 50),
        vol_ratio=_get(row, 'vol_ratio', 1.0),
        vol_up_3=_get(row, 'vol_up_3', False),
        vol_dn_3=_get(row, 'vol_dn_3', False),
        vol_diverge_up=_get(row, 'vol_diverge_up', False),
        vol_diverge_dn=_get(row, 'vol_diverge_dn', False),
    )

    return {
        'ma_score': ma, 'pullback_score': pb, 'boll_score': bl,
        'macd_score': mc, 'rsi_score': rs, 'total_1m': total,
        'vol_score': vol['vol_score'], 'vol_state': vol['vol_state'],
        'entry_ok': vol['entry_ok'],
    }


def score_dataframe(df_1m: pd.DataFrame) -> pd.DataFrame:
    """
    对整个 DataFrame 批量打分。
    df_1m: 含 OHLCV 的1分钟数据，列名 datetime/open/high/low/close/volume
    返回: 添加了 score 列的 DataFrame
    """
    df = df_1m.copy()
    df = add_indicators(df)

    # 5分钟数据
    df5 = resample_to_5min(df_1m.copy())

    # 逐行打分（1分钟）
    scores = []
    for i in range(len(df)):
        row = df.iloc[i]
        prev = df.iloc[i-1] if i > 0 else None
        boll_prev10 = df.iloc[i-10]['boll_mid'] if i >= 10 else row['boll_mid']
        s = score_single_bar(row, prev, boll_prev10)
        scores.append(s)

    score_df = pd.DataFrame(scores, index=df.index)
    for col in score_df.columns:
        df[col] = score_df[col].values

    # 5分钟打分（只用均线+布林）
    scores_5m = []
    for i in range(len(df5)):
        row5 = df5.iloc[i]
        boll_prev10_5 = df5.iloc[i-10]['boll_mid'] if i >= 10 else row5['boll_mid']
        ma5 = score_ma_position(
            float(row5['close']), row5['ma5'], row5['ma10'], row5['ma20'],
            row5['ma40'], row5['ma60'])
        bl5 = score_bollinger(
            float(row5['close']), row5['boll_mid'], row5['boll_upper'],
            row5['boll_lower'], boll_prev10_5)
        scores_5m.append({'datetime': row5['datetime'], 'score_5m': ma5 + bl5})

    df5_scores = pd.DataFrame(scores_5m)

    # 将5分钟得分映射回1分钟
    df['score_5m'] = 0.0
    if len(df5_scores) > 0:
        df5_scores = df5_scores.set_index('datetime')
        for i, row in df.iterrows():
            dt = row['datetime']
            # 找到最近的5分钟得分
            mask = df5_scores.index <= dt
            if mask.any():
                df.at[i, 'score_5m'] = df5_scores.loc[mask].iloc[-1]['score_5m']

    # 加权总分: 1分钟60% + 5分钟40%
    df['trend_score'] = df['total_1m'] * 0.6 + df['score_5m'] * 0.4

    # 方向判定
    def _direction(s):
        if s >= 2:  return '上涨'
        if s <= -2: return '下跌'
        return '震荡'

    df['direction'] = df['trend_score'].apply(_direction)

    # 执行建议 (结合趋势方向 + 波动率状态)
    def _advice(row):
        s = row['trend_score']
        vol_state = row.get('vol_state', 'NORMAL')
        entry_ok = row.get('entry_ok', True)

        # 波动率太高时，不管趋势方向都建议等待
        if vol_state == 'VOLATILE':
            return '波动过大·等待'
        if vol_state == 'ACTIVE':
            return '波动偏大·谨慎'

        # 正常波动率下按趋势方向建议
        if s >= 8:   return '等回调再卖沽'
        if s >= 5:   return '先卖沽·耐心等'
        if s >= 2:   return '先卖沽'
        if s >= -2:  return '同时卖'
        if s >= -5:  return '先卖购'
        if s >= -8:  return '先卖购·耐心等'
        return '等反弹再卖购'

    df['advice'] = df.apply(_advice, axis=1)

    return df


# ============================================================
# 数据加载
# ============================================================

def load_from_ctp(symbol: str, limit: int = 5000) -> pd.DataFrame:
    """从 CTP database.db 加载1分钟K线"""
    import sqlite3
    conn = sqlite3.connect('/Users/zhangxiaoyu/.vntrader/database.db')
    query = f"""
        SELECT datetime, open_price as open, high_price as high,
               low_price as low, close_price as close, volume
        FROM dbbardata
        WHERE symbol=? AND interval='1m'
        ORDER BY datetime
        LIMIT ?
    """
    df = pd.read_sql_query(query, conn, params=(symbol, limit))
    conn.close()
    df['datetime'] = pd.to_datetime(df['datetime'])
    return df


def load_from_parquet(path: str) -> pd.DataFrame:
    """从 parquet 加载（通用期货格式）"""
    df = pd.read_parquet(path)
    df = df.rename(columns={
        'open_price': 'open', 'high_price': 'high',
        'low_price': 'low', 'close_price': 'close'
    })
    if 'datetime' not in df.columns and df.index.name == 'datetime':
        df = df.reset_index()
    df['datetime'] = pd.to_datetime(df['datetime'])
    return df[['datetime', 'open', 'high', 'low', 'close', 'volume']]


# ============================================================
# CLI
# ============================================================

if __name__ == '__main__':
    import sys
    symbol = sys.argv[1] if len(sys.argv) > 1 else 'CF605'

    print(f'正在加载 {symbol} CTP数据...')
    df = load_from_ctp(symbol)
    print(f'  共{len(df)}根K线, {df["datetime"].iloc[0]} ~ {df["datetime"].iloc[-1]}')

    print('正在打分...')
    result = score_dataframe(df)

    # 统计
    total = len(result.dropna(subset=['trend_score']))
    for adv in ['波动过大·等待', '波动偏大·谨慎', '等回调再卖沽', '先卖沽·耐心等',
                '先卖沽', '同时卖', '先卖购', '先卖购·耐心等', '等反弹再卖购']:
        n = (result['advice'] == adv).sum()
        print(f'  {adv:>14}: {n:>5} ({n/total*100:5.1f}%)')

    # 波动率状态统计
    print('\n波动率状态:')
    for state in ['CALM', 'LOW', 'NORMAL', 'ACTIVE', 'VOLATILE']:
        n = (result['vol_state'] == state).sum()
        print(f'  {state:>10}: {n:>5} ({n/total*100:5.1f}%)')

    # 最近20条
    print(f'\n最近得分:')
    cols = ['datetime', 'close', 'trend_score', 'vol_score', 'vol_state', 'advice',
            'ma_score', 'boll_score', 'macd_score', 'rsi_score']
    print(result[cols].tail(20).to_string(index=False))
