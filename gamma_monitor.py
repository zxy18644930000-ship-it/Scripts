#!/usr/bin/env python3
"""
Gamma 风控外挂 v1.0

独立运行，不修改 trade2026 任何代码。
读取同一个数据库 (~/.vntrader/database.db)，实时计算：
  1. 每个期权对的 Greeks（Delta/Gamma/Theta/Vega）
  2. Theta/Gamma 比率 → 开仓安全性评估
  3. Gamma Exposure (GEX) → 组合风险监控
  4. 开仓建议：比率低于阈值时发出警告

用法:
  python3 gamma_monitor.py                    # 启动 Web 面板 (端口 8053)
  python3 gamma_monitor.py --check ag2604     # 命令行快速检查某品种
"""

import math
import json
import sqlite3
import os
import re
import sys
import logging
import threading
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple

DB_PATH = os.path.expanduser('~/.vntrader/database.db')
PAIRS_PATH = os.path.expanduser('~/Scripts/price_sum_pairs.json')
PORT = 8053
REFRESH_MS = 30_000  # 30秒刷新

logger = logging.getLogger(__name__)

# ============ 品种配置（从 trade2026 提取，只保留需要的字段）============

MULTIPLIERS = {
    'P': 10, 'TA': 5, 'AG': 15, 'EB': 10, 'CF': 5, 'SA': 20,
    'SP': 10, 'AO': 20, 'FG': 20, 'PG': 20, 'PS': 3, 'LC': 1,
    'Y': 10, 'SR': 10, 'RB': 10, 'MA': 10, 'OI': 10, 'LG': 90,
    'I': 100, 'CU': 5, 'AU': 1000, 'MO': 100, 'SC': 1000,
    'ZN': 5, 'AL': 5, 'V': 5, 'RU': 10, 'PF': 5, 'LH': 16,
    'SM': 5, 'SI': 5,
}

# 郑商所品种（月份3位，symbol大写）
CZCE_PRODUCTS = {'SA', 'FG', 'TA', 'MA', 'OI', 'RM', 'AP', 'CF', 'CJ',
                 'SR', 'PK', 'SM', 'SF', 'SH', 'UR', 'PF'}


# ============ Black-76 Greeks 计算 ============

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


@dataclass
class GreeksResult:
    """单腿期权的 Greeks"""
    price: float = 0.0        # 理论价格
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0        # 每天
    vega: float = 0.0         # 每1%IV变动
    volga: float = 0.0        # 每1%IV变动时Vega的变化量
    iv: float = 0.0           # 隐含波动率


@dataclass
class PairGreeks:
    """一对期权（Call+Put）的综合 Greeks"""
    call_sym: str = ''
    put_sym: str = ''
    futures_sym: str = ''
    futures_price: float = 0.0
    call_price: float = 0.0
    put_price: float = 0.0
    sum_price: float = 0.0
    call_strike: float = 0.0
    put_strike: float = 0.0
    dte: int = 0
    multiplier: int = 1
    # 单腿 Greeks
    call_greeks: GreeksResult = field(default_factory=GreeksResult)
    put_greeks: GreeksResult = field(default_factory=GreeksResult)
    # 组合 Greeks
    net_delta: float = 0.0       # Call Delta + Put Delta
    net_gamma: float = 0.0       # Call Gamma + Put Gamma（永远为正）
    net_theta: float = 0.0       # Call Theta + Put Theta（卖出时为正收入）
    net_vega: float = 0.0        # Call Vega + Put Vega
    net_volga: float = 0.0       # Call Volga + Put Volga
    # 关键比率
    theta_gamma_ratio: float = 0.0   # |Theta| / Gamma，越高越安全
    volga_vega_ratio: float = 0.0    # |Volga| / Vega，越高 IV 变动风险越大
    gex: float = 0.0                  # Gamma Exposure（元）
    # 评估
    signal: str = ''             # SAFE / CAUTION / DANGER
    reason: str = ''


def black76_greeks(F: float, K: float, T: float, sigma: float,
                   option_type: str = 'c', r: float = 0.02) -> GreeksResult:
    """
    Black-76 全套 Greeks 计算

    Args:
        F: 期货价格
        K: 行权价
        T: 到期时间（年）
        sigma: 隐含波动率（年化小数）
        option_type: 'c' 或 'p'
        r: 无风险利率
    """
    result = GreeksResult(iv=sigma)

    if T <= 1e-10 or sigma <= 1e-10 or F <= 0 or K <= 0:
        if option_type == 'c':
            result.delta = 1.0 if F > K else 0.0
            result.price = max(0.0, F - K)
        else:
            result.delta = -1.0 if K > F else 0.0
            result.price = max(0.0, K - F)
        return result

    sqrt_T = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    discount = math.exp(-r * T)
    pdf_d1 = _norm_pdf(d1)

    # Price
    if option_type == 'c':
        result.price = discount * (F * _norm_cdf(d1) - K * _norm_cdf(d2))
        result.delta = discount * _norm_cdf(d1)
    else:
        result.price = discount * (K * _norm_cdf(-d2) - F * _norm_cdf(-d1))
        result.delta = -discount * _norm_cdf(-d1)

    # Gamma（Call和Put相同）
    result.gamma = discount * pdf_d1 / (F * sigma * sqrt_T)

    # Theta（每天，注意是负数表示时间损耗）
    theta_annual = -discount * F * pdf_d1 * sigma / (2.0 * sqrt_T)
    if option_type == 'c':
        theta_annual += -r * discount * (F * _norm_cdf(d1) - K * _norm_cdf(d2))
    else:
        theta_annual += -r * discount * (K * _norm_cdf(-d2) - F * _norm_cdf(-d1))
    result.theta = theta_annual / 365.0

    # Vega（每1%IV变动 = sigma变0.01时价格变多少）
    vega_unit = discount * F * sqrt_T * pdf_d1  # per unit sigma
    result.vega = vega_unit * 0.01

    # Volga = dVega/dsigma（每1%IV变动时Vega的变化量）
    result.volga = vega_unit * d1 * d2 / sigma * 0.01

    return result


def implied_volatility(market_price: float, F: float, K: float, T: float,
                       option_type: str = 'c', r: float = 0.02) -> Optional[float]:
    """Newton-Raphson + 二分法求解IV"""
    if market_price <= 0 or T <= 1e-10 or F <= 0 or K <= 0:
        return None

    intrinsic = max(0.0, F - K) if option_type == 'c' else max(0.0, K - F)
    if market_price < intrinsic * 0.9:
        return None

    # Newton-Raphson
    sigma = 0.3
    for _ in range(50):
        try:
            g = black76_greeks(F, K, T, sigma, option_type, r)
            diff = g.price - market_price
            if abs(diff) < 1e-6:
                return sigma
            v = g.vega / 0.01  # 转回 per-unit vega
            if abs(v) < 1e-12:
                break
            sigma -= diff / v
            sigma = max(0.005, min(sigma, 10.0))
        except (ValueError, OverflowError):
            break

    # 二分法
    lo, hi = 0.005, 10.0
    for _ in range(200):
        mid = (lo + hi) / 2.0
        try:
            g = black76_greeks(F, K, T, mid, option_type, r)
        except (ValueError, OverflowError):
            hi = mid
            continue
        if abs(g.price - market_price) < 1e-6:
            return mid
        if g.price > market_price:
            hi = mid
        else:
            lo = mid
        if hi - lo < 1e-8:
            break

    result = (lo + hi) / 2.0
    return result if 0.005 < result < 10.0 else None


# ============ 数据库读取（线程安全）============

_thread_local = threading.local()


def get_db():
    conn = getattr(_thread_local, 'conn', None)
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        _thread_local.conn = conn
    return conn


def _resolve_symbol(sym: str, cur) -> str:
    """处理大商所短横线格式"""
    cur.execute("SELECT 1 FROM dbbardata WHERE symbol=? LIMIT 1", (sym,))
    if cur.fetchone():
        return sym
    m = re.match(r'([a-zA-Z]+\d{3,4})([CP])(\d+)', sym)
    if m:
        dash_sym = f'{m.group(1)}-{m.group(2)}-{m.group(3)}'
        cur.execute("SELECT 1 FROM dbbardata WHERE symbol=? LIMIT 1", (dash_sym,))
        if cur.fetchone():
            return dash_sym
    return sym


def _extract_futures_symbol(option_sym: str) -> Optional[str]:
    """ag2604C37600 -> ag2604, p2604-C-9000 -> p2604"""
    m = re.match(r'([a-zA-Z]+\d{3,4})[-CP]', option_sym)
    return m.group(1) if m else None


def _extract_strike(option_sym: str) -> Optional[float]:
    """ag2604C37600 -> 37600.0, p2604-C-9000 -> 9000.0"""
    m = re.search(r'[CP][-]?(\d+)$', option_sym.upper().replace('-', ''))
    return float(m.group(1)) if m else None


def _extract_option_type(option_sym: str) -> Optional[str]:
    """ag2604C37600 -> 'c', p2604-P-9000 -> 'p'"""
    m = re.search(r'([CP])', option_sym.upper().replace('-', ''))
    return m.group(1).lower() if m else None


def _extract_product_code(futures_sym: str) -> str:
    """ag2604 -> AG, p2604 -> P, TA604 -> TA"""
    m = re.match(r'([a-zA-Z]+)', futures_sym)
    return m.group(1).upper() if m else ''


def _estimate_dte(futures_sym: str) -> int:
    """估算 DTE（简化版，从 memory 中的规则）"""
    product = _extract_product_code(futures_sym)
    m = re.search(r'(\d{3,4})$', futures_sym)
    if not m:
        return 30

    month_str = m.group(1)
    if len(month_str) == 3:
        year = 2020 + int(month_str[0])
        month = int(month_str[1:])
    else:
        year = 2000 + int(month_str[:2])
        month = int(month_str[2:])

    # 到期月 = 合约月 - 1
    exp_month = month - 1
    exp_year = year
    if exp_month <= 0:
        exp_month = 12
        exp_year -= 1

    # 各交易所到期日近似
    if product in CZCE_PRODUCTS:
        exp_day = 11  # 15日前倒数第3个交易日
    elif product in {'P', 'Y', 'M', 'EB', 'PG', 'LG', 'I', 'V', 'LH'}:
        exp_day = 17  # 大商所：第12个交易日
    elif product in {'AG', 'AU', 'CU', 'ZN', 'AL', 'SP', 'AO', 'RU', 'RB'}:
        exp_day = 25  # 上期所：倒数第5个交易日
    elif product in {'SC'}:
        exp_day = 13  # 能源中心
    elif product in {'PS', 'LC', 'SI'}:
        exp_day = 6   # 广期所
    else:
        exp_day = 15

    try:
        exp_date = date(exp_year, exp_month, min(exp_day, 28))
    except ValueError:
        exp_date = date(exp_year, exp_month, 28)

    dte = (exp_date - date.today()).days
    return max(dte, 1)


def get_latest_price(sym: str) -> Optional[float]:
    """获取某合约的最新价格"""
    db = get_db()
    cur = db.cursor()
    sym = _resolve_symbol(sym, cur)
    cur.execute("""
        SELECT close_price FROM dbbardata
        WHERE symbol=? ORDER BY datetime DESC LIMIT 1
    """, (sym,))
    row = cur.fetchone()
    return row[0] if row else None


def get_recent_prices(sym: str, minutes: int = 60) -> List[Tuple[str, float]]:
    """获取最近N分钟的价格序列"""
    db = get_db()
    cur = db.cursor()
    sym = _resolve_symbol(sym, cur)
    now = datetime.now()
    start = (now - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("""
        SELECT datetime, close_price FROM dbbardata
        WHERE symbol=? AND datetime>=? ORDER BY datetime DESC LIMIT ?
    """, (sym, start, minutes))
    rows = cur.fetchall()
    return [(r[0], r[1]) for r in reversed(rows)]


# ============ 核心：计算期权对 Greeks ============

# Theta/Gamma 比率阈值
RATIO_SAFE = 50.0       # 比率 > 50: 安全（Theta 远大于 Gamma 风险）
RATIO_CAUTION = 20.0    # 比率 20-50: 注意
# 比率 < 20: 危险（Gamma 风险大，不建议卖出）

# GEX 阈值（元）
GEX_WARNING = 5000.0    # 单对 GEX 超过此值时警告


def calculate_pair_greeks(call_sym: str, put_sym: str,
                          lots: int = 1) -> Optional[PairGreeks]:
    """
    计算一对期权的完整 Greeks 和风控指标

    Args:
        call_sym: Call 合约代码
        put_sym: Put 合约代码
        lots: 持仓手数

    Returns:
        PairGreeks 或 None（数据不足时）
    """
    db = get_db()
    cur = db.cursor()

    # 解析 symbol
    call_sym_db = _resolve_symbol(call_sym, cur)
    put_sym_db = _resolve_symbol(put_sym, cur)
    futures_sym = _extract_futures_symbol(call_sym_db)
    if not futures_sym:
        return None

    # 获取最新价格
    call_price = get_latest_price(call_sym)
    put_price = get_latest_price(put_sym)
    futures_price = get_latest_price(futures_sym)

    if not all([call_price, put_price, futures_price]):
        return None
    if call_price <= 0 or put_price <= 0 or futures_price <= 0:
        return None

    # 解析行权价和类型
    call_strike = _extract_strike(call_sym_db)
    put_strike = _extract_strike(put_sym_db)
    if not call_strike or not put_strike:
        return None

    # DTE 和 T
    dte = _estimate_dte(futures_sym)
    T = dte / 365.0

    # 品种乘数
    product = _extract_product_code(futures_sym)
    multiplier = MULTIPLIERS.get(product, 10)

    # 求解 IV
    call_iv = implied_volatility(call_price, futures_price, call_strike, T, 'c')
    put_iv = implied_volatility(put_price, futures_price, put_strike, T, 'p')

    if call_iv is None or put_iv is None:
        return None

    # 计算 Greeks
    cg = black76_greeks(futures_price, call_strike, T, call_iv, 'c')
    pg = black76_greeks(futures_price, put_strike, T, put_iv, 'p')

    # 组合 Greeks
    net_delta = cg.delta + pg.delta
    net_gamma = cg.gamma + pg.gamma  # 永远为正
    net_theta = cg.theta + pg.theta  # 时间衰减（负数 = 买方成本 / 卖方收入）
    net_vega = cg.vega + pg.vega
    net_volga = cg.volga + pg.volga

    # Volga/Vega 比率（越高表示 IV 变动时 Vega 膨胀越快，卖方风险越大）
    volga_vega_ratio = abs(net_volga) / net_vega if net_vega > 1e-10 else 0.0

    # Theta/Gamma 比率
    # 对卖出方：Theta 收入（取绝对值）/ Gamma 风险
    # 物理含义：期货需要动多少点才能让 Gamma 亏损吃掉一天的 Theta 收益
    if net_gamma > 1e-15:
        # 简化：theta_gamma_ratio = |daily_theta| / (0.5 * gamma * F^2 * (1%move)^2)
        # 更直观：期货波动多少%才会让Gamma亏损=1天Theta
        # breakeven_move = sqrt(2 * |theta| / gamma) / F * 100%
        daily_theta_value = abs(net_theta) * multiplier * lots
        # Gamma PnL for 1% move = 0.5 * gamma * (F * 0.01)^2 * multiplier * lots
        gamma_1pct = 0.5 * net_gamma * (futures_price * 0.01) ** 2 * multiplier * lots
        theta_gamma_ratio = daily_theta_value / gamma_1pct if gamma_1pct > 1e-10 else 999.0

        # 盈亏平衡波动率（期货波动多少%，Gamma亏损=1天Theta）
        breakeven_pct = math.sqrt(2.0 * abs(net_theta) / net_gamma) / futures_price * 100.0
    else:
        theta_gamma_ratio = 999.0
        breakeven_pct = 99.0

    # GEX（Gamma Exposure，以元为单位）
    # 含义：期货涨1%时，Delta变化导致的额外PnL
    gex = net_gamma * (futures_price * 0.01) * futures_price * multiplier * lots

    # 评估信号
    if theta_gamma_ratio >= RATIO_SAFE:
        signal = 'SAFE'
        reason = f'T/G比={theta_gamma_ratio:.1f}，期货需波动{breakeven_pct:.1f}%才亏'
    elif theta_gamma_ratio >= RATIO_CAUTION:
        signal = 'CAUTION'
        reason = f'T/G比={theta_gamma_ratio:.1f}，盈亏平衡{breakeven_pct:.1f}%，注意风控'
    else:
        signal = 'DANGER'
        reason = f'T/G比={theta_gamma_ratio:.1f}，仅{breakeven_pct:.1f}%波动即亏损，不建议卖出'

    # Volga 补充警告（即使 T/G 安全，Volga 高也有 IV 爆炸风险）
    if volga_vega_ratio > 30:
        reason += f'  ⚠Volga/Vega={volga_vega_ratio:.0f}(爆发区!)'
        if signal == 'SAFE':
            signal = 'CAUTION'
    elif volga_vega_ratio > 15:
        reason += f'  Volga/Vega={volga_vega_ratio:.0f}(偏高)'

    return PairGreeks(
        call_sym=call_sym,
        put_sym=put_sym,
        futures_sym=futures_sym,
        futures_price=futures_price,
        call_price=call_price,
        put_price=put_price,
        sum_price=call_price + put_price,
        call_strike=call_strike,
        put_strike=put_strike,
        dte=dte,
        multiplier=multiplier,
        call_greeks=cg,
        put_greeks=pg,
        net_delta=net_delta,
        net_gamma=net_gamma,
        net_theta=net_theta,
        net_vega=net_vega,
        net_volga=net_volga,
        theta_gamma_ratio=theta_gamma_ratio,
        volga_vega_ratio=volga_vega_ratio,
        gex=gex,
        signal=signal,
        reason=reason,
    )


# ============ 全品种扫描 ============

def scan_all_pairs() -> List[PairGreeks]:
    """扫描 price_sum_pairs.json 中所有配对的 Greeks"""
    if not os.path.exists(PAIRS_PATH):
        return []

    with open(PAIRS_PATH, 'r') as f:
        pairs = json.load(f)

    results = []
    for pair in pairs:
        call_sym, put_sym = pair[0], pair[1]
        try:
            pg = calculate_pair_greeks(call_sym, put_sym)
            if pg:
                results.append(pg)
        except Exception as e:
            logger.warning(f"计算 {call_sym}+{put_sym} 失败: {e}")

    return results


def scan_product_pairs(futures_sym: str) -> List[PairGreeks]:
    """
    扫描某品种所有可交易的期权对

    自动从数据库中找出该品种的所有期权合约，组合成宽跨对并计算 Greeks。
    """
    db = get_db()
    cur = db.cursor()

    # 找出所有该品种的期权合约
    pattern = f'{futures_sym}%'
    dash_pattern = f'{futures_sym}-%'
    cur.execute("""
        SELECT DISTINCT symbol FROM dbbardata
        WHERE (symbol LIKE ? OR symbol LIKE ?)
        AND symbol != ?
        ORDER BY symbol
    """, (pattern, dash_pattern, futures_sym))

    symbols = [row[0] for row in cur.fetchall()]
    calls = []
    puts = []

    for sym in symbols:
        upper = sym.upper().replace('-', '')
        if 'C' in upper[len(futures_sym):]:
            calls.append(sym)
        elif 'P' in upper[len(futures_sym):]:
            puts.append(sym)

    # 按虚值程度配对（最远 Call + 最远 Put 等）
    results = []
    futures_price = get_latest_price(futures_sym)
    if not futures_price:
        return results

    # 过滤有成交的合约
    active_calls = []
    for sym in calls:
        price = get_latest_price(sym)
        if price and price > 0:
            strike = _extract_strike(sym)
            if strike and strike > futures_price:  # OTM Call
                active_calls.append((sym, strike, price))

    active_puts = []
    for sym in puts:
        price = get_latest_price(sym)
        if price and price > 0:
            strike = _extract_strike(sym)
            if strike and strike < futures_price:  # OTM Put
                active_puts.append((sym, strike, price))

    # 按行权价排序
    active_calls.sort(key=lambda x: x[1])   # 从近到远
    active_puts.sort(key=lambda x: x[1], reverse=True)  # 从近到远

    # 配对：对称配对（虚值程度相近的 Call+Put）
    for i, (c_sym, c_strike, c_price) in enumerate(active_calls):
        c_otm_pct = (c_strike - futures_price) / futures_price
        # 找虚值度最接近的 Put
        best_put = None
        best_diff = float('inf')
        for p_sym, p_strike, p_price in active_puts:
            p_otm_pct = (futures_price - p_strike) / futures_price
            diff = abs(c_otm_pct - p_otm_pct)
            if diff < best_diff:
                best_diff = diff
                best_put = (p_sym, p_strike, p_price)

        if best_put and best_diff < 0.05:
            try:
                pg = calculate_pair_greeks(c_sym, best_put[0])
                if pg:
                    results.append(pg)
            except Exception as e:
                logger.warning(f"计算 {c_sym}+{best_put[0]} 失败: {e}")

    return results


# ============ 命令行模式 ============

def print_pair_greeks(pg: PairGreeks):
    """打印单对 Greeks 详情"""
    signal_color = {'SAFE': '\033[92m', 'CAUTION': '\033[93m', 'DANGER': '\033[91m'}
    reset = '\033[0m'
    color = signal_color.get(pg.signal, '')

    print(f"\n{'='*70}")
    print(f"  {pg.call_sym} + {pg.put_sym}")
    print(f"  期货: {pg.futures_sym} = {pg.futures_price:.1f}   DTE: {pg.dte}天")
    print(f"{'='*70}")

    print(f"\n  {'':>12} {'Call':>12} {'Put':>12} {'合计':>12}")
    print(f"  {'─'*50}")
    print(f"  {'行权价':>12} {pg.call_strike:>12.0f} {pg.put_strike:>12.0f}")
    print(f"  {'市场价':>12} {pg.call_price:>12.1f} {pg.put_price:>12.1f} {pg.sum_price:>12.1f}")
    print(f"  {'IV':>12} {pg.call_greeks.iv*100:>11.1f}% {pg.put_greeks.iv*100:>11.1f}%")
    print(f"  {'Delta':>12} {pg.call_greeks.delta:>12.4f} {pg.put_greeks.delta:>12.4f} {pg.net_delta:>12.4f}")
    print(f"  {'Gamma':>12} {pg.call_greeks.gamma:>12.6f} {pg.put_greeks.gamma:>12.6f} {pg.net_gamma:>12.6f}")
    print(f"  {'Theta/天':>12} {pg.call_greeks.theta:>12.4f} {pg.put_greeks.theta:>12.4f} {pg.net_theta:>12.4f}")
    print(f"  {'Vega/1%':>12} {pg.call_greeks.vega:>12.4f} {pg.put_greeks.vega:>12.4f} {pg.net_vega:>12.4f}")
    print(f"  {'Volga/1%':>12} {pg.call_greeks.volga:>12.4f} {pg.put_greeks.volga:>12.4f} {pg.net_volga:>12.4f}")

    print(f"\n  {'─'*50}")
    print(f"  虚值度(Call): {(pg.call_strike - pg.futures_price)/pg.futures_price*100:+.1f}%")
    print(f"  虚值度(Put):  {(pg.futures_price - pg.put_strike)/pg.futures_price*100:+.1f}%")

    # Volga/Vega 颜色
    vv_color = '\033[91m' if pg.volga_vega_ratio > 30 else '\033[93m' if pg.volga_vega_ratio > 15 else '\033[92m'

    print(f"\n  ┌────────────────────────────────────────────────┐")
    print(f"  │ {color}信号: {pg.signal}{reset}{'':>{42-len(pg.signal)}}│")
    print(f"  │ Theta/Gamma 比率: {pg.theta_gamma_ratio:>8.1f}{'':>20}│")
    print(f"  │ {vv_color}Volga/Vega 比率:  {pg.volga_vega_ratio:>8.1f}{reset}{'':>20}│")

    # 盈亏平衡
    if pg.net_gamma > 1e-15:
        be = math.sqrt(2.0 * abs(pg.net_theta) / pg.net_gamma) / pg.futures_price * 100
        be_points = be * pg.futures_price / 100
        print(f"  │ 盈亏平衡波动: {be:>6.2f}% ({be_points:.0f}点){'':>14}│")

    print(f"  │ GEX(1手): {pg.gex:>10.1f} 元{'':>23}│")
    print(f"  │ {pg.reason[:46]:<46}│")
    print(f"  └────────────────────────────────────────────────┘")


def cli_check(query: str):
    """命令行检查模式"""
    # 检查是否是品种代码（如 ag2604）
    if re.match(r'^[a-zA-Z]+\d{3,4}$', query):
        print(f"\n扫描 {query} 所有可交易期权对...")
        results = scan_product_pairs(query)
        if not results:
            print(f"  未找到 {query} 的有效期权对（数据库中无数据或无成交）")
            return

        # 按 Theta/Gamma 比率排序
        results.sort(key=lambda x: x.theta_gamma_ratio, reverse=True)

        print(f"\n找到 {len(results)} 个期权对，按安全性排序：\n")
        for pg in results:
            print_pair_greeks(pg)
    else:
        # 检查 pairs.json 中的配对
        print(f"\n扫描工作台中所有期权对...")
        results = scan_all_pairs()
        if not results:
            print("  工作台中无有效期权对")
            return
        for pg in results:
            print_pair_greeks(pg)


# ============ Dash Web 面板 ============

def create_dash_app():
    """创建 Dash Web 应用"""
    from dash import Dash, dcc, html, ctx
    from dash.dependencies import Input, Output
    import plotly.graph_objects as go

    app = Dash(__name__, suppress_callback_exceptions=True)

    app.layout = html.Div([
        # 顶部标题
        html.Div([
            html.H2('Gamma 风控面板', style={'margin': '0', 'color': '#fff', 'display': 'inline-block'}),
            html.Span('  Theta/Gamma 比率 | GEX 监控 | 开仓安全性评估',
                       style={'color': '#aaa', 'fontSize': '14px', 'marginLeft': '15px'}),
            html.Span(id='last-update', style={'color': '#666', 'fontSize': '12px', 'float': 'right', 'marginTop': '8px'}),
        ], style={'backgroundColor': '#1a1a2e', 'padding': '15px 25px',
                  'borderBottom': '3px solid #00ff88'}),

        # 阈值说明
        html.Div([
            html.Span('SAFE', style={'color': '#00ff88', 'fontWeight': 'bold', 'marginRight': '5px'}),
            html.Span(f'比率>{RATIO_SAFE}', style={'color': '#888', 'marginRight': '20px'}),
            html.Span('CAUTION', style={'color': '#ffd700', 'fontWeight': 'bold', 'marginRight': '5px'}),
            html.Span(f'比率{RATIO_CAUTION}-{RATIO_SAFE}', style={'color': '#888', 'marginRight': '20px'}),
            html.Span('DANGER', style={'color': '#ff4444', 'fontWeight': 'bold', 'marginRight': '5px'}),
            html.Span(f'比率<{RATIO_CAUTION}', style={'color': '#888'}),
        ], style={'padding': '10px 25px', 'backgroundColor': '#16213e', 'borderBottom': '1px solid #2a2a4a'}),

        # Greeks 卡片区
        html.Div(id='greeks-cards'),

        # 定时刷新
        dcc.Interval(id='timer', interval=REFRESH_MS, n_intervals=0),

    ], style={'backgroundColor': '#0f0f23', 'minHeight': '100vh', 'fontFamily': 'monospace'})

    @app.callback(
        Output('greeks-cards', 'children'),
        Output('last-update', 'children'),
        Input('timer', 'n_intervals'),
    )
    def update_cards(_):
        results = scan_all_pairs()
        now_str = datetime.now().strftime('%H:%M:%S')

        if not results:
            return html.Div('工作台中无有效期权对（检查 price_sum_pairs.json）',
                            style={'color': '#666', 'padding': '50px', 'textAlign': 'center'}), f'更新: {now_str}'

        cards = []
        for pg in results:
            signal_colors = {
                'SAFE': {'bg': '#0d3320', 'border': '#00ff88', 'text': '#00ff88'},
                'CAUTION': {'bg': '#332d0d', 'border': '#ffd700', 'text': '#ffd700'},
                'DANGER': {'bg': '#330d0d', 'border': '#ff4444', 'text': '#ff4444'},
            }
            sc = signal_colors.get(pg.signal, signal_colors['CAUTION'])

            # 盈亏平衡
            if pg.net_gamma > 1e-15:
                be_pct = math.sqrt(2.0 * abs(pg.net_theta) / pg.net_gamma) / pg.futures_price * 100
                be_points = be_pct * pg.futures_price / 100
                be_text = f'{be_pct:.2f}% ({be_points:.0f}点)'
            else:
                be_text = 'N/A'

            card = html.Div([
                # 标题行：合约 + 信号
                html.Div([
                    html.Span(f'{pg.call_sym} + {pg.put_sym}',
                              style={'color': '#fff', 'fontSize': '16px', 'fontWeight': 'bold'}),
                    html.Span(f'  {pg.futures_sym}={pg.futures_price:.0f}  DTE={pg.dte}天',
                              style={'color': '#888', 'fontSize': '13px', 'marginLeft': '15px'}),
                    html.Span(f' {pg.signal} ',
                              style={'color': sc['text'], 'fontWeight': 'bold', 'fontSize': '14px',
                                     'float': 'right', 'border': f'1px solid {sc["border"]}',
                                     'padding': '2px 10px', 'borderRadius': '3px'}),
                ], style={'padding': '12px 20px', 'backgroundColor': '#1a1a2e'}),

                # Greeks 表格
                html.Div([
                    html.Table([
                        html.Thead(html.Tr([
                            html.Th('', style={'width': '100px'}),
                            html.Th('Call', style={'textAlign': 'right', 'color': '#00BFFF'}),
                            html.Th('Put', style={'textAlign': 'right', 'color': '#FF6B6B'}),
                            html.Th('合计', style={'textAlign': 'right', 'color': '#FFD700'}),
                        ])),
                        html.Tbody([
                            html.Tr([
                                html.Td('价格'),
                                html.Td(f'{pg.call_price:.1f}', style={'textAlign': 'right'}),
                                html.Td(f'{pg.put_price:.1f}', style={'textAlign': 'right'}),
                                html.Td(f'{pg.sum_price:.1f}', style={'textAlign': 'right', 'fontWeight': 'bold'}),
                            ]),
                            html.Tr([
                                html.Td(f'IV'),
                                html.Td(f'{pg.call_greeks.iv*100:.1f}%', style={'textAlign': 'right'}),
                                html.Td(f'{pg.put_greeks.iv*100:.1f}%', style={'textAlign': 'right'}),
                                html.Td(''),
                            ]),
                            html.Tr([
                                html.Td('Delta'),
                                html.Td(f'{pg.call_greeks.delta:.4f}', style={'textAlign': 'right'}),
                                html.Td(f'{pg.put_greeks.delta:.4f}', style={'textAlign': 'right'}),
                                html.Td(f'{pg.net_delta:+.4f}', style={'textAlign': 'right', 'fontWeight': 'bold'}),
                            ]),
                            html.Tr([
                                html.Td('Gamma', style={'color': '#ff8800', 'fontWeight': 'bold'}),
                                html.Td(f'{pg.call_greeks.gamma:.6f}', style={'textAlign': 'right'}),
                                html.Td(f'{pg.put_greeks.gamma:.6f}', style={'textAlign': 'right'}),
                                html.Td(f'{pg.net_gamma:.6f}', style={'textAlign': 'right', 'fontWeight': 'bold', 'color': '#ff8800'}),
                            ]),
                            html.Tr([
                                html.Td('Theta/天'),
                                html.Td(f'{pg.call_greeks.theta:.4f}', style={'textAlign': 'right'}),
                                html.Td(f'{pg.put_greeks.theta:.4f}', style={'textAlign': 'right'}),
                                html.Td(f'{pg.net_theta:.4f}', style={'textAlign': 'right', 'fontWeight': 'bold'}),
                            ]),
                            html.Tr([
                                html.Td('Vega/1%'),
                                html.Td(f'{pg.call_greeks.vega:.4f}', style={'textAlign': 'right'}),
                                html.Td(f'{pg.put_greeks.vega:.4f}', style={'textAlign': 'right'}),
                                html.Td(f'{pg.net_vega:.4f}', style={'textAlign': 'right', 'fontWeight': 'bold'}),
                            ]),
                            html.Tr([
                                html.Td('Volga/1%', style={'color': '#b388ff'}),
                                html.Td(f'{pg.call_greeks.volga:.4f}', style={'textAlign': 'right'}),
                                html.Td(f'{pg.put_greeks.volga:.4f}', style={'textAlign': 'right'}),
                                html.Td(f'{pg.net_volga:.4f}', style={'textAlign': 'right', 'fontWeight': 'bold',
                                         'color': '#b388ff'}),
                            ]),
                        ]),
                    ], style={'width': '100%', 'color': '#ddd', 'fontSize': '13px',
                              'borderCollapse': 'collapse'}),
                ], style={'padding': '5px 20px'}),

                # 风控指标
                html.Div([
                    html.Div([
                        html.Div([
                            html.Div('T/G 比率', style={'color': '#888', 'fontSize': '11px'}),
                            html.Div(f'{pg.theta_gamma_ratio:.1f}',
                                     style={'color': sc['text'], 'fontSize': '28px', 'fontWeight': 'bold'}),
                        ], style={'display': 'inline-block', 'width': '30%', 'textAlign': 'center'}),
                        html.Div([
                            html.Div('盈亏平衡波动', style={'color': '#888', 'fontSize': '11px'}),
                            html.Div(be_text,
                                     style={'color': '#fff', 'fontSize': '18px', 'fontWeight': 'bold'}),
                        ], style={'display': 'inline-block', 'width': '30%', 'textAlign': 'center'}),
                        html.Div([
                            html.Div('Volga/Vega', style={'color': '#888', 'fontSize': '11px'}),
                            html.Div(f'{pg.volga_vega_ratio:.1f}',
                                     style={'color': '#ff4444' if pg.volga_vega_ratio > 30
                                            else '#ffd700' if pg.volga_vega_ratio > 15
                                            else '#00ff88',
                                            'fontSize': '18px', 'fontWeight': 'bold'}),
                        ], style={'display': 'inline-block', 'width': '20%', 'textAlign': 'center'}),
                        html.Div([
                            html.Div('GEX (1手)', style={'color': '#888', 'fontSize': '11px'}),
                            html.Div(f'{pg.gex:.0f}元',
                                     style={'color': '#ff8800' if abs(pg.gex) > GEX_WARNING else '#fff',
                                            'fontSize': '18px', 'fontWeight': 'bold'}),
                        ], style={'display': 'inline-block', 'width': '20%', 'textAlign': 'center'}),
                    ]),
                    html.Div(pg.reason, style={'color': '#aaa', 'fontSize': '12px',
                                                'textAlign': 'center', 'marginTop': '8px'}),
                ], style={'padding': '15px 20px', 'backgroundColor': sc['bg'],
                          'borderTop': f'1px solid {sc["border"]}',
                          'borderBottom': f'2px solid {sc["border"]}'}),

            ], style={'marginBottom': '15px', 'backgroundColor': '#16213e',
                      'border': f'1px solid {sc["border"]}', 'borderRadius': '5px',
                      'margin': '15px 25px'})

            cards.append(card)

        return cards, f'更新: {now_str}'

    return app


# ============ 入口 ============

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

    if len(sys.argv) > 1 and sys.argv[1] == '--check':
        query = sys.argv[2] if len(sys.argv) > 2 else 'all'
        cli_check(query)
    else:
        app = create_dash_app()
        print(f'Gamma 风控面板: http://localhost:{PORT}')
        print(f'阈值: SAFE>{RATIO_SAFE}, CAUTION>{RATIO_CAUTION}, DANGER<{RATIO_CAUTION}')
        print(f'数据源: {DB_PATH}')
        print(f'配对源: {PAIRS_PATH}')
        app.run(host='0.0.0.0', port=PORT, debug=False)
