#!/usr/bin/env python3
"""
课时5: 波动率微笑 (Volatility Smile)
期权量化交易学习系统 — 交互式网页课程

Usage: python3 ~/Scripts/lesson5_smile.py
Opens browser at http://localhost:8060
"""

import json
import os
import sys
import threading
import http.server
import webbrowser
from math import log, sqrt, exp, pi, erf
from functools import partial

PORT = 8060
OUTPUT_HTML = '/tmp/lesson5_smile.html'

# ========== 数学工具 ==========

def norm_cdf(x):
    return 0.5 * (1 + erf(x / sqrt(2)))

def norm_pdf(x):
    return exp(-0.5 * x * x) / sqrt(2 * pi)

# ========== SVI 波动率模型 ==========

def svi_total_variance(k, a, b, rho, m, sig):
    """SVI参数化: w(k) = a + b*(ρ(k-m) + √((k-m)²+σ²))
    k = log(K/F) (log-moneyness)
    返回 total variance w = IV² × T
    """
    return a + b * (rho * (k - m) + sqrt((k - m)**2 + sig**2))

def svi_iv(k, a, b, rho, m, sig, T):
    """SVI → IV (年化百分比)"""
    w = svi_total_variance(k, a, b, rho, m, sig)
    if w <= 0 or T <= 0:
        return None
    return sqrt(w / T) * 100

# ========== 数据生成 ==========

def gen_moneyness_range(lo=-0.20, hi=0.20, step=0.005):
    """生成 log-moneyness 序列"""
    n = int((hi - lo) / step) + 1
    return [round(lo + i * step, 4) for i in range(n)]

def generate_theory_data():
    """生成理论对比数据: BS平坦 vs 三种微笑形状"""
    ks = gen_moneyness_range(-0.15, 0.15, 0.005)
    T = 30 / 365

    # BS理想世界: 平坦
    flat = [{'k': k, 'iv': 25.0} for k in ks]

    # 商品U型微笑 (近对称)
    commodity = []
    for k in ks:
        iv = svi_iv(k, a=0.018, b=0.12, rho=-0.05, m=0.0, sig=0.08, T=T)
        commodity.append({'k': k, 'iv': round(iv, 2) if iv else 25.0})

    # 股指负偏斜 (OTM Put IV >> OTM Call IV)
    equity = []
    for k in ks:
        iv = svi_iv(k, a=0.016, b=0.18, rho=-0.55, m=0.02, sig=0.06, T=T)
        equity.append({'k': k, 'iv': round(iv, 2) if iv else 25.0})

    # 正偏斜 (OTM Call IV > OTM Put IV, 某些农产品)
    reverse = []
    for k in ks:
        iv = svi_iv(k, a=0.016, b=0.14, rho=0.40, m=-0.01, sig=0.06, T=T)
        reverse.append({'k': k, 'iv': round(iv, 2) if iv else 25.0})

    return {
        'moneyness': ks,
        'flat': flat,
        'commodity': commodity,
        'equity': equity,
        'reverse': reverse,
    }

def generate_ag_smile():
    """白银(ag)波动率微笑 — 多DTE对比"""
    F = 8200
    strikes = list(range(6200, 10400, 100))
    dtes = [7, 30, 60, 90]
    curves = []

    for dte in dtes:
        T = dte / 365
        calls, puts = [], []
        for K in strikes:
            k = log(K / F)
            # Ag: 略负偏斜, 低DTE微笑更尖锐
            base_a = 0.015 + 0.02 * max(0, (30 - dte) / 30)
            base_b = 0.10 + 0.06 * max(0, (30 - dte) / 30)
            rho = -0.18
            sig = 0.07 + 0.03 * max(0, (30 - dte) / 30)

            iv_c = svi_iv(k, a=base_a, b=base_b, rho=rho, m=0.005, sig=sig, T=T)
            iv_p = svi_iv(k, a=base_a * 1.02, b=base_b * 1.05, rho=rho - 0.03, m=0.005, sig=sig, T=T)

            mn = round(K / F, 3)
            if iv_c and 5 < iv_c < 80:
                calls.append({'strike': K, 'mn': mn, 'iv': round(iv_c, 2)})
            if iv_p and 5 < iv_p < 80:
                puts.append({'strike': K, 'mn': mn, 'iv': round(iv_p, 2)})

        curves.append({
            'dte': dte,
            'label': f'DTE={dte}天',
            'calls': calls,
            'puts': puts,
        })

    return {'futures': F, 'curves': curves}

def generate_cf_sa_comparison():
    """CF vs SA 微笑形状对比"""
    ks = gen_moneyness_range(-0.12, 0.12, 0.004)
    T = 30 / 365

    cf_smile, sa_smile = [], []
    for k in ks:
        # CF: 波动大, 微笑更陡, 略正偏(农产品供给冲击)
        iv_cf = svi_iv(k, a=0.012, b=0.16, rho=0.12, m=-0.005, sig=0.10, T=T)
        if iv_cf and 5 < iv_cf < 80:
            cf_smile.append({'k': k, 'iv': round(iv_cf, 2)})

        # SA: 波动小, 微笑平缓, 近对称
        iv_sa = svi_iv(k, a=0.018, b=0.06, rho=-0.05, m=0.0, sig=0.05, T=T)
        if iv_sa and 5 < iv_sa < 80:
            sa_smile.append({'k': k, 'iv': round(iv_sa, 2)})

    return {'cf': cf_smile, 'sa': sa_smile}

def generate_strangle_annotation():
    """卖出宽跨在微笑曲线上的位置标注"""
    ks = gen_moneyness_range(-0.15, 0.15, 0.005)
    T = 30 / 365
    smile = []
    for k in ks:
        iv = svi_iv(k, a=0.018, b=0.12, rho=-0.08, m=0.0, sig=0.08, T=T)
        smile.append({'k': k, 'iv': round(iv, 2) if iv else 25.0})

    # 标注: 卖出OTM Put (k≈-0.08) 和 OTM Call (k≈+0.08)
    put_k, call_k = -0.08, 0.08
    put_iv = svi_iv(put_k, a=0.018, b=0.12, rho=-0.08, m=0.0, sig=0.08, T=T)
    call_iv = svi_iv(call_k, a=0.018, b=0.12, rho=-0.08, m=0.0, sig=0.08, T=T)
    atm_iv = svi_iv(0.0, a=0.018, b=0.12, rho=-0.08, m=0.0, sig=0.08, T=T)

    return {
        'smile': smile,
        'put_k': put_k, 'put_iv': round(put_iv, 2),
        'call_k': call_k, 'call_iv': round(call_iv, 2),
        'atm_iv': round(atm_iv, 2),
    }

# ========== HTML 生成 ==========

def generate_html(theory, ag, cf_sa, strangle):
    """生成完整课程HTML"""

    html = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>课时5: 波动率微笑 | 期权量化学习</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/katex.min.css">
<script src="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/katex.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/contrib/auto-render.min.js"></script>
<style>
:root {
  --bg: #0a0e17;
  --card: #131a2a;
  --card-hover: #182035;
  --border: #1e2d4a;
  --text: #c5d0e0;
  --text-dim: #6b7b95;
  --heading: #e8ecf1;
  --accent: #4fc3f7;
  --accent2: #81c784;
  --warn: #ffb74d;
  --danger: #ef5350;
  --code-bg: #0d1220;
  --tag-bg: #1a2540;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Hiragino Sans GB', sans-serif;
  background: var(--bg);
  color: var(--text);
  line-height: 1.8;
  font-size: 16px;
}
.container { max-width: 960px; margin: 0 auto; padding: 0 24px; }

/* Header */
.header {
  background: linear-gradient(135deg, #0d1a2e 0%, #152238 100%);
  border-bottom: 1px solid var(--border);
  padding: 32px 0 24px;
  position: sticky; top: 0; z-index: 100;
}
.header .progress-row {
  display: flex; align-items: center; gap: 12px;
  font-size: 13px; color: var(--text-dim); margin-bottom: 12px;
}
.progress-bar {
  flex: 1; height: 6px; background: #1a2540; border-radius: 3px; overflow: hidden;
}
.progress-fill { height: 100%; background: var(--accent); border-radius: 3px; transition: width 0.5s; }
.header h1 { font-size: 28px; color: var(--heading); font-weight: 700; }
.header .subtitle { color: var(--text-dim); font-size: 14px; margin-top: 4px; }
.badge {
  display: inline-block; padding: 2px 10px; border-radius: 12px;
  font-size: 12px; font-weight: 600;
}
.badge-stage { background: #1a3a5c; color: var(--accent); }
.badge-new { background: #1a3c1a; color: var(--accent2); }

/* Nav */
.nav {
  background: var(--card);
  border-bottom: 1px solid var(--border);
  padding: 8px 0;
  position: sticky; top: 100px; z-index: 99;
}
.nav-inner {
  display: flex; gap: 4px; overflow-x: auto; padding: 4px 0;
}
.nav a {
  color: var(--text-dim); text-decoration: none; font-size: 13px;
  padding: 6px 14px; border-radius: 6px; white-space: nowrap;
  transition: all 0.2s;
}
.nav a:hover, .nav a.active { color: var(--accent); background: var(--tag-bg); }

/* Sections */
.section { padding: 48px 0; border-bottom: 1px solid #111827; }
.section:last-child { border-bottom: none; }
.section-num {
  font-size: 13px; color: var(--accent); font-weight: 600;
  text-transform: uppercase; letter-spacing: 2px; margin-bottom: 8px;
}
.section h2 { font-size: 24px; color: var(--heading); margin-bottom: 20px; font-weight: 700; }
.section h3 { font-size: 18px; color: var(--heading); margin: 24px 0 12px; font-weight: 600; }

/* Cards */
.card {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 12px; padding: 24px; margin: 16px 0;
}
.card-accent { border-left: 3px solid var(--accent); }
.card-warn { border-left: 3px solid var(--warn); }
.card-danger { border-left: 3px solid var(--danger); }
.card-green { border-left: 3px solid var(--accent2); }

/* Lists */
ul, ol { padding-left: 24px; margin: 12px 0; }
li { margin: 6px 0; }

/* Chart container */
.chart-box {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 12px; padding: 16px; margin: 20px 0;
}
.chart-title {
  font-size: 14px; color: var(--text-dim); font-weight: 600;
  margin-bottom: 8px; text-align: center;
}
.chart { width: 100%; height: 420px; }

/* Inline code & formula */
code {
  background: var(--code-bg); color: var(--accent);
  padding: 2px 8px; border-radius: 4px; font-size: 14px;
  font-family: 'SF Mono', 'Fira Code', Consolas, monospace;
}
pre {
  background: var(--code-bg); border: 1px solid var(--border);
  border-radius: 8px; padding: 16px; margin: 12px 0;
  overflow-x: auto; font-size: 14px; line-height: 1.6;
  font-family: 'SF Mono', 'Fira Code', Consolas, monospace;
  color: var(--text);
}
.formula {
  background: var(--card); border-radius: 8px;
  padding: 16px 24px; margin: 16px 0; text-align: center;
  font-size: 18px; overflow-x: auto;
}

/* Tags */
.tag {
  display: inline-block; padding: 2px 10px; border-radius: 4px;
  font-size: 12px; background: var(--tag-bg); color: var(--text-dim);
  margin: 2px;
}
.tag-blue { background: #1a2d5c; color: var(--accent); }
.tag-green { background: #1a3c1a; color: var(--accent2); }
.tag-orange { background: #3c2a0a; color: var(--warn); }

/* Grid */
.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
@media (max-width: 700px) { .grid-2 { grid-template-columns: 1fr; } }

/* Highlight box */
.highlight {
  background: linear-gradient(135deg, #0d1f3c 0%, #0d2a1f 100%);
  border: 1px solid #1a3c2a; border-radius: 12px;
  padding: 20px 24px; margin: 16px 0;
}
.highlight-title {
  font-size: 14px; font-weight: 700; color: var(--accent2);
  margin-bottom: 8px;
}

/* Summary item */
.summary-item {
  display: flex; gap: 12px; padding: 12px 0;
  border-bottom: 1px solid #111827;
}
.summary-num {
  width: 32px; height: 32px; border-radius: 50%;
  background: var(--tag-bg); color: var(--accent);
  display: flex; align-items: center; justify-content: center;
  font-weight: 700; font-size: 14px; flex-shrink: 0;
}

/* Footer */
.footer {
  padding: 32px 0; text-align: center;
  color: var(--text-dim); font-size: 13px;
  border-top: 1px solid var(--border);
}
.footer a { color: var(--accent); text-decoration: none; }

/* Scroll animations */
.fade-in { opacity: 0; transform: translateY(20px); transition: all 0.6s ease-out; }
.fade-in.visible { opacity: 1; transform: translateY(0); }
</style>
</head>
<body>

<!-- ====== HEADER ====== -->
<div class="header">
  <div class="container">
    <div class="progress-row">
      <span>期权量化学习</span>
      <div class="progress-bar"><div class="progress-fill" style="width:17.8%"></div></div>
      <span>5/28 课时</span>
    </div>
    <h1>课时5: 波动率微笑 <span style="font-size:20px;color:var(--text-dim)">Volatility Smile</span></h1>
    <div class="subtitle">
      <span class="badge badge-stage">第二阶段</span>
      <span class="badge badge-new">新课</span>
      &nbsp; 波动率曲面 6课中的第1课 &middot; 预计阅读 15分钟
    </div>
  </div>
</div>

<!-- ====== NAV ====== -->
<div class="nav">
  <div class="container">
    <div class="nav-inner">
      <a href="#s1">什么是微笑</a>
      <a href="#s2">为什么存在</a>
      <a href="#s3">三种形状</a>
      <a href="#s4">白银微笑</a>
      <a href="#s5">CF vs SA</a>
      <a href="#s6">你的交易</a>
      <a href="#s7">测量方法</a>
      <a href="#s8">系统对接</a>
      <a href="#s9">总结</a>
    </div>
  </div>
</div>

<!-- ====== S1: 什么是波动率微笑 ====== -->
<div class="section" id="s1">
<div class="container fade-in">
  <div class="section-num">Section 01</div>
  <h2>什么是波动率微笑？</h2>

  <p>在 Black-Scholes 的理想世界里，波动率 $\\sigma$ 是一个<strong>常数</strong>。
  这意味着同一到期日、不同行权价的期权，用 BS 反推出来的隐含波动率（IV）应该<strong>完全相同</strong> —— 一条水平线。</p>

  <div class="formula">
    $$C = e^{-rT}[F \\cdot N(d_1) - K \\cdot N(d_2)], \\quad \\sigma \\text{ 应为常数}$$
  </div>

  <p>但现实中，当你把同一到期日所有行权价的 IV 画出来，看到的不是水平线，而是一条<strong>微笑曲线</strong>：</p>

  <div class="chart-box">
    <div class="chart-title">BS 理想 vs 市场现实</div>
    <div class="chart" id="chart-ideal-vs-real"></div>
  </div>

  <div class="card card-accent">
    <strong>关键洞察</strong>：波动率微笑的存在，意味着 BS 模型有系统性缺陷。
    市场参与者用「不同的 IV 给不同行权价的期权定价」来弥补这个缺陷。
    理解微笑，就是理解<strong>市场对尾部风险的真实定价</strong>。
  </div>

  <p>微笑曲线有一个标准的 x 轴表示法：<strong>log-moneyness</strong>（对数虚实度）：</p>
  <div class="formula">
    $$k = \\ln(K/F)$$
  </div>
  <p>$k = 0$ 是 ATM，$k < 0$ 是 OTM Put（或 ITM Call），$k > 0$ 是 OTM Call（或 ITM Put）。</p>
</div>
</div>

<!-- ====== S2: 为什么存在 ====== -->
<div class="section" id="s2">
<div class="container fade-in">
  <div class="section-num">Section 02</div>
  <h2>为什么微笑存在？</h2>

  <p>BS 模型有三个核心假设被市场现实打破：</p>

  <div class="grid-2">
    <div class="card">
      <h3>1. 肥尾 (Fat Tails)</h3>
      <p>BS 假设收益率服从<strong>正态分布</strong>。但实际市场有更多极端事件（黑天鹅）。
      棉花单日跌停、白银闪崩 —— 这些事件的概率远超正态分布预测。</p>
      <p>OTM 期权保护的就是这些极端事件 → 市场给 OTM 期权<strong>更高的 IV</strong>来反映真实的尾部风险。</p>
    </div>
    <div class="card">
      <h3>2. 波动率非恒定</h3>
      <p>BS 假设 $\\sigma$ 是常数。但你从 B005 和 B020 中深刻体会到：
      波动率随时间、价格、事件<strong>剧烈变化</strong>。</p>
      <p>中东战争 → IV 从 19% 飙到 21%（B005）。这种随机性本身需要被定价。</p>
    </div>
    <div class="card">
      <h3>3. 跳跃风险 (Jumps)</h3>
      <p>BS 假设价格路径连续（没有跳空）。但商品期货经常跳空开盘 —— 尤其是夜盘收盘到日盘开盘。</p>
      <p>OTM 期权对跳跃特别敏感（从零直接变成 ITM），所以需要额外溢价。</p>
    </div>
    <div class="card">
      <h3>4. Volga 溢价 <span class="tag tag-blue">课时4关联</span></h3>
      <p>课时4你学到：OTM 期权的 <strong>Volga/Vega 比率</strong>远高于 ATM。
      做市商持有 OTM 期权面临 Volga 风险（Vega 本身不稳定），需要收取溢价来补偿。</p>
      <p>DTE=7 时 Volga/Vega = 72.4，DTE=30 时仅 16.9。这解释了为什么<strong>短期微笑更陡峭</strong>。</p>
    </div>
  </div>

  <div class="card card-warn">
    <strong>供需视角</strong>：大量机构买 OTM Put 对冲尾部风险 → 推高 OTM Put 需求 → 推高 OTM Put IV。
    这在股指期权中特别明显（2008年后），但在商品期权中，供给中断风险使 OTM Call 也有类似需求。
  </div>
</div>
</div>

<!-- ====== S3: 三种形状 ====== -->
<div class="section" id="s3">
<div class="container fade-in">
  <div class="section-num">Section 03</div>
  <h2>微笑的三种基本形状</h2>

  <div class="chart-box">
    <div class="chart-title">三种典型微笑形状</div>
    <div class="chart" id="chart-three-shapes"></div>
  </div>

  <div class="grid-2" style="grid-template-columns: 1fr 1fr 1fr;">
    <div class="card" style="border-top: 3px solid var(--accent);">
      <h3 style="color:var(--accent)">U型微笑</h3>
      <p><strong>特征</strong>：ATM IV 最低，两侧 OTM 都高</p>
      <p><strong>常见于</strong>：商品期权、外汇</p>
      <p><strong>含义</strong>：市场认为上下两个方向都有尾部风险</p>
      <p class="tag tag-blue">你的 ag/SA 大部分时候是这种</p>
    </div>
    <div class="card" style="border-top: 3px solid var(--danger);">
      <h3 style="color:var(--danger)">负偏斜 (Skew)</h3>
      <p><strong>特征</strong>：OTM Put IV >> OTM Call IV</p>
      <p><strong>常见于</strong>：股指期权（SPX/沪深300）</p>
      <p><strong>含义</strong>：下跌恐慌的保险溢价（「恐惧指数」）</p>
      <p class="tag tag-orange">1987黑色星期一后永久改变</p>
    </div>
    <div class="card" style="border-top: 3px solid var(--accent2);">
      <h3 style="color:var(--accent2)">正偏斜 (Reverse)</h3>
      <p><strong>特征</strong>：OTM Call IV > OTM Put IV</p>
      <p><strong>常见于</strong>：某些农产品（天气风险）、天然气</p>
      <p><strong>含义</strong>：供给中断导致价格暴涨的风险更大</p>
      <p class="tag tag-green">CF棉花有时呈现此形态</p>
    </div>
  </div>

  <div class="highlight">
    <div class="highlight-title">商品期权 vs 股指期权的核心区别</div>
    <p>股指期权几乎永远是<strong>负偏斜</strong>（恐慌溢价）。但商品期权更复杂：
    既有暴跌风险（需求崩溃），也有暴涨风险（供给中断）。所以商品微笑通常更<strong>对称</strong>，
    呈 U 型而非纯偏斜。你的 B020 棉花双涨（+64%）就是供给侧冲击的典型。</p>
  </div>
</div>
</div>

<!-- ====== S4: 白银微笑 ====== -->
<div class="section" id="s4">
<div class="container fade-in">
  <div class="section-num">Section 04</div>
  <h2>白银(ag)波动率微笑 — DTE 对比</h2>

  <p>同一天、同一品种，不同到期月（不同 DTE）的微笑形状有显著差异。
  <strong>核心规律：DTE 越短，微笑越陡峭。</strong></p>

  <div class="chart-box">
    <div class="chart-title">白银(ag) Call IV 微笑 — 不同 DTE 对比 (模拟数据基于 SVI 模型)</div>
    <div class="chart" id="chart-ag-smile"></div>
  </div>

  <div class="card card-accent">
    <h3>为什么短期微笑更陡？</h3>
    <p>回忆课时4的结论：<strong>DTE 越短，Volga/Vega 比率越高</strong>（DTE=7 时 72.4，DTE=30 时 16.9）。</p>
    <ul>
      <li>做市商为短期 OTM 期权承担的 Volga 风险更大 → 需要更多溢价 → IV 更高</li>
      <li>短期内跳跃风险占比更大（一个隔夜跳空就足以穿越行权价）</li>
      <li>Gamma 在短期 ATM 附近极度集中（课时1的针状 Gamma），做市商对冲成本更高</li>
    </ul>
    <p><strong>对你的启示</strong>：你的 B021 策略入场 DTE25-60 → 出场 DTE0-7。
    在出场阶段，微笑极度陡峭 = 你卖出的 OTM 翅膀有更多溢价要衰减。这是策略盈利的来源之一！</p>
  </div>

  <div class="chart-box">
    <div class="chart-title">白银 Call IV vs Put IV (DTE=30, 模拟数据)</div>
    <div class="chart" id="chart-ag-cp"></div>
  </div>

  <p>注意 Put 侧（$k < 0$）的 IV 略高于 Call 侧 —— 这是白银作为贵金属的<strong>负偏斜特征</strong>
  （避险需求使 OTM Put 更贵）。但差距不大，远不如股指那么极端。</p>
</div>
</div>

<!-- ====== S5: CF vs SA ====== -->
<div class="section" id="s5">
<div class="container fade-in">
  <div class="section-num">Section 05</div>
  <h2>CF vs SA 微笑对比 — 为什么 MAE 差 5 倍</h2>

  <div class="chart-box">
    <div class="chart-title">CF(棉花) vs SA(纯碱) 波动率微笑对比 (DTE=30, 模拟数据)</div>
    <div class="chart" id="chart-cf-sa"></div>
  </div>

  <div class="grid-2">
    <div class="card card-danger">
      <h3>CF 棉花 <span class="tag tag-orange">MAE 128%</span></h3>
      <ul>
        <li>微笑<strong>更陡峭</strong>：翅膀 IV 比 ATM 高 8-12 个百分点</li>
        <li><strong>略正偏斜</strong>：OTM Call IV > OTM Put IV（天气/政策风险推高）</li>
        <li>Vanna 效应大 → Delta 剧变 → MAE 128%（课时3结论）</li>
        <li>Volga 效应大 → 波动率冲击时 Vega 膨胀（课时4结论）</li>
        <li><strong>策略含义</strong>：必须入场 DTE30+，留足时间缓冲</li>
      </ul>
    </div>
    <div class="card card-green">
      <h3>SA 纯碱 <span class="tag tag-green">MAE 27%</span></h3>
      <ul>
        <li>微笑<strong>更平缓</strong>：翅膀 IV 比 ATM 只高 3-5 个百分点</li>
        <li><strong>近对称</strong>：OTM Call/Put IV 差距小</li>
        <li>Vanna 效应小 → Delta 稳定 → MAE 仅 27%</li>
        <li>Volga 效应小 → IV 冲击造成的非线性损失有限</li>
        <li><strong>策略含义</strong>：入场 DTE20 就够，更灵活</li>
      </ul>
    </div>
  </div>

  <div class="highlight">
    <div class="highlight-title">第一阶段 + 第二阶段的整合：MAE 的完整解释</div>
    <p>CF 的 MAE 是 SA 的 <strong>4.7 倍</strong> (128% vs 27%)。现在你有了完整的理论解释：</p>
    <ol>
      <li><strong>Gamma</strong> (课时1): CF 近月 ATM Gamma 更大 → 极端行情中 Delta 加速变化</li>
      <li><strong>Vanna</strong> (课时3): CF IV 波动大 → Vanna 放大 Delta 变化 → 方向敞口剧变</li>
      <li><strong>Volga</strong> (课时4): CF 微笑陡 → OTM Volga 大 → IV 冲击时 Vega 非线性膨胀</li>
      <li><strong>微笑形状</strong> (本课): CF 微笑更陡 = 市场定价了更多尾部风险 = 确实更危险</li>
    </ol>
    <p>选品种时，<strong>微笑的陡峭程度</strong>是预判 MAE 的先行指标。</p>
  </div>
</div>
</div>

<!-- ====== S6: 你的交易 ====== -->
<div class="section" id="s6">
<div class="container fade-in">
  <div class="section-num">Section 06</div>
  <h2>微笑与你的宽跨策略</h2>

  <div class="chart-box">
    <div class="chart-title">卖出宽跨在微笑曲线上的位置</div>
    <div class="chart" id="chart-strangle"></div>
  </div>

  <div class="card card-accent">
    <h3>你在卖「翅膀」</h3>
    <p>卖出宽跨 = 卖出 OTM Put + 卖出 OTM Call。在微笑曲线上，你卖的是<strong>两端翅膀</strong>，
    而不是最低点（ATM）。</p>
    <ul>
      <li><strong>好消息</strong>：翅膀的 IV 比 ATM 高 → 你收到了<strong>微笑溢价</strong>（相对于 BS 理论价更多的权利金）</li>
      <li><strong>坏消息</strong>：翅膀的 IV 更高是有原因的 —— 市场真的认为这些极端行情会发生</li>
      <li><strong>核心矛盾</strong>：微笑越陡 = 权利金越多 = 但风险也越大。这是<strong>风险溢价</strong>，不是免费午餐</li>
    </ul>
  </div>

  <h3>高低腿比 = 偏斜暴露 (Skew Exposure)</h3>
  <p>你的选对规则之一：<strong>高低腿比 ≤ 2</strong>。现在你理解了它的深层含义：</p>

  <div class="card">
    <p><strong>高低腿比 = 高价腿权利金 / 低价腿权利金</strong></p>
    <p>如果 Put IV 远高于 Call IV（负偏斜）→ OTM Put 更贵 → 高低腿比增大。</p>
    <p>高低腿比 > 2 意味着：</p>
    <ul>
      <li>你的两条腿在微笑曲线上<strong>高度不对称</strong></li>
      <li>一侧的风险/权利金远大于另一侧</li>
      <li>这不是真正的「宽跨」，更像是<strong>赌单边</strong></li>
    </ul>
    <p>B024 VRP 策略验证了这一点：入场腿比 > 1.5 的交易 Sharpe 下降。筛选后 Sharpe 从 5.68 → 5.93。</p>
  </div>

  <h3>微笑变化对持仓的影响</h3>
  <div class="grid-2">
    <div class="card card-green">
      <h3>微笑变平 (对卖方有利)</h3>
      <p>恐慌消退 → OTM IV 回落 → 你卖出的翅膀价值下降 → <strong>盈利</strong></p>
      <p>典型场景：利空出尽、节假日后回归平静</p>
    </div>
    <div class="card card-danger">
      <h3>微笑变陡 (对卖方不利)</h3>
      <p>恐慌加剧 → OTM IV 飙升 → 你卖出的翅膀价值暴涨 → <strong>亏损</strong></p>
      <p>典型场景：黑天鹅事件、政策突变、中东战争(B005)</p>
    </div>
  </div>
</div>
</div>

<!-- ====== S7: 测量方法 ====== -->
<div class="section" id="s7">
<div class="container fade-in">
  <div class="section-num">Section 07</div>
  <h2>如何测量微笑</h2>

  <p>两个标准指标量化微笑的形状：</p>

  <div class="grid-2">
    <div class="card">
      <h3>25-Delta 风险逆转 (Risk Reversal)</h3>
      <div class="formula">$$RR_{25} = IV_{25\\Delta C} - IV_{25\\Delta P}$$</div>
      <p>衡量<strong>偏斜方向</strong>：</p>
      <ul>
        <li>$RR > 0$：Call 偏贵 → 市场偏看涨（正偏斜）</li>
        <li>$RR < 0$：Put 偏贵 → 市场偏看跌（负偏斜）</li>
        <li>$RR \\approx 0$：对称微笑</li>
      </ul>
      <p><span class="tag tag-blue">对应：你选对时看的偏斜方向</span></p>
    </div>
    <div class="card">
      <h3>25-Delta 蝶式 (Butterfly)</h3>
      <div class="formula">$$BF_{25} = \\frac{IV_{25\\Delta C} + IV_{25\\Delta P}}{2} - IV_{ATM}$$</div>
      <p>衡量<strong>微笑陡峭程度</strong>：</p>
      <ul>
        <li>$BF > 0$：微笑存在（翅膀贵于 ATM）</li>
        <li>$BF$ 越大：微笑越陡 = 尾部风险定价越高</li>
        <li>$BF$ 突然增大：恐慌信号！</li>
      </ul>
      <p><span class="tag tag-orange">对应：预判 MAE 的先行指标</span></p>
    </div>
  </div>

  <div class="card card-accent">
    <h3>SVI 参数化模型</h3>
    <p>Jim Gatheral 提出的 <strong>SVI (Stochastic Volatility Inspired)</strong> 模型，用 5 个参数拟合整条微笑曲线：</p>
    <div class="formula">$$w(k) = a + b\\left[\\rho(k - m) + \\sqrt{(k-m)^2 + \\sigma^2}\\right]$$</div>
    <p>其中 $w = IV^2 \\times T$（总方差），$k = \\ln(K/F)$（log-moneyness）。</p>
    <ul>
      <li>$a$：整体方差水平</li>
      <li>$b$：微笑陡峭程度（越大越陡）</li>
      <li>$\\rho$：偏斜方向（-1 到 +1，负=负偏斜）</li>
      <li>$m$：微笑最低点的偏移</li>
      <li>$\\sigma$：曲率（ATM 附近的平滑度）</li>
    </ul>
    <p>本课所有模拟图表都用 SVI 生成。课时7会详细学习 SVI 的拟合方法。</p>
  </div>
</div>
</div>

<!-- ====== S8: 系统对接 ====== -->
<div class="section" id="s8">
<div class="container fade-in">
  <div class="section-num">Section 08</div>
  <h2>系统对接 — 你已有的能力</h2>

  <p>你的 trade2026 系统已经有波动率微笑分析的雏形：</p>

  <div class="card">
    <h3><code>scripts/iv_surface.py</code></h3>
    <p>已实现的功能：</p>
    <ul>
      <li><code>print_smile()</code>：表格展示单月所有行权价的 Call/Put IV</li>
      <li><code>print_skew_for_strangle()</code>：宽跨视角的偏斜分析</li>
      <li>期限结构对比（近月 vs 远月 ATM IV）</li>
    </ul>
    <pre>python scripts/iv_surface.py AG          # 白银
python scripts/iv_surface.py SA 2605     # 纯碱指定月份</pre>
  </div>

  <div class="card">
    <h3><code>infra/indicators/greeks_engine.py</code></h3>
    <p>Black-76 定价 + Newton-Raphson IV 反推 + 全套 Greeks（含二阶 Vanna/Charm/Volga）。</p>
    <p>这是波动率微笑计算的<strong>底层引擎</strong>。</p>
  </div>

  <div class="highlight">
    <div class="highlight-title">课时5 升级建议</div>
    <ol>
      <li><strong>可视化</strong>：在工作台(8052)增加微笑曲线图表（ECharts），替代纯文本表格</li>
      <li><strong>RR/BF 指标</strong>：计算并显示 25-Delta Risk Reversal 和 Butterfly</li>
      <li><strong>微笑监控</strong>：追踪微笑形状随时间的变化，发现异常（如突然变陡 = 恐慌信号）</li>
      <li><strong>选品种辅助</strong>：BF 值低的品种 = 微笑平缓 = MAE 风险小 → 优先卖出</li>
      <li><strong>SVI 拟合</strong>：用 SVI 模型参数化微笑，5个数字概括整条曲线</li>
    </ol>
  </div>
</div>
</div>

<!-- ====== S9: 总结 ====== -->
<div class="section" id="s9">
<div class="container fade-in">
  <div class="section-num">Section 09</div>
  <h2>要点总结</h2>

  <div class="summary-item">
    <div class="summary-num">1</div>
    <div>
      <strong>波动率微笑 = BS 模型缺陷的市场补丁</strong><br>
      同一到期日不同行权价的 IV 不同，OTM 期权通常 IV 更高。这是市场对肥尾、跳跃、Volga 风险的真实定价。
    </div>
  </div>
  <div class="summary-item">
    <div class="summary-num">2</div>
    <div>
      <strong>商品微笑 ≈ U型（对称），股指微笑 = 负偏斜</strong><br>
      商品既有暴跌风险也有暴涨风险 → 两侧 OTM 都有溢价。但不同品种差异大（CF 陡 vs SA 平）。
    </div>
  </div>
  <div class="summary-item">
    <div class="summary-num">3</div>
    <div>
      <strong>DTE 越短，微笑越陡</strong><br>
      这是 Volga/Vega 比率随 DTE 下降而爆炸的直接表现（课时4）。你的 B021 策略在 DTE 衰减过程中收割了这个溢价。
    </div>
  </div>
  <div class="summary-item">
    <div class="summary-num">4</div>
    <div>
      <strong>卖出宽跨 = 卖微笑的翅膀 = 收取尾部风险溢价</strong><br>
      微笑越陡 = 权利金越多 = 但 MAE 也越大。微笑的陡峭程度（BF 指标）是预判 MAE 的先行指标。
    </div>
  </div>
  <div class="summary-item" style="border-bottom:none;">
    <div class="summary-num">5</div>
    <div>
      <strong>高低腿比 ≤ 2 的本质 = 控制偏斜暴露</strong><br>
      腿比过大说明微笑严重不对称，一侧风险远大于另一侧。RR 和 BF 是量化这种不对称的标准工具。
    </div>
  </div>

  <h3 style="margin-top:32px;">关联信念</h3>
  <div class="grid-2">
    <div class="card" style="font-size:14px;">
      <span class="tag tag-blue">B005</span> 三因素模型中的 Vega 因素 = 微笑曲面上的运动<br>
      <span class="tag tag-blue">B008</span> Gamma 加速 = 微笑在 ATM 附近的曲率<br>
      <span class="tag tag-blue">B020</span> 双涨 = 微笑翅膀的 Volga 放大效应<br>
    </div>
    <div class="card" style="font-size:14px;">
      <span class="tag tag-green">B021</span> CF/SA MAE 差异 = 微笑陡峭度差异<br>
      <span class="tag tag-green">B024</span> 入场腿比 ≤ 1.5 = 偏斜控制<br>
      <span class="tag tag-orange">新</span> 微笑陡峭度(BF)可作为品种筛选指标<br>
    </div>
  </div>

  <div class="card" style="margin-top:24px; text-align:center; background: linear-gradient(135deg, #0d1a2e, #152238);">
    <h3>下一课预告：课时6 — 波动率期限结构</h3>
    <p style="color:var(--text-dim);">
      不同到期月的 ATM IV 对比 &middot; Contango vs Backwardation &middot;
      604 vs 605 vs 606 合约的 IV 水平 &middot; 期限结构与 Calendar Spread
    </p>
  </div>
</div>
</div>

<!-- ====== FOOTER ====== -->
<div class="footer">
  <div class="container">
    <p>期权量化交易学习系统 &middot; 课时 5/28 &middot; 波动率曲面阶段 1/6</p>
    <p style="margin-top:4px;">基于 trade2026 系统 &amp; price_sum_knowledge.json 知识库</p>
  </div>
</div>

<!-- ====== CHARTS JS ====== -->
<script>
// === DATA ===
const theoryData = __THEORY_DATA__;
const agData = __AG_DATA__;
const cfSaData = __CF_SA_DATA__;
const strangleData = __STRANGLE_DATA__;

// === THEME ===
const colors = ['#4fc3f7', '#81c784', '#ffb74d', '#ef5350', '#ce93d8', '#90a4ae'];
const gridStyle = { left: 60, right: 30, top: 50, bottom: 40 };
const axisStyle = {
  axisLine: { lineStyle: { color: '#2a3a5c' } },
  axisLabel: { color: '#6b7b95', fontSize: 12 },
  splitLine: { lineStyle: { color: '#111827' } },
};

// === CHART 1: BS Ideal vs Reality ===
(function() {
  const chart = echarts.init(document.getElementById('chart-ideal-vs-real'), null, {renderer:'canvas'});
  const ks = theoryData.flat.map(d => (d.k * 100).toFixed(1) + '%');
  chart.setOption({
    backgroundColor: 'transparent',
    grid: gridStyle,
    tooltip: { trigger: 'axis', backgroundColor: '#1a1f2e', borderColor: '#2a3a5c',
      textStyle: { color: '#c5d0e0' },
      formatter: function(params) {
        let s = params[0].axisValue + '<br/>';
        params.forEach(p => { s += p.marker + ' ' + p.seriesName + ': <b>' + p.value + '%</b><br/>'; });
        return s;
      }
    },
    legend: { data: ['BS 理想 (σ=常数)', '市场现实 (微笑)'], top: 8,
      textStyle: { color: '#6b7b95' } },
    xAxis: { type: 'category', data: ks, name: 'log-moneyness (k)',
      nameLocation: 'center', nameGap: 28, nameTextStyle: { color: '#6b7b95' },
      ...axisStyle, axisLabel: { ...axisStyle.axisLabel, interval: 'auto',
        formatter: function(v, i) { return i % 10 === 0 ? v : ''; } } },
    yAxis: { type: 'value', name: 'IV (%)', min: 18, max: 42, ...axisStyle },
    series: [
      { name: 'BS 理想 (σ=常数)', type: 'line', data: theoryData.flat.map(d => d.iv),
        lineStyle: { type: 'dashed', width: 2, color: '#90a4ae' },
        itemStyle: { color: '#90a4ae' }, symbol: 'none' },
      { name: '市场现实 (微笑)', type: 'line', data: theoryData.commodity.map(d => d.iv),
        lineStyle: { width: 3, color: colors[0] },
        itemStyle: { color: colors[0] }, symbol: 'none',
        areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: 'rgba(79,195,247,0.15)' },
          { offset: 1, color: 'rgba(79,195,247,0)' }
        ]) } },
    ],
  });
  window.addEventListener('resize', () => chart.resize());
})();

// === CHART 2: Three Shapes ===
(function() {
  const chart = echarts.init(document.getElementById('chart-three-shapes'));
  const ks = theoryData.commodity.map(d => (d.k * 100).toFixed(1));
  chart.setOption({
    backgroundColor: 'transparent',
    grid: gridStyle,
    tooltip: { trigger: 'axis', backgroundColor: '#1a1f2e', borderColor: '#2a3a5c',
      textStyle: { color: '#c5d0e0' } },
    legend: { data: ['U型微笑 (商品)', '负偏斜 (股指)', '正偏斜 (农产品)'], top: 8,
      textStyle: { color: '#6b7b95' } },
    xAxis: { type: 'category', data: ks, name: 'log-moneyness (%)',
      nameLocation: 'center', nameGap: 28, nameTextStyle: { color: '#6b7b95' },
      ...axisStyle, axisLabel: { ...axisStyle.axisLabel,
        formatter: function(v, i) { return i % 10 === 0 ? v+'%' : ''; } } },
    yAxis: { type: 'value', name: 'IV (%)', ...axisStyle },
    series: [
      { name: 'U型微笑 (商品)', type: 'line', data: theoryData.commodity.map(d => d.iv),
        lineStyle: { width: 3, color: colors[0] }, itemStyle: { color: colors[0] }, symbol: 'none' },
      { name: '负偏斜 (股指)', type: 'line', data: theoryData.equity.map(d => d.iv),
        lineStyle: { width: 3, color: colors[3] }, itemStyle: { color: colors[3] }, symbol: 'none' },
      { name: '正偏斜 (农产品)', type: 'line', data: theoryData.reverse.map(d => d.iv),
        lineStyle: { width: 3, color: colors[1] }, itemStyle: { color: colors[1] }, symbol: 'none' },
    ],
  });
  window.addEventListener('resize', () => chart.resize());
})();

// === CHART 3: Ag multi-DTE ===
(function() {
  const chart = echarts.init(document.getElementById('chart-ag-smile'));
  const dteColors = [colors[3], colors[2], colors[0], colors[1]];
  const series = agData.curves.map((curve, i) => ({
    name: curve.label,
    type: 'line',
    data: curve.calls.map(d => [d.mn, d.iv]),
    lineStyle: { width: i === 0 ? 3 : 2, color: dteColors[i] },
    itemStyle: { color: dteColors[i] },
    symbol: 'none', smooth: true,
  }));
  chart.setOption({
    backgroundColor: 'transparent',
    grid: { left: 60, right: 30, top: 50, bottom: 50 },
    tooltip: { trigger: 'axis', backgroundColor: '#1a1f2e', borderColor: '#2a3a5c',
      textStyle: { color: '#c5d0e0' } },
    legend: { data: agData.curves.map(c => c.label), top: 8,
      textStyle: { color: '#6b7b95' } },
    xAxis: { type: 'value', name: 'K/F (moneyness)', nameLocation: 'center', nameGap: 32,
      nameTextStyle: { color: '#6b7b95' }, min: 0.80, max: 1.20,
      ...axisStyle, axisLabel: { ...axisStyle.axisLabel, formatter: v => v.toFixed(2) } },
    yAxis: { type: 'value', name: 'IV (%)', ...axisStyle },
    series: series,
    graphic: [{
      type: 'line', shape: { x1: 0, y1: 0, x2: 0, y2: 1 },
      style: { stroke: '#2a3a5c', lineWidth: 1, lineDash: [4, 4] },
      left: 'center', top: 50, bottom: 50,
    }],
  });
  // ATM annotation
  chart.setOption({
    series: series.map(s => s),
    graphic: [{
      type: 'text', left: 'center', top: 35,
      style: { text: 'ATM (K/F=1.0)', fill: '#6b7b95', fontSize: 11 }
    }],
  });
  window.addEventListener('resize', () => chart.resize());
})();

// === CHART 4: Ag Call vs Put ===
(function() {
  const chart = echarts.init(document.getElementById('chart-ag-cp'));
  // Use DTE=30 curve
  const curve30 = agData.curves.find(c => c.dte === 30);
  if (!curve30) return;
  chart.setOption({
    backgroundColor: 'transparent',
    grid: { left: 60, right: 30, top: 50, bottom: 50 },
    tooltip: { trigger: 'axis', backgroundColor: '#1a1f2e', borderColor: '#2a3a5c',
      textStyle: { color: '#c5d0e0' } },
    legend: { data: ['Call IV', 'Put IV'], top: 8,
      textStyle: { color: '#6b7b95' } },
    xAxis: { type: 'value', name: 'K/F', nameLocation: 'center', nameGap: 32,
      nameTextStyle: { color: '#6b7b95' }, min: 0.82, max: 1.18,
      ...axisStyle, axisLabel: { ...axisStyle.axisLabel, formatter: v => v.toFixed(2) } },
    yAxis: { type: 'value', name: 'IV (%)', ...axisStyle },
    series: [
      { name: 'Call IV', type: 'line', data: curve30.calls.map(d => [d.mn, d.iv]),
        lineStyle: { width: 2.5, color: colors[0] }, itemStyle: { color: colors[0] }, symbol: 'none', smooth: true },
      { name: 'Put IV', type: 'line', data: curve30.puts.map(d => [d.mn, d.iv]),
        lineStyle: { width: 2.5, color: colors[3] }, itemStyle: { color: colors[3] }, symbol: 'none', smooth: true },
    ],
  });
  window.addEventListener('resize', () => chart.resize());
})();

// === CHART 5: CF vs SA ===
(function() {
  const chart = echarts.init(document.getElementById('chart-cf-sa'));
  chart.setOption({
    backgroundColor: 'transparent',
    grid: { left: 60, right: 30, top: 50, bottom: 50 },
    tooltip: { trigger: 'axis', backgroundColor: '#1a1f2e', borderColor: '#2a3a5c',
      textStyle: { color: '#c5d0e0' } },
    legend: { data: ['CF 棉花 (陡峭)', 'SA 纯碱 (平缓)'], top: 8,
      textStyle: { color: '#6b7b95' } },
    xAxis: { type: 'value', name: 'log-moneyness (k)', nameLocation: 'center', nameGap: 32,
      nameTextStyle: { color: '#6b7b95' },
      ...axisStyle, axisLabel: { ...axisStyle.axisLabel, formatter: v => (v*100).toFixed(0)+'%' } },
    yAxis: { type: 'value', name: 'IV (%)', ...axisStyle },
    series: [
      { name: 'CF 棉花 (陡峭)', type: 'line', data: cfSaData.cf.map(d => [d.k, d.iv]),
        lineStyle: { width: 3, color: colors[2] }, itemStyle: { color: colors[2] }, symbol: 'none', smooth: true,
        areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: 'rgba(255,183,77,0.12)' }, { offset: 1, color: 'rgba(255,183,77,0)' }
        ]) } },
      { name: 'SA 纯碱 (平缓)', type: 'line', data: cfSaData.sa.map(d => [d.k, d.iv]),
        lineStyle: { width: 3, color: colors[1] }, itemStyle: { color: colors[1] }, symbol: 'none', smooth: true,
        areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: 'rgba(129,199,132,0.12)' }, { offset: 1, color: 'rgba(129,199,132,0)' }
        ]) } },
    ],
  });
  window.addEventListener('resize', () => chart.resize());
})();

// === CHART 6: Strangle on Smile ===
(function() {
  const chart = echarts.init(document.getElementById('chart-strangle'));
  const smileData = strangleData.smile.map(d => [d.k, d.iv]);
  chart.setOption({
    backgroundColor: 'transparent',
    grid: { left: 60, right: 30, top: 50, bottom: 50 },
    tooltip: { trigger: 'axis', backgroundColor: '#1a1f2e', borderColor: '#2a3a5c',
      textStyle: { color: '#c5d0e0' } },
    xAxis: { type: 'value', name: 'log-moneyness (k)', nameLocation: 'center', nameGap: 32,
      nameTextStyle: { color: '#6b7b95' },
      ...axisStyle, axisLabel: { ...axisStyle.axisLabel, formatter: v => (v*100).toFixed(0)+'%' } },
    yAxis: { type: 'value', name: 'IV (%)', ...axisStyle },
    series: [
      { name: '微笑曲线', type: 'line', data: smileData,
        lineStyle: { width: 3, color: '#4fc3f7' }, itemStyle: { color: '#4fc3f7' }, symbol: 'none', smooth: true,
        areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: 'rgba(79,195,247,0.1)' }, { offset: 1, color: 'rgba(79,195,247,0)' }
        ]) },
        markPoint: {
          symbol: 'circle', symbolSize: 16,
          data: [
            { coord: [strangleData.put_k, strangleData.put_iv],
              itemStyle: { color: '#ef5350', borderColor: '#fff', borderWidth: 2 },
              label: { show: true, formatter: 'Sell Put\\n' + strangleData.put_iv + '%',
                position: 'left', color: '#ef5350', fontSize: 13, fontWeight: 'bold',
                distance: 15 } },
            { coord: [strangleData.call_k, strangleData.call_iv],
              itemStyle: { color: '#81c784', borderColor: '#fff', borderWidth: 2 },
              label: { show: true, formatter: 'Sell Call\\n' + strangleData.call_iv + '%',
                position: 'right', color: '#81c784', fontSize: 13, fontWeight: 'bold',
                distance: 15 } },
            { coord: [0, strangleData.atm_iv],
              itemStyle: { color: '#90a4ae', borderColor: '#fff', borderWidth: 2 },
              label: { show: true, formatter: 'ATM\\n' + strangleData.atm_iv + '%',
                position: 'top', color: '#90a4ae', fontSize: 12,
                distance: 15 } },
          ],
        },
        markArea: {
          silent: true,
          data: [
            [{ xAxis: strangleData.put_k - 0.01, itemStyle: { color: 'rgba(239,83,80,0.08)' } },
             { xAxis: strangleData.put_k + 0.01 }],
            [{ xAxis: strangleData.call_k - 0.01, itemStyle: { color: 'rgba(129,199,132,0.08)' } },
             { xAxis: strangleData.call_k + 0.01 }],
          ],
        },
      },
    ],
  });
  window.addEventListener('resize', () => chart.resize());
})();

// === KaTeX auto-render ===
document.addEventListener('DOMContentLoaded', function() {
  renderMathInElement(document.body, {
    delimiters: [
      { left: '$$', right: '$$', display: true },
      { left: '$', right: '$', display: false },
    ],
    throwOnError: false,
  });
});

// === Scroll fade-in ===
const observer = new IntersectionObserver((entries) => {
  entries.forEach(entry => {
    if (entry.isIntersecting) entry.target.classList.add('visible');
  });
}, { threshold: 0.1 });
document.querySelectorAll('.fade-in').forEach(el => observer.observe(el));

// === Nav active state ===
const sections = document.querySelectorAll('.section');
const navLinks = document.querySelectorAll('.nav a');
window.addEventListener('scroll', () => {
  let current = '';
  sections.forEach(sec => {
    if (window.scrollY >= sec.offsetTop - 200) current = sec.id;
  });
  navLinks.forEach(link => {
    link.classList.toggle('active', link.getAttribute('href') === '#' + current);
  });
});
</script>
</body>
</html>"""
    return html

# ========== Server ==========

class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress logs

    def do_GET(self):
        if self.path == '/' or self.path == '/index.html':
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            with open(OUTPUT_HTML, 'rb') as f:
                self.wfile.write(f.read())
        else:
            self.send_error(404)

def serve():
    """启动HTTP服务器"""
    for port in range(PORT, PORT + 10):
        try:
            server = http.server.HTTPServer(('0.0.0.0', port), QuietHandler)
            print(f"🌐 课程页面: http://localhost:{port}")
            webbrowser.open(f'http://localhost:{port}')
            server.serve_forever()
            break
        except OSError:
            continue

# ========== Main ==========

def main():
    print("📊 课时5: 波动率微笑 (Volatility Smile)")
    print("正在生成课程页面...")

    # 生成数据
    theory = generate_theory_data()
    ag = generate_ag_smile()
    cf_sa = generate_cf_sa_comparison()
    strangle = generate_strangle_annotation()

    # 生成 HTML
    html = generate_html(theory, ag, cf_sa, strangle)

    # 注入数据
    html = html.replace('__THEORY_DATA__', json.dumps(theory))
    html = html.replace('__AG_DATA__', json.dumps(ag))
    html = html.replace('__CF_SA_DATA__', json.dumps(cf_sa))
    html = html.replace('__STRANGLE_DATA__', json.dumps(strangle))

    # 写入文件
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"✅ HTML 已生成: {OUTPUT_HTML}")

    # 启动服务器
    serve()

if __name__ == '__main__':
    main()
