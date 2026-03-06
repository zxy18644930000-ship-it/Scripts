#!/usr/bin/env python3
"""期权量化交易学习平台 — Port 8060"""

from flask import Flask, jsonify
from pathlib import Path
import json, re

app = Flask(__name__)
PORT = 8060
COURSE_KB = Path.home() / '.claude/projects/-Users-zhangxiaoyu/memory/课程知识库.md'

@app.route('/')
def index():
    return HTML

@app.route('/api/progress')
def progress():
    """从课程知识库解析进度"""
    try:
        text = COURSE_KB.read_text()
        completed = len(re.findall(r'✅', text))
        total = 28
        stages = []
        stage_info = [
            ("Greeks体系", 4, "blue"), ("波动率曲面", 6, "purple"),
            ("组合策略", 4, "green"), ("风险管理", 4, "orange"),
            ("定价理论", 4, "red"), ("高级策略+ML", 6, "gold")
        ]
        done_count = completed
        for name, count, color in stage_info:
            s_done = min(done_count, count)
            stages.append({"name": name, "total": count, "done": s_done, "color": color})
            done_count = max(0, done_count - count)
        return jsonify({"completed": completed, "total": total, "stages": stages})
    except:
        return jsonify({"completed": 0, "total": 28, "stages": []})

@app.route('/api/knowledge')
def knowledge():
    """读取信念知识库"""
    try:
        kb = Path.home() / 'Scripts/price_sum_knowledge.json'
        data = json.loads(kb.read_text())
        beliefs = {}
        for k, v in data.get('beliefs', {}).items():
            beliefs[k] = {"desc": v.get("description", ""), "confidence": v.get("confidence", 0)}
        return jsonify(beliefs)
    except:
        return jsonify({})

HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>期权量化交易 · 学习平台</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/katex.min.css">
<script src="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/katex.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/contrib/auto-render.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/plotly.js-dist@2.27.0/plotly.min.js"></script>
<style>
:root {
  --bg: #0d1117; --bg2: #161b22; --bg3: #21262d; --border: #30363d;
  --text: #c9d1d9; --text2: #8b949e; --text3: #484f58;
  --blue: #58a6ff; --purple: #bc8cff; --green: #3fb950;
  --orange: #d29922; --red: #f85149; --gold: #e3b341;
  --cyan: #39d2c0;
}
* { margin:0; padding:0; box-sizing:border-box; }
body { background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif; line-height:1.6; }
a { color:var(--blue); text-decoration:none; }
a:hover { text-decoration:underline; }

/* Layout */
.app { display:flex; min-height:100vh; }
.sidebar { width:280px; background:var(--bg2); border-right:1px solid var(--border); padding:20px 0; position:fixed; top:0; left:0; bottom:0; overflow-y:auto; z-index:10; }
.sidebar::-webkit-scrollbar { width:6px; }
.sidebar::-webkit-scrollbar-thumb { background:var(--border); border-radius:3px; }
.main { margin-left:280px; flex:1; padding:32px 40px; max-width:1100px; }

/* Sidebar */
.logo { padding:0 20px 20px; border-bottom:1px solid var(--border); margin-bottom:16px; }
.logo h1 { font-size:18px; color:var(--text); font-weight:600; }
.logo p { font-size:12px; color:var(--text2); margin-top:4px; }
.progress-mini { margin:12px 20px 16px; padding:10px 12px; background:var(--bg3); border-radius:8px; cursor:pointer; }
.progress-mini:hover { background:var(--border); }
.progress-mini .bar { height:6px; background:var(--bg); border-radius:3px; margin-top:8px; overflow:hidden; }
.progress-mini .bar-fill { height:100%; background:linear-gradient(90deg,var(--blue),var(--purple)); border-radius:3px; transition:width 0.5s; }
.progress-mini span { font-size:13px; color:var(--text2); }
.progress-mini strong { color:var(--text); float:right; }

.nav-stage { margin-bottom:4px; }
.nav-stage-header { padding:8px 20px; font-size:13px; font-weight:600; color:var(--text2); cursor:pointer; display:flex; align-items:center; gap:8px; }
.nav-stage-header:hover { color:var(--text); }
.nav-stage-header .dot { width:8px; height:8px; border-radius:50%; flex-shrink:0; }
.nav-stage-header .arrow { font-size:10px; margin-left:auto; transition:transform 0.2s; }
.nav-stage.open .arrow { transform:rotate(90deg); }
.nav-lessons { display:none; }
.nav-stage.open .nav-lessons { display:block; }
.nav-lesson { padding:6px 20px 6px 48px; font-size:13px; color:var(--text2); cursor:pointer; display:flex; align-items:center; gap:8px; }
.nav-lesson:hover { color:var(--text); background:var(--bg3); }
.nav-lesson.active { color:var(--blue); background:rgba(88,166,255,0.1); }
.nav-lesson .status { font-size:11px; }

/* Dashboard */
.dash-header { margin-bottom:32px; }
.dash-header h2 { font-size:28px; font-weight:700; margin-bottom:8px; }
.dash-header p { color:var(--text2); font-size:15px; }
.stats-row { display:grid; grid-template-columns:repeat(4,1fr); gap:16px; margin-bottom:32px; }
.stat-card { background:var(--bg2); border:1px solid var(--border); border-radius:12px; padding:20px; }
.stat-card .value { font-size:32px; font-weight:700; }
.stat-card .label { font-size:13px; color:var(--text2); margin-top:4px; }
.stage-cards { display:grid; grid-template-columns:repeat(2,1fr); gap:16px; margin-bottom:32px; }
.stage-card { background:var(--bg2); border:1px solid var(--border); border-radius:12px; padding:20px; cursor:pointer; transition:all 0.2s; }
.stage-card:hover { border-color:var(--text3); transform:translateY(-2px); }
.stage-card h3 { font-size:16px; margin-bottom:4px; display:flex; align-items:center; gap:8px; }
.stage-card .stage-num { font-size:12px; padding:2px 8px; border-radius:4px; color:#fff; }
.stage-card .desc { font-size:13px; color:var(--text2); margin-bottom:12px; }
.stage-card .bar { height:6px; background:var(--bg); border-radius:3px; overflow:hidden; }
.stage-card .bar-fill { height:100%; border-radius:3px; transition:width 0.5s; }
.stage-card .meta { font-size:12px; color:var(--text3); margin-top:8px; }

.next-lesson-card { background:linear-gradient(135deg, rgba(88,166,255,0.1), rgba(188,140,255,0.1)); border:1px solid rgba(88,166,255,0.3); border-radius:12px; padding:24px; margin-bottom:32px; display:flex; align-items:center; gap:20px; }
.next-lesson-card .icon { font-size:40px; }
.next-lesson-card h3 { font-size:18px; margin-bottom:4px; }
.next-lesson-card p { color:var(--text2); font-size:14px; }
.next-lesson-card button { margin-left:auto; padding:10px 24px; background:var(--blue); color:#fff; border:none; border-radius:8px; font-size:14px; cursor:pointer; white-space:nowrap; }
.next-lesson-card button:hover { opacity:0.9; }

.notes-section { margin-top:32px; }
.notes-section h3 { font-size:18px; margin-bottom:16px; }
.note-card { background:var(--bg2); border:1px solid var(--border); border-radius:10px; padding:16px 20px; margin-bottom:12px; }
.note-card .note-header { display:flex; justify-content:space-between; margin-bottom:8px; }
.note-card .note-title { font-weight:600; font-size:15px; }
.note-card .note-date { font-size:12px; color:var(--text3); }
.note-card ul { margin:0; padding-left:20px; }
.note-card li { font-size:13px; color:var(--text2); margin-bottom:4px; }

/* Lesson Page */
.lesson-header { margin-bottom:32px; padding-bottom:20px; border-bottom:1px solid var(--border); }
.lesson-header .breadcrumb { font-size:13px; color:var(--text3); margin-bottom:8px; }
.lesson-header h2 { font-size:28px; font-weight:700; margin-bottom:8px; }
.lesson-header .meta { display:flex; gap:12px; align-items:center; }
.badge { padding:3px 10px; border-radius:4px; font-size:12px; font-weight:600; }
.badge-done { background:rgba(63,185,80,0.15); color:var(--green); }
.badge-pending { background:rgba(139,148,158,0.15); color:var(--text2); }
.badge-stage { color:#fff; }

.lesson-content { line-height:1.8; }
.lesson-content h3 { font-size:20px; font-weight:600; margin:28px 0 12px; padding-bottom:8px; border-bottom:1px solid var(--border); }
.lesson-content h4 { font-size:16px; font-weight:600; margin:20px 0 8px; color:var(--blue); }
.lesson-content p { margin-bottom:14px; font-size:15px; }
.lesson-content ul, .lesson-content ol { margin-bottom:14px; padding-left:24px; }
.lesson-content li { margin-bottom:6px; font-size:15px; }
.lesson-content code { background:var(--bg3); padding:2px 6px; border-radius:4px; font-size:13px; color:var(--orange); }
.lesson-content .formula-block { background:var(--bg2); border:1px solid var(--border); border-radius:8px; padding:16px 20px; margin:16px 0; text-align:center; overflow-x:auto; }
.lesson-content .highlight-box { background:rgba(88,166,255,0.08); border-left:3px solid var(--blue); padding:12px 16px; margin:16px 0; border-radius:0 8px 8px 0; }
.lesson-content .warning-box { background:rgba(248,81,73,0.08); border-left:3px solid var(--red); padding:12px 16px; margin:16px 0; border-radius:0 8px 8px 0; }
.lesson-content .success-box { background:rgba(63,185,80,0.08); border-left:3px solid var(--green); padding:12px 16px; margin:16px 0; border-radius:0 8px 8px 0; }

.chart-container { background:var(--bg2); border:1px solid var(--border); border-radius:12px; padding:16px; margin:20px 0; }
.chart-container h4 { margin:0 0 12px; font-size:15px; color:var(--text); }
.chart-controls { display:flex; gap:12px; align-items:center; margin-bottom:12px; flex-wrap:wrap; }
.chart-controls label { font-size:13px; color:var(--text2); }
.chart-controls input[type=range] { width:120px; accent-color:var(--blue); }
.chart-controls .val { font-size:13px; color:var(--blue); min-width:40px; }
.chart-plot { width:100%; height:400px; }

.takeaways { background:var(--bg2); border:1px solid var(--border); border-radius:12px; padding:20px; margin:28px 0; }
.takeaways h4 { font-size:16px; margin-bottom:12px; color:var(--green); }
.takeaways li { margin-bottom:8px; }
.beliefs-box { background:var(--bg2); border:1px solid var(--border); border-radius:12px; padding:20px; margin:20px 0; }
.beliefs-box h4 { font-size:16px; margin-bottom:12px; color:var(--purple); }
.belief-tag { display:inline-block; padding:4px 10px; margin:4px; background:rgba(188,140,255,0.1); border:1px solid rgba(188,140,255,0.3); border-radius:6px; font-size:13px; }

.lesson-nav { display:flex; justify-content:space-between; margin-top:40px; padding-top:20px; border-top:1px solid var(--border); }
.lesson-nav button { padding:10px 20px; background:var(--bg3); border:1px solid var(--border); color:var(--text); border-radius:8px; cursor:pointer; font-size:14px; }
.lesson-nav button:hover { border-color:var(--blue); color:var(--blue); }
.lesson-nav button:disabled { opacity:0.3; cursor:default; }

@media (max-width:900px) {
  .sidebar { display:none; }
  .main { margin-left:0; padding:20px; }
  .stats-row { grid-template-columns:repeat(2,1fr); }
  .stage-cards { grid-template-columns:1fr; }
}
</style>
</head>
<body>
<div class="app">
  <nav class="sidebar" id="sidebar"></nav>
  <main class="main" id="main"></main>
</div>

<script>
// ============================================================
// Black-Scholes Greeks (client-side)
// ============================================================
const BS = {
  normPDF: x => Math.exp(-0.5*x*x) / Math.sqrt(2*Math.PI),
  normCDF: x => {
    const a1=0.254829592, a2=-0.284496736, a3=1.421413741, a4=-1.453152027, a5=1.061405429, p=0.3275911;
    const sign = x<0 ? -1 : 1; x = Math.abs(x)/Math.sqrt(2);
    const t = 1/(1+p*x);
    const y = 1 - (((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*Math.exp(-x*x);
    return 0.5*(1+sign*y);
  },
  d1: (S,K,T,r,σ) => (Math.log(S/K)+(r+0.5*σ*σ)*T)/(σ*Math.sqrt(T)),
  d2: (S,K,T,r,σ) => BS.d1(S,K,T,r,σ) - σ*Math.sqrt(T),
  delta: (S,K,T,r,σ,type='call') => {
    if(T<=0) return type==='call'?(S>K?1:0):(S<K?-1:0);
    const d = BS.d1(S,K,T,r,σ);
    return type==='call' ? BS.normCDF(d) : BS.normCDF(d)-1;
  },
  gamma: (S,K,T,r,σ) => {
    if(T<=0) return 0;
    return BS.normPDF(BS.d1(S,K,T,r,σ))/(S*σ*Math.sqrt(T));
  },
  theta: (S,K,T,r,σ,type='call') => {
    if(T<=0) return 0;
    const d1=BS.d1(S,K,T,r,σ), d2=BS.d2(S,K,T,r,σ);
    let th = -S*BS.normPDF(d1)*σ/(2*Math.sqrt(T));
    if(type==='call') th -= r*K*Math.exp(-r*T)*BS.normCDF(d2);
    else th += r*K*Math.exp(-r*T)*BS.normCDF(-d2);
    return th/365;
  },
  vega: (S,K,T,r,σ) => {
    if(T<=0) return 0;
    return S*Math.sqrt(T)*BS.normPDF(BS.d1(S,K,T,r,σ))/100;
  },
  vanna: (S,K,T,r,σ) => {
    if(T<=0) return 0;
    const d1=BS.d1(S,K,T,r,σ), d2=BS.d2(S,K,T,r,σ);
    return -BS.normPDF(d1)*d2/σ;
  },
  charm: (S,K,T,r,σ) => {
    if(T<=0) return 0;
    const d1=BS.d1(S,K,T,r,σ), d2=BS.d2(S,K,T,r,σ);
    return -BS.normPDF(d1)*(2*r*T - d2*σ*Math.sqrt(T))/(2*T*σ*Math.sqrt(T));
  },
  volga: (S,K,T,r,σ) => {
    if(T<=0) return 0;
    const d1=BS.d1(S,K,T,r,σ), d2=BS.d2(S,K,T,r,σ);
    return BS.vega(S,K,T,r,σ)*d1*d2/σ;
  }
};

// ============================================================
// Course Data
// ============================================================
const STAGE_COLORS = ['#58a6ff','#bc8cff','#3fb950','#d29922','#f85149','#e3b341'];
const STAGE_NAMES = ['Greeks体系','波动率曲面','组合策略','风险管理','定价理论','高级策略+ML'];
const STAGE_DESCS = ['从三因素直觉升级到全Greeks量化','从看单个IV到看整个波动率曲面','从只会宽跨到策略工具箱','从单策略止损到组合风险管理','理解引擎盖下面的东西','前沿探索：ML + 做市 + 统计套利'];
const STAGE_LESSONS = [4,6,4,4,4,6];

const LESSONS = [
  // Stage 1: Greeks
  {id:1, title:'Gamma深度理解', stage:0, status:'done', date:'2026-03-03',
   content: `
<h3>一、什么是Gamma？</h3>
<p>Gamma (Γ) 是Delta对标的价格的导数——也就是 <strong>Delta的加速度</strong>。</p>
<div class="formula-block">$$\\Gamma = \\frac{\\partial \\Delta}{\\partial S} = \\frac{\\partial^2 V}{\\partial S^2}$$</div>
<p>直觉理解：如果Delta告诉你"车速是多少"，Gamma告诉你"踩油门有多猛"。Gamma永远为正（无论Call还是Put），因为期权的凸性(convexity)保证了Delta变化方向与标的价格变化方向一致。</p>

<h4>ATM Gamma最大</h4>
<p>ATM期权的Gamma最大，因为ATM处期权的Delta对价格最敏感。随着期权走向深度ITM或深度OTM，Gamma趋近于零——Delta已经接近±1或0，不再需要"加速"。</p>

<div class="formula-block">$$\\Gamma_{ATM} \\approx \\frac{1}{S \\cdot \\sigma \\cdot \\sqrt{2\\pi T}}$$</div>

<h4>Gamma与时间的关系：针状效应</h4>
<div class="highlight-box">
<strong>关键洞察</strong>：DTE越短，ATM附近的Gamma越"尖锐"——像一根针。这意味着临近到期时，ATM期权的Delta变化极其剧烈。
</div>
<p>下面的交互图展示了不同DTE下Gamma的形态变化：</p>

<div class="chart-container">
  <h4>Gamma vs 行权价 (不同DTE)</h4>
  <div class="chart-controls">
    <label>标的价格 S:</label><input type="range" id="g1_S" min="8000" max="16000" value="12000" step="100"><span class="val" id="g1_S_v">12000</span>
    <label>波动率 σ:</label><input type="range" id="g1_sig" min="10" max="60" value="25" step="1"><span class="val" id="g1_sig_v">25%</span>
  </div>
  <div class="chart-plot" id="chart_gamma_curve"></div>
</div>

<h3>二、卖出宽跨 = Short Gamma</h3>
<p>你的核心策略——卖出宽跨(Short Strangle)——本质上是 <strong>做空Gamma</strong>：</p>
<ul>
  <li><strong>小波动时赚钱</strong>：Theta持续衰减为你创造利润</li>
  <li><strong>大波动时亏钱</strong>：Gamma加速放大Delta敞口，亏损非线性增长</li>
</ul>
<div class="warning-box">
<strong>B013失败案例解释</strong>：近ATM + 低DTE = Gamma爆发区。此时Gamma极高（针状峰值），标的稍微一动，Delta就剧烈变化，Theta根本无法覆盖Gamma带来的亏损。
</div>

<h3>三、Theta/Gamma比率</h3>
<p>判断一个卖出策略是否安全的核心指标：</p>
<div class="formula-block">$$\\text{T/G Ratio} = \\frac{|\\Theta|}{\\Gamma}$$</div>
<div class="success-box">
<strong>B018甜蜜点解释</strong>：DTE≤7 + 远OTM = 高Theta + 低Gamma = T/G比率高。你远离了Gamma峰值，安全地收割时间价值。
</div>

<div class="chart-container">
  <h4>Gamma 3D曲面：行权价 × DTE</h4>
  <div class="chart-plot" id="chart_gamma_3d"></div>
</div>
`,
   takeaways: [
     'Gamma = Delta的加速度，永远为正，ATM最大',
     'DTE越短，ATM处Gamma越尖锐（针状）——临近到期的ATM期权极度危险',
     '卖出宽跨 = Short Gamma，本质是赌"标的不会大幅波动"',
     'T/G比率是卖方安全度的核心指标：B018甜蜜点 = 高Theta + 低Gamma',
     '系统缺口：trade2026中0行Gamma代码，需加入GEX监控'
   ],
   beliefs: ['B008','B013','B018']
  },

  {id:2, title:'Gamma Scalping', stage:0, status:'done', date:'2026-03-04',
   content: `
<h3>一、核心思想：赚路径，不赚位移</h3>
<p>Gamma Scalping 是一种 <strong>Long Gamma</strong> 策略：买入期权（获得正Gamma），然后通过不断Delta对冲来"收割"标的价格的波动。</p>
<div class="highlight-box">
<strong>关键区别</strong>：传统的方向性交易赚的是"从A到B的位移"，而Gamma Scalping赚的是"来回晃动的路径"——价格波动越大，不管方向，都赚钱。
</div>

<h4>对冲机制</h4>
<p>每次标的上涨 → Delta增大 → 卖出期货锁利；每次标的下跌 → Delta减小 → 买入期货锁利。本质是自动化的"高卖低买"。</p>

<h3>二、数学基础</h3>
<p>单次对冲的利润公式：</p>
<div class="formula-block">$$P\\&L_{hedge} = \\frac{1}{2} \\cdot \\Gamma \\cdot (\\Delta S)^2$$</div>
<p>注意是 <strong>平方项</strong>！价格变动2倍，利润变4倍。而且不分方向——涨跌都赚。</p>
<p>但成本是Theta——每天要支付时间价值衰减。盈亏平衡波动率：</p>
<div class="formula-block">$$\\sigma_{BE} = \\sqrt{\\frac{2|\\Theta|}{\\Gamma}}$$</div>
<p>这正是 <code>/gamma</code> 扫描计算的核心指标！当实际波动 > 盈亏平衡波动时，Scalping盈利。</p>

<div class="chart-container">
  <h4>Gamma Scalping 模拟演示</h4>
  <div class="chart-controls">
    <label>波动幅度:</label><input type="range" id="gs_vol" min="5" max="40" value="20" step="1"><span class="val" id="gs_vol_v">20%</span>
    <label>对冲频率(分钟):</label><input type="range" id="gs_freq" min="15" max="240" value="60" step="15"><span class="val" id="gs_freq_v">60</span>
  </div>
  <div class="chart-plot" id="chart_scalping_sim"></div>
</div>

<h3>三、白银实盘验证</h3>
<div class="success-box">
<strong>120分钟实盘数据</strong>：Gamma Scalping +3,351元 vs 纯持有 +1,224元，收益2.7倍！
</div>
<p>关键条件：ATM期权 + 低DTE + 震荡行情。深OTM期权的Gamma太小，不适合Scalping。</p>

<h3>四、与现有策略的关系</h3>
<div class="highlight-box">
<strong>T/G比率的双面解读</strong>：
<ul>
  <li>T/G比率低 → 卖方噩梦（Theta少、Gamma大）→ 但却是Scalper的天堂！</li>
  <li>棕榈油P的T/G比率仅1.3，极其适合做Gamma Scalping</li>
  <li>B006"弹簧效应"其实是被动版Gamma Scalping——主动对冲可以更稳定地兑现</li>
</ul>
</div>
`,
   takeaways: [
     'Gamma Scalping = Long Gamma + Delta对冲，赚路径不赚位移',
     '利润公式 ½×Γ×ΔS²是平方项——波动越大赚越多，不分方向',
     '盈亏平衡波动 = √(2Θ/Γ)，即/gamma扫描的核心指标',
     'T/G比低 = 卖方噩梦但Scalper天堂，P棕榈油(T/G=1.3)最适合',
     'B006弹簧效应是被动版Gamma Scalping，主动对冲更稳定'
   ],
   beliefs: ['B006','B008','B018']
  },

  {id:3, title:'Vanna & Charm', stage:0, status:'done', date:'2026-03-04',
   content: `
<h3>一、Charm：时间对Delta的侵蚀</h3>
<div class="formula-block">$$\\text{Charm} = \\frac{\\partial \\Delta}{\\partial t} = -\\frac{\\partial \\Theta}{\\partial S}$$</div>
<p>Charm描述的是：<strong>即使标的价格不动，你的Delta每天都在变</strong>。</p>
<ul>
  <li>OTM期权：Charm使Delta趋向0——期权在"蒸发"</li>
  <li>ITM期权：Charm使Delta趋向±1——期权在"凝固"</li>
  <li>ATM期权：Charm最不稳定——Delta的变化方向取决于精确位置</li>
</ul>
<div class="success-box">
<strong>Charm是卖方的朋友</strong>：时间流逝→OTM期权Delta蒸发→方向风险自动降低。B021策略（入DTE25→出DTE7）完美利用了这个效应。
</div>

<div class="chart-container">
  <h4>Charm效应：Delta随时间的衰减</h4>
  <div class="chart-controls">
    <label>标的价格:</label><input type="range" id="ch_S" min="10000" max="14000" value="12000" step="100"><span class="val" id="ch_S_v">12000</span>
    <label>波动率:</label><input type="range" id="ch_sig" min="10" max="50" value="25" step="1"><span class="val" id="ch_sig_v">25%</span>
  </div>
  <div class="chart-plot" id="chart_charm"></div>
</div>

<h3>二、Vanna：波动率对Delta的扰动</h3>
<div class="formula-block">$$\\text{Vanna} = \\frac{\\partial \\Delta}{\\partial \\sigma} = \\frac{\\partial \\text{Vega}}{\\partial S}$$</div>
<p>Vanna描述的是：<strong>IV变化如何改变Delta</strong>。</p>
<ul>
  <li>IV升高 → OTM期权的Delta增大 → 原本"快死的"期权突然"活过来"</li>
  <li>IV降低 → OTM期权的Delta减小 → 期权加速"死亡"</li>
</ul>
<div class="warning-box">
<strong>Vanna是卖方的敌人</strong>：IV飙升时，你卖出的OTM期权Delta突然增大，方向敞口剧变，这是MAE（最大逆向偏移）的重要来源。
</div>

<div class="chart-container">
  <h4>Vanna效应：不同IV下的Delta变化</h4>
  <div class="chart-plot" id="chart_vanna"></div>
</div>

<h3>三、实战分解：B004白银翻转事件</h3>
<p>白银C25000 + P22000组合，Delta从+0.3翻转到-0.7的分解：</p>
<table style="width:100%;border-collapse:collapse;margin:16px 0;">
  <tr style="border-bottom:1px solid var(--border);"><th style="text-align:left;padding:8px;">因素</th><th style="text-align:right;padding:8px;">贡献</th></tr>
  <tr><td style="padding:8px;">Gamma（期货暴跌-11%）</td><td style="text-align:right;padding:8px;color:var(--red);">-100.7%</td></tr>
  <tr><td style="padding:8px;">Vanna（IV变化）</td><td style="text-align:right;padding:8px;color:var(--orange);">+6.2%</td></tr>
  <tr><td style="padding:8px;">Charm（时间流逝）</td><td style="text-align:right;padding:8px;color:var(--green);">-5.5%</td></tr>
</table>
<p>结论：期货暴跌(-11%)是压倒性主因，Charm和Vanna几乎互相抵消。但在温和波动中，Charm和Vanna的影响不可忽略。</p>

<h3>四、CF vs SA 的MAE差异根因</h3>
<div class="highlight-box">
<strong>为什么CF的MAE(128%)远大于SA(27%)？</strong>
<ul>
  <li>CF的IV波动大 → Vanna效应大 → Delta剧变 → 价格之和大幅偏移</li>
  <li>SA的IV较稳定 → Vanna效应小 → Delta温和变化 → 价格之和平稳</li>
</ul>
<p>选品种时，IV的波动性（即Volga/Vomma）是预判MAE的关键指标。</p>
</div>
`,
   takeaways: [
     'Charm = ∂Delta/∂t：即使标的不动，Delta每天都在变。OTM期权的Delta随时间"蒸发"',
     'Vanna = ∂Delta/∂σ：IV升高让OTM期权"活过来"，是MAE的重要来源',
     'Charm是卖方朋友（Delta蒸发=风险降低），Vanna是卖方敌人（IV飙升=风险骤增）',
     'B004翻转分解：Gamma贡献-100.7%是主因，Charm和Vanna几乎抵消',
     'CF高MAE根因 = IV波动大 → Vanna效应大；选品种看IV稳定性'
   ],
   beliefs: ['B004','B016','B020','B021']
  },

  {id:4, title:'Volga (Vomma)', stage:0, status:'pending',
   content:`
<h3>课程预告</h3>
<p>Volga（又称Vomma）= ∂Vega/∂σ，是Vega对波动率的敏感度。它解释了：</p>
<ul>
  <li>为什么波动率微笑(Volatility Smile)存在——做市商用Volga定价尾部风险</li>
  <li>为什么B020双涨现象在DTE&lt;15时幅度更大——Volga放大效应</li>
  <li>波动率的"凸性"：IV大幅变化时，Vega本身也在变化</li>
</ul>
<div class="highlight-box">完成本课后，第一阶段(Greeks体系)收官，可进入第二阶段「波动率曲面」。</div>
`, takeaways:[], beliefs:['B020']},

  // Stage 2: Volatility Surface
  {id:5, title:'波动率微笑(Smile)', stage:1, status:'pending',
   content:'<h3>课程预告</h3><p>同一到期日不同行权价的IV差异；为什么OTM Put的IV更高；用历史数据画出白银/棕榈油的Smile曲线。</p>', takeaways:[], beliefs:[]},
  {id:6, title:'波动率期限结构', stage:1, status:'pending',
   content:'<h3>课程预告</h3><p>不同到期月的ATM IV对比；Contango vs Backwardation；对比604/605/606合约的IV水平。</p>', takeaways:[], beliefs:[]},
  {id:7, title:'波动率曲面建模(SVI)', stage:1, status:'pending',
   content:'<h3>课程预告</h3><p>SVI参数化：a,b,ρ,m,σ；曲面拟合与校准；构建实时波动率曲面监控工具。</p>', takeaways:[], beliefs:[]},
  {id:8, title:'波动率交易策略', stage:1, status:'pending',
   content:'<h3>课程预告</h3><p>IV均值回归策略；波动率偏度交易；Vega中性组合；系统化Vega交易框架。</p>', takeaways:[], beliefs:[]},
  {id:9, title:'HV vs IV分析', stage:1, status:'pending',
   content:'<h3>课程预告</h3><p>已实现波动率vs隐含波动率；波动率风险溢价(VRP)；用期货数据算HV，与实时IV对比。</p>', takeaways:[], beliefs:[]},
  {id:10, title:'波动率锥(Volatility Cone)', stage:1, status:'pending',
   content:'<h3>课程预告</h3><p>不同周期的HV百分位；判断当前IV是高是低；12品种的波动率锥数据库。</p>', takeaways:[], beliefs:[]},

  // Stage 3: Combo Strategies
  {id:11, title:'Iron Condor', stage:2, status:'pending',
   content:'<h3>课程预告</h3><p>卖宽跨+买更远虚值保护；有限风险；改造strangle_sell.py支持四腿组合。</p>', takeaways:[], beliefs:[]},
  {id:12, title:'Butterfly', stage:2, status:'pending',
   content:'<h3>课程预告</h3><p>精准赌区间；低成本+高赔率；适合低DTE品种。</p>', takeaways:[], beliefs:[]},
  {id:13, title:'Calendar Spread', stage:2, status:'pending',
   content:'<h3>课程预告</h3><p>跨月套利；卖近月买远月赚Theta差；利用DTE计算优势。</p>', takeaways:[], beliefs:[]},
  {id:14, title:'Ratio Spread', stage:2, status:'pending',
   content:'<h3>课程预告</h3><p>1:2或2:3比例组合；系统化加权系数选择。</p>', takeaways:[], beliefs:[]},

  // Stage 4: Risk Management
  {id:15, title:'场景分析', stage:3, status:'pending',
   content:'<h3>课程预告</h3><p>期货±5%/±10%/±20%时P&L变化；场景矩阵与压力测试。</p>', takeaways:[], beliefs:[]},
  {id:16, title:'Greeks加总与对冲', stage:3, status:'pending',
   content:'<h3>课程预告</h3><p>组合Greeks汇总；Delta对冲比例计算；动态对冲策略。</p>', takeaways:[], beliefs:[]},
  {id:17, title:'仓位管理(Kelly)', stage:3, status:'pending',
   content:'<h3>课程预告</h3><p>Kelly公式；固定比例法；最优仓位计算。</p>', takeaways:[], beliefs:[]},
  {id:18, title:'VaR与压力测试', stage:3, status:'pending',
   content:'<h3>课程预告</h3><p>历史VaR；参数VaR；蒙特卡洛VaR；用328天数据计算。</p>', takeaways:[], beliefs:[]},

  // Stage 5: Pricing Theory
  {id:19, title:'Black-Scholes推导', stage:4, status:'pending',
   content:'<h3>课程预告</h3><p>几何布朗运动→Ito引理→BS PDE→解析解。</p>', takeaways:[], beliefs:[]},
  {id:20, title:'二叉树模型', stage:4, status:'pending',
   content:'<h3>课程预告</h3><p>CRR模型；美式期权定价；提前行权风险。</p>', takeaways:[], beliefs:[]},
  {id:21, title:'隐含波动率计算', stage:4, status:'pending',
   content:'<h3>课程预告</h3><p>Newton-Raphson迭代；优化实时IV计算速度。</p>', takeaways:[], beliefs:[]},
  {id:22, title:'波动率模型对比', stage:4, status:'pending',
   content:'<h3>课程预告</h3><p>BS局限→Local Vol→Heston→SABR。</p>', takeaways:[], beliefs:[]},

  // Stage 6: Advanced + ML
  {id:23, title:'波动率预测(GARCH)', stage:5, status:'pending',
   content:'<h3>课程预告</h3><p>GARCH(1,1)；EGARCH；波动率聚集。</p>', takeaways:[], beliefs:[]},
  {id:24, title:'做市策略基础', stage:5, status:'pending',
   content:'<h3>课程预告</h3><p>Bid-Ask报价；存货风险；Avellaneda-Stoikov模型。</p>', takeaways:[], beliefs:[]},
  {id:25, title:'统计套利', stage:5, status:'pending',
   content:'<h3>课程预告</h3><p>协整关系；配对交易；跨品种相关性交易。</p>', takeaways:[], beliefs:[]},
  {id:26, title:'ML波动率预测', stage:5, status:'pending',
   content:'<h3>课程预告</h3><p>LSTM/XGBoost预测IV；特征工程。</p>', takeaways:[], beliefs:[]},
  {id:27, title:'强化学习做仓位管理', stage:5, status:'pending',
   content:'<h3>课程预告</h3><p>动态调仓；自适应止损。</p>', takeaways:[], beliefs:[]},
  {id:28, title:'系统集成', stage:5, status:'pending',
   content:'<h3>课程预告</h3><p>将所有新模块集成到trade2026。</p>', takeaways:[], beliefs:[]}
];

// ============================================================
// Chart Generators
// ============================================================
const PLOTLY_LAYOUT = {
  paper_bgcolor:'#161b22', plot_bgcolor:'#0d1117',
  font:{color:'#c9d1d9', size:12},
  margin:{l:60,r:30,t:40,b:50},
  xaxis:{gridcolor:'#21262d',zerolinecolor:'#30363d'},
  yaxis:{gridcolor:'#21262d',zerolinecolor:'#30363d'},
  legend:{bgcolor:'transparent'},
  colorway:['#58a6ff','#3fb950','#d29922','#f85149','#bc8cff','#39d2c0']
};
const PLOTLY_CFG = {displayModeBar:false, responsive:true};

function chartGammaCurve() {
  const el = document.getElementById('chart_gamma_curve');
  if(!el) return;
  function draw() {
    const S = +document.getElementById('g1_S').value;
    const sig = +document.getElementById('g1_sig').value / 100;
    document.getElementById('g1_S_v').textContent = S;
    document.getElementById('g1_sig_v').textContent = (sig*100)+'%';
    const dtes = [30,14,7,3];
    const colors = ['#58a6ff','#3fb950','#d29922','#f85149'];
    const traces = dtes.map((dte,i) => {
      const T = dte/365;
      const ks = []; const gs = [];
      for(let k=S*0.8; k<=S*1.2; k+=S*0.005) {
        ks.push(k);
        gs.push(BS.gamma(S,k,T,0.03,sig)*S/100);
      }
      return {x:ks, y:gs, name:`DTE=${dte}`, line:{color:colors[i],width:2}};
    });
    const layout = {...PLOTLY_LAYOUT, title:'Gamma vs 行权价',
      xaxis:{...PLOTLY_LAYOUT.xaxis,title:'行权价 (K)'},
      yaxis:{...PLOTLY_LAYOUT.yaxis,title:'Gamma (标准化)'},
      shapes:[{type:'line',x0:S,x1:S,y0:0,y1:1,yref:'paper',line:{color:'#484f58',dash:'dash'}}],
      annotations:[{x:S,y:1,yref:'paper',text:'ATM',showarrow:false,font:{color:'#8b949e'}}]
    };
    Plotly.newPlot(el, traces, layout, PLOTLY_CFG);
  }
  draw();
  document.getElementById('g1_S').oninput = draw;
  document.getElementById('g1_sig').oninput = draw;
}

function chartGamma3D() {
  const el = document.getElementById('chart_gamma_3d');
  if(!el) return;
  const S=12000, sig=0.25;
  const dtes=[]; const ks=[]; const gs=[];
  for(let dte=1;dte<=60;dte+=1) {
    const row=[];
    if(dtes.length===0) for(let k=S*0.85;k<=S*1.15;k+=S*0.01) ks.push(k);
    dtes.push(dte);
    for(let k=S*0.85;k<=S*1.15;k+=S*0.01) {
      row.push(BS.gamma(S,k,dte/365,0.03,sig)*S/100);
    }
    gs.push(row);
  }
  Plotly.newPlot(el, [{z:gs,x:ks,y:dtes,type:'surface',colorscale:'YlOrRd',showscale:true,
    colorbar:{title:'Γ',titleside:'right'}}],
    {...PLOTLY_LAYOUT, title:'Gamma 3D曲面', scene:{
      xaxis:{title:'行权价',gridcolor:'#21262d'},
      yaxis:{title:'DTE(天)',gridcolor:'#21262d'},
      zaxis:{title:'Gamma',gridcolor:'#21262d'},
      bgcolor:'#0d1117',
      camera:{eye:{x:1.5,y:-1.5,z:1.2}}
    }, margin:{l:0,r:0,t:40,b:0}}, PLOTLY_CFG);
}

function chartScalpingSim() {
  const el = document.getElementById('chart_scalping_sim');
  if(!el) return;
  function draw() {
    const annVol = +document.getElementById('gs_vol').value / 100;
    const freq = +document.getElementById('gs_freq').value;
    document.getElementById('gs_vol_v').textContent = (annVol*100)+'%';
    document.getElementById('gs_freq_v').textContent = freq;

    const S0=12000, T=1/365, sig=0.25, K=S0;
    const gamma0 = BS.gamma(S0,K,30/365,0.03,sig);
    const theta0 = BS.theta(S0,K,30/365,0.03,sig,'call');
    const steps = Math.floor(240/freq*1); // 1 day, 240 min
    const dailyVol = annVol/Math.sqrt(252);
    const stepVol = dailyVol/Math.sqrt(240/freq);

    // Simulate price path
    let seed = 42;
    function rand() { seed=(seed*16807)%2147483647; return seed/2147483647-0.5; }
    const times=[0], prices=[S0], scalpPnL=[0], holdPnL=[0];
    let price=S0, cumScalp=0, cumHold=0, delta0=BS.delta(S0,K,30/365,0.03,sig,'call');
    let hedgeDelta=delta0;

    for(let i=1;i<=steps*5;i++) {  // 5 days
      const dp = price * stepVol * rand() * 3;
      price += dp;
      times.push(i*freq);
      prices.push(price);

      const newDelta = BS.delta(price,K,30/365,0.03,sig,'call');
      const scalpGain = (newDelta-hedgeDelta)*(price-prices[prices.length-2])*0.5;
      cumScalp += Math.abs(scalpGain);
      hedgeDelta = newDelta;

      cumHold += (newDelta-delta0)*(price-S0)*0.01;
      scalpPnL.push(cumScalp - Math.abs(theta0)*i*freq/240);
      holdPnL.push((price-S0)*delta0);
    }

    Plotly.newPlot(el, [
      {x:times, y:scalpPnL, name:'Gamma Scalping P&L', line:{color:'#3fb950',width:2}},
      {x:times, y:holdPnL, name:'纯持有 P&L', line:{color:'#58a6ff',width:2,dash:'dash'}},
      {x:times, y:prices.map(p=>(p-S0)/10), name:'价格变动/10', line:{color:'#484f58',width:1}, yaxis:'y2'}
    ], {...PLOTLY_LAYOUT, title:'Scalping vs 纯持有',
      xaxis:{...PLOTLY_LAYOUT.xaxis,title:'时间(分钟)'},
      yaxis:{...PLOTLY_LAYOUT.yaxis,title:'P&L'},
      yaxis2:{overlaying:'y',side:'right',showgrid:false,title:'价格偏移',font:{color:'#484f58'}}
    }, PLOTLY_CFG);
  }
  draw();
  document.getElementById('gs_vol').oninput = draw;
  document.getElementById('gs_freq').oninput = draw;
}

function chartCharm() {
  const el = document.getElementById('chart_charm');
  if(!el) return;
  function draw() {
    const S = +document.getElementById('ch_S').value;
    const sig = +document.getElementById('ch_sig').value / 100;
    document.getElementById('ch_S_v').textContent = S;
    document.getElementById('ch_sig_v').textContent = (sig*100)+'%';

    const strikes = [S*0.92, S*0.96, S, S*1.04, S*1.08];
    const labels = ['深OTM Put','OTM Put','ATM','OTM Call','深OTM Call'];
    const colors = ['#f85149','#d29922','#8b949e','#3fb950','#58a6ff'];
    const traces = strikes.map((K,i) => {
      const dtes=[]; const deltas=[];
      for(let d=60;d>=1;d--) {
        dtes.push(d);
        deltas.push(K<=S ? BS.delta(S,K,d/365,0.03,sig,'put') : BS.delta(S,K,d/365,0.03,sig,'call'));
      }
      return {x:dtes,y:deltas,name:`${labels[i]} K=${Math.round(K)}`,line:{color:colors[i],width:2}};
    });
    Plotly.newPlot(el, traces, {...PLOTLY_LAYOUT, title:'Delta随DTE衰减 (Charm效应)',
      xaxis:{...PLOTLY_LAYOUT.xaxis,title:'DTE (天)',autorange:'reversed'},
      yaxis:{...PLOTLY_LAYOUT.yaxis,title:'Delta'}
    }, PLOTLY_CFG);
  }
  draw();
  document.getElementById('ch_S').oninput = draw;
  document.getElementById('ch_sig').oninput = draw;
}

function chartVanna() {
  const el = document.getElementById('chart_vanna');
  if(!el) return;
  const S=12000, K_otm_c=S*1.05, K_atm=S, K_otm_p=S*0.95;
  const ivs=[]; const d_c=[]; const d_a=[]; const d_p=[];
  for(let iv=10;iv<=50;iv+=1) {
    ivs.push(iv);
    d_c.push(BS.delta(S,K_otm_c,30/365,0.03,iv/100,'call'));
    d_a.push(BS.delta(S,K_atm,30/365,0.03,iv/100,'call'));
    d_p.push(BS.delta(S,K_otm_p,30/365,0.03,iv/100,'put'));
  }
  Plotly.newPlot(el, [
    {x:ivs,y:d_c,name:`OTM Call K=${K_otm_c}`,line:{color:'#3fb950',width:2}},
    {x:ivs,y:d_a,name:`ATM Call K=${K_atm}`,line:{color:'#8b949e',width:2,dash:'dash'}},
    {x:ivs,y:d_p,name:`OTM Put K=${K_otm_p}`,line:{color:'#f85149',width:2}}
  ], {...PLOTLY_LAYOUT, title:'Delta随IV变化 (Vanna效应)',
    xaxis:{...PLOTLY_LAYOUT.xaxis,title:'IV (%)'},
    yaxis:{...PLOTLY_LAYOUT.yaxis,title:'Delta'},
    annotations:[{x:25,y:0.5,text:'IV升高→OTM Delta增大<br>期权"活过来"',showarrow:true,ax:60,ay:-40,font:{color:'#d29922',size:11}}]
  }, PLOTLY_CFG);
}

// ============================================================
// Router & Renderers
// ============================================================
let currentView = 'dashboard';
let currentLesson = null;

function navigate(view, lessonId) {
  currentView = view;
  currentLesson = lessonId;
  render();
  window.scrollTo(0,0);
}

function render() {
  renderSidebar();
  if(currentView === 'dashboard') renderDashboard();
  else if(currentView === 'lesson') renderLesson(currentLesson);
}

function renderSidebar() {
  const completed = LESSONS.filter(l=>l.status==='done').length;
  let html = `
    <div class="logo">
      <h1>期权量化交易</h1>
      <p>学习平台 · 28课时 · 6阶段</p>
    </div>
    <div class="progress-mini" onclick="navigate('dashboard')">
      <span>总进度</span><strong>${completed}/28</strong>
      <div class="bar"><div class="bar-fill" style="width:${completed/28*100}%"></div></div>
    </div>`;

  let lessonIdx = 0;
  STAGE_NAMES.forEach((name,si) => {
    const count = STAGE_LESSONS[si];
    const stageLessons = LESSONS.slice(lessonIdx, lessonIdx+count);
    const done = stageLessons.filter(l=>l.status==='done').length;
    const isOpen = currentView==='lesson' && stageLessons.some(l=>l.id===currentLesson);

    html += `<div class="nav-stage ${isOpen?'open':''}" data-stage="${si}">
      <div class="nav-stage-header" onclick="this.parentElement.classList.toggle('open')">
        <span class="dot" style="background:${STAGE_COLORS[si]}"></span>
        ${name} <span style="font-size:11px;color:var(--text3)">${done}/${count}</span>
        <span class="arrow">▶</span>
      </div>
      <div class="nav-lessons">`;
    stageLessons.forEach(l => {
      const icon = l.status==='done' ? '✅' : '○';
      const cls = (currentView==='lesson'&&currentLesson===l.id) ? 'active' : '';
      html += `<div class="nav-lesson ${cls}" onclick="navigate('lesson',${l.id})">
        <span class="status">${icon}</span> ${l.id}. ${l.title}
      </div>`;
    });
    html += '</div></div>';
    lessonIdx += count;
  });

  document.getElementById('sidebar').innerHTML = html;
}

function renderDashboard() {
  const completed = LESSONS.filter(l=>l.status==='done').length;
  const nextLesson = LESSONS.find(l=>l.status==='pending');
  const doneLessons = LESSONS.filter(l=>l.status==='done');
  const pct = Math.round(completed/28*100);

  // Stage stats
  let stagesDone = 0;
  let lessonIdx = 0;
  STAGE_NAMES.forEach((n,i) => {
    const sl = LESSONS.slice(lessonIdx, lessonIdx+STAGE_LESSONS[i]);
    if(sl.every(l=>l.status==='done')) stagesDone++;
    lessonIdx += STAGE_LESSONS[i];
  });

  let html = `
    <div class="dash-header">
      <h2>学习进度总览</h2>
      <p>从实战高手到量化研究员的进阶之路</p>
    </div>
    <div class="stats-row">
      <div class="stat-card"><div class="value" style="color:var(--blue)">${completed}</div><div class="label">已完成课时</div></div>
      <div class="stat-card"><div class="value" style="color:var(--text3)">${28-completed}</div><div class="label">剩余课时</div></div>
      <div class="stat-card"><div class="value" style="color:var(--green)">${stagesDone}/6</div><div class="label">已完成阶段</div></div>
      <div class="stat-card"><div class="value" style="color:var(--purple)">${pct}%</div><div class="label">总体进度</div></div>
    </div>`;

  if(nextLesson) {
    html += `
    <div class="next-lesson-card">
      <div class="icon">📖</div>
      <div>
        <h3>下一课：${nextLesson.title}</h3>
        <p>第${nextLesson.stage+1}阶段 · ${STAGE_NAMES[nextLesson.stage]} · 课时${nextLesson.id}/28</p>
      </div>
      <button onclick="navigate('lesson',${nextLesson.id})">继续学习 →</button>
    </div>`;
  }

  html += '<div class="stage-cards">';
  lessonIdx = 0;
  STAGE_NAMES.forEach((name,i) => {
    const count = STAGE_LESSONS[i];
    const sl = LESSONS.slice(lessonIdx, lessonIdx+count);
    const done = sl.filter(l=>l.status==='done').length;
    const pct = done/count*100;
    html += `
    <div class="stage-card" onclick="navigate('lesson',${sl[0].id})">
      <h3><span class="stage-num badge-stage" style="background:${STAGE_COLORS[i]}">第${i+1}阶段</span> ${name}</h3>
      <div class="desc">${STAGE_DESCS[i]}</div>
      <div class="bar"><div class="bar-fill" style="width:${pct}%;background:${STAGE_COLORS[i]}"></div></div>
      <div class="meta">${done}/${count} 课时 · ${done===count?'已完成':done>0?'进行中':'待开始'}</div>
    </div>`;
    lessonIdx += count;
  });
  html += '</div>';

  if(doneLessons.length > 0) {
    html += '<div class="notes-section"><h3>学习笔记</h3>';
    doneLessons.forEach(l => {
      html += `<div class="note-card">
        <div class="note-header"><span class="note-title">课时${l.id}: ${l.title}</span><span class="note-date">${l.date}</span></div>
        <ul>${l.takeaways.map(t=>'<li>'+t+'</li>').join('')}</ul>
      </div>`;
    });
    html += '</div>';
  }

  document.getElementById('main').innerHTML = html;
}

function renderLesson(id) {
  const lesson = LESSONS.find(l=>l.id===id);
  if(!lesson) return navigate('dashboard');

  const stageColor = STAGE_COLORS[lesson.stage];
  const statusBadge = lesson.status==='done'
    ? `<span class="badge badge-done">✅ 已完成 ${lesson.date}</span>`
    : '<span class="badge badge-pending">○ 待学习</span>';

  let html = `
    <div class="lesson-header">
      <div class="breadcrumb">第${lesson.stage+1}阶段 · ${STAGE_NAMES[lesson.stage]} → 课时${lesson.id}</div>
      <h2>${lesson.title}</h2>
      <div class="meta">
        ${statusBadge}
        <span class="badge badge-stage" style="background:${stageColor}">${STAGE_NAMES[lesson.stage]}</span>
      </div>
    </div>
    <div class="lesson-content">${lesson.content}</div>`;

  if(lesson.takeaways.length > 0) {
    html += `<div class="takeaways"><h4>关键收获</h4><ol>${lesson.takeaways.map(t=>'<li>'+t+'</li>').join('')}</ol></div>`;
  }
  if(lesson.beliefs.length > 0) {
    html += `<div class="beliefs-box"><h4>关联信念</h4>${lesson.beliefs.map(b=>'<span class="belief-tag">'+b+'</span>').join('')}</div>`;
  }

  // Navigation
  const prev = id > 1 ? LESSONS.find(l=>l.id===id-1) : null;
  const next = id < 28 ? LESSONS.find(l=>l.id===id+1) : null;
  html += `<div class="lesson-nav">
    <button ${prev?`onclick="navigate('lesson',${prev.id})"`:'disabled'}>${prev?'← '+prev.title:'首页'}</button>
    <button onclick="navigate('dashboard')">总览</button>
    <button ${next?`onclick="navigate('lesson',${next.id})"`:'disabled'}>${next?next.title+' →':'已完成'}</button>
  </div>`;

  document.getElementById('main').innerHTML = html;

  // Render math
  setTimeout(() => {
    renderMathInElement(document.getElementById('main'), {
      delimiters: [
        {left:'$$',right:'$$',display:true},
        {left:'$',right:'$',display:false}
      ],
      throwOnError: false
    });
  }, 50);

  // Render charts
  setTimeout(() => {
    if(id===1) { chartGammaCurve(); chartGamma3D(); }
    if(id===2) { chartScalpingSim(); }
    if(id===3) { chartCharm(); chartVanna(); }
  }, 100);
}

// ============================================================
// Init
// ============================================================
window.onload = () => {
  const hash = window.location.hash;
  if(hash.startsWith('#lesson/')) {
    const id = parseInt(hash.split('/')[1]);
    if(id>=1 && id<=28) navigate('lesson', id);
    else navigate('dashboard');
  } else {
    navigate('dashboard');
  }
};
window.onhashchange = () => {
  const hash = window.location.hash;
  if(hash.startsWith('#lesson/')) navigate('lesson', parseInt(hash.split('/')[1]));
  else if(hash==='' || hash==='#') navigate('dashboard');
};
</script>
</body>
</html>"""

if __name__ == '__main__':
    print(f"\n  期权量化交易学习平台")
    print(f"  http://localhost:{PORT}\n")
    app.run(host='0.0.0.0', port=PORT, debug=False)
