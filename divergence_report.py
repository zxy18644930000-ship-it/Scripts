"""
腿背离策略 全市场回测报告生成器
读取 divergence_results/*.json → 生成 HTML 可视化报告
"""
import os, json, glob
import pandas as pd

HOME = os.path.expanduser('~')
RESULT_DIR = f'{HOME}/Scripts/divergence_results'
OUT_HTML = f'{HOME}/Scripts/divergence_report.html'


def load_all_results():
    """加载所有品种结果"""
    products = []
    for fp in sorted(glob.glob(f'{RESULT_DIR}/*.json')):
        fname = os.path.basename(fp).replace('.json', '')
        if fname.startswith('_'):
            continue
        try:
            with open(fp) as f:
                d = json.load(f)
            ex, prod = fname.split('_', 1)
            best_a = d.get('best_A', {})
            best_b = d.get('best_B', {})

            # 计算策略A所有组合盈利率
            summary = pd.DataFrame(d.get('summary', []))
            a_combos = summary[summary['strategy'] == 'A']
            b_combos = summary[summary['strategy'] == 'B']
            a_profit_pct = (a_combos['total_pnl'] > 0).mean() * 100 if len(a_combos) > 0 else 0
            b_profit_pct = (b_combos['total_pnl'] > 0).mean() * 100 if len(b_combos) > 0 else 0

            products.append({
                'symbol': f'{ex}.{prod}',
                'exchange': ex,
                'product': prod,
                'valid_days': d.get('valid_days', 0),
                'total_days': d.get('total_days', 0),
                'tick_size': d.get('tick_size', 0),
                # Strategy A
                'a_pnl': best_a.get('total_pnl', 0),
                'a_wr': best_a.get('wr', 0),
                'a_trades': best_a.get('trades', 0),
                'a_avg_pnl': best_a.get('avg_pnl', 0),
                'a_avg_hold': best_a.get('avg_hold', 0),
                'a_tp_pct': best_a.get('tp_pct', 0),
                'a_sl_pct': best_a.get('sl_pct', 0),
                'a_lookback': best_a.get('lookback', ''),
                'a_comp': best_a.get('comp_th', ''),
                'a_hold': best_a.get('hold_limit', ''),
                'a_tp_mult': best_a.get('tp_mult', ''),
                'a_sl_mult': best_a.get('sl_mult', ''),
                'a_profit_pct': round(a_profit_pct, 0),
                # Strategy B
                'b_pnl': best_b.get('total_pnl', 0),
                'b_wr': best_b.get('wr', 0),
                'b_trades': best_b.get('trades', 0),
                'b_profit_pct': round(b_profit_pct, 0),
                # Winner
                'winner': 'A' if best_a.get('total_pnl', 0) > best_b.get('total_pnl', 0) else 'B',
                # All combos for heatmaps
                'all_a': a_combos.to_dict('records') if len(a_combos) > 0 else [],
                'all_b': b_combos.to_dict('records') if len(b_combos) > 0 else [],
            })
        except Exception as e:
            print(f"  跳过 {fname}: {e}")
    return products


def generate_html(products):
    """生成完整HTML报告"""
    n = len(products)
    a_wins = sum(1 for p in products if p['winner'] == 'A')
    b_wins = n - a_wins

    # 按 A PnL 排序
    products.sort(key=lambda x: x['a_pnl'], reverse=True)

    # 分层
    tiers = {'S': [], 'A': [], 'B': [], 'C': []}
    for p in products:
        if p['a_pnl'] > 3000 and p['a_wr'] > 65:
            tiers['S'].append(p)
        elif p['a_pnl'] > 1000 and p['a_wr'] > 60:
            tiers['A'].append(p)
        elif p['a_pnl'] > 0 and p['a_wr'] > 55:
            tiers['B'].append(p)
        else:
            tiers['C'].append(p)

    # 统计
    avg_a_wr = sum(p['a_wr'] for p in products) / n if n > 0 else 0
    avg_a_pnl = sum(p['a_pnl'] for p in products) / n if n > 0 else 0
    total_a_pnl = sum(p['a_pnl'] for p in products)
    a_100pct = sum(1 for p in products if p['a_profit_pct'] == 100)

    # 参数频率统计 (top产品)
    top_products = [p for p in products if p['a_pnl'] > 500]
    lb_counts = {}
    comp_counts = {}
    hold_counts = {}
    tp_counts = {}
    sl_counts = {}
    for p in top_products:
        lb = p['a_lookback']
        if lb: lb_counts[lb] = lb_counts.get(lb, 0) + 1
        comp = p['a_comp']
        if comp: comp_counts[comp] = comp_counts.get(comp, 0) + 1
        hold = p['a_hold']
        if hold: hold_counts[hold] = hold_counts.get(hold, 0) + 1
        tp = p['a_tp_mult']
        if tp: tp_counts[tp] = tp_counts.get(tp, 0) + 1
        sl = p['a_sl_mult']
        if sl: sl_counts[sl] = sl_counts.get(sl, 0) + 1

    # 交易所分布
    ex_counts = {}
    for p in products:
        ex = p['exchange']
        ex_counts[ex] = ex_counts.get(ex, 0) + 1

    # 排名数据 (for bar chart)
    rank_symbols = [p['symbol'] for p in products[:30]]
    rank_a_pnl = [p['a_pnl'] for p in products[:30]]
    rank_b_pnl = [p['b_pnl'] for p in products[:30]]
    rank_a_wr = [p['a_wr'] for p in products[:30]]

    # 表格行
    table_rows = ""
    for i, p in enumerate(products):
        tier = 'S' if p in tiers['S'] else ('A' if p in tiers['A'] else ('B' if p in tiers['B'] else 'C'))
        tier_colors = {'S': '#ff6b35', 'A': '#4ecdc4', 'B': '#45b7d1', 'C': '#96ceb4'}
        winner_badge = '<span style="color:#4ecdc4;font-weight:bold">A✓</span>' if p['winner'] == 'A' else '<span style="color:#ff6b6b;font-weight:bold">B✓</span>'
        a_profit_bar = f'<div style="background:linear-gradient(90deg,#4ecdc4 {p["a_profit_pct"]}%,#2d3436 {p["a_profit_pct"]}%);height:18px;border-radius:3px;text-align:center;color:white;font-size:11px;line-height:18px">{p["a_profit_pct"]:.0f}%</div>'

        params = f'LB{p["a_lookback"]}/C{p["a_comp"]}/H{p["a_hold"]}m/TP{p["a_tp_mult"]}x/SL{p["a_sl_mult"]}x'

        table_rows += f'''<tr>
            <td>{i+1}</td>
            <td><span style="display:inline-block;width:20px;height:20px;line-height:20px;text-align:center;border-radius:50%;background:{tier_colors[tier]};color:white;font-weight:bold;font-size:11px">{tier}</span></td>
            <td style="font-weight:bold">{p['symbol']}</td>
            <td>{p['valid_days']}/{p['total_days']}</td>
            <td style="color:{'#4ecdc4' if p['a_pnl']>0 else '#ff6b6b'};font-weight:bold">{p['a_pnl']:.0f}</td>
            <td>{p['a_wr']:.1f}%</td>
            <td>{p['a_trades']}</td>
            <td>{a_profit_bar}</td>
            <td style="color:{'#4ecdc4' if p['b_pnl']>0 else '#ff6b6b'}">{p['b_pnl']:.0f}</td>
            <td>{winner_badge}</td>
            <td style="font-size:11px;color:#aaa">{params}</td>
        </tr>'''

    html = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>腿背离策略 全市场回测报告</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ background:#1a1a2e; color:#e0e0e0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; padding:20px; }}
.header {{ text-align:center; margin-bottom:30px; }}
.header h1 {{ color:#4ecdc4; font-size:28px; margin-bottom:5px; }}
.header p {{ color:#888; font-size:14px; }}
.cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:15px; margin-bottom:25px; }}
.card {{ background:#16213e; border-radius:10px; padding:18px; text-align:center; border:1px solid #0f3460; }}
.card .value {{ font-size:32px; font-weight:bold; margin:8px 0; }}
.card .label {{ color:#888; font-size:12px; }}
.card .sub {{ color:#aaa; font-size:11px; margin-top:5px; }}
.green {{ color:#4ecdc4; }}
.red {{ color:#ff6b6b; }}
.orange {{ color:#ff6b35; }}
.section {{ background:#16213e; border-radius:10px; padding:20px; margin-bottom:20px; border:1px solid #0f3460; }}
.section h2 {{ color:#4ecdc4; font-size:18px; margin-bottom:15px; border-bottom:1px solid #0f3460; padding-bottom:8px; }}
.chart {{ width:100%; height:400px; }}
.chart-wide {{ width:100%; height:500px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th {{ background:#0f3460; color:#4ecdc4; padding:10px 8px; text-align:left; position:sticky; top:0; }}
td {{ padding:8px; border-bottom:1px solid #1a1a2e; }}
tr:hover {{ background:#1a2744; }}
.tier-legend {{ display:flex; gap:15px; margin-bottom:15px; }}
.tier-legend span {{ display:inline-flex; align-items:center; gap:5px; font-size:12px; }}
.conclusion {{ background:#0f3460; border-radius:10px; padding:20px; margin-top:20px; border-left:4px solid #ff6b35; }}
.conclusion h2 {{ color:#ff6b35; margin-bottom:10px; }}
.conclusion li {{ margin:5px 0; line-height:1.6; }}
</style>
</head>
<body>

<div class="header">
    <h1>腿背离策略 全市场回测报告</h1>
    <p>背离信号: 高价腿方向 ≠ Sum方向 + IV不动 | 策略A=均值回归(卖) | 策略B=动量(买) | {n}个品种</p>
</div>

<!-- KPI Cards -->
<div class="cards">
    <div class="card">
        <div class="label">回测品种</div>
        <div class="value green">{n}</div>
        <div class="sub">5交易所全覆盖</div>
    </div>
    <div class="card">
        <div class="label">A策略(均值回归)胜出</div>
        <div class="value green">{a_wins}/{n}</div>
        <div class="sub">{a_wins/n*100:.0f}%品种A更优</div>
    </div>
    <div class="card">
        <div class="label">A策略100%组合盈利</div>
        <div class="value orange">{a_100pct}</div>
        <div class="sub">216组参数全盈利的品种</div>
    </div>
    <div class="card">
        <div class="label">A策略平均WR</div>
        <div class="value green">{avg_a_wr:.1f}%</div>
        <div class="sub">最优参数平均胜率</div>
    </div>
    <div class="card">
        <div class="label">A策略总PnL</div>
        <div class="value green">{total_a_pnl:,.0f}</div>
        <div class="sub">所有品种最优参数求和</div>
    </div>
    <div class="card">
        <div class="label">B策略(动量)胜出</div>
        <div class="value red">{b_wins}/{n}</div>
        <div class="sub">仅{b_wins}品种B更优</div>
    </div>
</div>

<!-- Tier Summary -->
<div class="cards">
    <div class="card" style="border-color:#ff6b35">
        <div class="label">S级 (PnL>3000, WR>65%)</div>
        <div class="value orange">{len(tiers['S'])}</div>
        <div class="sub">{', '.join(p['symbol'] for p in tiers['S'][:8])}</div>
    </div>
    <div class="card" style="border-color:#4ecdc4">
        <div class="label">A级 (PnL>1000, WR>60%)</div>
        <div class="value green">{len(tiers['A'])}</div>
        <div class="sub">{', '.join(p['symbol'] for p in tiers['A'][:8])}</div>
    </div>
    <div class="card" style="border-color:#45b7d1">
        <div class="label">B级 (PnL>0, WR>55%)</div>
        <div class="value" style="color:#45b7d1">{len(tiers['B'])}</div>
        <div class="sub">{', '.join(p['symbol'] for p in tiers['B'][:8])}</div>
    </div>
    <div class="card" style="border-color:#96ceb4">
        <div class="label">C级 (其余)</div>
        <div class="value" style="color:#96ceb4">{len(tiers['C'])}</div>
        <div class="sub">{', '.join(p['symbol'] for p in tiers['C'][:8])}</div>
    </div>
</div>

<!-- Ranking Chart -->
<div class="section">
    <h2>品种排名 (Top 30)</h2>
    <div id="rankChart" class="chart-wide"></div>
</div>

<!-- A vs B Comparison -->
<div class="section">
    <h2>策略A vs B PnL对比</h2>
    <div id="abChart" class="chart"></div>
</div>

<!-- WR vs PnL Scatter -->
<div class="section">
    <h2>胜率 vs PnL 散点图 (策略A)</h2>
    <div id="scatterChart" class="chart"></div>
</div>

<!-- Parameter Distribution -->
<div class="section">
    <h2>最优参数分布 (盈利品种)</h2>
    <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;">
        <div id="lbPie" style="height:250px"></div>
        <div id="compPie" style="height:250px"></div>
        <div id="holdPie" style="height:250px"></div>
        <div id="tpPie" style="height:250px"></div>
        <div id="slPie" style="height:250px"></div>
    </div>
</div>

<!-- Data Table -->
<div class="section">
    <h2>详细数据表</h2>
    <div class="tier-legend">
        <span><span style="background:#ff6b35;width:12px;height:12px;border-radius:50%;display:inline-block"></span> S级</span>
        <span><span style="background:#4ecdc4;width:12px;height:12px;border-radius:50%;display:inline-block"></span> A级</span>
        <span><span style="background:#45b7d1;width:12px;height:12px;border-radius:50%;display:inline-block"></span> B级</span>
        <span><span style="background:#96ceb4;width:12px;height:12px;border-radius:50%;display:inline-block"></span> C级</span>
    </div>
    <div style="max-height:600px;overflow-y:auto;">
    <table>
        <thead>
            <tr>
                <th>#</th><th>Tier</th><th>品种</th><th>有效/总天</th>
                <th>A最优PnL</th><th>A-WR</th><th>A-笔数</th><th>A组合盈利率</th>
                <th>B最优PnL</th><th>Winner</th><th>A最优参数</th>
            </tr>
        </thead>
        <tbody>{table_rows}</tbody>
    </table>
    </div>
</div>

<!-- Conclusion -->
<div class="conclusion">
    <h2>核心结论</h2>
    <ul>
        <li><strong>背离是均值回归信号</strong>：{a_wins}/{n}品种({a_wins/n*100:.0f}%)策略A(卖出/均值回归)优于策略B(买入/动量)</li>
        <li><strong>极高鲁棒性</strong>：{a_100pct}个品种在全部216组参数下100%盈利</li>
        <li><strong>当高价腿下跌但Sum上涨时（背离发生）→ 卖出Sum</strong>，高胜率({avg_a_wr:.0f}%)、正期望</li>
        <li><strong>S级品种</strong>最适合实盘部署：{', '.join(p['symbol'] for p in tiers['S'][:5])}</li>
        <li><strong>动量策略(策略B)在绝大多数品种亏损</strong>，确认背离不是趋势延续的信号</li>
    </ul>
</div>

<script>
// Color theme
const GREEN = '#4ecdc4', RED = '#ff6b6b', ORANGE = '#ff6b35', BLUE = '#45b7d1';

// Ranking Bar Chart
var rankChart = echarts.init(document.getElementById('rankChart'));
rankChart.setOption({{
    tooltip: {{ trigger:'axis', axisPointer:{{ type:'shadow' }} }},
    legend: {{ data:['A-PnL','B-PnL','A-WR'], textStyle:{{ color:'#aaa' }} }},
    grid: {{ left:60, right:60, bottom:100 }},
    xAxis: {{ type:'category', data:{json.dumps(rank_symbols)}, axisLabel:{{ rotate:45, color:'#aaa', fontSize:10 }} }},
    yAxis: [
        {{ type:'value', name:'PnL', nameTextStyle:{{ color:'#aaa' }}, axisLabel:{{ color:'#aaa' }} }},
        {{ type:'value', name:'WR%', max:100, nameTextStyle:{{ color:'#aaa' }}, axisLabel:{{ color:'#aaa' }} }}
    ],
    series: [
        {{ name:'A-PnL', type:'bar', data:{json.dumps(rank_a_pnl)}, itemStyle:{{ color:GREEN }} }},
        {{ name:'B-PnL', type:'bar', data:{json.dumps(rank_b_pnl)}, itemStyle:{{ color:RED }} }},
        {{ name:'A-WR', type:'line', yAxisIndex:1, data:{json.dumps(rank_a_wr)}, lineStyle:{{ color:ORANGE }}, itemStyle:{{ color:ORANGE }} }}
    ]
}});

// A vs B Comparison
var abChart = echarts.init(document.getElementById('abChart'));
abChart.setOption({{
    tooltip: {{ trigger:'item', formatter: function(p) {{ return p.data[2] + '<br>A: ' + p.data[0] + '<br>B: ' + p.data[1]; }} }},
    grid: {{ left:60, right:30, bottom:40 }},
    xAxis: {{ type:'value', name:'策略A PnL', nameTextStyle:{{ color:'#aaa' }}, axisLabel:{{ color:'#aaa' }}, splitLine:{{ lineStyle:{{ color:'#2d3436' }} }} }},
    yAxis: {{ type:'value', name:'策略B PnL', nameTextStyle:{{ color:'#aaa' }}, axisLabel:{{ color:'#aaa' }}, splitLine:{{ lineStyle:{{ color:'#2d3436' }} }} }},
    series: [{{
        type:'scatter', symbolSize:12,
        data: {json.dumps([[p['a_pnl'], p['b_pnl'], p['symbol']] for p in products])},
        itemStyle: {{ color: function(p) {{ return p.data[0] > p.data[1] ? GREEN : RED; }} }},
        label: {{ show:true, formatter: function(p) {{ return p.data[2]; }}, position:'right', fontSize:9, color:'#aaa' }}
    }}],
    markLine: {{ data: [{{ type:'average' }}] }}
}});

// WR vs PnL Scatter
var scatterChart = echarts.init(document.getElementById('scatterChart'));
scatterChart.setOption({{
    tooltip: {{ trigger:'item', formatter: function(p) {{ return p.data[2] + '<br>WR: ' + p.data[0] + '%<br>PnL: ' + p.data[1]; }} }},
    grid: {{ left:60, right:30, bottom:40 }},
    xAxis: {{ type:'value', name:'胜率 WR%', min:45, max:90, nameTextStyle:{{ color:'#aaa' }}, axisLabel:{{ color:'#aaa' }}, splitLine:{{ lineStyle:{{ color:'#2d3436' }} }} }},
    yAxis: {{ type:'value', name:'总PnL', nameTextStyle:{{ color:'#aaa' }}, axisLabel:{{ color:'#aaa' }}, splitLine:{{ lineStyle:{{ color:'#2d3436' }} }} }},
    series: [{{
        type:'scatter', symbolSize: function(p) {{ return Math.max(8, Math.min(25, p[3]/100)); }},
        data: {json.dumps([[p['a_wr'], p['a_pnl'], p['symbol'], p['a_trades']] for p in products])},
        itemStyle: {{ color: function(p) {{ return p.data[1] > 3000 ? ORANGE : (p.data[1] > 1000 ? GREEN : BLUE); }} }},
        label: {{ show:true, formatter: function(p) {{ return p.data[2]; }}, position:'right', fontSize:9, color:'#aaa' }}
    }}]
}});

// Parameter Pies
function makePie(el, title, data) {{
    var chart = echarts.init(document.getElementById(el));
    chart.setOption({{
        title: {{ text:title, textStyle:{{ color:'#aaa', fontSize:12 }}, left:'center' }},
        tooltip: {{ trigger:'item' }},
        series: [{{
            type:'pie', radius:['30%','65%'], center:['50%','55%'],
            data: data.map(function(d) {{ return {{ name:String(d[0]), value:d[1] }}; }}),
            label: {{ color:'#aaa', fontSize:10 }},
            itemStyle: {{ borderColor:'#16213e', borderWidth:1 }}
        }}]
    }});
}}
makePie('lbPie', 'Lookback', {json.dumps(sorted(lb_counts.items(), key=lambda x: -x[1]))});
makePie('compPie', 'Comp阈值', {json.dumps(sorted(comp_counts.items(), key=lambda x: -x[1]))});
makePie('holdPie', 'Hold时间', {json.dumps(sorted(hold_counts.items(), key=lambda x: -x[1]))});
makePie('tpPie', 'TP倍数', {json.dumps(sorted(tp_counts.items(), key=lambda x: -x[1]))});
makePie('slPie', 'SL倍数', {json.dumps(sorted(sl_counts.items(), key=lambda x: -x[1]))});

// Responsive
window.addEventListener('resize', function() {{
    rankChart.resize(); abChart.resize(); scatterChart.resize();
}});
</script>

</body>
</html>'''

    with open(OUT_HTML, 'w') as f:
        f.write(html)
    print(f"报告已生成: {OUT_HTML}")
    print(f"品种: {n}, A胜: {a_wins}, B胜: {b_wins}")
    print(f"S级: {len(tiers['S'])}, A级: {len(tiers['A'])}, B级: {len(tiers['B'])}, C级: {len(tiers['C'])}")


if __name__ == '__main__':
    products = load_all_results()
    if products:
        generate_html(products)
    else:
        print("无结果文件")
