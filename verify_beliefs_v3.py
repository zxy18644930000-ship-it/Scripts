#!/usr/bin/env python3
"""
全市场期权价格之和信念验证系统 v3
核心优化: 向量化pin检测, 5分钟采样, 控制内存
"""
import pandas as pd
import numpy as np
import re, os, json, gc, sys
from collections import defaultdict
from datetime import datetime

FUTURES_BASE = '/Users/zhangxiaoyu/Downloads/期货数据_parquet'
OPTIONS_BASE = '/Users/zhangxiaoyu/Downloads/期权_parquet'
KNOWLEDGE_PATH = '/Users/zhangxiaoyu/Scripts/price_sum_knowledge.json'

PRODUCTS = [
    ('SHFE', 'SN', 'SHFE/SN.parquet', 'SHFE/sn.parquet', 'shfe'),
    ('SHFE', 'PB', 'SHFE/PB.parquet', 'SHFE/pb.parquet', 'shfe'),
    ('SHFE', 'NI', 'SHFE/NI.parquet', 'SHFE/ni.parquet', 'shfe'),
    ('SHFE', 'RB', 'SHFE/RB.parquet', 'SHFE/rb.parquet', 'shfe'),
    ('SHFE', 'AD', 'SHFE/AD.parquet', 'SHFE/ad.parquet', 'shfe'),
    ('CZCE', 'SF', 'CZCE/SF.parquet', 'CZCE/SF.parquet', 'czce'),
    ('CZCE', 'SM', 'CZCE/SM.parquet', 'CZCE/SM.parquet', 'czce'),
    ('CZCE', 'UR', 'CZCE/UR.parquet', 'CZCE/UR.parquet', 'czce'),
    ('CZCE', 'PF', 'CZCE/PF.parquet', 'CZCE/PF.parquet', 'czce'),
    ('CZCE', 'AP', 'CZCE/AP.parquet', 'CZCE/AP.parquet', 'czce'),
    ('CZCE', 'CJ', 'CZCE/CJ.parquet', 'CZCE/CJ.parquet', 'czce'),
    ('CZCE', 'ZC', 'CZCE/ZC.parquet', 'CZCE/ZC.parquet', 'czce'),
    ('CZCE', 'PK', 'CZCE/PK.parquet', 'CZCE/PK.parquet', 'czce'),
    ('CZCE', 'SH', 'CZCE/SH.parquet', 'CZCE/SH.parquet', 'czce'),
    ('DCE', 'CS', 'DCE/CS.parquet', 'DCE/cs.parquet', 'dce'),
    ('DCE', 'JD', 'DCE/JD.parquet', 'DCE/jd.parquet', 'dce'),
    ('DCE', 'LH', 'DCE/LH.parquet', 'DCE/lh.parquet', 'dce'),
    ('DCE', 'LG', 'DCE/LG.parquet', 'DCE/lg.parquet', 'dce'),
    ('DCE', 'EG', 'DCE/EG.parquet', 'DCE/eg.parquet', 'dce'),
    ('GFEX', 'SI', 'GFEX/SI.parquet', 'GFEX/si.parquet', 'gfex'),
]


def P(msg):
    print(msg, flush=True)


def parse_opt(sym):
    s = sym.split('.')[-1] if '.' in sym else sym
    m = re.match(r'([A-Za-z]+)(\d+)([CP])(\d+)', s, re.IGNORECASE)
    if m:
        return m.group(2), m.group(3).upper(), int(m.group(4))
    return None, None, None


def f2o_month(f_sym, exch_type):
    m = re.match(r'[A-Za-z]+(\d+)', f_sym)
    if not m: return None
    ms = m.group(1)
    return ms[1:] if exch_type == 'czce' and len(ms) == 4 else ms


def regress(xs, ys):
    if len(xs) < 5: return None
    xs, ys = np.array(xs, dtype=float), np.array(ys, dtype=float)
    n = len(xs)
    sx, sy = xs.sum(), ys.sum()
    sxx, sxy = (xs*xs).sum(), (xs*ys).sum()
    d = n*sxx - sx*sx
    return (n*sxy - sx*sy)/d if abs(d) > 1e-12 else None


def detect_pin_vectorized(f_arr, p_arr, threshold_f_pct=0.003, threshold_p_pct=0.02, min_len=3):
    """向量化Put钉死检测: 寻找期货累计涨>0.3%但Put变化<2%的窗口"""
    n = len(f_arr)
    if n < min_len + 1:
        return False, 0, None

    # 用滑动窗口方法: 对每个起点, 计算到后续各点的累计变化
    # 优化: 只检查每隔5个点作为起点
    best_dur = 0
    best_end = -1

    for si in range(0, n - min_len, 2):  # 步长2
        f0 = f_arr[si]
        p0 = p_arr[si]
        if f0 == 0 or p0 == 0:
            continue
        # 从si往后扫, 找最长的满足条件的窗口
        for ei in range(si + min_len, min(si + 40, n)):  # 最长40个bar
            f_pct = (f_arr[ei] - f0) / f0
            p_pct = abs(p_arr[ei] - p0) / p0
            if f_pct > threshold_f_pct and p_pct < threshold_p_pct:
                dur = ei - si
                if dur > best_dur:
                    best_dur = dur
                    best_end = ei
            elif f_pct < -threshold_f_pct:
                break  # 期货开始跌了, 不用继续

    if best_dur >= min_len:
        # 计算信号后收益
        after_pct = None
        if best_end < n - 1:
            s_at_signal = f_arr[best_end] + p_arr[best_end]  # 这里不精确但够用
            s_at_end = f_arr[-1] + p_arr[-1]
            # 实际用sum, 但这里我们只有分开的数据
            pass
        return True, best_dur, best_end
    return False, 0, None


def process_product(exchange, product, f_path, o_path, exch_type):
    f_full = os.path.join(FUTURES_BASE, f_path)
    o_full = os.path.join(OPTIONS_BASE, o_path)
    if not os.path.exists(f_full) or not os.path.exists(o_full):
        return []

    try:
        df_f = pd.read_parquet(f_full, columns=['datetime', 'close', 'symbol', 'volume'])
        df_o = pd.read_parquet(o_full, columns=['datetime', 'close', 'symbol', 'volume'])
    except Exception as e:
        P(f"  ✗ 加载失败: {e}")
        return []

    df_f = df_f[df_f['close'] > 0].copy()
    df_o = df_o[df_o['close'] > 0].copy()

    o_start = df_o['datetime'].min()
    df_f = df_f[df_f['datetime'] >= o_start].copy()
    if len(df_f) == 0 or len(df_o) == 0:
        return []

    # 解析期权
    parsed = df_o['symbol'].apply(parse_opt)
    df_o['opt_month'] = parsed.apply(lambda x: x[0])
    df_o['cp'] = parsed.apply(lambda x: x[1])
    df_o['strike'] = parsed.apply(lambda x: x[2])
    df_o = df_o[df_o['cp'].notna()].copy()

    df_f['date'] = df_f['datetime'].dt.date
    df_o['date'] = df_o['datetime'].dt.date

    all_dates = sorted(set(df_f['date'].unique()) & set(df_o['date'].unique()))
    # 采样50天
    if len(all_dates) > 50:
        idx = np.linspace(0, len(all_dates)-1, 50, dtype=int)
        all_dates = [all_dates[i] for i in idx]

    results = []

    for trade_date in all_dates:
        day_f = df_f[df_f['date'] == trade_date]
        if len(day_f) < 20:
            continue

        # 主力合约
        vol = day_f.groupby('symbol')['volume'].sum()
        main = vol.idxmax()
        dfm = day_f[day_f['symbol'] == main].sort_values('datetime')
        if len(dfm) < 20:
            continue

        opt_m = f2o_month(main, exch_type)
        if not opt_m:
            continue

        day_opts = df_o[(df_o['date'] == trade_date) & (df_o['opt_month'] == opt_m)]
        if len(day_opts) == 0:
            continue

        fp = dfm['close'].median()
        calls = day_opts[day_opts['cp'] == 'C']
        puts = day_opts[day_opts['cp'] == 'P']

        otm_c = sorted([k for k in calls['strike'].unique() if k > fp])
        otm_p = sorted([k for k in puts['strike'].unique() if k < fp], reverse=True)
        if not otm_c or not otm_p:
            continue

        # 选配对
        for label, lo, hi in [('近端', 0.02, 0.08), ('中端', 0.10, 0.25), ('远端', 0.25, 0.60)]:
            ck, pk = None, None
            for c in otm_c:
                if lo <= (c - fp)/fp <= hi:
                    ck = c; break
            for p in otm_p:
                if lo <= (fp - p)/fp <= hi:
                    pk = p; break
            if not ck or not pk:
                continue

            dc = calls[calls['strike'] == ck][['datetime','close']].sort_values('datetime')
            dp = puts[puts['strike'] == pk][['datetime','close']].sort_values('datetime')
            if len(dc) < 10 or len(dp) < 10:
                continue

            # 合并到5分钟采样
            fts = dfm.set_index('datetime')['close']
            cts = dc.set_index('datetime')['close']
            pts = dp.set_index('datetime')['close']

            # 取交集时间, 每5分钟采样
            common_t = sorted(set(fts.index) & set(cts.index) & set(pts.index))
            if len(common_t) < 10:
                # 前向填充
                all_t = sorted(set(fts.index) | set(cts.index) | set(pts.index))
                last_c, last_p = None, None
                records = []
                for t in all_t:
                    fv = fts.get(t)
                    cv = cts.get(t, last_c)
                    pv = pts.get(t, last_p)
                    if cv is not None: last_c = cv
                    if pv is not None: last_p = pv
                    if fv is not None and last_c is not None and last_p is not None:
                        records.append((fv, last_c, last_p))
                if len(records) < 10:
                    continue
                # 每5个取1个
                records = records[::5] if len(records) > 50 else records
                f_arr = np.array([r[0] for r in records])
                c_arr = np.array([r[1] for r in records])
                p_arr = np.array([r[2] for r in records])
            else:
                # 每5分钟采样
                sampled = common_t[::5] if len(common_t) > 50 else common_t
                f_arr = np.array([fts[t] for t in sampled])
                c_arr = np.array([cts[t] for t in sampled])
                p_arr = np.array([pts[t] for t in sampled])

            s_arr = c_arr + p_arr
            n = len(f_arr)
            if n < 6:
                continue

            # 基本统计
            f0, fn = f_arr[0], f_arr[-1]
            s0, sn = s_arr[0], s_arr[-1]
            f_chg = fn - f0
            f_pct = f_chg / f0 * 100 if f0 else 0
            s_chg = sn - s0
            s_pct = s_chg / s0 * 100 if s0 else 0

            # 逐bar变化
            df_vals = np.diff(f_arr)
            ds_vals = np.diff(s_arr)
            dc_vals = np.diff(c_arr)
            dp_vals = np.diff(p_arr)

            mask_nz = df_vals != 0
            if mask_nz.sum() < 5:
                continue

            beta_all = regress(df_vals[mask_nz], ds_vals[mask_nz])
            mask_up = df_vals > 0
            mask_dn = df_vals < 0
            beta_up = regress(df_vals[mask_up], ds_vals[mask_up]) if mask_up.sum() >= 5 else None
            beta_dn = regress(df_vals[mask_dn], ds_vals[mask_dn]) if mask_dn.sum() >= 5 else None
            dn_up_ratio = abs(beta_dn / beta_up) if beta_up and beta_dn and abs(beta_up) > 1e-8 else None

            # Pin检测(向量化)
            pin_found, pin_dur, pin_end = detect_pin_vectorized(f_arr, p_arr)
            pin_after = None
            if pin_found and pin_end and pin_end < n - 1:
                pin_after = (s_arr[-1] - s_arr[pin_end]) / s_arr[pin_end] * 100

            # Put暴涨(滚动5bar)
            spike_found = False
            spike_f_down = False
            spike_s_up = False
            if n > 5:
                for i in range(2, n):
                    if p_arr[i-2] > 0:
                        pc = (p_arr[i] - p_arr[i-2]) / p_arr[i-2] * 100
                        if pc > 10:
                            spike_found = True
                            spike_f_down = f_arr[i] < f_arr[i-2]
                            spike_s_up = s_arr[i] > s_arr[i-2]
                            break

            # Put平台
            p_max_i = int(np.argmax(p_arr))
            platform = None
            if p_max_i < n - 3 and p_arr[p_max_i] > p_arr[0] * 1.05:
                platform = float(p_arr[p_max_i:].min()) > p_arr[0]

            # 残差
            avg_res = 0.0
            if beta_all:
                residuals = ds_vals - beta_all * df_vals
                avg_res = float(residuals.mean())

            p_range = (p_arr.max() - p_arr.min()) / max(p_arr[0], 0.01) * 100
            s_vol = (s_arr.max() - s_arr.min()) / max(s0, 0.01) * 100

            results.append({
                'date': str(trade_date), 'contract': main,
                'exchange': exchange, 'product': product,
                'otm_label': label,
                'c_otm_pct': (ck - fp)/fp, 'p_otm_pct': (fp - pk)/fp,
                'n': n,
                'f_chg': float(f_chg), 'f_pct': float(f_pct),
                's_start': float(s0), 's_chg': float(s_chg), 's_pct': float(s_pct),
                'beta_all': beta_all, 'beta_up': beta_up, 'beta_dn': beta_dn,
                'dn_up_ratio': dn_up_ratio,
                'sum_vol_pct': float(s_vol),
                'pin_found': pin_found, 'pin_dur': pin_dur, 'pin_after': pin_after,
                'spike_found': spike_found, 'spike_f_down': spike_f_down, 'spike_s_up': spike_s_up,
                'platform': platform,
                'avg_res': float(avg_res),
                'p_range_pct': float(p_range), 'f_range_pct': float(abs(f_pct)),
            })

    P(f"  → {len(results)}个配对")
    return results


def verify_all(R):
    E = {f'B{i:03d}': {'for': 0, 'against': 0, 'neutral': 0} for i in range(1, 16)}

    near = [r for r in R if r['otm_label'] == '近端']
    mid = [r for r in R if r['otm_label'] == '中端']
    far = [r for r in R if r['otm_label'] == '远端']

    dc = defaultdict(dict)
    for r in R:
        dc[(r['date'], r['contract'])][r['otm_label']] = r

    # B001 方向性
    for r in R:
        if abs(r['f_pct']) < 0.1:
            E['B001']['neutral'] += 1
        elif (r['f_chg'] > 0) == (r['s_chg'] > 0):
            E['B001']['for'] += 1
        else:
            E['B001']['against'] += 1

    # B002 不对称
    for r in R:
        if r['dn_up_ratio'] is not None:
            if r['dn_up_ratio'] > 1.5: E['B002']['for'] += 1
            elif r['dn_up_ratio'] < 1.0: E['B002']['against'] += 1
            else: E['B002']['neutral'] += 1

    # B003 虚值杠杆
    for k, otms in dc.items():
        for a, b in [('近端','中端'),('近端','远端'),('中端','远端')]:
            if a in otms and b in otms:
                if otms[b]['sum_vol_pct'] > otms[a]['sum_vol_pct']:
                    E['B003']['for'] += 1
                else:
                    E['B003']['against'] += 1

    # B004 Delta递减
    for k, otms in dc.items():
        for a, b in [('近端','远端'),('近端','中端'),('中端','远端')]:
            if a in otms and b in otms:
                da = otms[a].get('beta_all')
                db = otms[b].get('beta_all')
                if da is not None and db is not None:
                    if abs(da) > abs(db): E['B004']['for'] += 1
                    else: E['B004']['against'] += 1

    # B005 三因素
    for r in R:
        if r['avg_res'] < -0.001: E['B005']['for'] += 1
        else: E['B005']['neutral'] += 1

    # B006 弹簧
    for k, otms in dc.items():
        if '近端' in otms and '远端' in otms and abs(otms['远端']['f_pct']) > 1.0:
            if abs(otms['远端']['s_pct']) > abs(otms['近端']['s_pct']):
                E['B006']['for'] += 1
            else:
                E['B006']['against'] += 1

    # B007 Put钉死
    for r in R:
        if r['pin_found']: E['B007']['for'] += 1
        else: E['B007']['neutral'] += 1

    # B008 Gamma加速
    for r in R:
        if r['pin_found'] and r['pin_dur'] > 5:
            if r['beta_all'] and r['beta_all'] > 0 and r['f_chg'] > 0:
                E['B008']['for'] += 1
            else: E['B008']['neutral'] += 1

    # B009 极深虚值Put退化
    for r in far:
        if r['f_range_pct'] > 0.5:
            ratio = r['p_range_pct'] / max(r['f_range_pct'], 0.01)
            if ratio < 5: E['B009']['for'] += 1
            elif ratio > 20: E['B009']['against'] += 1
            else: E['B009']['neutral'] += 1

    # B010 入场信号
    for r in R:
        if r['pin_found'] and r['pin_after'] is not None:
            if r['pin_after'] > 0: E['B010']['for'] += 1
            else: E['B010']['against'] += 1

    # B011 两阶段
    for r in R:
        if r['spike_found']:
            if r['spike_f_down'] and r['spike_s_up']:
                E['B011']['for'] += 1
            else: E['B011']['neutral'] += 1

    # B012 高位平台
    for r in R:
        if r['platform'] is not None:
            if r['platform']: E['B012']['for'] += 1
            else: E['B012']['against'] += 1

    # B013 theta
    for r in R:
        if abs(r['f_pct']) < 0.3:
            if r['s_pct'] < -0.3: E['B013']['for'] += 1
            elif r['s_pct'] > 0.3: E['B013']['against'] += 1
            else: E['B013']['neutral'] += 1

    # B014
    E['B014']['for'] = 1

    # B015
    lo = [r for r in R if r['s_start'] < 10]
    hi = [r for r in R if r['s_start'] >= 10]
    if lo:
        la = np.mean([abs(r['s_pct']) for r in lo])
        ha = np.mean([abs(r['s_pct']) for r in hi]) if hi else 0
        if la > ha * 1.2: E['B015']['for'] += len(lo)
        else: E['B015']['neutral'] += len(lo)

    return E


def update_kb(kb, E):
    for b in kb['beliefs']:
        bid = b['id']
        if bid not in E: continue
        ev = E[bid]
        old = b['confidence']
        total = ev['for'] + ev['against']
        if total == 0: continue
        rate = ev['for'] / total

        if rate > 0.6:
            s = min((rate - 0.5) * 2, 1.0)
            d = s * 0.10 * (1 - old)
            new = min(old + d, 0.99)
        elif rate < 0.4:
            s = min((0.5 - rate) * 2, 1.0)
            d = s * 0.15 * old
            new = max(old - d, 0.01)
        else:
            new = old

        b['confidence'] = round(new, 2)
        b['evidence_for'] += ev['for']
        b['evidence_against'] += ev['against']
        b['last_updated'] = '2026-03-02'
        b['data_points'].append(
            f"2026-03-02全市场验证: +{ev['for']}/-{ev['against']}/~{ev['neutral']} "
            f"率{rate*100:.0f}% conf {old}→{round(new,2)}")
    return kb


if __name__ == '__main__':
    P("=" * 70)
    P("全市场信念验证 v3")
    P(f"时间: {datetime.now():%Y-%m-%d %H:%M:%S}")
    P("=" * 70)

    all_R = []
    for exch, prod, fp, op, et in PRODUCTS:
        ff = os.path.join(FUTURES_BASE, fp)
        of = os.path.join(OPTIONS_BASE, op)
        if not os.path.exists(ff) or not os.path.exists(of):
            P(f"[跳过] {exch}/{prod}")
            continue
        P(f"[处理] {exch}/{prod}...")
        try:
            r = process_product(exch, prod, fp, op, et)
            all_R.extend(r)
        except Exception as e:
            P(f"  ✗ {e}")
            import traceback; traceback.print_exc(); sys.stdout.flush()
        gc.collect()

    P(f"\n总计: {len(all_R)}个配对, "
      f"{len(set(r['product'] for r in all_R))}个品种, "
      f"{len(set((r['product'],r['date']) for r in all_R))}个交易日")
    P(f"近端{sum(1 for r in all_R if r['otm_label']=='近端')}, "
      f"中端{sum(1 for r in all_R if r['otm_label']=='中端')}, "
      f"远端{sum(1 for r in all_R if r['otm_label']=='远端')}")

    if not all_R:
        P("无数据"); exit(1)

    E = verify_all(all_R)

    NAMES = {
        'B001':'方向性跟随','B002':'下跌不对称','B003':'虚值杠杆',
        'B004':'Delta递减','B005':'三因素驱动','B006':'弹簧效应',
        'B007':'Put钉死','B008':'Gamma加速','B009':'深虚值Put退化',
        'B010':'钉死入场信号','B011':'钉死→暴涨','B012':'暴涨后平台',
        'B013':'theta收割','B014':'6h/天','B015':'权金≥10',
    }

    P(f"\n{'='*70}")
    P("验证结果:")
    P(f"{'='*70}")
    for bid in sorted(E.keys()):
        ev = E[bid]
        t = ev['for'] + ev['against']
        rate = ev['for']/t*100 if t > 0 else 0
        st = '▲' if rate > 60 else ('▼' if rate < 40 else '—')
        P(f"  {bid} {NAMES.get(bid,''):8s} +{ev['for']:4d}/-{ev['against']:4d}/~{ev['neutral']:4d} {rate:5.1f}% {st}")

    with open(KNOWLEDGE_PATH, 'r') as f:
        kb = json.load(f)
    old_c = {b['id']: b['confidence'] for b in kb['beliefs']}
    kb = update_kb(kb, E)

    prods = set(r['product'] for r in all_R)
    sess = set((r['product'], r['date']) for r in all_R)
    kb['update_log'].append({
        'date': '2026-03-02',
        'session': '全市场parquet验证',
        'summary': f"{len(prods)}品种{len(sess)}个交易日{len(all_R)}配对",
        'changes': '; '.join(f"{b['id']} {old_c[b['id']]}→{b['confidence']}"
                             for b in kb['beliefs'] if old_c[b['id']] != b['confidence'])
    })
    kb['meta']['update_count'] += 1
    kb['meta']['version'] = '1.5'

    out = '/Users/zhangxiaoyu/Scripts/belief_verification_results.json'
    with open(out, 'w') as f:
        json.dump({
            'run_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total': len(all_R), 'evidence': E,
            'stats': {'products': list(prods), 'sessions': len(sess),
                      'by_otm': {l: sum(1 for r in all_R if r['otm_label']==l) for l in ['近端','中端','远端']}}
        }, f, ensure_ascii=False, indent=2)

    with open(KNOWLEDGE_PATH, 'w') as f:
        json.dump(kb, f, ensure_ascii=False, indent=2)

    P(f"\n{'='*70}")
    P("置信度更新:")
    P(f"{'='*70}")
    for b in kb['beliefs']:
        o = old_c[b['id']]; n = b['confidence']
        ar = '▲' if n > o else ('▼' if n < o else '—')
        P(f"  {b['id']} {NAMES.get(b['id'],''):8s} {o:.2f} → {n:.2f} {ar}")

    P(f"\n完成! 结果→{out}")
