#!/usr/bin/env python3
"""بوت إشارات تليجرام - يفحص شموع Binance المقفولة ويبعت تنبيهات حسب طريقة التحليل في config.json
تشغيل:  python bot.py            (عادي)     python bot.py --dry   (يطبع بدل ما يبعت)     python bot.py --test  (رسالة تجربة)
"""
import html, json, os, re, sys, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from itertools import combinations
from zoneinfo import ZoneInfo
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CFG = {
    'top_n': 100, 'extra_coins': [], 'exclude': [], 'coins': ['BTCUSDT', 'ETHUSDT'], 'workers': 8, 'max_messages_per_run': 40,
    'htf_rule': 'ema200', 'htf_mode': 'tag', 'htf_exempt': ['div_bull', 'div_bear'],
    'signals_off_tf': {'15m': ['patterns'], '30m': ['patterns']}, 'timeframes': ['15m', '30m', '1h', '4h', '1d'],
    'lookback': {'15m': 3, '30m': 2, '1h': 2, '4h': 2, '1d': 2}, 'tz': 'Africa/Cairo',
    'pivot_window': 5, 'div_min_gap': 5, 'div_max_gap': 60,
    'ma200_confirm_closes': 2,
    'ma_type': 'EMA', 'adx_min': 20, 'breakout_confirm': 'tag', 'breakout_vol_ratio': 1.2, 'candles': 600, 'confluence': {'enabled': True, 'min_families': 2, 'exempt': []}, 'vol_spike_ratio': 3.0, 'digest': {'enabled': True, 'over': 10, 'top_cards': 8},
    'htf_filter': {'15m': '30m', '30m': '1h', '1h': '4h', '4h': '1d'},
    'macd_min_strength': 0.5, 'macd_strong': 1.0,
    'alert_rsi_ma_cross': False,
    'pat_min_range_atr': 0.8, 'pat_min_vol_ratio': 1.0,
    'flag_pole_atr': 4.0, 'flag_pole_len': 10,
    'trend_tol_atr': 0.3, 'trend_min_touches': 4, 'trend_min_span': 50,
    'cooldown_candles': 8, 'min_confluence': {'15m': 2, '30m': 2, '1h': 1, '4h': 1, '1d': 1},
    'trend_confirm_closes': 2, 'trend_buffer_atr': 0.1,
    'signals': {k: True for k in ['golden_death', 'ema_9_21', 'ma200', 'macd', 'divergence', 'patterns', 'flags', 'trendlines', 'bollinger', 'obv', 'sr_breaks']},
    'market_context': True, 'context_tfs': ['1h', '4h', '1d'],
}
_p = os.path.join(HERE, 'config.json')
if os.path.exists(_p):
    _u = json.load(open(_p, encoding='utf-8'))
    for k, v in _u.items():
        if isinstance(v, dict) and isinstance(CFG.get(k), dict): CFG[k].update(v)
        else: CFG[k] = v
STATE_PATH = os.path.join(HERE, 'state.json')
MT = CFG['ma_type'].upper()   # EMA أو SMA (للتقاطع الذهبي و200)
HTF = {}
HOSTS = ['https://data-api.binance.vision', 'https://api.binance.com']  # الأولى شغالة من سيرفرات GitHub
TF_MS = {'15m': 900000, '30m': 1800000, '1h': 3600000, '4h': 14400000, '1d': 86400000, '1w': 604800000}
TF_AR = {'15m': '15 دقيقة', '30m': '30 دقيقة', '1h': 'ساعة', '4h': '4 ساعات', '1d': 'يوم', '1w': 'أسبوع'}

# ---------------- بيانات ----------------
def klines(sym, tf, limit=None, min_len=250):
    limit = limit or CFG['candles']
    for h in HOSTS:
        try:
            r = requests.get(f'{h}/api/v3/klines', params={'symbol': sym, 'interval': tf, 'limit': limit}, timeout=20)
            if r.status_code != 200: continue
            now = int(time.time() * 1000)
            d = [k for k in r.json() if k[6] < now]  # الشموع المقفولة بس
            if len(d) >= min_len:
                return {'t': [k[0] for k in d], 'o': [float(k[1]) for k in d], 'h': [float(k[2]) for k in d],
                        'l': [float(k[3]) for k in d], 'c': [float(k[4]) for k in d], 'v': [float(k[5]) for k in d]}
        except Exception as e:
            print('fetch error', sym, tf, e)
    return None

# ---------------- مؤشرات ----------------
def sma(a, n):
    return [sum(a[i-n+1:i+1]) / n if i >= n-1 and None not in a[i-n+1:i+1] else None for i in range(len(a))]
def ema(a, n):
    out, k, e, buf = [None]*len(a), 2/(n+1), None, []
    for i, v in enumerate(a):
        if v is None: continue
        if e is None:
            buf.append(v)
            if len(buf) == n: e = sum(buf)/n; out[i] = e
        else:
            e = v*k + e*(1-k); out[i] = e
    return out
def rsi(c, n=14):
    out = [None]*len(c); g = l = 0.0
    for i in range(1, n+1):
        d = c[i]-c[i-1]; g += max(d, 0); l += max(-d, 0)
    g /= n; l /= n; out[n] = 100.0 if l == 0 else 100-100/(1+g/l)
    for i in range(n+1, len(c)):
        d = c[i]-c[i-1]; g = (g*(n-1)+max(d, 0))/n; l = (l*(n-1)+max(-d, 0))/n
        out[i] = 100.0 if l == 0 else 100-100/(1+g/l)
    return out
def atr(h, l, c, n=14):
    tr = [h[0]-l[0]] + [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(1, len(c))]
    out = [None]*len(c); a = sum(tr[:n])/n; out[n-1] = a
    for i in range(n, len(c)): a = (a*(n-1)+tr[i])/n; out[i] = a
    return out
def adx_di(h, l, c, n=14):
    L = len(c); out = [None]*L; PD, MD = [None]*L, [None]*L
    if L < 2*n+1: return out, PD, MD
    tr, pd, md = [0.0]*L, [0.0]*L, [0.0]*L
    for i in range(1, L):
        up, dn = h[i]-h[i-1], l[i-1]-l[i]
        pd[i] = up if up > dn and up > 0 else 0.0; md[i] = dn if dn > up and dn > 0 else 0.0
        tr[i] = max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1]))
    t, p, m = sum(tr[1:n+1]), sum(pd[1:n+1]), sum(md[1:n+1]); dx = [0.0]*L
    for i in range(n, L):
        if i > n: t = t-t/n+tr[i]; p = p-p/n+pd[i]; m = m-m/n+md[i]
        pdi, mdi = (100*p/t, 100*m/t) if t else (0, 0); PD[i], MD[i] = pdi, mdi
        dx[i] = 100*abs(pdi-mdi)/(pdi+mdi) if pdi+mdi else 0.0
    a = sum(dx[n:2*n])/n; out[2*n-1] = a
    for i in range(2*n, L): a = (a*(n-1)+dx[i])/n; out[i] = a
    return out, PD, MD
def adx(h, l, c, n=14): return adx_di(h, l, c, n)[0]
def pivots(h, l, w):
    ph, pl = [], []
    for i in range(w, len(h)-w):
        if all(h[j] < h[i] for j in range(i-w, i)) and all(h[j] <= h[i] for j in range(i+1, i+w+1)): ph.append(i)
        if all(l[j] > l[i] for j in range(i-w, i)) and all(l[j] >= l[i] for j in range(i+1, i+w+1)): pl.append(i)
    return ph, pl
def prep(x):
    c = x['c']
    x['e9'], x['e21'] = ema(c, 9), ema(c, 21)
    mf = ema if MT == 'EMA' else sma
    x['m50'], x['m200'] = mf(c, 50), mf(c, 200)
    x['adx'], x['pdi'], x['mdi'] = adx_di(x['h'], x['l'], c)
    x['rsi'] = rsi(c); x['rsima'] = sma(x['rsi'], 14)
    e12, e26 = ema(c, 12), ema(c, 26)
    x['macd'] = [a-b if a is not None and b is not None else None for a, b in zip(e12, e26)]
    x['sig'] = ema(x['macd'], 9)
    x['atr'] = atr(x['h'], x['l'], c)
    x['ph'], x['pl'] = pivots(x['h'], x['l'], CFG['pivot_window'])
    n20 = sma(c, 20); x['bbU'], x['bbL'], x['bbw'] = [], [], []
    for i, m in enumerate(n20):
        if m is None: x['bbU'].append(None); x['bbL'].append(None); x['bbw'].append(None); continue
        sd = (sum((v-m)**2 for v in c[i-19:i+1])/20)**.5
        x['bbU'].append(m+2*sd); x['bbL'].append(m-2*sd); x['bbw'].append(4*sd/m*100)
    ob = [0.0]
    for i in range(1, len(c)): ob.append(ob[-1] + (x['v'][i] if c[i] > c[i-1] else -x['v'][i] if c[i] < c[i-1] else 0))
    x['obv'] = ob; x['vma'] = sma(x['v'], 20)
    return x
def light(x):
    c = x['c']; mf = ema if MT == 'EMA' else sma
    x['e9'], x['e21'], x['m50'], x['m200'] = ema(c, 9), ema(c, 21), mf(c, 50), mf(c, 200)
    return x
def trend_dir(x, j):
    """اتجاه الفريم الأكبر عند الشمعة j: 1 صاعد / -1 هابط / 0 متضارب / None مفيش بيانات"""
    c = x['c'][j]; m = x['m200'][j] if x['m200'][j] is not None else x['m50'][j]   # لو الشموع أقل من 200 بنستخدم 50
    a = None if m is None else (1 if c > m else -1)
    e9, e21 = x['e9'][j], x['e21'][j]; b = None if e9 is None or e21 is None else (1 if e9 > e21 else -1)
    rule = CFG['htf_rule']
    if rule == 'ema200': return a
    if rule == 'ema9_21': return b
    if a is None or b is None: return a if b is None else b
    return a if a == b else 0
def htf_ok(tr, sid, d):
    return tr is None or d == 'neutral' or sid in CFG['htf_exempt'] or (tr != 0 and (d == 'bull') == (tr > 0))
def choppy(x, i): return CFG['adx_min'] > 0 and x['adx'][i] is not None and x['adx'][i] < CFG['adx_min']
def cu(a, b, i): return None not in (a[i], b[i], a[i-1], b[i-1]) and a[i-1] <= b[i-1] and a[i] > b[i]
def cd(a, b, i): return None not in (a[i], b[i], a[i-1], b[i-1]) and a[i-1] >= b[i-1] and a[i] < b[i]

# ---------------- كاشفات الإشارات ----------------
def sig_ma(x, i):
    o = []
    if cu(x['m50'], x['m200'], i): o.append(('golden', 'bull', f'تقاطع ذهبي||{MT} 50 اخترق {MT} 200 لأعلى'))
    if cd(x['m50'], x['m200'], i): o.append(('death', 'bear', f'تقاطع الموت||{MT} 50 كسر {MT} 200 لأسفل'))
    return o
def sig_ema(x, i):
    o = []
    if choppy(x, i): return o
    if cu(x['e9'], x['e21'], i): o.append(('e921u', 'bull', 'تقاطع EMA صاعد||EMA 9 اخترق EMA 21 لأعلى'))
    if cd(x['e9'], x['e21'], i): o.append(('e921d', 'bear', 'تقاطع EMA هابط||EMA 9 كسر EMA 21 لأسفل'))
    return o
def sig_200(x, i):
    n, c, s = CFG['ma200_confirm_closes'], x['c'], x['m200']
    if i-n < 0 or None in s[i-n:i+1]: return []
    if all(c[i-k] > s[i-k] for k in range(n)) and c[i-n] <= s[i-n]:
        return [('c200u', 'bull', f'إغلاقات فوق {MT} 200||{n} إغلاقات متتالية فوقه بعد ما كان تحته')]
    if all(c[i-k] < s[i-k] for k in range(n)) and c[i-n] >= s[i-n]:
        return [('c200d', 'bear', f'إغلاقات تحت {MT} 200||{n} إغلاقات متتالية تحته بعد ما كان فوقه')]
    return []
def sig_macd(x, i):
    m, s = x['macd'], x['sig']
    if choppy(x, i): return []
    if None in (m[i], s[i], m[i-1], s[i-1]): return []
    win = [abs(v) for v in m[max(0, i-100):i+1] if v is not None]
    st = abs(m[i]) / (sum(win)/len(win)) if win and sum(win) > 0 else 0
    if st < CFG['macd_min_strength']: return []
    tag = 'قوي (بعيد عن الصفر)' if st >= CFG['macd_strong'] else 'متوسط'
    if cu(m, s, i) and m[i] < 0: return [('macd_up', 'bull', f'MACD تحت الصفر: إشارة صعود||قطع خط الإشارة لأعلى · القوة: {tag}')]
    if cd(m, s, i) and m[i] > 0: return [('macd_dn', 'bear', f'MACD فوق الصفر: إشارة نزول||قطع خط الإشارة لأسفل · القوة: {tag}')]
    return []
def sig_rsima(x, i):
    if not CFG['alert_rsi_ma_cross'] or choppy(x, i): return []
    if cu(x['rsi'], x['rsima'], i): return [('rsima_u', 'bull', 'RSI يخترق متوسطه||RSI اخترق متوسطه 14 لأعلى')]
    if cd(x['rsi'], x['rsima'], i): return [('rsima_d', 'bear', 'RSI يكسر متوسطه||RSI كسر متوسطه 14 لأسفل')]
    return []
def sig_div(x, i):
    w, r, o = CFG['pivot_window'], x['rsi'], []
    b = i-w
    if b < 0: return o
    for lst, key, kind in ((x['pl'], 'l', 'bull'), (x['ph'], 'h', 'bear')):
        if b not in lst: continue
        prev = [p for p in lst if p < b and CFG['div_min_gap'] <= b-p <= CFG['div_max_gap']]
        if not prev: continue
        a = prev[-1]; y = x[key]
        if r[a] is None or r[b] is None: continue
        if kind == 'bull' and y[b] < y[a] and r[b] > r[a]+1 and r[a] <= 50:
            o.append(('div_bull', 'bull', f'دايفيرجن إيجابي (RSI)||السعر عمل قاع أدنى وRSI عمل قاع أعلى: {r[a]:.0f} ← {r[b]:.0f} · بيتأكد بعد {w} شموع'))
        if kind == 'bear' and y[b] > y[a] and r[b] < r[a]-1 and r[a] >= 50:
            o.append(('div_bear', 'bear', f'دايفيرجن سلبي (RSI)||السعر عمل قمة أعلى وRSI عمل قمة أدنى: {r[a]:.0f} ← {r[b]:.0f} · بيتأكد بعد {w} شموع'))
    return o
def sig_pat(x, i):
    o_, h, l, c, v, a = x['o'], x['h'], x['l'], x['c'], x['v'], x['atr']
    if i < 25 or a[i] is None: return []
    rg = lambda j: h[j]-l[j]; bd = lambda j: abs(c[j]-o_[j])
    if rg(i) < CFG['pat_min_range_atr']*a[i] or rg(i) == 0: return []
    av = sum(v[i-20:i])/20
    if av > 0 and v[i] < CFG['pat_min_vol_ratio']*av: return []
    dn = c[i-1] < c[i-6] and c[i-1] < c[i-11]; up = c[i-1] > c[i-6] and c[i-1] > c[i-11]
    bull, bear = c[i] > o_[i], c[i] < o_[i]
    lw, uw = min(o_[i], c[i])-l[i], h[i]-max(o_[i], c[i])
    ab = sum(bd(j) for j in range(i-14, i))/14
    out = []
    if bull and dn and c[i-1] < o_[i-1] and o_[i] <= c[i-1] and c[i] >= o_[i-1] and bd(i) > bd(i-1):
        out.append(('p_beng', 'bull', 'ابتلاع شرائي||شمعة ابتلاع بعد هبوط'))
    if bear and up and c[i-1] > o_[i-1] and o_[i] >= c[i-1] and c[i] <= o_[i-1] and bd(i) > bd(i-1):
        out.append(('p_seng', 'bear', 'ابتلاع بيعي||شمعة ابتلاع بعد صعود'))
    if dn and lw >= 0.6*rg(i) and uw <= 0.2*rg(i): out.append(('p_ham', 'bull', 'مطرقة||شمعة مطرقة بعد هبوط'))
    if up and uw >= 0.6*rg(i) and lw <= 0.2*rg(i): out.append(('p_star', 'bear', 'شهاب||شمعة شهاب بعد صعود'))
    if bull and c[i-2] < o_[i-2] and bd(i-2) >= 0.6*ab and bd(i-1) <= 0.3*bd(i-2) and c[i] > (o_[i-2]+c[i-2])/2 and c[i-2] < c[i-7]:
        out.append(('p_morn', 'bull', 'نجمة الصباح||نموذج انعكاسي صاعد من 3 شموع'))
    if bear and c[i-2] > o_[i-2] and bd(i-2) >= 0.6*ab and bd(i-1) <= 0.3*bd(i-2) and c[i] < (o_[i-2]+c[i-2])/2 and c[i-2] > c[i-7]:
        out.append(('p_eve', 'bear', 'نجمة المساء||نموذج انعكاسي هابط من 3 شموع'))
    return out
def sig_flag(x, i):
    c, h, l, a, P = x['c'], x['h'], x['l'], x['atr'], CFG['flag_pole_len']
    for F in (6, 8, 10, 12, 15, 20):
        s = i-F
        if s-P < 0 or a[s-1] is None: continue
        A, top, bot = a[s-1], c[s-1], c[s-1]
        pole_h, pole_l = h[s-P:s], l[s-P:s]
        fh, fl = max(h[s:i]), min(l[s:i]); slope = (c[i-1]-c[s])/(F-1)
        lo = min(pole_l); up_pole = top-lo
        if up_pole >= CFG['flag_pole_atr']*A and pole_l.index(lo) < pole_h.index(max(pole_h)) and max(pole_h)-top <= 0.5*A \
           and top-fl <= 0.5*up_pole and fh-fl <= 0.5*up_pole and slope <= 0.05*A and c[i] > fh and c[i-1] <= fh:
            return [('flag_bull', 'bull', f'اختراق بولش فلاج||سطح الفلاج {fh:.6g} · طول الفلاج {F} شمعة')]
        hi = max(pole_h); dn_pole = hi-bot
        if dn_pole >= CFG['flag_pole_atr']*A and pole_h.index(hi) < pole_l.index(min(pole_l)) and bot-min(pole_l) <= 0.5*A \
           and fh-bot <= 0.5*dn_pole and fh-fl <= 0.5*dn_pole and slope >= -0.05*A and c[i] < fl and c[i-1] >= fl:
            return [('flag_bear', 'bear', f'كسر بيرش فلاج||سطح الفلاج {fl:.6g} · طول الفلاج {F} شمعة')]
    return []
def sig_trend(x, i):
    a_i = x['atr'][i]
    if a_i is None: return []
    w, N, c = CFG['pivot_window'], CFG['trend_confirm_closes'], x['c']
    tol, buf, out = CFG['trend_tol_atr']*a_i, CFG['trend_buffer_atr']*a_i, []
    for kind, plist, y in (('res', x['ph'], x['h']), ('sup', x['pl'], x['l'])):
        pv = [p for p in plist if i-200 <= p <= i-w-N][-7:]
        best = None
        for p1, p2 in combinations(pv, 2):
            if p2-p1 < 10: continue
            sl = (y[p2]-y[p1])/(p2-p1); ln = lambda j: y[p1]+sl*(j-p1)
            if kind == 'res': ok = all(c[j] <= ln(j)+tol for j in range(p1, i-N+1))
            else: ok = all(c[j] >= ln(j)-tol for j in range(p1, i-N+1))
            if not ok: continue
            touches = sum(1 for p in pv if p >= p1 and abs(y[p]-ln(p)) <= tol); span = i-N-p1
            if touches < CFG['trend_min_touches'] or span < CFG['trend_min_span']: continue
            if kind == 'res': brk = all(c[i-k] > ln(i-k)+buf for k in range(N)) and c[i-N] <= ln(i-N)+tol
            else: brk = all(c[i-k] < ln(i-k)-buf for k in range(N)) and c[i-N] >= ln(i-N)-tol
            if brk and (best is None or (touches, span) > best[:2]): best = (touches, span)
        if best:
            if kind == 'res': out.append(('tl_res', 'bull', f'اختراق ترند مقاومة||اتحترم {best[0]} مرات على مدى {best[1]} شمعة · تأكيد بـ{N} إغلاقات فوقه'))
            else: out.append(('tl_sup', 'bear', f'كسر ترند دعم||اتحترم {best[0]} مرات على مدى {best[1]} شمعة · تأكيد بـ{N} إغلاقات تحته'))
    return out

def sq_at(x, j):
    w = [v for v in x['bbw'][j-120:j+1] if v is not None]
    return len(w) > 30 and x['bbw'][j] is not None and x['bbw'][j] <= sorted(w)[int(len(w)*.2)]
def sig_bb(x, i):
    if i < 140 or x['bbw'][i] is None: return []
    c, o = x['c'], []
    if sq_at(x, i) and not sq_at(x, i-1):
        o.append(('bb_sq', 'neutral', f'ضغط التذبذب (بولنجر)||عرض النطاق {x["bbw"][i]:.1f}% من أضيق مستوياته في آخر 120 شمعة · احتمال حركة قوية قريبة'))
    if any(sq_at(x, j) for j in range(i-10, i)):
        if c[i] > x['bbU'][i] and c[i-1] <= x['bbU'][i-1]: o.append(('bb_up', 'bull', 'اختراق بولنجر لأعلى||إغلاق فوق النطاق العلوي بعد ضغط في التذبذب'))
        if c[i] < x['bbL'][i] and c[i-1] >= x['bbL'][i-1]: o.append(('bb_dn', 'bear', 'كسر بولنجر لأسفل||إغلاق تحت النطاق السفلي بعد ضغط في التذبذب'))
    return o
def obv_div(x, j):
    c, ob = x['c'], x['obv']
    if j < 31: return None
    cs, os_ = c[j-29:j+1], ob[j-29:j+1]; r = (max(os_)-min(os_)) or 1
    if c[j] >= max(cs)*.995 and (max(os_)-ob[j])/r > .1: return 'bear'
    if c[j] <= min(cs)*1.005 and (ob[j]-min(os_))/r > .1: return 'bull'
    return None
def sig_obv(x, i):
    d = obv_div(x, i)
    if d is None or obv_div(x, i-1) == d: return []
    if d == 'bull': return [('obv_bull', 'bull', 'تباعد OBV إيجابي||السعر عند قاع آخر 30 شمعة والسيولة (OBV) أقوى منه')]
    return [('obv_bear', 'bear', 'تباعد OBV سلبي||السعر عند قمة آخر 30 شمعة والسيولة (OBV) أضعف منها')]
def sig_sr(x, i):
    c, h, l, w, o = x['c'], x['h'], x['l'], CFG['pivot_window'], []
    for kind, lst, y in (('up', x['ph'], h), ('dn', x['pl'], l)):
        for p in [p for p in lst if i-200 <= p <= i-w-1][-8:]:
            lvl = y[p]
            cr = lambda j: (c[j] > lvl and c[j-1] <= lvl) if kind == 'up' else (c[j] < lvl and c[j-1] >= lvl)
            if cr(i) and not any(cr(j) for j in range(p+3, i)):
                t = ('sr_up', 'bull', f'اختراق مقاومة||السعر قفل فوق قمة {lvl:.6g} (اتكونت قبل {i-p} شمعة)') if kind == 'up' else ('sr_dn', 'bear', f'كسر دعم||السعر قفل تحت قاع {lvl:.6g} (اتكون قبل {i-p} شمعة)')
                o.append(t); break
    return o
def sig_vol(x, i):
    if i < 25: return []
    v = x['v']; av = sum(v[i-20:i])/20
    if av <= 0 or v[i] < CFG['vol_spike_ratio']*av: return []
    if x['c'][i] > x['o'][i]: return [('vol_up', 'bull', f'انفجار حجم مع شمعة صاعدة||الحجم ×{v[i]/av:.1f} من متوسط 20 شمعة · دخول شراء قوي')]
    if x['c'][i] < x['o'][i]: return [('vol_dn', 'bear', f'انفجار حجم مع شمعة هابطة||الحجم ×{v[i]/av:.1f} من متوسط 20 شمعة · ضغط بيع قوي')]
    return []
DETECT = [('golden_death', sig_ma), ('ema_9_21', sig_ema), ('ma200', sig_200), ('macd', sig_macd), ('macd', sig_rsima),
          ('divergence', sig_div), ('patterns', sig_pat), ('flags', sig_flag), ('trendlines', sig_trend),
          ('bollinger', sig_bb), ('obv', sig_obv), ('sr_breaks', sig_sr), ('volume', sig_vol)]
BREAKOUT = {'p_beng', 'p_seng', 'p_ham', 'p_star', 'p_morn', 'p_eve', 'flag_bull', 'flag_bear', 'tl_res', 'tl_sup', 'sr_up', 'sr_dn'}
def confirm(x, i, sid, d):
    """تأكيد الاختراقات/النماذج: RSI فوق/تحت متوسطه 14 + حجم التداول (للفلاج والترند)"""
    ok, parts = True, []
    r, rm = x['rsi'][i], x['rsima'][i]
    if r is not None and rm is not None:
        ok &= (r > rm) if d == 'bull' else (r < rm); parts.append('RSI فوق متوسطه' if r > rm else 'RSI تحت متوسطه')
    if sid.startswith(('flag', 'tl_', 'sr_')) and i >= 22:
        a = sum(x['v'][i-21:i-1])/20
        if a > 0:
            ratio = max(x['v'][i], x['v'][i-1])/a; ok &= ratio >= CFG['breakout_vol_ratio']; parts.append(f'حجم ×{ratio:.1f}')
    return ok, ('✅ مؤكد: ' if ok else '⚠️ غير مؤكد: ') + ' · '.join(parts)
def detect(x, i, tf=None):
    out = []
    off = CFG['signals_off_tf'].get(tf, [])
    for name, fn in DETECT:
        if CFG['signals'].get(name, True) and name not in off: out += fn(x, i)
    mode, res = CFG['breakout_confirm'], []
    for sid, d, txt in out:
        if sid in BREAKOUT and mode != 'off':
            ok, tag = confirm(x, i, sid, d)
            if not ok and mode == 'require': continue
            txt += '||' + tag
        res.append((sid, d, txt))
    return res

# ---------------- رسائل ----------------
def fp(v): return f'{v:,.2f}' if v >= 100 else f'{v:,.4f}' if v >= 1 else f'{v:.6g}'
RL = '\u200f'   # علامة اتجاه يمين-لشمال في أول كل سطر عشان العربي والإنجليزي ما يتلخبطوش
ICON = {'bull': '🟢', 'bear': '🔴', 'neutral': '🔸'}
TOK = re.compile(r'([A-Za-z][A-Za-z0-9]*(?: \d+)?|[+\-−]?\d[\d.,]*[%×]?)')
def fmtd(t): return TOK.sub(lambda m: f'<code>{m.group(1)}</code>', html.escape(t, quote=False))
def big(n): return f'{n/1e9:.2f}B' if n >= 1e9 else f'{n/1e6:.1f}M' if n >= 1e6 else f'{n/1e3:.0f}K' if n >= 1e3 else f'{n:.0f}'
def build(sym, tf, items, x):
    tz = ZoneInfo(CFG['tz']); n = len(x['c'])-1; c = x['c'][n]
    nb, ns = sum(1 for it in items if it[2] == 'bull'), sum(1 for it in items if it[2] == 'bear')
    L = [f'{RL}🔔 <b>{html.escape(sym[:-4])}</b> / USDT  ·  ⏱ <b>{TF_AR[tf]}</b>']
    if nb and ns: L.append(f'{RL}⚖️ إشارات متعارضة: {nb} صعود · {ns} نزول')
    elif max(nb, ns) > 1: L.append(f'{RL}{"🟢" if nb else "🔴"} <b>تلاقي {max(nb, ns)} إشارات</b> في نفس الاتجاه')
    L.append(f'{RL}━━━━━━━━━━━━━━')
    ht = CFG['htf_filter'].get(tf); tr = HTF.get((sym, ht)) if ht else None
    for key, i, d, txt in sorted(items, key=lambda z: z[1]):
        title, det, tag = (txt.split('||')+['', ''])[:3]
        t = datetime.fromtimestamp((x['t'][i]+TF_MS[tf])/1000, tz).strftime('%H:%M · %d/%m')
        L.append(f'{RL}{ICON[d]} <b>{html.escape(title, quote=False)}</b>')
        if det: L.append(f'{RL}   ↳ {fmtd(det)}')
        L.append(f'{RL}   🕒 <code>{t}</code>')
        ad_, pd_, md_ = x['adx'][i], x['pdi'][i], x['mdi'][i]
        if d != 'neutral' and ad_ is not None and pd_ is not None:
            lab = 'عرضي ضعيف' if ad_ < 20 else 'متوسط' if ad_ < 25 else 'قوي' if ad_ < 40 else 'قوي جدًا'
            p3 = x['adx'][i-3] if i >= 3 else None
            tr_ = ('، بيقوى ↑' if ad_-p3 > 1 else '، بيضيق ↓' if ad_-p3 < -1 else '') if p3 is not None else ''
            ag = ad_ >= 20 and ((d == 'bull') == (pd_ > md_))
            L.append(f'{RL}   📶 ' + fmtd(f'قوة الاتجاه ADX {ad_:.0f} — {lab}{tr_} · اتجاه {"صاعد" if pd_ > md_ else "هابط"}') + (' · ✅ مع الإشارة' if ag else ' · ⚠️ عكس الإشارة' if ad_ >= 20 else ''))
        av_i = sum(x['v'][i-20:i])/20 if i >= 20 else 0
        if av_i > 0:
            vr = x['v'][i]/av_i; L.append(f'{RL}   📦 ' + fmtd(f'الحجم ×{vr:.1f} من المتوسط — ' + ('انفجار' if vr >= 3 else 'مرتفع' if vr >= 1.5 else 'عادي' if vr >= .7 else 'ضعيف')))
        if tag: L.append(f'{RL}   {fmtd(tag)}')
        if ht and tr is not None and CFG['htf_mode'] == 'tag' and d != 'neutral':
            ok = tr != 0 and (d == 'bull') == (tr > 0)
            L.append(f'{RL}   ' + (f'✅ فريم {TF_AR[ht]} معاها' if ok else f'⚠️ فريم {TF_AR[ht]} ' + ('عكسها' if tr != 0 else 'متضارب')))
        L.append(RL)
    L.append(f'{RL}━━━━━━━━━━━━━━')
    r, rm, s2, a, ad = x['rsi'][n], x['rsima'][n], x['m200'][n], x['atr'][n], x['adx'][n]
    L.append(f'{RL}💰 السعر: <code>{fp(c)}</code>')
    if r is not None and rm is not None: L.append(f'{RL}📊 RSI: <code>{r:.1f}</code> — {"فوق" if r > rm else "تحت"} متوسطه <code>{rm:.1f}</code>')
    if ad is not None: L.append(f'{RL}📈 قوة الاتجاه (ADX): <code>{ad:.0f}</code> — ' + ('قوي' if ad >= 25 else 'عرضي ضعيف' if ad < 20 else 'متوسط'))
    if a: L.append(f'{RL}📐 ATR: متوسط حركة الشمعة <code>{a/c*100:.2f}%</code>')
    if s2: L.append(f'{RL}🧭 السعر {"فوق" if c > s2 else "تحت"} <code>{MT} 200</code>' + (f' · اتجاه فريم {TF_AR[ht]}: ' + {1: "صاعد", -1: "هابط", 0: "متضارب"}[tr] if tr is not None else ''))
    bw = x['bbw'][n]
    if bw is not None:
        pb = (c-x['bbL'][n])/((x['bbU'][n]-x['bbL'][n]) or 1)*100
        L.append(f'{RL}🎯 بولنجر: التذبذب <code>{bw:.1f}%</code>' + (' 🔸 ضغط، احتمال حركة قوية' if sq_at(x, n) else '') + f' · موقع السعر <code>{pb:.0f}%</code> من النطاق')
    v, ob = x['v'], x['obv']; av = sum(v[-21:-1])/20
    L.append(f'{RL}📦 الحجم <code>×{(v[-1]/av if av else 0):.1f}</code> من المتوسط · OBV {"فوق" if ob[-1] > ema(ob, 20)[-1] else "تحت"} متوسطه')
    H30, L30 = max(x['h'][-30:]), min(x['l'][-30:]); P = (H30+L30+c)/3; R1, S1 = 2*P-L30, 2*P-H30
    L.append(f'{RL}📍 مقاومة <code>{fp(R1)}</code> ↑<code>{abs(R1/c-1)*100:.1f}%</code> · دعم <code>{fp(S1)}</code> ↓<code>{abs(S1/c-1)*100:.1f}%</code>')
    L.append('{{CTX}}')
    L.append(f'{RL}ℹ️ إشارة آلية للمراجعة وليست توصية')
    return '\n'.join(L)

# ---------------- سياق السوق: دفتر الأوامر (سبوت) + العقود + تصفية تقديرية ----------------
FU = 'https://fapi.binance.com'; CTX = {}
def jget(urls):
    for u in urls:
        try:
            r = requests.get(u, timeout=15)
            if r.status_code == 200: return r.json()
        except Exception: pass
    return None
def liq_model(rows, p, ratio, span=.2, nb=40):
    LEV = ((5, .10), (10, .25), (25, .30), (50, .20), (100, .15)); MM = .005; n = len(rows)
    sh = ratio/(1+ratio) if ratio > 0 else .5
    H, L = [float(r[2]) for r in rows], [float(r[3]) for r in rows]
    smin, smax = [float('inf')]*(n+1), [-float('inf')]*(n+1)
    for j in range(n-1, -1, -1): smin[j] = min(L[j], smin[j+1]); smax[j] = max(H[j], smax[j+1])
    dn, up, step = [0.0]*nb, [0.0]*nb, span/nb*p
    for j, r in enumerate(rows):
        e = (float(r[2])+float(r[3])+float(r[4]))/3; w = float(r[7])*0.5**((n-1-j)/240)
        for lv, wt in LEV:
            ll, sl = e*(1-1/lv+MM), e*(1+1/lv-MM)
            if ll < p and smin[j+1] > ll:
                b = int((p-ll)//step)
                if b < nb: dn[b] += w*wt*sh
            if sl > p and smax[j+1] < sl:
                b = int((sl-p)//step)
                if b < nb: up[b] += w*wt*(1-sh)
    return dn, up, step
def context(sym):
    if sym in CTX: return CTX[sym]
    d = {}
    dep = jget([f'{h}/api/v3/depth?symbol={sym}&limit=1000' for h in HOSTS])
    if dep and dep.get('bids') and dep.get('asks'):
        B = [(float(p), float(p)*float(q)) for p, q in dep['bids']]; A = [(float(p), float(p)*float(q)) for p, q in dep['asks']]
        mid = (B[0][0]+A[0][0])/2
        bb, aa = [x for x in B if x[0] >= mid*.95], [x for x in A if x[0] <= mid*1.05]
        d['ob'] = (sum(u for p, u in B if p >= mid*.98), sum(u for p, u in A if p <= mid*1.02))
        if bb and aa: d['wb'], d['wa'], d['mid'] = max(bb, key=lambda z: z[1]), max(aa, key=lambda z: z[1]), mid
    prem = jget([f'{FU}/fapi/v1/premiumIndex?symbol={sym}'])
    if prem and 'lastFundingRate' in prem: d['fr'] = float(prem['lastFundingRate'])*100
    gl = jget([f'{FU}/futures/data/globalLongShortAccountRatio?symbol={sym}&period=1h&limit=1'])
    if gl: d['ls'] = float(gl[0]['longShortRatio'])
    fk = jget([f'{FU}/fapi/v1/klines?symbol={sym}&interval=1h&limit=500'])
    if fk and len(fk) > 100:
        p = float(fk[-1][4]); dn, up, step = liq_model(fk, p, d.get('ls', 1.0)); mx = max(dn+up) or 1
        near = lambda arr: next(((i+.5)*step/p*100 for i, v in enumerate(arr) if v >= mx*.5), None)
        td, tu = sum(dn[:10]), sum(up[:10]); d['liq'] = (near(dn), near(up), td/((td+tu) or 1), sum(dn[:5]), sum(up[:5]))
    CTX[sym] = d; return d
def ctx_lines(sym, tf):
    if not CFG['market_context'] or tf not in CFG['context_tfs']: return ''
    d = context(sym); L = []
    if 'ob' in d:
        b, a = d['ob']; im = (b-a)/((b+a) or 1)*100
        L.append(f'{RL}💧 دفتر الأوامر (±2%): شراء <code>${big(b)}</code> مقابل بيع <code>${big(a)}</code> — ' + ('ضغط شراء أقوى' if im > 15 else 'ضغط بيع أقوى' if im < -15 else 'متوازن'))
    if 'wb' in d:
        (pb, ub), (pa, ua), mid = d['wb'], d['wa'], d['mid']
        L.append(f'{RL}🧱 أكبر جدار شراء <code>${big(ub)}</code> عند <code>{fp(pb)}</code> · بيع <code>${big(ua)}</code> عند <code>{fp(pa)}</code>')
    if 'fr' in d:
        L.append(f'{RL}⚖️ التمويل (Funding): <code>{d["fr"]:.4f}%</code>' + (' — لونج مزدحم' if d['fr'] > .03 else ' — شورت مزدحم' if d['fr'] < -.01 else '') + (f' · لونج <code>{d["ls"]/(1+d["ls"])*100:.0f}%</code> من الحسابات' if 'ls' in d else ''))
    if 'liq' in d:
        nd, nu, sh, sd, su = d['liq']
        side = 'تصفية اللونج (تحت السعر)' if sh >= .6 else 'تصفية الشورت (فوق السعر)' if sh <= .4 else 'متقاربة من الجهتين'
        L.append(f'{RL}🎯 التصفية (تقدير): أقرب تجمع لونج ' + (f'<code>−{nd:.1f}%</code>' if nd else '—') + ' · شورت ' + (f'<code>+{nu:.1f}%</code>' if nu else '—') + f' · الأكبر: {side}')
    return ('\n'.join(L)+'\n') if L else ''
def send(text, dry):
    if dry: print(re.sub(r'<[^>]+>', '', html.unescape(text)), '\n'); return True
    tok, chat = os.environ.get('TELEGRAM_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
    if not tok or not chat: print('TELEGRAM_TOKEN / TELEGRAM_CHAT_ID ناقصين'); return False
    try:
        url = f'https://api.telegram.org/bot{tok}/sendMessage'
        r = requests.post(url, json={'chat_id': chat, 'text': text, 'parse_mode': 'HTML', 'disable_web_page_preview': True}, timeout=20)
        if r.status_code == 400:   # لو التنسيق فشل نبعت نص عادي
            print('html failed, retry plain', r.text[:150])
            r = requests.post(url, json={'chat_id': chat, 'text': re.sub(r'<[^>]+>', '', html.unescape(text))}, timeout=20)
        if r.status_code != 200: print('telegram error', r.status_code, r.text[:200])
        return r.status_code == 200
    except Exception as e:
        print('telegram exception', e); return False

# ---------------- قائمة العملات (أعلى N بالقيمة السوقية المتاحة على Binance) ----------------
STABLE = {'usdt', 'usdc', 'dai', 'fdusd', 'tusd', 'usde', 'usds', 'pyusd', 'usdd', 'usd1', 'usdy', 'susde', 'susds', 'bsc-usd', 'usdt0',
          'paxg', 'xaut', 'wbtc', 'steth', 'wsteth', 'weth', 'weeth', 'wbeth', 'reth', 'cbbtc', 'cbeth', 'solvbtc', 'tbtc', 'lbtc',
          'bnsol', 'jitosol', 'msol', 'ezeth', 'rseth', 'eeth', 'sweth', 'wtrx', 'bfusd', 'usdtb', 'usdf'}
def universe(st):
    u = st.get('universe')
    if u and time.time()-u['ts'] < 86400 and u.get('n') == CFG['top_n']: return u['coins']
    coins = []
    try:
        cg = requests.get('https://api.coingecko.com/api/v3/coins/markets', timeout=30,
                          params={'vs_currency': 'usd', 'order': 'market_cap_desc', 'per_page': min(250, CFG['top_n']), 'page': 1}).json()
        have = set()
        for h in HOSTS:
            r = requests.get(f'{h}/api/v3/ticker/price', timeout=30)
            if r.status_code == 200: have = {d['symbol'] for d in r.json()}; break
        for d in cg:
            sym = d['symbol'].upper()+'USDT'
            if d['symbol'].lower() in STABLE or sym in CFG['exclude'] or sym not in have or sym in coins: continue
            coins.append(sym)
        print(f'universe: {len(coins)} عملة من أعلى {CFG["top_n"]}')
    except Exception as e:
        print('universe error', e)
    if not coins: return u['coins'] if u else CFG['coins']
    coins += [c for c in CFG['extra_coins'] if c not in coins]
    st['universe'] = {'ts': time.time(), 'coins': coins, 'n': CFG['top_n']}
    return coins

WEAK = {'e921u', 'e921d', 'macd_up', 'macd_dn', 'rsima_u', 'rsima_d', 'p_beng', 'p_seng', 'p_ham', 'p_star', 'p_morn', 'p_eve'}
STRONG = {'golden', 'death', 'c200u', 'c200d', 'div_bull', 'div_bear', 'flag_bull', 'flag_bear', 'tl_res', 'tl_sup', 'sr_up', 'sr_dn', 'bb_up', 'bb_dn', 'vol_up', 'vol_dn', 'obv_bull', 'obv_bear'}
def score(m): return len(m[4])+sum(1 for it in m[4] if it[0].split('|')[2] in STRONG)+m[0]*.5
def digest_chunks(msgs):
    L = [f'{RL}📋 <b>ملخص الإشارات</b> — {len(msgs)} عملة/فريم', f'{RL}━━━━━━━━━━━━━━']
    for idx in sorted({m[0] for m in msgs}, reverse=True):
        grp = [m for m in msgs if m[0] == idx]; L.append(f'{RL}⏱ <b>{TF_AR[grp[0][2]]}</b> · {len(grp)}')
        for m in sorted(grp, key=lambda z: -score(z)):
            ds = {it[2] for it in m[4]}; ts = [html.escape(it[3].split('||')[0], quote=False) for it in m[4]]
            L.append(f'{RL}{"🟢" if ds == {"bull"} else "🔴" if ds == {"bear"} else "⚖️"} <b>{html.escape(m[1][:-4])}</b>: ' + ' · '.join(ts[:3]) + (f' +{len(ts)-3}' if len(ts) > 3 else ''))
        L.append(RL)
    out, cur = [], []
    for ln in L:
        if sum(len(z)+1 for z in cur)+len(ln) > 3500: out.append('\n'.join(cur)); cur = []
        cur.append(ln)
    out.append('\n'.join(cur)); return out
# عائلات الإشارات: التلاقي بيتحسب بعائلات مختلفة عشان إشارتين من نفس الحدث (زي اختراق بولنجر + كسر مقاومة) ما يتحسبوش تأكيد مضاعف
FAMILY = {**{k: 'اتجاه' for k in ('golden', 'death', 'c200u', 'c200d', 'e921u', 'e921d')},
          **{k: 'زخم' for k in ('macd_up', 'macd_dn', 'rsima_u', 'rsima_d', 'div_bull', 'div_bear')},
          **{k: 'هيكل' for k in ('sr_up', 'sr_dn', 'tl_res', 'tl_sup', 'flag_bull', 'flag_bear', 'bb_up', 'bb_dn')},
          **{k: 'حجم' for k in ('obv_bull', 'obv_bear', 'vol_up', 'vol_dn')},
          **{k: 'شموع' for k in ('p_beng', 'p_seng', 'p_ham', 'p_star', 'p_morn', 'p_eve')}}
def confluence(out):
    cf = CFG['confluence']
    if not cf['enabled'] or not out: return out
    fams = {'bull': set(), 'bear': set()}
    for it in out:
        sid = it[0].split('|')[2]
        if it[2] in fams and sid in FAMILY: fams[it[2]].add(FAMILY[sid])
    keep = [it for it in out if it[2] in fams and (len(fams[it[2]]) >= cf['min_families'] or it[0].split('|')[2] in cf['exempt'])]
    return keep + [it for it in out if it[2] == 'neutral'] if keep else []   # الإشارات المحايدة (ضغط التذبذب) بتتبعت بس مع إشارات اتجاهية
def filt(items, st, tf, x):
    """فلترة: مبعوتة قبل كده، تهدئة (cooldown) لنفس الإشارة، وتلاقي إشارات للإشارات الضعيفة في الفريمات القصيرة"""
    items = [it for it in items if it[0] not in st['sent']]; out = []; loc = {}
    for it in sorted(items, key=lambda z: z[1]):
        sym, _, sid, t = it[0].split('|'); t = int(t); base = f'{sym}|{tf}|{sid}'
        last = loc.get(base, st['last'].get(base))
        if last is not None and (t-last)/TF_MS[tf] < CFG['cooldown_candles']: continue
        loc[base] = t; out.append(it)
    ht = CFG['htf_filter'].get(tf)
    if ht and out and CFG['htf_mode'] == 'require':
        tr = HTF.get((out[0][0].split('|')[0], ht))
        out = [it for it in out if htf_ok(tr, it[0].split('|')[2], it[2])]
    m = CFG['min_confluence'].get(tf, 1)
    if m > 1:
        cnt = {d: sum(1 for it in out if it[2] == d) for d in ('bull', 'bear')}
        out = [it for it in out if it[0].split('|')[2] not in WEAK or cnt[it[2]] >= m]
    return confluence(out)
def mark(st, it, now_ms, now):
    sym, tf, sid, t = it[0].split('|'); st['sent'][it[0]] = now; st['last'][f'{sym}|{tf}|{sid}'] = int(t)
def closed_open(tf):
    off = 4*86400000 if tf == '1w' else 0   # أول اتنين بعد 1/1/1970 = +4 أيام
    return ((int(time.time()*1000)-off)//TF_MS[tf])*TF_MS[tf]+off-TF_MS[tf]
def htf_trend(job):
    sym, tf = job; x = klines(sym, tf, min_len=60)
    if not x: return job, None
    light(x); return job, trend_dir(x, len(x['c'])-1)
def work(job):
    sym, tf = job
    x = klines(sym, tf)
    if not x: return job, None, []
    prep(x); n = len(x['c']); items = []
    for i in range(n-CFG['lookback'].get(tf, 2), n):
        for sid, d, txt in detect(x, i, tf): items.append((f'{sym}|{tf}|{sid}|{x["t"][i]}', i, d, txt))
    return job, x, items
def run(dry=False):
    st = json.load(open(STATE_PATH)) if os.path.exists(STATE_PATH) else {}
    st.setdefault('sent', {}); st.setdefault('done', {}); st.setdefault('last', {})
    first = not st.get('initialized')   # أول تشغيل: بنسجل الإشارات القديمة من غير ما نبعتها
    now = time.time(); now_ms = int(now*1000); st['sent'] = {k: v for k, v in st['sent'].items() if now-v < 5*86400}
    st['last'] = {k: v for k, v in st['last'].items() if now_ms-v < 5*86400000}
    coins = universe(st)
    jobs = [(sym, tf) for tf in CFG['timeframes'] for sym in coins if st['done'].get(f'{sym}|{tf}') != closed_open(tf)]
    print(f'{len(coins)} عملة · {len(jobs)} فحص')
    need = sorted({(sym, CFG['htf_filter'][tf]) for sym, tf in jobs if tf in CFG['htf_filter']})
    with ThreadPoolExecutor(CFG['workers']) as ex:
        HTF.clear(); HTF.update(dict(ex.map(htf_trend, need)))
        results = list(ex.map(work, jobs))
    msgs = []
    for (sym, tf), x, items in results:
        if x is None: continue
        items = filt(items, st, tf, x)
        if not items or (first and not dry):
            for it in items: mark(st, it, now_ms, now)
            st['done'][f'{sym}|{tf}'] = closed_open(tf); continue
        msgs.append((list(TF_MS).index(tf), sym, tf, build(sym, tf, items, x), items))
    msgs.sort(key=lambda m: -m[0]); cap = CFG['max_messages_per_run']; dg = CFG['digest']
    if dg['enabled'] and len(msgs) > dg['over']:
        cards = sorted(msgs, key=lambda m: -score(m))[:dg['top_cards']]; ids = {id(m) for m in cards}; okd = True
        for part in digest_chunks(msgs):
            okd &= send(part, dry)
            if not dry: time.sleep(1.1)
        if okd:
            for m in msgs:
                if id(m) not in ids:
                    for it in m[4]: mark(st, it, now_ms, now)
                    st['done'][f'{m[1]}|{m[2]}'] = closed_open(m[2])
        sel = cards
    else: sel = msgs[:cap]
    if CFG['market_context']:
        want = sorted({m[1] for m in sel if m[2] in CFG['context_tfs']})
        with ThreadPoolExecutor(CFG['workers']) as ex: list(ex.map(context, want))
    for _, sym, tf, text, items in sel:
        if send(text.replace('{{CTX}}', ctx_lines(sym, tf).rstrip('\n')), dry):
            for it in items: mark(st, it, now_ms, now)
            st['done'][f'{sym}|{tf}'] = closed_open(tf)
        if not dry: time.sleep(1.1)
    if not (dg['enabled'] and len(msgs) > dg['over']) and len(msgs) > cap: print(f'{len(msgs)-cap} رسالة اتأجلت للتشغيل الجاي')
    if first and not dry: st['initialized'] = True; print('أول تشغيل: اتسجلت الإشارات الحالية بدون إرسال')
    if not dry: json.dump(st, open(STATE_PATH, 'w'), indent=1)

def demo(dry):
    sym = 'BTCUSDT'; x = klines(sym, '1h')
    if not x: print('demo: تعذر جلب البيانات'); return
    prep(x); n = len(x['c'])-1
    items = [('demo', n, 'bull', 'رسالة تجريبية لعرض الشكل الجديد||دي مش إشارة حقيقية · الغرض شكل الرسالة بس||✅ مؤكد: RSI فوق متوسطه · حجم ×1.5')]
    context(sym); send(build(sym, '1h', items, x).replace('{{CTX}}', ctx_lines(sym, '1h').rstrip('\n')), dry)
if __name__ == '__main__':
    if '--test' in sys.argv: send('✅ البوت شغال وبيبعت تنبيهات.', '--dry' in sys.argv); demo('--dry' in sys.argv)
    else: run('--dry' in sys.argv)
