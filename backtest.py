#!/usr/bin/env python3
"""اختبار تاريخي مبسّط للإشارات: بنقيس اتجاه السعر بعد كل إشارة بعدد شموع ثابت (مش استراتيجية دخول/خروج كاملة).
تشغيل:  python backtest.py [--tf 4h,1d] [--coins 30] [--telegram]
النتايج بتتكتب في backtest_results.md"""
import os, sys, time, statistics as st
from bisect import bisect_right
from multiprocessing import Pool
import requests
import bot

HZ = {'15m': [4, 12, 24], '30m': [4, 12, 24], '1h': [4, 12, 24], '4h': [3, 6, 12], '1d': [3, 7, 14]}   # الآفاق بالشموع
NC = {'15m': 6000, '30m': 6000, '1h': 5000, '4h': 3000, '1d': 1500, '1w': 700}                                  # عدد الشموع التاريخية
FEE = 0.2   # % تكلفة الصفقة الكاملة (عمولة + انزلاق) بتتخصم من كل نتيجة
NAMES = {'golden': 'تقاطع ذهبي', 'death': 'تقاطع الموت', 'e921u': 'EMA9 يخترق 21', 'e921d': 'EMA9 يكسر 21',
         'c200u': 'إغلاقات فوق 200', 'c200d': 'إغلاقات تحت 200', 'macd_up': 'MACD صعود', 'macd_dn': 'MACD نزول',
         'div_bull': 'دايفيرجن إيجابي', 'div_bear': 'دايفيرجن سلبي', 'flag_bull': 'بولش فلاج', 'flag_bear': 'بيرش فلاج',
         'tl_res': 'اختراق ترند', 'tl_sup': 'كسر ترند', 'p_beng': 'ابتلاع شرائي', 'p_seng': 'ابتلاع بيعي', 'p_ham': 'مطرقة',
         'p_star': 'شهاب', 'p_morn': 'نجمة الصباح', 'p_eve': 'نجمة المساء'}

def hist(sym, tf):
    rows, end, total = [], None, int(NC[tf]*float(os.environ.get('BT_SCALE', 1)))
    while len(rows) < total:
        p = {'symbol': sym, 'interval': tf, 'limit': 1000}
        if end: p['endTime'] = end
        d = None
        for h in bot.HOSTS:
            try:
                r = requests.get(f'{h}/api/v3/klines', params=p, timeout=30)
                if r.status_code == 200: d = r.json(); break
            except Exception: pass
        if not d: break
        rows = d+rows; end = d[0][0]-1
        if len(d) < 1000: break
    now = int(time.time()*1000); rows = [k for k in rows if k[6] < now]
    if len(rows) < 400: return None
    return {'t': [k[0] for k in rows], 'o': [float(k[1]) for k in rows], 'h': [float(k[2]) for k in rows],
            'l': [float(k[3]) for k in rows], 'c': [float(k[4]) for k in rows], 'v': [float(k[5]) for k in rows]}

def job(a):
    sym, tf = a; x = hist(sym, tf)
    if not x: return []
    bot.prep(x); n = len(x['c']); c = x['c']; hz = HZ[tf]; out = []; last = {}
    ht = bot.CFG['htf_filter'].get(tf); xh = None
    if ht:
        xh = hist(sym, ht)
        if xh: bot.light(xh); ct = [t+bot.TF_MS[ht] for t in xh['t']]
    for i in range(250, n-min(hz)):
        for sid, d, txt in bot.detect(x, i, tf):
            if i-last.get(sid, -999) < bot.CFG['cooldown_candles']: continue
            last[sid] = i
            conf = '-' if sid not in bot.BREAKOUT else ('✅' if '✅' in txt else '⚠️')
            tr = None
            if xh:
                j = bisect_right(ct, x['t'][i]+bot.TF_MS[tf])-1
                tr = bot.trend_dir(xh, j) if j >= 0 else None
            flt_ok = bot.htf_ok(tr, sid, d)
            sgn = 1 if d == 'bull' else -1
            rets = [sgn*(c[i+h]/c[i]-1)*100 - FEE if i+h < n else None for h in hz]
            out.append((tf, sid, conf, d, rets, flt_ok))
    drift = [[(c[i+h]/c[i]-1)*100 for i in range(250, n-h)] for h in hz]
    out.append((tf, '_drift', '-', '-', [sum(v)/len(v) if v else 0 for v in drift], True))
    return out

def main():
    arg = lambda k, dflt: sys.argv[sys.argv.index(k)+1] if k in sys.argv else dflt
    tfs = arg('--tf', ','.join(bot.CFG['timeframes'])).split(','); ncoin = int(arg('--coins', 40))
    st_ = {}; coins = bot.universe(st_)[:ncoin]
    jobs = [(s, tf) for tf in tfs for s in coins]
    print(f'{len(coins)} عملة × {len(tfs)} فريم = {len(jobs)} اختبار')
    with Pool(2) as p: res = p.map(job, jobs)
    agg, drift = {}, {}
    for r in res:
        for tf, sid, conf, d, rets, ok in r:
            if sid == '_drift': drift.setdefault(tf, []).append(rets)
            else:
                agg.setdefault((tf, sid, conf, 'كله'), []).append((d, rets))
                if ok: agg.setdefault((tf, sid, conf, 'بفلتر'), []).append((d, rets))
    md = ['# نتايج الاختبار التاريخي', '',
          f'- العملات: {len(coins)} من أعلى {bot.CFG["top_n"]} · التكلفة المخصومة لكل إشارة: {FEE}%',
          '- صح% = نسبة الإشارات اللي نتيجتها (في اتجاه الإشارة وبعد التكلفة) موجبة. **الفرق** = المتوسط بعد التكلفة ناقص متوسط حركة السوق العامة في نفس الفترة.',
          f'- فلتر الفريم الأكبر (سلسلة التأكيد): {bot.CFG["htf_filter"]} · القاعدة: {bot.CFG["htf_rule"]} · معفى منه: {bot.CFG["htf_exempt"]}. "بفلتر" = الإشارات اللي كان الفريم الأكبر معاها.',
          '- مفيش حجم صفقة ولا وقف خسارة ولا خروج، فده قياس اتجاه بعد عدد شموع ثابت، مش ربح استراتيجية.', '']
    for tf in tfs:
        dr = [sum(v[k] for v in drift[tf])/len(drift[tf]) for k in range(len(HZ[tf]))] if tf in drift else [0]*3
        md += [f'## فريم {bot.TF_AR[tf]}', '', f'| الإشارة | تأكيد RSI | الفلتر | العدد | ' + ' | '.join(f'بعد {h} شمعة: صح% / متوسط%' for h in HZ[tf]) + ' | الفرق (الأفق الأوسط) |',
               '|' + '---|'*(5+len(HZ[tf]))]
        rows = []
        for (t, sid, conf, flt), L in agg.items():
            if t != tf: continue
            cells, edge = [], None
            for k, h in enumerate(HZ[tf]):
                v = [r[1][k] for r in L if r[1][k] is not None]
                if not v: cells.append('—'); continue
                cells.append(f'{sum(1 for z in v if z > 0)/len(v)*100:.0f}% / {sum(v)/len(v):+.2f}%')
                if k == 1:
                    base = sum((1 if r[0] == 'bull' else -1)*dr[k] for r in L)/len(L)
                    edge = sum(v)/len(v) - base
            rows.append((edge if edge is not None else -99, f'| {NAMES.get(sid, sid)} | {conf} | {flt} | {len(L)} | ' + ' | '.join(cells) + f' | {edge:+.2f}% |' if edge is not None else ''))
        md += [r[1] for r in sorted(rows, reverse=True) if r[1]] + ['']
    text = '\n'.join(md); open(os.path.join(bot.HERE, 'backtest_results.md'), 'w', encoding='utf-8').write(text); print(text)
    if '--telegram' in sys.argv and os.environ.get('TELEGRAM_TOKEN'):
        with open(os.path.join(bot.HERE, 'backtest_results.md'), 'rb') as f:
            requests.post(f'https://api.telegram.org/bot{os.environ["TELEGRAM_TOKEN"]}/sendDocument',
                          data={'chat_id': os.environ['TELEGRAM_CHAT_ID']}, files={'document': f}, timeout=60)
if __name__ == '__main__': main()
