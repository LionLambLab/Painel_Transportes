#!/usr/bin/env python3
"""
fetch_data.py — Coleta automática de dados para o Painel de Transportes
"""

import json, os, re, io
from datetime import datetime, timedelta
import requests
import pandas as pd

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'indicators.json')
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

HEADERS = {'User-Agent': 'Mozilla/5.0 (PainelTransportes/1.0; github-actions)'}
TIMEOUT = 20
MESES   = ['','Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']

def fmt_date(s):
    """'dd/mm/yyyy' → 'Mmm/aa'"""
    try:
        p = s.split('/'); return f"{MESES[int(p[1])]}/{p[2][2:]}"
    except: return s

def fmt_ym(ym):
    """'YYYY-MM' → 'Mmm/aa'"""
    try:
        y,m = ym.split('-'); return f"{MESES[int(m)]}/{y[2:]}"
    except: return ym

def get_json(url, label=''):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f'  ⚠️  {label}: {e}')
        return None

def bcb_sgs(cod, n=15):
    url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{cod}/dados/ultimos/{n}?formato=json'
    d = get_json(url, f'BCB SGS {cod}')
    if not d: return []
    out = []
    for item in d:
        try:
            v = item.get('valor','').replace(',','.')
            if v and v not in ('-',''):
                out.append({'data': item['data'], 'valor': float(v)})
        except: pass
    return out

# ─────────────────────────────────────────────
# 1. ANP — DIESEL S10
# ─────────────────────────────────────────────

def fetch_anp():
    print('📥 ANP...')
    urls = [
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2025/ca-2025.csv',
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2025/ca-2025-01.csv',
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2024/ca-2024-02.csv',
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200 and len(r.content) > 1000:
                df = pd.read_csv(io.StringIO(r.content.decode('latin-1')), sep=';', decimal=',', low_memory=False)
                print(f'  ✅ CSV {url.split("/")[-1]} ({len(df)} linhas)')
                return _anp_process(df)
        except Exception as e:
            print(f'  ⚠️  ANP CSV: {e}')
    return _anp_fallback()

def _anp_process(df):
    try:
        df.columns = [c.strip().upper() for c in df.columns]
        prod  = next((c for c in df.columns if 'PRODUTO' in c), None)
        preco = next((c for c in df.columns if 'REVENDA' in c and 'PRECO' in c), None)
        data  = next((c for c in df.columns if 'DATA' in c and 'INICIAL' in c), None)
        regiao= next((c for c in df.columns if 'REGIAO' in c), None)
        if not all([prod, preco, data]): return _anp_fallback()
        diesel = df[df[prod].str.contains('S10|DIESEL S-10', case=False, na=False)].copy()
        if diesel.empty: return _anp_fallback()
        diesel[preco] = pd.to_numeric(diesel[preco], errors='coerce')
        diesel[data]  = pd.to_datetime(diesel[data], dayfirst=True, errors='coerce')
        nac = diesel.groupby(data)[preco].mean().reset_index().sort_values(data).tail(16)
        semanas = []
        for _, row in nac.iterrows():
            dt = row[data]; dt2 = dt + timedelta(days=6)
            semanas.append({'ini':dt.strftime('%Y-%m-%d'),'fim':dt2.strftime('%Y-%m-%d'),
                            'ini_br':dt.strftime('%d/%m'),'fim_br':dt2.strftime('%d/%m/%Y'),
                            'label':fmt_date(dt.strftime('%d/%m/%Y')),'preco':round(float(row[preco]),3)})
        ult = semanas[-1] if semanas else {}
        return {'semana_referencia':f"{ult.get('ini_br','')}/a {ult.get('fim_br','')}",
                'preco_atual':ult.get('preco',6.89),'semanas':semanas,
                'regioes':_anp_regioes(diesel,data,preco,regiao),
                'atualizado':datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC')}
    except Exception as e:
        print(f'  ⚠️  ANP process: {e}'); return _anp_fallback()

def _anp_regioes(df, dc, pc, rc):
    mapa = {'NORTE':('Norte','🟢'),'NORDESTE':('Nordeste','🟡'),
            'CENTRO':('Centro-Oeste','🟠'),'SUDESTE':('Sudeste','🔵'),'SUL':('Sul','⚪')}
    out = []
    if rc and rc in df.columns:
        ult = df[df[dc] == df[dc].max()]
        for k,(n,e) in mapa.items():
            sub = ult[ult[rc].str.upper().str.contains(k,na=False)]
            if not sub.empty:
                out.append({'nome':n,'emoji':e,'preco':round(float(sub[pc].mean()),3)})
    if not out:
        ref = 6.89
        out = [{'nome':'Norte','emoji':'🟢','preco':round(ref+0.31,3)},
               {'nome':'Nordeste','emoji':'🟡','preco':round(ref+0.18,3)},
               {'nome':'Centro-Oeste','emoji':'🟠','preco':round(ref-0.04,3)},
               {'nome':'Sudeste','emoji':'🔵','preco':round(ref-0.13,3)},
               {'nome':'Sul','emoji':'⚪','preco':round(ref-0.22,3)}]
    return out

def _anp_fallback():
    return {'semana_referencia':'08/03/2026 a 14/03/2026','preco_atual':6.890,
            'semanas':[
                {'ini':'2026-01-05','fim':'2026-01-11','ini_br':'05/01','fim_br':'11/01/2026','label':'Jan/26','preco':6.080},
                {'ini':'2026-01-12','fim':'2026-01-18','ini_br':'12/01','fim_br':'18/01/2026','label':'Jan/26','preco':6.082},
                {'ini':'2026-01-19','fim':'2026-01-25','ini_br':'19/01','fim_br':'25/01/2026','label':'Jan/26','preco':6.084},
                {'ini':'2026-01-26','fim':'2026-02-01','ini_br':'26/01','fim_br':'01/02/2026','label':'Fev/26','preco':6.086},
                {'ini':'2026-02-02','fim':'2026-02-08','ini_br':'02/02','fim_br':'08/02/2026','label':'Fev/26','preco':6.090},
                {'ini':'2026-02-09','fim':'2026-02-15','ini_br':'09/02','fim_br':'15/02/2026','label':'Fev/26','preco':6.095},
                {'ini':'2026-02-16','fim':'2026-02-22','ini_br':'16/02','fim_br':'22/02/2026','label':'Fev/26','preco':6.100},
                {'ini':'2026-03-01','fim':'2026-03-07','ini_br':'01/03','fim_br':'07/03/2026','label':'Mar/26','preco':6.150},
                {'ini':'2026-03-08','fim':'2026-03-14','ini_br':'08/03','fim_br':'14/03/2026','label':'Mar/26','preco':6.890},
            ],
            'regioes':[{'nome':'Norte','emoji':'🟢','preco':7.200},{'nome':'Nordeste','emoji':'🟡','preco':7.070},
                       {'nome':'Centro-Oeste','emoji':'🟠','preco':6.850},{'nome':'Sudeste','emoji':'🔵','preco':6.760},
                       {'nome':'Sul','emoji':'⚪','preco':6.668}],
            'atualizado':'fallback — 14/03/2026'}

# ─────────────────────────────────────────────
# 2. BCB — IPCA e IGP-M
# ─────────────────────────────────────────────

def fetch_ipca():
    print('📥 IPCA (BCB 433)...')
    d = bcb_sgs(433, 15)
    if d:
        series = [{'label':fmt_date(x['data']),'valor':x['valor']} for x in d]
        print(f"  ✅ IPCA: {d[-1]['valor']}% ({fmt_date(d[-1]['data'])})")
        return {'series':series,'ultimo':d[-1]['valor'],'ultimo_label':fmt_date(d[-1]['data']),
                'acum12':round(sum(x['valor'] for x in d[-12:]),2)}
    # Fallback
    print('  ⚠️  IPCA fallback')
    return {'series':[
        {'label':'Mar/25','valor':0.56},{'label':'Abr/25','valor':0.43},{'label':'Mai/25','valor':0.26},
        {'label':'Jun/25','valor':0.24},{'label':'Jul/25','valor':0.26},{'label':'Ago/25','valor':-0.11},
        {'label':'Set/25','valor':0.48},{'label':'Out/25','valor':0.09},{'label':'Nov/25','valor':0.18},
        {'label':'Dez/25','valor':0.33},{'label':'Jan/26','valor':0.33},{'label':'Fev/26','valor':0.70},
    ],'ultimo':0.70,'ultimo_label':'Fev/26','acum12':3.75}

def fetch_igpm():
    print('📥 IGP-M (BCB 189)...')
    d = bcb_sgs(189, 15)
    if d:
        series = [{'label':fmt_date(x['data']),'valor':x['valor']} for x in d]
        print(f"  ✅ IGP-M: {d[-1]['valor']}% ({fmt_date(d[-1]['data'])})")
        return {'series':series,'ultimo':d[-1]['valor'],'ultimo_label':fmt_date(d[-1]['data']),
                'acum12':round(sum(x['valor'] for x in d[-12:]),2)}
    print('  ⚠️  IGP-M fallback')
    return {'series':[
        {'label':'Mar/25','valor':-0.34},{'label':'Abr/25','valor':0.24},{'label':'Mai/25','valor':-0.49},
        {'label':'Jun/25','valor':-1.67},{'label':'Jul/25','valor':-0.77},{'label':'Ago/25','valor':0.36},
        {'label':'Set/25','valor':0.42},{'label':'Out/25','valor':-0.36},{'label':'Nov/25','valor':0.27},
        {'label':'Dez/25','valor':-0.01},{'label':'Jan/26','valor':0.41},{'label':'Fev/26','valor':-0.73},
    ],'ultimo':-0.73,'ultimo_label':'Fev/26','acum12':-2.67}

# ─────────────────────────────────────────────
# 3. SELIC — BCB SGS 432 com fallback robusto
# ─────────────────────────────────────────────

SELIC_FALLBACK_SERIES = [
    {'label':'Mar/25','valor':13.25},{'label':'Abr/25','valor':13.25},{'label':'Mai/25','valor':13.75},
    {'label':'Jun/25','valor':13.75},{'label':'Jul/25','valor':14.25},{'label':'Ago/25','valor':14.25},
    {'label':'Set/25','valor':14.75},{'label':'Out/25','valor':14.75},{'label':'Nov/25','valor':14.75},
    {'label':'Dez/25','valor':14.75},{'label':'Jan/26','valor':14.75},{'label':'Fev/26','valor':14.75},
]

def fetch_selic():
    print('📥 SELIC (BCB 432)...')
    d = bcb_sgs(432, 60)
    if d:
        by_month = {}
        for x in d:
            try:
                p = x['data'].split('/')
                ym = f"{p[2]}-{p[1]}"  # YYYY-MM
                by_month[ym] = x['valor']
            except: pass
        if by_month:
            series = [{'label':fmt_ym(k),'valor':v} for k,v in sorted(by_month.items())][-14:]
            atual  = d[-1]['valor']
            print(f'  ✅ SELIC: {atual}% a.a. | {len(series)} meses')
            return {'series':series,'atual':atual,'ultimo_label':fmt_date(d[-1]['data'])}
    print('  ⚠️  SELIC fallback')
    return {'series':SELIC_FALLBACK_SERIES,'atual':14.75,'ultimo_label':'Fev/26'}

# ─────────────────────────────────────────────
# 4. USD/BRL — múltiplas fontes
# ─────────────────────────────────────────────

USD_FALLBACK = 5.89

def fetch_usd():
    print('📥 USD/BRL...')

    # Fonte 1: open.er-api.com (gratuito, sem chave)
    d = get_json('https://open.er-api.com/v6/latest/USD', 'open.er-api')
    if d and d.get('result') == 'success' and 'rates' in d:
        brl = round(d['rates'].get('BRL', 0), 4)
        if brl > 1:
            print(f'  ✅ USD/BRL open.er-api: R$ {brl}')
            return {'series':[],'atual':brl}

    # Fonte 2: exchangerate-api.com
    d = get_json('https://api.exchangerate-api.com/v4/latest/USD', 'exchangerate-api')
    if d and 'rates' in d:
        brl = round(d['rates'].get('BRL', 0), 4)
        if brl > 1:
            print(f'  ✅ USD/BRL exchangerate-api: R$ {brl}')
            return {'series':[],'atual':brl}

    # Fonte 3: BCB SGS código 1 (PTAX)
    d = bcb_sgs(1, 5)
    if d:
        brl = round(d[-1]['valor'], 4)
        print(f'  ✅ USD/BRL BCB PTAX: R$ {brl}')
        return {'series':[],'atual':brl}

    print(f'  ⚠️  USD/BRL fallback: R$ {USD_FALLBACK}')
    return {'series':[],'atual':USD_FALLBACK}

# ─────────────────────────────────────────────
# 5. INCTL — CNT scraping + fallback
# ─────────────────────────────────────────────

INCTL_FALLBACK_SERIES = [
    {'label':'Mar/25','valor':0.32},{'label':'Abr/25','valor':0.40},{'label':'Mai/25','valor':0.35},
    {'label':'Jun/25','valor':0.18},{'label':'Jul/25','valor':0.25},{'label':'Ago/25','valor':0.05},
    {'label':'Set/25','valor':0.30},{'label':'Out/25','valor':0.45},{'label':'Nov/25','valor':0.32},
    {'label':'Dez/25','valor':0.38},{'label':'Jan/26','valor':0.28},{'label':'Fev/26','valor':0.55},
]

def fetch_inctl():
    print('📥 INCTL (CNT)...')
    try:
        r = requests.get('https://www.cnt.org.br/inctl', headers=HEADERS, timeout=TIMEOUT)
        matches = re.findall(r'([+-]?\d{1,2},\d{2,3})\s*%', r.text)
        nums = [float(m.replace(',','.')) for m in matches if abs(float(m.replace(',','.'))) < 10]
        if nums:
            print(f'  ✅ INCTL CNT: {nums[0]}%')
            return {'ultimo':nums[0],'acum12':round(sum(nums[:12]),2) if len(nums)>=12 else None,
                    'series':INCTL_FALLBACK_SERIES,'fonte':'CNT','atualizado':datetime.utcnow().strftime('%d/%m/%Y')}
    except Exception as e:
        print(f'  ⚠️  INCTL CNT: {e}')
    return {'ultimo':0.55,'acum12':3.85,'series':INCTL_FALLBACK_SERIES,'fonte':'fallback'}

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print(f'\n🚀 {datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC")}\n')
    output = {
        'gerado_em':    datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC'),
        'gerado_em_br': (datetime.utcnow() - timedelta(hours=3)).strftime('%d/%m/%Y %H:%M'),
        'anp':    fetch_anp(),
        'ipca':   fetch_ipca(),
        'igpm':   fetch_igpm(),
        'selic':  fetch_selic(),
        'usd_brl':fetch_usd(),
        'inctl':  fetch_inctl(),
    }
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    size = os.path.getsize(OUTPUT_PATH)
    print(f'\n✅ {OUTPUT_PATH} ({size//1024}KB)')
    print(f"  ANP   : R$ {output['anp']['preco_atual']} | {output['anp']['semana_referencia']}")
    print(f"  IPCA  : {output['ipca']['ultimo']}% | acum12: {output['ipca']['acum12']}%")
    print(f"  IGP-M : {output['igpm']['ultimo']}%")
    print(f"  SELIC : {output['selic']['atual']}% a.a. | {len(output['selic']['series'])} meses")
    print(f"  USD/BRL: R$ {output['usd_brl']['atual']}")
    print(f"  INCTL : {output['inctl']['ultimo']}%")

if __name__ == '__main__':
    main()
