#!/usr/bin/env python3
"""
fetch_data.py — Coleta automática para o Painel de Transportes

Modos:
  --mode=full  → ANP + BCB + Notícias  (segunda-feira)
  --mode=news  → apenas notícias       (demais dias)
  sem argumento → full (compatibilidade)
"""

import json, os, re, io, sys, xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
import requests
import pandas as pd

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'indicators.json')
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

HEADERS = {'User-Agent': 'Mozilla/5.0 (PainelTransportes/1.0; github-actions)'}
TIMEOUT = 20
MESES   = ['','Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']

def fmt_date(s):
    try: p=s.split('/'); return f"{MESES[int(p[1])]}/{p[2][2:]}"
    except: return s

def fmt_ym(ym):
    try: y,m=ym.split('-'); return f"{MESES[int(m)]}/{y[2:]}"
    except: return ym

def get_json(url, label=''):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f'  ⚠️  {label}: {e}'); return None

def bcb_sgs(cod, n=15):
    url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{cod}/dados/ultimos/{n}?formato=json'
    d = get_json(url, f'BCB SGS {cod}')
    if not d: return []
    out = []
    for item in d:
        try:
            v = item.get('valor','').replace(',','.')
            if v and v not in ('-',''): out.append({'data':item['data'],'valor':float(v)})
        except: pass
    return out

# ─────────────────────────────────────────────
# NOTÍCIAS — RSS direto (sem proxy, sem CORS)
# ─────────────────────────────────────────────

NEWS_SOURCES = [
    # ── G1 / Globo (melhor cobertura nacional) ───────────────────────
    ('https://g1.globo.com/rss/g1/brasil/',                        'G1'),
    ('https://g1.globo.com/rss/g1/economia/',                      'G1'),
    ('https://g1.globo.com/rss/g1/economia/negocios/',             'G1'),
    ('https://g1.globo.com/rss/g1/politica/',                      'G1'),
    # ── UOL (alta cobertura de greves e transporte) ──────────────────
    ('https://rss.uol.com.br/feed/noticias.xml',                   'UOL'),
    ('https://www.uol.com.br/rss/economia',                        'UOL Economia'),
    ('https://transportes.uol.com.br/rss.xml',                     'UOL Transportes'),
    # ── Folha / Valor / Estadao ──────────────────────────────────────
    ('https://feeds.folha.uol.com.br/mercado/rss091.xml',          'Folha SP'),
    ('https://feeds.folha.uol.com.br/cotidiano/rss091.xml',        'Folha SP'),
    ('https://feeds.folha.uol.com.br/poder/rss091.xml',            'Folha SP'),
    ('https://valor.globo.com/rss/economia',                       'Valor Economico'),
    ('https://valor.globo.com/rss/empresas',                       'Valor Economico'),
    # ── Agencias e portais ───────────────────────────────────────────
    ('https://agenciabrasil.ebc.com.br/economia/feed',             'Agencia Brasil'),
    ('https://agenciabrasil.ebc.com.br/geral/feed',                'Agencia Brasil'),
    ('https://www.cnnbrasil.com.br/economia/feed/',                 'CNN Brasil'),
    ('https://www.cnnbrasil.com.br/nacional/feed/',                 'CNN Brasil'),
    ('https://noticias.r7.com/rss/brasil.xml',                     'R7'),
    ('https://www.infomoney.com.br/feed/',                         'InfoMoney'),
    # ── Especializados transporte e logistica ────────────────────────
    ('https://www.cnt.org.br/feed',                                'CNT'),
    ('https://www.ntcelogistica.org.br/feed/',                     'NTC'),
    ('https://www.transportabrasil.com.br/feed/',                  'Transporta Brasil'),
    ('https://www.revistalogistica.com.br/feed/',                   'Rev. Logistica'),
    # ── Governo ──────────────────────────────────────────────────────
    ('https://www.gov.br/anp/pt-br/assuntos/noticias/RSS',         'ANP'),
    ('https://www.gov.br/transportes/pt-br/assuntos/noticias/RSS', 'Min. Transportes'),
    # ── Internacional ────────────────────────────────────────────────
    ('https://oilprice.com/rss/main',                              'OilPrice'),
    ('https://feeds.reuters.com/reuters/businessNews',             'Reuters'),
]

KEYWORDS_PRIORITY = re.compile(
    r'greve|paralisa|paralis|parar|parou|protesto|manifesta|bloqueio|lock.?out|'
    r'caminhon|caminhao|caminhoes|carreteiro|motorista.{0,15}camin|'
    r'frete|tabela.{0,10}frete|piso.{0,10}frete|'
    r'diesel|combustivel|combustiveis|gasolina|etanol|gnv|'
    r'antt|transporte rodoviar|rodoviari|'
    r'petrobras|reajuste|alta.{0,15}combus|preco.{0,10}combustiv|'
    r'abcam|fenatran|setcergs|setcesp|fetranspar',
    re.IGNORECASE
)

KEYWORDS_SECONDARY = re.compile(
    r'petr[oe]leo|brent|wti|barril|refinaria|'
    r'opep|opec|iran|saudi|russia|ukraine|'
    r'dolar|cambio|selic|inflacao|ipca|igpm|'
    r'logistica|infraestrutura|rodovia|pedagio|'
    r'oil|crude|fuel|energy|energia',
    re.IGNORECASE
)

def classify_news(title, desc):
    text = title + ' ' + (desc or '')
    if KEYWORDS_PRIORITY.search(text):
        return 1
    if KEYWORDS_SECONDARY.search(text):
        return 2
    return None

def parse_pub(pub):
    if not pub: return ''
    try: return parsedate_to_datetime(pub).strftime('%d/%m/%Y')
    except: return pub[:10] if len(pub) >= 10 else pub


def get_og_image(url):
    if not url or not url.startswith('http'):
        return ''
    try:
        import re as re2
        resp = requests.get(url, headers=HEADERS, timeout=6, stream=True)
        if not resp.ok: return ''
        chunk = b''
        for c in resp.iter_content(8192):
            chunk += c
            if len(chunk) > 12288: break
        text = chunk.decode('utf-8', errors='ignore')
        patterns = [
            re2.compile(r'property=["\']+og:image["\']+[^>]+content=["\'](https?://[^\"\' ]+)["\'\']', re2.I),
            re2.compile(r'content=["\'](https?://[^\"\' ]+)["\']+[^>]+property=["\']+og:image', re2.I),
            re2.compile(r'name=["\']+twitter:image["\']+[^>]+content=["\'](https?://[^\"\' ]+)["\'\']', re2.I),
        ]
        for pat in patterns:
            m = pat.search(text)
            if m:
                img = m.group(1).strip()
                if 5 < len(img) < 400: return img
    except Exception:
        pass
    return ''

def fetch_news():
    print('Clipping de noticias...')
    items = []
    ok_sources = 0

    for url, src in NEWS_SOURCES:
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if not r.ok:
                print(f'  skip {src}: HTTP {r.status_code}')
                continue
            root = ET.fromstring(r.content)
            count = 0
            for item in root.findall('.//item')[:15]:
                def tag(t, item=item):
                    el = item.find(t)
                    return (el.text or '').strip() if el is not None else ''
                title = tag('title')
                link  = tag('link') or tag('guid')
                pub   = tag('pubDate')
                desc  = tag('description') or ''
                if not title or len(title) < 8: continue
                prio = classify_news(title, desc)
                if prio is None: continue
                items.append({
                    'titulo':     title[:160],
                    'link':       link,
                    'data':       parse_pub(pub),
                    'fonte':      src,
                    'prioridade': prio,
                    'imagem':     get_og_image(link),
                })
                count += 1
            if count > 0:
                ok_sources += 1
                print(f'  ok {src}: {count}')
        except Exception as e:
            print(f'  err {src}: {e}')

    # Filter last 15 days
    cutoff_dt = datetime.utcnow() - timedelta(days=15)
    def is_recent(item):
        d = item['data']
        if not d or len(d) < 8: return True
        try:
            p = d.split('/')
            return datetime(int(p[2]), int(p[1]), int(p[0])) >= cutoff_dt
        except: return True
    items = [x for x in items if is_recent(x)]

    # Deduplicate
    seen = set(); deduped = []
    for item in items:
        k = item['titulo'][:55].lower().strip()
        if k not in seen:
            seen.add(k)
            deduped.append(item)

    # Sort: priority 1 first, then date desc
    deduped.sort(key=lambda x: x['data'], reverse=True)
    deduped.sort(key=lambda x: x['prioridade'])

    result = deduped[:30]
    print(f'  Total: {len(result)} noticias | {ok_sources}/{len(NEWS_SOURCES)} fontes')
    return result



# ─────────────────────────────────────────────────────────────────
# NOTÍCIAS ECONOMICS — para KPI's Economics tab
# ─────────────────────────────────────────────────────────────────

ECON_NEWS_SOURCES = [
    # Principais fontes de macro/economia
    ('https://valor.globo.com/rss/financas',                       'Valor Econômico'),
    ('https://valor.globo.com/rss/brasil',                         'Valor Econômico'),
    ('https://www.infomoney.com.br/feed/',                         'InfoMoney'),
    ('https://feeds.folha.uol.com.br/mercado/rss091.xml',          'Folha SP'),
    ('https://agenciabrasil.ebc.com.br/economia/feed',             'Agência Brasil'),
    ('https://g1.globo.com/rss/g1/economia/',                      'G1'),
    ('https://www.cnnbrasil.com.br/economia/feed/',                 'CNN Brasil'),
    ('https://www.uol.com.br/rss/economia',                        'UOL'),
    ('https://feeds.reuters.com/reuters/businessNews',             'Reuters'),
    ('https://exame.com/feed/',                                    'Exame'),
]

ECON_KEYWORDS = re.compile(
    r'ipca|igpm|igp-m|infl[aã]|inpc|deflac|pre[cç]o.{0,10}consumidor|'
    r'selic|copom|juros|taxa.{0,10}bás|banco.{0,10}central|bcb|bacen|politica.{0,10}monetar|'
    r'd[oó]lar|câmbio|cambio|real.{0,8}desvalori|real.{0,8}valori|brl|ptax|'
    r'pib|produto.{0,10}interno|crescimento.{0,10}econom|desemprego|caged|pnad|'
    r'dficit|superávit|superavit|fiscal|arc.{0,6}fiscal|previdência|imposto|arrecad|tesouro|'
    r'spread|cds|risco.{0,10}brasil|embi|'
    r'commodit|petroleo|barril|soja|milho|minério',
    re.IGNORECASE
)

def fetch_econ_news():
    print('📥 Notícias Economics...')
    items = []
    ok_sources = 0
    for url, src in ECON_NEWS_SOURCES:
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if not r.ok: continue
            root = ET.fromstring(r.content)
            count = 0
            for item in root.findall('.//item')[:12]:
                def tag(t, item=item):
                    el = item.find(t)
                    return (el.text or '').strip() if el is not None else ''
                title = tag('title'); link = tag('link') or tag('guid')
                pub = tag('pubDate'); desc = tag('description') or ''
                if not title or len(title) < 8: continue
                if not ECON_KEYWORDS.search(title + ' ' + desc): continue
                items.append({
                    'titulo':     title[:160],
                    'link':       link,
                    'data':       parse_pub(pub),
                    'fonte':      src,
                    'prioridade': 1,
                    'imagem':     get_og_image(link),
                })
                count += 1
            if count > 0:
                ok_sources += 1
                print(f'  ✅ {src}: {count}')
        except Exception as e:
            print(f'  ⚠️  {src}: {e}')

    # Filter 15 days, dedup, sort
    cutoff_dt = datetime.utcnow() - timedelta(days=15)
    def is_recent(item):
        d = item['data']
        if not d or len(d) < 8: return True
        try:
            p = d.split('/')
            return datetime(int(p[2]),int(p[1]),int(p[0])) >= cutoff_dt
        except: return True
    items = [x for x in items if is_recent(x)]
    seen = set(); deduped = []
    for item in items:
        k = item['titulo'][:55].lower().strip()
        if k not in seen: seen.add(k); deduped.append(item)
    deduped.sort(key=lambda x: x['data'], reverse=True)
    result = deduped[:25]
    print(f'  📰 Economics: {len(result)} notícias | {ok_sources}/{len(ECON_NEWS_SOURCES)} fontes')
    return result


# ─────────────────────────────────────────────
# ANP — DIESEL S10
# ─────────────────────────────────────────────

def fetch_anp():
    print('📥 ANP...')
    # 2026 first, then 2025 fallbacks
    urls = [
        # ── 2026 (formato novo com semanas) ──────────────────────────
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2026/ca-2026.csv',
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2026/ca-2026-01.csv',
        # ── 2025 fallback ────────────────────────────────────────────
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2025/ca-2025.csv',
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2025/ca-2025-01.csv',
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2025/ca-2025-02.csv',
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code==200 and len(r.content)>1000:
                df = pd.read_csv(io.StringIO(r.content.decode('latin-1')),sep=';',decimal=',',low_memory=False)
                print(f'  ✅ {url.split("/")[-1]} ({len(df)} linhas)')
                return _anp_process(df)
        except Exception as e:
            print(f'  ⚠️  ANP: {e}')
    return _anp_fallback()

def _anp_process(df):
    try:
        df.columns = [c.strip().upper() for c in df.columns]
        prod  = next((c for c in df.columns if 'PRODUTO' in c),None)
        preco = next((c for c in df.columns if 'REVENDA' in c and 'PRECO' in c),None)
        data  = next((c for c in df.columns if 'DATA' in c and 'INICIAL' in c),None)
        regiao= next((c for c in df.columns if 'REGIAO' in c),None)
        if not all([prod,preco,data]): return _anp_fallback()
        diesel = df[df[prod].str.contains('S10|DIESEL S-10',case=False,na=False)].copy()
        if diesel.empty: return _anp_fallback()
        diesel[preco] = pd.to_numeric(diesel[preco],errors='coerce')
        diesel[data]  = pd.to_datetime(diesel[data],dayfirst=True,errors='coerce')
        nac = diesel.groupby(data)[preco].mean().reset_index().sort_values(data).tail(16)
        semanas = []
        for _,row in nac.iterrows():
            dt=row[data]; dt2=dt+timedelta(days=6)
            semanas.append({'ini':dt.strftime('%Y-%m-%d'),'fim':dt2.strftime('%Y-%m-%d'),
                            'ini_br':dt.strftime('%d/%m'),'fim_br':dt2.strftime('%d/%m/%Y'),
                            'label':fmt_date(dt.strftime('%d/%m/%Y')),'preco':round(float(row[preco]),3)})
        ult = semanas[-1] if semanas else {}
        return {'semana_referencia':f"{ult.get('ini_br','')} a {ult.get('fim_br','')}",
                'preco_atual':ult.get('preco',6.89),'semanas':semanas,
                'regioes':_anp_regioes(diesel,data,preco,regiao),
                'atualizado':datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC')}
    except Exception as e:
        print(f'  ⚠️  ANP process: {e}'); return _anp_fallback()

def _anp_regioes(df,dc,pc,rc):
    mapa={'NORTE':('Norte','🟢'),'NORDESTE':('Nordeste','🟡'),
          'CENTRO':('Centro-Oeste','🟠'),'SUDESTE':('Sudeste','🔵'),'SUL':('Sul','⚪')}
    out=[]
    if rc and rc in df.columns:
        ult=df[df[dc]==df[dc].max()]
        for k,(n,e) in mapa.items():
            sub=ult[ult[rc].str.upper().str.contains(k,na=False)]
            if not sub.empty: out.append({'nome':n,'emoji':e,'preco':round(float(sub[pc].mean()),3)})
    if not out:
        ref=6.89
        out=[{'nome':'Norte','emoji':'🟢','preco':round(ref+0.31,3)},
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
# BCB — IPCA, IGP-M, SELIC
# ─────────────────────────────────────────────

def fetch_ipca():
    print('📥 IPCA...')
    d = bcb_sgs(433,15)
    if d:
        series=[{'label':fmt_date(x['data']),'valor':x['valor']} for x in d]
        print(f"  ✅ {d[-1]['valor']}% ({fmt_date(d[-1]['data'])})")
        return {'series':series,'ultimo':d[-1]['valor'],'ultimo_label':fmt_date(d[-1]['data']),'acum12':round(sum(x['valor'] for x in d[-12:]),2)}
    return {'series':[
        {'label':'Mar/25','valor':0.56},{'label':'Abr/25','valor':0.43},{'label':'Mai/25','valor':0.26},
        {'label':'Jun/25','valor':0.24},{'label':'Jul/25','valor':0.26},{'label':'Ago/25','valor':-0.11},
        {'label':'Set/25','valor':0.48},{'label':'Out/25','valor':0.09},{'label':'Nov/25','valor':0.18},
        {'label':'Dez/25','valor':0.33},{'label':'Jan/26','valor':0.33},{'label':'Fev/26','valor':0.70},
    ],'ultimo':0.70,'ultimo_label':'Fev/26','acum12':3.75}

def fetch_igpm():
    print('📥 IGP-M...')
    d = bcb_sgs(189,15)
    if d:
        series=[{'label':fmt_date(x['data']),'valor':x['valor']} for x in d]
        print(f"  ✅ {d[-1]['valor']}% ({fmt_date(d[-1]['data'])})")
        return {'series':series,'ultimo':d[-1]['valor'],'ultimo_label':fmt_date(d[-1]['data']),'acum12':round(sum(x['valor'] for x in d[-12:]),2)}
    return {'series':[
        {'label':'Mar/25','valor':-0.34},{'label':'Abr/25','valor':0.24},{'label':'Mai/25','valor':-0.49},
        {'label':'Jun/25','valor':-1.67},{'label':'Jul/25','valor':-0.77},{'label':'Ago/25','valor':0.36},
        {'label':'Set/25','valor':0.42},{'label':'Out/25','valor':-0.36},{'label':'Nov/25','valor':0.27},
        {'label':'Dez/25','valor':-0.01},{'label':'Jan/26','valor':0.41},{'label':'Fev/26','valor':-0.73},
    ],'ultimo':-0.73,'ultimo_label':'Fev/26','acum12':-2.67}

def fetch_selic():
    print('📥 SELIC...')
    d = bcb_sgs(432,60)
    if d:
        by_month={}
        for x in d:
            try:
                p=x['data'].split('/')
                ym=f"{p[2]}-{p[1]}"
                by_month[ym]=x['valor']
            except: pass
        if by_month:
            series=[{'label':fmt_ym(k),'valor':v} for k,v in sorted(by_month.items())][-14:]
            print(f"  ✅ {d[-1]['valor']}% a.a.")
            return {'series':series,'atual':d[-1]['valor'],'ultimo_label':fmt_date(d[-1]['data'])}
    return {'series':[
        {'label':'Mar/25','valor':13.25},{'label':'Abr/25','valor':13.25},{'label':'Mai/25','valor':13.75},
        {'label':'Jun/25','valor':13.75},{'label':'Jul/25','valor':14.25},{'label':'Ago/25','valor':14.25},
        {'label':'Set/25','valor':14.75},{'label':'Out/25','valor':14.75},{'label':'Nov/25','valor':14.75},
        {'label':'Dez/25','valor':14.75},{'label':'Jan/26','valor':14.75},{'label':'Fev/26','valor':14.75},
    ],'atual':14.75,'ultimo_label':'Fev/26'}

def fetch_usd():
    print('📥 USD/BRL...')
    for url, label in [
        ('https://open.er-api.com/v6/latest/USD','open.er-api'),
        ('https://api.exchangerate-api.com/v4/latest/USD','exchangerate-api'),
    ]:
        d = get_json(url, label)
        if d and 'rates' in d:
            brl = round(d['rates'].get('BRL',0),4)
            if brl > 1:
                print(f'  ✅ R$ {brl}')
                return {'series':[],'atual':brl}
    d = bcb_sgs(1,5)
    if d:
        brl=round(d[-1]['valor'],4)
        print(f'  ✅ BCB PTAX: R$ {brl}')
        return {'series':[],'atual':brl}
    return {'series':[],'atual':5.89}

def fetch_inctl():
    print('📥 INCTL...')
    try:
        r = requests.get('https://www.cnt.org.br/inctl',headers=HEADERS,timeout=TIMEOUT)
        matches = re.findall(r'([+-]?\d{1,2},\d{2,3})\s*%',r.text)
        nums = [float(m.replace(',','.')) for m in matches if abs(float(m.replace(',','.'))) < 10]
        if nums:
            print(f'  ✅ {nums[0]}%')
            return {'ultimo':nums[0],'series':_inctl_fallback_series(),'fonte':'CNT','atualizado':datetime.utcnow().strftime('%d/%m/%Y')}
    except Exception as e:
        print(f'  ⚠️  INCTL: {e}')
    return {'ultimo':0.55,'acum12':3.85,'series':_inctl_fallback_series(),'fonte':'fallback'}

def _inctl_fallback_series():
    return [
        {'label':'Mar/25','valor':0.32},{'label':'Abr/25','valor':0.40},{'label':'Mai/25','valor':0.35},
        {'label':'Jun/25','valor':0.18},{'label':'Jul/25','valor':0.25},{'label':'Ago/25','valor':0.05},
        {'label':'Set/25','valor':0.30},{'label':'Out/25','valor':0.45},{'label':'Nov/25','valor':0.32},
        {'label':'Dez/25','valor':0.38},{'label':'Jan/26','valor':0.28},{'label':'Fev/26','valor':0.55},
    ]

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    mode = 'full'
    for arg in sys.argv[1:]:
        if arg.startswith('--mode='): mode = arg.split('=')[1]

    now_utc = datetime.utcnow()
    print(f'\n🚀 fetch_data.py [{mode.upper()}] — {now_utc.strftime("%d/%m/%Y %H:%M UTC")}\n')

    # Load existing data to preserve fields not being updated
    existing = {}
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH,'r',encoding='utf-8') as f:
                existing = json.load(f)
        except: pass

    if mode == 'news':
        # ── Modo NOTÍCIAS: só atualiza os feeds de notícias ──────────
        print('📰 Modo: apenas notícias')
        existing['noticias']       = fetch_news()
        existing['econ_noticias']  = fetch_econ_news()
        existing['noticias_em']    = now_utc.strftime('%d/%m/%Y %H:%M UTC')
        existing['noticias_em_br'] = (now_utc - timedelta(hours=3)).strftime('%d/%m/%Y %H:%M')
        output = existing

    elif mode == 'indicators':
        # ── Modo INDICADORES: BCB + INCTL + notícias, preserva ANP ───
        print('📊 Modo: indicadores econômicos (BCB) + notícias')
        existing['ipca']          = fetch_ipca()
        existing['igpm']          = fetch_igpm()
        existing['selic']         = fetch_selic()
        existing['usd_brl']       = fetch_usd()
        existing['inctl']         = fetch_inctl()
        existing['noticias']      = fetch_news()
        existing['econ_noticias'] = fetch_econ_news()
        existing['indicadores_em']    = now_utc.strftime('%d/%m/%Y %H:%M UTC')
        existing['indicadores_em_br'] = (now_utc - timedelta(hours=3)).strftime('%d/%m/%Y %H:%M')
        existing['noticias_em']    = now_utc.strftime('%d/%m/%Y %H:%M UTC')
        existing['noticias_em_br'] = (now_utc - timedelta(hours=3)).strftime('%d/%m/%Y %H:%M')
        output = existing

    else:
        # ── Modo FULL: tudo (ANP + BCB + notícias) ───────────────────
        print('🔄 Modo: completo (ANP + BCB + notícias)')
        output = {
            'gerado_em':    now_utc.strftime('%d/%m/%Y %H:%M UTC'),
            'gerado_em_br': (now_utc - timedelta(hours=3)).strftime('%d/%m/%Y %H:%M'),
            'anp':    fetch_anp(),
            'ipca':   fetch_ipca(),
            'igpm':   fetch_igpm(),
            'selic':  fetch_selic(),
            'usd_brl':fetch_usd(),
            'inctl':  fetch_inctl(),
            'noticias':           fetch_news(),
            'econ_noticias':      fetch_econ_news(),
            'noticias_em':        now_utc.strftime('%d/%m/%Y %H:%M UTC'),
            'noticias_em_br':     (now_utc - timedelta(hours=3)).strftime('%d/%m/%Y %H:%M'),
            'indicadores_em':     now_utc.strftime('%d/%m/%Y %H:%M UTC'),
            'indicadores_em_br':  (now_utc - timedelta(hours=3)).strftime('%d/%m/%Y %H:%M'),
        }

    with open(OUTPUT_PATH,'w',encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    size = os.path.getsize(OUTPUT_PATH)
    print(f'\n✅ Salvo: {size//1024}KB')
    if mode == 'news':
        print(f"  📰 Notícias: {len(output.get('noticias',[]))} itens")
    if output.get('indicadores_em_br'):
        print(f"  📊 Indicadores atualizados: {output['indicadores_em_br']}")
    else:
        print(f"  ANP:     R$ {output['anp']['preco_atual']}")
        print(f"  IPCA:    {output['ipca']['ultimo']}%")
        print(f"  SELIC:   {output['selic']['atual']}% a.a.")
        print(f"  USD/BRL: R$ {output['usd_brl']['atual']}")
        print(f"  📰 Notícias: {len(output.get('noticias',[]))} itens")
    if output.get('indicadores_em_br'):
        print(f"  📊 Indicadores atualizados: {output['indicadores_em_br']}")

if __name__ == '__main__':
    main()
