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
    # ── Portais financeiros e negócios ───────────────────────────────
    ('https://www.infomoney.com.br/mercados/feed/',                 'InfoMoney'),
    ('https://moneytimes.com.br/feed/',                            'Money Times'),
    ('https://exame.com/feed/',                                    'Exame'),
    ('https://exame.com/negocios/feed/',                           'Exame Negócios'),
    ('https://www.metropoles.com/feed',                            'Metrópoles'),
    ('https://www.metropoles.com/brasil/feed',                     'Metrópoles'),
    ('https://feeds.folha.uol.com.br/brasil/rss091.xml',           'Folha SP'),
    ('https://valor.globo.com/rss/financas',                       'Valor Economico'),
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
    ('https://moneytimes.com.br/feed/',                            'Money Times'),
    ('https://www.metropoles.com/economia/feed',                   'Metrópoles'),
    ('https://valor.globo.com/rss/financas',                       'Valor Economico'),
    ('https://feeds.folha.uol.com.br/mercado/rss091.xml',          'Folha SP'),
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

def _anp_fetch_latest_weekly():
    """
    Constrói a URL do xlsx semanal da ANP baseada nas datas.
    Padrão confirmado: /arquivos-lpc/{ano}/resumo_semanal_lpc_{YYYY-MM-DD}_{YYYY-MM-DD}.xlsx
    Semana ANP: domingo a sábado. Publicado na semana seguinte.
    Tenta as últimas 3 semanas para garantir disponibilidade.
    """
    from datetime import datetime, timedelta

    today    = datetime.now()
    weekday  = today.weekday()                    # Mon=0 ... Sun=6
    # Último domingo (início de semana ANP)
    days_since_sun = (weekday + 1) % 7
    last_sun = today - timedelta(days=days_since_sun)

    BASE = (
        'https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia'
        '/precos/arquivos-lpc/{year}/resumo_semanal_lpc_{start}_{end}.xlsx'
    )

    # Tenta as últimas 3 semanas publicadas (mais recente primeiro)
    for weeks_back in range(1, 4):
        sun = last_sun - timedelta(weeks=weeks_back)
        sat = sun + timedelta(days=6)
        url = BASE.format(
            year  = sun.year,
            start = sun.strftime('%Y-%m-%d'),
            end   = sat.strftime('%Y-%m-%d'),
        )
        print(f'  📅 Tentando semana {sun.strftime("%d/%m")} a {sat.strftime("%d/%m/%Y")}: {url[-60:]}')
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.ok and len(r.content) > 5000 and r.content[:2] == b'PK':
                print(f'  ✅ xlsx semanal baixado: {len(r.content)//1024}KB')
                return r.content
            else:
                print(f'  ↩️  {r.status_code} — tentando semana anterior')
        except Exception as e:
            print(f'  ⚠️  {e}')

    print('  ℹ️  Nenhuma semana recente disponível via URL direta')
    return None


def fetch_anp():
    print('📥 ANP — tentando semana mais recente primeiro...')

    # ── Fonte 1: xlsx da semana atual (página de últimas semanas) ─────
    # Publicado no mesmo dia da coleta — mais atualizado
    weekly_content = _anp_fetch_latest_weekly()
    if weekly_content:
        result = _anp_process_xlsx(weekly_content)
        if result and result.get('semanas'):
            print(f'  ✅ Dados da semana atual carregados')
            return result

    print('  ↩️  Fallback: xlsx consolidado desde 2013')

    # ── Fonte 2: xlsx consolidado (semanal-brasil-desde-2013.xlsx) ────
    XLSX_URL = (
        'https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia'
        '/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/semanal'
        '/semanal-brasil-desde-2013.xlsx'
    )

    try:
        r = requests.get(XLSX_URL, headers=HEADERS, timeout=60)
        if r.ok and len(r.content) > 10000:
            print(f'  ✅ xlsx consolidado: {len(r.content)//1024}KB')
            return _anp_process_xlsx(r.content)
        else:
            print(f'  ⚠️  xlsx consolidado status: {r.status_code}')
    except Exception as e:
        print(f'  ⚠️  xlsx consolidado: {e}')

    # ── Fallback: CSVs anuais ──────────────────────────────────────────
    csv_urls = [
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2026/ca-2026.csv',
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2025/ca-2025.csv',
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2025/ca-2025-01.csv',
    ]
    for url in csv_urls:
        try:
            r2 = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r2.ok and len(r2.content) > 1000:
                df = pd.read_csv(io.StringIO(r2.content.decode('latin-1')), sep=';', decimal=',', low_memory=False)
                print(f'  ✅ csv: {url.split("/")[-1]} ({len(df)} linhas)')
                return _anp_process(df)
        except Exception as e:
            print(f'  ⚠️  {url.split("/")[-1]}: {e}')

    return _anp_fallback()


def _anp_process_xlsx(content):
    """Processa o xlsx semanal consolidado da ANP para TODOS os combustíveis."""
    try:
        import io as _io
        xf = pd.ExcelFile(_io.BytesIO(content))
        print(f'  📋 Abas: {xf.sheet_names[:10]}')

        # Mapa de combustíveis: chave no xlsx → chave no painel
        FUEL_MAP = {
            'OLEO DIESEL S10':    'diesel-s10',
            'DIESEL S10':         'diesel-s10',
            'OLEO DIESEL S500':   'diesel-s500',
            'DIESEL S500':        'diesel-s500',
            'DIESEL':             'diesel-s500',
            'GASOLINA COMUM':     'gasolina',
            'GASOLINA':           'gasolina',
            'GASOLINA ADITIVADA': 'gasolina-ad',
            'ETANOL HIDRATADO':   'etanol',
            'ETANOL':             'etanol',
            'GNV':                'gnv',
        }

        # Encontra aba principal (Semanal / Brasil / geral)
        target = None
        for sh in xf.sheet_names:
            su = sh.upper()
            if any(k in su for k in ['SEMANAL', 'BRASIL', 'REVENDA', 'NACIONAL']):
                target = sh
                break
        if target is None:
            # pega a maior aba (mais dados)
            sizes = {}
            for sh in xf.sheet_names[:6]:
                try:
                    df_tmp = pd.read_excel(_io.BytesIO(content), sheet_name=sh, nrows=3)
                    sizes[sh] = len(df_tmp.columns)
                except: pass
            target = max(sizes, key=sizes.get) if sizes else xf.sheet_names[0]
        print(f'  📄 Aba: {target}')

        # Lê a aba com múltiplos cabeçalhos possíveis
        df_raw = pd.read_excel(_io.BytesIO(content), sheet_name=target, header=None)

        # Detecta linha de cabeçalho
        header_row = 0
        for i, row in df_raw.iterrows():
            vals = [str(v).upper() for v in row.values if pd.notna(v)]
            if sum(1 for v in vals if any(k in v for k in ['DATA','PREÇO','PRECO','PRODUTO','SEMANA'])) >= 2:
                header_row = i
                break

        df_raw.columns = [str(c).strip().upper() if pd.notna(c) else f'_COL{i}'
                          for i,c in enumerate(df_raw.iloc[header_row])]
        df = df_raw.iloc[header_row+1:].reset_index(drop=True)
        print(f'  🔑 Colunas: {list(df.columns[:12])}')

        # Detecta colunas
        col_ini  = next((c for c in df.columns if 'INIC' in c and 'DATA' in c), None) or                    next((c for c in df.columns if c in ['DATA INICIAL','DATA_INICIAL','DATA INIC']), None) or                    next((c for c in df.columns if 'DATA' in c), None)
        col_prec = next((c for c in df.columns if 'MÉDIO' in c or 'MEDIO' in c), None) or                    next((c for c in df.columns if 'PREÇO' in c or 'PRECO' in c), None) or                    next((c for c in df.columns if 'REVENDA' in c), None)
        col_prod = next((c for c in df.columns if 'PRODUTO' in c), None)
        col_reg  = next((c for c in df.columns if 'REGIAO' in c or 'REGIÃO' in c), None)
        # ANP columns: "PREÇO MÍNIMO REVENDA", "PREÇO MÁXIMO REVENDA"
        # or "PRECO MINIMO REVENDA", "PRECO MAXIMO REVENDA"
        col_min = None
        col_max = None
        for c in df.columns:
            cu = c.upper()
            if col_min is None and ('MIN' in cu or 'MÍN' in cu):
                col_min = c
            if col_max is None and ('MAX' in cu or 'MÁX' in cu):
                col_max = c
        # Also log all columns for debugging
        print(f'  📊 Todas as colunas: {list(df.columns[:15])}')

        print(f'  🔍 ini={col_ini} | prec={col_prec} | min={col_min} | max={col_max} | prod={col_prod} | reg={col_reg}')
        if not col_ini or not col_prec:
            return _anp_process_xlsx_positional(df)

        df[col_prec] = pd.to_numeric(df[col_prec], errors='coerce')
        df[col_ini]  = pd.to_datetime(df[col_ini], dayfirst=True, errors='coerce')
        df = df.dropna(subset=[col_prec, col_ini])
        # Strip tz if present — xlsx may return tz-aware dates
        try:
            if df[col_ini].dt.tz is not None:
                df[col_ini] = df[col_ini].dt.tz_localize(None)
        except Exception: pass
        df = df[df[col_prec] > 0.5]

        # Exclui semana corrente
        today      = pd.Timestamp.now().normalize()  # tz-naive to match xlsx dates
        week_start = today - pd.Timedelta(days=today.dayofweek)
        try:
            df = df[df[col_ini] < week_start]
        except TypeError:
            df = df[df[col_ini].astype(str) < week_start.strftime('%Y-%m-%d')]

        # Filtra Nacional
        if col_reg:
            mask_nac = df[col_reg].astype(str).str.upper().isin(['BRASIL','NACIONAL','NAN',''])
            df_nac = df[mask_nac] if not df[mask_nac].empty else df
        else:
            df_nac = df

        def build_semanas(df_fuel, n=20):
            """Constrói lista de semanas — inclui min/max reais da ANP."""
            # Aba BRASIL: cada linha já é o agregado semanal nacional
            # Não re-agregar min/max pois isso pegaria extremos históricos
            # Pega a média por semana (caso haja duplicatas por produto)
            grp = df_fuel.groupby(col_ini).first().reset_index().sort_values(col_ini).tail(n)
            # Recalcula coluna de preço médio corretamente
            grp_avg = df_fuel.groupby(col_ini)[col_prec].mean().reset_index()
            grp_avg.columns = [col_ini, '_avg']
            grp = grp.merge(grp_avg, on=col_ini, how='left')

            semanas = []
            for _, row in grp.iterrows():
                dt  = row[col_ini]
                dt2 = dt + pd.Timedelta(days=6)
                avg = round(float(row['_avg']), 3)

                # Usa min/max da MESMA linha (já são valores da semana)
                vmin = avg - 0.34  # fallback
                vmax = avg + 0.34  # fallback
                if col_min and col_min in row.index:
                    v = pd.to_numeric(row[col_min], errors='coerce')
                    # Validação: min deve estar entre avg-2 e avg
                    if not pd.isna(v) and avg - 2.0 <= v <= avg:
                        vmin = round(float(v), 3)
                if col_max and col_max in row.index:
                    v = pd.to_numeric(row[col_max], errors='coerce')
                    # Validação: max deve estar entre avg e avg+2
                    if not pd.isna(v) and avg <= v <= avg + 2.0:
                        vmax = round(float(v), 3)

                semanas.append({
                    'ini':       dt.strftime('%Y-%m-%d'),
                    'fim':       dt2.strftime('%Y-%m-%d'),
                    'ini_br':    dt.strftime('%d/%m'),
                    'fim_br':    dt2.strftime('%d/%m/%Y'),
                    'label':     dt.strftime('%d/%m/%y'),
                    'preco':     avg,
                    'preco_min': round(vmin, 3),
                    'preco_max': round(vmax, 3)
                })
            return semanas

        # ── Diesel S10 (combustível principal) ──────────────────────
        fuels_result = {}

        if col_prod:
            for fuel_key_upper, panel_key in FUEL_MAP.items():
                if panel_key in fuels_result: continue
                mask = df_nac[col_prod].astype(str).str.upper().str.contains(fuel_key_upper, na=False)
                df_fuel = df_nac[mask]
                if not df_fuel.empty:
                    semanas = build_semanas(df_fuel)
                    if semanas:
                        fuels_result[panel_key] = semanas
                        print(f'  🛢️  {panel_key}: {len(semanas)} semanas | último R$ {semanas[-1]["preco"]}')
        else:
            # Sem coluna produto — assume que toda a aba é Diesel S10
            semanas = build_semanas(df_nac)
            if semanas:
                fuels_result['diesel-s10'] = semanas

        if not fuels_result:
            return _anp_fallback()

        # ── Monta resultado principal (Diesel S10) ──────────────────
        s10 = fuels_result.get('diesel-s10', list(fuels_result.values())[0])
        ult  = s10[-1]

        # Busca preços regionais reais
        regioes_reais = fetch_anp_regioes()
        if not regioes_reais:
            regioes_reais = _anp_regioes_xlsx(content, ult['preco'])

        # Busca preços por UF reais
        estados_reais = fetch_anp_estados()

        return {
            'semana_referencia':     f'{ult["ini_br"]} a {ult["fim_br"]}',
            'preco_atual':           ult['preco'],
            'semanas':               s10,
            'semanas_por_combustivel': fuels_result,
            'regioes':               regioes_reais,
            'estados':               estados_reais,
            'atualizado':            datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC')
        }

    except Exception as e:
        print(f'  ⚠️  xlsx process: {e}')
        import traceback; traceback.print_exc()
        return _anp_fallback()


def _anp_process_xlsx_positional(df):
    """Fallback posicional caso colunas não sejam detectadas."""
    try:
        # Tenta detectar semanas por formato de data nas primeiras colunas
        for col in df.columns[:5]:
            sample = df[col].dropna().head(10)
            dates = pd.to_datetime(sample, dayfirst=True, errors='coerce').dropna()
            if len(dates) > 3:
                col_ini = col
                break
        else:
            return _anp_fallback()

        # Pega a primeira coluna numérica como preço
        for col in df.columns:
            if col == col_ini: continue
            nums = pd.to_numeric(df[col], errors='coerce').dropna()
            if len(nums) > 5 and 3 < nums.mean() < 15:
                col_prec = col
                break
        else:
            return _anp_fallback()

        df[col_prec] = pd.to_numeric(df[col_prec], errors='coerce')
        df[col_ini]  = pd.to_datetime(df[col_ini], dayfirst=True, errors='coerce')
        if hasattr(df[col_ini], 'dt') and df[col_ini].dt.tz is not None:
            df[col_ini] = df[col_ini].dt.tz_localize(None)
        df = df.dropna(subset=[col_prec, col_ini])
        df = df[df[col_prec] > 3]
        df = df.sort_values(col_ini).tail(20)

        semanas = []
        for _, row in df.iterrows():
            dt = row[col_ini]; dt2 = dt + pd.Timedelta(days=6)
            semanas.append({'ini': dt.strftime('%Y-%m-%d'), 'fim': dt2.strftime('%Y-%m-%d'),
                            'ini_br': dt.strftime('%d/%m'), 'fim_br': dt2.strftime('%d/%m/%Y'),
                            'label': dt.strftime('%d/%m/%y'), 'preco': round(float(row[col_prec]), 3)})

        ult = semanas[-1]
        return {'semana_referencia': f'{ult["ini_br"]} a {ult["fim_br"]}',
                'preco_atual': ult['preco'], 'semanas': semanas,
                'regioes': [], 'atualizado': datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC')}
    except Exception as e:
        print(f'  ⚠️  posicional: {e}')
        return _anp_fallback()


def fetch_anp_regioes():
    """Baixa e processa o xlsx regional da ANP."""
    # Tenta o xlsx mais recente da semana via scraping da página
    XLSX_REGIOES = (
        'https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia'
        '/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/semanal'
        '/semanal-regioes-desde-2013.xlsx'
    )
    # Primeira tentativa: scraping de última semana
    weekly = _anp_fetch_latest_weekly()
    if weekly:
        result = _anp_regioes_process(weekly)
        if result:
            return result
    # Fallback: consolidado
    try:
        r = requests.get(XLSX_REGIOES, headers=HEADERS, timeout=60)
        if r.ok and len(r.content) > 5000:
            print(f'  ✅ xlsx regiões consolidado: {len(r.content)//1024}KB')
            return _anp_regioes_process(r.content)
        else:
            print(f'  ⚠️  xlsx regiões status: {r.status_code}')
    except Exception as e:
        print(f'  ⚠️  xlsx regiões: {e}')
    return []


def _anp_regioes_process(content):
    """Processa o xlsx regional da ANP — extrai última semana por região."""
    try:
        import io as _io
        MAPA = {
            'NORTE':        ('Norte',        '🟢'),
            'NORDESTE':     ('Nordeste',      '🟡'),
            'CENTRO-OESTE': ('Centro-Oeste',  '🟠'),
            'CENTRO OESTE': ('Centro-Oeste',  '🟠'),
            'SUDESTE':      ('Sudeste',       '🔵'),
            'SUL':          ('Sul',           '⚪'),
        }

        xf = pd.ExcelFile(_io.BytesIO(content))
        print(f'  📋 Abas regiões: {xf.sheet_names[:8]}')

        # Procura aba com dados de revenda / diesel
        target = None
        for sh in xf.sheet_names:
            su = sh.upper()
            if any(k in su for k in ['DIESEL', 'S10', 'REVENDA', 'SEMANAL']):
                target = sh; break
        if target is None:
            target = xf.sheet_names[0]
        print(f'  📄 Aba regiões: {target}')

        df_raw = pd.read_excel(_io.BytesIO(content), sheet_name=target, header=None)

        # Detecta linha de cabeçalho
        header_row = 0
        for i, row in df_raw.iterrows():
            vals = [str(v).upper() for v in row.values if pd.notna(v)]
            if sum(1 for v in vals if any(k in v for k in ['DATA','PREÇO','PRECO','REGIAO','REGIÃO'])) >= 2:
                header_row = i; break

        df_raw.columns = [str(c).strip().upper() if pd.notna(c) else f'_C{i}'
                          for i, c in enumerate(df_raw.iloc[header_row])]
        df = df_raw.iloc[header_row+1:].reset_index(drop=True)
        print(f'  🔑 Colunas regiões: {list(df.columns[:10])}')

        # Detecta colunas
        col_ini  = next((c for c in df.columns if 'INIC' in c and 'DATA' in c), None) or                    next((c for c in df.columns if 'DATA' in c), None)
        col_prec = next((c for c in df.columns if 'MÉDIO' in c or 'MEDIO' in c), None) or                    next((c for c in df.columns if 'PREÇO' in c or 'PRECO' in c), None)
        col_reg  = next((c for c in df.columns if 'REGIAO' in c or 'REGIÃO' in c or 'REGION' in c), None)
        col_prod = next((c for c in df.columns if 'PRODUTO' in c), None)

        print(f'  🔍 ini={col_ini} | prec={col_prec} | reg={col_reg} | prod={col_prod}')
        if not col_ini or not col_prec or not col_reg:
            print('  ⚠️  colunas insuficientes para regiões')
            return []

        df[col_prec] = pd.to_numeric(df[col_prec], errors='coerce')
        df[col_ini]  = pd.to_datetime(df[col_ini], dayfirst=True, errors='coerce')
        if hasattr(df[col_ini], 'dt') and df[col_ini].dt.tz is not None:
            df[col_ini] = df[col_ini].dt.tz_localize(None)
        df = df.dropna(subset=[col_prec, col_ini])
        df = df[df[col_prec] > 3]

        # Filtra Diesel S10
        if col_prod:
            mask_s10 = df[col_prod].astype(str).str.upper().str.contains('S10|S-10', na=False)
            df_s10 = df[mask_s10] if not df[mask_s10].empty else df
        else:
            df_s10 = df

        # Exclui semana corrente
        today      = pd.Timestamp.now().normalize()  # tz-naive to match xlsx dates
        week_start = today - pd.Timedelta(days=today.dayofweek)
        df_s10 = df_s10[df_s10[col_ini] < week_start]

        # Pega última semana disponível
        ultima_data = df_s10[col_ini].max()
        df_ult = df_s10[df_s10[col_ini] == ultima_data]
        print(f'  📅 Última semana regiões: {ultima_data.strftime("%d/%m/%Y")} | {len(df_ult)} regiões')

        # Oct/25 reference
        oct_data = pd.Timestamp('2025-10-06')
        df_oct = df_s10[df_s10[col_ini] <= oct_data].sort_values(col_ini)
        df_oct_ref = df_s10[abs((df_s10[col_ini] - oct_data).dt.days) <= 14]

        regioes = []
        for reg_key, (reg_nome, emoji) in MAPA.items():
            mask = df_ult[col_reg].astype(str).str.upper().str.contains(reg_key, na=False)
            linhas = df_ult[mask]
            if linhas.empty:
                continue
            preco = round(float(linhas[col_prec].mean()), 3)

            # Oct/25 reference for this region
            mask_oct = df_oct_ref[col_reg].astype(str).str.upper().str.contains(reg_key, na=False)
            preco_out = round(float(df_oct_ref[mask_oct][col_prec].mean()), 3) if not df_oct_ref[mask_oct].empty else preco * 0.878

            # Distribuição: ~92.1% of revenda
            dist = round(preco * 0.921, 3)

            regioes.append({
                'nome':               reg_nome,
                'emoji':              emoji,
                'revenda_media':      preco,
                'revenda_variacao':   round(preco - preco_out, 3),
                'revenda_outubro':    preco_out,
                'distribuicao_media': dist,
            })

        print(f'  ✅ {len(regioes)} regiões extraídas')
        return regioes

    except Exception as e:
        print(f'  ⚠️  regiões process: {e}')
        import traceback; traceback.print_exc()
        return []


def _anp_regioes_xlsx(content, preco_nacional):
    """Legacy — estimativa por diferencial quando xlsx regional não disponível."""
    diffs  = {'Norte':0.31,'Nordeste':0.18,'Centro-Oeste':-0.04,'Sudeste':-0.13,'Sul':-0.22}
    emojis = {'Norte':'🟢','Nordeste':'🟡','Centro-Oeste':'🟠','Sudeste':'🔵','Sul':'⚪'}
    s10ref = 6.165
    return [{'nome':n,'emoji':emojis[n],
             'revenda_media':     round(preco_nacional + d * (preco_nacional/s10ref), 3),
             'revenda_variacao':  round(preco_nacional + d * (preco_nacional/s10ref) - (6.165 + d), 3),
             'revenda_outubro':   round(6.165 + d, 3),
             'distribuicao_media':round((preco_nacional + d*(preco_nacional/s10ref))*0.921, 3)}
            for n,d in diffs.items()]



def fetch_anp_estados():
    """Baixa e processa o xlsx por UF da ANP (estados desde 2013)."""
    XLSX_ESTADOS = (
        'https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia'
        '/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/semanal'
        '/semanal-estados-desde-2013.xlsx'
    )
    # Tenta scraping da semana mais recente primeiro
    weekly = _anp_fetch_latest_weekly()
    if weekly:
        result = _anp_estados_process(weekly)
        if result:
            return result
    # Fallback: consolidado
    try:
        r = requests.get(XLSX_ESTADOS, headers=HEADERS, timeout=60)
        if r.ok and len(r.content) > 5000:
            print(f'  ✅ xlsx estados consolidado: {len(r.content)//1024}KB')
            return _anp_estados_process(r.content)
        else:
            print(f'  ⚠️  xlsx estados status: {r.status_code}')
    except Exception as e:
        print(f'  ⚠️  xlsx estados: {e}')
    return {}


def _anp_estados_process(content):
    """Processa xlsx de estados — retorna dict {UF: {preco, preco_outubro, distribucao}}."""
    try:
        import io as _io

        # Mapeamento UF → sigla (o xlsx pode usar nome completo)
        UF_SIGLA = {
            'ACRE':'AC','ALAGOAS':'AL','AMAPÁ':'AP','AMAZONAS':'AM','BAHIA':'BA',
            'CEARÁ':'CE','DISTRITO FEDERAL':'DF','ESPÍRITO SANTO':'ES','GOIÁS':'GO',
            'MARANHÃO':'MA','MATO GROSSO':'MT','MATO GROSSO DO SUL':'MS',
            'MINAS GERAIS':'MG','PARÁ':'PA','PARAÍBA':'PB','PARANÁ':'PR',
            'PERNAMBUCO':'PE','PIAUÍ':'PI','RIO DE JANEIRO':'RJ',
            'RIO GRANDE DO NORTE':'RN','RIO GRANDE DO SUL':'RS','RONDÔNIA':'RO',
            'RORAIMA':'RR','SANTA CATARINA':'SC','SÃO PAULO':'SP',
            'SERGIPE':'SE','TOCANTINS':'TO',
            # Também aceita direto a sigla
            'AC':'AC','AL':'AL','AP':'AP','AM':'AM','BA':'BA','CE':'CE',
            'DF':'DF','ES':'ES','GO':'GO','MA':'MA','MT':'MT','MS':'MS',
            'MG':'MG','PA':'PA','PB':'PB','PR':'PR','PE':'PE','PI':'PI',
            'RJ':'RJ','RN':'RN','RS':'RS','RO':'RO','RR':'RR','SC':'SC',
            'SP':'SP','SE':'SE','TO':'TO',
        }

        xf = pd.ExcelFile(_io.BytesIO(content))
        print(f'  📋 Abas estados: {xf.sheet_names[:8]}')

        # Procura aba Diesel S10
        target = None
        for sh in xf.sheet_names:
            su = sh.upper()
            if any(k in su for k in ['DIESEL','S10','REVENDA','SEMANAL']):
                target = sh; break
        if target is None:
            target = xf.sheet_names[0]
        print(f'  📄 Aba estados: {target}')

        df_raw = pd.read_excel(_io.BytesIO(content), sheet_name=target, header=None)

        # Detecta cabeçalho
        header_row = 0
        for i, row in df_raw.iterrows():
            vals = [str(v).upper() for v in row.values if pd.notna(v)]
            if sum(1 for v in vals if any(k in v for k in ['DATA','PREÇO','PRECO','ESTADO','UF','SIGLA'])) >= 2:
                header_row = i; break

        df_raw.columns = [str(c).strip().upper() if pd.notna(c) else f'_C{i}'
                          for i, c in enumerate(df_raw.iloc[header_row])]
        df = df_raw.iloc[header_row+1:].reset_index(drop=True)
        print(f'  🔑 Colunas estados: {list(df.columns[:12])}')

        # Detecta colunas
        col_ini  = next((c for c in df.columns if 'INIC' in c and 'DATA' in c), None) or                    next((c for c in df.columns if 'DATA' in c), None)
        col_prec = next((c for c in df.columns if 'MÉDIO' in c or 'MEDIO' in c), None) or                    next((c for c in df.columns if 'PREÇO' in c or 'PRECO' in c), None)
        col_uf   = next((c for c in df.columns if c in ['UF','SIGLA','ESTADO','ESTADOS']), None) or                    next((c for c in df.columns if 'UF' in c or 'ESTADO' in c), None)
        col_prod = next((c for c in df.columns if 'PRODUTO' in c), None)

        print(f'  🔍 ini={col_ini} | prec={col_prec} | uf={col_uf} | prod={col_prod}')
        if not col_ini or not col_prec or not col_uf:
            print('  ⚠️  colunas insuficientes para estados')
            return {}

        df[col_prec] = pd.to_numeric(df[col_prec], errors='coerce')
        df[col_ini]  = pd.to_datetime(df[col_ini], dayfirst=True, errors='coerce')
        if hasattr(df[col_ini], 'dt') and df[col_ini].dt.tz is not None:
            df[col_ini] = df[col_ini].dt.tz_localize(None)
        df = df.dropna(subset=[col_prec, col_ini])
        df = df[df[col_prec] > 3]

        # Filtra Diesel S10
        if col_prod:
            mask_s10 = df[col_prod].astype(str).str.upper().str.contains('S10|S-10', na=False)
            df_s10 = df[mask_s10] if not df[mask_s10].empty else df
        else:
            df_s10 = df

        # Exclui semana corrente
        today      = pd.Timestamp.now().normalize()  # tz-naive to match xlsx dates
        week_start = today - pd.Timedelta(days=today.dayofweek)
        df_s10 = df_s10[df_s10[col_ini] < week_start]

        # Última semana
        ultima_data = df_s10[col_ini].max()
        df_ult = df_s10[df_s10[col_ini] == ultima_data]
        print(f'  📅 Última semana estados: {ultima_data.strftime("%d/%m/%Y")} | {len(df_ult)} UFs')

        # Out/25 reference
        df_oct = df_s10[
            (df_s10[col_ini] >= pd.Timestamp('2025-10-01')) &
            (df_s10[col_ini] <= pd.Timestamp('2025-10-12'))
        ]

        result = {}
        for _, row in df_ult.iterrows():
            uf_raw = str(row[col_uf]).strip().upper()
            sigla  = UF_SIGLA.get(uf_raw, uf_raw[:2] if len(uf_raw) >= 2 else '')
            if not sigla or len(sigla) != 2:
                continue
            preco = round(float(row[col_prec]), 3)

            # Out/25 ref for this UF
            df_oct_uf = df_oct[df_oct[col_uf].astype(str).str.upper().str.contains(uf_raw[:4], na=False)]
            preco_out = round(float(df_oct_uf[col_prec].mean()), 3) if not df_oct_uf.empty else round(preco * 0.878, 3)

            result[sigla] = {
                'preco':       preco,
                'preco_out':   preco_out,
                'variacao_pct': round((preco - preco_out) / preco_out * 100, 2) if preco_out > 0 else 0,
            }

        print(f'  ✅ {len(result)} UFs extraídas')
        return result

    except Exception as e:
        print(f'  ⚠️  estados process: {e}')
        import traceback; traceback.print_exc()
        return {}


def _anp_fallback():
    return {'semana_referencia':'29/03/2026 a 04/04/2026','preco_atual':7.580,
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
