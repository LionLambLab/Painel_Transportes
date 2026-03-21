#!/usr/bin/env python3
"""
fetch_data.py — Coleta automática de dados para o Painel de Transportes
Fontes:
  - ANP:   dados.gov.br (série histórica CSV) + fallback
  - IPCA:  Banco Central SGS 433
  - IGP-M: Banco Central SGS 189
  - SELIC: Banco Central SGS 432
  - USD/BRL: AwesomeAPI (sem chave, CORS livre)
  - INCTL: CNT scraping com fallback
"""

import json, os, re, io
from datetime import datetime, timedelta
import requests
import pandas as pd

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'indicators.json')
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

HEADERS = {'User-Agent': 'Mozilla/5.0 (PainelTransportes/1.0; github-actions)'}
TIMEOUT = 30

MESES = ['','Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']

def fmt_mes_from_date(data_str: str) -> str:
    """Converte 'dd/mm/yyyy' → 'Mmm/aa'"""
    try:
        partes = data_str.split('/')
        m, a = int(partes[1]), partes[2][2:]
        return f'{MESES[m]}/{a}'
    except Exception:
        return data_str

def fmt_mes_from_ym(ym: str) -> str:
    """Converte 'YYYY-MM' → 'Mmm/aa'"""
    try:
        y, m = ym.split('-')
        return f'{MESES[int(m)]}/{y[2:]}'
    except Exception:
        return ym

def bcb_sgs(codigo: int, n: int = 15) -> list[dict]:
    url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados/ultimos/{n}?formato=json'
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return [{'data': d['data'], 'valor': float(d['valor'].replace(',', '.'))}
                for d in data if d.get('valor') not in (None, '', '-')]
    except Exception as e:
        print(f'  ⚠️  BCB SGS {codigo}: {e}')
        return []

# ─────────────────────────────────────────────
# 1. ANP — DIESEL S10
# ─────────────────────────────────────────────

def fetch_anp() -> dict:
    print('📥 ANP: buscando série histórica...')
    anp_csv_urls = [
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2025/ca-2025.csv',
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2025/ca-2025-01.csv',
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2024/ca-2024-02.csv',
    ]
    for url in anp_csv_urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200 and len(r.content) > 1000:
                df = pd.read_csv(io.StringIO(r.content.decode('latin-1')),
                                 sep=';', decimal=',', low_memory=False)
                print(f'  ✅ CSV: {url.split("/")[-1]} ({len(df)} linhas)')
                return _anp_process(df)
        except Exception as e:
            print(f'  ⚠️  {url.split("/")[-1]}: {e}')
    print('  ⚠️  Fallback ANP')
    return _anp_fallback()

def _anp_process(df: pd.DataFrame) -> dict:
    try:
        df.columns = [c.strip().upper() for c in df.columns]
        prod_col  = next((c for c in df.columns if 'PRODUTO' in c), None)
        preco_col = next((c for c in df.columns if 'REVENDA' in c and 'PRECO' in c), None)
        data_col  = next((c for c in df.columns if 'DATA' in c and 'INICIAL' in c), None)
        regiao_col= next((c for c in df.columns if 'REGIAO' in c), None)
        if not all([prod_col, preco_col, data_col]):
            return _anp_fallback()
        diesel = df[df[prod_col].str.contains('S10|DIESEL S-10', case=False, na=False)].copy()
        if diesel.empty:
            return _anp_fallback()
        diesel[preco_col] = pd.to_numeric(diesel[preco_col], errors='coerce')
        diesel[data_col]  = pd.to_datetime(diesel[data_col], dayfirst=True, errors='coerce')
        nacional = diesel.groupby(data_col)[preco_col].mean().reset_index()
        nacional = nacional.sort_values(data_col).tail(16)
        semanas = []
        for _, row in nacional.iterrows():
            dt = row[data_col]; dt_fim = dt + timedelta(days=6)
            semanas.append({
                'ini': dt.strftime('%Y-%m-%d'), 'fim': dt_fim.strftime('%Y-%m-%d'),
                'ini_br': dt.strftime('%d/%m'), 'fim_br': dt_fim.strftime('%d/%m/%Y'),
                'label': fmt_mes_from_date(dt.strftime('%d/%m/%Y')),
                'preco': round(float(row[preco_col]), 3)
            })
        ultima = semanas[-1] if semanas else {}
        return {
            'semana_referencia': f"{ultima.get('ini_br','')}/a {ultima.get('fim_br','')}",
            'preco_atual': ultima.get('preco', 6.89),
            'semanas': semanas,
            'regioes': _anp_regioes(diesel, data_col, preco_col, regiao_col),
            'atualizado': datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC')
        }
    except Exception as e:
        print(f'  ⚠️  Erro ANP process: {e}')
        return _anp_fallback()

def _anp_regioes(df, data_col, preco_col, regiao_col) -> list:
    mapa = {
        'NORTE':        {'nome':'Norte','emoji':'🟢'},
        'NORDESTE':     {'nome':'Nordeste','emoji':'🟡'},
        'CENTRO':       {'nome':'Centro-Oeste','emoji':'🟠'},
        'SUDESTE':      {'nome':'Sudeste','emoji':'🔵'},
        'SUL':          {'nome':'Sul','emoji':'⚪'},
    }
    out = []
    if regiao_col and regiao_col in df.columns:
        ult = df[df[data_col] == df[data_col].max()]
        for chave, info in mapa.items():
            sub = ult[ult[regiao_col].str.upper().str.contains(chave, na=False)]
            if not sub.empty:
                out.append({'nome':info['nome'],'emoji':info['emoji'],
                            'preco': round(float(sub[preco_col].mean()), 3)})
    if not out:
        ref = 6.89
        out = [
            {'nome':'Norte','emoji':'🟢','preco':round(ref+0.31,3)},
            {'nome':'Nordeste','emoji':'🟡','preco':round(ref+0.18,3)},
            {'nome':'Centro-Oeste','emoji':'🟠','preco':round(ref-0.04,3)},
            {'nome':'Sudeste','emoji':'🔵','preco':round(ref-0.13,3)},
            {'nome':'Sul','emoji':'⚪','preco':round(ref-0.22,3)},
        ]
    return out

def _anp_fallback() -> dict:
    return {
        'semana_referencia': '08/03/2026 a 14/03/2026',
        'preco_atual': 6.890,
        'semanas': [
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
        'regioes': [
            {'nome':'Norte','emoji':'🟢','preco':7.200},
            {'nome':'Nordeste','emoji':'🟡','preco':7.070},
            {'nome':'Centro-Oeste','emoji':'🟠','preco':6.850},
            {'nome':'Sudeste','emoji':'🔵','preco':6.760},
            {'nome':'Sul','emoji':'⚪','preco':6.668},
        ],
        'atualizado': 'fallback — 14/03/2026'
    }

# ─────────────────────────────────────────────
# 2. BANCO CENTRAL — IPCA, IGP-M, SELIC
# ─────────────────────────────────────────────

def fetch_bcb() -> dict:
    print('📥 BCB: buscando IPCA, IGP-M, SELIC...')
    result = {}

    # IPCA mensal (%)
    ipca = bcb_sgs(433, 15)
    if ipca:
        result['ipca'] = {
            'series': [{'label': fmt_mes_from_date(d['data']), 'valor': d['valor']} for d in ipca],
            'ultimo': ipca[-1]['valor'],
            'ultimo_label': fmt_mes_from_date(ipca[-1]['data']),
            'acum12': round(sum(d['valor'] for d in ipca[-12:]), 2),
        }
        print(f"  ✅ IPCA: {result['ipca']['ultimo']}% ({result['ipca']['ultimo_label']})")

    # IGP-M mensal (%)
    igpm = bcb_sgs(189, 15)
    if igpm:
        result['igpm'] = {
            'series': [{'label': fmt_mes_from_date(d['data']), 'valor': d['valor']} for d in igpm],
            'ultimo': igpm[-1]['valor'],
            'ultimo_label': fmt_mes_from_date(igpm[-1]['data']),
            'acum12': round(sum(d['valor'] for d in igpm[-12:]), 2),
        }
        print(f"  ✅ IGP-M: {result['igpm']['ultimo']}% ({result['igpm']['ultimo_label']})")

    # SELIC meta % a.a. (código 432)
    # SGS 432 retorna datas no formato dd/mm/yyyy — agrupa por mês corretamente
    selic = bcb_sgs(432, 60)  # pega mais para ter ao menos 14 meses distintos
    if selic:
        by_month: dict = {}
        for d in selic:
            partes = d['data'].split('/')
            if len(partes) == 3:
                ym = f"{partes[2]}-{partes[1]}"   # YYYY-MM
                by_month[ym] = d['valor']
        selic_monthly = [{'label': fmt_mes_from_ym(k), 'valor': v}
                         for k, v in sorted(by_month.items())][-14:]
        result['selic'] = {
            'series': selic_monthly,
            'atual': selic[-1]['valor'],
            'ultimo_label': fmt_mes_from_date(selic[-1]['data']),
        }
        print(f"  ✅ SELIC: {result['selic']['atual']}% a.a.")

    return result

# ─────────────────────────────────────────────
# 3. USD/BRL — AwesomeAPI (sem chave, CORS ok)
# ─────────────────────────────────────────────

def fetch_usd() -> dict:
    print('📥 USD/BRL: buscando AwesomeAPI...')
    try:
        # Pega últimos 30 dias de cotação diária
        r = requests.get('https://economia.awesomeapi.com.br/json/daily/USD-BRL/30',
                         headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        # Agrupa por mês
        by_month: dict = {}
        for d in data:
            dt = datetime.fromtimestamp(int(d['timestamp']))
            ym = dt.strftime('%Y-%m')
            by_month[ym] = round(float(d['bid']), 4)
        usd_monthly = [{'label': fmt_mes_from_ym(k), 'valor': v}
                       for k, v in sorted(by_month.items())]
        atual = round(float(data[0]['bid']), 4)
        print(f'  ✅ USD/BRL: R$ {atual}')
        return {'series': usd_monthly[-14:], 'atual': atual}
    except Exception as e:
        print(f'  ⚠️  USD/BRL AwesomeAPI: {e}')
        # Fallback: BCB SGS 1
        try:
            usd = bcb_sgs(1, 30)
            if usd:
                by_month = {}
                for d in usd:
                    partes = d['data'].split('/')
                    ym = f"{partes[2]}-{partes[1]}"
                    by_month[ym] = round(d['valor'], 4)
                series = [{'label': fmt_mes_from_ym(k), 'valor': v}
                          for k, v in sorted(by_month.items())]
                atual = usd[-1]['valor']
                print(f'  ✅ USD/BRL BCB: R$ {atual}')
                return {'series': series[-14:], 'atual': round(atual, 4)}
        except Exception as e2:
            print(f'  ⚠️  USD/BRL BCB fallback: {e2}')
        return {'series': [], 'atual': 5.89}

# ─────────────────────────────────────────────
# 4. INCTL — CNT scraping + fallback
# ─────────────────────────────────────────────

def fetch_inctl() -> dict:
    print('📥 INCTL: tentando CNT...')
    try:
        r = requests.get('https://www.cnt.org.br/inctl', headers=HEADERS, timeout=TIMEOUT)
        matches = re.findall(r'([+-]?\d{1,2},\d{2,3})\s*%', r.text)
        numeros = [float(m.replace(',','.')) for m in matches if abs(float(m.replace(',','.'))) < 10]
        if numeros:
            print(f'  ✅ INCTL: {numeros[0]}%')
            return {'ultimo': numeros[0], 'fonte': 'CNT', 'atualizado': datetime.utcnow().strftime('%d/%m/%Y'),
                    'series': []}
    except Exception as e:
        print(f'  ⚠️  INCTL CNT: {e}')
    return {
        'ultimo': 0.55, 'acum12': 3.85, 'fonte': 'fallback',
        'series': [
            {'label':'Mar/25','valor':0.32},{'label':'Abr/25','valor':0.40},
            {'label':'Mai/25','valor':0.35},{'label':'Jun/25','valor':0.18},
            {'label':'Jul/25','valor':0.25},{'label':'Ago/25','valor':0.05},
            {'label':'Set/25','valor':0.30},{'label':'Out/25','valor':0.45},
            {'label':'Nov/25','valor':0.32},{'label':'Dez/25','valor':0.38},
            {'label':'Jan/26','valor':0.28},{'label':'Fev/26','valor':0.55},
        ]
    }

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print(f'\n🚀 Iniciando coleta — {datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC")}\n')
    output = {
        'gerado_em':    datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC'),
        'gerado_em_br': (datetime.utcnow() - timedelta(hours=3)).strftime('%d/%m/%Y %H:%M'),
        'anp':   fetch_anp(),
        **fetch_bcb(),
        'usd_brl': fetch_usd(),
        'inctl': fetch_inctl(),
    }
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f'\n✅ Salvo: {OUTPUT_PATH} ({os.path.getsize(OUTPUT_PATH)//1024}KB)')
    print(f"  ANP:    R$ {output['anp']['preco_atual']} | {output['anp']['semana_referencia']}")
    if 'ipca'  in output: print(f"  IPCA:   {output['ipca']['ultimo']}% | acum12: {output['ipca']['acum12']}%")
    if 'igpm'  in output: print(f"  IGP-M:  {output['igpm']['ultimo']}%")
    if 'selic' in output: print(f"  SELIC:  {output['selic']['atual']}% a.a.")
    print(f"  USD/BRL: R$ {output['usd_brl']['atual']}")
    print(f"  INCTL:  {output['inctl']['ultimo']}%")

if __name__ == '__main__':
    main()
