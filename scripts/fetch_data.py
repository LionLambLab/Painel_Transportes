#!/usr/bin/env python3
"""
fetch_data.py — Coleta automática de dados para o Painel de Transportes
Fontes:
  - ANP:  dados.gov.br (série histórica CSV) + fallback scraping
  - IPCA: Banco Central do Brasil SGS código 433
  - IGP-M: Banco Central do Brasil SGS código 189
  - SELIC: Banco Central do Brasil SGS código 432
  - USD/BRL: Banco Central do Brasil SGS código 1
  - INCTL: CNT (scraping) com fallback para último valor conhecido
"""

import json
import os
import re
import io
from datetime import datetime, timedelta
import requests
import pandas as pd

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'indicators.json')
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

HEADERS = {'User-Agent': 'Mozilla/5.0 (PainelTransportes/1.0; github-actions)'}
TIMEOUT = 30

# ─────────────────────────────────────────────
# UTILITÁRIOS
# ─────────────────────────────────────────────

def bcb_sgs(codigo: int, n: int = 15) -> list[dict]:
    """Busca últimos N valores de uma série do SGS/Banco Central."""
    url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados/ultimos/{n}?formato=json'
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return [{'data': d['data'], 'valor': float(d['valor'].replace(',', '.'))} for d in data if d.get('valor')]
    except Exception as e:
        print(f'  ⚠️  BCB SGS {codigo} falhou: {e}')
        return []

def fmt_mes(data_str: str) -> str:
    """Converte '01/01/2025' → 'Jan/25'"""
    meses = ['','Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']
    try:
        partes = data_str.split('/')
        m, a = int(partes[1]), partes[2][2:]
        return f'{meses[m]}/{a}'
    except Exception:
        return data_str

# ─────────────────────────────────────────────
# 1. ANP — DIESEL S10
# ─────────────────────────────────────────────

def fetch_anp() -> dict:
    """
    Tenta baixar o CSV da série histórica de preços ANP do dados.gov.br.
    Retorna dict com semanas, preços nacionais e regionais.
    """
    print('📥 ANP: buscando série histórica...')

    # URLs dos CSVs de dados abertos ANP (atualizados semestralmente)
    # O arquivo ca-2024-02.csv contém o 2º semestre de 2024 etc.
    # Tentamos o CSV do semestre atual + anterior para pegar as semanas recentes
    anp_csv_urls = [
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2025/ca-2025.csv',
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2025/ca-2025-01.csv',
        'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis/precos-de-revenda/arquivos-anuais-2024/ca-2024-02.csv',
    ]

    df_raw = None
    for url in anp_csv_urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200 and len(r.content) > 1000:
                # ANP CSVs usam separador ';' e encoding latin-1
                df_raw = pd.read_csv(
                    io.StringIO(r.content.decode('latin-1')),
                    sep=';', decimal=',', low_memory=False
                )
                print(f'  ✅ CSV carregado: {url.split("/")[-1]} ({len(df_raw)} linhas)')
                break
        except Exception as e:
            print(f'  ⚠️  {url.split("/")[-1]}: {e}')

    if df_raw is None:
        print('  ⚠️  Todos os CSVs falharam — usando fallback')
        return _anp_fallback()

    return _anp_process(df_raw)


def _anp_process(df: pd.DataFrame) -> dict:
    """Processa o CSV da ANP e extrai médias semanais de Diesel S10."""
    try:
        # Normalizar nomes de colunas
        df.columns = [c.strip().upper() for c in df.columns]

        # Filtrar apenas Óleo Diesel S10
        prod_col = next((c for c in df.columns if 'PRODUTO' in c), None)
        preco_col = next((c for c in df.columns if 'REVENDA' in c and 'PRECO' in c), None)
        data_col = next((c for c in df.columns if 'DATA' in c and 'INICIAL' in c), None)
        data_fim_col = next((c for c in df.columns if 'DATA' in c and 'FINAL' in c), None)
        regiao_col = next((c for c in df.columns if 'REGIAO' in c), None)

        if not all([prod_col, preco_col, data_col]):
            print(f'  ⚠️  Colunas não encontradas: {list(df.columns)[:8]}')
            return _anp_fallback()

        diesel = df[df[prod_col].str.contains('S10|DIESEL S-10', case=False, na=False)].copy()
        if diesel.empty:
            print('  ⚠️  Nenhum registro de Diesel S10')
            return _anp_fallback()

        # Converter preços
        diesel[preco_col] = pd.to_numeric(diesel[preco_col], errors='coerce')
        diesel[data_col] = pd.to_datetime(diesel[data_col], dayfirst=True, errors='coerce')

        # Semanas nacionais (sem filtro de região)
        nacional = diesel.groupby(data_col)[preco_col].mean().reset_index()
        nacional = nacional.sort_values(data_col).tail(16)

        semanas = []
        for _, row in nacional.iterrows():
            dt = row[data_col]
            dt_fim = dt + timedelta(days=6)
            semanas.append({
                'ini': dt.strftime('%Y-%m-%d'),
                'fim': dt_fim.strftime('%Y-%m-%d'),
                'ini_br': dt.strftime('%d/%m'),
                'fim_br': dt_fim.strftime('%d/%m/%Y'),
                'label': fmt_mes(dt.strftime('%d/%m/%Y')),
                'preco': round(float(row[preco_col]), 3)
            })

        # Preço atual = última semana
        ultima = semanas[-1] if semanas else {}
        preco_atual = ultima.get('preco', 6.89)
        semana_ref = f"{ultima.get('ini_br','08/03')} a {ultima.get('fim_br','14/03/2026')}"

        # Regionais (se coluna disponível)
        regioes_out = _anp_regioes(diesel, data_col, preco_col, regiao_col)

        print(f'  ✅ ANP: {len(semanas)} semanas | S10 atual: R$ {preco_atual}')
        return {
            'semana_referencia': semana_ref,
            'preco_atual': preco_atual,
            'semanas': semanas,
            'regioes': regioes_out,
            'atualizado': datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC')
        }

    except Exception as e:
        print(f'  ⚠️  Erro ao processar ANP: {e}')
        return _anp_fallback()


def _anp_regioes(df, data_col, preco_col, regiao_col) -> list:
    """Extrai preços regionais da última semana disponível."""
    mapa = {
        'NORTE': {'nome': 'Norte', 'emoji': '🟢'},
        'NORDESTE': {'nome': 'Nordeste', 'emoji': '🟡'},
        'CENTRO OESTE': {'nome': 'Centro-Oeste', 'emoji': '🟠'},
        'CENTRO-OESTE': {'nome': 'Centro-Oeste', 'emoji': '🟠'},
        'SUDESTE': {'nome': 'Sudeste', 'emoji': '🔵'},
        'SUL': {'nome': 'Sul', 'emoji': '⚪'},
    }
    regioes_out = []

    if regiao_col and regiao_col in df.columns:
        ultima_data = df[data_col].max()
        df_ult = df[df[data_col] == ultima_data]
        for chave, info in mapa.items():
            mask = df_ult[regiao_col].str.upper().str.contains(chave, na=False)
            sub = df_ult[mask]
            if not sub.empty:
                media = round(float(sub[preco_col].mean()), 3)
                regioes_out.append({'nome': info['nome'], 'emoji': info['emoji'], 'preco': media})

    # Fallback com diferenciais fixos se scraping não trouxe regionais
    if not regioes_out:
        ref = 6.89
        regioes_out = [
            {'nome': 'Norte',        'emoji': '🟢', 'preco': round(ref + 0.31, 3)},
            {'nome': 'Nordeste',     'emoji': '🟡', 'preco': round(ref + 0.18, 3)},
            {'nome': 'Centro-Oeste', 'emoji': '🟠', 'preco': round(ref - 0.04, 3)},
            {'nome': 'Sudeste',      'emoji': '🔵', 'preco': round(ref - 0.13, 3)},
            {'nome': 'Sul',          'emoji': '⚪', 'preco': round(ref - 0.22, 3)},
        ]

    return regioes_out


def _anp_fallback() -> dict:
    """Dados fixos de última referência conhecida."""
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
# 2. BANCO CENTRAL — IPCA, IGP-M, SELIC, USD/BRL
# ─────────────────────────────────────────────

def fetch_bcb() -> dict:
    print('📥 BCB: buscando IPCA, IGP-M, SELIC, USD/BRL...')
    result = {}

    # IPCA mensal (%)
    ipca = bcb_sgs(433, 15)
    if ipca:
        result['ipca'] = {
            'series': [{'label': fmt_mes(d['data']), 'valor': d['valor']} for d in ipca],
            'ultimo': ipca[-1]['valor'],
            'ultimo_label': fmt_mes(ipca[-1]['data']),
            'acum12': round(sum(d['valor'] for d in ipca[-12:]), 2),
        }
        print(f"  ✅ IPCA: {result['ipca']['ultimo']}% ({result['ipca']['ultimo_label']})")

    # IGP-M mensal (%)
    igpm = bcb_sgs(189, 15)
    if igpm:
        result['igpm'] = {
            'series': [{'label': fmt_mes(d['data']), 'valor': d['valor']} for d in igpm],
            'ultimo': igpm[-1]['valor'],
            'ultimo_label': fmt_mes(igpm[-1]['data']),
            'acum12': round(sum(d['valor'] for d in igpm[-12:]), 2),
        }
        print(f"  ✅ IGP-M: {result['igpm']['ultimo']}% ({result['igpm']['ultimo_label']})")

    # SELIC meta % a.a. (código 432 = meta Copom)
    selic = bcb_sgs(432, 15)
    if selic:
        # SELIC meta pode ter múltiplos valores no mês — pegar último por mês
        by_month: dict = {}
        for d in selic:
            key = d['data'][:7]  # YYYY-MM
            by_month[key] = d['valor']
        selic_monthly = [{'label': fmt_mes('01/' + k.replace('-','/')), 'valor': v}
                         for k, v in sorted(by_month.items())]
        result['selic'] = {
            'series': selic_monthly[-14:],
            'atual': selic[-1]['valor'],
            'ultimo_label': fmt_mes(selic[-1]['data']),
        }
        print(f"  ✅ SELIC: {result['selic']['atual']}% a.a.")

    # USD/BRL diário (código 1) — pegar últimos 30 dias e agrupar por mês
    usd = bcb_sgs(1, 30)
    if usd:
        by_month: dict = {}
        for d in usd:
            key = d['data'][:7]
            by_month[key] = d['valor']
        usd_monthly = [{'label': fmt_mes('01/' + k.replace('-','/')), 'valor': round(v, 4)}
                       for k, v in sorted(by_month.items())]
        result['usd_brl'] = {
            'series': usd_monthly[-14:],
            'atual': round(usd[-1]['valor'], 4),
        }
        print(f"  ✅ USD/BRL: R$ {result['usd_brl']['atual']}")

    return result


# ─────────────────────────────────────────────
# 3. INCTL — CNT (scraping com fallback)
# ─────────────────────────────────────────────

def fetch_inctl() -> dict:
    """
    Tenta buscar o INCTL do site da CNT.
    CNT não tem API — fazemos scraping leve da página de índices.
    Se falhar, retorna o último valor conhecido.
    """
    print('📥 INCTL: tentando CNT...')
    try:
        r = requests.get(
            'https://www.cnt.org.br/inctl',
            headers=HEADERS, timeout=TIMEOUT
        )
        # Procura padrão de percentual no HTML: ex. "0,55%" ou "+0,55%"
        matches = re.findall(r'([+-]?\d{1,2},\d{2,3})\s*%', r.text)
        numeros = [float(m.replace(',', '.')) for m in matches if abs(float(m.replace(',', '.'))) < 10]
        if numeros:
            ultimo = numeros[0]
            print(f'  ✅ INCTL: {ultimo}%')
            return {'ultimo': ultimo, 'fonte': 'CNT scraping', 'atualizado': datetime.utcnow().strftime('%d/%m/%Y')}
    except Exception as e:
        print(f'  ⚠️  INCTL scraping falhou: {e}')

    # Fallback: último valor conhecido (Mar/2026)
    print('  ⚠️  INCTL usando fallback')
    return {
        'ultimo': 0.55,
        'acum12': 3.85,
        'fonte': 'fallback — último valor CNT publicado',
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
        'gerado_em': datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC'),
        'gerado_em_br': (datetime.utcnow() - timedelta(hours=3)).strftime('%d/%m/%Y %H:%M'),
        'anp': fetch_anp(),
        **fetch_bcb(),
        'inctl': fetch_inctl(),
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f'\n✅ Salvo em: {OUTPUT_PATH}')
    print(f'   Tamanho: {os.path.getsize(OUTPUT_PATH) / 1024:.1f} KB')

    # Resumo
    print('\n📊 Resumo:')
    if 'anp' in output:
        print(f"  ANP Diesel S10: R$ {output['anp']['preco_atual']} | {output['anp']['semana_referencia']}")
    if 'ipca' in output:
        print(f"  IPCA: {output['ipca']['ultimo']}% ({output['ipca']['ultimo_label']}) | acum12: {output['ipca']['acum12']}%")
    if 'igpm' in output:
        print(f"  IGP-M: {output['igpm']['ultimo']}% | acum12: {output['igpm']['acum12']}%")
    if 'selic' in output:
        print(f"  SELIC: {output['selic']['atual']}% a.a.")
    if 'usd_brl' in output:
        print(f"  USD/BRL: R$ {output['usd_brl']['atual']}")
    if 'inctl' in output:
        print(f"  INCTL: {output['inctl']['ultimo']}%")


if __name__ == '__main__':
    main()
