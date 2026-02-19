"""
Script para processar a Base MUNIC 2024 (IBGE)
- Lê a aba "Dicionário" para mapear códigos de variáveis → nomes descritivos
- Renomeia as colunas de cada aba de dados conforme o dicionário
- Filtra pelos municípios dos Territórios Socio-Biogeográficos (AC, AM, PA, AP)
- Mantém a coluna "Código do Município" com código IBGE de 7 dígitos
- Exporta um CSV por aba
"""

import pandas as pd
import os
import sys

# ============================================================
# CONFIGURAÇÃO
# ============================================================

ARQUIVO_ENTRADA = "Base_MUNIC_2024_20251107_.xlsx"
PASTA_SAIDA = "csvs_munic_2024"

# Municípios por TSBio (código IBGE 7 dígitos)
MUNICIPIOS = [
    # TSBio 1 - Altamira (PA)
    '1500602', '1500859', '1501725', '1504455',
    '1505486', '1507805', '1508159', '1508357',
    # TSBio 2 - Macapá (AP)
    '1600212', '1600303', '1600253', '1600238',
    '1600535', '1600600', '1600154', '1600055',
    # TSBio 3 - Portel (PA)
    '1503101', '1504505', '1505809', '1501105',
    # TSBio 4 - Juruá-Tefé (AM)
    '1301654', '1301803', '1301407', '1301506',
    '1301951', '1301001', '1304203', '1302207',
    '1304260', '1300029',
    # TSBio 5 - Rio Branco-Brasileia (AC)
    '1200401', '1200708', '1200252', '1200104',
    '1200054', '1200138', '1200807', '1200450',
    '1200013', '1200385', '1200179',
    # TSBio 6 - Bragança (PA)
    '1508209', '1508035', '1507961', '1507474',
    '1507466', '1507409', '1507102', '1506906',
    '1506609', '1506203', '1506112', '1506104',
    '1505601', '1505007', '1504406', '1504307',
    '1504109', '1503200', '1502905', '1502608',
    '1502202', '1501709', '1501600', '1500909',
]

# Abas de dados a processar (excluindo "Evento climático RS" que só tem municípios do RS)
ABAS_DADOS = [
    'Recursos humanos',
    'Informática e comunicação',
    'Governanca',
    'Habitacao',
    'Transporte e mobilidade urbana',
    'Agropecuária',
    'Gestão migratória',
    'Igualdade racial',
]

# Nomes dos CSVs de saída
NOMES_CSV = {
    'Recursos humanos':              '01_recursos_humanos.csv',
    'Informática e comunicação':     '02_informatica_comunicacao.csv',
    'Governanca':                    '03_governanca.csv',
    'Habitacao':                     '04_habitacao.csv',
    'Transporte e mobilidade urbana':'05_transporte_mobilidade_urbana.csv',
    'Agropecuária':                  '06_agropecuaria.csv',
    'Gestão migratória':             '07_gestao_migratoria.csv',
    'Igualdade racial':              '08_igualdade_racial.csv',
}


# ============================================================
# FUNÇÕES
# ============================================================

def construir_dicionario(arquivo):
    """Lê a aba Dicionário e constrói o mapeamento variável → descrição."""
    df = pd.read_excel(arquivo, sheet_name='Dicionário', header=None)
    mapping = {}
    for _, row in df.iterrows():
        if len(row) <= 5:
            continue
        val = row.iloc[5]
        if pd.notna(val) and str(val).strip() not in ['', 'variável', 'título link']:
            var_name = str(val).strip()
            desc = None
            for c in [3, 2, 1, 0]:
                if pd.notna(row.iloc[c]) and str(row.iloc[c]).strip():
                    desc = str(row.iloc[c]).strip()
                    break
            if desc is None:
                desc = str(row.iloc[4]).strip() if pd.notna(row.iloc[4]) else var_name
            mapping[var_name] = desc
    return mapping


def mapear_coluna(col_name, dicionario):
    """Tenta encontrar o nome descritivo para uma coluna do dado."""
    # Correspondência exata
    if col_name in dicionario:
        return dicionario[col_name]

    # Mapeamentos manuais para variações de nomes entre abas
    # (apenas para colunas que NÃO existem no dicionário nem por case-insensitive)
    manual = {
        'Sigla UF':       'Sigla da UF',
        'Cod Munic':      'Código do Município',
        'Desc Mun':       'Nome do Município',
        'Populacao':      'População do Município',
        'Faixa_populacao':'Faixa da população',
    }
    if col_name in manual:
        return manual[col_name]

    # Case-insensitive (pega 'Uf'→'UF', 'codmun'→'CodMun', etc.)
    for k, v in dicionario.items():
        if k.upper() == col_name.upper():
            return v

    # Se não encontrou, mantém o nome original
    return col_name


def detectar_coluna_codmun(df):
    """Detecta o nome da coluna de código do município (varia entre abas)."""
    for candidate in ['CodMun', 'Cod Munic', 'codmun', 'cod munic']:
        if candidate in df.columns:
            return candidate
    # Tenta case-insensitive
    for col in df.columns:
        if col.lower().replace(' ', '') in ['codmun', 'codmunic']:
            return col
    raise ValueError("Coluna de código do município não encontrada!")


def processar_aba(arquivo, nome_aba, dicionario, codigos_municipios):
    """Lê uma aba, renomeia colunas, filtra municípios."""
    df = pd.read_excel(arquivo, sheet_name=nome_aba)

    # Detectar coluna de código do município
    col_codmun = detectar_coluna_codmun(df)

    # Garantir que código é string de 7 dígitos
    df[col_codmun] = df[col_codmun].astype(str).str.strip().str.zfill(7)

    # Filtrar pelos municípios de interesse
    df_filtrado = df[df[col_codmun].isin(codigos_municipios)].copy()

    if df_filtrado.empty:
        print(f"  [AVISO] Nenhum município encontrado na aba '{nome_aba}'")
        return df_filtrado

    # Renomear colunas conforme dicionário
    rename_map = {}
    for col in df_filtrado.columns:
        novo_nome = mapear_coluna(col, dicionario)
        rename_map[col] = novo_nome
    df_filtrado = df_filtrado.rename(columns=rename_map)

    return df_filtrado


# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================

def main():
    if not os.path.exists(ARQUIVO_ENTRADA):
        print(f"Erro: Arquivo '{ARQUIVO_ENTRADA}' não encontrado.")
        sys.exit(1)

    os.makedirs(PASTA_SAIDA, exist_ok=True)

    print("Construindo dicionário de variáveis...")
    dicionario = construir_dicionario(ARQUIVO_ENTRADA)
    print(f"  {len(dicionario)} variáveis mapeadas.\n")

    print(f"Filtrando {len(MUNICIPIOS)} municípios (AC, AM, PA, AP)...\n")

    for aba in ABAS_DADOS:
        print(f"Processando: {aba}")
        df = processar_aba(ARQUIVO_ENTRADA, aba, dicionario, MUNICIPIOS)

        nome_csv = NOMES_CSV.get(aba, aba.replace(' ', '_') + '.csv')
        caminho = os.path.join(PASTA_SAIDA, nome_csv)

        df.to_csv(caminho, index=False, encoding='utf-8-sig', sep=';')
        print(f"  -> {caminho} ({len(df)} municípios, {len(df.columns)} colunas)")

    print(f"\nConcluído! CSVs salvos em: {PASTA_SAIDA}/")


if __name__ == '__main__':
    main()