#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
CONSOLIDADOR DE BASE VERTICAL PARA DASHBOARD - v5 (Python / Local)
============================================================================
Lê CSVs organizados em subpastas por tema, consolida cada categoria
em um único CSV vertical (long format) para uso em dashboards (Looker, PBI).

Estrutura de entrada esperada:
  csv/
    Agropecuária/
      Agricultura familiar pct - IBGE Censo Agro 2017.csv
      ...
    População/
      Pop por cor e sexo - Censo IBGE.csv
      ...

Uso:
    python consolidador_tsbio.py
    python consolidador_tsbio.py --categorias "População,Educação"

============================================================================
"""

import os
import re
import sys
import csv
import time
import logging
import argparse
import unicodedata
from pathlib import Path
from typing import Optional

import pandas as pd

# ============================================================================
# CAMINHOS PADRÃO (ajuste aqui se necessário)
# ============================================================================

PASTA_ENTRADA_PADRAO = (
    r"C:\Users\luiz.felipe\Desktop\FLP\MapiaEng\GitHub"
    r"\fas_tsbio\data\Indicadores_processado_por_tema\csv"
)

PASTA_SAIDA_PADRAO = (
    r"C:\Users\luiz.felipe\Desktop\FLP\MapiaEng\GitHub"
    r"\fas_tsbio\data\Consolidado"
)

# ============================================================================
# LOG
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("consolidador")


# ============================================================================
# UTILITÁRIOS
# ============================================================================

def normalizar_chave(texto: str) -> str:
    """Remove acentos, converte minúsculas, colapsa espaços."""
    s = str(texto or "").strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = re.sub(r"[\u0300-\u036f]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def limpar_nome_arquivo(nome: str) -> str:
    """Remove caracteres inválidos para nomes de arquivo."""
    return re.sub(r'[\/\\?*\[\]:"><|]', "_", str(nome or ""))[:100]


def eh_ano(valor: str) -> bool:
    """Verifica se string representa um ano (1900-2100)."""
    v = str(valor).strip()
    return bool(re.match(r"^\d{4}$", v)) and 1900 <= int(v) <= 2100


# ============================================================================
# DIMENSÕES CONHECIDAS
# Tudo que NÃO estiver aqui será convertido em INDICADOR (métrica).
# ============================================================================

DIMENSOES_GLOBAIS = {
    # Identificação Geográfica
    "territorio_id", "territorio_nome",
    "cod_municipio", "codigo_do_municipio", "codigo_municipio",
    "codigo do municipio", "código do município",
    "id_municipio", "geocodigo", "geocod_ibge", "geocodibge",
    "municipio_nome", "nome_municipio", "município", "municipality",
    "sigla_uf", "uf", "state", "sigla_uf_nome",
    "codigo_da_grande_regiao", "nome_da_grande_regiao",
    "codigo_da_unidade_da_federacao", "nome_da_unidade_da_federacao",
    "sigla_da_unidade_da_federacao",

    # Metadados
    "arquivo_origem", "recorte_origem",
    "fonte", "tema", "categoria", "indicador", "indicador_id",

    # Temporal
    "ano", "ano_da_pesquisa", "viewdate",

    # Demográficas
    "sexo", "cor_ou_raca", "cor_raca",
    "faixa_de_idade", "grupo_de_idade", "idade",
    "nacionalidade", "situacao", "situacao_do_domicilio",

    # Agropecuária / Extrativismo
    "produto", "tipo_produto", "subtipo_produto", "categoria_produto",
    "tipo_rebanho", "unidade",

    # Domicílios
    "caracteristica_do_domicilio", "condicao", "entorno",

    # Povos Tradicionais
    "etnia", "etnia_s", "etnias",
    "localidade_indigena", "localidade_quilombola",
    "tipo_de_registro", "pertencimento_etnico",

    # Geográficas Especiais
    "lugar", "pais", "grupo_de_unidade_de_conservacao", "uc",

    # Religião
    "religiao", "religioes",

    # Educação
    "area_de_formacao",

    # Línguas
    "lingua_s", "linguas_faladas",

    # Adapta Brasil (Vulnerabilidade) - formato vertical
    "id", "nome", "classe", "classname",
    "class_color", "class_name_n1", "class_name_n4",
    "classe_nivel_0", "classe_nivel_1", "classe_nivel_2",
    "classe_nivel_3", "classe_nivel_4",

    # Metadados Técnicos (ignorar como métrica)
    "geo", "system_index", "system:index", "territory",
    "areakm", "areamunkm", "areauckm",
    "unnamed_6", "unnamed: 7", "unnamed: 10", "r",

    # Colunas extras MUNIC
    "populacao_do_municipio", "faixa_da_populacao", "grande_regiao",
    "caracterizacao_do_orgao_gestor", "escolaridade",
    "foi_respondido_pelo_proprio_titular_do_orgao_gestor",
}

DIMENSOES_SET = {normalizar_chave(d) for d in DIMENSOES_GLOBAIS}


# ============================================================================
# DIMENSÕES POR CATEGORIA (atualizado conforme catálogo 2025)
# ============================================================================

DIMENSOES_OUTPUT = {
    "Agropecuária": [
        "produto", "tipo_produto", "subtipo_produto",
        "categoria_produto", "tipo_rebanho", "unidade", "sexo", "cor_raca",
    ],
    "Ambiental": ["classe", "classname"],
    "Cooperativa": ["sexo"],
    "Domicílios": ["situacao", "caracteristica_do_domicilio", "condicao"],
    "Economia": [],
    "Educação": [
        "cor_ou_raca", "sexo", "faixa_de_idade",
        "grupo_de_idade", "situacao", "area_de_formacao",
    ],
    "Entorno Domicílios": ["entorno"],
    "Extrativismo": ["produto", "tipo_produto"],
    "Favelas e Comunidades Urbanas": ["cor_ou_raca", "entorno"],
    "Fundiário e Áreas Protegidas": [
        "classe", "classe_nivel_0", "classe_nivel_1",
        "classe_nivel_2", "classe_nivel_3", "classe_nivel_4",
    ],
    "Indígenas": [
        "sexo", "faixa_de_idade", "grupo_de_idade", "situacao",
        "situacao_do_domicilio", "caracteristica_do_domicilio",
        "etnia", "localidade_indigena", "tipo_de_registro",
    ],
    "Infraestrutura": ["sexo", "cor_raca", "pais"],
    "Índices": [],
    "Políticas Públicas": ["sexo", "cor_raca", "pais", "produto", "situacao"],
    "População": [
        "cor_ou_raca", "sexo", "grupo_de_idade", "situacao",
        "nacionalidade", "grupo_de_unidade_de_conservacao", "lugar", "pais",
    ],
    "Quilombola": [
        "sexo", "faixa_de_idade", "grupo_de_idade", "situacao",
        "situacao_do_domicilio", "caracteristica_do_domicilio",
        "condicao", "localidade_quilombola", "tipo_de_registro",
    ],
    "Religião": ["religiao", "cor_ou_raca", "sexo"],
    "Trabalho e Renda": ["situacao"],
    # Vulnerabilidades (Adapta Brasil)
    "Vulnerabiliade Saúde": ["classe"],
    "Vulnerabilidade Biodiversidade": ["classe"],
    "Vulnerabilidade Desastres Geo-hidrológicos": ["classe"],
    "Vulnerabilidade Recursos Hídricos": ["classe"],
    "Vulnerabilidade Segurança Alimentar": ["classe"],
    "Vulnerabilidade Segurança Energética": ["classe"],
}


# ============================================================================
# CABEÇALHO BASE
# ============================================================================

CABECALHO_BASE = [
    "territorio_id",
    "territorio_nome",
    "cod_municipio",
    "municipio_nome",
    "sigla_uf",
    "ano",
    "fonte",
    "tema",
    "indicador",
    "valor",
]


# ============================================================================
# MAPEAMENTO DE COLUNAS
# ============================================================================

def criar_mapa_colunas(colunas: list) -> dict:
    mapa = {}
    for i, col in enumerate(colunas):
        nome = str(col).strip()
        if nome:
            mapa[normalizar_chave(nome)] = i
    return mapa


def obter_valor(row, mapa: dict, campo: str):
    idx = mapa.get(normalizar_chave(campo))
    if idx is not None:
        val = row.iloc[idx] if hasattr(row, "iloc") else row[idx]
        if pd.notna(val):
            return val
    return ""


def obter_valor_multi(row, mapa: dict, campos: list):
    for campo in campos:
        val = obter_valor(row, mapa, campo)
        if val != "":
            return val
    return ""


def identificar_metricas(colunas: list) -> list:
    metricas = []
    for i, col_raw in enumerate(colunas):
        nome = str(col_raw).strip()
        if not nome:
            continue
        if normalizar_chave(nome) not in DIMENSOES_SET:
            metricas.append({
                "nome": nome,
                "indice": i,
                "eh_ano": eh_ano(nome),
            })
    return metricas


# ============================================================================
# EXTRAÇÃO DE METADADOS DO NOME DO ARQUIVO
# Padrão: "TEMA - FONTE.csv"
# ============================================================================

def extrair_fonte_tema(nome_arquivo: str, categoria: str = "") -> dict:
    nome = re.sub(r"\.(csv|xlsx?)$", "", nome_arquivo, flags=re.IGNORECASE).strip()

    partes = [p.strip() for p in nome.split(" - ") if p.strip()]
    if len(partes) >= 2:
        return {"tema": partes[0], "fonte": " - ".join(partes[1:])}

    partes2 = [p.strip() for p in nome.split("_") if p.strip()]
    if len(partes2) >= 2:
        return {"tema": partes2[0], "fonte": " ".join(partes2[1:])}

    return {"tema": nome, "fonte": categoria or "Desconhecida"}


# ============================================================================
# LER UM CSV (tenta vários encodings e separadores)
# ============================================================================

def ler_csv_robusto(caminho: str) -> pd.DataFrame:
    """Lê CSV tentando diferentes encodings e separadores."""
    encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]
    separadores = [",", ";", "\t"]

    for enc in encodings:
        for sep in separadores:
            try:
                df = pd.read_csv(
                    caminho,
                    encoding=enc,
                    sep=sep,
                    dtype=str,
                    on_bad_lines="skip",
                    low_memory=False,
                )
                if len(df.columns) > 1:
                    return df
            except Exception:
                continue

    # Último recurso
    return pd.read_csv(caminho, dtype=str, on_bad_lines="skip", low_memory=False)


# ============================================================================
# PROCESSAR UM ARQUIVO CSV
# ============================================================================

def processar_arquivo(
    caminho: str,
    nome_categoria: str,
    dimensoes_categoria: list,
) -> list:
    linhas_resultado = []

    try:
        df = ler_csv_robusto(caminho)
    except Exception as e:
        logger.warning(f"  Erro ao ler {caminho}: {e}")
        return []

    if df.empty:
        return []

    colunas = list(df.columns)
    mapa = criar_mapa_colunas(colunas)
    info = extrair_fonte_tema(os.path.basename(caminho), nome_categoria)

    # Detecta formato Adapta Brasil (vertical: id, nome, valor, classe)
    eh_adapta = (
        normalizar_chave("nome") in mapa
        and normalizar_chave("valor") in mapa
        and normalizar_chave("classe") in mapa
    )

    colunas_metricas = identificar_metricas(colunas)

    for _, row in df.iterrows():
        # --- Geografia + tempo ---
        geo = {
            "territorio_id": obter_valor(row, mapa, "territorio_id"),
            "territorio_nome": obter_valor(row, mapa, "territorio_nome"),
            "cod_municipio": obter_valor_multi(row, mapa, [
                "cod_municipio", "codigo_do_municipio", "codigo_municipio",
                "código do município", "codigo do municipio",
                "id_municipio", "geocod_ibge", "geocodigo",
                "codigo_da_unidade_da_federacao", "id",
            ]),
            "municipio_nome": obter_valor_multi(row, mapa, [
                "municipio_nome", "nome_municipio", "município", "municipality",
            ]),
            "sigla_uf": obter_valor_multi(row, mapa, [
                "sigla_uf", "uf", "state", "sigla_da_unidade_da_federacao",
            ]),
            "ano": obter_valor_multi(row, mapa, [
                "ano", "ano_da_pesquisa", "viewdate",
            ]),
        }

        # --- Dimensões específicas ---
        dims = {}
        for dim_nome in dimensoes_categoria:
            dims[dim_nome] = obter_valor(row, mapa, dim_nome)

        if eh_adapta:
            nome_ind = obter_valor(row, mapa, "nome")
            valor_ind = obter_valor(row, mapa, "valor")
            if nome_ind != "" and valor_ind != "":
                linhas_resultado.append(
                    montar_linha(geo, info, str(nome_ind), valor_ind, dims, dimensoes_categoria)
                )
        else:
            for metrica in colunas_metricas:
                valor = row.iloc[metrica["indice"]]
                if pd.isna(valor) or str(valor).strip() == "":
                    continue

                geo_linha = geo.copy()
                indicador_nome = metrica["nome"]

                if metrica["eh_ano"] and (not geo["ano"] or str(geo["ano"]).strip() == ""):
                    geo_linha["ano"] = str(metrica["nome"])
                    indicador_nome = info["tema"] or "valor"

                linhas_resultado.append(
                    montar_linha(geo_linha, info, indicador_nome, valor, dims, dimensoes_categoria)
                )

    return linhas_resultado


def montar_linha(geo, info, indicador, valor, dims, dimensoes_categoria):
    linha = [
        geo["territorio_id"],
        geo["territorio_nome"],
        geo["cod_municipio"],
        geo["municipio_nome"],
        geo["sigla_uf"],
        geo["ano"],
        info["fonte"],
        info["tema"],
        indicador,
        valor,
    ]
    for dim in dimensoes_categoria:
        linha.append(dims.get(dim, ""))
    return linha


# ============================================================================
# PROCESSAR UMA CATEGORIA (1 pasta → 1 CSV consolidado)
# ============================================================================

def processar_categoria(
    pasta_categoria: Path,
    pasta_saida: Path,
    nome_categoria: str,
) -> dict:
    dimensoes_categoria = DIMENSOES_OUTPUT.get(nome_categoria, [])
    cabecalho = CABECALHO_BASE + dimensoes_categoria

    arquivos_csv = sorted(pasta_categoria.glob("*.csv"))
    if not arquivos_csv:
        logger.warning(f"  Nenhum .csv encontrado em {pasta_categoria}")
        return {"arquivos": 0, "linhas": 0}

    nome_saida = limpar_nome_arquivo(nome_categoria) + ".csv"
    caminho_saida = pasta_saida / nome_saida

    total_linhas = 0
    total_arquivos = 0

    with open(caminho_saida, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(cabecalho)

        for arq in arquivos_csv:
            logger.info(f"  📄 {arq.name}")
            total_arquivos += 1

            try:
                linhas = processar_arquivo(str(arq), nome_categoria, dimensoes_categoria)
                if linhas:
                    writer.writerows(linhas)
                    total_linhas += len(linhas)
                    logger.info(f"     ↳ {len(linhas):,} linhas")
            except Exception as e:
                logger.error(f"     ⚠️ ERRO: {e}")

    logger.info(f"  ✓ {total_linhas:,} linhas → {nome_saida}")
    return {"arquivos": total_arquivos, "linhas": total_linhas}


# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def executar_consolidacao(
    pasta_entrada: str,
    pasta_saida: str,
    categorias_filtro: Optional[list] = None,
):
    inicio = time.time()

    pasta_entrada = Path(pasta_entrada)
    pasta_saida = Path(pasta_saida)
    pasta_saida.mkdir(parents=True, exist_ok=True)

    if not pasta_entrada.exists():
        logger.error(f"Pasta de entrada não encontrada: {pasta_entrada}")
        sys.exit(1)

    # Descobre categorias (subpastas)
    subpastas = sorted([
        p for p in pasta_entrada.iterdir()
        if p.is_dir() and not p.name.startswith(".")
    ], key=lambda p: p.name)

    if categorias_filtro:
        nomes_filtro = {c.strip() for c in categorias_filtro}
        subpastas_filtradas = [p for p in subpastas if p.name in nomes_filtro]
        nao_encontradas = nomes_filtro - {p.name for p in subpastas_filtradas}
        for ne in nao_encontradas:
            logger.warning(f"⚠️ Pasta não encontrada: {ne}")
        subpastas = subpastas_filtradas

    logger.info("=" * 60)
    logger.info("CONSOLIDADOR TSBio v5 — Python / Local")
    logger.info("=" * 60)
    logger.info(f"Entrada: {pasta_entrada}")
    logger.info(f"Saída:   {pasta_saida}")
    logger.info(f"Categorias ({len(subpastas)}):")
    for p in subpastas:
        n_csv = len(list(p.glob("*.csv")))
        logger.info(f"  • {p.name}  ({n_csv} csvs)")
    logger.info("=" * 60)

    stats = {"categorias": 0, "arquivos": 0, "linhas": 0, "erros": []}
    log_linhas = []

    for pasta_cat in subpastas:
        nome_cat = pasta_cat.name
        logger.info(f"\n📁 {nome_cat}")

        if nome_cat not in DIMENSOES_OUTPUT:
            logger.warning(
                f"  ⚠️ '{nome_cat}' sem dimensões configuradas — "
                "colunas não-geo viram indicador."
            )

        try:
            resultado = processar_categoria(pasta_cat, pasta_saida, nome_cat)
            stats["categorias"] += 1
            stats["arquivos"] += resultado["arquivos"]
            stats["linhas"] += resultado["linhas"]
            log_linhas.append({
                "categoria": nome_cat,
                "arquivos": resultado["arquivos"],
                "linhas": resultado["linhas"],
                "status": "OK",
            })
        except Exception as e:
            msg = f'{nome_cat}: {e}'
            stats["erros"].append(msg)
            logger.error(f"❌ {msg}")
            log_linhas.append({
                "categoria": nome_cat,
                "arquivos": 0,
                "linhas": 0,
                "status": f"ERRO: {e}",
            })

    # Grava log
    log_path = pasta_saida / "_LOG.csv"
    pd.DataFrame(log_linhas).to_csv(log_path, index=False, encoding="utf-8-sig")

    duracao = time.time() - inicio

    logger.info("\n" + "=" * 60)
    logger.info("✅ CONCLUÍDO")
    logger.info(f"   Tempo:      {duracao:.1f}s")
    logger.info(f"   Categorias: {stats['categorias']}")
    logger.info(f"   Arquivos:   {stats['arquivos']}")
    logger.info(f"   Linhas:     {stats['linhas']:,}")
    if stats["erros"]:
        logger.info(f"   Erros:      {len(stats['erros'])}")
    logger.info(f"   Log:        {log_path}")
    logger.info("=" * 60)

    return stats


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Consolidador TSBio v5 — gera CSVs verticais por categoria",
    )
    parser.add_argument(
        "--entrada", "-e",
        default=PASTA_ENTRADA_PADRAO,
        help=f"Pasta raiz dos CSVs por tema (default: caminho configurado)",
    )
    parser.add_argument(
        "--saida", "-s",
        default=PASTA_SAIDA_PADRAO,
        help=f"Pasta de saída dos CSVs consolidados (default: caminho configurado)",
    )
    parser.add_argument(
        "--categorias", "-c",
        default=None,
        help='Filtrar categorias (ex: "População,Educação")',
    )

    args = parser.parse_args()

    categorias = None
    if args.categorias:
        categorias = [c.strip() for c in args.categorias.split(",") if c.strip()]

    executar_consolidacao(
        pasta_entrada=args.entrada,
        pasta_saida=args.saida,
        categorias_filtro=categorias,
    )


if __name__ == "__main__":
    main()