#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
GERADOR DE DOCUMENTAÇÃO DA BASE VERTICAL TSBio
============================================================================
Lê os CSVs consolidados (saída do consolidador_tsbio.py) e gera
documentação em Markdown descrevendo a estrutura, indicadores,
dimensões e cobertura de cada categoria.

Opcionalmente enriquece com descrições do catálogo e da documentação
original (catalogo_indicadores_tsbio.xlsx e _documentacao.md).

Uso:
    python gerar_documentacao.py
    python gerar_documentacao.py --catalogo catalogo_indicadores_tsbio.xlsx
    python gerar_documentacao.py --catalogo catalogo.xlsx --doc-md _documentacao.md

============================================================================
"""

import os
import re
import sys
import argparse
import unicodedata
from pathlib import Path
from datetime import datetime
from typing import Optional
from collections import OrderedDict

import pandas as pd

# ============================================================================
# CAMINHOS PADRÃO
# ============================================================================

PASTA_CONSOLIDADO_PADRAO = (
    r"C:\Users\luiz.felipe\Desktop\FLP\MapiaEng\GitHub"
    r"\fas_tsbio\data\Consolidado"
)

CATALOGO_PADRAO = (
    r"C:\Users\luiz.felipe\Desktop\FLP\MapiaEng\GitHub"
    r"\fas_tsbio\data\data\Indicadores_processado_por_tema\outputs\catalogo_indicadores_tsbio.xlsx"
)

DOC_MD_PADRAO = (
    r"C:\Users\luiz.felipe\Desktop\FLP\MapiaEng\GitHub"
    r"\fas_tsbio\data\data\Indicadores_processado_por_tema\outputs\_documentacao.md"
)

# ============================================================================
# COLUNAS BASE (espelho do consolidador)
# ============================================================================

COLUNAS_BASE = [
    "territorio_id", "territorio_nome", "cod_municipio",
    "municipio_nome", "sigla_uf", "ano",
    "fonte", "tema", "indicador", "valor",
]

# ============================================================================
# UTILITÁRIOS
# ============================================================================

def normalizar(texto: str) -> str:
    s = str(texto or "").strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = re.sub(r"[\u0300-\u036f]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def formatar_numero(n) -> str:
    return f"{n:,}".replace(",", ".")


def truncar(texto: str, max_len: int = 80) -> str:
    texto = str(texto or "").strip()
    if len(texto) <= max_len:
        return texto
    return texto[: max_len - 3] + "..."


def listar_valores(series: pd.Series, max_itens: int = 15) -> str:
    """Retorna string com valores únicos não-nulos, ordenados."""
    vals = sorted(series.dropna().unique().astype(str))
    vals = [v for v in vals if v.strip()]
    if not vals:
        return "_nenhum_"
    if len(vals) <= max_itens:
        return ", ".join(f"`{v}`" for v in vals)
    exibir = vals[:max_itens]
    return ", ".join(f"`{v}`" for v in exibir) + f" … (+{len(vals) - max_itens})"


# ============================================================================
# CARREGAR METADADOS EXTERNOS (catálogo + md)
# ============================================================================

def carregar_catalogo(caminho: str) -> Optional[pd.DataFrame]:
    if not caminho or not os.path.exists(caminho):
        return None
    try:
        df = pd.read_excel(caminho)
        return df
    except Exception:
        return None


def parsear_descricoes_md(caminho: str) -> dict:
    """
    Parseia _documentacao.md e retorna dict:
      {(tema_normalizado): descricao}
    """
    if not caminho or not os.path.exists(caminho):
        return {}

    try:
        with open(caminho, encoding="utf-8") as f:
            md = f.read()
    except Exception:
        return {}

    pattern = r"## (.+?)\n\n> (.+?)\n\n\*\*Fonte:\*\*"
    matches = re.findall(pattern, md, re.DOTALL)

    desc_map = {}
    for tema, descricao in matches:
        chave = normalizar(tema.strip())
        desc_map[chave] = descricao.strip()

    return desc_map


def buscar_descricao(tema: str, fonte: str, catalogo: Optional[pd.DataFrame], desc_md: dict) -> str:
    """Busca descrição no catálogo ou no MD."""
    # 1) Tenta catálogo
    if catalogo is not None:
        mask = catalogo["tema"].astype(str).apply(normalizar) == normalizar(tema)
        if fonte:
            mask2 = catalogo["fonte"].astype(str).apply(normalizar) == normalizar(fonte)
            match = catalogo[mask & mask2]
            if match.empty:
                match = catalogo[mask]
        else:
            match = catalogo[mask]

        # Pega primeiro match que tem descrição se o catálogo tiver coluna descricao
        if not match.empty:
            for col in ["descricao", "Descricao", "descrição"]:
                if col in match.columns:
                    val = match.iloc[0][col]
                    if pd.notna(val) and str(val).strip():
                        return str(val).strip()

    # 2) Tenta MD
    chave = normalizar(tema)
    if chave in desc_md:
        return desc_md[chave]

    return ""


def buscar_unidade(tema: str, fonte: str, catalogo: Optional[pd.DataFrame]) -> str:
    if catalogo is None:
        return ""
    mask = catalogo["tema"].astype(str).apply(normalizar) == normalizar(tema)
    if fonte:
        mask2 = catalogo["fonte"].astype(str).apply(normalizar) == normalizar(fonte)
        match = catalogo[mask & mask2]
        if match.empty:
            match = catalogo[mask]
    else:
        match = catalogo[mask]

    if not match.empty and "unidade" in match.columns:
        val = match.iloc[0]["unidade"]
        if pd.notna(val):
            return str(val).strip()
    return ""


# ============================================================================
# LER CSV CONSOLIDADO
# ============================================================================

def ler_csv_consolidado(caminho: str) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]
    for enc in encodings:
        try:
            return pd.read_csv(caminho, encoding=enc, dtype=str, low_memory=False)
        except Exception:
            continue
    return pd.read_csv(caminho, dtype=str, low_memory=False)


# ============================================================================
# ANALISAR UM CSV CONSOLIDADO
# ============================================================================

def analisar_categoria(df: pd.DataFrame, nome_categoria: str, catalogo, desc_md) -> dict:
    """Analisa um CSV consolidado e retorna dict com todas as métricas."""

    info = OrderedDict()
    info["nome"] = nome_categoria
    info["total_registros"] = len(df)

    # --- Colunas presentes ---
    colunas = list(df.columns)
    colunas_dim = [c for c in colunas if c not in COLUNAS_BASE]
    info["colunas_base"] = [c for c in colunas if c in COLUNAS_BASE]
    info["colunas_dimensao"] = colunas_dim

    # --- Cobertura geográfica ---
    if "municipio_nome" in df.columns:
        municipios = df["municipio_nome"].dropna().unique()
        municipios = [m for m in municipios if str(m).strip()]
        info["n_municipios"] = len(municipios)
        info["municipios"] = sorted(municipios)
    else:
        info["n_municipios"] = 0
        info["municipios"] = []

    if "sigla_uf" in df.columns:
        info["ufs"] = sorted(df["sigla_uf"].dropna().unique().tolist())
    else:
        info["ufs"] = []

    # --- Cobertura temporal ---
    if "ano" in df.columns:
        anos = df["ano"].dropna().unique()
        anos = sorted([a for a in anos if str(a).strip()])
        info["anos"] = anos
    else:
        info["anos"] = []

    # --- Fontes ---
    if "fonte" in df.columns:
        info["fontes"] = sorted(df["fonte"].dropna().unique().tolist())
    else:
        info["fontes"] = []

    # --- Indicadores (agrupados por tema + fonte) ---
    indicadores = []
    if "indicador" in df.columns and "tema" in df.columns:
        grupo = df.groupby(["tema", "fonte", "indicador"], dropna=False)

        for (tema, fonte, indicador), sub in grupo:
            tema_str = str(tema) if pd.notna(tema) else ""
            fonte_str = str(fonte) if pd.notna(fonte) else ""
            ind_str = str(indicador) if pd.notna(indicador) else ""

            anos_ind = []
            if "ano" in sub.columns:
                anos_ind = sorted(sub["ano"].dropna().unique().tolist())

            descricao = buscar_descricao(tema_str, fonte_str, catalogo, desc_md)
            unidade = buscar_unidade(tema_str, fonte_str, catalogo)

            indicadores.append({
                "tema": tema_str,
                "fonte": fonte_str,
                "indicador": ind_str,
                "registros": len(sub),
                "anos": anos_ind,
                "descricao": descricao,
                "unidade": unidade,
            })

    # Ordena por tema → indicador
    indicadores.sort(key=lambda x: (x["tema"], x["indicador"]))
    info["indicadores"] = indicadores

    # --- Dimensões (valores únicos) ---
    dims_info = []
    for col in colunas_dim:
        vals = df[col].dropna().unique()
        vals = [str(v).strip() for v in vals if str(v).strip()]
        dims_info.append({
            "coluna": col,
            "n_valores": len(vals),
            "valores": sorted(vals),
            "preenchimento_pct": round(
                df[col].apply(lambda x: pd.notna(x) and str(x).strip() != "").mean() * 100,
                1,
            ),
        })
    info["dimensoes"] = dims_info

    return info


# ============================================================================
# GERAR MARKDOWN
# ============================================================================

def gerar_markdown(analises: list, pasta_consolidado: str) -> str:
    """Gera o documento markdown completo."""

    linhas = []
    w = linhas.append  # atalho

    w("# Documentação da Base Vertical Consolidada — TSBio\n")
    w(f"> Gerado automaticamente em {datetime.now().strftime('%d/%m/%Y %H:%M')}\n")
    w(f"> Pasta de origem: `{pasta_consolidado}`\n")

    # ---- RESUMO GERAL ----
    w("---\n")
    w("## Resumo Geral\n")

    total_registros = sum(a["total_registros"] for a in analises)
    total_indicadores = sum(len(a["indicadores"]) for a in analises)
    todos_municipios = set()
    for a in analises:
        todos_municipios.update(a["municipios"])

    w(f"| Métrica | Valor |")
    w(f"|---------|-------|")
    w(f"| Categorias | {len(analises)} |")
    w(f"| Total de registros | {formatar_numero(total_registros)} |")
    w(f"| Total de indicadores (tema × fonte × indicador) | {formatar_numero(total_indicadores)} |")
    w(f"| Municípios cobertos | {len(todos_municipios)} |")
    w("")

    # ---- ESTRUTURA DA BASE ----
    w("## Estrutura da Base Vertical\n")
    w("Todos os CSVs consolidados compartilham as mesmas **10 colunas base**, ")
    w("acrescidas de colunas de **dimensão** específicas por categoria.\n")
    w("```")
    w("territorio_id | territorio_nome | cod_municipio | municipio_nome | sigla_uf")
    w("ano | fonte | tema | indicador | valor")
    w("+ [dimensões da categoria: cor_ou_raca, sexo, classe, ...]")
    w("```\n")

    w("| Coluna | Tipo | Descrição |")
    w("|--------|------|-----------|")
    w("| `territorio_id` | texto | Código do território TSBio |")
    w("| `territorio_nome` | texto | Nome do território TSBio |")
    w("| `cod_municipio` | texto | Código IBGE do município (7 dígitos) |")
    w("| `municipio_nome` | texto | Nome do município |")
    w("| `sigla_uf` | texto | UF (ex: PA, AM, MT) |")
    w("| `ano` | texto | Ano de referência do dado |")
    w("| `fonte` | texto | Fonte/instituição de origem (ex: IBGE Censo Agro 2017) |")
    w("| `tema` | texto | Tema/arquivo de origem do indicador |")
    w("| `indicador` | texto | Nome do indicador ou variável medida |")
    w("| `valor` | texto | Valor numérico ou categórico do indicador |")
    w("")

    # ---- ÍNDICE DE CATEGORIAS ----
    w("## Índice de Categorias\n")
    w("| # | Categoria | Arquivo CSV | Registros | Indicadores | Fontes | Anos |")
    w("|---|-----------|-------------|-----------|-------------|--------|------|")

    for i, a in enumerate(analises, 1):
        nome_csv = f"{a['nome']}.csv"
        n_reg = formatar_numero(a["total_registros"])
        n_ind = len(a["indicadores"])
        n_fontes = len(a["fontes"])
        anos = a["anos"]
        anos_str = f"{anos[0]}–{anos[-1]}" if len(anos) > 1 else (anos[0] if anos else "—")
        anchor = a["nome"].lower().replace(" ", "-").replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ã", "a").replace("ç", "c")
        w(f"| {i} | [{a['nome']}](#{anchor}) | `{nome_csv}` | {n_reg} | {n_ind} | {n_fontes} | {anos_str} |")

    w("")

    # ---- DETALHE POR CATEGORIA ----
    for a in analises:
        w("---\n")
        w(f"## {a['nome']}\n")

        w(f"**Arquivo:** `{a['nome']}.csv`\n")

        # Resumo rápido
        w(f"| Métrica | Valor |")
        w(f"|---------|-------|")
        w(f"| Registros | {formatar_numero(a['total_registros'])} |")
        w(f"| Indicadores | {len(a['indicadores'])} |")
        w(f"| Municípios | {a['n_municipios']} |")
        w(f"| UFs | {', '.join(a['ufs']) if a['ufs'] else '—'} |")
        w(f"| Anos | {', '.join(str(x) for x in a['anos']) if a['anos'] else '—'} |")
        w(f"| Fontes | {', '.join(a['fontes']) if a['fontes'] else '—'} |")
        w(f"| Dimensões extras | {', '.join(f'`{d['coluna']}`' for d in a['dimensoes']) if a['dimensoes'] else '—'} |")
        w("")

        # ---- Colunas do CSV ----
        w(f"### Colunas\n")
        w("**Base:** " + ", ".join(f"`{c}`" for c in a["colunas_base"]))
        if a["colunas_dimensao"]:
            w("\n**Dimensões:** " + ", ".join(f"`{c}`" for c in a["colunas_dimensao"]))
        w("")

        # ---- Dimensões: valores ----
        dims_com_valor = [d for d in a["dimensoes"] if d["n_valores"] > 0]
        dims_vazias = [d for d in a["dimensoes"] if d["n_valores"] == 0]

        if dims_com_valor:
            w(f"### Dimensões\n")
            for dim in dims_com_valor:
                preenche = dim["preenchimento_pct"]
                n_val = dim["n_valores"]
                w(f"**`{dim['coluna']}`** — {n_val} valores únicos ({preenche:.0f}% preenchido)")
                if n_val <= 30:
                    w(f": {', '.join(f'`{v}`' for v in dim['valores'])}")
                else:
                    exibir = dim["valores"][:20]
                    w(f": {', '.join(f'`{v}`' for v in exibir)} … (+{n_val - 20})")
                w("")

            if dims_vazias:
                w(f"**Sem dados nesta base:** {', '.join(f'`{d['coluna']}`' for d in dims_vazias)}\n")

        # ---- Indicadores ----
        w(f"### Indicadores\n")

        # Agrupa por tema
        temas_vistos = OrderedDict()
        for ind in a["indicadores"]:
            tema = ind["tema"]
            if tema not in temas_vistos:
                temas_vistos[tema] = []
            temas_vistos[tema].append(ind)

        for tema, inds in temas_vistos.items():
            # Pega descrição e fonte do primeiro indicador
            primeiro = inds[0]
            fonte = primeiro["fonte"]
            descricao = primeiro["descricao"]
            unidade = primeiro["unidade"]

            w(f"#### {tema}\n")
            if descricao:
                w(f"> {descricao}\n")
            meta_parts = []
            if fonte:
                meta_parts.append(f"**Fonte:** {fonte}")
            if unidade:
                meta_parts.append(f"**Unidade:** {unidade}")
            if primeiro["anos"]:
                if len(primeiro["anos"]) > 5:
                    anos_str = f"{primeiro['anos'][0]} – {primeiro['anos'][-1]}"
                else:
                    anos_str = ", ".join(str(x) for x in primeiro["anos"])
                meta_parts.append(f"**Anos:** {anos_str}")
            if meta_parts:
                w(" | ".join(meta_parts) + "\n")

            if len(inds) == 1:
                ind = inds[0]
                if ind["indicador"] != tema:
                    w(f"- Indicador: `{ind['indicador']}` ({formatar_numero(ind['registros'])} registros)")
                else:
                    w(f"- {formatar_numero(ind['registros'])} registros")
            else:
                w(f"| Indicador | Registros | Anos |")
                w(f"|-----------|-----------|------|")
                for ind in inds:
                    anos_ind = ""
                    if ind["anos"]:
                        if len(ind["anos"]) > 3:
                            anos_ind = f"{ind['anos'][0]}–{ind['anos'][-1]}"
                        else:
                            anos_ind = ", ".join(str(x) for x in ind["anos"])
                    w(f"| `{truncar(ind['indicador'], 60)}` | {formatar_numero(ind['registros'])} | {anos_ind} |")

            w("")

        # ---- Municípios cobertos ----
        if a["municipios"]:
            w(f"### Municípios ({a['n_municipios']})\n")
            if a["n_municipios"] <= 100:
                # Lista compacta
                w(", ".join(f"`{m}`" for m in a["municipios"]))
            else:
                w(f"Primeiros 50: {', '.join(f'`{m}`' for m in a['municipios'][:50])} …")
            w("")

    # ---- RODAPÉ ----
    w("---\n")
    w("## Notas Técnicas\n")
    w("- **Formato vertical (long):** cada linha = 1 observação (município × ano × indicador × dimensão)")
    w("- **Valores nulos:** campos não aplicáveis ficam em branco")
    w("- **Encoding:** UTF-8 com BOM (`utf-8-sig`) para compatibilidade com Excel")
    w("- **Separador:** vírgula (`,`)")
    w("- **Adapta Brasil:** dados de vulnerabilidade chegam em formato já vertical (id, nome, valor, classe), ")
    w("  o `cod_municipio` é preenchido a partir da coluna `id` original")
    w("- **Colunas-ano:** arquivos com anos como colunas (ex: `2000`, `2010`, `2022`) são ")
    w("  pivotados automaticamente, com o ano indo para a coluna `ano` e o nome do tema para `indicador`")
    w("")
    w(f"_Documentação gerada por `gerar_documentacao.py` em {datetime.now().strftime('%d/%m/%Y %H:%M')}_")

    return "\n".join(linhas)


# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def executar(
    pasta_consolidado: str,
    caminho_catalogo: Optional[str] = None,
    caminho_doc_md: Optional[str] = None,
    arquivo_saida: Optional[str] = None,
):
    pasta = Path(pasta_consolidado)

    if not pasta.exists():
        print(f"❌ Pasta não encontrada: {pasta}")
        sys.exit(1)

    # Carrega metadados
    catalogo = carregar_catalogo(caminho_catalogo)
    if catalogo is not None:
        print(f"✓ Catálogo carregado: {len(catalogo)} indicadores")
    else:
        print("⚠ Catálogo não encontrado — documentação sem descrições enriquecidas")

    desc_md = parsear_descricoes_md(caminho_doc_md)
    if desc_md:
        print(f"✓ Descrições MD carregadas: {len(desc_md)} temas")

    # Lista CSVs consolidados (ignora _LOG.csv)
    csvs = sorted([
        f for f in pasta.glob("*.csv")
        if not f.name.startswith("_")
    ])

    if not csvs:
        print(f"❌ Nenhum CSV consolidado encontrado em {pasta}")
        sys.exit(1)

    print(f"\n📄 {len(csvs)} CSVs encontrados:\n")
    for f in csvs:
        print(f"   • {f.name}")

    # Analisa cada CSV
    analises = []
    for csv_path in csvs:
        nome_cat = csv_path.stem
        print(f"\n🔍 Analisando: {nome_cat} ...", end=" ")
        try:
            df = ler_csv_consolidado(str(csv_path))
            info = analisar_categoria(df, nome_cat, catalogo, desc_md)
            analises.append(info)
            print(f"✓ {formatar_numero(info['total_registros'])} registros, {len(info['indicadores'])} indicadores")
        except Exception as e:
            print(f"⚠ Erro: {e}")

    # Gera markdown
    md = gerar_markdown(analises, str(pasta))

    # Salva
    if not arquivo_saida:
        arquivo_saida = str(pasta / "DOCUMENTACAO_BASE_VERTICAL.md")

    with open(arquivo_saida, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"\n✅ Documentação gerada: {arquivo_saida}")
    print(f"   {len(analises)} categorias documentadas")

    return arquivo_saida


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Gera documentação Markdown da base vertical consolidada TSBio",
    )
    parser.add_argument(
        "--pasta", "-p",
        default=PASTA_CONSOLIDADO_PADRAO,
        help="Pasta com os CSVs consolidados",
    )
    parser.add_argument(
        "--catalogo",
        default=CATALOGO_PADRAO,
        help="Caminho do catálogo de indicadores (.xlsx)",
    )
    parser.add_argument(
        "--doc-md",
        default=DOC_MD_PADRAO,
        help="Caminho da documentação original (.md)",
    )
    parser.add_argument(
        "--saida", "-s",
        default=None,
        help="Arquivo de saída (.md). Default: DOCUMENTACAO_BASE_VERTICAL.md na pasta consolidado",
    )

    args = parser.parse_args()

    executar(
        pasta_consolidado=args.pasta,
        caminho_catalogo=args.catalogo,
        caminho_doc_md=args.doc_md,
        arquivo_saida=args.saida,
    )


if __name__ == "__main__":
    main()