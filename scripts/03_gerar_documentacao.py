"""
03_gerar_documentacao.py
Etapa 3 — Gera documentação (MD + XLSX) a partir do relatório de validação.

Saídas:
- data/Indicadores_processado_por_tema/outputs/_documentacao.md
- data/Indicadores_processado_por_tema/outputs/_documentacao.xlsx

Alterações:
- Remove caminhos de arquivo da documentação
- Adiciona descrições automáticas geradas com base no tema, categoria, fonte e colunas
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Tuple

import pandas as pd

import pipeline_config as cfg


def gerar_descricao(tema: str, categoria: str, fonte: str, colunas: list) -> str:
    """
    Gera descrição automática do indicador baseada no tema, categoria, fonte e colunas.
    """
    tema = str(tema).strip()
    categoria = str(categoria).strip()
    fonte = str(fonte).strip()
    
    # Colunas de valor (excluindo metadados)
    cols_meta = {'territorio_id', 'territorio_nome', 'cod_municipio', 'ano', 'mes',
                 'arquivo_origem', 'recorte_origem', 'municipio_nome', 'sigla_uf', ''}
    cols_valor = [c.strip() for c in colunas if c.strip().lower() not in cols_meta]
    
    desc = ""
    
    # === AGROPECUÁRIA ===
    if categoria == "Agropecuária":
        if "pct" in tema.lower() or any("_perc" in c for c in cols_valor):
            if "Agricultura familiar" in tema:
                desc = "Percentual de estabelecimentos agropecuários classificados como agricultura familiar em relação ao total de estabelecimentos."
            elif "Assistência Técnica" in tema:
                desc = "Percentual de estabelecimentos agropecuários que receberam assistência técnica."
            elif "Atividade-" in tema:
                atividade = tema.replace("Atividade-", "").replace(" pct", "").strip()
                desc = f"Percentual de estabelecimentos agropecuários com atividade principal de {atividade.lower()}."
            elif "Aves-" in tema:
                tipo = tema.replace("Aves-", "").replace(" pct", "").strip()
                desc = f"Percentual de estabelecimentos com criação de aves para {tipo.lower()}."
            elif "Bovinos-" in tema:
                tipo = tema.replace("Bovinos-", "").replace(" pct", "").strip()
                desc = f"Percentual de estabelecimentos com criação de bovinos para {tipo.lower()}."
            elif "Cisterna" in tema:
                desc = "Percentual de estabelecimentos agropecuários que possuem cisterna para captação de água."
            elif "Agrotóxicos" in tema:
                desc = "Percentual das despesas totais dos estabelecimentos agropecuários destinadas à aquisição de agrotóxicos."
            elif "Produtor com escolaridade" in tema:
                desc = "Percentual de produtores rurais com nível de escolaridade até o Ensino Fundamental."
            else:
                desc = f"Percentual relativo a {tema.replace(' pct', '').lower()} nos estabelecimentos agropecuários."
        elif "Rendimento-" in tema:
            cultura = tema.replace("Rendimento-", "").replace(" kg-ha", "").strip()
            desc = f"Produtividade média da cultura de {cultura.lower()}, expressa em quilogramas por hectare (kg/ha)."
        elif "Carga de Bovinos" in tema:
            desc = "Densidade de bovinos por hectare de pastagem, indicando a intensidade de uso da área."
        elif "Estabelecimento Agropecuário" in tema:
            desc = "Número total de estabelecimentos agropecuários no município."
        elif "Média da área de lavouras por" in tema:
            equipamento = tema.split("por ")[-1].replace(" ha", "").strip()
            desc = f"Área média de lavouras por {equipamento}, indicando o grau de mecanização agrícola (ha/equipamento)."
        elif "Média de pessoal ocupado" in tema:
            desc = "Número médio de pessoas ocupadas por estabelecimento agropecuário."
        elif "PRONAF" in fonte:
            desc = "Dados do Programa Nacional de Fortalecimento da Agricultura Familiar (PRONAF)."
        elif "PAM" in fonte:
            desc = "Produção agrícola municipal - dados de área plantada, colhida, quantidade produzida e valor da produção."
        elif "PEVS" in fonte:
            desc = "Produção da extração vegetal e silvicultura - dados de produtos florestais."
        elif "PPM" in fonte:
            desc = "Pesquisa da pecuária municipal - efetivos de rebanhos e produção de origem animal."
        else:
            desc = f"Indicador agropecuário: {tema}."
    
    # === POPULAÇÃO ===
    elif categoria == "População":
        if "Densidade demográfica" in tema:
            desc = "Número de habitantes por quilômetro quadrado (hab/km²), indicando a concentração populacional no território."
        elif "Alfabetização" in tema:
            desc = "Taxa de alfabetização da população, indicando o percentual de pessoas que sabem ler e escrever."
        elif "Taxa de crescimento" in tema:
            desc = "Taxa média geométrica de variação anual da população no período intercensitário."
        elif "Filhos tidos" in tema:
            desc = "Número de filhos nascidos vivos nos 12 meses anteriores ao Censo, por grupo de idade da mãe."
        elif "Pirâmide" in tema or "grupo de idade" in tema.lower():
            desc = "Distribuição da população por grupos de idade e sexo."
        elif "Cor ou raça" in tema.lower():
            desc = "Distribuição da população segundo autodeclaração de cor ou raça."
        elif "Situação do domicílio" in tema:
            desc = "Distribuição da população entre áreas urbanas e rurais."
        elif "Idade mediana" in tema.lower():
            desc = "Idade mediana da população, indicando o ponto central da distribuição etária."
        else:
            desc = f"Indicador demográfico: {tema}."
    
    # === DOMICÍLIOS ===
    elif categoria == "Domicílios":
        if "Abastecimento de água" in tema:
            desc = "Percentual de domicílios com abastecimento de água pela rede geral de distribuição."
        elif "Banheiro" in tema:
            desc = "Percentual de domicílios com banheiro de uso exclusivo dos moradores."
        elif "Esgotamento" in tema or "esgoto" in tema.lower():
            desc = "Percentual de domicílios segundo o tipo de esgotamento sanitário."
        elif "Lixo" in tema.lower() or "coleta" in tema.lower():
            desc = "Percentual de domicílios com coleta de lixo."
        elif "Energia" in tema or "elétrica" in tema.lower():
            desc = "Percentual de domicílios com acesso à energia elétrica."
        elif "Material" in tema or "parede" in tema.lower():
            desc = "Distribuição dos domicílios segundo o material predominante das paredes externas."
        elif "Posse" in tema:
            desc = "Distribuição dos domicílios segundo a condição de posse ou ocupação."
        elif "Características" in tema:
            desc = "Características gerais dos domicílios particulares permanentes."
        else:
            desc = f"Característica dos domicílios: {tema}."
    
    # === VULNERABILIDADE (Adapta Brasil) ===
    elif categoria == "Vulnerabilidade":
        if "Adapta Brasil" in fonte:
            if "Integridade do Bioma" in tema:
                desc = "Índice de integridade do bioma que avalia o estado de conservação e pressões ambientais sobre os ecossistemas."
            elif "Risco" in tema:
                desc = "Índice de risco que combina exposição, sensibilidade e capacidade adaptativa a eventos climáticos extremos."
            elif "Exposição" in tema:
                desc = "Índice de exposição a ameaças climáticas e ambientais."
            elif "Sensibilidade" in tema:
                desc = "Índice de sensibilidade que mede a susceptibilidade do sistema a impactos climáticos."
            elif "Capacidade Adaptativa" in tema or "Capacidade_Adaptativa" in tema:
                desc = "Índice de capacidade adaptativa que mede a habilidade de ajuste às mudanças climáticas."
            elif "Disponibilidade" in tema:
                if "Energia" in tema:
                    if "Eolica" in tema:
                        desc = "Índice de disponibilidade de potencial de energia eólica no município."
                    elif "Solar" in tema:
                        desc = "Índice de disponibilidade de potencial de energia solar no município."
                    elif "Hidreletr" in tema:
                        desc = "Índice de disponibilidade de potencial de energia hidrelétrica no município."
                    else:
                        desc = "Índice de disponibilidade energética no município."
                else:
                    desc = "Índice de disponibilidade de recursos."
            elif "Segurança Alimentar" in fonte:
                desc = "Indicador de segurança alimentar que avalia a vulnerabilidade do sistema alimentar às mudanças climáticas."
            elif "Recursos Hídricos" in fonte:
                desc = "Indicador de recursos hídricos que avalia a disponibilidade e vulnerabilidade da água."
            elif "Saúde" in fonte:
                desc = "Indicador de saúde que avalia impactos climáticos sobre a saúde pública."
            elif "Biodiversidade" in fonte:
                desc = "Indicador de biodiversidade que avalia a conservação e ameaças à fauna e flora."
            elif "Desastres" in fonte:
                desc = "Indicador de risco a desastres geo-hidrológicos como deslizamentos e inundações."
            elif "Segurança Energética" in fonte:
                desc = "Indicador de segurança energética que avalia a matriz e vulnerabilidade do setor energético."
            else:
                desc = "Indicador de vulnerabilidade climática do sistema Adapta Brasil."
            
            # Adicionar info de cenário se presente
            if "2055" in tema or "swl" in tema.lower():
                desc += " Projeção para cenário futuro."
            elif "2019" in tema or "2017" in tema:
                desc += " Situação atual/linha de base."
        else:
            desc = f"Indicador de vulnerabilidade: {tema}."
    
    # === ÍNDICES ===
    elif categoria == "Índices":
        if "Gini" in tema:
            desc = "Índice de Gini que mede a desigualdade de renda. Varia de 0 (igualdade perfeita) a 1 (desigualdade máxima)."
        elif "IDHM" in tema or "Desenvolvimento" in tema:
            desc = "Índice de Desenvolvimento Humano Municipal (IDHM), composto por longevidade, educação e renda."
        else:
            desc = f"Índice socioeconômico: {tema}."
    
    # === EDUCAÇÃO ===
    elif categoria == "Educação":
        if "Alfabetização" in tema:
            desc = "Taxa de alfabetização por grupo populacional."
        elif "Nível de instrução" in tema:
            desc = "Distribuição da população por nível de instrução (escolaridade)."
        elif "Frequência escolar" in tema:
            desc = "Taxa de frequência escolar por faixa etária."
        else:
            desc = f"Indicador educacional: {tema}."
    
    # === INDÍGENAS ===
    elif categoria == "Indígenas":
        if "Alfabetização" in tema:
            desc = "Taxa de alfabetização da população indígena."
        elif "Características dos domicílios" in tema:
            desc = "Características dos domicílios ocupados por moradores indígenas."
        elif "Cor ou raça" in tema.lower():
            desc = "Distribuição da população indígena por cor ou raça autodeclarada."
        elif "Rendimento" in tema:
            desc = "Distribuição de rendimento da população indígena."
        elif "Pirâmide" in tema or "grupo de idade" in tema.lower():
            desc = "Distribuição etária da população indígena por sexo."
        else:
            desc = f"Indicador da população indígena: {tema}."
    
    # === QUILOMBOLA ===
    elif categoria == "Quilombola":
        if "Alfabetização" in tema:
            desc = "Taxa de alfabetização da população quilombola."
        elif "Banheiro" in tema:
            desc = "Condições sanitárias dos domicílios quilombolas."
        elif "Rendimento" in tema:
            desc = "Distribuição de rendimento da população quilombola."
        elif "Pirâmide" in tema or "grupo de idade" in tema.lower():
            desc = "Distribuição etária da população quilombola por sexo."
        else:
            desc = f"Indicador da população quilombola: {tema}."
    
    # === FAVELAS ===
    elif categoria == "Favelas e Comunidades Urbanas":
        if "água" in tema.lower():
            desc = "Condições de abastecimento de água em domicílios localizados em favelas e comunidades urbanas."
        elif "Caracteristicas" in tema or "Características" in tema:
            desc = "Características gerais dos domicílios localizados em favelas e comunidades urbanas."
        elif "Cor ou raça" in tema.lower():
            desc = "Distribuição da população por cor ou raça em favelas e comunidades urbanas."
        elif "Esgoto" in tema.lower():
            desc = "Condições de esgotamento sanitário em favelas e comunidades urbanas."
        elif "Pirâmide" in tema or "grupo de idade" in tema.lower():
            desc = "Distribuição etária da população em favelas e comunidades urbanas."
        else:
            desc = f"Indicador de favelas e comunidades urbanas: {tema}."
    
    # === ENTORNO DOMICÍLIOS ===
    elif categoria == "Entorno Domicílios":
        if "Arborização" in tema:
            desc = "Percentual de domicílios com presença de arborização no entorno."
        elif "Bueiro" in tema:
            desc = "Percentual de domicílios com presença de bueiro ou boca de lobo no entorno."
        elif "Calçada" in tema:
            desc = "Percentual de domicílios com presença de calçada ou passeio no entorno."
        elif "Iluminação" in tema:
            desc = "Percentual de domicílios com iluminação pública no entorno."
        elif "Pavimentação" in tema:
            desc = "Percentual de domicílios em vias com pavimentação no entorno."
        elif "Esgoto" in tema.lower():
            desc = "Percentual de domicílios com presença de esgoto a céu aberto no entorno."
        elif "Lixo" in tema.lower():
            desc = "Percentual de domicílios com presença de lixo acumulado no entorno."
        else:
            desc = f"Característica do entorno dos domicílios: {tema}."
    
    # === TRABALHO E RENDA ===
    elif categoria in ["Trabalho e Renda", "Mercado de Trabalho"]:
        if "CNPJ" in tema:
            desc = "Distribuição dos trabalhadores segundo posse de CNPJ (formalização como pessoa jurídica)."
        elif "Carteira" in tema:
            desc = "Distribuição dos trabalhadores segundo posse de carteira de trabalho assinada."
        elif "Rendimento" in tema:
            desc = "Rendimento domiciliar ou individual da população."
        elif "Empregados" in tema:
            if "privado" in tema.lower():
                desc = "Percentual de empregados no setor privado."
            elif "público" in tema.lower():
                desc = "Percentual de empregados no setor público."
            else:
                desc = "Distribuição de empregados por setor."
        elif "Número de trabalhos" in tema:
            desc = "Distribuição da população por número de trabalhos exercidos."
        else:
            desc = f"Indicador de trabalho e renda: {tema}."
    
    # === RELIGIÃO ===
    elif categoria == "Religião":
        if "Grandes grupos" in tema:
            desc = "Distribuição da população por grandes grupos de religião."
        elif "Cor ou raça" in tema.lower():
            desc = "Distribuição por cor ou raça segundo religiões selecionadas."
        elif "alfabetização" in tema.lower():
            desc = "Taxa de alfabetização por religiões selecionadas."
        else:
            desc = f"Indicador religioso: {tema}."
    
    # === DESMATAMENTO ===
    elif categoria == "Desmatamento":
        if "DETER" in fonte:
            desc = "Alertas de degradação e desmatamento detectados pelo sistema DETER/INPE."
        elif "PRODES" in fonte:
            desc = "Taxa anual de desmatamento medida pelo sistema PRODES/INPE."
        else:
            desc = f"Indicador de desmatamento: {tema}."
    
    # === QUEIMADAS ===
    elif categoria == "Queimadas":
        desc = "Área queimada mapeada pelo projeto MapBiomas Fogo, em hectares."
    
    # === USO DO SOLO ===
    elif categoria == "Uso e Cobertura do Solo":
        desc = "Classes de uso e cobertura do solo mapeadas pelo projeto MapBiomas."
    
    # === OUTROS ===
    elif categoria == "Cooperativa":
        desc = "Dados sobre cooperativas de crédito e número de cooperados no município."
    elif categoria == "Assistência Social":
        desc = "Dados do Cadastro Único para Programas Sociais, incluindo número de famílias e pessoas cadastradas."
    elif categoria == "Fundiário":
        desc = "Dados da malha fundiária com informações sobre regularização de terras."
    elif categoria == "Economia":
        if "PIB" in tema:
            desc = "Produto Interno Bruto municipal - valor total dos bens e serviços produzidos."
        else:
            desc = f"Indicador econômico: {tema}."
    
    # Fallback
    if not desc:
        desc = f"Indicador da categoria {categoria}: {tema}."
    
    return desc


def gerar_documentacao(rep_df: pd.DataFrame, out_md: Path, out_xlsx: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    col_rows = []

    for _, r in rep_df.iterrows():
        csv_path = Path(r["arquivo_csv"]) if pd.notna(r.get("arquivo_csv")) and r.get("arquivo_csv") else None

        cols = []
        if csv_path and csv_path.exists():
            try:
                cols = list(pd.read_csv(csv_path, sep=cfg.OUT_SEP, encoding=cfg.OUT_ENCODING, nrows=0).columns)
            except Exception:
                cols = []

        # Gerar descrição automática
        descricao = gerar_descricao(
            tema=r.get("tema", ""),
            categoria=r.get("categoria", ""),
            fonte=r.get("fonte", ""),
            colunas=cols
        )

        # Filtrar colunas de metadados para exibição
        cols_meta = {'territorio_id', 'territorio_nome', 'cod_municipio', 'ano', 'mes',
                     'arquivo_origem', 'recorte_origem'}
        cols_valor = [c for c in cols if c.lower() not in cols_meta]

        rows.append({
            "categoria": r.get("categoria", ""),
            "fonte": r.get("fonte", ""),
            "tema": r.get("tema", ""),
            "descricao": descricao,
            "status": r.get("status", ""),
            "linhas": r.get("linhas", ""),
            "n_colunas": len(cols) if cols else r.get("n_colunas", ""),
            "colunas_valor": "; ".join(cols_valor),
        })

        for c in cols_valor:
            col_rows.append({
                "categoria": r.get("categoria", ""),
                "fonte": r.get("fonte", ""),
                "tema": r.get("tema", ""),
                "coluna": c,
            })

    doc_df = pd.DataFrame(rows).fillna("").sort_values(["categoria", "fonte", "tema"])
    cols_df = pd.DataFrame(col_rows).fillna("").sort_values(["categoria", "fonte", "tema", "coluna"])

    # ---- MD ----
    lines = []
    lines.append("# Documentação dos Indicadores TSBio")
    lines.append("")
    lines.append("Este documento descreve os indicadores processados para os territórios TSBio.")
    lines.append("")
    
    for categoria, gcat in doc_df.groupby("categoria", sort=True, dropna=False):
        categoria_txt = categoria if str(categoria).strip() else "(sem categoria)"
        lines.append(f"# {categoria_txt}")
        lines.append("")
        
        for _, rr in gcat.iterrows():
            tema_txt = rr.get("tema", "").strip() or "(sem tema)"
            lines.append(f"## {tema_txt}")
            lines.append("")
            
            # Descrição
            if rr.get("descricao"):
                lines.append(f"> {rr['descricao']}")
                lines.append("")
            
            # Metadados em formato compacto
            meta_items = []
            if rr.get("fonte"):
                meta_items.append(f"**Fonte:** {rr['fonte']}")
            if rr.get("linhas") != "":
                meta_items.append(f"**Registros:** {rr['linhas']}")
            
            if meta_items:
                lines.append(" | ".join(meta_items))
                lines.append("")
            
            # Colunas de valor
            cols = [c.strip() for c in str(rr.get("colunas_valor", "")).split(";") if c.strip()]
            if cols:
                lines.append("**Variáveis:**")
                for c in cols:
                    lines.append(f"- `{c}`")
                lines.append("")
            
            lines.append("---")
            lines.append("")
        
        lines.append("")

    out_md.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    # ---- XLSX ----
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        doc_df.to_excel(writer, index=False, sheet_name="indicadores")
        cols_df.to_excel(writer, index=False, sheet_name="variaveis")

    print("✅ Documentação gerada:")
    print(" -", out_md)
    print(" -", out_xlsx)

    return doc_df, cols_df


def main():
    cfg.ensure_dirs()
    assert cfg.RELATORIO_VALIDACAO.exists(), f"Relatório não encontrado: {cfg.RELATORIO_VALIDACAO}"
    rep_df = pd.read_csv(cfg.RELATORIO_VALIDACAO, encoding=cfg.OUT_ENCODING)
    gerar_documentacao(rep_df, cfg.OUT_DOC_MD, cfg.OUT_DOC_XLSX)


if __name__ == "__main__":
    main()
