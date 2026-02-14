"""
02_gerar_catalogo.py
Etapa 2 — Gera o catálogo (CSV + XLSX) a partir do relatório de validação
e dos arquivos processados (CSV).

Também cria (se não existir) o arquivo de curadoria:
  outputs/catalogo_indicadores_tsbio_curado.csv
com coluna 'dashboard' para marcar o que entra no Looker.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd
from tqdm import tqdm

import pipeline_config as cfg
from pipeline_utils import read_csv_local

UNIT_SUFFIX_TO_UNIT = {
    "perc": "%",
    "ha": "ha",
    "km2": "km²",
    "m2": "m²",
    "rs": "R$",
    "pessoas": "pessoas",
}

def identificar_variaveis_valor(cols: List[str]) -> List[str]:
    excluir = {
        "indicador_id","categoria","fonte","tema","recorte_origem","arquivo_origem",
        "territorio_id","territorio_nome","cod_municipio","ano","mes",
    }
    return [c for c in cols if c not in excluir]

def inferir_unidade(variaveis: List[str]) -> str:
    units = set()
    for v in variaveis:
        suf = str(v).split("_")[-1].lower()
        u = UNIT_SUFFIX_TO_UNIT.get(suf, "")
        if u:
            units.add(u)
    if len(units) == 1:
        return list(units)[0]
    if len(units) > 1:
        return "multiplas"
    return ""

def inferir_periodo(csv_path: Path) -> str:
    try:
        df = pd.read_csv(csv_path, sep=cfg.OUT_SEP, encoding=cfg.OUT_ENCODING, usecols=["ano"])
        anos = pd.to_numeric(df["ano"], errors="coerce").dropna()
        if anos.empty:
            return ""
        return f"{int(anos.min())}-{int(anos.max())}"
    except Exception:
        return ""

def gerar_catalogo() -> pd.DataFrame:
    cfg.ensure_dirs()
    assert cfg.RELATORIO_VALIDACAO.exists(), f"Relatório não encontrado: {cfg.RELATORIO_VALIDACAO}"

    rep = pd.read_csv(cfg.RELATORIO_VALIDACAO, encoding=cfg.OUT_ENCODING)
    registros = []

    for _, r in tqdm(rep.iterrows(), total=len(rep), desc="Catalogando"):
        csv_path = Path(r["arquivo_csv"]) if isinstance(r.get("arquivo_csv"), str) and r.get("arquivo_csv") else None
        if not csv_path or not csv_path.exists():
            continue

        try:
            cols = list(pd.read_csv(csv_path, sep=cfg.OUT_SEP, encoding=cfg.OUT_ENCODING, nrows=0).columns)
        except Exception:
            continue

        variaveis = identificar_variaveis_valor(cols)
        unidade = inferir_unidade(variaveis)
        periodo = inferir_periodo(csv_path)

        registros.append({
            "indicador_id": r.get("indicador_id", ""),
            "categoria": r.get("categoria", ""),
            "fonte": r.get("fonte", ""),
            "tema": r.get("tema", ""),
            "unidade": unidade,
            "periodo": periodo,
            "arquivo_csv": str(csv_path),
            "arquivo_excel": r.get("arquivo_excel", ""),
            "n_variaveis": len(variaveis),
            "variaveis": ", ".join(variaveis[:150]),
        })

    cat = pd.DataFrame(registros).sort_values(["categoria", "fonte", "tema"])
    cfg.OUT_DIR.mkdir(parents=True, exist_ok=True)

    cat.to_csv(cfg.OUT_CATALOGO_CSV, index=False, encoding=cfg.OUT_ENCODING)
    try:
        cat.to_excel(cfg.OUT_CATALOGO_XLSX, index=False)
    except Exception as e:
        print("⚠️ Não consegui salvar XLSX do catálogo:", e)

    # cria curado se não existir
    if not cfg.OUT_CATALOGO_CURADO.exists():
        cat2 = cat.copy()
        cat2[cfg.DASHBOARD_FLAG_COLUMN] = ""
        cat2.to_csv(cfg.OUT_CATALOGO_CURADO, index=False, encoding=cfg.OUT_ENCODING)
        print("✅ Catálogo curado criado:", cfg.OUT_CATALOGO_CURADO)

    print("✅ Catálogo gerado:")
    print(" -", cfg.OUT_CATALOGO_CSV)
    print(" -", cfg.OUT_CATALOGO_XLSX)
    return cat

if __name__ == "__main__":
    gerar_catalogo()
