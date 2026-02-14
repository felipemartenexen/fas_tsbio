"""
02_gerar_catalogo.py
Etapa 2 — Gera o catálogo (CSV + XLSX) a partir do relatório de validação
e dos arquivos processados (CSV).

⚙️ Importante:
- Sempre "sincroniza" o arquivo de curadoria:
    outputs/catalogo_indicadores_tsbio_curado.csv
  preservando a(s) coluna(s) manual(is) (ex.: 'dashboard', 'anexo_ii') e adicionando
  automaticamente novos indicadores que aparecerem após novos processamentos.

Como funciona a curadoria:
- A coluna cfg.DASHBOARD_FLAG_COLUMN (padrão: 'dashboard') pode receber: sim/nao, s/n, 1/0, true/false.
- Você pode adicionar outras colunas manuais (ex.: 'anexo_ii', 'notas') e elas serão preservadas.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd
from tqdm import tqdm

import pipeline_config as cfg

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

def sync_catalogo_curado(cat: pd.DataFrame) -> pd.DataFrame:
    """Atualiza outputs/catalogo_indicadores_tsbio_curado.csv preservando colunas manuais."""
    curado_path = cfg.OUT_CATALOGO_CURADO
    flag_col = cfg.DASHBOARD_FLAG_COLUMN

    manual_cols = [flag_col, "anexo_ii"]  # comuns na curadoria
    curado_df = None

    if curado_path.exists():
        try:
            curado_df = pd.read_csv(curado_path, encoding=cfg.OUT_ENCODING)
        except Exception:
            curado_df = pd.read_csv(curado_path, encoding="utf-8", errors="replace")

    if curado_df is None or curado_df.empty:
        out = cat.copy()
        if flag_col not in out.columns:
            out[flag_col] = ""
        out.to_csv(curado_path, index=False, encoding=cfg.OUT_ENCODING)
        print("✅ Catálogo curado criado:", curado_path)
        return out

    if "indicador_id" not in curado_df.columns:
        raise ValueError(f"Catálogo curado existe mas não tem coluna 'indicador_id': {curado_path}")

    # colunas manuais extras (fora do catálogo)
    extra_manual = [c for c in curado_df.columns if c not in cat.columns and c != "indicador_id"]

    # garante flag no merge
    if flag_col not in curado_df.columns:
        curado_df[flag_col] = ""

    # preserva flag + extras
    keep_cols = ["indicador_id", flag_col] + [c for c in extra_manual if c != flag_col]

    # preserva manuais explícitas se existirem
    for c in manual_cols:
        if c in curado_df.columns and c not in keep_cols:
            keep_cols.append(c)

    curado_keep = curado_df[keep_cols].copy()

    out = cat.merge(curado_keep, on="indicador_id", how="left")
    if flag_col not in out.columns:
        out[flag_col] = ""

    out.to_csv(curado_path, index=False, encoding=cfg.OUT_ENCODING)

    cat_ids = set(cat["indicador_id"].dropna().astype(str))
    curado_ids = set(curado_df["indicador_id"].dropna().astype(str))
    added = len(cat_ids - curado_ids)
    removed = len(curado_ids - cat_ids)

    print("✅ Catálogo curado sincronizado:", curado_path)
    print(f" - Novos indicadores adicionados ao curado: {added}")
    print(f" - Indicadores que estavam no curado e não estão mais no catálogo: {removed} (removidos do arquivo)")
    return out

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

    # 🔥 sempre sincroniza o curado
    _ = sync_catalogo_curado(cat)

    print("✅ Catálogo gerado:")
    print(" -", cfg.OUT_CATALOGO_CSV)
    print(" -", cfg.OUT_CATALOGO_XLSX)
    return cat

if __name__ == "__main__":
    gerar_catalogo()
