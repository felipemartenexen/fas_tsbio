"""
04_gerar_base_consolidada_full_e_dashboard.py
Etapa 4 — Gera base consolidada FULL (completa) e DASHBOARD (subconjunto).

- Lê os CSVs processados em data/Indicadores_processado_por_tema/csv/**.csv
- Converte para formato long/tidy (melt)
- Salva:
    outputs/base_consolidada_tsbio_full.(parquet|csv.gz|csv)
    outputs/base_consolidada_tsbio_dashboard.(parquet|csv.gz|csv)

Para o DASHBOARD:
- Usa outputs/catalogo_indicadores_tsbio_curado.csv com coluna 'dashboard' marcada como "sim".
"""

from __future__ import annotations

import gzip
import os
from pathlib import Path
from typing import List, Optional, Set

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

def infer_unidade_from_variavel(variavel: str) -> str:
    if not variavel:
        return ""
    v = str(variavel).strip().lower()
    suf = v.split("_")[-1]
    return UNIT_SUFFIX_TO_UNIT.get(suf, "")

def identificar_colunas_valor(df: pd.DataFrame) -> List[str]:
    excluir = {
        "indicador_id","categoria","fonte","tema","recorte_origem","arquivo_origem",
        "territorio_id","territorio_nome","cod_municipio","ano","mes",
    }
    cols_valor = []
    for col in df.columns:
        if col in excluir:
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            cols_valor.append(col)
            continue
        if df[col].dtype == "object":
            s = df[col].astype(str)
            numeric = pd.to_numeric(
                s.str.replace(".", "", regex=False)
                 .str.replace(",", ".", regex=False)
                 .str.replace("%", "", regex=False)
                 .str.replace(" ", "", regex=False),
                errors="coerce",
            )
            if numeric.notna().mean() > 0.5:
                cols_valor.append(col)
    return cols_valor


def _identificar_dimensoes_extras(df: pd.DataFrame, id_cols: List[str], value_cols: List[str]) -> List[str]:
    """Retorna colunas adicionais (dimensões) para manter na base 'RICA'.

    Regra: tudo que NÃO for id básico e NÃO for coluna de valor.
    Exclui colunas técnicas/automáticas.
    """
    excluir = set(id_cols) | set(value_cols) | {
        "variavel", "valor", "valor_raw", "valor_num", "unidade"
    }
    extras = []
    for c in df.columns:
        if c in excluir:
            continue
        cl = str(c).strip()
        if not cl:
            continue
        if cl.lower().startswith("unnamed"):
            continue
        extras.append(c)
    return extras

def transformar_para_long(df: pd.DataFrame, rich: bool = False) -> pd.DataFrame:
    id_cols = [c for c in [
        "territorio_id","territorio_nome","cod_municipio",
        "ano","mes",
        "indicador_id","tema","categoria","fonte",
        "recorte_origem","arquivo_origem"
    ] if c in df.columns]

    # --- padroniza tipos (evita erro no parquet e warnings) ---
    for c in ("ano", "mes", "territorio_id"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    if "cod_municipio" in df.columns:
        df["cod_municipio"] = (
            df["cod_municipio"].astype(str)
            .str.replace(r"\D+", "", regex=True)
            .str.zfill(7)
        )

    value_cols = identificar_colunas_valor(df)
    if not value_cols:
        return pd.DataFrame()

    # --- base RICA: mantém dimensões extras (ex.: produto, classe, etc.) ---
    if rich and getattr(cfg, 'RICH_INCLUDE_EXTRA_DIMS', True):
        dim_cols = _identificar_dimensoes_extras(df, id_cols, value_cols)
        # evita duplicar
        id_cols = id_cols + [c for c in dim_cols if c not in id_cols]

    melted = pd.melt(df, id_vars=id_cols, value_vars=value_cols, var_name="variavel", value_name="valor_raw")

    s = melted["valor_raw"].astype(str)
    melted["valor_num"] = pd.to_numeric(
        s.str.replace(".", "", regex=False)
         .str.replace(",", ".", regex=False)
         .str.replace("%", "", regex=False)
         .str.replace(" ", "", regex=False),
        errors="coerce",
    )

    melted["unidade"] = melted["variavel"].map(infer_unidade_from_variavel)

    if cfg.ONLY_NUMERIC_ROWS and "valor_num" in melted.columns:
        melted = melted[melted["valor_num"].notna()].copy()

    if cfg.DROP_REPEATED_TEXT and (not rich or not getattr(cfg, 'RICH_KEEP_TEXT_COLUMNS', True)):
        drop_cols = [
            "valor_raw",
            "tema","categoria","fonte","recorte_origem","arquivo_origem",
            "territorio_nome",
        ]
        melted = melted.drop(columns=drop_cols, errors="ignore")

    order = [
        "territorio_id","cod_municipio","ano","mes",
        "indicador_id","variavel","valor_num","unidade",
    ]
    cols = [c for c in order if c in melted.columns] + [c for c in melted.columns if c not in order]
    return melted[cols]

def _truthy(x) -> bool:
    if x is None:
        return False
    s = str(x).strip().lower()
    return s in {"1","true","t","yes","y","sim","s"}

def selecionar_ids_dashboard() -> List[str]:
    """Define quais indicadores entram no DASHBOARD.

    Regra:
    1) Se existir catálogo curado e houver 'dashboard=sim', usa apenas esses.
    2) Se NÃO houver nenhum marcado:
       - se cfg.DASHBOARD_USE_FALLBACK=True: usa os primeiros cfg.DASHBOARD_FALLBACK_MAX do catálogo.
       - se cfg.DASHBOARD_USE_FALLBACK=False: retorna lista vazia (não gera DASHBOARD).
    """
    # Preferência: catálogo curado
    if cfg.OUT_CATALOGO_CURADO.exists():
        df = pd.read_csv(cfg.OUT_CATALOGO_CURADO, encoding=cfg.OUT_ENCODING)
        if cfg.DASHBOARD_FLAG_COLUMN in df.columns:
            ids = (
                df[df[cfg.DASHBOARD_FLAG_COLUMN].map(_truthy)]["indicador_id"]
                .dropna().astype(str).unique().tolist()
            )
            if ids:
                print(f"✅ Dashboard: {len(ids)} indicadores marcados em {cfg.OUT_CATALOGO_CURADO.name}")
                return ids

            # curado existe mas ninguém marcou
            if hasattr(cfg, "DASHBOARD_USE_FALLBACK") and not bool(getattr(cfg, "DASHBOARD_USE_FALLBACK")):
                print(f"⚠️ Nenhum indicador marcado como '{cfg.DASHBOARD_FLAG_COLUMN}=sim' em {cfg.OUT_CATALOGO_CURADO.name}.")
                print("ℹ️ DASHBOARD_USE_FALLBACK=False -> não será gerada base de dashboard.")
                return []

    # fallback: usa catálogo normal (se habilitado)
    use_fb = True
    if hasattr(cfg, "DASHBOARD_USE_FALLBACK"):
        use_fb = bool(getattr(cfg, "DASHBOARD_USE_FALLBACK"))
    if use_fb and cfg.OUT_CATALOGO_CSV.exists():
        df = pd.read_csv(cfg.OUT_CATALOGO_CSV, encoding=cfg.OUT_ENCODING)
        max_n = int(getattr(cfg, "DASHBOARD_FALLBACK_MAX", 80) or 0)
        ids = df["indicador_id"].dropna().astype(str).unique().tolist()[:max_n]
        print(f"⚠️ Dashboard fallback: primeiros {len(ids)} indicadores do catálogo (DASHBOARD_FALLBACK_MAX={max_n}).")
        return ids

    print("⚠️ Sem catálogo curado/normal ou fallback desabilitado; dashboard ficará vazio.")
    return []


def _save_parquet(df: pd.DataFrame, out_path: Path) -> bool:
    try:
        df.to_parquet(out_path, index=False)
        return True
    except Exception as e:
        print("⚠️ Falha ao salvar parquet:", e)
        return False

def _compress_csv_to_gz(csv_path: Path, gz_path: Path) -> None:
    with open(csv_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
        while True:
            chunk = f_in.read(1024 * 1024 * 8)
            if not chunk:
                break
            f_out.write(chunk)

def _stream_write_csv(files: List[Path], out_csv: Path, filter_ids: Optional[Set[str]] = None, rich: bool = False) -> int:
    """
    Escreve CSV incremental (sem carregar tudo na memória).
    Retorna total de linhas gravadas (aprox).
    """
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    if out_csv.exists():
        out_csv.unlink()

    wrote_header = False
    total_rows = 0

    for p in tqdm(files, desc=f"Consolidando -> {out_csv.name}"):
        try:
            df = pd.read_csv(p, sep=cfg.OUT_SEP, encoding=cfg.OUT_ENCODING, low_memory=False)
        except Exception:
            continue

        if filter_ids is not None and "indicador_id" in df.columns:
            iid = str(df["indicador_id"].dropna().iloc[0]) if df["indicador_id"].notna().any() else ""
            if iid and iid not in filter_ids:
                continue

        df_long = transformar_para_long(df, rich=rich)
        if df_long.empty:
            continue

        df_long.to_csv(out_csv, index=False, sep=cfg.OUT_SEP, encoding=cfg.OUT_ENCODING, mode="a", header=not wrote_header)
        wrote_header = True
        total_rows += len(df_long)

    return total_rows


def _get_fmt_for_kind(kind: str, rich: bool = False) -> str:
    """
    Permite formatos diferentes:
      - cfg.OUTPUT_FORMAT_FULL (FULL)
      - cfg.OUTPUT_FORMAT_DASH (DASHBOARD)
    Fallback: cfg.OUTPUT_FORMAT
    """
    k = (kind or "").strip().upper()
    if k == "FULL" and hasattr(cfg, "OUTPUT_FORMAT_FULL"):
        return str(getattr(cfg, "OUTPUT_FORMAT_FULL") or "").lower().strip()
    if k != "FULL" and rich and hasattr(cfg, "OUTPUT_FORMAT_DASH_RICH"):
        return str(getattr(cfg, "OUTPUT_FORMAT_DASH_RICH") or "").lower().strip()
    if k != "FULL" and hasattr(cfg, "OUTPUT_FORMAT_DASH"):
        return str(getattr(cfg, "OUTPUT_FORMAT_DASH") or "").lower().strip()
    return str(getattr(cfg, "OUTPUT_FORMAT", "csv_gz")).lower().strip()

def gerar_base(kind: str, filter_ids: Optional[Set[str]] = None, rich: bool = False) -> None:
    files = sorted(cfg.OUT_PROCESSADO_CSV.rglob("*.csv"))
    print(f"Processados CSV encontrados: {len(files)} em {cfg.OUT_PROCESSADO_CSV}")

    fmt = _get_fmt_for_kind(kind, rich=rich)

    if kind.upper() == "FULL":
        out_parquet = cfg.OUT_BASE_FULL_PARQUET
        out_gz = cfg.OUT_BASE_FULL_CSV_GZ
        out_csv = cfg.OUT_BASE_FULL_CSV
    else:
        if rich:
            out_parquet = cfg.OUT_BASE_DASH_RICH_PARQUET
            out_gz = cfg.OUT_BASE_DASH_RICH_CSV_GZ
            out_csv = cfg.OUT_BASE_DASH_RICH_CSV
        else:
            out_parquet = cfg.OUT_BASE_DASH_PARQUET
            out_gz = cfg.OUT_BASE_DASH_CSV_GZ
            out_csv = cfg.OUT_BASE_DASH_CSV

    # 1) Se parquet estiver disponível, tenta escrever parquet (em memória por arquivo? sem, aqui fazemos fallback)
    # Para robustez, escrevemos primeiro CSV incremental e depois convertemos.
    tmp_csv = out_csv

    total = _stream_write_csv(files, tmp_csv, filter_ids=filter_ids, rich=rich)
    print(f"✅ {kind}: CSV incremental escrito (linhas ~ {total}): {tmp_csv}")

    if fmt == "csv":
        return

    if fmt == "csv_gz":
        if out_gz.exists():
            out_gz.unlink()
        _compress_csv_to_gz(tmp_csv, out_gz)
        print(f"✅ {kind}: CSV.GZ gerado: {out_gz}")
        if hasattr(cfg, "KEEP_INTERMEDIATE_CSV") and not cfg.KEEP_INTERMEDIATE_CSV:
            try:
                tmp_csv.unlink()
                print(f"🧹 {kind}: removido CSV intermediário: {tmp_csv.name}")
            except Exception:
                pass
        return

    # parquet
    df_parq = pd.read_csv(
        tmp_csv,
        sep=cfg.OUT_SEP,
        encoding=cfg.OUT_ENCODING,
        low_memory=False,
        dtype={"cod_municipio":"string","indicador_id":"string","variavel":"string","unidade":"string"},
    )
    # normaliza objetos para string (evita falhas por tipo misto no parquet)
    for c in df_parq.columns:
        if df_parq[c].dtype == object:
            df_parq[c] = df_parq[c].astype("string")

    for c in ("ano","mes","territorio_id"):
        if c in df_parq.columns:
            df_parq[c] = pd.to_numeric(df_parq[c], errors="coerce").astype("Int64")
    if "cod_municipio" in df_parq.columns:
        df_parq["cod_municipio"] = (
            df_parq["cod_municipio"].astype(str)
            .str.replace(r"\D+", "", regex=True)
            .str.zfill(7)
        )
    ok = _save_parquet(df_parq, out_parquet)
    if ok:
        print(f"✅ {kind}: PARQUET gerado: {out_parquet}")
        if hasattr(cfg, "KEEP_INTERMEDIATE_CSV") and not cfg.KEEP_INTERMEDIATE_CSV:
            try:
                tmp_csv.unlink()
                print(f"🧹 {kind}: removido CSV intermediário: {tmp_csv.name}")
            except Exception:
                pass
        # opcional: manter o CSV ou remover
        # tmp_csv.unlink(missing_ok=True)  # python 3.8+? (em 3.10 sim)
    else:
        # fallback gzip
        if out_gz.exists():
            out_gz.unlink()
        _compress_csv_to_gz(tmp_csv, out_gz)
        print(f"✅ {kind}: fallback CSV.GZ gerado: {out_gz}")
        if hasattr(cfg, "KEEP_INTERMEDIATE_CSV") and not cfg.KEEP_INTERMEDIATE_CSV:
            try:
                tmp_csv.unlink()
                print(f"🧹 {kind}: removido CSV intermediário: {tmp_csv.name}")
            except Exception:
                pass

def main():
    cfg.ensure_dirs()
    assert cfg.OUT_PROCESSADO_CSV.exists(), f"Pasta processada CSV não existe: {cfg.OUT_PROCESSADO_CSV}"

    dashboard_ids = selecionar_ids_dashboard()
    dash_set = set(dashboard_ids)

    if cfg.GENERATE_FULL_BASE:
        gerar_base("FULL", filter_ids=None, rich=False)
    else:
        print("ℹ️ GENERATE_FULL_BASE=False (pulando FULL)")

    if cfg.GENERATE_DASHBOARD_BASE:
        if len(dash_set) == 0:
            print("ℹ️ DASHBOARD: nenhum indicador selecionado -> pulando geração.")
        else:
            gerar_base("DASHBOARD", filter_ids=dash_set, rich=False)
        # DASHBOARD RICO
        if getattr(cfg, 'GENERATE_DASHBOARD_RICH_BASE', False):
            gerar_base("DASHBOARD", filter_ids=dash_set, rich=True)
    else:
        print("ℹ️ GENERATE_DASHBOARD_BASE=False (pulando DASHBOARD)")

if __name__ == "__main__":
    main()
