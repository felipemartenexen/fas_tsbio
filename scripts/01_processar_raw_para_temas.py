"""
01_processar_raw_para_temas.py
Etapa 1 — Processa brutos em data/Indicadores -> 1 arquivo por TEMA (com todos municípios TSBio).
Exporta CSV + XLSX por tema e gera relatórios.

Regras:
- fonte = antes do 1º " - "
- tema  = parte do meio (entre 1º e 2º " - ")
- recorte = resto (metadado)
- Agrupa por (categoria, fonte, tema) e salva como <tema>.{csv|xlsx}
"""

from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from tqdm import tqdm

import pipeline_config as cfg
from pipeline_utils import (
    safe_filename, parse_parts_from_filename, read_csv_local, load_dictionary,
    normalize_column_name, zfill_mun, build_indicador_id,
)

# ---- Colunas a remover nos arquivos por TEMA (saída) ----
# Como o nome do arquivo já carrega Tema e Fonte, podemos remover metadados redundantes.
DROP_OUTPUT_COLS = ["indicador_id", "categoria", "fonte", "tema"]
CSV_EXPORT_ENCODING = "utf-8-sig"


def _is_extra_municipio_nome_col(col: str) -> bool:
    c = (col or "").strip().lower()
    if not c:
        return False
    if c in {"municipio_nome", "cod_municipio", "sigla_uf"}:
        return False
    return ("municip" in c and "nome" in c) or c in {
        "munic_pio",
        "municipio",
        "nome_municipio",
        "nome_do_municipio",
    }

# ---- Heurística para localizar a coluna de município ----


# ====== FILTROS DE PROCESSAMENTO (para rodar mais rápido) ======
# Por padrão (listas vazias), processa TUDO.
# Para processar só uma(s) categoria(s), preencha, por exemplo:
#   CATEGORIAS_ALVO = ["Vulnerabilidade"]
# Para processar só um(s) tema(s) (parte do meio do nome "Fonte - Tema - Recorte"):
#   TEMAS_ALVO = ["Área média ha", "IDHM"]
#
# Observação: a comparação é "flexível" (case-insensitive e ignora acentos/pontuação).
CATEGORIAS_ALVO = ["Índices"]  # ex.: ["Vulnerabilidade"]
TEMAS_ALVO = []       # ex.: ["Área média ha"]

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def _passa_filtros(categoria: str, tema: str) -> bool:
    if CATEGORIAS_ALVO:
        if _norm(categoria) not in {_norm(x) for x in CATEGORIAS_ALVO}:
            return False
    if TEMAS_ALVO:
        if _norm(tema) not in {_norm(x) for x in TEMAS_ALVO}:
            return False
    return True

MUN_CANDIDATES = [
    "cod_municipio",
    "codigo_do_municipio",
    "código_do_município",
    "codigo_municipio",
    "código_municipio",
    "cd_mun",
    "geocod_ibge",
    "geocod_ibge_7",
    "id_municipio",
    "codmun",
]

def find_mun_col(cols: List[str]) -> str | None:
    low = {c.lower(): c for c in cols}
    for cand in MUN_CANDIDATES:
        if cand in low:
            return low[cand]
    # fallback: contém "municip" e "cod"
    for c in cols:
        cl = c.lower()
        if "municip" in cl and ("cod" in cl or "geocod" in cl):
            return c
    return None


def choose_best_mun_col_idx(df: pd.DataFrame, expected_muns: set[str]) -> int | None:
    candidates: List[int] = []
    for idx, col in enumerate(df.columns):
        cl = str(col).lower().strip()
        if cl in MUN_CANDIDATES:
            candidates.append(idx)
            continue
        if "municip" in cl and ("cod" in cl or "geocod" in cl):
            candidates.append(idx)

    if not candidates:
        return None

    best_idx = None
    best_score = (-1, -1)
    for idx in candidates:
        series_codes = df.iloc[:, idx]
        norm_codes = series_codes.apply(zfill_mun)
        valid = int((norm_codes.str.len() == 7).sum())
        in_scope = int(norm_codes.isin(expected_muns).sum())
        score = (in_scope, valid)
        if score > best_score:
            best_score = score
            best_idx = idx

    return best_idx

def processar() -> pd.DataFrame:
    cfg.ensure_dirs()

    if cfg.OUT_ENCODING != CSV_EXPORT_ENCODING:
        print(
            f"ℹ️ Forçando encoding CSV para {CSV_EXPORT_ENCODING} "
            f"(config atual: {cfg.OUT_ENCODING})"
        )

    assert cfg.ROOT_RAW.exists(), f"ROOT_RAW não encontrado: {cfg.ROOT_RAW}"
    assert cfg.DICT_PATH.exists(), f"DICT_PATH não encontrado: {cfg.DICT_PATH}"

    syn_map = load_dictionary(cfg.DICT_PATH)

    expected_muns = set()
    mun_to_tsbio = {}
    mun_to_nome_uf = {
        zfill_mun(cod): (meta.get("municipio_nome", ""), meta.get("sigla_uf", ""))
        for cod, meta in cfg.MUNICIPIOS_TSBIO.items()
    }
    for t in cfg.TSBIO:
        tid = int(t["territorio_id"])
        tname = str(t["territorio_nome"])
        for m in t["CD_MUN"]:
            mm = zfill_mun(m)
            expected_muns.add(mm)
            mun_to_tsbio[mm] = (tid, tname)

    csv_files = sorted(cfg.ROOT_RAW.rglob("*.csv"))
    print(f"CSV brutos encontrados: {len(csv_files)} em {cfg.ROOT_RAW}")

    skipped_by_filter = 0

    # Buckets por (categoria, fonte, tema)
    buckets: Dict[Tuple[str, str, str], List[pd.DataFrame]] = defaultdict(list)

    errors = []
    missing_mun_col = []

    for p in tqdm(csv_files, desc="Lendo brutos"):
        # categoria = 1º nível de pasta abaixo de ROOT_RAW
        try:
            rel = p.relative_to(cfg.ROOT_RAW)
            categoria = rel.parts[0] if len(rel.parts) >= 2 else "(raiz)"
        except Exception:
            categoria = "(raiz)"

        fonte, tema, recorte = parse_parts_from_filename(p.name)
        indicador_id = build_indicador_id(categoria, fonte, tema)

        # --- filtros (opcional) ---
        if not _passa_filtros(categoria, tema):
            skipped_by_filter += 1
            continue

        try:
            df = read_csv_local(p)
        except Exception as e:
            errors.append((str(p), str(e)))
            continue

        # Normaliza colunas
        col_map = {c: normalize_column_name(c, syn_map) for c in df.columns}
        df = df.rename(columns=col_map)

        mun_col_idx = choose_best_mun_col_idx(df, expected_muns)
        if mun_col_idx is None:
            mun_col = find_mun_col(list(df.columns))
            if not mun_col:
                missing_mun_col.append(str(p))
                continue
            col_values = df[mun_col]
            if isinstance(col_values, pd.DataFrame):
                col_values = col_values.iloc[:, 0]
            df["cod_municipio"] = col_values
        else:
            df["cod_municipio"] = df.iloc[:, mun_col_idx]

        if "cod_municipio" not in df.columns:
            missing_mun_col.append(str(p))
            continue

        df["cod_municipio"] = df["cod_municipio"].apply(zfill_mun)
        df = df[df["cod_municipio"].astype(str).str.len() > 0].copy()

        # Filtra apenas municípios TSBio
        df = df[df["cod_municipio"].isin(expected_muns)].copy()
        if df.empty:
            continue

        if "municipio_nome" not in df.columns:
            df["municipio_nome"] = ""
        if "sigla_uf" not in df.columns:
            df["sigla_uf"] = ""

        nome_existente = df["municipio_nome"].fillna("").astype(str).str.strip()
        uf_existente = df["sigla_uf"].fillna("").astype(str).str.strip()

        nome_map = df["cod_municipio"].map(lambda cod: mun_to_nome_uf.get(cod, ("", ""))[0]).fillna("")
        uf_map = df["cod_municipio"].map(lambda cod: mun_to_nome_uf.get(cod, ("", ""))[1]).fillna("")

        nome_final = nome_map.where(nome_map.str.len() > 0, nome_existente)
        uf_final = uf_map.where(uf_map.str.len() > 0, uf_existente)

        df["sigla_uf"] = uf_final
        df["municipio_nome"] = nome_final

        mask_nome_uf = (df["municipio_nome"].str.len() > 0) & (df["sigla_uf"].str.len() > 0)
        df.loc[mask_nome_uf, "municipio_nome"] = (
            df.loc[mask_nome_uf, "municipio_nome"]
            + " ("
            + df.loc[mask_nome_uf, "sigla_uf"]
            + ")"
        )

        # Metadados internos (não exportados no CSV final)
        df["indicador_id"] = indicador_id
        df["categoria"] = categoria
        df["fonte"] = fonte
        df["tema"] = tema
        df["recorte_origem"] = recorte
        df["arquivo_origem"] = p.name
        df["territorio_id"] = df["cod_municipio"].map(lambda x: mun_to_tsbio.get(x, (None, None))[0])
        df["territorio_nome"] = df["cod_municipio"].map(lambda x: mun_to_tsbio.get(x, (None, None))[1])
        df = df[df["territorio_id"].notna()].copy()

        buckets[(categoria, fonte, tema)].append(df)

    report_rows = []

    for (categoria, fonte, tema), dfs in buckets.items():
        big = pd.concat(dfs, ignore_index=True)

        present = set(big["cod_municipio"].dropna().unique())
        missing = sorted(list(expected_muns - present))

        status = "ok" if len(big) else "vazio"
        if missing:
            # Não trava; só registra no relatório
            status = "parcial"

        # Pastas de saída
        cat_dir_csv = cfg.OUT_PROCESSADO_CSV / safe_filename(categoria)
        cat_dir_xlsx = cfg.OUT_PROCESSADO_XLSX / safe_filename(categoria)
        cat_dir_csv.mkdir(parents=True, exist_ok=True)
        cat_dir_xlsx.mkdir(parents=True, exist_ok=True)

        base = safe_filename(f"{tema} - {fonte}")
        out_csv_path = cat_dir_csv / f"{base}.csv"
        out_xlsx_path = cat_dir_xlsx / f"{base}.xlsx"

        # Ordena colunas (saída por tema)
        out_df = big.copy()
        first_cols = [
            "indicador_id","categoria","fonte","tema",
            "territorio_id","territorio_nome",
            "cod_municipio","municipio_nome","sigla_uf",
            "ano","mes",
            "arquivo_origem","recorte_origem"
        ]
        cols = [c for c in first_cols if c in out_df.columns] + [c for c in out_df.columns if c not in first_cols]
        out_df = out_df[cols]

        # Remove metadados redundantes na saída (mantém em memória para relatório)
        if DROP_OUTPUT_COLS:
            out_df = out_df.drop(columns=[c for c in DROP_OUTPUT_COLS if c in out_df.columns], errors="ignore")

        # Não exportar colunas de rastreabilidade nem colunas duplicadas de nome de município
        non_export_cols = ["arquivo_origem", "recorte_origem"]
        non_export_cols.extend([c for c in out_df.columns if _is_extra_municipio_nome_col(c)])
        if non_export_cols:
            out_df = out_df.drop(columns=[c for c in non_export_cols if c in out_df.columns], errors="ignore")

        if cfg.EXPORT_PROCESSADO_CSV:
            out_df.to_csv(out_csv_path, index=False, sep=cfg.OUT_SEP, encoding=CSV_EXPORT_ENCODING)
        if cfg.EXPORT_PROCESSADO_XLSX:
            try:
                out_df.to_excel(out_xlsx_path, index=False)
            except Exception as e:
                # Excel é opcional, mas TdR pede; loga erro
                errors.append((str(out_xlsx_path), f"excel_write_error: {e}"))

        report_rows.append({
            "categoria": categoria,
            "fonte": fonte,
            "tema": tema,
            "indicador_id": big["indicador_id"].iloc[0] if "indicador_id" in big.columns and len(big) else "",
            "status": status,
            "arquivo_csv": str(out_csv_path) if cfg.EXPORT_PROCESSADO_CSV else "",
            "arquivo_excel": str(out_xlsx_path) if cfg.EXPORT_PROCESSADO_XLSX else "",
            "linhas": len(out_df),
            "n_colunas": len(out_df.columns),
            "faltando_cod_municipio": ",".join(missing) if missing else "",
        })

    rep_df = pd.DataFrame(report_rows).sort_values(["categoria", "fonte", "tema"])
    rep_df.to_csv(cfg.RELATORIO_VALIDACAO, index=False, encoding=CSV_EXPORT_ENCODING)

    if missing_mun_col:
        pd.DataFrame({"arquivo": missing_mun_col}).to_csv(
            cfg.RELATORIO_SEM_MUN,
            index=False,
            encoding=CSV_EXPORT_ENCODING,
        )

    if errors:
        pd.DataFrame(errors, columns=["arquivo", "erro"]).to_csv(
            cfg.RELATORIO_ERROS,
            index=False,
            encoding=CSV_EXPORT_ENCODING,
        )

    print(f"ℹ️ Arquivos ignorados pelos filtros: {skipped_by_filter}")
    print("✅ Processamento concluído.")
    print(" - Relatório:", cfg.RELATORIO_VALIDACAO)
    print(" - Sem coluna município:", cfg.RELATORIO_SEM_MUN)
    print(" - Erros:", cfg.RELATORIO_ERROS)
    return rep_df

if __name__ == "__main__":
    processar()
