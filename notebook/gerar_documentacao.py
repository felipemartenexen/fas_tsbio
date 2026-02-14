# %%
# ============================================================
# DOCUMENTAÇÃO AUTOMÁTICA DOS CSVs (Indicadores_processado_por_tema)
# Gera:
#   1) _documentacao.xlsx  -> abas: doc_obrigatoria, campos, erros
#   2) _documentacao.md    -> lista arquivo -> colunas (+ alertas)
#
# Obrigatório (por arquivo): arquivo_nome, tema, categoria, colunas
# ============================================================

from pathlib import Path
import pandas as pd
import csv
import re

# =======================
# CONFIG
# =======================
OUT_DIR = Path(r"C:\Users\luiz.felipe\Desktop\FLP\MapiaEng\GitHub\fas_tsbio\data\Indicadores_processado_por_tema")

DEFAULT_SAMPLE_ROWS = 5000
SEPS_CANDIDATES = [";", ",", "\t"]

# como juntar a lista de colunas na aba "doc_obrigatoria"
COLS_JOIN_SEP = " | "   # pode trocar por "\n" se preferir uma coluna com quebras de linha

# limitar tamanho da lista no markdown (evita md gigante)
MAX_COLS_MD = 300  # None = sem limite

# saída
OUT_XLSX = OUT_DIR / "_documentacao.xlsx"
OUT_MD   = OUT_DIR / "_documentacao.md"

# =======================
# FUNÇÕES DE LEITURA
# =======================
def sniff_sep(path: Path, encoding: str = "utf-8-sig") -> str:
    """Infere separador a partir de um sample (robusto a 'sep=;')."""
    try:
        with open(path, "r", encoding=encoding, errors="replace") as f:
            sample = f.read(8192)

        # remove possível linha "sep=;"
        if sample.lower().startswith("sep="):
            lines = sample.splitlines(True)
            sample = "".join(lines[1:]) if len(lines) > 1 else ""

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
            return dialect.delimiter
        except Exception:
            header = sample.splitlines()[0] if sample else ""
            counts = {s: header.count(s) for s in SEPS_CANDIDATES}
            best = max(counts, key=counts.get)
            return best if counts[best] > 0 else ","
    except Exception:
        return ","


def read_sample(path: Path, nrows: int = DEFAULT_SAMPLE_ROWS) -> pd.DataFrame:
    """Lê uma amostra do CSV para inferir schema."""
    last = None
    for enc in ("utf-8-sig", "latin1"):
        try:
            sep = sniff_sep(path, enc)
            return pd.read_csv(path, sep=sep, encoding=enc, nrows=nrows, low_memory=False)
        except Exception as e:
            last = e
    raise last


def count_rows_fast(path: Path):
    """Conta linhas sem carregar em memória (best effort)."""
    for enc in ("utf-8-sig", "latin1"):
        try:
            with open(path, "r", encoding=enc, errors="replace") as f:
                n = sum(1 for _ in f)
            return max(0, n - 1)  # desconta header
        except Exception:
            pass
    return None


# =======================
# INFERÊNCIA DE TIPO
# =======================
BOOL_SET = {"0", "1", "true", "false", "sim", "nao", "não", "yes", "no"}

def infer_semantic_type(s: pd.Series, colname: str) -> str:
    """Tipo semântico sugerido (útil pra dicionário de dados)."""
    name = (colname or "").strip().lower()

    if any(k in name for k in ["código", "codigo", "cd_", "cod", "id"]):
        return "codigo"
    if any(k in name for k in ["município", "municipio", "uf", "sigla", "região", "regiao"]):
        return "texto"
    if any(k in name for k in ["percentual", "percent", "%"]):
        return "percentual"
    if any(k in name for k in ["data", "mês", "mes", "ano"]):
        s0 = s.dropna().astype(str).str.strip()
        if not s0.empty:
            dt = pd.to_datetime(s0, errors="coerce", dayfirst=True)
            if dt.notna().mean() >= 0.9:
                return "data"

    s0 = s.dropna()
    if s0.empty:
        return "desconhecido"

    s_str = s0.astype(str).str.strip()
    s_str = s_str[s_str != ""]
    if s_str.empty:
        return "desconhecido"

    # boolean
    uniq = set(v.lower() for v in s_str.head(200).unique())
    if uniq and uniq.issubset(BOOL_SET):
        return "booleano"

    # numérico: tenta pt-BR e en-US
    s_pt = s_str.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    num_pt = pd.to_numeric(s_pt, errors="coerce")
    ratio_pt = num_pt.notna().mean()

    s_en = s_str.str.replace(",", "", regex=False)
    num_en = pd.to_numeric(s_en, errors="coerce")
    ratio_en = num_en.notna().mean()

    if max(ratio_pt, ratio_en) >= 0.9:
        num = num_pt if ratio_pt >= ratio_en else num_en
        frac = (num.dropna() % 1 != 0).mean() if not num.dropna().empty else 0
        return "decimal" if frac > 0 else "inteiro"

    return "texto"


def top_examples(s: pd.Series, k: int = 3) -> str:
    s0 = s.dropna()
    if s0.empty:
        return ""
    vc = s0.astype(str).value_counts().head(k)
    return " | ".join([f"{idx} ({n})" for idx, n in vc.items()])


# =======================
# PERFIL DO ARQUIVO
# =======================
def profile_file(path: Path):
    """
    Retorna:
      resumo: 1 linha por arquivo (inclui obrigatórios)
      campos: N linhas por arquivo (1 por coluna)
    """
    categoria = path.parent.name
    tema = path.stem
    filename = path.name  # obrigatório

    df = read_sample(path, nrows=DEFAULT_SAMPLE_ROWS)

    size_mb = path.stat().st_size / (1024 * 1024)
    nrows = count_rows_fast(path)
    ncols = len(df.columns)

    # obrigatório: lista de colunas
    cols_list = [str(c) for c in df.columns]
    cols_joined = COLS_JOIN_SEP.join(cols_list)

    resumo = {
        # obrigatórios:
        "arquivo_nome": filename,
        "tema": tema,
        "categoria": categoria,
        "colunas_lista": cols_joined,

        # úteis:
        "arquivo_path": str(path),
        "tamanho_mb": round(size_mb, 3),
        "linhas_aprox": nrows,
        "n_colunas": ncols,
    }

    campos = []
    for col in df.columns:
        ser = df[col]
        campos.append({
            "arquivo_nome": filename,
            "tema": tema,
            "categoria": categoria,
            "coluna": col,
            "dtype_pandas_amostra": str(ser.dtype),
            "tipo_sugerido": infer_semantic_type(ser, col),
            "pct_nulos_amostra": round(float(ser.isna().mean()) * 100, 2),
            "n_unicos_amostra": int(ser.nunique(dropna=True)),
            "exemplos_top": top_examples(ser, k=3),
        })

    return resumo, campos


# =======================
# MARKDOWN
# =======================
def write_markdown(out_dir: Path, doc_obrigatoria: pd.DataFrame, campos: pd.DataFrame, out_md: Path) -> None:
    lines = []
    lines.append("# Documentação dos Indicadores (processados)\n")
    lines.append(f"Fonte: `{out_dir}`\n")

    lines.append("## Visão geral\n")
    lines.append(f"- Arquivos documentados: **{len(doc_obrigatoria)}**")
    lines.append(f"- Categorias: **{doc_obrigatoria['categoria'].nunique()}**")
    lines.append(f"- Temas únicos: **{doc_obrigatoria['tema'].nunique()}**\n")

    lines.append("## Categorias e número de temas\n")
    cat_counts = doc_obrigatoria.groupby("categoria")["tema"].nunique().sort_values(ascending=False)
    for cat, n in cat_counts.items():
        lines.append(f"- **{cat}**: {n} temas")

    lines.append("\n## Arquivos e colunas\n")
    for _, r in doc_obrigatoria.sort_values(["categoria", "tema", "arquivo_nome"]).iterrows():
        cols = r["colunas_lista"]
        if MAX_COLS_MD is not None and len(cols) > MAX_COLS_MD:
            cols = cols[:MAX_COLS_MD] + " ..."
        lines.append(f"\n### {r['categoria']} / {r['tema']} — `{r['arquivo_nome']}`")
        lines.append(f"- Colunas ({int(r['n_colunas'])}): {cols}")

    lines.append("\n## Alertas de consistência (tipos sugeridos variando)\n")
    type_var = campos.groupby("coluna")["tipo_sugerido"].nunique().sort_values(ascending=False)
    var_cols = type_var[type_var > 1].head(30)

    if var_cols.empty:
        lines.append("- Nenhuma divergência relevante nas colunas mais comuns.")
    else:
        lines.append("- Colunas com tipos sugeridos variando entre arquivos (amostra):")
        for col, ntypes in var_cols.items():
            tlist = ", ".join(sorted(campos.loc[campos["coluna"] == col, "tipo_sugerido"].unique()))
            lines.append(f"  - `{col}`: {ntypes} tipos ({tlist})")

    out_md.write_text("\n".join(lines), encoding="utf-8")


# =======================
# EXECUÇÃO
# =======================
files = [p for p in OUT_DIR.rglob("*.csv") if not p.name.startswith("_")]

if not files:
    raise SystemExit(f"Nenhum CSV encontrado em: {OUT_DIR}")

catalogo_rows = []
campos_rows = []
errors_rows = []

for p in sorted(files):
    try:
        resumo, campos = profile_file(p)
        catalogo_rows.append(resumo)
        campos_rows.extend(campos)
    except Exception as e:
        errors_rows.append({
            "arquivo_nome": p.name,
            "arquivo_path": str(p),
            "categoria": p.parent.name,
            "tema": p.stem,
            "erro": str(e),
        })

# 1) Aba obrigatória: 1 linha por arquivo
doc_obrigatoria = (
    pd.DataFrame(catalogo_rows)
      .sort_values(["categoria", "tema", "arquivo_nome"])
      [["arquivo_nome", "tema", "categoria", "colunas_lista",
        "arquivo_path", "tamanho_mb", "linhas_aprox", "n_colunas"]]
)

# 2) Aba detalhada: 1 linha por coluna
campos_df = (
    pd.DataFrame(campos_rows)
      .sort_values(["categoria", "tema", "arquivo_nome", "coluna"])
)

# 3) Aba de erros
erros_df = pd.DataFrame(errors_rows).sort_values(["categoria", "tema", "arquivo_nome"]) if errors_rows else pd.DataFrame(
    columns=["arquivo_nome","arquivo_path","categoria","tema","erro"]
)

# Exporta Excel
with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as w:
    doc_obrigatoria.to_excel(w, index=False, sheet_name="doc_obrigatoria")
    campos_df.to_excel(w, index=False, sheet_name="campos")
    erros_df.to_excel(w, index=False, sheet_name="erros")

# Exporta Markdown
write_markdown(OUT_DIR, doc_obrigatoria, campos_df, OUT_MD)

print("✅ Gerado:")
print(" -", OUT_XLSX)
print(" -", OUT_MD)
if not erros_df.empty:
    print(f"⚠️ Atenção: {len(erros_df)} arquivo(s) com erro (veja a aba 'erros').")

# Preview (se estiver em notebook)
try:
    display(doc_obrigatoria.head(10))
    display(campos_df.head(10))
    if not erros_df.empty:
        display(erros_df.head(10))
except NameError:
    pass

# %%
