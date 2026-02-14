# %% Imports
import re
import csv
import sys
import unicodedata
from pathlib import Path
from typing import Dict, Tuple
from collections import defaultdict, Counter

import pandas as pd
from tqdm import tqdm



# %% Configurações (ajuste aqui)
ROOT_LOCAL = Path(r"C:\Users\luiz.felipe\Desktop\FLP\MapiaEng\GitHub\fas_tsbio\data\Indicadores")
OUT_DIR    = Path(r"C:\Users\luiz.felipe\Desktop\FLP\MapiaEng\GitHub\fas_tsbio\data\Indicadores_processado_por_tema")
OUT_DIR.mkdir(parents=True, exist_ok=True)

MUN_COL = "Código do Município"

# Entrada: separador automático
CSV_SEP_DEFAULT = ";"
SEPS_CANDIDATES = [";", ",", "\t"]

# Saída
OUT_CSV_SEP = ";"          # recomendado para Excel pt-BR
OUT_ENCODING = "utf-8-sig"
EXPORT_CSV = True
EXPORT_EXCEL = True
EXCEL_ENGINE = "openpyxl"
EXCEL_SHEET_NAME = "dados"

# TSBio (municípios esperados)
tsbio = [
    {"Código TSBio": 1, "Nome TSBio": "Altamira", "CD_MUN": ["1500602","1500859","1501725","1504455","1505486","1507805","1508159","1508357"]},
    {"Código TSBio": 2, "Nome TSBio": "Macapá", "CD_MUN": ["1600212","1600303","1600253","1600238","1600535","1600600","1600154","1600055"]},
    {"Código TSBio": 3, "Nome TSBio": "Portel", "CD_MUN": ["1503101","1504505","1505809","1501105"]},
    {"Código TSBio": 4, "Nome TSBio": "Juruá-Tefé", "CD_MUN": ["1301654","1301803","1301407","1301506","1301951","1301001","1304203","1302207","1304260","1300029"]},
    {"Código TSBio": 5, "Nome TSBio": "Rio Branco-Brasileia", "CD_MUN": ["1200401","1200708","1200252","1200104","1200054","1200138","1200807","1200450","1200013","1200385","1200179"]},
    {"Código TSBio": 6, "Nome TSBio": "Salgado-Bragantino", "CD_MUN": ["1508209","1508035","1507961","1507474","1507466","1507409","1507102","1506906","1506609","1506203","1506112","1506104","1505601","1505007","1504406","1504307","1504109","1503200","1502905","1502608","1502202","1501709","1501600","1500909"]},
]

# Processar só alguns temas (vazio/None => processa tudo)
TEMAS_ALVO = []  # exemplo: ["População", "Educação"]

# Debug
DEBUG_TEMAS = True
MODO_LISTAR_TEMAS = False  # True => só lista temas e encerra
DEBUG_TOP_N = 50

# Regras de filtro/validação
FILTRAR_POR_TSBIO = True       # mantém apenas municípios da lista TSBio
REQUIRE_FULL_COVERAGE = True   # só salva temas com todos expected_muns presentes

def safe_filename(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|]+', "_", (name or "").strip())
    name = re.sub(r"\s+", " ", name).strip()
    return name or "_"


def norm_txt(s: str) -> str:
    s = (s or "").strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    return s


def get_categoria_from_path(root: Path, file_path: Path) -> str:
    rel = file_path.relative_to(root)
    # categoria = primeira pasta abaixo do ROOT
    return rel.parts[0] if len(rel.parts) >= 2 else "(raiz)"


def zfill_mun(x) -> str:
    x = re.sub(r"\D+", "", str(x))
    return x.zfill(7) if x else ""


def parse_parts_from_filename(filename: str):
    """
    Divide em até 3 partes pelo padrão ' - ' (com espaços).
    Tema = 2ª parte.

    Ex:
      'Censo 2022 - Alfabetização - Acrelândia (AC).csv'
        -> fonte='Censo 2022', tema='Alfabetização', recorte='Acrelândia (AC)'

      'br_ibge_pevs - Produção Extração Vegetal - BR.csv'
        -> fonte='br_ibge_pevs', tema='Produção Extração Vegetal', recorte='BR'
    """
    name = filename[:-4] if filename.lower().endswith(".csv") else filename
    name = name.strip()

    # split só quando o hífen tem espaços ao redor (evita quebrar 'Rio Branco-Brasileia')
    parts = [p.strip() for p in re.split(r"\s+-\s+", name, maxsplit=2) if p.strip()]

    fonte = parts[0] if len(parts) >= 1 else ""
    tema = parts[1] if len(parts) >= 2 else fonte
    recorte = parts[2] if len(parts) >= 3 else ""

    return fonte, tema, recorte

# Leitura CSV com auto-detect separador + encoding
def detect_csv_sep(path: Path, encoding: str) -> str:
    """Detecta o separador pelo cabeçalho (lida com linha opcional `sep=;`)."""
    try:
        with open(path, "r", encoding=encoding, errors="replace") as f:
            header = f.readline()
            if header.lower().startswith("sep="):
                header = f.readline()
    except Exception:
        return CSV_SEP_DEFAULT

    if not header:
        return CSV_SEP_DEFAULT

    counts = {s: header.count(s) for s in SEPS_CANDIDATES}
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else CSV_SEP_DEFAULT


def read_csv_local(path: Path) -> pd.DataFrame:
    """Lê CSV detectando automaticamente `;` vs `,` (e tab)."""
    last_err = None

    for enc in ("utf-8-sig", "latin1"):
        try:
            sep = detect_csv_sep(path, enc)
            df = pd.read_csv(path, sep=sep, encoding=enc)

            # Sanidade: se não achou a coluna-chave, tenta separadores alternativos
            if MUN_COL not in df.columns:
                for s in SEPS_CANDIDATES:
                    if s == sep:
                        continue
                    try:
                        df2 = pd.read_csv(path, sep=s, encoding=enc)
                        if MUN_COL in df2.columns:
                            df = df2
                            break
                    except Exception:
                        pass

            return df

        except UnicodeDecodeError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            continue

    # Último recurso: tentar o Sniffer do Python
    try:
        with open(path, "r", encoding="latin1", errors="replace") as f:
            sample = f.read(8192)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
            sep = dialect.delimiter
        except Exception:
            sep = CSV_SEP_DEFAULT
        return pd.read_csv(path, sep=sep, encoding="latin1")
    except Exception:
        raise last_err


# Construir mapas TSBio
mun_to_tsbio: Dict[str, Tuple[int, str]] = {}
expected_muns = set()
for t in tsbio:
    for m in t.get("CD_MUN", []):
        mm = zfill_mun(m)
        expected_muns.add(mm)
        mun_to_tsbio[mm] = (int(t["Código TSBio"]), str(t["Nome TSBio"]))


# Pré-scan + debug (listar temas disponíveis)
print("ROOT_LOCAL existe?", ROOT_LOCAL.exists())
print("Municípios esperados (TSBio):", len(expected_muns))
print("Saída:", OUT_DIR)

csv_files = sorted(ROOT_LOCAL.rglob("*.csv"))
print("\nTotal de CSVs encontrados:", len(csv_files))
if csv_files:
    print("Exemplo de arquivo:", csv_files[0])

# categorias (top)
if DEBUG_TEMAS and csv_files:
    counts_cat = Counter(get_categoria_from_path(ROOT_LOCAL, p) for p in csv_files)
    print("\nTop categorias por quantidade:")
    for cat, n in counts_cat.most_common(15):
        print(f" - {cat}: {n}")

temas_norm = {norm_txt(t) for t in TEMAS_ALVO} if TEMAS_ALVO else None

if DEBUG_TEMAS and csv_files:
    tema_counter = Counter()
    tema_raw_map = {}

    for p in csv_files:
        fonte, tema, recorte = parse_parts_from_filename(p.name)
        tn = norm_txt(tema)
        tema_counter[tn] += 1
        tema_raw_map.setdefault(tn, tema)

    total = len(csv_files)
    print("\n========== DEBUG: TEMAS ENCONTRADOS ==========")
    print(f"Total de CSVs: {total}")
    print(f"Temas únicos: {len(tema_counter)}")
    print("\nTop temas por nº de arquivos:")
    for tn, n in tema_counter.most_common(DEBUG_TOP_N):
        print(f" - {tema_raw_map.get(tn, tn)}: {n}")

    if temas_norm:
        print("\nTemas alvo (TEMAS_ALVO):", TEMAS_ALVO)

        will_run = sum(n for tn, n in tema_counter.items() if tn in temas_norm)
        will_skip = total - will_run

        missing_targets = [t for t in TEMAS_ALVO if norm_txt(t) not in tema_counter]
        present_targets = [tema_raw_map[norm_txt(t)] for t in TEMAS_ALVO if norm_txt(t) in tema_counter]

        print(f"\nPrevisão: processar {will_run}/{total} arquivos; pular {will_skip}/{total}")
        if missing_targets:
            print("⚠️ Temas alvo NÃO encontrados:", missing_targets)
        if present_targets:
            print("✅ Temas alvo encontrados:", present_targets)

    print("=============================================\n")

    if MODO_LISTAR_TEMAS:
        print("🛑 MODO_LISTAR_TEMAS=True — encerrando antes do processamento.")
        sys.exit(0)


# %% Processamento (bucket por categoria+fonte+tema)
buckets = defaultdict(list)
errors = []
missing_mun_col = []

processed = 0
skipped = 0
skipped_by_tema = Counter()

# Debug: códigos fora do expected_muns vistos antes do filtro
mun_extra_seen_global = set()

for p in tqdm(csv_files, desc="Lendo CSVs"):
    categoria = get_categoria_from_path(ROOT_LOCAL, p)
    fonte, tema, recorte = parse_parts_from_filename(p.name)

    tn = norm_txt(tema)
    if temas_norm and tn not in temas_norm:
        skipped += 1
        skipped_by_tema[tn] += 1
        continue

    try:
        df = read_csv_local(p)
    except Exception as e:
        errors.append((str(p), str(e)))
        continue

    if MUN_COL not in df.columns:
        missing_mun_col.append(str(p))
        continue

    # Padroniza código mun
    df[MUN_COL] = df[MUN_COL].apply(zfill_mun)

    # remove linhas vazias / totais
    df = df[df[MUN_COL].astype(str).str.len() > 0].copy()

    # Debug extras antes de filtrar
    if expected_muns:
        present_raw = set(df[MUN_COL].dropna().unique())
        mun_extra_seen_global |= (present_raw - expected_muns)

    # Filtro TSBio (só CD_MUN da lista)
    if FILTRAR_POR_TSBIO and expected_muns:
        df = df[df[MUN_COL].isin(expected_muns)].copy()
        if df.empty:
            continue

    # Metadados
    df["Categoria"] = categoria
    df["Fonte"] = fonte
    df["Tema"] = tema

    # Mapeamento TSBio
    df["Código TSBio"] = df[MUN_COL].map(lambda x: mun_to_tsbio.get(x, (None, None))[0])
    df["Nome TSBio"] = df[MUN_COL].map(lambda x: mun_to_tsbio.get(x, (None, None))[1])

    if FILTRAR_POR_TSBIO:
        df = df[df["Código TSBio"].notna()].copy()

    buckets[(categoria, fonte, tema)].append(df)
    processed += 1

print("\nResumo leitura:")
print(" - Buckets (categoria+fonte+tema):", len(buckets))
print(" - Arquivos processados:", processed)
print(" - Arquivos pulados por tema:", skipped)
print(" - Erros de leitura:", len(errors))
print(" - Sem coluna 'Código do Município':", len(missing_mun_col))

if DEBUG_TEMAS and skipped_by_tema:
    print("\nPulados por tema (top):")
    for tn, n in skipped_by_tema.most_common(20):
        print(f" - {tn}: {n}")

if DEBUG_TEMAS and mun_extra_seen_global:
    print("\n⚠️ CD_MUN fora do expected_muns (vistos antes do filtro):", len(mun_extra_seen_global))
    print("Exemplos:", list(sorted(mun_extra_seen_global))[:20])


# %% Exportar por categoria
report = []

for (categoria, fonte, tema), dfs in buckets.items():
    big = pd.concat(dfs, ignore_index=True)

    # validação de cobertura
    if expected_muns:
        present = set(big[MUN_COL].dropna().unique())
        missing = sorted(list(expected_muns - present))
    else:
        present, missing = set(), []

    if REQUIRE_FULL_COVERAGE and missing:
        report.append({
            "categoria": categoria,
            "fonte": fonte,
            "tema": tema,
            "status": "incompleto",
            "n_presentes": len(present),
            "n_esperados": len(expected_muns),
            "faltando_cd_mun": ",".join(missing),
        })
        continue

    # ordenação de colunas (opcional)
    first_cols = ["Código TSBio","Nome TSBio","Categoria","Fonte","Tema",MUN_COL,"Município","Sigla UF"]
    cols = [c for c in first_cols if c in big.columns] + [c for c in big.columns if c not in first_cols]
    big = big[cols]

    cat_dir = OUT_DIR / safe_filename(categoria)
    cat_dir.mkdir(parents=True, exist_ok=True)

    base = safe_filename(tema)
    out_csv_path = cat_dir / f"{base}.csv"
    out_xlsx_path = cat_dir / f"{base}.xlsx"

    big = big.drop(columns=["Fonte", "Recorte"], errors="ignore")

    if EXPORT_CSV:
        big.to_csv(out_csv_path, index=False, sep=OUT_CSV_SEP, encoding=OUT_ENCODING)

    excel_path_info = None
    if EXPORT_EXCEL:
        try:
            big.to_excel(
                out_xlsx_path,
                index=False,
                engine=EXCEL_ENGINE,
                sheet_name=EXCEL_SHEET_NAME,
            )
            excel_path_info = str(out_xlsx_path)
        except Exception as e:
            excel_path_info = f"erro: {e}"
            print(f"⚠️ Falha ao exportar Excel ({out_xlsx_path}): {e}")

    report.append({
        "categoria": categoria,
        "fonte": fonte,
        "tema": tema,
        "status": "ok",
        "arquivo_csv": str(out_csv_path) if EXPORT_CSV else None,
        "arquivo_excel": excel_path_info,
        "linhas": len(big),
        "n_presentes": len(present) if expected_muns else None,
        "n_esperados": len(expected_muns) if expected_muns else None,
    })

rep_df = pd.DataFrame(report).sort_values(["status", "categoria", "fonte", "tema"])
rep_df.to_csv(OUT_DIR / "_relatorio_validacao.csv", index=False, encoding=OUT_ENCODING)

if errors:
    pd.DataFrame(errors, columns=["arquivo", "erro"]).to_csv(
        OUT_DIR / "_erros_leitura.csv", index=False, encoding=OUT_ENCODING
    )

if missing_mun_col:
    pd.DataFrame({"arquivo": missing_mun_col}).to_csv(
        OUT_DIR / "_sem_coluna_codigo_municipio.csv", index=False, encoding=OUT_ENCODING
    )

print("\n✅ Processo finalizado.")
print("Relatório:", OUT_DIR / "_relatorio_validacao.csv")


# %%
