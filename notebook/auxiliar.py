#%%
import pandas as pd

# Carregar o ficheiro original
file_path = r'C:\Users\luiz.felipe\Downloads\tabela8_evolucao mensal admissoes desligamentos.csv'

# Ler o ficheiro sem cabeçalho para tratar as células mescladas
# 'latin1' é comum para ficheiros CAGED/governo, sep=';' é o padrão visualizado
try:
    df_raw = pd.read_csv(file_path, sep=';', header=None, dtype=str, encoding='latin1')
except:
    df_raw = pd.read_csv(file_path, sep=';', header=None, dtype=str, encoding='utf-8')

# A primeira linha contém as datas (ex: Janeiro/2020)
header_row = df_raw.iloc[0]

# Mapeamento de meses para número
meses_map = {
    'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6,
    'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 'Dezembro': 12
}

processed_data = []

# Iterar pelas colunas. O padrão é:
# Coluna 0: Código Município
# Colunas 1 a N: Blocos de 5 colunas (Estoque, Adm, Desl, Saldo, Variação/Separador)
num_cols = df_raw.shape[1]

for i in range(1, num_cols, 5):
    # Verificar se ainda temos colunas suficientes (precisamos de pelo menos 4 dados)
    if i + 3 >= num_cols:
        break
        
    # Ler o cabeçalho da data (apenas na primeira coluna do bloco)
    header_text = str(header_row[i])
    
    if pd.isna(header_text) or header_text.strip() == '' or header_text.lower() == 'nan':
        continue
        
    try:
        # Separar Mês e Ano (ex: "Janeiro/2020")
        parts = header_text.split('/')
        if len(parts) != 2:
            continue
            
        mes_nome = parts[0].strip()
        ano = int(parts[1].strip())
        mes_num = meses_map.get(mes_nome, 0)
        
        if mes_num == 0: continue

        # Extrair as colunas de dados para este mês
        # Dados começam na linha 2 (índice 2), pois 0 é cabeçalho data, 1 é cabeçalho métrica vazio
        # As colunas são: i=Estoque, i+1=Adm, i+2=Desl, i+3=Saldo
        subset = df_raw.iloc[2:, [0, i, i+1, i+2, i+3]].copy()
        subset.columns = ['Código do Município', 'Estoque', 'Admissões', 'Desligamentos', 'Saldos']
        
        # Adicionar colunas de tempo
        subset['Ano'] = ano
        subset['Mês'] = mes_num
        
        processed_data.append(subset)
        
    except Exception as e:
        continue

# Concatenar tudo
if processed_data:
    df_final = pd.concat(processed_data, ignore_index=True)

    # Limpeza final
    cols_numericas = ['Estoque', 'Admissões', 'Desligamentos', 'Saldos']
    for col in cols_numericas:
        df_final[col] = df_final[col].astype(str).str.replace('.', '', regex=False)
        df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).astype(int)

    # Reordenar colunas
    df_final = df_final[['Código do Município', 'Ano', 'Mês', 'Estoque', 'Admissões', 'Desligamentos', 'Saldos']]
    
    # Remover linhas sem código de município (totais/vazios)
    df_final = df_final[pd.to_numeric(df_final['Código do Município'], errors='coerce').notna()]
    
    # Ordenar
    df_final = df_final.sort_values(by=['Código do Município', 'Ano', 'Mês'])

    # Salvar
    df_final.to_csv('tabela_reorganizada.csv', index=False, sep=';', encoding='utf-8-sig')
    print("Arquivo processado com sucesso.")
else:
    print("Nenhum dado pôde ser processado. Verifique o formato do arquivo.")

#%% Ajustar censo AGRO
import re
from pathlib import Path
import pandas as pd

# ========= CONFIG =========
INPUT_CSV = Path(r"C:\Users\luiz.felipe\Downloads\censo_agro_basico_2017_v2.csv")  # <- ajuste
OUT_DIR = Path(r"C:\Users\luiz.felipe\Downloads\censo_agro_2017_por_coluna_v2")       # <- ajuste
OUT_DIR.mkdir(parents=True, exist_ok=True)

MUN_COL = "Código do Município"
OUT_SEP = ";"          # bom para Excel pt-BR
OUT_ENCODING = "utf-8-sig"

# Se quiser forçar quais colunas virarão arquivos, coloque aqui (opcional).
# Se deixar [] ou None, exporta todas (exceto MUN_COL).
COLUNAS_ALVO = [
    "Estabelecimento Agropecuário",
    "Área média (ha)",
    "Média de pessoal ocupado por estabelecimento",
    "Média da área de lavouras por adubadeira (ha)",
    "Média da área de lavouras por colheitadeira (ha)",
    "Média da área de lavouras por semeadeira (ha)",
    "Média da área de lavouras por trator (ha)",
    "Atividade-Lavoura Temporária (%)",
    "Atividade-Lavoura Permanente (%)",
    "Atividade-Pecuária (%)",
    "Atividade-Horticultura&Floricultura (%)",
    "Atividade-Sementes&Mudas (%)",
    "Atividade-Produção Florestal (%)",
    "Atividade-Pesca (%)",
    "Atividade-Aquicultura (%)",
    "Uso das terras-Lavoura (%)",
    "Uso das terras-Pastagem (%)",
    "Aves-Corte (%)",
    "Aves-Ovos (%)",
    "Bovinos-Corte (%)",
    "Bovinos-Leite (%)",
    "Rendimento-Arroz (kg/ha)",
    "Rendimento-Cana (kg/ha)",
    "Rendimento-Mandioca (kg/ha)",
    "Rendimento-Milho (kg/ha)",
    "Rendimento-Soja (kg/ha)",
    "Rendimento-Trigo (kg/ha)",
    "Rendimento-Cacau (kg/ha)",
    "Rendimento-Café (kg/ha)",
    "Rendimento-Laranja (kg/ha)",
    "Rendimento-Uva (kg/ha)",
    "Carga de Bovinos (n/ha)",
    "Cisterna (%)",
    "Utilização de Agrotóxicos (%)",
    "Despesa com Agrotóxicos (%)",
    "Uso de irrigação (%)",
    "Assistência Técnica (%)",
    "Agricultura familiar (%)",
    "Produtor com escolaridade até Ensino Fundamental (%)",
]
# =========================


def sniff_sep_and_skiprows(path: Path):
    """
    Detecta separador (;, , ou tab) e identifica se existe linha inicial 'sep=;'
    (muito comum em CSV exportado pelo Excel).
    Retorna (sep, skiprows).
    """
    for enc in ("utf-8-sig", "latin1"):
        try:
            with open(path, "r", encoding=enc, errors="replace") as f:
                first = f.readline()
                second = f.readline()

            skiprows = 0
            header_line = first

            if first.lower().startswith("sep="):
                # a 1ª linha é "sep=;" então o header real é a 2ª linha
                skiprows = 1
                header_line = second

            counts = {s: header_line.count(s) for s in [";", ",", "\t"]}
            best = max(counts, key=counts.get)
            sep = best if counts[best] > 0 else ";"
            return sep, skiprows
        except Exception:
            pass

    return ";", 0


def safe_filename(name: str) -> str:
    """Remove caracteres ruins pra Windows e encurta."""
    name = (name or "").strip()
    name = re.sub(r"\s+", " ", name)

    name = name.replace("%", "pct")
    name = name.replace("/", "-")
    name = name.replace("\\", "-")
    name = re.sub(r"[()]", "", name)
    name = re.sub(r'[<>:"|?*]+', "_", name)

    name = name.strip()
    return name[:160] if len(name) > 160 else name


def normalize_cols(cols):
    """Padroniza espaços duplicados e remove BOM estranhos."""
    out = []
    for c in cols:
        c = str(c).replace("\ufeff", "").strip()
        c = re.sub(r"\s+", " ", c)
        out.append(c)
    return out


def normalize_mun_code(series: pd.Series) -> pd.Series:
    """
    Normaliza código IBGE municipal para 7 dígitos.
    Corrige o problema clássico: 1508159.0 -> "1508159" (não vira "15081590").
    Estratégia:
      1) converte para string e remove sufixo .0
      2) extrai o primeiro bloco com exatamente 7 dígitos
      3) fallback: remove não-dígitos e zfill(7) se ficar <=7
      4) valida tamanho final
    """
    raw = series.astype(str).str.strip()

    # remove sufixo decimal típico do float lido como texto (ex.: "1508159.0")
    raw = raw.str.replace(r"\.0+$", "", regex=True)

    # pega exatamente 7 dígitos em qualquer lugar do texto (resolve "15081590" -> "1508159")
    extracted = raw.str.extract(r"(\d{7})", expand=False)

    # fallback: remove tudo que não for dígito
    digits = raw.str.replace(r"\D+", "", regex=True)

    # só aplica zfill se não estiver vazio e tiver <=7 (pra não inventar coisa em códigos grandes)
    digits = digits.where(digits.eq(""), digits.where(digits.str.len() > 7, digits.str.zfill(7)))

    out = extracted.fillna(digits).fillna("")

    # valida: se não for vazio e não tiver 7 dígitos, zera e avisa
    bad = (out != "") & (out.str.len() != 7)
    if bad.any():
        print("⚠️ Atenção: existem códigos fora do padrão (não 7 dígitos). Exemplos:")
        print(raw.loc[bad].head(10).tolist())
        out = out.mask(bad, "")

    return out


# ====== LEITURA ======
sep, skiprows = sniff_sep_and_skiprows(INPUT_CSV)

df = None
last_err = None
for enc in ("utf-8-sig", "latin1"):
    try:
        df = pd.read_csv(
            INPUT_CSV,
            sep=sep,
            encoding=enc,
            low_memory=False,
            skiprows=skiprows
        )
        break
    except Exception as e:
        last_err = e

if df is None:
    raise RuntimeError(f"Falha ao ler {INPUT_CSV}: {last_err}")

df.columns = normalize_cols(df.columns)

# garante que a coluna do município exista
if MUN_COL not in df.columns:
    raise ValueError(
        f"Coluna '{MUN_COL}' não encontrada.\n"
        f"Colunas disponíveis: {list(df.columns)}"
    )

# corrige / normaliza código do município (7 dígitos)
df[MUN_COL] = normalize_mun_code(df[MUN_COL])

# se COLUNAS_ALVO estiver vazia, processa tudo exceto MUN_COL
cols_to_export = COLUNAS_ALVO if COLUNAS_ALVO else [c for c in df.columns if c != MUN_COL]

# valida colunas
missing = [c for c in cols_to_export if c not in df.columns]
if missing:
    print("⚠️ Estas colunas não foram encontradas e serão ignoradas:")
    for c in missing:
        print(" -", c)
    cols_to_export = [c for c in cols_to_export if c in df.columns]

print(f"Separador detectado: '{sep}' | skiprows: {skiprows}")
print(f"Vai gerar {len(cols_to_export)} arquivos em: {OUT_DIR}")

# ====== EXPORTA UM CSV POR COLUNA ======
for col in cols_to_export:
    out_name = f"IBGE Censo Agro 2017 - {safe_filename(col)} - Municípios.csv"
    out_path = OUT_DIR / out_name

    out_df = df[[MUN_COL, col]].copy()
    out_df.to_csv(out_path, index=False, sep=OUT_SEP, encoding=OUT_ENCODING)

print("✅ Concluído.")

# %% Ajustar Vulnerabilidade
from __future__ import annotations

import re
import csv
import shutil
import unicodedata
from pathlib import Path

# =============================
# CONFIG
# =============================
BASE_DIR = Path(r"C:\Users\luiz.felipe\Desktop\FLP\MapiaEng\GitHub\fas_tsbio\data\Indicadores\Vulnerabilidade")
ORIG_DIR = BASE_DIR / "originais"

DRY_RUN = False          # <- confira o log; depois mude para False
OVERWRITE = True       # <- se False, nunca sobrescreve; cria (2), (3)...

# Mapa: "slug do setor (sem acento)" -> "Nome do eixo (bonito)"
EIXO_MAP = {
    "recursos_hidricos": "Recursos Hídricos",
    "seguranca_hidrica": "Recursos Hídricos",  # padrão do AdaptaBrasil
    "seguranca_alimentar": "Segurança Alimentar",
    "seguranca_energetica": "Segurança Energética",
    "infraestrutura_portuaria": "Infraestrutura Portuária",
    "infraestrutura_rodoviaria": "Infraestrutura Rodoviária",
    "infraestrutura_ferroviaria": "Infraestrutura Ferroviária",
    "saude": "Saúde",
    "desastres_geo_hidrologicos": "Desastres Geo-hidrológicos",
    "biodiversidade": "Biodiversidade",
}

# Aliases comuns (variações de escrita)
EIXO_ALIASES = {
    "desastres_geohidrologicos": "desastres_geo_hidrologicos",
    "desastres_geo-hidrologicos": "desastres_geo_hidrologicos",
    "recursos-hidricos": "recursos_hidricos",
    "seguranca_hidrica": "seguranca_hidrica",
}

KNOWN_FORMAT_TOKENS = {"csv", "xlsx", "geojson", "kmz", "shp", "png", "zip"}
INVALID_WIN_CHARS = r'<>:"/\|?*'

LOWER_WORDS = {"do", "da", "de", "dos", "das", "e", "em", "no", "na", "nos", "nas", "para", "por", "a", "o", "as", "os"}


def strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))


def slugify(s: str) -> str:
    s = strip_accents(s).lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def titleish(text: str) -> str:
    """Capitaliza mantendo preposições/artigos em minúsculo."""
    words = re.split(r"(\s+)", text.strip())
    out = []
    for w in words:
        if w.isspace() or w == "":
            out.append(w)
            continue
        lw = w.lower()
        if lw in LOWER_WORDS:
            out.append(lw)
        else:
            out.append(w[:1].upper() + w[1:])
    return "".join(out)


def safe_windows_name(name: str) -> str:
    """Remove caracteres inválidos e normaliza espaços."""
    for ch in INVALID_WIN_CHARS:
        name = name.replace(ch, " ")
    name = re.sub(r"\s+", " ", name).strip()
    return name.rstrip(" .")


def dedupe_path(target: Path) -> Path:
    """Evita colisões criando (2), (3)..."""
    if not target.exists():
        return target
    stem = target.stem
    suffix = target.suffix
    parent = target.parent
    i = 2
    while True:
        candidate = parent / f"{stem} ({i}){suffix}"
        if not candidate.exists():
            return candidate
        i += 1


def match_eixo_prefix(tokens: list[str]) -> tuple[str | None, int]:
    """
    Descobre qual eixo é o prefixo do arquivo.
    Tenta o maior prefixo possível (até 6 tokens).
    Retorna: (nome_eixo_bonito, qtd_tokens_consumidos)
    """
    max_try = min(6, len(tokens))
    for n in range(max_try, 0, -1):
        cand = "_".join(tokens[:n])
        cand_slug = slugify(cand)
        cand_slug = EIXO_ALIASES.get(cand_slug, cand_slug)
        if cand_slug in EIXO_MAP:
            return EIXO_MAP[cand_slug], n
    return None, 0


def build_new_name(stem: str, ext: str) -> str | None:
    tokens = stem.split("_")

    eixo, used = match_eixo_prefix(tokens)
    if not eixo:
        return None

    rest = tokens[used:]

    # encontra o primeiro token só com dígitos = ID do indicador
    id_pos = None
    for i, t in enumerate(rest):
        if t.isdigit():
            id_pos = i
            break
    if id_pos is None:
        return None

    tema_tokens = rest[:id_pos]
    id_token = rest[id_pos]
    tail = rest[id_pos + 1 :]

    # tail esperado: recorte, resolucao, ano, (cenario?), (formato?)
    recorte = tail[0].upper() if len(tail) > 0 else ""
    resol = tail[1] if len(tail) > 1 else ""
    ano = tail[2] if len(tail) > 2 else ""
    cenario = tail[3] if len(tail) > 3 else ""
    formato = tail[4] if len(tail) > 4 else ""

    # corrige casos onde cenário é na verdade o formato
    if cenario.lower() in KNOWN_FORMAT_TOKENS:
        formato = cenario
        cenario = ""

    # remove "csv" redundante (quando já é ..._csv.csv)
    if formato.lower() == ext.lower():
        formato = ""

    tema_raw = " ".join([t.replace("-", " ") for t in tema_tokens]).replace("  ", " ").strip()
    tema = titleish(tema_raw)

    parts_mid = []
    if tema:
        parts_mid.append(tema)
    parts_mid.append(id_token)
    if recorte:
        parts_mid.append(recorte)
    if resol:
        parts_mid.append(resol)
    if ano:
        parts_mid.append(ano)
    if cenario:
        parts_mid.append(cenario)

    mid = " ".join(parts_mid).strip()

    new_name = f"Adapta Brasil {eixo} - {mid} - Municípios.{ext}"
    return safe_windows_name(new_name)


def shorten_if_needed(dest: Path) -> Path:
    """Proteção contra nome muito longo (só no nome do arquivo)."""
    if len(dest.name) <= 240:
        return dest

    ext = dest.suffix.lstrip(".")
    m = re.match(rf"^Adapta Brasil (.+?) - (.+?) - Municípios\.{re.escape(ext)}$", dest.name)
    if not m:
        return dest

    eixo_nome, mid_text = m.group(1), m.group(2)

    max_mid = max(
        20,
        240 - (len("Adapta Brasil ") + len(eixo_nome) + len(" - ") + len(" - Municípios.") + len(ext))
    )
    if len(mid_text) > max_mid:
        mid_text = mid_text[: max_mid - 1] + "…"

    return dest.with_name(safe_windows_name(f"Adapta Brasil {eixo_nome} - {mid_text} - Municípios.{ext}"))


def main() -> None:
    if not BASE_DIR.exists():
        raise FileNotFoundError(f"Pasta BASE não encontrada: {BASE_DIR}")
    if not ORIG_DIR.exists():
        raise FileNotFoundError(f"Pasta ORIGINAIS não encontrada: {ORIG_DIR}")

    src_files = sorted(ORIG_DIR.rglob("*.csv"))
    log_path = BASE_DIR / "_log_copy_renomeacao_adaptabrasil.csv"

    rows = []
    copied = 0
    skipped = 0
    already_ok = 0

    for src in src_files:
        # ignora logs dentro de originais, se existirem
        if src.name.lower().startswith("_log_"):
            continue

        # se alguém colocou arquivo já renomeado em originais, pula
        if src.name.startswith("Adapta Brasil "):
            rows.append([str(src), "", "ALREADY_OK_IN_ORIGINAIS", ""])
            already_ok += 1
            continue

        stem = src.stem
        ext = src.suffix.lstrip(".") or "csv"

        new_name = build_new_name(stem, ext)
        if not new_name:
            rows.append([str(src), "", "SKIP", "Não consegui interpretar eixo/ID pelo nome"])
            skipped += 1
            continue

        dest = BASE_DIR / new_name
        dest = shorten_if_needed(dest)

        if dest.exists() and not OVERWRITE:
            dest = dedupe_path(dest)

        if DRY_RUN:
            rows.append([str(src), str(dest), "DRY_RUN_COPY", ""])
        else:
            try:
                # copia mantendo metadata (data modificação etc.)
                shutil.copy2(src, dest)
                rows.append([str(src), str(dest), "COPIED", ""])
                copied += 1
            except Exception as e:
                rows.append([str(src), str(dest), "ERROR", repr(e)])

    with open(log_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["src_path", "dest_path", "status", "note"])
        w.writerows(rows)

    print(f"Originais encontrados: {len(src_files)}")
    print(f"Copiados: {copied} | Skip: {skipped} | Já OK em originais: {already_ok} | DRY_RUN: {DRY_RUN}")
    print(f"Log: {log_path}")


if __name__ == "__main__":
    main()

# %% Renomear coluna Vulnerabilidade
from __future__ import annotations

import csv
import os
from pathlib import Path

# =============================
# CONFIG
# =============================
BASE_DIR = Path(r"C:\Users\luiz.felipe\Desktop\FLP\MapiaEng\GitHub\fas_tsbio\data\Indicadores\Vulnerabilidade")
DRY_RUN = False  # <- teste primeiro; depois coloque False

OLD_COL = "geocod_ibge"
NEW_COL = "Código do Município"

LOG_PATH = BASE_DIR / "_log_rename_col_geocod_ibge.csv"

SEPS_CANDIDATES = [";", ",", "\t"]


def sniff_sep(path: Path, encoding: str) -> str:
    """Infere separador a partir de um sample (robusto a 'sep=;')."""
    with open(path, "r", encoding=encoding, errors="replace", newline="") as f:
        sample = f.read(8192)

    # remove possível linha "sep=;"
    if sample.lower().startswith("sep="):
        lines = sample.splitlines(True)
        sample = "".join(lines[1:]) if len(lines) > 1 else ""

    # tentativa simples: contar ocorrências no header
    header_line = sample.splitlines()[0] if sample else ""
    counts = {sep: header_line.count(sep) for sep in SEPS_CANDIDATES}
    # escolhe o separador com mais ocorrências
    return max(counts, key=counts.get) if counts else ";"


def try_read_encoding(path: Path) -> str:
    """Escolhe encoding provável sem quebrar."""
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                f.read(4096)
            return enc
        except Exception:
            continue
    return "utf-8-sig"


def rename_header_in_csv(path: Path) -> tuple[str, str]:
    """
    Renomeia coluna no cabeçalho.
    Retorna (status, note).
    """
    enc = try_read_encoding(path)
    sep = sniff_sep(path, enc)

    tmp_path = path.with_suffix(path.suffix + ".tmp")

    with open(path, "r", encoding=enc, errors="replace", newline="") as fin:
        # trata 'sep=;' na primeira linha
        first = fin.readline()
        has_sep_line = first.lower().startswith("sep=")
        if not has_sep_line:
            # volta ao início se não for sep=
            fin.seek(0)

        # leitor CSV
        reader = csv.reader(fin, delimiter=sep)

        try:
            header = next(reader)
        except StopIteration:
            return "SKIP_EMPTY", "arquivo vazio"

        # renomeia (case-insensitive)
        idxs = [i for i, c in enumerate(header) if c.strip().lower() == OLD_COL.lower()]
        if not idxs:
            return "SKIP_NO_COL", f"coluna '{OLD_COL}' não encontrada"

        for i in idxs:
            header[i] = NEW_COL

        if DRY_RUN:
            return "DRY_RUN", f"sep='{sep}' enc='{enc}' renamed={len(idxs)}"

        # escreve de volta (sempre em utf-8-sig para manter acentos bem)
        with open(tmp_path, "w", encoding="utf-8-sig", newline="") as fout:
            if has_sep_line:
                # mantém a dica do Excel
                fout.write(f"sep={sep}\n")
            writer = csv.writer(fout, delimiter=sep, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
            for row in reader:
                writer.writerow(row)

    # troca atômica
    os.replace(tmp_path, path)
    return "UPDATED", f"sep='{sep}' old_enc='{enc}' renamed={len(idxs)}"


def main():
    if not BASE_DIR.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {BASE_DIR}")

    rows = []
    total = 0
    updated = 0
    skipped = 0

    # só pega CSV na RAIZ de Vulnerabilidade (renomeados), não entra em subpastas
    for path in sorted(BASE_DIR.glob("*.csv")):
        name_l = path.name.lower()
        if name_l.startswith("_log_"):
            continue

        total += 1
        status, note = rename_header_in_csv(path)
        rows.append([str(path), status, note])

        if status in ("UPDATED",):
            updated += 1
        elif status.startswith("SKIP"):
            skipped += 1

    # log
    with open(LOG_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["file", "status", "note"])
        w.writerows(rows)

    print(f"Arquivos processados (raiz): {total}")
    print(f"Atualizados: {updated} | Skips: {skipped} | DRY_RUN: {DRY_RUN}")
    print(f"Log: {LOG_PATH}")


if __name__ == "__main__":
    main()
