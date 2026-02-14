#%%
import re
import csv
from pathlib import Path
from typing import Dict, Tuple
import pandas as pd
from tqdm import tqdm

#%%
ROOT_LOCAL = Path(r"C:\Users\luiz.felipe\Desktop\FLP\MapiaEng\GitHub\fas_tsbio\data\Indicadores")
OUT_DIR = Path(r"C:\Users\luiz.felipe\Desktop\FLP\MapiaEng\GitHub\fas_tsbio\data\Indicadores_processado_por_tema")
OUT_DIR.mkdir(parents=True, exist_ok=True)

CSV_SEP_DEFAULT = ";"
SEPS_CANDIDATES = [";", ",", "	"]
MUN_COL = "Código do Município"

tsbio = [
    {"Código TSBio": 1, "Nome TSBio": "Altamira", "CD_MUN": ["1500602","1500859","1501725","1504455","1505486","1507805","1508159","1508357"]},
    {"Código TSBio": 2, "Nome TSBio": "Macapá", "CD_MUN": ["1600212","1600303","1600253","1600238","1600535","1600600","1600154","1600055"]},
    {"Código TSBio": 3, "Nome TSBio": "Portel", "CD_MUN": ["1503101","1504505","1505809","1501105"]},
    {"Código TSBio": 4, "Nome TSBio": "Juruá-Tefé", "CD_MUN": ["1301654","1301803","1301407","1301506","1301951","1301001","1304203","1302207","1304260","1300029"]},
    {"Código TSBio": 5, "Nome TSBio": "Rio Branco-Brasileia", "CD_MUN": ["1200401","1200708","1200252","1200104","1200054","1200138","1200807","1200450","1200013","1200385","1200179"]},
    {"Código TSBio": 6, "Nome TSBio": "Salgado-Bragantino", "CD_MUN": ["1508209","1508035","1507961","1507474","1507466","1507409","1507102","1506906","1506609","1506203","1506112","1506104","1505601","1505007","1504406","1504307","1504109","1503200","1502905","1502608","1502202","1501709","1501600","1500909"]},
]

def zfill_mun(x) -> str:
    x = re.sub(r"\D+", "", str(x))
    return x.zfill(7) if x else ""

mun_to_tsbio: Dict[str, Tuple[int, str]] = {}
expected_muns = set()
for t in tsbio:
    for m in t["CD_MUN"]:
        mm = zfill_mun(m)
        expected_muns.add(mm)
        mun_to_tsbio[mm] = (int(t["Código TSBio"]), str(t["Nome TSBio"]))

print("ROOT_LOCAL existe?", ROOT_LOCAL.exists())
print("Municípios esperados:", len(expected_muns))
print("Saída:", OUT_DIR)

# %%
from collections import Counter

def get_categoria_from_path(root: Path, file_path: Path) -> str:
    rel = file_path.relative_to(root)
    # categoria = primeira pasta abaixo do ROOT
    return rel.parts[0] if len(rel.parts) >= 2 else "(raiz)"

csv_files = sorted(ROOT_LOCAL.rglob("*.csv"))

print("Total de CSVs encontrados:", len(csv_files))
if csv_files:
    print("Exemplo de arquivo:", csv_files[0])
    counts = Counter(get_categoria_from_path(ROOT_LOCAL, p) for p in csv_files)
    print("\nTop 15 categorias por quantidade:")
    for cat, n in counts.most_common(15):
        print(f"{cat}: {n}")



# %%
def parse_tema_from_filename(filename: str) -> str:
    """
    'Censo 2022 - Alfabetização - Plácido de Castro (AC).csv' -> 'Alfabetização'
    """
    name = filename[:-4] if filename.lower().endswith(".csv") else filename
    name = name.strip()
    name = re.sub(r"^Censo 2022\s*-\s*", "", name)
    return name.split(" - ")[0].strip()

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
            dialect = csv.Sniffer().sniff(sample, delimiters=";,	")
            sep = dialect.delimiter
        except Exception:
            sep = CSV_SEP_DEFAULT
        return pd.read_csv(path, sep=sep, encoding="latin1")
    except Exception:
        raise last_err

# %%
from collections import defaultdict

buckets = defaultdict(list)
errors = []
missing_mun_col = []
mun_extra_global = set()

for p in tqdm(csv_files, desc="Lendo CSVs"):
    categoria = get_categoria_from_path(ROOT_LOCAL, p)
    tema = parse_tema_from_filename(p.name)

    try:
        df = read_csv_local(p)
    except Exception as e:
        errors.append((str(p), str(e)))
        continue

    if MUN_COL not in df.columns:
        missing_mun_col.append(str(p))
        continue

    df[MUN_COL] = df[MUN_COL].apply(zfill_mun)

    df["Categoria"] = categoria
    df["Tema"] = tema

    df["Código TSBio"] = df[MUN_COL].map(lambda x: mun_to_tsbio.get(x, (None, None))[0])
    df["Nome TSBio"] = df[MUN_COL].map(lambda x: mun_to_tsbio.get(x, (None, None))[1])

    mun_extra_global |= (set(df[MUN_COL].dropna().unique()) - expected_muns)

    buckets[(categoria, tema)].append(df)

print("Temas encontrados:", len(buckets))
print("Erros de leitura:", len(errors))
print("Sem coluna 'Código do Município':", len(missing_mun_col))

# %% Bloco 5 — Exportar criando pasta por categoria
def safe_filename(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|]+', "_", name.strip())
    name = re.sub(r"\s+", " ", name).strip()
    return name

report = []

for (categoria, tema), dfs in buckets.items():
    big = pd.concat(dfs, ignore_index=True)

    present = set(big[MUN_COL].dropna().unique())
    missing = sorted(list(expected_muns - present))

    if missing:
        report.append({
            "categoria": categoria,
            "tema": tema,
            "status": "incompleto",
            "n_presentes": len(present),
            "n_esperados": len(expected_muns),
            "faltando_cd_mun": ",".join(missing)
        })
        continue  # NÃO salva

    # ordenação de colunas (opcional)
    first_cols = ["Código TSBio","Nome TSBio","Categoria","Tema",MUN_COL,"Município","Sigla UF"]
    cols = [c for c in first_cols if c in big.columns] + [c for c in big.columns if c not in first_cols]
    big = big[cols]

    cat_dir = OUT_DIR / safe_filename(categoria)
    cat_dir.mkdir(parents=True, exist_ok=True)

    out_path = cat_dir / f"{safe_filename(tema)}.csv"
    big.to_csv(out_path, index=False, encoding="utf-8-sig")

    report.append({
        "categoria": categoria,
        "tema": tema,
        "status": "ok",
        "arquivo": str(out_path),
        "linhas": len(big)
    })

rep_df = pd.DataFrame(report).sort_values(["status", "categoria", "tema"])
rep_df.to_csv(OUT_DIR / "_relatorio_validacao.csv", index=False, encoding="utf-8-sig")

print("Processo finalizado.")
print("Saída:", OUT_DIR)
print("Relatório:", OUT_DIR / "_relatorio_validacao.csv")
rep_df.head(20)

# %%
#Logs de problemas (se houver)
if errors:
    pd.DataFrame(errors, columns=["arquivo","erro"]).to_csv(OUT_DIR / "_erros_leitura.csv", index=False, encoding="utf-8-sig")

if missing_mun_col:
    pd.DataFrame({"arquivo": missing_mun_col}).to_csv(OUT_DIR / "_sem_col_municipio.csv", index=False, encoding="utf-8-sig")

if mun_extra_global:
    pd.DataFrame({"cd_mun_extra": sorted(list(mun_extra_global))}).to_csv(OUT_DIR / "_cd_mun_fora_do_tsbio.csv", index=False, encoding="utf-8-sig")

print("Logs auxiliares gerados (se necessário).")

# %%
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
# %%
