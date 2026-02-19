import re
import unicodedata
from pathlib import Path

import pandas as pd


# =========================================================
# 1) CONFIG: pasta de entrada e saída
# =========================================================
PASTA = Path(r"C:\Users\luiz.felipe\Downloads\SUBVENÇÃO PAGA")
SAIDA_CSV = PASTA / "SUBVENCAO_PAGA_UNIAO_FILTRADA_CD_MUN.csv"


# =========================================================
# 2) Funções utilitárias
# =========================================================
def norm(txt) -> str:
    """Normaliza texto: remove acento, upper, espaço único."""
    if pd.isna(txt):
        return ""
    s = str(txt).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s.upper())
    return s


def extrair_ano_relatorio(nome_arquivo: str):
    """Pega o primeiro YYYY que aparecer no nome do arquivo."""
    m = re.search(r"(19|20)\d{2}", nome_arquivo)
    return int(m.group(0)) if m else None


def ler_arquivo(path: Path) -> pd.DataFrame:
    """
    Lê:
    - .xlsx normal: read_excel
    - .xls da Conab (muitas vezes é HTML): read_html e pega a maior tabela
    """
    suf = path.suffix.lower()

    if suf == ".xlsx":
        return pd.read_excel(path, dtype=str)

    if suf == ".xls":
        # tenta como excel real; se falhar, trata como HTML
        try:
            return pd.read_excel(path, dtype=str)
        except Exception:
            tables = pd.read_html(path, flavor="lxml")
            df = max(tables, key=lambda t: t.shape[0] * t.shape[1]).copy()
            return df.astype(str)

    raise ValueError(f"Extensão não suportada: {path}")


def detectar_colunas(df: pd.DataFrame):
    cols = list(df.columns)

    def find(patterns):
        for p in patterns:
            for c in cols:
                if re.search(p, str(c), flags=re.I):
                    return c
        return None

    uf_col = find([r"UF\s*do\s*Produtor", r"^UF$", r"\bUF\b", r"Estado"])
    mun_col = find([r"Munic[ií]pio", r"Unidade\s*Territorial", r"Localidade"])
    return uf_col, mun_col


# =========================================================
# 3) Lookup CD_MUN (seus 65 municípios)
# =========================================================
MUNICIPIOS = [
    # PA (Altamira)
    ("PA","Altamira","1500602"), ("PA","Anapu","1500859"), ("PA","Brasil Novo","1501725"),
    ("PA","Medicilândia","1504455"), ("PA","Pacajá","1505486"), ("PA","Senador José Porfírio","1507805"),
    ("PA","Uruará","1508159"), ("PA","Vitória do Xingu","1508357"),
    # AP (Macapá)
    ("AP","Cutias","1600212"), ("AP","Macapá","1600303"), ("AP","Itaubal","1600253"),
    ("AP","Ferreira Gomes","1600238"), ("AP","Porto Grande","1600535"), ("AP","Santana","1600600"),
    ("AP","Pedra Branca do Amapari","1600154"), ("AP","Serra do Navio","1600055"),
    # PA (Portel)
    ("PA","Gurupá","1503101"), ("PA","Melgaço","1504505"), ("PA","Portel","1505809"), ("PA","Bagre","1501105"),
    # AM (Juruá-Tefé)
    ("AM","Guajará","1301654"), ("AM","Ipixuna","1301803"), ("AM","Eirunepé","1301407"),
    ("AM","Envira","1301506"), ("AM","Itamarati","1301951"), ("AM","Carauari","1301001"),
    ("AM","Tefé","1304203"), ("AM","Juruá","1302207"), ("AM","Uarini","1304260"), ("AM","Alvarães","1300029"),
    # AC (Rio Branco–Brasiléia)
    ("AC","Rio Branco","1200401"), ("AC","Xapuri","1200708"), ("AC","Epitaciolândia","1200252"),
    ("AC","Brasiléia","1200104"), ("AC","Assis Brasil","1200054"), ("AC","Bujari","1200138"),
    ("AC","Porto Acre","1200807"), ("AC","Senador Guiomard","1200450"), ("AC","Acrelândia","1200013"),
    ("AC","Plácido de Castro","1200385"), ("AC","Capixaba","1200179"),
    # PA (Nordeste Paraense)
    ("PA","Vigia","1508209"), ("PA","Tracuateua","1508035"), ("PA","Terra Alta","1507961"),
    ("PA","São João de Pirabas","1507474"), ("PA","São João da Ponta","1507466"),
    ("PA","São Francisco do Pará","1507409"), ("PA","São Caetano de Odivelas","1507102"),
    ("PA","Santarém Novo","1506906"), ("PA","Santa Maria do Pará","1506609"), ("PA","Salinópolis","1506203"),
    ("PA","Quatipuru","1506112"), ("PA","Primavera","1506104"), ("PA","Peixe-Boi","1505601"),
    ("PA","Nova Timboteua","1505007"), ("PA","Marapanim","1504406"), ("PA","Maracanã","1504307"),
    ("PA","Magalhães Barata","1504109"), ("PA","Igarapé-Açu","1503200"), ("PA","Curuçá","1502905"),
    ("PA","Colares","1502608"), ("PA","Capanema","1502202"), ("PA","Bragança","1501709"),
    ("PA","Bonito","1501600"), ("PA","Augusto Corrêa","1500909"),
]

lk = pd.DataFrame(MUNICIPIOS, columns=["UF_LK", "MUNICIPIO_LK", "CD_MUN"])
lk["MUNICIPIO_NORM"] = lk["MUNICIPIO_LK"].map(norm)


# =========================================================
# 4) Rodar: ler tudo, juntar, filtrar, exportar
# =========================================================
arquivos = sorted(list(PASTA.glob("*.xlsx")) + list(PASTA.glob("*.xls")))

frames = []
for path in arquivos:
    df = ler_arquivo(path)
    df.columns = [str(c).strip() for c in df.columns]

    ano = extrair_ano_relatorio(path.name)
    uf_col, mun_col = detectar_colunas(df)

    if uf_col is None or mun_col is None:
        print(f"[AVISO] Pulei (sem UF/município detectável): {path.name}")
        continue

    # cria CD_MUN se ainda não existir
    if "CD_MUN" not in df.columns:
        df["_UF"] = df[uf_col].astype(str).str.strip().str.upper()
        df["_MUNICIPIO_NORM"] = df[mun_col].map(norm)

        df = df.merge(
            lk[["UF_LK", "MUNICIPIO_NORM", "CD_MUN"]],
            left_on=["_UF", "_MUNICIPIO_NORM"],
            right_on=["UF_LK", "MUNICIPIO_NORM"],
            how="left",
        )

        # limpa colunas auxiliares
        df = df.drop(columns=[c for c in ["_UF","_MUNICIPIO_NORM","UF_LK","MUNICIPIO_NORM"] if c in df.columns])

    # filtra só seus municípios
    df = df[df["CD_MUN"].notna()].copy()

    # adiciona ano (mesmo se None; você pode tratar depois)
    df.insert(0, "ANO_RELATORIO", ano)

    frames.append(df)

final = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

final.to_csv(SAIDA_CSV, sep=";", index=False, encoding="utf-8-sig")
print("OK ->", SAIDA_CSV, "| linhas:", len(final), "| colunas:", final.shape[1])
