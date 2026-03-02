"""
pipeline_config.py
Configuração central do pipeline TSBio (Produto 1).

➡️ Edite apenas os caminhos abaixo (Windows).
Os scripts 01..04 importam este arquivo.
"""

from pathlib import Path

# ===== AJUSTE AQUI =====
PROJECT_DIR = Path(r"C:\Users\luiz.felipe\Desktop\FLP\MapiaEng\GitHub\fas_tsbio")
DATA_DIR = PROJECT_DIR / "data"

ROOT_RAW = DATA_DIR / "Indicadores"

OUT_PROCESSADO = DATA_DIR / "Indicadores_processado_por_tema"
OUT_PROCESSADO_CSV = OUT_PROCESSADO / "csv"
OUT_PROCESSADO_XLSX = OUT_PROCESSADO / "xlsx"
OUT_DIR = OUT_PROCESSADO / "outputs"

# Dicionário oficial (coloque este arquivo em fas_tsbio\notebook\)
DICT_PATH = PROJECT_DIR / "notebook" / "dicionario_nomes_oficial_tsbio.csv"

# CSV de saída (processados)
OUT_SEP = ";"
OUT_ENCODING = "utf-8-sig"

# Exportar por tema (TdR pediu CSV + XLSX)
EXPORT_PROCESSADO_CSV = True
EXPORT_PROCESSADO_XLSX = True

# Relatórios do processamento (ficam na raiz de Indicadores_processado_por_tema)
RELATORIO_VALIDACAO = OUT_PROCESSADO / "_relatorio_validacao.csv"
RELATORIO_SEM_MUN = OUT_PROCESSADO / "_sem_coluna_cod_municipio.csv"
RELATORIO_ERROS = OUT_PROCESSADO / "_erros_leitura.csv"

# Catálogo / documentação
OUT_CATALOGO_CSV = OUT_DIR / "catalogo_indicadores_tsbio.csv"
OUT_CATALOGO_XLSX = OUT_DIR / "catalogo_indicadores_tsbio.xlsx"
OUT_CATALOGO_CURADO = OUT_DIR / "catalogo_indicadores_tsbio_curado.csv"

OUT_DOC_MD = OUT_DIR / "_documentacao.md"
OUT_DOC_XLSX = OUT_DIR / "_documentacao.xlsx"

# Base consolidada (FULL / DASHBOARD)
OUTPUT_FORMAT_FULL = "csv_gz"   # "parquet" | "csv_gz" | "csv"
OUTPUT_FORMAT_DASH = "parquet"  # "parquet" | "csv_gz" | "csv"
# (compat) Se algum script ainda usar OUTPUT_FORMAT, ele será inferido abaixo.
OUTPUT_FORMAT = OUTPUT_FORMAT_DASH
ONLY_NUMERIC_ROWS = True
DROP_REPEATED_TEXT = True

GENERATE_FULL_BASE = True
GENERATE_DASHBOARD_BASE = True

# Gera uma base DASHBOARD 'RICA' (mantém dimensões extras como produto, classe, etc.)
GENERATE_DASHBOARD_RICH_BASE = True
# Em geral: parquet para Looker/BI
OUTPUT_FORMAT_DASH_RICH = "parquet"  # "parquet" | "csv_gz" | "csv"
# Na base rica, manter textos repetidos (tema/categoria/fonte/arquivo_origem/territorio_nome)?
RICH_KEEP_TEXT_COLUMNS = True
# Incluir automaticamente dimensões extras (todas colunas não-valor fora do id_cols básico)
RICH_INCLUDE_EXTRA_DIMS = True

OUT_BASE_FULL_PARQUET = OUT_DIR / "base_consolidada_tsbio_full.parquet"
OUT_BASE_FULL_CSV_GZ = OUT_DIR / "base_consolidada_tsbio_full.csv.gz"
OUT_BASE_FULL_CSV = OUT_DIR / "base_consolidada_tsbio_full.csv"

OUT_BASE_DASH_PARQUET = OUT_DIR / "base_consolidada_tsbio_dashboard.parquet"
OUT_BASE_DASH_CSV_GZ = OUT_DIR / "base_consolidada_tsbio_dashboard.csv.gz"
OUT_BASE_DASH_CSV = OUT_DIR / "base_consolidada_tsbio_dashboard.csv"

OUT_BASE_DASH_RICH_PARQUET = OUT_DIR / "base_consolidada_tsbio_dashboard_rich.parquet"
OUT_BASE_DASH_RICH_CSV_GZ = OUT_DIR / "base_consolidada_tsbio_dashboard_rich.csv.gz"
OUT_BASE_DASH_RICH_CSV = OUT_DIR / "base_consolidada_tsbio_dashboard_rich.csv"

# Seleção de dashboard (catálogo curado)
DASHBOARD_FLAG_COLUMN = "dashboard"  # sim/nao, 1/0, true/false
# Se ninguém estiver marcado como "sim" no curado, usar fallback automático?
DASHBOARD_USE_FALLBACK = True
# Quantos indicadores pegar no fallback (se habilitado)
DASHBOARD_FALLBACK_MAX = 80
# Manter o CSV intermediário (não comprimido) gerado na consolidação?
# - False: mantém apenas parquet/csv.gz (menos confuso e economiza espaço)
# - True: mantém também o .csv
KEEP_INTERMEDIATE_CSV = False

# ===== TSBio (6 territórios) =====
TSBIO = [
    {"territorio_id": 1, "territorio_nome": "Altamira", "CD_MUN": ["1500602","1500859","1501725","1504455","1505486","1507805","1508159","1508357"]},
    {"territorio_id": 2, "territorio_nome": "Macapá", "CD_MUN": ["1600212","1600303","1600253","1600238","1600535","1600600","1600154","1600055"]},
    {"territorio_id": 3, "territorio_nome": "Portel", "CD_MUN": ["1503101","1504505","1505809","1501105"]},
    {"territorio_id": 4, "territorio_nome": "Juruá-Tefé", "CD_MUN": ["1301654","1301803","1301407","1301506","1301951","1301001","1304203","1302207","1304260","1300029"]},
    {"territorio_id": 5, "territorio_nome": "Rio Branco-Brasiléia", "CD_MUN": ["1200401","1200708","1200252","1200104","1200054","1200138","1200807","1200450","1200013","1200385","1200179"]},
    {"territorio_id": 6, "territorio_nome": "Salgado-Bragantino", "CD_MUN": ["1508209","1508035","1507961","1507474","1507466","1507409","1507102","1506906","1506609","1506203","1506112","1506104","1505601","1505007","1504406","1504307","1504109","1503200","1502905","1502608","1502202","1501709","1501600","1500909"]},
]

# ===== Cadastro oficial de municípios TSBio (IBGE -> Nome/UF) =====
MUNICIPIOS_TSBIO = {
    "1200013": {"municipio_nome": "Acrelândia", "sigla_uf": "AC"},
    "1200054": {"municipio_nome": "Assis Brasil", "sigla_uf": "AC"},
    "1200104": {"municipio_nome": "Brasiléia", "sigla_uf": "AC"},
    "1200138": {"municipio_nome": "Bujari", "sigla_uf": "AC"},
    "1200179": {"municipio_nome": "Capixaba", "sigla_uf": "AC"},
    "1200252": {"municipio_nome": "Epitaciolândia", "sigla_uf": "AC"},
    "1200385": {"municipio_nome": "Plácido de Castro", "sigla_uf": "AC"},
    "1200401": {"municipio_nome": "Rio Branco", "sigla_uf": "AC"},
    "1200450": {"municipio_nome": "Senador Guiomard", "sigla_uf": "AC"},
    "1200708": {"municipio_nome": "Xapuri", "sigla_uf": "AC"},
    "1200807": {"municipio_nome": "Porto Acre", "sigla_uf": "AC"},
    "1300029": {"municipio_nome": "Alvarães", "sigla_uf": "AM"},
    "1301001": {"municipio_nome": "Carauari", "sigla_uf": "AM"},
    "1301407": {"municipio_nome": "Eirunepé", "sigla_uf": "AM"},
    "1301506": {"municipio_nome": "Envira", "sigla_uf": "AM"},
    "1301654": {"municipio_nome": "Guajará", "sigla_uf": "AM"},
    "1301803": {"municipio_nome": "Ipixuna", "sigla_uf": "AM"},
    "1301951": {"municipio_nome": "Itamarati", "sigla_uf": "AM"},
    "1302207": {"municipio_nome": "Juruá", "sigla_uf": "AM"},
    "1304203": {"municipio_nome": "Tefé", "sigla_uf": "AM"},
    "1304260": {"municipio_nome": "Uarini", "sigla_uf": "AM"},
    "1500602": {"municipio_nome": "Altamira", "sigla_uf": "PA"},
    "1500859": {"municipio_nome": "Anapu", "sigla_uf": "PA"},
    "1500909": {"municipio_nome": "Augusto Corrêa", "sigla_uf": "PA"},
    "1501105": {"municipio_nome": "Bagre", "sigla_uf": "PA"},
    "1501600": {"municipio_nome": "Bonito", "sigla_uf": "PA"},
    "1501709": {"municipio_nome": "Bragança", "sigla_uf": "PA"},
    "1501725": {"municipio_nome": "Brasil Novo", "sigla_uf": "PA"},
    "1502202": {"municipio_nome": "Capanema", "sigla_uf": "PA"},
    "1502608": {"municipio_nome": "Colares", "sigla_uf": "PA"},
    "1502905": {"municipio_nome": "Curuçá", "sigla_uf": "PA"},
    "1503101": {"municipio_nome": "Gurupá", "sigla_uf": "PA"},
    "1503200": {"municipio_nome": "Igarapé-Açu", "sigla_uf": "PA"},
    "1504109": {"municipio_nome": "Magalhães Barata", "sigla_uf": "PA"},
    "1504307": {"municipio_nome": "Maracanã", "sigla_uf": "PA"},
    "1504406": {"municipio_nome": "Marapanim", "sigla_uf": "PA"},
    "1504455": {"municipio_nome": "Medicilândia", "sigla_uf": "PA"},
    "1504505": {"municipio_nome": "Melgaço", "sigla_uf": "PA"},
    "1505007": {"municipio_nome": "Nova Timboteua", "sigla_uf": "PA"},
    "1505486": {"municipio_nome": "Pacajá", "sigla_uf": "PA"},
    "1505601": {"municipio_nome": "Peixe-Boi", "sigla_uf": "PA"},
    "1505809": {"municipio_nome": "Portel", "sigla_uf": "PA"},
    "1506104": {"municipio_nome": "Primavera", "sigla_uf": "PA"},
    "1506112": {"municipio_nome": "Quatipuru", "sigla_uf": "PA"},
    "1506203": {"municipio_nome": "Salinópolis", "sigla_uf": "PA"},
    "1506609": {"municipio_nome": "Santa Maria do Pará", "sigla_uf": "PA"},
    "1506906": {"municipio_nome": "Santarém Novo", "sigla_uf": "PA"},
    "1507102": {"municipio_nome": "São Caetano de Odivelas", "sigla_uf": "PA"},
    "1507409": {"municipio_nome": "São Francisco do Pará", "sigla_uf": "PA"},
    "1507466": {"municipio_nome": "São João da Ponta", "sigla_uf": "PA"},
    "1507474": {"municipio_nome": "São João de Pirabas", "sigla_uf": "PA"},
    "1507805": {"municipio_nome": "Senador José Porfírio", "sigla_uf": "PA"},
    "1507961": {"municipio_nome": "Terra Alta", "sigla_uf": "PA"},
    "1508035": {"municipio_nome": "Tracuateua", "sigla_uf": "PA"},
    "1508159": {"municipio_nome": "Uruará", "sigla_uf": "PA"},
    "1508209": {"municipio_nome": "Vigia", "sigla_uf": "PA"},
    "1508357": {"municipio_nome": "Vitória do Xingu", "sigla_uf": "PA"},
    "1600055": {"municipio_nome": "Serra do Navio", "sigla_uf": "AP"},
    "1600154": {"municipio_nome": "Pedra Branca do Amapari", "sigla_uf": "AP"},
    "1600212": {"municipio_nome": "Cutias", "sigla_uf": "AP"},
    "1600238": {"municipio_nome": "Ferreira Gomes", "sigla_uf": "AP"},
    "1600253": {"municipio_nome": "Itaubal", "sigla_uf": "AP"},
    "1600303": {"municipio_nome": "Macapá", "sigla_uf": "AP"},
    "1600535": {"municipio_nome": "Porto Grande", "sigla_uf": "AP"},
    "1600600": {"municipio_nome": "Santana", "sigla_uf": "AP"},
}

def ensure_dirs() -> None:
    OUT_PROCESSADO_CSV.mkdir(parents=True, exist_ok=True)
    OUT_PROCESSADO_XLSX.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
