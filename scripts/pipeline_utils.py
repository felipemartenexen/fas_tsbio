"""
pipeline_utils.py
Funções utilitárias compartilhadas pelo pipeline.
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

SEPS_CANDIDATES = [";", ",", "\t"]
CSV_SEP_DEFAULT = ";"

def safe_filename(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|]+', "_", (name or "").strip())
    name = re.sub(r"\s+", " ", name).strip()
    return name or "_"

def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def zfill_mun(x) -> str:
    """
    Converte código de município para string de 7 dígitos com zeros à esquerda.
    
    CORREÇÃO: Trata valores float (ex: 1508159.0) convertendo para int primeiro,
    evitando que o ".0" seja interpretado como dígitos extras.
    
    Problema anterior:
        str(1508159.0) = "1508159.0"
        re.sub(r"\D+", "", "1508159.0") = "15081590"  # ERRADO! 8 dígitos
    
    Solução:
        Converte float para int primeiro, removendo o ".0"
    """
    if pd.isna(x):
        return ""
    
    # Se for float ou int, converte para int primeiro para remover decimais
    if isinstance(x, (int, float)):
        try:
            x = int(x)
        except (ValueError, OverflowError):
            pass
    
    # Converte para string e remove caracteres não-numéricos
    x_str = str(x)
    x_clean = re.sub(r"\D+", "", x_str)
    
    return x_clean.zfill(7) if x_clean else ""

def parse_parts_from_filename(filename: str) -> Tuple[str, str, str]:
    """
    Divide em até 3 partes pelo padrão ' - ' (com espaços).
    Fonte = 1ª parte, Tema = 2ª parte (meio), Recorte = 3ª (resto).
    """
    name = filename[:-4] if filename.lower().endswith(".csv") else filename
    name = name.strip()
    parts = [p.strip() for p in re.split(r"\s+-\s+", name, maxsplit=2) if p.strip()]
    fonte = parts[0] if len(parts) >= 1 else ""
    tema = parts[1] if len(parts) >= 2 else fonte
    recorte = parts[2] if len(parts) >= 3 else ""
    return fonte, tema, recorte

def detect_csv_sep(path: Path, encoding: str) -> str:
    """Detecta o separador a partir do cabeçalho."""
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
    """Leitura robusta (utf-8-sig / latin1) + auto separador."""
    last_err = None
    for enc in ("utf-8-sig", "latin1"):
        try:
            sep = detect_csv_sep(path, enc)
            return pd.read_csv(path, sep=sep, encoding=enc)
        except Exception as e:
            last_err = e
            continue
    raise last_err

# ---------- Dicionário oficial de nomes ----------
_UNIT_SUFFIX_MAP = {
    "%": "perc",
    "percent": "perc",
    "porcentagem": "perc",
    "ha": "ha",
    "hectare": "ha",
    "hectares": "ha",
    "km2": "km2",
    "km²": "km2",
    "m2": "m2",
    "m²": "m2",
    "r$": "rs",
    "rs": "rs",
    "reais": "rs",
    "pessoas": "pessoas",
    "pessoa": "pessoas",
}

_UNIT_PATTERN = re.compile(r"^(.*?)[\s]*[\(\[\{]([^\)\]\}]+)[\)\]\}]\s*$")

def _unit_to_suffix(unit_raw: str) -> str:
    u = (unit_raw or "").strip().lower()
    if not u:
        return ""
    u = u.replace(" ", "")
    u = u.replace("R$", "r$").replace("²", "2")
    if "/" in u:
        left, right = u.split("/", 1)
        left_s = _UNIT_SUFFIX_MAP.get(left, slugify(left))
        right_s = _UNIT_SUFFIX_MAP.get(right, slugify(right))
        return f"{left_s}_por_{right_s}".strip("_")
    return _UNIT_SUFFIX_MAP.get(u, slugify(u))

def load_dictionary(path: Path) -> Dict[str, str]:
    """
    CSV esperado:
      canonical,label_pt,synonyms
    - synonyms separado por '|'
    """
    if not path.exists():
        return {}
    df = pd.read_csv(path, encoding="utf-8-sig")
    syn_map: Dict[str, str] = {}
    for _, r in df.iterrows():
        canonical = str(r.get("canonical", "")).strip()
        label_pt = str(r.get("label_pt", "") or "").strip()
        syns = str(r.get("synonyms", "") or "").split("|")
        for s in syns + [label_pt]:
            ss = slugify(s)
            if ss:
                syn_map[ss] = canonical
    return syn_map

def normalize_column_name(col: str, syn_map: Dict[str, str]) -> str:
    """
    1) Se bater no dicionário -> canonical
    2) Senão: snake_case + unidade no sufixo se houver.
    """
    col = (col or "").strip()
    if not col:
        return col

    s = slugify(col)
    if s in syn_map:
        return syn_map[s]

    m = _UNIT_PATTERN.match(col)
    unit_suffix = ""
    base = col
    if m:
        base = m.group(1).strip()
        unit_suffix = _unit_to_suffix(m.group(2).strip())

    base_slug = slugify(base)
    if unit_suffix and not base_slug.endswith("_" + unit_suffix):
        base_slug = f"{base_slug}_{unit_suffix}"
    return base_slug or s

def build_indicador_id(categoria: str, fonte: str, tema: str) -> str:
    return f"{slugify(categoria)}__{slugify(fonte)}__{slugify(tema)}"
