#!/usr/bin/env python3
"""
================================================================================
 SICONFI — Download de Série Histórica de Receitas e Despesas Orçamentárias
 Municípios dos estados: PA, AC, AM, AP | Período: 2013–2024
================================================================================

 Fontes:
   - Lista de municípios: API do IBGE (rápida e confiável)
   - Dados contábeis: API do Tesouro Nacional (SICONFI/DCA)

 Anexos baixados:
   - DCA Anexo I-C → Receitas Orçamentárias
   - DCA Anexo I-D → Despesas Orçamentárias por Natureza

 Como usar:
   1. pip install requests pandas openpyxl
   2. python siconfi_despesas_receitas_norte.py
   3. Resultados na pasta ./dados_siconfi/

 Recursos:
   - Checkpoint automático (Ctrl+C e rode de novo: retoma de onde parou)
   - Retentativas com backoff exponencial
   - Progresso detalhado no terminal
   - Exportação CSV e Excel
================================================================================
"""

import requests
import pandas as pd
import time
import json
import sys
from datetime import datetime
from pathlib import Path


# ==============================================================================
# CONFIGURAÇÕES (edite aqui conforme necessidade)
# ==============================================================================

# Estados de interesse (sigla: código IBGE da UF)
ESTADOS = {
    "AC": 12,
    "AM": 13,
    "AP": 16,
    "PA": 15,
}

# Período
ANO_INICIO = 2013
ANO_FIM = 2024

# Anexos da DCA a baixar
ANEXOS = {
    "DCA-Anexo I-C": "receitas_orcamentarias",
    "DCA-Anexo I-D": "despesas_orcamentarias",
}

# Diretório de saída
OUTPUT_DIR = Path("./dados_siconfi")
CHECKPOINT_FILE = OUTPUT_DIR / "_checkpoint.json"
LOG_FILE = OUTPUT_DIR / "_erros.log"

# URLs das APIs
IBGE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"
SICONFI_DCA_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/dca"

# Controle de requisições
MAX_RETRIES = 3
RETRY_DELAY = 5          # segundos (backoff: 5, 10, 20)
REQUEST_DELAY = 0.8       # pausa entre requisições ao SICONFI
REQUEST_TIMEOUT = 120      # timeout por requisição (API pode ser lenta)


# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================

def log_erro(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")


def carregar_checkpoint() -> set:
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, "r") as f:
            return set(json.load(f).get("concluidos", []))
    return set()


def salvar_checkpoint(concluidos: set):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"concluidos": list(concluidos),
                    "atualizado": datetime.now().isoformat()}, f)


def get_json(url: str, params: dict = None, timeout: int = REQUEST_TIMEOUT):
    """GET request com retentativas. Retorna dict/list ou None."""
    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)

            if r.status_code == 200:
                return r.json()

            if r.status_code == 404:
                return {"items": []}  # Sem dados (normal)

            if r.status_code == 429:
                espera = 30 * tentativa
                print(f"    ⏳ Rate limited. Esperando {espera}s...")
                time.sleep(espera)
                continue

            log_erro(f"HTTP {r.status_code} | {url} | params={params}")

        except requests.exceptions.Timeout:
            log_erro(f"Timeout | {url} | tentativa {tentativa}/{MAX_RETRIES}")
        except requests.exceptions.ConnectionError:
            log_erro(f"Sem conexão | {url} | tentativa {tentativa}/{MAX_RETRIES}")
        except Exception as e:
            log_erro(f"{type(e).__name__}: {e} | tentativa {tentativa}/{MAX_RETRIES}")

        if tentativa < MAX_RETRIES:
            espera = RETRY_DELAY * (2 ** (tentativa - 1))
            print(f"    ⚠ Falhou. Retentando em {espera}s...")
            time.sleep(espera)

    return None


# ==============================================================================
# ETAPA 1 — LISTA DE MUNICÍPIOS (API do IBGE)
# ==============================================================================

def obter_municipios() -> pd.DataFrame:
    cache = OUTPUT_DIR / "_municipios.csv"
    if cache.exists():
        df = pd.read_csv(cache, dtype={"cod_ibge": str})
        print(f"📋 Municípios (cache): {len(df)} encontrados\n")
        return df

    print("📋 Buscando municípios na API do IBGE...")
    registros = []

    for sigla, cod_uf in sorted(ESTADOS.items()):
        print(f"   {sigla}...", end=" ", flush=True)
        url = IBGE_URL.format(uf=cod_uf)
        data = get_json(url, timeout=30)

        if data is None or not isinstance(data, list):
            print("❌")
            continue

        for m in data:
            registros.append({
                "cod_ibge": str(m["id"]),
                "nome": m["nome"],
                "uf": sigla,
            })
        print(f"✅ {len(data)} municípios")

    if not registros:
        print("\n❌ Nenhum município encontrado! Verifique sua conexão.")
        sys.exit(1)

    df = pd.DataFrame(registros).sort_values(["uf", "nome"]).reset_index(drop=True)
    df.to_csv(cache, index=False)
    print(f"   Total: {len(df)} municípios\n")
    return df


# ==============================================================================
# ETAPA 2 — DOWNLOAD DOS DADOS DCA DO SICONFI
# ==============================================================================

def baixar_dados(municipios: pd.DataFrame):
    anos = list(range(ANO_INICIO, ANO_FIM + 1))
    concluidos = carregar_checkpoint()

    # Montar tarefas pendentes
    tarefas = []
    for _, mun in municipios.iterrows():
        for ano in anos:
            for anexo_api, anexo_label in ANEXOS.items():
                chave = f"{mun['cod_ibge']}|{ano}|{anexo_api}"
                if chave not in concluidos:
                    tarefas.append({
                        "cod_ibge": mun["cod_ibge"],
                        "nome": mun["nome"],
                        "uf": mun["uf"],
                        "ano": ano,
                        "anexo_api": anexo_api,
                        "anexo_label": anexo_label,
                        "chave": chave,
                    })

    ja = len(concluidos)
    total = len(tarefas)

    if total == 0:
        print("✅ Download completo (checkpoint encontrado).\n")
        return

    print(f"📥 {total} requisições pendentes" +
          (f" ({ja} já concluídas)" if ja else "") + "\n")

    # CSVs incrementais
    csv_fh = {}
    needs_header = {}
    for anexo_api, anexo_label in ANEXOS.items():
        p = OUTPUT_DIR / f"_{anexo_label}_raw.csv"
        existe = p.exists() and p.stat().st_size > 0
        csv_fh[anexo_api] = open(p, "a", encoding="utf-8", newline="")
        needs_header[anexo_api] = not existe

    ok = vazio = erro = 0
    t0 = time.time()

    try:
        for i, t in enumerate(tarefas):
            # === Progresso ===
            if i % 20 == 0 or i == total - 1:
                pct = (i + 1) / total * 100
                elapsed = time.time() - t0
                vel = (i + 1) / elapsed if elapsed > 0 else 0
                eta = (total - i - 1) / vel / 60 if vel > 0 else 0
                print(f"  [{i+1:>5}/{total}] {pct:5.1f}%  "
                      f"✓{ok} ○{vazio} ✗{erro}  "
                      f"ETA ~{eta:.0f}min  "
                      f"| {t['uf']}/{t['nome'][:20]}/{t['ano']}/{t['anexo_api'][-3:]}")

            # === Requisição ===
            params = {
                "an_exercicio": t["ano"],
                "no_anexo": t["anexo_api"],
                "id_ente": t["cod_ibge"],
            }

            data = get_json(SICONFI_DCA_URL, params=params)

            if data is not None and "items" in data and data["items"]:
                df = pd.DataFrame(data["items"])
                fh = csv_fh[t["anexo_api"]]
                df.to_csv(fh, index=False, header=needs_header[t["anexo_api"]])
                needs_header[t["anexo_api"]] = False
                fh.flush()
                ok += 1
            elif data is not None:
                vazio += 1
            else:
                erro += 1

            # Checkpoint periódico
            concluidos.add(t["chave"])
            if (i + 1) % 100 == 0:
                salvar_checkpoint(concluidos)

            time.sleep(REQUEST_DELAY)

    except KeyboardInterrupt:
        print("\n\n⏸ Interrompido! Checkpoint salvo. Rode novamente para retomar.\n")

    finally:
        for fh in csv_fh.values():
            fh.close()
        salvar_checkpoint(concluidos)

    elapsed_min = (time.time() - t0) / 60
    print(f"\n📊 Resultado: {ok} com dados | {vazio} sem dados | {erro} erros")
    print(f"   Tempo: {elapsed_min:.1f} minutos\n")


# ==============================================================================
# ETAPA 3 — CONSOLIDAR E EXPORTAR
# ==============================================================================

def consolidar():
    print("📦 Consolidando arquivos finais...\n")

    for anexo_api, anexo_label in ANEXOS.items():
        raw = OUTPUT_DIR / f"_{anexo_label}_raw.csv"
        if not raw.exists() or raw.stat().st_size == 0:
            print(f"  ⚠ Sem dados para {anexo_label}")
            continue

        print(f"  {anexo_label}...", end=" ", flush=True)
        df = pd.read_csv(raw, dtype=str, low_memory=False)

        # Deduplicar
        key_cols = [c for c in ["exercicio", "cod_ibge", "instituicao", "conta", "coluna"]
                    if c in df.columns]
        if key_cols:
            n0 = len(df)
            df = df.drop_duplicates(subset=key_cols, keep="last")
            dup = n0 - len(df)
            if dup:
                print(f"(-{dup} duplicatas)", end=" ")

        # Converter valor
        if "valor" in df.columns:
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

        # CSV
        csv_out = OUTPUT_DIR / f"{anexo_label}_{ANO_INICIO}_{ANO_FIM}.csv"
        df.to_csv(csv_out, index=False, encoding="utf-8-sig")
        print(f"\n    ✅ {csv_out.name} — {len(df):,} linhas")

        # Excel
        try:
            if len(df) <= 1_000_000:
                xlsx_out = OUTPUT_DIR / f"{anexo_label}_{ANO_INICIO}_{ANO_FIM}.xlsx"
                df.to_excel(xlsx_out, index=False, engine="openpyxl")
                print(f"    ✅ {xlsx_out.name}")
            else:
                print(f"    Dividindo Excel por estado ({len(df):,} linhas)...")
                if "cod_ibge" in df.columns:
                    df["_uf"] = df["cod_ibge"].astype(str).str[:2]
                    cod_sigla = {str(v): k for k, v in ESTADOS.items()}
                    for cod, sigla in cod_sigla.items():
                        sub = df[df["_uf"] == cod].drop(columns=["_uf"])
                        if len(sub):
                            xp = OUTPUT_DIR / f"{anexo_label}_{ANO_INICIO}_{ANO_FIM}_{sigla}.xlsx"
                            sub.to_excel(xp, index=False, engine="openpyxl")
                            print(f"    ✅ {xp.name} — {len(sub):,} linhas")
        except Exception as e:
            print(f"    ⚠ Excel falhou ({e}), mas o CSV está OK.")

    # Limpeza
    print("\n🧹 Limpeza...")
    for f in OUTPUT_DIR.glob("_*_raw.csv"):
        f.unlink()
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()

    print(f"\n{'='*70}")
    print(f" ✅ CONCLUÍDO! Arquivos em: {OUTPUT_DIR.resolve()}")
    print(f"{'='*70}\n")


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print()
    print("=" * 70)
    print(" SICONFI — Receitas e Despesas Orçamentárias (DCA)")
    print(f" Estados: {', '.join(sorted(ESTADOS.keys()))}")
    print(f" Período: {ANO_INICIO}–{ANO_FIM}")
    print(f" Anexos:  {', '.join(ANEXOS.keys())}")
    print("=" * 70)
    print()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Municípios
    mun = obter_municipios()
    for sigla in sorted(ESTADOS.keys()):
        print(f"   {sigla}: {len(mun[mun['uf'] == sigla])} municípios")
    print()

    # 2) Download
    baixar_dados(mun)

    # 3) Consolidação
    consolidar()


if __name__ == "__main__":
    main()