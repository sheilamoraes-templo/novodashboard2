from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
import hashlib
import re

import duckdb
import pandas as pd

# Garantir que o diretório raiz do projeto esteja no PYTHONPATH
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from configs.settings import get_settings


def _sha1(text: str) -> str:
    h = hashlib.sha1()
    h.update((text or "").encode("utf-8"))
    return h.hexdigest()[:12]


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c: re.sub(r"\s+", " ", str(c)).strip().lower() for c in df.columns}
    df = df.rename(columns=cols)
    # Mapas comuns PT/EN
    mapping = {
        "date": ["date", "data", "data de envio", "enviado em", "send date", "sent at"],
        "campaign": ["campaign", "campanha", "assunto", "subject", "título"],
        "sends": ["sends", "enviados", "enviadas", "delivered", "sent"],
        "opens": ["opens", "aberturas", "aberturas únicas", "unique opens"],
        "clicks": ["clicks", "cliques", "cliques únicos", "unique clicks"],
        "campaignid": ["campaignid", "campaign_id", "id", "id campanha"],
    }
    rename: dict[str, str] = {}
    for target, candidates in mapping.items():
        for c in candidates:
            if c in df.columns:
                rename[c] = target
                break
    if rename:
        df = df.rename(columns=rename)
    return df


def derive_fields(df: pd.DataFrame) -> pd.DataFrame:
    # campaignId: preferir campaignid; senão, hash do título
    if "campaignid" in df.columns and df["campaignid"].notna().any():
        df["campaignId"] = df["campaignid"].astype(str)
    else:
        title_col = "campaign" if "campaign" in df.columns else None
        df["campaignId"] = df[title_col].astype(str).map(lambda t: f"csv:{_sha1(t)}") if title_col else "csv:unknown"

    # date: tentar converter
    date_col = "date" if "date" in df.columns else None
    if date_col:
        df["date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date.astype(str)
    else:
        df["date"] = pd.NaT

    # métricas: coerção numérica
    for m in ["sends", "opens", "clicks"]:
        if m in df.columns:
            df[m] = pd.to_numeric(df[m], errors="coerce").fillna(0).astype(int)
        else:
            df[m] = 0
    # título
    if "campaign" not in df.columns:
        df["campaign"] = "(sem título)"
    return df[["date", "campaignId", "sends", "opens", "clicks", "campaign"]]


def main() -> None:
    parser = argparse.ArgumentParser(description="Importa CSV de estatísticas de campanhas RD para fact_rd_email_campaign.")
    parser.add_argument("--path", type=str, default=str(Path(ROOT_DIR) / "catalog" / "estatisticas das campanhas.csv"), help="Caminho do CSV exportado do RD")
    args = parser.parse_args()

    csv_path = Path(args.path)
    if not csv_path.exists():
        # tentar qualquer csv na pasta catalog começando com 'estatisticas'
        candidates = sorted((Path(ROOT_DIR) / "catalog").glob("estatisticas*.*"))
        if candidates:
            csv_path = candidates[0]
        else:
            raise SystemExit(f"CSV não encontrado: {args.path}")

    # Leitura robusta
    encodings = ["utf-8", "latin-1", "utf-16"]
    df_raw = None
    for enc in encodings:
        try:
            df_raw = pd.read_csv(csv_path, encoding=enc)
            break
        except Exception:
            continue
    if df_raw is None:
        raise SystemExit(f"Falha ao ler CSV: {csv_path}")

    df_n = normalize_columns(df_raw)
    df = derive_fields(df_n)

    s = get_settings()
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_rd_email_campaign (
            date DATE,
            campaignId TEXT,
            sends BIGINT,
            opens BIGINT,
            clicks BIGINT
        );
        """
    )
    con.execute("BEGIN TRANSACTION;")
    try:
        # Import direto; não apagar tudo para preservar histórico de outras fontes
        con.register("_tmp_rd_csv", df)
        con.execute(
            """
            INSERT INTO fact_rd_email_campaign(date, campaignId, sends, opens, clicks)
            SELECT CAST(date AS DATE), CAST(campaignId AS TEXT), CAST(sends AS BIGINT), CAST(opens AS BIGINT), CAST(clicks AS BIGINT)
            FROM _tmp_rd_csv
            WHERE date IS NOT NULL;
            """
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        con.close()
        raise
    con.close()
    print(f"Importado CSV de campanhas RD: {csv_path}")


if __name__ == "__main__":
    main()


