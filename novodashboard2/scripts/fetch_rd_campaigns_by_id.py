from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb

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
from integrations.rd.client import RDClient


def main() -> None:
    parser = argparse.ArgumentParser(description="Busca campanhas RD por IDs e salva métricas em fact_rd_email_campaign.")
    parser.add_argument("--ids", type=str, required=True, help="IDs separados por vírgula. Ex: 18152729,18189629,18189861")
    args = parser.parse_args()

    ids = [s.strip() for s in args.ids.split(",") if s.strip()]
    if not ids:
        raise SystemExit("Nenhum ID fornecido.")

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

    client = RDClient.from_env()
    inserted = 0
    for cid in ids:
        meta = client.fetch_email_campaign_by_id(cid)
        metrics = client.fetch_email_metrics(cid)
        # data de envio pode vir como sent_at/date/send_date; best effort
        send_date = (
            meta.get("send_date") or meta.get("date") or meta.get("sent_at") or meta.get("created_at") or ""
        )
        send_date = (send_date or "")[:10]
        if not send_date:
            # fallback: inserir sem data
            continue
        con.execute(
            "INSERT INTO fact_rd_email_campaign(date, campaignId, sends, opens, clicks) VALUES (?, ?, ?, ?, ?);",
            [send_date, cid, int(metrics.get("sends", 0)), int(metrics.get("opens", 0)), int(metrics.get("clicks", 0))],
        )
        inserted += 1

    con.close()
    print(f"Campanhas processadas: {len(ids)}; inseridas: {inserted}")


if __name__ == "__main__":
    main()


