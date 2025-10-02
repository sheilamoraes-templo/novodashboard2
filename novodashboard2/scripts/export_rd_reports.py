from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

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


def iso_days_ago(n: int) -> str:
    return (date.today() - timedelta(days=n)).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta relatórios RD Station (campanhas e estágios) para CSV.")
    parser.add_argument("--start", type=str, default=iso_days_ago(30), help="Data inicial YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=date.today().isoformat(), help="Data final YYYY-MM-DD")
    args = parser.parse_args()

    s = get_settings()
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))

    out_dir = Path(ROOT_DIR) / "catalog"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Campanhas de e-mail
    try:
        df_campaigns = pd.DataFrame(
            con.execute(
                """
                SELECT CAST(date AS DATE) AS date,
                       campaignId,
                       SUM(sends) AS sends,
                       SUM(opens) AS opens,
                       SUM(clicks) AS clicks
                FROM fact_rd_email_campaign
                WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                GROUP BY 1,2
                ORDER BY date DESC
                """,
                [args.start, args.end],
            ).fetchdf()
        )
    except Exception:
        df_campaigns = pd.DataFrame(columns=["date", "campaignId", "sends", "opens", "clicks"])
    out_campaigns = out_dir / f"rd_email_campaign_report_{args.start}_{args.end}.csv"
    df_campaigns.to_csv(out_campaigns, index=False, encoding="utf-8")

    # Estágios de leads (se existir)
    try:
        df_stages = pd.DataFrame(
            con.execute(
                """
                SELECT CAST(date AS DATE) AS date,
                       stage,
                       SUM(count) AS count
                FROM fact_rd_lead_stage_daily
                WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                GROUP BY 1,2
                ORDER BY date DESC
                """,
                [args.start, args.end],
            ).fetchdf()
        )
    except Exception:
        df_stages = pd.DataFrame(columns=["date", "stage", "count"])
    out_stages = out_dir / f"rd_lead_stage_report_{args.start}_{args.end}.csv"
    df_stages.to_csv(out_stages, index=False, encoding="utf-8")

    con.close()
    print(f"Exportados:\n- {out_campaigns}\n- {out_stages}")


if __name__ == "__main__":
    main()


