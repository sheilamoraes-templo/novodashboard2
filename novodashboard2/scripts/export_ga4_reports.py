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
    parser = argparse.ArgumentParser(description="Exporta relatórios GA4 (páginas e UTM) para CSV.")
    parser.add_argument("--start", type=str, default=iso_days_ago(30), help="Data inicial YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=date.today().isoformat(), help="Data final YYYY-MM-DD")
    args = parser.parse_args()

    s = get_settings()
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))

    out_dir = Path(ROOT_DIR) / "catalog"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Relatório de páginas (GA4)
    df_pages = pd.DataFrame(
        con.execute(
            """
            SELECT pagePath,
                   COALESCE(MAX(pageTitle), pagePath) AS pageTitle,
                   SUM(screenPageViews) AS screenPageViews,
                   SUM(sessions) AS sessions,
                   SUM(totalUsers) AS totalUsers
            FROM fact_ga4_pages_daily
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            GROUP BY pagePath
            ORDER BY screenPageViews DESC NULLS LAST
            """,
            [args.start, args.end],
        ).fetchdf()
    )
    out_pages = out_dir / f"ga4_pages_report_{args.start}_{args.end}.csv"
    df_pages.to_csv(out_pages, index=False, encoding="utf-8")

    # Relatório UTM (GA4)
    try:
        df_utm = pd.DataFrame(
            con.execute(
                """
                SELECT CAST(date AS DATE) AS date,
                       COALESCE(source,'') AS source,
                       COALESCE(medium,'') AS medium,
                       COALESCE(campaign,'') AS campaign,
                       SUM(sessions) AS sessions,
                       SUM(users) AS users
                FROM fact_ga4_sessions_by_utm_daily
                WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                GROUP BY 1,2,3,4
                ORDER BY date DESC, sessions DESC NULLS LAST
                """,
                [args.start, args.end],
            ).fetchdf()
        )
    except Exception:
        df_utm = pd.DataFrame(columns=["date", "source", "medium", "campaign", "sessions", "users"])
    out_utm = out_dir / f"ga4_utm_sessions_report_{args.start}_{args.end}.csv"
    df_utm.to_csv(out_utm, index=False, encoding="utf-8")

    con.close()
    print(f"Exportados:\n- {out_pages}\n- {out_utm}")


if __name__ == "__main__":
    main()


