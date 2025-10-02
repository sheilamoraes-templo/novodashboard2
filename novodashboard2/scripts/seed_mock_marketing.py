from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from typing import List, Tuple

import duckdb

# Garantir PYTHONPATH correto
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from configs.settings import get_settings


def _get_con():
    s = get_settings()
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def _generate_dates(n_days: int) -> List[date]:
    today = date.today()
    start = today - timedelta(days=n_days - 1)
    return [start + timedelta(days=i) for i in range(n_days)]


def seed_map_utm_campaign(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS map_utm_campaign (
            utm_source_norm TEXT,
            utm_medium_norm TEXT,
            utm_campaign_norm TEXT,
            campaignId TEXT
        );
        """
    )
    rows = [
        ("email", "newsletter", "camp_news", "RD-NEWS-001"),
        ("google", "cpc", "search_brand", "SEM-BRAND-001"),
        ("youtube", "video", "yt_brand", "YT-BRAND-001"),
    ]
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM map_utm_campaign;")
        con.executemany(
            "INSERT INTO map_utm_campaign (utm_source_norm, utm_medium_norm, utm_campaign_norm, campaignId) VALUES (?,?,?,?);",
            rows,
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise


def seed_fact_rd_email_campaign(con: duckdb.DuckDBPyConnection, dates: List[date]) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_rd_email_campaign (
            campaignId TEXT,
            date DATE,
            sends BIGINT,
            opens BIGINT,
            clicks BIGINT
        );
        """
    )
    # Uma campanha a cada 14 dias
    rows: List[Tuple[str, str, int, int, int]] = []
    for i, d in enumerate(dates):
        if i % 14 == 0:
            cid = f"RD-NEWS-001"
            sends = 1000 + (i * 3)
            opens = int(sends * 0.25)
            clicks = int(opens * 0.12)
            rows.append((cid, d.isoformat(), sends, opens, clicks))
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM fact_rd_email_campaign;")
        con.executemany(
            "INSERT INTO fact_rd_email_campaign (campaignId, date, sends, opens, clicks) VALUES (?,?,?,?,?);",
            rows,
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise


def main() -> None:
    con = _get_con()
    try:
        seed_map_utm_campaign(con)
        dates = _generate_dates(90)
        seed_fact_rd_email_campaign(con, dates)
    finally:
        con.close()
    print("Dados de marketing simulados inseridos com sucesso.")


if __name__ == "__main__":
    main()


