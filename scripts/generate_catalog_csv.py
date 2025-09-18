from __future__ import annotations

import csv
from pathlib import Path
import os
import sys

# Ensure project root on path
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import duckdb

from configs.settings import get_settings
from integrations.ga4.client import GA4Client


def main() -> None:
    s = get_settings()
    catalog_dir = Path(ROOT_DIR) / "catalog"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    target = catalog_dir / "datasets_catalog.csv"

    rows: list[dict[str, str]] = []

    # GA4 metadata (dimensions/metrics)
    try:
        ga4 = GA4Client.from_env()
        md = ga4.fetch_metadata()
        for d in md.get("dimensions", []) or []:
            rows.append({
                "source": "GA4",
                "dataset": "ga4_metadata",
                "type": "dimension",
                "name": d,
                "description": "",
            })
        for m in md.get("metrics", []) or []:
            rows.append({
                "source": "GA4",
                "dataset": "ga4_metadata",
                "type": "metric",
                "name": m,
                "description": "",
            })
    except Exception:
        pass

    # DuckDB: list columns of materialized tables
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    if db_path.exists():
        con = duckdb.connect(str(db_path))
        for tbl, src in [
            ("fact_sessions", "GA4"),
            ("fact_ga4_events_daily", "GA4"),
            ("fact_ga4_pages_daily", "GA4"),
            ("fact_yt_channel_daily", "YouTube"),
            ("fact_yt_video_period", "YouTube"),
            ("fact_sessions_by_country", "GA4/CSV"),
            ("fact_events", "GA4/CSV"),
        ]:
            try:
                schema = con.execute(f"PRAGMA table_info('{tbl}')").fetchall()
                for _, name, dtype, *_ in schema:
                    rows.append({
                        "source": src,
                        "dataset": tbl,
                        "type": "column",
                        "name": f"{name}:{dtype}",
                        "description": "",
                    })
            except Exception:
                continue
        con.close()

    # RD Station (planned datasets)
    for ds, fields in [
        ("fact_rd_lead_stage_daily", ["date", "stage", "count"]),
        ("fact_rd_email_campaign", ["date", "campaignId", "sends", "opens", "clicks"]),
    ]:
        for f in fields:
            rows.append({
                "source": "RD",
                "dataset": ds,
                "type": "planned_field",
                "name": f,
                "description": "planned",
            })

    with target.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["source", "dataset", "type", "name", "description"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Catalog gerado em {target} ({len(rows)} linhas)")


if __name__ == "__main__":
    main()


