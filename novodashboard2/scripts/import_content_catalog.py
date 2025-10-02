from __future__ import annotations

import os
import sys
from pathlib import Path
import argparse

import duckdb
import pandas as pd

# Garantir PYTHONPATH
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


def import_content_catalog(csv_path: Path) -> str:
    con = _get_con()
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_content (
            content_id TEXT PRIMARY KEY,
            title TEXT,
            channel TEXT,
            category TEXT,
            author TEXT,
            duration_sec INTEGER
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS bridge_ga4_content (
            pagePath TEXT,
            content_id TEXT
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS bridge_yt_content (
            videoId TEXT,
            content_id TEXT
        );
        """
    )

    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]

    # dim_content
    dim_cols = ["content_id", "title", "channel", "category", "author", "duration_sec"]
    dim_df = df[[c for c in dim_cols if c in df.columns]].drop_duplicates("content_id")

    # bridges
    bga_df = df[["pagePath", "content_id"]].dropna().drop_duplicates() if set(["pagePath", "content_id"]).issubset(df.columns) else pd.DataFrame(columns=["pagePath", "content_id"]) 
    byt_df = df[["videoId", "content_id"]].dropna().drop_duplicates() if set(["videoId", "content_id"]).issubset(df.columns) else pd.DataFrame(columns=["videoId", "content_id"]) 

    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM dim_content;")
        con.execute("DELETE FROM bridge_ga4_content;")
        con.execute("DELETE FROM bridge_yt_content;")
        con.register("_dim_content", dim_df)
        con.register("_bga", bga_df)
        con.register("_byt", byt_df)
        con.execute("INSERT INTO dim_content SELECT * FROM _dim_content;")
        con.execute("INSERT INTO bridge_ga4_content SELECT * FROM _bga;")
        con.execute("INSERT INTO bridge_yt_content SELECT * FROM _byt;")
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        con.close()
        raise
    con.close()
    return "Importado content_catalog em dim/bridges"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True, help="Caminho para catalog/content_catalog.csv")
    args = ap.parse_args()
    print(import_content_catalog(Path(args.path)))


if __name__ == "__main__":
    main()
