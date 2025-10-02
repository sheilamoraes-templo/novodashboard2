from __future__ import annotations

from pathlib import Path
import os
import sys

import duckdb
import polars as pl

# Garantir que o diretório raiz do projeto esteja no PYTHONPATH
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from configs.settings import get_settings
from integrations.ga4.csv_fallback import import_ga4_csvs


def normalize(df: pl.DataFrame, event_name: str) -> pl.DataFrame:
    rename_map = {
        "Título do vídeo": "video_title",
        "Contagem de eventos": "event_count",
    }
    cols = {c: rename_map.get(c, c) for c in df.columns}
    df = df.rename(cols)
    df = df.select([pl.col("video_title").cast(pl.Utf8), pl.col("event_count").cast(pl.Int64, strict=False)])
    df = df.with_columns(pl.lit(event_name).alias("event_name"))
    return df


def to_warehouse(df: pl.DataFrame) -> None:
    if df.is_empty():
        return
    s = get_settings()
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE IF NOT EXISTS dim_video (video_title TEXT PRIMARY KEY);")
    con.execute("CREATE TABLE IF NOT EXISTS fact_events (date DATE, event_name TEXT, event_count BIGINT, video_title TEXT);")
    con.register("_tmp", df.to_pandas())
    con.execute("INSERT OR REPLACE INTO dim_video SELECT DISTINCT video_title FROM _tmp;")
    con.execute("INSERT INTO fact_events(date, event_name, event_count, video_title) SELECT CURRENT_DATE, event_name, event_count, video_title FROM _tmp;")
    con.close()


def main() -> None:
    s = get_settings()
    raw = s.data_dir / "raw"

    # video_start
    vs_paths = [p for p in [raw / "video_start (2).csv", raw / "video_start.csv"] if p.exists()]
    if vs_paths:
        df = import_ga4_csvs(vs_paths)
        df = normalize(df, "video_start")
        to_warehouse(df)
        print("video_start importado.")

    # video_progress
    vp_paths = [p for p in [raw / "video_progress (2).csv", raw / "video_progress.csv"] if p.exists()]
    if vp_paths:
        df = import_ga4_csvs(vp_paths)
        df = normalize(df, "video_progress")
        to_warehouse(df)
        print("video_progress importado.")


if __name__ == "__main__":
    main()


