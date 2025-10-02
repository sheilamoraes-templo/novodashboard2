from __future__ import annotations

from pathlib import Path
import re
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


RE_TOTAL = re.compile(r"^total( geral)?$", re.IGNORECASE)


def normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    rename_map = {
        "Título da página": "page_title",
        "Caminho da página e classe da tela": "page_path",
        "Visualizações": "pageviews",
        "Usuários ativos": "users",
        "Tempo médio de engajamento por sessão": "avg_session_duration",
        "Contagem de eventos": "event_count",
        "Data": "date",
    }
    cols = {c: rename_map.get(c, c) for c in df.columns}
    df = df.rename(cols)
    return df


def drop_totals(df: pl.DataFrame) -> pl.DataFrame:
    if "page_title" in df.columns:
        return df.filter(~pl.col("page_title").cast(pl.Utf8).str.to_lowercase().str.strip().is_in(["total geral", "total"]))
    return df


def to_warehouse(df: pl.DataFrame) -> None:
    s = get_settings()
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE IF NOT EXISTS dim_page (page_path TEXT PRIMARY KEY, page_title TEXT);")
    con.execute(
        "CREATE TABLE IF NOT EXISTS fact_sessions (date DATE, pageviews BIGINT, sessions BIGINT, users BIGINT, avg_session_duration DOUBLE);"
    )
    # Derivar date se presente
    cols = [c for c in ["page_path", "page_title", "pageviews", "users", "avg_session_duration"] if c in df.columns]
    con.register("_tmp", df.select(cols).to_pandas())
    con.execute("INSERT OR REPLACE INTO dim_page SELECT DISTINCT page_path, page_title FROM _tmp;")
    # Para fact_sessions: sem sessões via CSV, manter NULL; acrescentar por pageviews/users
    con.execute(
        "INSERT INTO fact_sessions(date, pageviews, sessions, users, avg_session_duration) SELECT CURRENT_DATE, pageviews, NULL, users, avg_session_duration FROM _tmp;"
    )
    con.close()


def main() -> None:
    s = get_settings()
    raw = s.data_dir / "raw"
    # aceita variações do nome do arquivo
    candidates = [
        raw / "acessos_paginas.csv",
        raw / "Acessos_paginas.csv",
        raw / "acessos_paginas (2).csv",
    ]
    paths = [p for p in candidates if p.exists()]
    if not paths:
        print("Arquivo acessos_paginas.csv não encontrado em data/raw.")
        return
    # Leitura robusta
    from integrations.ga4.csv_fallback import import_ga4_csvs

    df = import_ga4_csvs(paths)
    if df.is_empty():
        print("CSV vazio.")
        return
    df = normalize_columns(df)
    df = drop_totals(df)
    to_warehouse(df)
    print("acessos_paginas.csv importado para warehouse.")


if __name__ == "__main__":
    main()


