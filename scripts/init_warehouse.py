from __future__ import annotations

from pathlib import Path
import os
import sys

import duckdb

# Garantir que o diretório raiz do projeto esteja no PYTHONPATH
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from configs.settings import get_settings


DDL = [
    # Dimensões mínimas (expansível)
    """
    CREATE TABLE IF NOT EXISTS dim_date (
        date DATE PRIMARY KEY,
        year INTEGER,
        month INTEGER,
        week INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_page (
        page_path TEXT PRIMARY KEY,
        page_title TEXT
    );
    """,
    # Fatos mínimos
    """
    CREATE TABLE IF NOT EXISTS fact_sessions (
        date DATE,
        pageviews BIGINT,
        sessions BIGINT,
        users BIGINT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_events (
        date DATE,
        event_name TEXT,
        event_count BIGINT
    );
    """,
]


def main() -> None:
    s = get_settings()
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    print(f"Warehouse inicializado em: {db_path}")


if __name__ == "__main__":
    main()


