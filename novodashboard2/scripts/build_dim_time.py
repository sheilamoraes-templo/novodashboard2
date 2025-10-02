from __future__ import annotations

import os
import sys
from datetime import date, timedelta

import duckdb

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


def build_dim_time(n_days: int = 365) -> str:
    con = _get_con()
    # Recria a tabela para garantir o esquema atualizado
    con.execute("DROP TABLE IF EXISTS dim_date;")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date DATE PRIMARY KEY,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            week INTEGER,
            dow INTEGER,
            is_weekend BOOLEAN
        );
        """
    )
    today = date.today()
    start = today - timedelta(days=n_days - 1)
    # Construir registros em Python para evitar problemas de bind em DuckDB
    rows = []
    d = start
    while d <= today:
        year = d.year
        month = d.month
        day = d.day
        week = int(d.strftime("%W"))
        dow = d.weekday()  # 0=Mon .. 6=Sun
        is_weekend = dow in (5, 6)
        rows.append((d.isoformat(), year, month, day, week, dow, is_weekend))
        d += timedelta(days=1)
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM dim_date;")
        con.executemany(
            "INSERT INTO dim_date (date, year, month, day, week, dow, is_weekend) VALUES (?,?,?,?,?,?,?);",
            rows,
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        con.close()
        raise
    con.close()
    return "Materializada dim_date"


def main() -> None:
    print(build_dim_time())


if __name__ == "__main__":
    main()


