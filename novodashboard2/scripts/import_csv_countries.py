from __future__ import annotations

import duckdb
import polars as pl
import os
import sys

# Garantir que o diretório raiz do projeto esteja no PYTHONPATH
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from configs.settings import get_settings
from integrations.ga4.csv_fallback import import_ga4_csvs


def normalize(df: pl.DataFrame) -> pl.DataFrame:
    rename_map = {
        "ID do país": "country_id",
        "Usuários ativos": "users",
    }
    cols = {c: rename_map.get(c, c) for c in df.columns}
    df = df.rename(cols)
    return df.select(["country_id", pl.col("users").cast(pl.Int64, strict=False)])


def to_warehouse(df: pl.DataFrame) -> None:
    if df.is_empty():
        return
    s = get_settings()
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE IF NOT EXISTS dim_country (country_id TEXT PRIMARY KEY);")
    con.execute("CREATE TABLE IF NOT EXISTS fact_sessions_by_country (date DATE, country_id TEXT, users BIGINT);")
    con.register("_tmp", df.to_pandas())
    con.execute("INSERT OR REPLACE INTO dim_country SELECT DISTINCT country_id FROM _tmp;")
    con.execute("INSERT INTO fact_sessions_by_country(date, country_id, users) SELECT CURRENT_DATE, country_id, users FROM _tmp;")
    con.close()


def main() -> None:
    s = get_settings()
    raw = s.data_dir / "raw"
    paths = [p for p in [raw / "Visão_geral_dos_atributos_do_usuário.csv", raw / "visao_geral_dos_atributos_do_usuario.csv"] if p.exists()]
    if not paths:
        print("Arquivo de atributos do usuário por país não encontrado.")
        return
    df = import_ga4_csvs(paths)
    df = normalize(df)
    to_warehouse(df)
    print("Visão_geral_dos_atributos_do_usuário importado.")


if __name__ == "__main__":
    main()


