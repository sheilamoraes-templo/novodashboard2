from __future__ import annotations

from datetime import date, timedelta

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
from integrations.ga4.client import GA4Client
from services.ga4_refresh import refresh_events_last_n_days, refresh_pages_last_n_days
from integrations.youtube.client import YouTubeClient


def main() -> None:
    s = get_settings()
    client = GA4Client.from_env()

    # Últimos 30 dias como exemplo inicial
    end = date.today()
    start = end - timedelta(days=30)
    start_s, end_s = start.isoformat(), end.isoformat()

    # Consulta básica: usuários, sessões, pageviews por data
    parquet_path = client.run_report_cached(
        dimensions=["date"],
        metrics=["totalUsers", "sessions", "screenPageViews"],
        start_date=start_s,
        end_date=end_s,
    )
    df = pl.read_parquet(parquet_path)
    if df.is_empty():
        print("Nenhum dado retornado do GA4.")
        return

    # Normalização: renomear colunas para o warehouse
    df = df.rename({
        "date": "date",
        "totalUsers": "users",
        "sessions": "sessions",
        "screenPageViews": "pageviews",
    })
    # Converter YYYYMMDD para YYYY-MM-DD
    if "date" in df.columns:
        df = df.with_columns(
            pl.col("date")
            .cast(pl.Utf8)
            .str.strptime(pl.Date, format="%Y%m%d", strict=False)
            .dt.strftime("%Y-%m-%d")
        )

    # Persistir no DuckDB
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE IF NOT EXISTS fact_sessions (date DATE, pageviews BIGINT, sessions BIGINT, users BIGINT);")
    con.execute("DELETE FROM fact_sessions WHERE date BETWEEN ? AND ?;", [start_s, end_s])
    con.register("_tmp", df.to_pandas())
    con.execute("INSERT INTO fact_sessions SELECT CAST(date AS DATE), pageviews, sessions, users FROM _tmp;")
    con.close()
    print(f"Atualizado fact_sessions para {start_s}..{end_s}")

    # Materializações adicionais da Fase 1
    try:
        msg_e = refresh_events_last_n_days(30)
        print(msg_e)
    except Exception as e:
        print(f"Falha ao materializar eventos: {e}")

    try:
        msg_p = refresh_pages_last_n_days(30)
        print(msg_p)
    except Exception as e:
        print(f"Falha ao materializar páginas: {e}")

    # YouTube: coleta canal diário + top vídeos do período (persistência)
    try:
        from services.youtube_refresh import refresh_yt_channel_and_videos

        msg_yt = refresh_yt_channel_and_videos(30)
        print(msg_yt)
    except Exception as e:
        print(f"YouTube: coleta não realizada ({e})")


if __name__ == "__main__":
    main()


