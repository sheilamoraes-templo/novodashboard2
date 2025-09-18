from __future__ import annotations

from datetime import date, timedelta
import uuid

import duckdb
import polars as pl

from configs.settings import get_settings
from integrations.ga4.client import GA4Client


def refresh_sessions_last_n_days(days: int = 30) -> str:
    s = get_settings()
    client = GA4Client.from_env()

    end = date.today()
    start = end - timedelta(days=days)
    start_s, end_s = start.isoformat(), end.isoformat()

    parquet_path = client.run_report_cached(
        dimensions=["date"],
        metrics=["totalUsers", "sessions", "screenPageViews"],
        start_date=start_s,
        end_date=end_s,
        force=True,
    )
    df = pl.read_parquet(parquet_path)
    if df.is_empty():
        return "Nenhum dado retornado do GA4."

    df = df.rename({
        "date": "date",
        "totalUsers": "users",
        "sessions": "sessions",
        "screenPageViews": "pageviews",
    })
    # Normalizar data de YYYYMMDD -> YYYY-MM-DD
    if "date" in df.columns:
        df = df.with_columns(
            pl.col("date")
            .cast(pl.Utf8)
            .str.strptime(pl.Date, format="%Y%m%d", strict=False)
            .dt.strftime("%Y-%m-%d")
        )

    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(
        "CREATE TABLE IF NOT EXISTS fact_sessions (date DATE, pageviews BIGINT, sessions BIGINT, users BIGINT);"
    )
    tmp_name = f"_tmp_{uuid.uuid4().hex}"
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM fact_sessions WHERE date BETWEEN ? AND ?;", [start_s, end_s])
        con.register(tmp_name, df.to_pandas())
        con.execute(
            f"INSERT INTO fact_sessions SELECT CAST(date AS DATE), pageviews, sessions, users FROM {tmp_name};"
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        con.close()
        raise
    con.close()
    return f"Atualizado fact_sessions para {start_s}..{end_s}"


def refresh_sessions_by_utm_last_n_days(days: int = 30) -> str:
    """Materializa sessões/usuários por UTM do GA4 em fact_ga4_sessions_by_utm_daily.

    Colunas: date, source, medium, campaign, sessions, users
    """
    s = get_settings()
    client = GA4Client.from_env()

    end = date.today()
    start = end - timedelta(days=days)
    start_s, end_s = start.isoformat(), end.isoformat()

    parquet_path = client.run_report_cached(
        dimensions=["date", "sessionSource", "sessionMedium", "sessionCampaignName"],
        metrics=["sessions", "totalUsers"],
        start_date=start_s,
        end_date=end_s,
        force=True,
    )
    df = pl.read_parquet(parquet_path)
    if df.is_empty():
        return "Nenhum dado UTM retornado do GA4."

    df = df.rename({
        "date": "date",
        "sessionSource": "source",
        "sessionMedium": "medium",
        "sessionCampaignName": "campaign",
        "sessions": "sessions",
        "totalUsers": "users",
    })
    df = df.with_columns([
        pl.col("sessions").cast(pl.Int64, strict=False),
        pl.col("users").cast(pl.Int64, strict=False),
    ])
    if "date" in df.columns:
        df = df.with_columns(
            pl.col("date")
            .cast(pl.Utf8)
            .str.strptime(pl.Date, format="%Y%m%d", strict=False)
            .dt.strftime("%Y-%m-%d")
        )

    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_ga4_sessions_by_utm_daily (
            date DATE,
            source TEXT,
            medium TEXT,
            campaign TEXT,
            sessions BIGINT,
            users BIGINT
        );
        """
    )
    tmp = f"_tmp_utm_{uuid.uuid4().hex}"
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM fact_ga4_sessions_by_utm_daily WHERE date BETWEEN ? AND ?;", [start_s, end_s])
        con.register(tmp, df.to_pandas())
        con.execute(
            f"""
            INSERT INTO fact_ga4_sessions_by_utm_daily
            SELECT CAST(date AS DATE), CAST(source AS TEXT), CAST(medium AS TEXT), CAST(campaign AS TEXT),
                   CAST(sessions AS BIGINT), CAST(users AS BIGINT)
            FROM {tmp};
            """
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        con.close()
        raise
    con.close()
    return f"Atualizado fact_ga4_sessions_by_utm_daily para {start_s}..{end_s}"


def refresh_events_last_n_days(days: int = 30) -> str:
    """Materializa eventos diários do GA4 em fact_ga4_events_daily.

    Colunas: date, eventName, eventCount, activeUsers
    """
    s = get_settings()
    client = GA4Client.from_env()

    end = date.today()
    start = end - timedelta(days=days)
    start_s, end_s = start.isoformat(), end.isoformat()

    parquet_path = client.run_report_cached(
        dimensions=["date", "eventName"],
        metrics=["eventCount", "activeUsers"],
        start_date=start_s,
        end_date=end_s,
        force=True,
    )
    df = pl.read_parquet(parquet_path)
    if df.is_empty():
        return "Nenhum dado de eventos retornado do GA4."

    # Tipos e nomes normalizados
    # GA4 retorna métricas como float em string; aqui garantimos inteiros quando possível
    df = df.rename({
        "date": "date",
        "eventName": "eventName",
        "eventCount": "eventCount",
        "activeUsers": "activeUsers",
    })
    df = df.with_columns(
        [
            pl.col("eventCount").cast(pl.Int64, strict=False),
            pl.col("activeUsers").cast(pl.Int64, strict=False),
        ]
    )
    if "date" in df.columns:
        df = df.with_columns(
            pl.col("date")
            .cast(pl.Utf8)
            .str.strptime(pl.Date, format="%Y%m%d", strict=False)
            .dt.strftime("%Y-%m-%d")
        )

    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_ga4_events_daily (
            date DATE,
            eventName TEXT,
            eventCount BIGINT,
            activeUsers BIGINT
        );
        """
    )
    tmp_events = f"_tmp_events_{uuid.uuid4().hex}"
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM fact_ga4_events_daily WHERE date BETWEEN ? AND ?;", [start_s, end_s])
        con.register(tmp_events, df.to_pandas())
        con.execute(
            f"""
            INSERT INTO fact_ga4_events_daily
            SELECT CAST(date AS DATE), CAST(eventName AS TEXT), CAST(eventCount AS BIGINT), CAST(activeUsers AS BIGINT)
            FROM {tmp_events};
            """
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        con.close()
        raise
    con.close()
    return f"Atualizado fact_ga4_events_daily para {start_s}..{end_s}"


def refresh_pages_last_n_days(days: int = 30) -> str:
    """Materializa métricas por página diárias do GA4 em fact_ga4_pages_daily.

    Colunas: date, pagePath, pageTitle, screenPageViews, sessions, totalUsers
    """
    s = get_settings()
    client = GA4Client.from_env()

    end = date.today()
    start = end - timedelta(days=days)
    start_s, end_s = start.isoformat(), end.isoformat()

    parquet_path = client.run_report_cached(
        dimensions=["date", "pagePath", "pageTitle"],
        metrics=["screenPageViews", "sessions", "totalUsers"],
        start_date=start_s,
        end_date=end_s,
        force=True,
    )
    df = pl.read_parquet(parquet_path)
    if df.is_empty():
        return "Nenhum dado de páginas retornado do GA4."

    df = df.rename({
        "date": "date",
        "pagePath": "pagePath",
        "pageTitle": "pageTitle",
        "screenPageViews": "screenPageViews",
        "sessions": "sessions",
        "totalUsers": "totalUsers",
    })
    df = df.with_columns(
        [
            pl.col("screenPageViews").cast(pl.Int64, strict=False),
            pl.col("sessions").cast(pl.Int64, strict=False),
            pl.col("totalUsers").cast(pl.Int64, strict=False),
        ]
    )
    if "date" in df.columns:
        df = df.with_columns(
            pl.col("date")
            .cast(pl.Utf8)
            .str.strptime(pl.Date, format="%Y%m%d", strict=False)
            .dt.strftime("%Y-%m-%d")
        )

    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_ga4_pages_daily (
            date DATE,
            pagePath TEXT,
            pageTitle TEXT,
            screenPageViews BIGINT,
            sessions BIGINT,
            totalUsers BIGINT
        );
        """
    )
    tmp_pages = f"_tmp_pages_{uuid.uuid4().hex}"
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM fact_ga4_pages_daily WHERE date BETWEEN ? AND ?;", [start_s, end_s])
        con.register(tmp_pages, df.to_pandas())
        con.execute(
            f"""
            INSERT INTO fact_ga4_pages_daily
            SELECT CAST(date AS DATE), CAST(pagePath AS TEXT), CAST(pageTitle AS TEXT),
                   CAST(screenPageViews AS BIGINT), CAST(sessions AS BIGINT), CAST(totalUsers AS BIGINT)
            FROM {tmp_pages};
            """
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        con.close()
        raise
    con.close()
    return f"Atualizado fact_ga4_pages_daily para {start_s}..{end_s}"


