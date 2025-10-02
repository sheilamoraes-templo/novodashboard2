from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import List, Tuple

import duckdb
import os
import sys

# Garantir que o pacote 'novodashboard2' esteja no PYTHONPATH
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

try:
    # Carrega .env via settings indiretamente
    from configs.settings import get_settings
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"Falha ao importar settings: {exc}")


def _connect_duckdb() -> duckdb.DuckDBPyConnection:
    settings = get_settings()
    db_path = settings.data_dir / "warehouse" / "warehouse.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def _generate_dates(n_days: int) -> List[date]:
    today = date.today()
    start = today - timedelta(days=n_days - 1)
    return [start + timedelta(days=i) for i in range(n_days)]


def seed_dim_date(con: duckdb.DuckDBPyConnection, dates: List[date]) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date DATE PRIMARY KEY
        );
        """
    )
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM dim_date;")
        con.executemany(
            "INSERT INTO dim_date (date) VALUES (?);",
            [(d.isoformat(),) for d in dates],
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise


def seed_fact_sessions(con: duckdb.DuckDBPyConnection, dates: List[date]) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_sessions (
            date DATE,
            users BIGINT,
            sessions BIGINT,
            pageviews BIGINT
        );
        """
    )
    rows: List[Tuple[str, int, int, int]] = []
    for i, d in enumerate(dates):
        # Padrão simples por dia da semana
        weekday = i % 7
        sessions = 80 + weekday * 12  # 80..152
        users = int(sessions * 0.65)
        pageviews = int(sessions * 1.6)
        rows.append((d.isoformat(), users, sessions, pageviews))
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM fact_sessions;")
        con.executemany(
            "INSERT INTO fact_sessions (date, users, sessions, pageviews) VALUES (?,?,?,?);",
            rows,
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise


def seed_fact_ga4_pages_daily(con: duckdb.DuckDBPyConnection, dates: List[date]) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_ga4_pages_daily (
            date DATE,
            pagePath TEXT,
            pageTitle TEXT,
            screenPageViews BIGINT,
            sessions BIGINT,
            totalUsers BIGINT,
            userEngagementDuration BIGINT
        );
        """
    )
    page_catalog = [
        ("/", "Home"),
        ("/artigos", "Artigos"),
        ("/videos", "Vídeos"),
        ("/sobre", "Sobre"),
        ("/contato", "Contato"),
    ]
    rows: List[Tuple[str, str, str, int, int, int, int]] = []
    for i, d in enumerate(dates):
        base = 50 + (i % 7) * 10
        for j, (path, title) in enumerate(page_catalog):
            pv = max(0, int(base - j * 8))
            ses = max(0, int(pv * 0.6))
            users = max(0, int(ses * 0.8))
            engagement_sec = int(pv * (20 + j * 5))
            rows.append(
                (
                    d.isoformat(),
                    path,
                    title,
                    pv,
                    ses,
                    users,
                    engagement_sec,
                )
            )
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM fact_ga4_pages_daily;")
        con.executemany(
            """
            INSERT INTO fact_ga4_pages_daily
            (date, pagePath, pageTitle, screenPageViews, sessions, totalUsers, userEngagementDuration)
            VALUES (?,?,?,?,?,?,?);
            """,
            rows,
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise


def seed_fact_yt_channel_daily(con: duckdb.DuckDBPyConnection, dates: List[date]) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_yt_channel_daily (
            date DATE,
            views BIGINT,
            estimatedMinutesWatched BIGINT,
            averageViewDuration DOUBLE
        );
        """
    )
    rows: List[Tuple[str, int, int, float]] = []
    for i, d in enumerate(dates):
        weekday = i % 7
        views = 120 + weekday * 18  # 120..228
        minutes = views * 3  # minutos estimados
        avg_view_sec = 180.0 + weekday * 6.0
        rows.append((d.isoformat(), views, minutes, avg_view_sec))
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM fact_yt_channel_daily;")
        con.executemany(
            "INSERT INTO fact_yt_channel_daily (date, views, estimatedMinutesWatched, averageViewDuration) VALUES (?,?,?,?);",
            rows,
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise


def seed_fact_ga4_sessions_by_utm_daily(con: duckdb.DuckDBPyConnection, dates: List[date]) -> None:
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
    utm_rows: List[Tuple[str, str, str, str, int, int]] = []
    campaigns = [
        ("email", "newsletter", "camp_news"),
        ("google", "cpc", "search_brand"),
        ("youtube", "video", "yt_brand"),
    ]
    for i, d in enumerate(dates):
        src, med, camp = campaigns[i % len(campaigns)]
        ses = 40 + (i % 5) * 8
        users = int(ses * 0.7)
        utm_rows.append((d.isoformat(), src, med, camp, ses, users))
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM fact_ga4_sessions_by_utm_daily;")
        con.executemany(
            """
            INSERT INTO fact_ga4_sessions_by_utm_daily
            (date, source, medium, campaign, sessions, users) VALUES (?,?,?,?,?,?);
            """,
            utm_rows,
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise


def main() -> None:
    con = _connect_duckdb()
    try:
        dates = _generate_dates(90)
        seed_dim_date(con, dates)
        seed_fact_sessions(con, dates)
        seed_fact_ga4_pages_daily(con, dates)
        seed_fact_yt_channel_daily(con, dates)
        seed_fact_ga4_sessions_by_utm_daily(con, dates)
    finally:
        con.close()
    print("Dados simulados inseridos com sucesso.")


if __name__ == "__main__":
    main()


