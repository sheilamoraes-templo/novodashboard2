from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import duckdb
import polars as pl

from configs.settings import get_settings


def _get_duckdb_path() -> Path:
    settings = get_settings()
    return settings.data_dir / "warehouse" / "warehouse.duckdb"


def _ensure_duckdb() -> duckdb.DuckDBPyConnection:
    db_path = _get_duckdb_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    return con


def get_health() -> Dict[str, str]:
    con = _ensure_duckdb()
    res_sessions = con.execute("SELECT COUNT(*) FROM fact_sessions").fetchone() if con else (0,)
    try:
        res_events = con.execute("SELECT COUNT(*) FROM fact_events").fetchone()
    except Exception:
        res_events = (0,)
    try:
        res_countries = con.execute("SELECT COUNT(*) FROM fact_sessions_by_country").fetchone()
    except Exception:
        res_countries = (0,)
    latest_sessions = con.execute("SELECT MAX(date) FROM fact_sessions").fetchone()
    # Fase 1: adicionar freshness para novas tabelas GA4
    latest_ga4_pages = None
    latest_ga4_events = None
    try:
        latest_ga4_pages = con.execute("SELECT MAX(date) FROM fact_ga4_pages_daily").fetchone()
    except Exception:
        latest_ga4_pages = (None,)
    try:
        latest_ga4_events = con.execute("SELECT MAX(date) FROM fact_ga4_events_daily").fetchone()
    except Exception:
        latest_ga4_events = (None,)
    con.close()
    return {
        "rows_fact_sessions": str(res_sessions[0] if res_sessions else 0),
        "rows_fact_events": str(res_events[0] if res_events else 0),
        "rows_fact_sessions_by_country": str(res_countries[0] if res_countries else 0),
        "latest_date_fact_sessions": str(latest_sessions[0]) if latest_sessions else "None",
        "latest_date_ga4_pages_daily": str(latest_ga4_pages[0]) if latest_ga4_pages else "None",
        "latest_date_ga4_events_daily": str(latest_ga4_events[0]) if latest_ga4_events else "None",
    }


def get_kpis(start_date: str, end_date: str) -> Dict[str, float]:
    con = _ensure_duckdb()
    query = """
        SELECT
          COALESCE(SUM(users), 0) AS users,
          COALESCE(SUM(sessions), 0) AS sessions,
          COALESCE(SUM(pageviews), 0) AS pageviews
        FROM fact_sessions
        WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
    """
    res = con.execute(query, [start_date, end_date]).fetchone()
    con.close()
    users, sessions, pageviews = res if res else (0, 0, 0)
    return {"users": float(users or 0), "sessions": float(sessions or 0), "pageviews": float(pageviews or 0)}


def get_top_pages(start_date: str, end_date: str, limit: int = 10) -> List[Dict[str, str]]:
    con = _ensure_duckdb()
    # Preferir a nova tabela materializada fact_ga4_pages_daily; fallback para fact_sessions agregada
    try:
        query_new = """
            SELECT pagePath, COALESCE(MAX(pageTitle), pagePath) AS pageTitle,
                   COALESCE(SUM(screenPageViews),0) AS pageviews
            FROM fact_ga4_pages_daily
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            GROUP BY pagePath
            ORDER BY pageviews DESC NULLS LAST
            LIMIT ?
        """
        rows = con.execute(query_new, [start_date, end_date, limit]).fetchall()
        if rows:
            con.close()
            return [{"page_path": r[0], "page_title": r[1], "pageviews": int(r[2] or 0)} for r in rows]
    except Exception:
        pass

    query_fallback = """
        SELECT 'NA' as page_path, 'Total' as page_title, COALESCE(SUM(pageviews),0) AS pageviews
        FROM fact_sessions
        WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
        ORDER BY pageviews DESC NULLS LAST
        LIMIT ?
    """
    rows = con.execute(query_fallback, [start_date, end_date, limit]).fetchall()
    con.close()
    return [{"page_path": r[0], "page_title": r[1], "pageviews": int(r[2] or 0)} for r in rows]


def get_pages_weekly_comparison(weeks: int = 8) -> List[Dict[str, str]]:
    con = _ensure_duckdb()
    query = """
        WITH weekly AS (
          SELECT strftime(date, '%Y-%W') AS year_week,
                 COALESCE(SUM(pageviews),0) AS pageviews
          FROM fact_sessions
          GROUP BY 1
          ORDER BY 1 DESC
          LIMIT ?
        )
        SELECT * FROM weekly ORDER BY year_week ASC
    """
    rows = con.execute(query, [weeks]).fetchall()
    con.close()
    return [
        {"year_week": r[0], "pageviews": int(r[1] or 0)} for r in rows
    ]


def get_event_catalog() -> List[Dict[str, str]]:
    # Futuro: ler de dim_event; por ora retorna vazio
    return []


def get_video_funnel(start_date: str, end_date: str) -> Dict[str, int]:
    con = _ensure_duckdb()
    # Preferir fato materializado do GA4; fallback para CSV
    try:
        query_new = """
            SELECT eventName AS event_name, COALESCE(SUM(eventCount),0) AS total
            FROM fact_ga4_events_daily
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              AND eventName IN ('video_start','video_progress')
            GROUP BY 1
        """
        rows = con.execute(query_new, [start_date, end_date]).fetchall()
    except Exception:
        query_old = """
            SELECT event_name, COALESCE(SUM(event_count),0) AS total
            FROM fact_events
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              AND event_name IN ('video_start','video_progress')
            GROUP BY 1
        """
        rows = con.execute(query_old, [start_date, end_date]).fetchall()
    con.close()
    totals = {r[0]: int(r[1] or 0) for r in rows}
    start = totals.get('video_start', 0)
    progress = totals.get('video_progress', 0)
    completion_rate = (progress / start * 100.0) if start > 0 else 0.0
    return {"start": start, "progress": progress, "completion_rate": round(completion_rate, 2)}


def get_top_countries(start_date: str, end_date: str, limit: int = 10) -> List[Dict[str, str]]:
    con = _ensure_duckdb()
    try:
        query = """
            SELECT country_id, COALESCE(SUM(users),0) AS users
            FROM fact_sessions_by_country
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            GROUP BY 1
            ORDER BY users DESC NULLS LAST
            LIMIT ?
        """
        rows = con.execute(query, [start_date, end_date, limit]).fetchall()
        con.close()
        return [{"country_id": r[0], "users": int(r[1] or 0)} for r in rows]
    except Exception:
        con.close()
        return []


def get_top_days(start_date: str, end_date: str) -> List[Dict[str, str]]:
    con = _ensure_duckdb()
    query = """
        SELECT strftime(date, '%w') AS weekday,
               COALESCE(SUM(users),0) AS users
        FROM fact_sessions
        WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
        GROUP BY 1
        ORDER BY users DESC
    """
    rows = con.execute(query, [start_date, end_date]).fetchall()
    con.close()
    return [{"weekday": r[0], "users": int(r[1] or 0)} for r in rows]


