from __future__ import annotations

from pathlib import Path
from typing import Dict, List
from datetime import date, timedelta

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
    try:
        latest_yt = con.execute("SELECT MAX(date) FROM fact_yt_video_daily").fetchone()
    except Exception:
        latest_yt = (None,)
    # Novas tabelas
    try:
        latest_ga4_utm = con.execute("SELECT MAX(date) FROM fact_ga4_sessions_by_utm_daily").fetchone()
    except Exception:
        latest_ga4_utm = (None,)
    try:
        latest_rd_campaign = con.execute("SELECT MAX(date) FROM fact_rd_email_campaign").fetchone()
    except Exception:
        latest_rd_campaign = (None,)
    try:
        latest_engagement = con.execute("SELECT MAX(date) FROM fact_engagement_daily").fetchone()
    except Exception:
        latest_engagement = (None,)
    try:
        latest_comms = con.execute("SELECT MAX(date) FROM fact_comms_impact_daily").fetchone()
    except Exception:
        latest_comms = (None,)
    con.close()
    return {
        "rows_fact_sessions": str(res_sessions[0] if res_sessions else 0),
        "rows_fact_events": str(res_events[0] if res_events else 0),
        "rows_fact_sessions_by_country": str(res_countries[0] if res_countries else 0),
        "latest_date_fact_sessions": str(latest_sessions[0]) if latest_sessions else "None",
        "latest_date_ga4_pages_daily": str(latest_ga4_pages[0]) if latest_ga4_pages else "None",
        "latest_date_ga4_events_daily": str(latest_ga4_events[0]) if latest_ga4_events else "None",
        "latest_date_fact_yt_video_daily": str(latest_yt[0]) if latest_yt else "None",
        "latest_date_fact_ga4_sessions_by_utm_daily": str(latest_ga4_utm[0]) if latest_ga4_utm else "None",
        "latest_date_fact_rd_email_campaign": str(latest_rd_campaign[0]) if latest_rd_campaign else "None",
        "latest_date_fact_engagement_daily": str(latest_engagement[0]) if latest_engagement else "None",
        "latest_date_fact_comms_impact_daily": str(latest_comms[0]) if latest_comms else "None",
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


def get_yt_channel_daily(start_date: str, end_date: str) -> List[Dict[str, str]]:
    con = _ensure_duckdb()
    try:
        rows = con.execute(
            """
            SELECT CAST(date AS DATE) AS date,
                   COALESCE(views,0) AS views,
                   COALESCE(estimatedMinutesWatched,0) AS minutes,
                   COALESCE(averageViewDuration,0.0) AS avg_view_sec
            FROM fact_yt_channel_daily
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            ORDER BY date ASC
            """,
            [start_date, end_date],
        ).fetchall()
        con.close()
        return [
            {
                "date": str(r[0]),
                "views": int(r[1] or 0),
                "estimatedMinutesWatched": int(r[2] or 0),
                "averageViewDuration": float(r[3] or 0.0),
            }
            for r in rows
        ]
    except Exception:
        con.close()
        return []


def get_yt_top_videos(start_date: str, end_date: str, limit: int = 20) -> List[Dict[str, str]]:
    con = _ensure_duckdb()
    try:
        rows = con.execute(
            """
            SELECT videoId,
                   COALESCE(SUM(views),0) AS views,
                   COALESCE(SUM(estimatedMinutesWatched),0) AS minutes,
                   AVG(averageViewDuration) AS avg_view_sec
            FROM fact_yt_video_period
            WHERE startDate = CAST(? AS DATE) OR endDate = CAST(? AS DATE)
            GROUP BY 1
            ORDER BY views DESC NULLS LAST
            LIMIT ?
            """,
            [start_date, end_date, limit],
        ).fetchall()
        con.close()
        return [
            {
                "videoId": r[0],
                "views": int(r[1] or 0),
                "estimatedMinutesWatched": int(r[2] or 0),
                "averageViewDuration": float(r[3] or 0.0),
            }
            for r in rows
        ]
    except Exception:
        con.close()
        return []


def get_engagement_kpis(start_date: str, end_date: str) -> Dict[str, float]:
    """KPIs combinados GA4 + YT a partir de fact_engagement_daily."""
    con = _ensure_duckdb()
    try:
        row = con.execute(
            """
            SELECT
              COALESCE(SUM(sessions),0) AS sessions,
              COALESCE(SUM(pageviews),0) AS pageviews,
              COALESCE(SUM(views),0) AS views,
              COALESCE(SUM(estimatedMinutesWatched),0) AS minutes
            FROM fact_engagement_daily
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            """,
            [start_date, end_date],
        ).fetchone()
        sessions, pageviews, views, minutes = row if row else (0, 0, 0, 0)
        return {
            "sessions": float(sessions or 0),
            "pageviews": float(pageviews or 0),
            "views": float(views or 0),
            "minutes": float(minutes or 0),
        }
    finally:
        con.close()


def get_engagement_series(start_date: str, end_date: str) -> List[Dict[str, str]]:
    """Série temporal com minutos assistidos (YT) e sessões (GA4)."""
    con = _ensure_duckdb()
    try:
        rows = con.execute(
            """
            SELECT CAST(date AS DATE) AS date,
                   COALESCE(sessions,0) AS sessions,
                   COALESCE(estimatedMinutesWatched,0) AS minutes
            FROM fact_engagement_daily
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            ORDER BY date ASC
            """,
            [start_date, end_date],
        ).fetchall()
        return [
            {"date": str(r[0]), "sessions": int(r[1] or 0), "minutes": int(r[2] or 0)} for r in rows
        ]
    finally:
        con.close()


def get_yt_retention_by_video(start_date: str, end_date: str, limit: int = 20) -> List[Dict[str, str]]:
    """Retenção por vídeo (minutos por view) a partir de fact_yt_video_period.

    Observação: a coleta atual grava períodos completos (startDate/endDate). Usamos correspondência por igualdade nas bordas.
    """
    con = _ensure_duckdb()
    try:
        rows = con.execute(
            """
            SELECT
              videoId,
              COALESCE(SUM(estimatedMinutesWatched),0) AS minutes,
              COALESCE(SUM(views),0) AS views,
              CASE WHEN COALESCE(SUM(views),0) > 0
                   THEN COALESCE(SUM(estimatedMinutesWatched),0) * 1.0 / COALESCE(SUM(views),0)
                   ELSE 0.0 END AS min_per_view
            FROM fact_yt_video_period
            WHERE startDate = CAST(? AS DATE) OR endDate = CAST(? AS DATE)
            GROUP BY 1
            ORDER BY minutes DESC NULLS LAST
            LIMIT ?
            """,
            [start_date, end_date, limit],
        ).fetchall()
        return [
            {
                "videoId": r[0],
                "minutes": int(r[1] or 0),
                "views": int(r[2] or 0),
                "min_per_view": float(r[3] or 0.0),
            }
            for r in rows
        ]
    except Exception:
        con.close()
        return []


def get_pages_pareto(start_date: str, end_date: str, limit: int = 50) -> List[Dict[str, str]]:
    """Pareto de páginas (GA4) usando fact_ga4_pages_daily.

    Retorna ranking com page_title, pageviews, cum_share (0..1).
    """
    con = _ensure_duckdb()
    try:
        rows = con.execute(
            """
            WITH agg AS (
              SELECT pagePath,
                     COALESCE(MAX(pageTitle), pagePath) AS pageTitle,
                     COALESCE(SUM(screenPageViews),0) AS pageviews
              FROM fact_ga4_pages_daily
              WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              GROUP BY 1
            ), ranked AS (
              SELECT *,
                     SUM(pageviews) OVER () AS total_pv,
                     SUM(pageviews) OVER (ORDER BY pageviews DESC, pagePath ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_pv
              FROM agg
            )
            SELECT pageTitle, pageviews, (cum_pv * 1.0) / NULLIF(total_pv,0) AS cum_share
            FROM ranked
            ORDER BY pageviews DESC NULLS LAST
            LIMIT ?
            """,
            [start_date, end_date, limit],
        ).fetchall()
        return [
            {"page_title": r[0], "pageviews": int(r[1] or 0), "cum_share": float(r[2] or 0.0)} for r in rows
        ]
    except Exception:
        con.close()
        return []


def get_utm_aggregate(start_date: str, end_date: str, limit: int = 20) -> List[Dict[str, str]]:
    """Agrupa sessões/usuários por source/medium/campaign em fact_ga4_sessions_by_utm_daily."""
    con = _ensure_duckdb()
    try:
        rows = con.execute(
            """
            SELECT COALESCE(source,'(na)') AS source,
                   COALESCE(medium,'(na)') AS medium,
                   COALESCE(campaign,'(na)') AS campaign,
                   COALESCE(SUM(sessions),0) AS sessions,
                   COALESCE(SUM(users),0) AS users
            FROM fact_ga4_sessions_by_utm_daily
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            GROUP BY 1,2,3
            ORDER BY sessions DESC NULLS LAST
            LIMIT ?
            """,
            [start_date, end_date, limit],
        ).fetchall()
        return [
            {"source": r[0], "medium": r[1], "campaign": r[2], "sessions": int(r[3] or 0), "users": int(r[4] or 0)}
            for r in rows
        ]
    finally:
        con.close()


def get_comms_sessions_by_campaign(start_date: str, end_date: str, limit: int = 20) -> List[Dict[str, str]]:
    """Soma sessões por campanha em fact_comms_impact_daily (depende do mapping UTM)."""
    con = _ensure_duckdb()
    try:
        rows = con.execute(
            """
            SELECT campaignId, COALESCE(SUM(sessions),0) AS sessions, COALESCE(SUM(users),0) AS users
            FROM fact_comms_impact_daily
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            GROUP BY 1
            ORDER BY sessions DESC NULLS LAST
            LIMIT ?
            """,
            [start_date, end_date, limit],
        ).fetchall()
        return [
            {"campaignId": r[0], "sessions": int(r[1] or 0), "users": int(r[2] or 0)} for r in rows
        ]
    finally:
        con.close()


def _parse_date(s: str) -> date:
    y, m, d = s.split("-")
    return date(int(y), int(m), int(d))


def get_wow_comparatives(end_date: str) -> Dict[str, Dict[str, float]]:
    """Compara semana corrente (D-6..D) vs. semana anterior (D-13..D-7) para sessions e minutes."""
    d_end = _parse_date(end_date)
    cur_start = d_end - timedelta(days=6)
    prev_end = cur_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=6)
    con = _ensure_duckdb()
    try:
        cur = con.execute(
            "SELECT COALESCE(SUM(sessions),0), COALESCE(SUM(estimatedMinutesWatched),0) FROM fact_engagement_daily WHERE date BETWEEN ? AND ?;",
            [cur_start.isoformat(), d_end.isoformat()],
        ).fetchone() or (0, 0)
        prv = con.execute(
            "SELECT COALESCE(SUM(sessions),0), COALESCE(SUM(estimatedMinutesWatched),0) FROM fact_engagement_daily WHERE date BETWEEN ? AND ?;",
            [prev_start.isoformat(), prev_end.isoformat()],
        ).fetchone() or (0, 0)
    finally:
        con.close()
    cur_ses, cur_min = float(cur[0] or 0), float(cur[1] or 0)
    prv_ses, prv_min = float(prv[0] or 0), float(prv[1] or 0)
    def _pct(a: float, b: float) -> float:
        return ((a - b) / b * 100.0) if b else (0.0 if a == 0 else 100.0)
    return {
        "sessions": {"current": cur_ses, "previous": prv_ses, "delta_abs": cur_ses - prv_ses, "delta_pct": round(_pct(cur_ses, prv_ses), 1)},
        "minutes": {"current": cur_min, "previous": prv_min, "delta_abs": cur_min - prv_min, "delta_pct": round(_pct(cur_min, prv_min), 1)},
    }


def get_mtd_vs_prev_month(end_date: str) -> Dict[str, Dict[str, float]]:
    """Compara MTD vs. mês anterior até o mesmo dia (sessions e minutes)."""
    d_end = _parse_date(end_date)
    cur_start = d_end.replace(day=1)
    day_n = d_end.day
    # mês anterior
    prev_month_end = cur_start - timedelta(days=1)
    prev_start = prev_month_end.replace(day=1)
    prev_end = prev_start + timedelta(days=day_n - 1)
    con = _ensure_duckdb()
    try:
        cur = con.execute(
            "SELECT COALESCE(SUM(sessions),0), COALESCE(SUM(estimatedMinutesWatched),0) FROM fact_engagement_daily WHERE date BETWEEN ? AND ?;",
            [cur_start.isoformat(), d_end.isoformat()],
        ).fetchone() or (0, 0)
        prv = con.execute(
            "SELECT COALESCE(SUM(sessions),0), COALESCE(SUM(estimatedMinutesWatched),0) FROM fact_engagement_daily WHERE date BETWEEN ? AND ?;",
            [prev_start.isoformat(), prev_end.isoformat()],
        ).fetchone() or (0, 0)
    finally:
        con.close()
    cur_ses, cur_min = float(cur[0] or 0), float(cur[1] or 0)
    prv_ses, prv_min = float(prv[0] or 0), float(prv[1] or 0)
    def _pct(a: float, b: float) -> float:
        return ((a - b) / b * 100.0) if b else (0.0 if a == 0 else 100.0)
    return {
        "sessions": {"current": cur_ses, "previous": prv_ses, "delta_abs": cur_ses - prv_ses, "delta_pct": round(_pct(cur_ses, prv_ses), 1)},
        "minutes": {"current": cur_min, "previous": prv_min, "delta_abs": cur_min - prv_min, "delta_pct": round(_pct(cur_min, prv_min), 1)},
    }
