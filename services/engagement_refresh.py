from __future__ import annotations

from datetime import date, timedelta

import duckdb

from configs.settings import get_settings


def _get_con():
    s = get_settings()
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def materialize_engagement_daily() -> str:
    """Cria/atualiza fact_engagement_daily unindo GA4 e YT por data.

    Colunas: date, sessions, pageviews, views, estimatedMinutesWatched, averageViewDuration
    (engaged_sessions fica para futura disponibilidade)
    """
    con = _get_con()
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_engagement_daily (
            date DATE,
            sessions BIGINT,
            pageviews BIGINT,
            views BIGINT,
            estimatedMinutesWatched BIGINT,
            averageViewDuration DOUBLE
        );
        """
    )
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM fact_engagement_daily;")
        con.execute(
            """
            INSERT INTO fact_engagement_daily
            SELECT
              d.date,
              COALESCE(s.sessions, 0) AS sessions,
              COALESCE(s.pageviews, 0) AS pageviews,
              COALESCE(y.views, 0) AS views,
              COALESCE(y.estimatedMinutesWatched, 0) AS estimatedMinutesWatched,
              COALESCE(y.averageViewDuration, 0.0) AS averageViewDuration
            FROM dim_date d
            LEFT JOIN fact_sessions s USING(date)
            LEFT JOIN fact_yt_channel_daily y USING(date)
            ;
            """
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        con.close()
        raise
    con.close()
    return "Materializado fact_engagement_daily"



