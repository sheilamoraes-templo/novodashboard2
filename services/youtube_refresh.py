from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import uuid

import duckdb
import polars as pl

from configs.settings import get_settings
from integrations.youtube.client import YouTubeClient


def _get_db_con():
    s = get_settings()
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def refresh_yt_channel_and_videos(days: int) -> str:
    """Coleta duas visões suportadas pela YouTube Analytics API:
    - Canal diário (dimensions=day)
    - Top vídeos no período (dimensions=video)
    Materializa em fact_yt_channel_daily e fact_yt_video_period.
    """
    end = date.today()
    start = end - timedelta(days=days)
    start_s, end_s = start.isoformat(), end.isoformat()

    yt = YouTubeClient.from_env()
    # Canal diário
    df_day = yt.fetch_channel_daily(start_s, end_s)
    # Top vídeos no período
    df_vid = yt.fetch_top_videos_period(start_s, end_s, max_results=50)

    con = _get_db_con()
    # Tabela canal diário
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
    # Tabela top vídeos período
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_yt_video_period (
            videoId TEXT,
            views BIGINT,
            estimatedMinutesWatched BIGINT,
            averageViewDuration DOUBLE,
            startDate DATE,
            endDate DATE
        );
        """
    )

    con.execute("BEGIN TRANSACTION;")
    try:
        if df_day is not None and df_day.height > 0:
            df_day = df_day.rename({"day": "date"}).with_columns([
                pl.col("date").cast(pl.Utf8),
                pl.col("views").cast(pl.Int64, strict=False),
                pl.col("estimatedMinutesWatched").cast(pl.Int64, strict=False),
                pl.col("averageViewDuration").cast(pl.Float64, strict=False),
            ])
            tmp1 = f"_tmp_yt_day_{uuid.uuid4().hex}"
            con.register(tmp1, df_day.to_pandas())
            con.execute(
                "DELETE FROM fact_yt_channel_daily WHERE date BETWEEN ? AND ?;",
                [start_s, end_s],
            )
            con.execute(
                f"INSERT INTO fact_yt_channel_daily SELECT CAST(date AS DATE), views, estimatedMinutesWatched, averageViewDuration FROM {tmp1};"
            )

        if df_vid is not None and df_vid.height > 0:
            df_vid = df_vid.rename({"video": "videoId"}).with_columns([
                pl.col("videoId").cast(pl.Utf8),
                pl.col("views").cast(pl.Int64, strict=False),
                pl.col("estimatedMinutesWatched").cast(pl.Int64, strict=False),
                pl.col("averageViewDuration").cast(pl.Float64, strict=False),
            ])
            df_vid = df_vid.with_columns([
                pl.lit(start_s).alias("startDate"),
                pl.lit(end_s).alias("endDate"),
            ])
            tmp2 = f"_tmp_yt_vid_{uuid.uuid4().hex}"
            con.register(tmp2, df_vid.to_pandas())
            con.execute(
                "DELETE FROM fact_yt_video_period WHERE startDate = ? AND endDate = ?;",
                [start_s, end_s],
            )
            con.execute(
                f"INSERT INTO fact_yt_video_period SELECT videoId, views, estimatedMinutesWatched, averageViewDuration, CAST(startDate AS DATE), CAST(endDate AS DATE) FROM {tmp2};"
            )

        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        con.close()
        raise
    con.close()
    return f"YouTube: canal diário e top vídeos atualizados para {start_s}..{end_s}"


