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


def refresh_yt_video_daily_last_n_days(days: int, channel_id: str) -> str:
    """Coleta métricas diárias por vídeo do YouTube Analytics e materializa em fact_yt_video_daily.

    Colunas: date (DATE), videoId (TEXT), views (BIGINT), estimatedMinutesWatched (BIGINT), averageViewDuration (DOUBLE)
    """
    if not channel_id:
        return "YouTube: channel_id não definido (YT_CHANNEL_ID)."

    end = date.today()
    start = end - timedelta(days=days)
    start_s, end_s = start.isoformat(), end.isoformat()

    yt = YouTubeClient.from_env()
    df = yt.fetch_video_analytics_daily(start_s, end_s, channel_id)
    if df.is_empty():
        return "YouTube: nenhum dado retornado."

    # Espera colunas: day, video, views, estimatedMinutesWatched, averageViewDuration
    # Padronizar nomes e tipos
    df = df.rename({
        "day": "date",
        "video": "videoId",
    })
    # Garantir tipos
    cast_cols = [
        pl.col("date").cast(pl.Utf8),
        pl.col("videoId").cast(pl.Utf8),
        pl.col("views").cast(pl.Int64, strict=False),
        pl.col("estimatedMinutesWatched").cast(pl.Int64, strict=False),
        pl.col("averageViewDuration").cast(pl.Float64, strict=False),
    ]
    df = df.with_columns(cast_cols)

    con = _get_db_con()
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_yt_video_daily (
            date DATE,
            videoId TEXT,
            views BIGINT,
            estimatedMinutesWatched BIGINT,
            averageViewDuration DOUBLE
        );
        """
    )

    tmp_name = f"_tmp_yt_{uuid.uuid4().hex}"
    con.execute("BEGIN TRANSACTION;")
    try:
        con.register(tmp_name, df.to_pandas())
        con.execute(
            f"""
            DELETE FROM fact_yt_video_daily
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE);
            """,
            [start_s, end_s],
        )
        con.execute(
            f"""
            INSERT INTO fact_yt_video_daily(date, videoId, views, estimatedMinutesWatched, averageViewDuration)
            SELECT CAST(date AS DATE), videoId, views, estimatedMinutesWatched, averageViewDuration
            FROM {tmp_name};
            """
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        con.close()
        raise
    con.close()
    return f"YouTube: atualizado fact_yt_video_daily para {start_s}..{end_s}"


