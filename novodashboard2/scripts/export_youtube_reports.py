from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

# Garantir que o diretório raiz do projeto esteja no PYTHONPATH
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from configs.settings import get_settings


def iso_days_ago(n: int) -> str:
    return (date.today() - timedelta(days=n)).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta relatórios YouTube (vídeos por período) para CSV.")
    parser.add_argument("--start", type=str, default=iso_days_ago(30), help="Data inicial YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=date.today().isoformat(), help="Data final YYYY-MM-DD")
    args = parser.parse_args()

    s = get_settings()
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))

    out_dir = Path(ROOT_DIR) / "catalog"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Relatório de vídeos (YT) - agregação no período
    try:
        df_videos = pd.DataFrame(
            con.execute(
                """
                SELECT videoId,
                       SUM(views) AS views,
                       SUM(estimatedMinutesWatched) AS estimatedMinutesWatched,
                       AVG(averageViewDuration) AS averageViewDuration
                FROM fact_yt_video_period
                WHERE startDate = CAST(? AS DATE) OR endDate = CAST(? AS DATE)
                GROUP BY 1
                ORDER BY estimatedMinutesWatched DESC NULLS LAST
                """,
                [args.start, args.end],
            ).fetchdf()
        )
    except Exception:
        df_videos = pd.DataFrame(columns=["videoId", "views", "estimatedMinutesWatched", "averageViewDuration"])

    # Nota: títulos dos vídeos não estão persistidos nesta fase; manteremos somente videoId e métricas.
    out_videos = out_dir / f"youtube_videos_report_{args.start}_{args.end}.csv"
    df_videos.to_csv(out_videos, index=False, encoding="utf-8")

    con.close()
    print(f"Exportado:\n- {out_videos}")


if __name__ == "__main__":
    main()


