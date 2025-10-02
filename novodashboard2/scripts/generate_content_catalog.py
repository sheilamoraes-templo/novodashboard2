from __future__ import annotations

import os
import sys
import hashlib
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


def _sha1(text: str) -> str:
    h = hashlib.sha1()
    h.update((text or "").encode("utf-8"))
    return h.hexdigest()[:16]


def main() -> None:
    s = get_settings()
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))

    # GA4 pages: pagePath + pageTitle
    ga4_rows: list[tuple[str, str]] = []
    try:
        ga4_rows = con.execute(
            """
            SELECT pagePath,
                   COALESCE(MAX(pageTitle), pagePath) AS title
            FROM fact_ga4_pages_daily
            GROUP BY pagePath
            ORDER BY title ASC
            """
        ).fetchall()
    except Exception:
        ga4_rows = []

    # YT videos: videoId; título pode não estar no warehouse — usar fallback
    yt_rows: list[tuple[str, str]] = []
    try:
        yt_ids = con.execute(
            """
            SELECT DISTINCT videoId
            FROM fact_yt_video_period
            WHERE videoId IS NOT NULL AND videoId <> ''
            ORDER BY videoId ASC
            """
        ).fetchall()
        yt_rows = [(vid[0], vid[0]) for vid in yt_ids]  # (videoId, title=fallback=videoId)
    except Exception:
        yt_rows = []

    con.close()

    # Montar DataFrame unificado com colunas mínimas
    rows: list[dict[str, str]] = []

    for page_path, title in ga4_rows:
        content_id = f"cp:{_sha1(page_path or title)}"
        rows.append(
            {
                "content_id": content_id,
                "title": title or page_path or "",
                "pagePath": page_path or "",
                "videoId": "",
            }
        )

    for video_id, title in yt_rows:
        content_id = f"yt:{video_id}"
        rows.append(
            {
                "content_id": content_id,
                "title": title or video_id or "",
                "pagePath": "",
                "videoId": video_id or "",
            }
        )

    df = pd.DataFrame(rows, columns=["content_id", "title", "pagePath", "videoId"]) if rows else pd.DataFrame(
        columns=["content_id", "title", "pagePath", "videoId"]
    )

    out_dir = Path(ROOT_DIR) / "catalog"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "content_catalog.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Catálogo gerado: {out_path} ({len(df)} linhas)")


if __name__ == "__main__":
    main()


