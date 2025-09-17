from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

import polars as pl


def read_csv_robust(path: Path) -> pl.DataFrame:
    """Tenta múltiplas estratégias de leitura para arquivos GA4 exportados."""
    encodings = ["utf-8", "latin-1", "utf-16"]
    for enc in encodings:
        try:
            return pl.read_csv(path, encoding=enc, ignore_errors=True)
        except Exception:
            continue
    raise ValueError(f"Falha ao ler CSV: {path}")


def import_ga4_csvs(paths: Iterable[Path]) -> pl.DataFrame:
    frames: List[pl.DataFrame] = []
    for p in paths:
        if p.exists() and p.is_file():
            frames.append(read_csv_robust(p))
    if not frames:
        return pl.DataFrame()
    df = pl.concat(frames, how="vertical_relaxed")
    # Normalização mínima de nomes comuns GA4 CSV
    rename_map = {
        "Usuários": "users",
        "Usuários ativos": "users",
        "Sessões": "sessions",
        "Visualizações de página": "pageviews",
        "Visualizações de tela": "pageviews",
        "Data": "date",
    }
    cols = {c: rename_map.get(c, c) for c in df.columns}
    df = df.rename(cols)
    return df


