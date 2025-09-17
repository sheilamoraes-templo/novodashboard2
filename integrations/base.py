from __future__ import annotations

from typing import Any, Dict, Iterable, Protocol


class DataConnector(Protocol):
    def fetch(self, params: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        ...

    def normalize(self, records: Iterable[Dict[str, Any]]):  # returns a DataFrame (Polars)
        ...

    def to_parquet(self, df, target_path: str) -> str:
        ...


