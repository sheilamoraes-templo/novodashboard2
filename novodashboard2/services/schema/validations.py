from __future__ import annotations

import pandera as pa
import pandera.polars as pa_pl


class DimDateSchema(pa_pl.DataFrameModel):
    date = pa_pl.Series(pa.DateTime, nullable=False)
    year = pa_pl.Series(pa.Int32, nullable=False)
    month = pa_pl.Series(pa.Int32, nullable=False)
    week = pa_pl.Series(pa.Int32, nullable=False)


class FactSessionsSchema(pa_pl.DataFrameModel):
    date = pa_pl.Series(pa.DateTime, nullable=False)
    users = pa_pl.Series(pa.Int64, nullable=True)
    sessions = pa_pl.Series(pa.Int64, nullable=True)
    pageviews = pa_pl.Series(pa.Int64, nullable=True)


class FactEventsSchema(pa_pl.DataFrameModel):
    date = pa_pl.Series(pa.DateTime, nullable=False)
    event_name = pa_pl.Series(pa.String, nullable=False)
    event_count = pa_pl.Series(pa.Int64, nullable=True)


