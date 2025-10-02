from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import polars as pl
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.analytics.data_v1beta.types import GetMetadataRequest
from google.analytics.admin_v1beta import AnalyticsAdminServiceClient

from configs.settings import get_settings
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type


@dataclass
class GA4Client:
    property_id: str
    cache_dir: Path

    @classmethod
    def from_env(cls) -> "GA4Client":
        settings = get_settings()
        if not settings.ga4_property_id:
            raise RuntimeError("GA4_PROPERTY_ID não definido no ambiente")
        cache_dir = settings.data_dir / "api_cache" / "ga4"
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cls(property_id=str(settings.ga4_property_id), cache_dir=cache_dir)

    def _client(self) -> BetaAnalyticsDataClient:
        # Preferência: Service Account via GOOGLE_APPLICATION_CREDENTIALS
        # Alternativa: OAuth Installed App via GA4_OAUTH_TOKEN_PATH
        token_path = os.getenv("GA4_OAUTH_TOKEN_PATH")
        if token_path and Path(token_path).exists():
            creds = Credentials.from_authorized_user_file(token_path)
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            return BetaAnalyticsDataClient(credentials=creds)
        return BetaAnalyticsDataClient()

    # Cache simples dos metadados (dimensões/métricas válidas)
    _dims_cache: Optional[Set[str]] = None
    _mets_cache: Optional[Set[str]] = None

    def _ensure_metadata_cached(self) -> None:
        if self._dims_cache is not None and self._mets_cache is not None:
            return
        md = self.fetch_metadata()
        self._dims_cache = set(md.get("dimensions", []) or [])
        self._mets_cache = set(md.get("metrics", []) or [])

    def _validate_dimensions_metrics(self, dimensions: List[str], metrics: List[str]) -> None:
        # Validação best-effort para evitar queries inválidas
        try:
            self._ensure_metadata_cached()
            invalid_dims = [d for d in dimensions if d not in (self._dims_cache or set())]
            invalid_mets = [m for m in metrics if m not in (self._mets_cache or set())]
            if invalid_dims or invalid_mets:
                raise ValueError(
                    f"GA4: dimensões/métricas inválidas. dims_invalidas={invalid_dims} mets_invalidas={invalid_mets}"
                )
        except Exception:
            # Se falhar para obter metadados, não bloqueia a execução; deixa a API devolver o erro
            pass

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((Exception,)),
    )
    def _run_report_once(self, *, dimensions: List[str], metrics: List[str], start_date: str, end_date: str, offset: int, limit: int) -> Any:
        client = self._client()
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            offset=offset,
            limit=limit,
        )
        return client.run_report(request)

    def run_report(
        self,
        *,
        dimensions: List[str],
        metrics: List[str],
        start_date: str,
        end_date: str,
        filters: Optional[Dict[str, Any]] = None,
        page_size: int = 100000,
    ) -> pl.DataFrame:
        # Validação leve contra metadados
        self._validate_dimensions_metrics(dimensions, metrics)

        offset = 0
        all_rows: List[Dict[str, Any]] = []
        dim_headers: List[str] = []
        met_headers: List[str] = []

        while True:
            response = self._run_report_once(
                dimensions=dimensions,
                metrics=metrics,
                start_date=start_date,
                end_date=end_date,
                offset=offset,
                limit=page_size,
            )
            if not dim_headers:
                dim_headers = [h.name for h in response.dimension_headers]
                met_headers = [h.name for h in response.metric_headers]

            batch_count = 0
            for r in response.rows:
                rec: Dict[str, Any] = {}
                for i, v in enumerate(r.dimension_values):
                    rec[dim_headers[i]] = v.value
                for i, v in enumerate(r.metric_values):
                    num_str = v.value
                    try:
                        # GA4 retorna string; tentar float e deixar o cast final para a materialização
                        rec[met_headers[i]] = float(num_str)
                    except Exception:
                        rec[met_headers[i]] = None
                all_rows.append(rec)
                batch_count += 1

            if batch_count < page_size:
                break
            offset += page_size

        if not all_rows:
            return pl.DataFrame()
        return pl.DataFrame(all_rows)

    def cache_key(self, *, dimensions: List[str], metrics: List[str], start_date: str, end_date: str) -> str:
        dims = "-".join(dimensions)
        mets = "-".join(metrics)
        return f"ga4__{dims}__{mets}__{start_date}_{end_date}.parquet"

    def run_report_cached(self, *, dimensions: List[str], metrics: List[str], start_date: str, end_date: str, force: bool = False) -> Path:
        key = self.cache_key(dimensions=dimensions, metrics=metrics, start_date=start_date, end_date=end_date)
        target = self.cache_dir / key
        if target.exists() and not force:
            return target
        df = self.run_report(dimensions=dimensions, metrics=metrics, start_date=start_date, end_date=end_date)
        if df.height == 0:
            # cria arquivo vazio para marcar cache
            pl.DataFrame().write_parquet(target)
            return target
        df.write_parquet(target)
        return target

    def fetch_metadata(self) -> Dict[str, Any]:
        client = self._client()
        req = GetMetadataRequest(name=f"properties/{self.property_id}/metadata")
        md = client.get_metadata(req)
        dims = [d.api_name for d in md.dimensions]
        mets = [m.api_name for m in md.metrics]
        return {"dimensions": dims, "metrics": mets}

    def fetch_custom_definitions(self) -> Dict[str, Any]:
        admin = AnalyticsAdminServiceClient()
        prop = f"properties/{self.property_id}"
        custom_dims = [
            {"parameter_name": d.parameter_name, "scope": d.scope}
            for d in admin.list_custom_dimensions(parent=prop)
        ]
        custom_mets = [
            {"parameter_name": m.parameter_name, "unit": m.measurement_unit}
            for m in admin.list_custom_metrics(parent=prop)
        ]
        return {"custom_dimensions": custom_dims, "custom_metrics": custom_mets}



