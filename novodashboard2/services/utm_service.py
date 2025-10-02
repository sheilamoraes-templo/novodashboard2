from __future__ import annotations

from pathlib import Path

import duckdb

from configs.settings import get_settings


def _get_db_con():
    s = get_settings()
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def import_map_utm_campaign(csv_path: str | None = None) -> str:
    """Importa CSV de mapeamento UTM → campaignId para DuckDB (tabela map_utm_campaign).

    Espera colunas: utm_source, utm_medium, utm_campaign, campaignId, campaign_name
    Normaliza (lower/trim) as colunas UTM em colunas _norm.
    """
    s = get_settings()
    default_csv = Path("catalog") / "map_utm_campaign.csv"
    path = Path(csv_path).resolve() if csv_path else default_csv.resolve()

    con = _get_db_con()
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS map_utm_campaign (
            utm_source TEXT,
            utm_medium TEXT,
            utm_campaign TEXT,
            campaignId TEXT,
            campaign_name TEXT,
            utm_source_norm TEXT,
            utm_medium_norm TEXT,
            utm_campaign_norm TEXT
        );
        """
    )
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM map_utm_campaign;")
        if path.exists():
            norm_path = str(path).replace("\\", "/")
            con.execute(
                """
                INSERT INTO map_utm_campaign(utm_source, utm_medium, utm_campaign, campaignId, campaign_name, utm_source_norm, utm_medium_norm, utm_campaign_norm)
                SELECT
                  utm_source,
                  utm_medium,
                  utm_campaign,
                  campaignId,
                  campaign_name,
                  lower(trim(utm_source)) AS utm_source_norm,
                  lower(trim(utm_medium)) AS utm_medium_norm,
                  lower(trim(utm_campaign)) AS utm_campaign_norm
                FROM read_csv_auto(?, header=True);
                """,
                [norm_path],
            )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        con.close()
        raise
    con.close()
    return f"map_utm_campaign importado de {path if path.exists() else '(arquivo não encontrado; tabela limpa)'}"



