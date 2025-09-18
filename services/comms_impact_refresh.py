from __future__ import annotations

import duckdb

from configs.settings import get_settings


def _get_con():
    s = get_settings()
    db_path = s.data_dir / "warehouse" / "warehouse.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def materialize_comms_impact_daily() -> str:
    """Cria/atualiza fact_comms_impact_daily a partir de GA4 UTM + mapping UTM/campanha.

    Escopo MVP: GA4 sessões por campanha/data. (YT por campanha pode ser adicionado depois.)
    """
    con = _get_con()
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_comms_impact_daily (
            date DATE,
            campaignId TEXT,
            sessions BIGINT,
            users BIGINT
        );
        """
    )
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM fact_comms_impact_daily;")
        con.execute(
            """
            INSERT INTO fact_comms_impact_daily
            SELECT
              g.date,
              m.campaignId,
              SUM(g.sessions) AS sessions,
              SUM(g.users) AS users
            FROM fact_ga4_sessions_by_utm_daily g
            LEFT JOIN map_utm_campaign m
              ON lower(trim(g.campaign)) = m.utm_campaign_norm
             AND lower(trim(g.source)) = m.utm_source_norm
             AND lower(trim(g.medium)) = m.utm_medium_norm
            WHERE m.campaignId IS NOT NULL
            GROUP BY 1,2
            ;
            """
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        con.close()
        raise
    con.close()
    return "Materializado fact_comms_impact_daily"



