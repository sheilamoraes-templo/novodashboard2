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


def materialize_comms_impact_summary() -> str:
    """Gera resumo por campanha com janelas D-1, D0 e D0–D+2 e uplift.

    Requer: fact_comms_impact_daily (sessões por campanha e data) e
            fact_rd_email_campaign (send date, sends/opens/clicks).
    """
    con = _get_con()
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_comms_impact_summary (
            campaignId TEXT,
            send_date DATE,
            ses_d_1 BIGINT,
            ses_d0 BIGINT,
            ses_d0_d2 BIGINT,
            uplift_abs BIGINT,
            uplift_pct DOUBLE,
            sends BIGINT,
            opens BIGINT,
            clicks BIGINT
        );
        """
    )
    con.execute("BEGIN TRANSACTION;")
    try:
        con.execute("DELETE FROM fact_comms_impact_summary;")
        # Monta base com datas relativas à data de envio
        con.execute(
            """
            WITH base AS (
              SELECT c.campaignId,
                     r.date AS d,
                     r.sessions
              FROM fact_comms_impact_daily r
              JOIN fact_rd_email_campaign c ON c.campaignId = r.campaignId
            ), pvt AS (
              SELECT c.campaignId,
                     CAST(c.date AS DATE) AS send_date,
                     MAX(CASE WHEN b.d = (CAST(c.date AS DATE) - INTERVAL '1 day') THEN b.sessions END) AS ses_d_1,
                     MAX(CASE WHEN b.d = CAST(c.date AS DATE) THEN b.sessions END) AS ses_d0,
                     SUM(CASE WHEN b.d BETWEEN CAST(c.date AS DATE) AND (CAST(c.date AS DATE) + INTERVAL '2 day') THEN b.sessions END) AS ses_d0_d2
              FROM fact_rd_email_campaign c
              LEFT JOIN base b ON b.campaignId = c.campaignId
              GROUP BY 1,2
            )
            INSERT INTO fact_comms_impact_summary
            SELECT pvt.campaignId,
                   pvt.send_date,
                   COALESCE(pvt.ses_d_1, 0) AS ses_d_1,
                   COALESCE(pvt.ses_d0, 0) AS ses_d0,
                   COALESCE(pvt.ses_d0_d2, 0) AS ses_d0_d2,
                   COALESCE(pvt.ses_d0, 0) - COALESCE(pvt.ses_d_1, 0) AS uplift_abs,
                   CASE WHEN COALESCE(pvt.ses_d_1, 0) > 0 THEN (COALESCE(pvt.ses_d0, 0) - COALESCE(pvt.ses_d_1, 0)) * 100.0 / pvt.ses_d_1 ELSE 0.0 END AS uplift_pct,
                   c.sends,
                   c.opens,
                   c.clicks
            FROM pvt
            LEFT JOIN (
              SELECT campaignId,
                     SUM(sends) AS sends,
                     SUM(opens) AS opens,
                     SUM(clicks) AS clicks
              FROM fact_rd_email_campaign
              GROUP BY 1
            ) c ON c.campaignId = pvt.campaignId
            ;
            """
        )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        con.close()
        raise
    con.close()
    return "Materializado fact_comms_impact_summary"


