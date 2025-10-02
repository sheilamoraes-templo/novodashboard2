from __future__ import annotations

from datetime import date, timedelta

import duckdb

from configs.settings import get_settings
from integrations.rd.client import RDClient


def refresh_rd_lead_stage_last_n_days(days: int = 30) -> str:
    """Exemplo inicial: busca contatos atualizados e agrega por um campo de estágio, se disponível.
    A estrutura de RD pode variar; este é um placeholder para materialização mínima.
    """
    s = get_settings()
    client = RDClient.from_env()
    end = date.today()
    start = end - timedelta(days=days)
    start_iso, end_iso = start.isoformat(), end.isoformat()

    contacts = client.fetch_contacts_paginated(updated_start_iso=start_iso, updated_end_iso=end_iso)
    # Extrair estágio se existir em um campo comum
    stage_counts: dict[str, int] = {}
    for c in contacts:
        stage = (
            c.get("lifecycle_stage")
            or c.get("funnel_stage")
            or c.get("status")
            or "unknown"
        )
        stage_counts[stage] = stage_counts.get(stage, 0) + 1

    con = duckdb.connect(str(s.data_dir / "warehouse" / "warehouse.duckdb"))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_rd_lead_stage_daily (
            date DATE,
            stage TEXT,
            count BIGINT
        );
        """
    )
    # Inserir um snapshot agregado da janela
    for stg, cnt in stage_counts.items():
        con.execute(
            "INSERT INTO fact_rd_lead_stage_daily(date, stage, count) VALUES (?, ?, ?);",
            [end_iso, stg, cnt],
        )
    con.close()
    return f"RD: atualizado fact_rd_lead_stage_daily com {len(stage_counts)} estágios (janela {start_iso}..{end_iso})"


def refresh_rd_email_campaign_last_n_days(days: int = 30) -> str:
    """Materializa campanhas de e-mail (RD) com métricas básicas em fact_rd_email_campaign.

    Colunas: date, campaignId, sends, opens, clicks
    """
    s = get_settings()
    client = RDClient.from_env()
    end = date.today()
    start = end - timedelta(days=days)
    start_iso, end_iso = start.isoformat(), end.isoformat()

    campaigns = client.fetch_email_campaigns(start_iso=start_iso, end_iso=end_iso)
    # Monta registros com data (data de envio se disponível)
    rows: list[tuple[str, str, int, int, int]] = []
    for c in campaigns:
        cid = str(c.get("id") or c.get("campaignId") or c.get("uuid") or "")
        if not cid:
            continue
        send_dt = c.get("send_datetime") or c.get("sent_at") or c.get("scheduled_at") or c.get("created_at")
        send_date = (send_dt or end_iso)[:10]
        m = client.fetch_email_metrics(cid)
        rows.append((send_date, cid, int(m.get("sends", 0)), int(m.get("opens", 0)), int(m.get("clicks", 0))))

    con = duckdb.connect(str(s.data_dir / "warehouse" / "warehouse.duckdb"))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_rd_email_campaign (
            date DATE,
            campaignId TEXT,
            sends BIGINT,
            opens BIGINT,
            clicks BIGINT
        );
        """
    )
    con.execute("BEGIN TRANSACTION;")
    try:
        # Apaga janela e insere novamente (idempotente por janela)
        con.execute("DELETE FROM fact_rd_email_campaign WHERE date BETWEEN ? AND ?;", [start_iso, end_iso])
        if rows:
            con.executemany(
                "INSERT INTO fact_rd_email_campaign(date, campaignId, sends, opens, clicks) VALUES (?, ?, ?, ?, ?);",
                rows,
            )
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        con.close()
        raise
    con.close()
    return f"RD: atualizado fact_rd_email_campaign para {start_iso}..{end_iso} (n={len(rows)})"


