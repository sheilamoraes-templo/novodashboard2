import os
import sys
import streamlit as st

# Garantir que o diretório raiz do projeto esteja no PYTHONPATH quando o Streamlit
# executar a partir de app/dashboard.py
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
from datetime import date, timedelta

from services.data_service import (
    get_kpis,
    get_top_pages,
    get_pages_weekly_comparison,
    get_video_funnel,
    get_top_countries,
    get_top_days,
    get_health,
    get_yt_channel_daily,
    get_yt_top_videos,
    get_engagement_kpis,
    get_engagement_series,
    get_utm_aggregate,
    get_comms_sessions_by_campaign,
    get_wow_comparatives,
    get_mtd_vs_prev_month,
    get_yt_retention_by_video,
    get_pages_pareto,
)
import plotly.express as px
from services.report_service import build_weekly_report
from integrations.slack.client import SlackClient
from services.ga4_refresh import (
    refresh_sessions_last_n_days,
    refresh_events_last_n_days,
    refresh_pages_last_n_days,
)
from services.youtube_refresh import refresh_yt_channel_and_videos


st.set_page_config(page_title="CLASSPLAY Dashboard", layout="wide")


def main() -> None:
    st.title("CLASSPLAY Dashboard")
    st.caption("GA4 primeiro; YouTube e RD preparados para integração")

    st.subheader("Período")
    col1, col2 = st.columns(2)
    with col1:
        end = st.date_input("Fim", value=date.today())
    with col2:
        start = st.date_input("Início", value=date.today() - timedelta(days=7))
    if start > end:
        st.error("Data inicial maior que final")
        return

    start_s, end_s = start.isoformat(), end.isoformat()

    st.header("KPIs Principais")
    if st.button("Atualizar dados GA4 (últimos 30 dias)"):
        try:
            msgs = []
            msgs.append(refresh_sessions_last_n_days(30))
            msgs.append(refresh_events_last_n_days(30))
            msgs.append(refresh_pages_last_n_days(30))
            for m in msgs:
                st.success(m)
        except Exception as e:
            st.error(f"Falha ao atualizar GA4: {e}")

    st.subheader("YouTube")
    colyt1, colyt2 = st.columns(2)
    with colyt1:
        yt_days = st.number_input("Dias (YT)", min_value=7, max_value=60, value=30)
    with colyt2:
        if st.button("Atualizar YouTube (últimos N dias)"):
            try:
                msg = refresh_yt_channel_and_videos(int(yt_days))
                st.success(msg)
            except Exception as e:
                st.error(f"Falha ao atualizar YouTube: {e}")

    # Cards YouTube
    st.header("YouTube — Evolução diária (views)")
    try:
        yt_daily = get_yt_channel_daily(start_s, end_s)
        if yt_daily:
            figy = px.line(yt_daily, x="date", y="views")
            st.plotly_chart(figy, use_container_width=True)
        else:
            st.info("Sem dados diários do YouTube no período.")
    except Exception as e:
        st.warning(f"Falha ao carregar evolução YouTube: {e}")

    st.header("YouTube — Top vídeos (views)")
    try:
        yt_top = get_yt_top_videos(start_s, end_s, 20)
        if yt_top:
            figt = px.bar(yt_top, x="views", y="videoId", orientation="h")
            st.plotly_chart(figt, use_container_width=True)
        else:
            st.info("Sem dados de vídeos do YouTube no período.")
    except Exception as e:
        st.warning(f"Falha ao carregar Top vídeos YouTube: {e}")
    kpis = get_kpis(start_s, end_s)
    eng = get_engagement_kpis(start_s, end_s)
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Usuários", int(kpis.get("users", 0)))
    k2.metric("Sessões", int(kpis.get("sessions", 0)))
    k3.metric("Pageviews", int(kpis.get("pageviews", 0)))
    k4.metric("Views (YT)", int(eng.get("views", 0)))
    k5.metric("Minutos (YT)", int(eng.get("minutes", 0)))
    # Comparativos WoW e MTD
    try:
        wow = get_wow_comparatives(end_s)
        mtd = get_mtd_vs_prev_month(end_s)
        st.caption(
            f"WoW – Sessões: {wow['sessions']['delta_pct']}% | Minutos: {wow['minutes']['delta_pct']}%  •  "
            f"MTD vs M-1 – Sessões: {mtd['sessions']['delta_pct']}% | Minutos: {mtd['minutes']['delta_pct']}%"
        )
    except Exception as e:
        st.caption(f"Comparativos indisponíveis: {e}")

    st.header("Top Páginas (Top 10)")
    try:
        pages = get_top_pages(start_s, end_s, 10)
        if pages:
            fig = px.bar(pages, x="pageviews", y="page_title", orientation="h")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados de páginas no período.")
    except Exception as e:
        st.warning(f"Falha ao carregar Top Páginas: {e}")

    st.header("Comparação Semanal (últimas 8)")
    try:
        weekly = get_pages_weekly_comparison(8)
        if weekly:
            figw = px.bar(weekly, x="year_week", y="pageviews")
            st.plotly_chart(figw, use_container_width=True)
        else:
            st.info("Sem dados semanais.")
    except Exception as e:
        st.warning(f"Falha ao carregar comparação semanal: {e}")

    st.header("Funil de Vídeos")
    try:
        funnel = get_video_funnel(start_s, end_s)
        f1, f2, f3 = st.columns(3)
        f1.metric("Start", funnel.get("start", 0))
        f2.metric("Progress", funnel.get("progress", 0))
        f3.metric("Completion %", funnel.get("completion_rate", 0.0))
    except Exception as e:
        st.warning(f"Falha ao carregar Funil de Vídeos: {e}")

    st.header("Top Países")
    try:
        countries = get_top_countries(start_s, end_s, 10)
        if countries:
            figc = px.bar(countries, x="users", y="country_id", orientation="h")
            st.plotly_chart(figc, use_container_width=True)
        else:
            st.info("Sem dados de países no período.")
    except Exception as e:
        st.warning(f"Falha ao carregar Top Países: {e}")

    st.header("Top Dias da Semana")
    try:
        days = get_top_days(start_s, end_s)
        if days:
            figd = px.bar(days, x="weekday", y="users")
            st.plotly_chart(figd, use_container_width=True)
        else:
            st.info("Sem dados por dia no período.")
    except Exception as e:
        st.warning(f"Falha ao carregar Top Dias: {e}")

    st.header("Conteúdo & Retenção")
    colc1, colc2 = st.columns(2)
    with colc1:
        try:
            ret = get_yt_retention_by_video(start_s, end_s, 20)
            if ret:
                import pandas as pd
                dfr = pd.DataFrame(ret)
                dfr_display = dfr.copy()
                dfr_display["min_per_view"] = dfr_display["min_per_view"].round(2)
                st.dataframe(dfr_display, use_container_width=True, hide_index=True)
            else:
                st.info("Sem dados de retenção YT no período.")
        except Exception as e:
            st.warning(f"Falha na retenção YT: {e}")
    with colc2:
        try:
            pareto = get_pages_pareto(start_s, end_s, 20)
            if pareto:
                import pandas as pd
                dfp = pd.DataFrame(pareto)
                st.bar_chart(dfp.set_index("page_title")["pageviews"], use_container_width=True)
            else:
                st.info("Sem dados de páginas para Pareto no período.")
        except Exception as e:
            st.warning(f"Falha no Pareto de páginas: {e}")

    # Aquisição & CRM (MVP)
    st.header("Aquisição & CRM — UTM e Campanhas")
    try:
        utm = get_utm_aggregate(start_s, end_s, 20)
        if utm:
            import pandas as pd
            dfu = pd.DataFrame(utm)
            st.bar_chart(dfu.set_index("campaign")["sessions"], use_container_width=True)
            st.dataframe(dfu, use_container_width=True, hide_index=True)
        else:
            st.info("Sem dados UTM no período.")
    except Exception as e:
        st.warning(f"Falha ao carregar UTM: {e}")

    try:
        comms = get_comms_sessions_by_campaign(start_s, end_s, 10)
        if comms:
            import pandas as pd
            dfc = pd.DataFrame(comms)
            st.bar_chart(dfc.set_index("campaignId")["sessions"], use_container_width=True)
        else:
            st.info("Sem dados de impacto de campanhas no período.")
    except Exception as e:
        st.warning(f"Falha ao carregar impacto de campanhas: {e}")

    st.header("Análise de Classes (/classes)")
    st.caption("Em breve: filtro e ranking específico de páginas de classes.")

    st.header("Informações dos Dados")
    st.json(get_health())

    st.header("Envio de Relatório (Slack)")
    top_n = st.number_input("Top N páginas", min_value=5, max_value=20, value=10)
    try:
        report_text = build_weekly_report(start_s, end_s, int(top_n))
        st.text_area("Prévia do relatório", report_text, height=240)
    except Exception as e:
        st.warning(f"Falha ao montar prévia do relatório: {e}")
    if st.button("Enviar para Slack"):
        try:
            sc = SlackClient.from_env()
            resp = sc.send_text(report_text)
            if resp.get("status_code") == 200:
                st.success("Relatório enviado ao Slack")
            else:
                st.error(f"Falha ao enviar Slack: {resp}")
        except Exception as e:
            st.error(f"Erro no envio Slack: {e}")

    st.header("Tendência combinada (Sessões × Minutos, últimos dias)")
    try:
        eng_series = get_engagement_series(start_s, end_s)
        if eng_series:
            import pandas as pd
            df = pd.DataFrame(eng_series)
            fig_combo = px.line(df, x="date", y=["sessions", "minutes"], labels={"value": "valor", "variable": "métrica"})
            st.plotly_chart(fig_combo, use_container_width=True)
        else:
            st.info("Sem dados de engajamento no período.")
    except Exception as e:
        st.warning(f"Falha ao carregar tendência combinada: {e}")


if __name__ == "__main__":
    main()


