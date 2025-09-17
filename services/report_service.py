from __future__ import annotations

from datetime import date, timedelta
from typing import List

from services.data_service import (
    get_kpis,
    get_top_pages,
    get_video_funnel,
    get_top_countries,
)


def _fmt_num(value: float | int | None) -> str:
    if value is None:
        return "0"
    try:
        return f"{int(value):,}".replace(",", ".")
    except Exception:
        return str(value)


def build_weekly_report(start_date: str, end_date: str, top_n: int = 10) -> str:
    kpis = get_kpis(start_date, end_date)
    pages = get_top_pages(start_date, end_date, top_n)
    funnel = get_video_funnel(start_date, end_date)
    countries = get_top_countries(start_date, end_date, top_n)

    lines: List[str] = []
    lines.append("CLASSPLAY – Resumo Semanal")
    lines.append(f"Período: {start_date} a {end_date}")
    lines.append("")

    lines.append("KPIs")
    lines.append(f"- Usuários: {_fmt_num(kpis.get('users', 0))}")
    lines.append(f"- Sessões: {_fmt_num(kpis.get('sessions', 0))}")
    lines.append(f"- Pageviews: {_fmt_num(kpis.get('pageviews', 0))}")
    lines.append("")

    lines.append("Top Páginas (Top 10)")
    if pages:
        for i, p in enumerate(pages, start=1):
            title = p.get("page_title") or p.get("page_path") or "(sem título)"
            lines.append(f"{i}. {title} – {_fmt_num(p.get('pageviews', 0))} pv")
    else:
        lines.append("(sem dados)")
    lines.append("")

    lines.append("Funil de Vídeos")
    lines.append(f"- Start: {_fmt_num(funnel.get('start', 0))}")
    lines.append(f"- Progress: {_fmt_num(funnel.get('progress', 0))}")
    lines.append(f"- Completion: {funnel.get('completion_rate', 0.0)}%")
    lines.append("")

    lines.append("Top Países")
    if countries:
        for i, c in enumerate(countries, start=1):
            lines.append(f"{i}. {c.get('country_id', 'NA')} – {_fmt_num(c.get('users', 0))} usuários")
    else:
        lines.append("(sem dados)")
    lines.append("")

    lines.append("Fonte: GA4 (cache local). Este relatório é determinístico (sem IA).")
    return "\n".join(lines)


def build_weekly_summary(today: date | None = None) -> str:
    d_end = today or date.today()
    d_start = d_end - timedelta(days=7)
    start_s, end_s = d_start.isoformat(), d_end.isoformat()

    kpis = get_kpis(start_s, end_s)
    funnel = get_video_funnel(start_s, end_s)
    pages = get_top_pages(start_s, end_s, 5)

    lines = [
        f"Resumo semanal ({start_s} a {end_s})",
        "",
        f"- Usuários: {int(kpis.get('users', 0))}",
        f"- Sessões: {int(kpis.get('sessions', 0))}",
        f"- Pageviews: {int(kpis.get('pageviews', 0))}",
        "",
        "Funil de Vídeos:",
        f"- Start: {funnel.get('start', 0)}",
        f"- Progress: {funnel.get('progress', 0)}",
        f"- Completion %: {funnel.get('completion_rate', 0.0)}",
        "",
        "Top páginas (5):",
    ]
    for i, p in enumerate(pages, 1):
        title = p.get("page_title") or p.get("page_path") or "(sem título)"
        lines.append(f"{i}. {title} — {p.get('pageviews', 0)} pageviews")

    return "\n".join(lines)


