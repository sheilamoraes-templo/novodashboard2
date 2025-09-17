from __future__ import annotations

import itertools
import json
from datetime import date, timedelta
import os
import sys

# Garantir que o diretório raiz do projeto esteja no PYTHONPATH
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from integrations.ga4.client import GA4Client


DIM_CANDIDATES = ["date", "pagePath", "pageTitle"]
MET_CANDIDATES = ["totalUsers", "sessions", "screenPageViews"]


def main() -> None:
    client = GA4Client.from_env()
    md = client.fetch_metadata()
    dims_ok = [d for d in DIM_CANDIDATES if d in md.get("dimensions", [])]
    mets_ok = [m for m in MET_CANDIDATES if m in md.get("metrics", [])]

    end = date.today()
    start = end - timedelta(days=7)
    results = []
    for d, m in itertools.product(dims_ok, mets_ok):
        try:
            df = client.run_report(dimensions=[d], metrics=[m], start_date=start.isoformat(), end_date=end.isoformat())
            results.append({"dimension": d, "metric": m, "rows": df.height})
        except Exception as e:
            results.append({"dimension": d, "metric": m, "error": str(e)})

    print(json.dumps({"tested": results}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()


