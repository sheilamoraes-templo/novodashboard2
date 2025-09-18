from __future__ import annotations

import argparse
import os
import sys

# Garantir que o diretório raiz do projeto esteja no PYTHONPATH
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from services.ga4_refresh import refresh_sessions_by_utm_last_n_days


def main() -> None:
    parser = argparse.ArgumentParser(description="Atualiza fact_ga4_sessions_by_utm_daily para os últimos N dias.")
    parser.add_argument("--days", type=int, default=30, help="Número de dias a atualizar (padrão: 30)")
    args = parser.parse_args()
    msg = refresh_sessions_by_utm_last_n_days(args.days)
    print(msg)


if __name__ == "__main__":
    main()



