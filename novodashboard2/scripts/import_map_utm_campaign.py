from __future__ import annotations

import argparse
import os
import sys

# Garantir que o diretório raiz do projeto esteja no PYTHONPATH
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from services.utm_service import import_map_utm_campaign


def main() -> None:
    parser = argparse.ArgumentParser(description="Importa CSV de mapeamento UTM → campaignId para DuckDB.")
    parser.add_argument("--path", type=str, default=None, help="Caminho para CSV (padrão: catalog/map_utm_campaign.csv)")
    args = parser.parse_args()

    msg = import_map_utm_campaign(args.path)
    print(msg)


if __name__ == "__main__":
    main()

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

from services.utm_service import import_map_utm_campaign


def main() -> None:
    parser = argparse.ArgumentParser(description="Importa CSV map_utm_campaign para DuckDB.")
    parser.add_argument("--path", type=str, default="catalog/map_utm_campaign.csv", help="Caminho do CSV de mapeamento")
    args = parser.parse_args()
    msg = import_map_utm_campaign(args.path)
    print(msg)


if __name__ == "__main__":
    main()



