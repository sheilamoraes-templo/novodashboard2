from __future__ import annotations

import json
import os
import sys

# Garantir que o diretório raiz do projeto esteja no PYTHONPATH
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from integrations.ga4.client import GA4Client


def main() -> None:
    client = GA4Client.from_env()
    metadata = client.fetch_metadata()
    custom = client.fetch_custom_definitions()
    report = {"metadata": metadata, "custom": custom}
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()


