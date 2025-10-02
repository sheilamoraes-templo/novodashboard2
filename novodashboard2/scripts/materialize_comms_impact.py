from __future__ import annotations

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

from services.comms_impact_refresh import materialize_comms_impact_daily, materialize_comms_impact_summary


def main() -> None:
    print(materialize_comms_impact_daily())
    print(materialize_comms_impact_summary())


if __name__ == "__main__":
    main()



