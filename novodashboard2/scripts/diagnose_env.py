from __future__ import annotations

import json
import os
from pathlib import Path
import sys

# Garantir que o diretório raiz do projeto esteja no PYTHONPATH
CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from configs.settings import get_settings


def main() -> None:
    s = get_settings()
    report = {
        "DATA_DIR": str(s.data_dir),
        "dirs": {"raw": (s.data_dir / "raw").exists(), "api_cache": (s.data_dir / "api_cache").exists(), "warehouse": (s.data_dir / "warehouse").exists()},
        "GA4_PROPERTY_ID": s.ga4_property_id,
        "GOOGLE_APPLICATION_CREDENTIALS_exists": bool(s.google_credentials_path and s.google_credentials_path.exists()),
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()


