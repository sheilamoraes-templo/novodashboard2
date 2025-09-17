from __future__ import annotations

import json
import os
from pathlib import Path
import sys

from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


SCOPES = [
    "https://www.googleapis.com/auth/analytics.readonly",
]


def main() -> None:
    # Garantir que o diretório raiz do projeto esteja no PYTHONPATH
    CURRENT_DIR = os.path.dirname(__file__)
    ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
    if ROOT_DIR not in sys.path:
        sys.path.insert(0, ROOT_DIR)
    if load_dotenv:
        try:
            load_dotenv()
        except Exception:
            pass
    client_secret_path = os.getenv("GA4_OAUTH_CLIENT_SECRET")
    token_path = os.getenv("GA4_OAUTH_TOKEN_PATH", "./ga4_token.json")
    if not client_secret_path or not Path(client_secret_path).exists():
        raise SystemExit("Defina GA4_OAUTH_CLIENT_SECRET com o caminho para client_secret_*.json")

    flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
    creds = flow.run_local_server(port=0)

    with open(token_path, "w", encoding="utf-8") as f:
        f.write(creds.to_json())
    print(f"Token salvo em {token_path}")


if __name__ == "__main__":
    main()


