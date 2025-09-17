from __future__ import annotations

import os
import sys
from pathlib import Path

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

from google_auth_oauthlib.flow import InstalledAppFlow

from configs.settings import get_settings


SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]


def main() -> None:
    s = get_settings()
    client_secret_path = s.yt_oauth_client_secret or os.getenv("YT_OAUTH_CLIENT_SECRET")
    token_path = s.yt_oauth_token_path or Path(os.getenv("YT_OAUTH_TOKEN_PATH", "./yt_token.json"))
    if not client_secret_path or not Path(client_secret_path).exists():
        raise SystemExit("Defina YT_OAUTH_CLIENT_SECRET com o caminho para client_secret_*.json do YouTube")

    flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
    creds = flow.run_local_server(port=0)

    Path(token_path).write_text(creds.to_json(), encoding="utf-8")
    print(f"Token do YouTube salvo em {token_path}")


if __name__ == "__main__":
    main()


