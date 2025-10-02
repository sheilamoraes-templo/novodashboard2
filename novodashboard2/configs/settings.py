from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
    # Força o carregamento do .env da raiz do projeto com override
    ROOT_ENV = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(dotenv_path=str(ROOT_ENV), override=True)
except Exception:
    pass


@dataclass
class Settings:
    data_dir: Path = Path(os.getenv("DATA_DIR", "./data")).resolve()
    # Flag para modo simulado (sem APIs externas)
    mock_mode: bool = (
        True
        if str(os.getenv("MOCK_MODE", "0")).strip().lower() in {"1", "true", "yes"}
        else False
    )

    # GA4
    ga4_property_id: int | None = (
        int(os.getenv("GA4_PROPERTY_ID")) if os.getenv("GA4_PROPERTY_ID") else None
    )
    google_credentials_path: Path | None = (
        Path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS")).resolve()
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        else None
    )

    # Futuro
    youtube_api_key: str | None = os.getenv("YOUTUBE_API_KEY")
    yt_oauth_client_secret: str | None = os.getenv("YT_OAUTH_CLIENT_SECRET")
    yt_oauth_token_path: Path | None = (
        Path(os.getenv("YT_OAUTH_TOKEN_PATH")).resolve() if os.getenv("YT_OAUTH_TOKEN_PATH") else None
    )
    yt_channel_id: str | None = os.getenv("YT_CHANNEL_ID")
    rd_client_id: str | None = os.getenv("RD_CLIENT_ID")
    rd_client_secret: str | None = os.getenv("RD_CLIENT_SECRET")
    rd_redirect_uri: str | None = os.getenv("RD_REDIRECT_URI")
    rd_token_path: Path | None = (
        Path(os.getenv("RD_TOKEN_PATH")).resolve() if os.getenv("RD_TOKEN_PATH") else None
    )
    rd_account_id: str | None = os.getenv("RD_ACCOUNT_ID")
    slack_webhook_url: str | None = os.getenv("SLACK_WEBHOOK_URL")
    openrouter_api_key: str | None = os.getenv("OPENROUTER_API_KEY")


def get_settings() -> Settings:
    settings = Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    for sub in ("raw", "api_cache", "warehouse"):
        (settings.data_dir / sub).mkdir(parents=True, exist_ok=True)
    return settings


