from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from pathlib import Path

import polars as pl
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

from configs.settings import get_settings

from configs.settings import get_settings


@dataclass
class YouTubeClient:
    api_key: Optional[str] = None
    token_path: Optional[Path] = None

    @classmethod
    def from_env(cls) -> "YouTubeClient":
        s = get_settings()
        token_path = s.yt_oauth_token_path or Path("./yt_token.json").resolve()
        if not token_path.exists() and not s.youtube_api_key:
            raise RuntimeError("Forneça YT_OAUTH_TOKEN_PATH (OAuth) ou YOUTUBE_API_KEY")
        return cls(api_key=s.youtube_api_key, token_path=token_path)

    def _yt_service(self):
        if self.token_path and Path(self.token_path).exists():
            creds = Credentials.from_authorized_user_file(str(self.token_path))
            return build("youtubeAnalytics", "v2", credentials=creds)
        # Sem OAuth, não é possível usar a Analytics API. O fallback por API key é apenas para Data API.
        raise RuntimeError("YouTube Analytics requer OAuth (defina YT_OAUTH_TOKEN_PATH)")

    def fetch_video_analytics_daily(self, start_date: str, end_date: str, channel_id: Optional[str]) -> pl.DataFrame:
        svc = self._yt_service()
        # Use ids=MINE com OAuth para evitar 403 quando o usuário autenticado não for exatamente o owner do channel_id fornecido
        ids_value = "channel==MINE"
        query = svc.reports().query(
            ids=ids_value,
            startDate=start_date,
            endDate=end_date,
            dimensions="day,video",
            metrics="views,estimatedMinutesWatched,averageViewDuration",
            sort="day",
        )
        resp = query.execute()
        rows: List[List[Any]] = resp.get("rows", [])
        cols: List[str] = [h.get("name") for h in resp.get("columnHeaders", [])]
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows, schema=cols)


