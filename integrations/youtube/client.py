from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from configs.settings import get_settings


@dataclass
class YouTubeClient:
    api_key: str

    @classmethod
    def from_env(cls) -> "YouTubeClient":
        s = get_settings()
        if not s.youtube_api_key:
            raise RuntimeError("YOUTUBE_API_KEY não definido")
        return cls(api_key=s.youtube_api_key)

    def fetch_channel_metrics(self, channel_id: str) -> Dict[str, Any]:
        raise NotImplementedError


