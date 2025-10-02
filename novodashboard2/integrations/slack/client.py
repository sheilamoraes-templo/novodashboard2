from __future__ import annotations

from dataclasses import dataclass
import json
import os
from typing import Any, Dict

import requests


@dataclass
class SlackClient:
    webhook_url: str

    @classmethod
    def from_env(cls) -> "SlackClient":
        url = os.getenv("SLACK_WEBHOOK_URL")
        if not url:
            raise RuntimeError("SLACK_WEBHOOK_URL não definido no ambiente")
        return cls(webhook_url=url)

    def send_text(self, text: str, **kwargs: Any) -> Dict[str, Any]:
        payload = {"text": text}
        payload.update(kwargs)
        resp = requests.post(self.webhook_url, data=json.dumps(payload), headers={"Content-Type": "application/json"})
        return {"status_code": resp.status_code, "text": resp.text}




