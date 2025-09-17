from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import json
import time
import requests

from configs.settings import get_settings


@dataclass
class RDClient:
    client_id: str
    client_secret: str
    redirect_uri: Optional[str] = None
    token_path: Optional[Path] = None

    @classmethod
    def from_env(cls) -> "RDClient":
        s = get_settings()
        if not s.rd_client_id or not s.rd_client_secret:
            raise RuntimeError("RD_CLIENT_ID/SECRET não definidos")
        return cls(
            client_id=s.rd_client_id,
            client_secret=s.rd_client_secret,
            redirect_uri=s.rd_redirect_uri,
            token_path=s.rd_token_path,
        )

    def fetch_leads(self) -> Dict[str, Any]:
        raise NotImplementedError

    # --- OAuth helpers ---
    @staticmethod
    def _token_endpoint() -> str:
        return "https://api.rd.services/auth/token"

    def load_token(self) -> Dict[str, Any]:
        if not self.token_path or not self.token_path.exists():
            return {}
        try:
            return json.loads(self.token_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def save_token(self, token: Dict[str, Any]) -> None:
        if not self.token_path:
            return
        self.token_path.parent.mkdir(parents=True, exist_ok=True)
        self.token_path.write_text(json.dumps(token, ensure_ascii=False, indent=2), encoding="utf-8")

    def _refresh_token_if_needed(self) -> Dict[str, Any]:
        tok = self.load_token()
        if not tok:
            raise RuntimeError("Token RD Station ausente. Execute o fluxo OAuth.")
        expires_at = tok.get("expires_at")
        if expires_at and time.time() < expires_at - 60:
            return tok
        # refresh
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": tok.get("refresh_token"),
            "grant_type": "refresh_token",
        }
        resp = requests.post(self._token_endpoint(), json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        new_tok = {
            "access_token": data.get("access_token"),
            "refresh_token": data.get("refresh_token", tok.get("refresh_token")),
            "expires_in": data.get("expires_in"),
        }
        if new_tok.get("expires_in"):
            new_tok["expires_at"] = int(time.time()) + int(new_tok["expires_in"])
        self.save_token(new_tok)
        return new_tok

    def authorized_session(self) -> requests.Session:
        tok = self._refresh_token_if_needed()
        s = requests.Session()
        s.headers.update({"Authorization": f"Bearer {tok.get('access_token')}"})
        return s


