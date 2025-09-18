from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Iterable, List
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
    base_url: str = "https://api.rd.services"

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

    # --- High-level fetchers (best-effort; endpoints podem variar por plano/versionamento) ---
    def fetch_contacts_paginated(
        self,
        *,
        updated_start_iso: str,
        updated_end_iso: str,
        page_size: int = 100,
        max_pages: int = 50,
    ) -> List[Dict[str, Any]]:
        """Busca contatos/lead em páginas, filtrando por intervalo de atualização quando suportado.

        Nota: O endpoint e parâmetros podem variar. Tentamos com /platform/contacts e caímos em uma variante comum.
        """
        sess = self.authorized_session()
        contacts: List[Dict[str, Any]] = []
        # Tenta endpoint v2
        url = f"{self.base_url}/platform/contacts"
        params = {
            "page": 1,
            "size": page_size,
            # Filtros comuns; algumas versões usam updated_at[start]/[end]
            "updated_at[start]": updated_start_iso,
            "updated_at[end]": updated_end_iso,
        }
        for _ in range(max_pages):
            resp = sess.get(url, params=params, timeout=30)
            if resp.status_code == 404:
                break
            resp.raise_for_status()
            data = resp.json()
            items = data if isinstance(data, list) else data.get("items") or data.get("contacts") or []
            if not items:
                break
            contacts.extend(items)
            params["page"] = int(params.get("page", 1)) + 1
        return contacts


    # --- Email campaigns (best-effort; endpoints podem variar por plano/versionamento) ---
    def fetch_email_campaigns(
        self,
        *,
        start_iso: str,
        end_iso: str,
        page_size: int = 100,
        max_pages: int = 50,
    ) -> List[Dict[str, Any]]:
        sess = self.authorized_session()
        campaigns: List[Dict[str, Any]] = []

        candidates = [
            f"{self.base_url}/platform/emails/campaigns",
            f"{self.base_url}/marketing/email/campaigns",
        ]
        for base in candidates:
            # Tentar múltiplas variantes de filtros de data; se falhar, tentar sem filtro
            param_variants = [
                {"page": 1, "size": page_size, "start_date": start_iso, "end_date": end_iso},
                {"page": 1, "size": page_size, "sent_at[start]": start_iso, "sent_at[end]": end_iso},
            ]
            ok = False
            for variant in param_variants + [{"page": 1, "size": page_size}]:
                params = dict(variant)
                for _ in range(max_pages):
                    resp = sess.get(base, params=params, timeout=30)
                    if resp.status_code in (400, 422):
                        # Troca de variante de parâmetros
                        break
                    if resp.status_code == 404:
                        # Tenta próximo endpoint base
                        break
                    resp.raise_for_status()
                    data = resp.json()
                    items = data if isinstance(data, list) else data.get("items") or data.get("campaigns") or []
                    if not items:
                        break
                    campaigns.extend(items)
                    params["page"] = int(params.get("page", 1)) + 1
                    ok = True
                if ok:
                    break
            if ok:
                break
        return campaigns

    def fetch_email_metrics(self, campaign_id: str) -> Dict[str, int]:
        sess = self.authorized_session()
        endpoints = [
            f"{self.base_url}/platform/emails/campaigns/{campaign_id}/metrics",
            f"{self.base_url}/marketing/email/campaigns/{campaign_id}/metrics",
        ]
        for url in endpoints:
            resp = sess.get(url, timeout=30)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            data = resp.json()
            # Normaliza chaves comuns
            sends = data.get("sends") or data.get("delivered") or data.get("sent") or 0
            opens = data.get("opens") or data.get("unique_opens") or 0
            clicks = data.get("clicks") or data.get("unique_clicks") or 0
            return {"sends": int(sends or 0), "opens": int(opens or 0), "clicks": int(clicks or 0)}
        return {"sends": 0, "opens": 0, "clicks": 0}


