from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Iterable, List
import json
import time
import requests
import os

from configs.settings import get_settings


@dataclass
class RDClient:
    client_id: str
    client_secret: str
    redirect_uri: Optional[str] = None
    token_path: Optional[Path] = None
    base_url: str = "https://api.rd.services"
    account_id: Optional[str] = None

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
            account_id=s.rd_account_id,
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
        # Algumas contas exigem identificação explícita da conta
        if self.account_id:
            # Header best-effort; será ignorado se não suportado
            s.headers.update({"X-Account-Id": str(self.account_id)})
        return s

    # --- Debug helper ---
    @staticmethod
    def _debug_enabled() -> bool:
        val = os.getenv("RD_DEBUG", "0").strip().lower()
        return val in ("1", "true", "yes", "on")

    @classmethod
    def _dbg(cls, *, action: str, url: str, params: Optional[Dict[str, Any]] = None, status: Optional[int] = None, body_preview: Optional[str] = None) -> None:
        if not cls._debug_enabled():
            return
        safe_params = dict(params or {})
        # mascarar valores potencialmente sensíveis
        for k in list(safe_params.keys()):
            if "token" in k.lower() or "secret" in k.lower():
                safe_params[k] = "***"
        preview = (body_preview or "")
        if len(preview) > 300:
            preview = preview[:300] + "..."
        print(json.dumps({
            "RD_DEBUG": {
                "action": action,
                "url": url,
                "status": status,
                "params": safe_params,
                "body_preview": preview,
            }
        }, ensure_ascii=False))

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
        endpoints = [
            f"{self.base_url}/platform/contacts",
            f"{self.base_url}/contacts",
        ]
        # Variações de parâmetros: com filtros updated_at e sem filtros
        base_list: List[Dict[str, Any]] = [
            {"page": 1, "size": page_size, "updated_at[start]": updated_start_iso, "updated_at[end]": updated_end_iso},
            {"page": 1, "size": page_size, "updated_start": updated_start_iso, "updated_end": updated_end_iso},
            {"page": 1, "size": page_size},
        ]
        param_variants: List[Dict[str, Any]] = []
        for bp in base_list:
            param_variants.append(dict(bp))
            # com account_id
            if self.account_id:
                with_acc = dict(bp)
                with_acc["account_id"] = self.account_id
                param_variants.append(with_acc)
                with_acc2 = dict(bp)
                with_acc2["accountId"] = self.account_id
                param_variants.append(with_acc2)
        for url in endpoints:
            success_any = False
            for base_params in param_variants:
                params = dict(base_params)
                for _ in range(max_pages):
                    self._dbg(action="GET", url=url, params=params)
                    resp = sess.get(url, params=params, timeout=30)
                    self._dbg(action="RESP", url=url, params=params, status=resp.status_code, body_preview=resp.text)
                    # Fallback em erros comuns de API (400/422/500)
                    if resp.status_code in (400, 422, 500):
                        break
                    if resp.status_code == 404:
                        break
                    resp.raise_for_status()
                    data = resp.json()
                    items = data if isinstance(data, list) else data.get("items") or data.get("contacts") or []
                    if not items:
                        break
                    contacts.extend(items)
                    params["page"] = int(params.get("page", 1)) + 1
                    success_any = True
                if success_any:
                    break
            if success_any:
                break
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
            base_list = [
                {"page": 1, "size": page_size, "start_date": start_iso, "end_date": end_iso},
                {"page": 1, "size": page_size, "sent_at[start]": start_iso, "sent_at[end]": end_iso},
                {"page": 1, "size": page_size},
            ]
            param_variants = []
            for bp in base_list:
                param_variants.append(dict(bp))
                if self.account_id:
                    acc1 = dict(bp); acc1["account_id"] = self.account_id; param_variants.append(acc1)
                    acc2 = dict(bp); acc2["accountId"] = self.account_id; param_variants.append(acc2)
            ok = False
            for variant in param_variants + [{"page": 1, "size": page_size}]:
                params = dict(variant)
                for _ in range(max_pages):
                    self._dbg(action="GET", url=base, params=params)
                    resp = sess.get(base, params=params, timeout=30)
                    self._dbg(action="RESP", url=base, params=params, status=resp.status_code, body_preview=resp.text)
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
            self._dbg(action="GET", url=url, params=None)
            resp = sess.get(url, timeout=30)
            self._dbg(action="RESP", url=url, params=None, status=resp.status_code, body_preview=resp.text)
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

    def fetch_email_campaign_by_id(self, campaign_id: str) -> Dict[str, Any]:
        """Busca os metadados/estatísticas de uma campanha específica por ID.

        Tenta múltiplos endpoints conhecidos. Inclui account_id/accountId quando disponível.
        """
        sess = self.authorized_session()
        candidates = [
            f"{self.base_url}/platform/emails/campaigns/{campaign_id}",
            f"{self.base_url}/marketing/email/campaigns/{campaign_id}",
        ]
        param_variants: List[Dict[str, Any]] = [{}]
        if self.account_id:
            param_variants = [{}, {"account_id": self.account_id}, {"accountId": self.account_id}]
        for url in candidates:
            for params in param_variants:
                self._dbg(action="GET", url=url, params=params)
                resp = sess.get(url, params=params, timeout=30)
                self._dbg(action="RESP", url=url, params=params, status=resp.status_code, body_preview=resp.text)
                if resp.status_code == 404:
                    continue
                if resp.status_code in (400, 422, 500):
                    break
                resp.raise_for_status()
                data = resp.json()
                return data if isinstance(data, dict) else {"raw": data}
        return {}


