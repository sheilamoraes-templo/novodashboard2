from __future__ import annotations

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

import requests
import webbrowser

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

from configs.settings import get_settings


class _CallbackHandler(BaseHTTPRequestHandler):
    code: str | None = None

    def do_GET(self):  # type: ignore[override]
        parsed = urlparse(self.path)
        if parsed.path != "/callback":
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")
            return
        qs = parse_qs(parsed.query)
        _CallbackHandler.code = (qs.get("code") or [None])[0]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"RD Station OAuth concluido. Pode fechar esta janela.")


def main() -> None:
    s = get_settings()
    client_id = s.rd_client_id
    client_secret = s.rd_client_secret
    redirect_uri = s.rd_redirect_uri or "http://localhost:8050/callback"
    token_path = s.rd_token_path or s.data_dir / "rd_token.json"

    if not client_id or not client_secret:
        raise SystemExit("Defina RD_CLIENT_ID e RD_CLIENT_SECRET no .env")

    auth_url = (
        "https://api.rd.services/auth/dialog"
        f"?client_id={client_id}&redirect_uri={redirect_uri}&state=xyz&response_type=code"
    )
    print("Abra no navegador e autorize:\n", auth_url)
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass

    # Servidor HTTP local para capturar o callback
    host, port = "", 8050
    srv = HTTPServer((host, port), _CallbackHandler)
    th = threading.Thread(target=srv.handle_request)
    th.start()
    th.join(timeout=300)
    srv.server_close()

    code = _CallbackHandler.code
    if not code:
        raise SystemExit("Código de autorização não recebido. Tente novamente.")

    # Trocar code por token
    token_endpoint = "https://api.rd.services/auth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }
    resp = requests.post(token_endpoint, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    tok = {
        "access_token": data.get("access_token"),
        "refresh_token": data.get("refresh_token"),
        "expires_in": data.get("expires_in"),
    }
    if tok.get("expires_in"):
        import time

        tok["expires_at"] = int(time.time()) + int(tok["expires_in"])  # type: ignore[arg-type]

    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(json.dumps(tok, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Token salvo em {token_path}")


if __name__ == "__main__":
    main()


