from __future__ import annotations

import os
import os.path as osp
import sys

# Garantir que o diretório raiz do projeto esteja no PYTHONPATH
CURRENT_DIR = osp.dirname(__file__)
ROOT_DIR = osp.abspath(osp.join(CURRENT_DIR, os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from integrations.slack.client import SlackClient


def main() -> None:
    sc = SlackClient.from_env()
    resp = sc.send_text("Teste do webhook do dashboard (ok)")
    print(resp)


if __name__ == "__main__":
    main()


