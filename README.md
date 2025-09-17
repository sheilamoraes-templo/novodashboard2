# NOVO DASHBOARD 2 – GA4 + YouTube + RD Station (Plano completo)

Dashboard modular, resiliente e flexível para consolidar dados de Google Analytics 4 (GA4), YouTube e RD Station, com camada de dados robusta, cache local, validações e UI configurável.

## Objetivos e princípios
- Separar dados da visualização (UI consome datasets padronizados).
- Config-driven: datasets e views definidos em YAML, sem “hardcode”.
- Resiliência: erros isolados por card (circuit breaker e último snapshot bom).
- Observabilidade: freshness, health, logs estruturados.

## Arquitetura (visão geral)
Ingestão (Conectores) → Tratamento/Modelo (ELT) → Armazenamento (DuckDB/Parquet) → Service Layer (funções/endpoints) → UI (Streamlit) configurada por YAML.

## Camada de dados
- Catálogo de metadados por fonte: `catalog/*_metadata.json` (descoberta GA4/YT/RD).
- Dataset Registry: `configs/datasets.yml` define datasets canônicos (dimensões, métricas, granularidade, checks, TTL).
- Formato “tidy”: dimensões explícitas, métricas numéricas, metacampos (`ingested_at`, `query_hash`).

## Fase 1 (GA4 primeiro) – Implementado
- Conector GA4 com validação de dimensões/métricas, paginação e retry/backoff.
- Materializações DuckDB:
  - `fact_ga4_events_daily(date, eventName, eventCount, activeUsers)`
  - `fact_ga4_pages_daily(date, pagePath, pageTitle, screenPageViews, sessions, totalUsers)`
- UI: botão “Atualizar dados GA4” executa as três materializações (sessions, events, pages). Cards isolados com try/except.
- Health: mostra freshness de `fact_sessions`, `fact_ga4_pages_daily` e `fact_ga4_events_daily`.

## Próximas fases
- Fase 2 – Views/Layout dinâmicos (YAML → `render_view(view_id)`)
- Fase 3 – YouTube (Analytics + Data APIs; fatos diários por vídeo)
- Fase 4 – RD Station (OAuth2, webhooks/polling, funil e campanhas)
- Fase 5 – Qualidade/Operação (checks, retries/agendamento, alertas)
- Fase 6 – UX/Governança (freshness por card, filtros globais, docs)

## Estrutura do projeto
- `app/` (dashboard.py)
- `configs/` (settings.py, datasets.yml)
- `data/` (raw, api_cache, warehouse)
- `integrations/` (ga4, youtube, rd, slack)
- `services/` (data_service.py, report_service.py, ga4_refresh.py, schema)
- `scripts/` (init_warehouse.py, refresh_ga4.py, diagnose_env.py, ga4_inventory.py, ga4_matrix_test.py, ga4_oauth_login.py, rd_oauth_login.py, slack_test_webhook.py, import_csv_*.py)
- `tests/`

## Requisitos
- Python 3.11+
- GA4: OAuth Installed App (client_secret_*.json) ou Service Account.
- Slack: Webhook (para envio de relatório).

## Configuração (.env)
Crie `.env` na raiz:

```
# GA4
GA4_PROPERTY_ID=383182413
# OAuth (Installed App)
GA4_OAUTH_CLIENT_SECRET=./gcloud_token.json
GA4_OAUTH_TOKEN_PATH=./ga4_token.json
# (Opcional) Service Account
# GOOGLE_APPLICATION_CREDENTIALS=C:/caminho/para/service_account.json

# RD Station
RD_CLIENT_ID=
RD_CLIENT_SECRET=
RD_REDIRECT_URI=http://localhost:8050/callback
RD_TOKEN_PATH=./rd_token.json

# Slack
SLACK_WEBHOOK_URL=

DATA_DIR=./data
```

Boas práticas: `.env`, `gcloud_token.json`, `ga4_token.json` e `secrets/**` estão no `.gitignore`.

## Fluxos OAuth
- GA4: `python scripts/ga4_oauth_login.py` → abre navegador e salva `ga4_token.json`.
- RD Station: `python scripts/rd_oauth_login.py` → captura `http://localhost:8050/callback` e salva `rd_token.json`.

## Instalação e uso
1. Instalar dependências:
```
pip install -r requirements.txt
```
2. Inicializar warehouse:
```
python scripts/init_warehouse.py
```
3. Atualizar GA4 (sessions, events, pages):
```
python scripts/refresh_ga4.py
```
4. Testar Slack (webhook):
```
python scripts/slack_test_webhook.py
```
5. Executar dashboard:
```
streamlit run app/dashboard.py --server.port 8050
```

## Observações
- “Top Páginas” usa `fact_ga4_pages_daily` (corrige agregação incorreta anterior).
- CSVs permanecem como fallback opcional.

## Referências
- GA4: https://developers.google.com/analytics/devguides/reporting/data/v1
- YouTube: https://developers.google.com/youtube
- RD Station: https://developers.rdstation.com/pt-BR/
- DuckDB: https://duckdb.org/
- Polars: https://pola.rs/
- Streamlit: https://docs.streamlit.io/
