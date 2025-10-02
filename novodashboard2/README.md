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
GA4_PROPERTY_ID=476192590
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

## Plano de Ação Executável (MVP → Evolução)

Objetivo: evoluir o `novodashboard2` com uma camada de dados sólida e entregáveis incrementais, testáveis e executáveis por partes, mantendo a compatibilidade com a estrutura atual.

### Decisões de escopo
- Manter a estrutura existente (app, services, integrations, scripts, configs, data/warehouse).
- Evoluir por incrementos pequenos, cada um com DoD (Definition of Done) e comandos de execução.

### Entregáveis do MVP desta rodada
- Tabelas novas: `fact_ga4_sessions_by_utm_daily`, `fact_rd_email_campaign`, `dim_time`, `dim_content`, `bridge_ga4_content`, `bridge_yt_content`, `map_utm_campaign`, `fact_engagement_daily`, `fact_comms_impact_daily`.
- Helpers: comparativos WoW/MoM/MTD, normalização de UTM, expansão de freshness/qualidade.
- UI: páginas Executivo, Aquisição & CRM, Conteúdo & Retenção, Operação & Saúde (cards mínimos).

### 1) GA4 por UTM (impacto de campanhas)
- Materializar sessões/usuários por UTM:
  - Dimensions: `date, sessionSource, sessionMedium, sessionCampaignName`.
  - Metrics: `sessions, totalUsers`.
  - Tabela: `fact_ga4_sessions_by_utm_daily(date, source, medium, campaign, sessions, users)`.
- DoD:
  - Tabela criada e preenchida para últimos 30 dias.
  - `get_health()` reporta `latest_date_fact_ga4_sessions_by_utm_daily`.

### 2) RD — campanhas de e‑mail
- Coletar envios e métricas (sends, opens, clicks) e materializar em `fact_rd_email_campaign(date, campaignId, sends, opens, clicks)`.
- DoD:
  - Tabela criada e populada para janela alvo.
  - `get_health()` reporta `latest_date_fact_rd_email_campaign`.

### 3) Normalização UTM e mapeamento de campanha
- CSV: `catalog/map_utm_campaign.csv` com `utm_source,utm_medium,utm_campaign,campaignId,campaign_name`.
- Carregar para DuckDB em tabela `map_utm_campaign` e aplicar normalização (lower/strip).
- DoD: tabela com ao menos 1 mapping carregada e utilizada em joins.

### 4) Dimensões e bridges
- `dim_time`: calendário diário com ano/mês/semana e flags úteis (ex.: `is_today`).
- `dim_content` e bridges:
  - CSV `catalog/content_catalog.csv`: `content_id,title,channel,category,author,duration_sec,pagePath,videoId`.
  - Tabelas: `dim_content`, `bridge_ga4_content(pagePath,content_id)`, `bridge_yt_content(videoId,content_id)`.
- DoD: tabelas criadas e preenchidas a partir dos CSVs.

### 5) Fatos derivadas
- `fact_engagement_daily` (por data): une GA4 (`fact_sessions`) e YT (`fact_yt_channel_daily`).
- `fact_comms_impact_daily`: une `fact_rd_email_campaign` e `fact_ga4_sessions_by_utm_daily` via `map_utm_campaign` para janelas D-1, D0, D0–D+2.
- DoD: ambas as tabelas materializadas e consultáveis no DuckDB.

### 6) Helpers de comparativos e métricas
- Funções (services):
  - Semana corrente vs. anterior (seg–dom, exclui parciais).
  - MTD vs. mês anterior (até o mesmo dia).
  - 7d vs. 28d.
- Métricas derivadas:
  - Retenção YT: `estimatedMinutesWatched / views`.
  - Engajamento GA4: `screenPageViews / sessions` (e `engagedSessions/sessions` quando disponível).
- DoD: helpers retornam dicionários coerentes e são usados na UI/relatórios.

### 7) UI (Streamlit) — páginas e cards
- Executivo: KPIs (Usuários, Sessões, Minutos, Views, Leads), Δ WoW/MTD, tendência combinada (28d), Top Conteúdos (7d).
- Aquisição & CRM: UTM → Sessões/Leads; Funil RD (se disponível).
- Conteúdo & Retenção: retenção por vídeo (min/view), Pareto páginas.
- Operação & Saúde: freshness expandido e checks.
- DoD: páginas renderizam sem erro, com mensagens de fallback “sem dados”.

### 8) Qualidade e Freshness
- Checks simples (DuckDB): not null/unique e volumetria (queda >40% DoD).
- `get_health()` expandido com `latest_date_*` das novas tabelas.
- DoD: `st.json(get_health())` mostra novos campos.

### 9) Scripts utilitários (a adicionar em `scripts/`)
- `refresh_ga4_utm.py` → chama `refresh_sessions_by_utm_last_n_days`.
- `refresh_rd_campaigns.py` → chama `refresh_rd_email_campaign_last_n_days`.
- `import_content_catalog.py` → importa CSV para `dim_content` e bridges.
- DoD: scripts não interativos, aceitam `--days` e respeitam `.env`.

### 10) Configurações e documentação
- Atualizar `configs/datasets.yml` com novos datasets e checks mínimos.
- Atualizar este README com o fluxo de execução e comandos.

### Ordem sugerida de implementação
1. `fact_ga4_sessions_by_utm_daily`
2. `fact_rd_email_campaign`
3. `dim_time`
4. `map_utm_campaign`
5. `fact_engagement_daily`
6. `fact_comms_impact_daily`
7. Helpers comparativos
8. UI (páginas e cards)
9. Qualidade e `get_health()`
10. README e `datasets.yml`

### Comandos de execução (exemplos)
```
# Ambiente
pip install -r requirements.txt

# Warehouse inicial
python scripts/init_warehouse.py

# GA4
python scripts/refresh_ga4.py
# UTM (GA4)
python scripts/refresh_ga4_utm.py --days 30

# RD (campanhas)
python scripts/refresh_rd_campaigns.py --days 30

# Conteúdo (após criar CSV)
python scripts/import_content_catalog.py --path catalog/content_catalog.csv
# Mapping UTM → campanha (após criar CSV)
python scripts/import_map_utm_campaign.py --path catalog/map_utm_campaign.csv

# Dashboard
streamlit run app/dashboard.py --server.port 8050
```

Notas:
- Para YouTube Analytics é necessário OAuth (`YT_OAUTH_TOKEN_PATH`).
- Para GA4, use Service Account (`GOOGLE_APPLICATION_CREDENTIALS`) ou OAuth (`GA4_OAUTH_TOKEN_PATH`).
- Para RD Station, preencha `RD_CLIENT_ID`, `RD_CLIENT_SECRET`, `RD_REDIRECT_URI` e gere `rd_token.json` via `scripts/rd_oauth_login.py`.
