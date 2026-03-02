# CLAUDE.md — dataBobCFOdemoLight Development Guide

## Project Overview

Financial scenario planning application powered by Claude AI. Connects to Power BI Desktop models or Excel files, allows users to explore data models conversationally, then stage and apply financial scenario adjustments that produce SQL INSERT scripts.

## Architecture

Two-agent system where DiscoveryAgent builds a ModelUnderstanding document, which the ScenarioAgent consumes to plan scenarios:

```
ui.html (Single-page web UI)
  └── server.py (Flask API — bridges sync HTTP with async agents)
        ├── discovery/discovery_agent.py   → Data Understanding tab
        │     ├── discovery/schema_extractor.py
        │     └── discovery/model_understanding.py (central data structure)
        ├── agent.py                       → Scenario tab
        │     ├── prompts/builder.py (dynamic system prompt generation)
        │     └── queries.py (template-based query execution)
        ├── scenario.py (adjustment math + SQL generation)
        ├── datasources/
        │     ├── base.py           (abstract DataSource interface)
        │     ├── pbi_desktop.py    (Power BI Desktop via MCP)
        │     ├── excel_source.py   (Excel via openpyxl + DuckDB)
        │     ├── composite_source.py (multi-source wrapper)
        │     └── factory.py
        └── storage/sqlite_storage.py (persistent storage)
```

## Key Patterns

- **ModelUnderstanding** is the central contract between discovery and scenario phases. All model-specific knowledge (tables, accounts, queries, relationships) lives here — no hardcoded constants.
- **Template-based queries**: `query_templates` in ModelUnderstanding drive all data access. Templates use `{year}`, `{month_filter}`, `{company_id}`, `{value_type_id}` placeholders. The baseline query (key: `fetch_baseline`, legacy: `fetch_budget`) is NOT necessarily budget — it can be any value type, switched at runtime via `{value_type_id}`.
- **Stage → Apply workflow**: Adjustments accumulate via ` ```stage ` JSON blocks across turns. SQL is only generated when user confirms via ` ```apply ` block.
- **Async agents, sync Flask**: Agents are async (for MCP/data source calls). Flask runs in threads. A background asyncio event loop + `_run(coro)` helper bridges them.
- **Thread safety**: Global `_lock` in server.py protects shared state mutations.

## How to Run

```bash
pip install -r requirements.txt
# cp .env.example .env  — then edit .env with your API keys
python server.py
# Opens http://localhost:5000
```

## Configuration (.env)

| Variable | Purpose | Default |
|----------|---------|---------|
| `DISCOVERY_API_KEY` | Anthropic key for discovery agent | Falls back to `ANTHROPIC_API_KEY` |
| `SCENARIO_API_KEY` | Anthropic key for scenario agent | Falls back to `ANTHROPIC_API_KEY` |
| `DISCOVERY_MODEL` | Claude model for discovery | `claude-sonnet-4-6` |
| `SCENARIO_MODEL` | Claude model for scenarios | `claude-sonnet-4-6` |
| `POWERBI_MCP_EXE` | Path to PBI MCP executable | Required for PBI features |
| `HOST` / `PORT` | Server bind address | `127.0.0.1:5000` |

## File Quick Reference

| File | Lines | Purpose |
|------|-------|---------|
| `server.py` | ~960 | Flask API — all HTTP endpoints, agent lifecycle |
| `ui.html` | ~2800 | Single-page UI (HTML + CSS + JS) |
| `agent.py` | ~390 | ScenarioAgent — tool calls, staging, SQL generation |
| `queries.py` | ~460 | Query template execution (DAX/SQL), auto-builders |
| `discovery/discovery_agent.py` | ~600 | DiscoveryAgent — schema exploration, MU building |
| `discovery/model_understanding.py` | ~260 | ModelUnderstanding dataclass with typed accessors |
| `scenario.py` | ~175 | `build_scenario()` math + `make_sql()` generation |
| `prompts/builder.py` | ~200 | Dynamic system prompt from ModelUnderstanding |
| `config.py` | ~55 | Infrastructure config, .env loading |

## Coding Conventions

- Python 3.11+ with type hints
- Async/await for all DataSource operations
- `_run(coro)` to call async from Flask thread context
- ModelUnderstanding: `@property` accessors over `raw` dict
- API responses: always `{"ok": bool, ...}` JSON
- No hardcoded model-specific values — everything through ModelUnderstanding
