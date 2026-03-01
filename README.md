# dataBob CFO — Financial Scenario Planning Agent

A model-agnostic AI agent for building financial scenarios on top of Power BI Desktop or Excel data sources.

## What It Does

1. **Discovers** your data model — tables, relationships, accounts, measures — via a conversational Discovery Agent
2. **Plans scenarios** — percentage or absolute adjustments to budget baselines, across any account group, month, or dimension
3. **Generates SQL** — produces INSERT scripts ready to load back into your data warehouse as new scenario rows

Supports **Power BI Desktop** (DAX queries via MCP) and **Excel** files (SQL via DuckDB).

---

## Architecture

```
UI (ui.html)  ──>  Flask Server (server.py)
                        │
          ┌─────────────┴─────────────┐
    DiscoveryAgent              ScenarioAgent
    (discovery_agent.py)        (agent.py)
          │                          │
    SchemaExtractor             PromptBuilder
    (schema_extractor.py)       (prompts/builder.py)
          │                          │
          └──────── DataSource ──────┘
                   (base.py)
                  /          \
        PBIDesktopSource   ExcelSource
        (pbi_desktop.py)   (excel_source.py)
```

**Two-phase workflow:**
- **Phase 1 — Data Understanding**: The Discovery Agent explores the connected data source, extracts schema, and builds a Model Understanding document through conversation.
- **Phase 2 — Scenario Planning**: The Scenario Agent uses the Model Understanding to query baselines, stage adjustments, preview GL impact, and generate SQL output.

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

Copy `.env.example` to `.env` and fill in your API keys:

```bash
cp .env.example .env
# Then edit .env with your keys:
# DISCOVERY_API_KEY=sk-ant-...
# SCENARIO_API_KEY=sk-ant-...
```

You can use a single key for both agents, or separate keys for cost tracking. If neither is set, the system falls back to `ANTHROPIC_API_KEY`.

### 3. (Optional) Configure Power BI MCP

If connecting to Power BI Desktop, set the MCP executable path:

```bash
# .env
POWERBI_MCP_EXE=C:\Users\YourName\.vscode\extensions\analysis-services.powerbi-modeling-mcp-0.1.9-win32-x64\server\powerbi-modeling-mcp.exe
```

### 4. Run

```bash
python server.py
```

Open `http://localhost:5000` in your browser.

For CLI-only mode (scenario agent without the web UI):
```bash
python scenario_agent.py
```

---

## Usage

### Web UI

1. **Models** — Click the blue Models button to create or select a saved model
2. **Connect** — Link a Power BI Desktop instance or upload an Excel file
3. **Data Understanding** tab — Chat with the Discovery Agent to explore your schema and build the Model Understanding
4. **Confirm** — Once the understanding is complete, confirm it to unlock scenarios
5. **Scenario** tab — Describe adjustments in natural language, stage them, preview GL impact, and generate SQL

### Example prompts

```
Load 2026 budget data
Create a scenario with +3% revenue across all months
Add -5% to COGS in Q1 (months 1,2,3)
Who are the top 10 customers by revenue?
Create a scenario reducing the biggest customer by 25%
```

---

## Configuration

All configuration is in `.env` (loaded by `config.py`):

| Variable | Default | Description |
|----------|---------|-------------|
| `DISCOVERY_API_KEY` | `ANTHROPIC_API_KEY` | API key for the Discovery Agent |
| `SCENARIO_API_KEY` | `ANTHROPIC_API_KEY` | API key for the Scenario Agent |
| `DISCOVERY_MODEL` | `claude-sonnet-4-6` | Claude model for discovery |
| `SCENARIO_MODEL` | `claude-sonnet-4-6` | Claude model for scenarios |
| `POWERBI_MCP_EXE` | *(empty)* | Path to PBI MCP executable |
| `HOST` | `127.0.0.1` | Server bind address |
| `PORT` | `5000` | Server port |

---

## Output

SQL scripts are saved to `./output/` with timestamped names:
```
output/scenario_Rev+3pct_20260301_143021.sql
```

Each script includes:
- Header comment with adjustment description
- Commented-out DELETE for safe re-loading
- INSERT statements for all affected account × month combinations
- Verification SELECT

---

## Troubleshooting

**"No API key configured"**
Set at least one of `DISCOVERY_API_KEY`, `SCENARIO_API_KEY`, or `ANTHROPIC_API_KEY` in `.env` or your environment.

**"POWERBI_MCP_EXE not configured"**
Set `POWERBI_MCP_EXE` in `.env` to the full path of your PBI MCP executable.

**"Query returned 0 rows"**
Go to Data Understanding and verify the query templates are working. Use the Edit button on Query Templates to inspect and fix them.

**Scenario queries return different results than discovery**
After making changes in the Data Understanding tab, the Scenario Agent is automatically refreshed. If issues persist, reload the page.
