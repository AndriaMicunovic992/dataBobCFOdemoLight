"""
DiscoveryAgent — Conversational agent for building model understanding.

Has its own system prompt, conversation history, and tools. Lives in the
Data Understanding tab and converses with the user to fill gaps in the
automatically extracted schema.
"""

import os
import json
import anthropic

from datasources.base import DataSource
from discovery.schema_extractor import SchemaExtractor
from discovery.model_understanding import ModelUnderstanding
from storage.sqlite_storage import SQLiteStorage


# ── System Prompt ──────────────────────────────────────────────────────────────

DISCOVERY_PROMPT = """You are a data model analyst. Your job is to connect to a data source,
explore its schema, and build a complete "Model Understanding" document that will be used
by a scenario planning agent.

== YOUR GOAL ==
Produce a structured JSON document (the Model Understanding) that describes:
1. What tables exist and their roles (fact vs dimension)
2. How tables relate to each other
3. Which table contains the main financial/transactional data (the "fact table")
4. What the key columns are (accounts, dates, amounts, categories)
5. How accounts are grouped (revenue, costs, balance sheet, etc.)
6. What filters apply (company, value type, etc.)
7. What DAX measures exist and which are important for analysis
8. **CRITICAL: Working query templates** for fetching budget data and account metadata

== WORKFLOW ==
1. Start by calling `extract_schema` to get the raw schema with sample data.
2. Analyze what you see. Identify the most likely fact table (largest table with
   amounts/dates), dimension tables (lookups with IDs and names), and relationships.
3. Present your findings to the user and ask targeted questions to fill gaps:
   - "Which table contains the main financial data?"
   - "What does column X mean?"
   - "How are accounts grouped? Which are revenue, which are costs?"
   - "Is there a company/entity filter?"
   - "What do the value_type values mean? (e.g., 1=actuals, 2=budget)"
4. Build query templates (see QUERY TEMPLATES section below).
5. **ALWAYS validate your query templates** by calling `run_test_query` before saving.
6. After the user confirms key aspects, call `save_understanding` with the full JSON.
7. The user can continue refining — call `save_understanding` again to update.

== FOR POWER BI MODELS ==
PBI models have rich metadata: relationships are auto-discovered, column types are known,
hierarchies may exist. You can auto-infer most of the structure. Ask fewer questions —
focus on business semantics (what do account groups mean, what's the scenario convention).

== FOR EXCEL FILES ==
Excel has no relationships or type metadata. You must:
- Ask which sheet is the main data table
- Ask how sheets relate (shared key columns)
- Ask about column meanings since names may be ambiguous
- Be more conversational — the user is your primary source of knowledge

== MODEL UNDERSTANDING JSON FORMAT ==
When calling save_understanding, provide JSON with this structure:
{
  "model_name": "Human-readable name",
  "domain": "finance",
  "description": "Brief description",
  "status": "draft",
  "tables": {
    "TableName": {
      "role": "fact|dimension",
      "description": "Short desc",
      "key_columns": ["id_col"],
      "important_columns": {
        "col_name": {"purpose": "desc", "data_type": "string|int|float|date"}
      }
    }
  },
  "relationships": [
    {"from_table": "A", "from_column": "x", "to_table": "B", "to_column": "y"}
  ],
  "account_structure": {
    "account_table": "DimAccounts",
    "account_id_column": "account_id",
    "account_name_column": "account_name",
    "grouping_columns": ["ReportingGroup"],
    "groups": {
      "revenue": {"description": "Revenue", "account_ids": [1, 2, 3]},
      "cogs": {"description": "COGS", "account_ids": [4, 5]}
    }
  },
  "filter_dimensions": {
    "company": {"table": "FactTable", "column": "company_id", "default_value": 4}
  },
  "scenario_target": {
    "fact_table": "FactTable",
    "date_column": "accounting_date",
    "amount_columns": ["amount", "budget_amount"],
    "scenario_type_column": "value_type_id",
    "scenario_type_values": {"actuals": 1, "budget": 2, "scenario_base": 3}
  },
  "reporting_groups": {
    "pl_groups": ["Revenue", "COGS", "Operating Expenses"],
    "bs_groups": ["Assets", "Liabilities"]
  },
  "query_language": "DAX",
  "query_templates": {
    "fetch_budget": "<REQUIRED - see QUERY TEMPLATES section>",
    "fetch_account_map": "<REQUIRED - see QUERY TEMPLATES section>"
  },
  "sql_target": {
    "table_name": "[Fact Table Name]",
    "columns": ["main_account_id", "company_id", "accounting_date", "..."]
  },
  "measures": {
    "MeasureName": {"expression": "SUM(...)", "table": "TableName", "description": "Short desc"}
  },
  "cashflow_config": {
    "structure_table": "DimCashflow",
    "position_column": "cf_position"
  },
  "customer_config": {
    "customer_table": "DimCustomer",
    "customer_id_column": "customer_id",
    "invoice_table": "FactInvoice"
  }
}

== QUERY TEMPLATES (CRITICAL) ==
The scenario agent CANNOT function without working query templates. You MUST include these
two templates in every model understanding. Templates use Python format placeholders.

--- fetch_budget ---
Purpose: Fetch all budget/baseline rows for a given year. The scenario agent uses this
data to apply percentage or absolute adjustments.

Required placeholders:
  {year}          — integer, e.g. 2026
  {month_filter}  — string, auto-built by the system. Will be empty string for full year,
                    or a DAX/SQL filter clause for specific months. Your template must
                    place this where an additional AND/&& clause can be appended.
  {company_id}    — integer or string, company/entity filter value

Required output column aliases (use these EXACT names):
  "main_account_id"  — the account/GL ID (integer)
  "accounting_date"  — the date (date or string YYYY-MM-DD)
  "amount"           — the actual/budget amount (number)
  "budget_amount"    — the budget amount (number, can be same as amount)
  Plus any additional FK columns the fact table has: currency_id, cost_object_id,
  cost_center_id, settlement_type_id, item_group_id, project_id, etc.

DAX example (adapt table/column names to actual model):
  EVALUATE SELECTCOLUMNS(
    FILTER(
      'Fakten Hauptbuch',
      YEAR('Fakten Hauptbuch'[Buchungsdatum]) = {year}
      && 'Fakten Hauptbuch'[Firma] = {company_id}
      && 'Fakten Hauptbuch'[Wertart] = 2
      {month_filter}
    ),
    "main_account_id", 'Fakten Hauptbuch'[Hauptkonto],
    "accounting_date", 'Fakten Hauptbuch'[Buchungsdatum],
    "amount", 'Fakten Hauptbuch'[Betrag],
    "budget_amount", 'Fakten Hauptbuch'[Budgetbetrag],
    "currency_id", 'Fakten Hauptbuch'[Währung],
    "cost_object_id", 'Fakten Hauptbuch'[Kostenträger],
    "cost_center_id", 'Fakten Hauptbuch'[Kostenstelle]
  )

SQL/DuckDB example:
  SELECT account_id AS main_account_id, date AS accounting_date,
         amount, budget_amount, currency_id, cost_center_id
  FROM transactions
  WHERE YEAR(date) = {year} AND company_id = {company_id}
    AND value_type = 2 {month_filter}

IMPORTANT for {month_filter} placement:
- In DAX: place it after other FILTER conditions, so the system can append
  "&& (MONTH('Table'[DateCol])=1 || MONTH('Table'[DateCol])=2)"
- In SQL: place it after WHERE conditions, so the system can append
  "AND EXTRACT(MONTH FROM date_col) IN (1, 2)"

--- fetch_account_map ---
Purpose: Fetch GL account metadata (names, groups, cashflow positions) for a set of
account IDs. Used to enrich budget rows with human-readable names.

Required placeholders:
  {account_ids}  — comma-separated integers, e.g. "112, 114, 200, 300"

Required output column aliases (use these EXACT names):
  "id"           — the account ID (integer)
  "nr"           — the account number/code (string, e.g. "320000")
  "name"         — the account name (string)
  "group"        — the reporting group (string, e.g. "Revenue", "COGS")
  "cf_position"  — cashflow position ID (integer, 0 if N/A)

DAX example:
  EVALUATE SELECTCOLUMNS(
    FILTER('Dim Hauptkonten',
      'Dim Hauptkonten'[MainAccountID] IN {{{account_ids}}}
    ),
    "id", 'Dim Hauptkonten'[MainAccountID],
    "nr", 'Dim Hauptkonten'[MainAccountNumber],
    "name", 'Dim Hauptkonten'[MainAccountName],
    "group", 'Dim Hauptkonten'[ReportingGroup],
    "cf_position", 'Dim Hauptkonten'[CashflowPosition]
  )

NOTE on DAX IN syntax: The placeholder {account_ids} produces "112, 114, 200".
In DAX, the IN operator needs curly braces: IN {112, 114, 200}. Since Python
.format() uses {{ and }} for literal braces, write it as: IN {{{account_ids}}}
This renders as IN {112, 114, 200} at runtime.

SQL/DuckDB example:
  SELECT id, account_number AS nr, name, reporting_group AS "group",
         COALESCE(cf_position, 0) AS cf_position
  FROM accounts WHERE id IN ({account_ids})

--- Optional: query_customers_top ---
If the model has a customer dimension with invoice/sales data, you can optionally include:
  "query_customers_top": a query template returning top N customers by revenue
  "query_customers_total": a query returning total revenue for a year

== VALIDATION ==
Before saving the understanding, you MUST:
1. Build the fetch_budget template with actual table/column names from the schema
2. Test it with run_test_query (use a concrete year, e.g. 2025 or 2026, and fill
   {month_filter} with empty string, {company_id} with the actual company ID)
3. Verify the result has columns: main_account_id, accounting_date, amount, budget_amount
4. Build the fetch_account_map template and test it too (use a few real account IDs
   from the fetch_budget results as the {account_ids} value)
5. Verify the result has columns: id, nr, name, group
6. Only save the understanding once BOTH templates return valid data

If a query fails, debug it: check column names, table names, filter values.
Use get_sample_data to inspect table contents. Fix and re-test until it works.

== RULES ==
- Always start with extract_schema before asking questions.
- Be concise — ask 2-3 questions at a time.
- Save understanding when you have enough info (always with status="draft").
- NEVER set status="confirmed" — the user will confirm via the UI button when ready.
- When the understanding is complete, tell the user: "The model understanding looks ready. You can confirm it using the Confirm button."
- If the user corrects something, update and re-save.
- Focus on what the scenario agent needs: fact table, accounts, amounts, dates, groups, QUERY TEMPLATES.
- **Never save without working query_templates.** The scenario agent is useless without them.

== IMPORTANT: COMPACT JSON ==
When calling save_understanding, you MUST keep the JSON compact to avoid output truncation:
- Do NOT include whitespace/indentation in the JSON — output it as a single dense blob.
- Only include sections you have real data for. Skip empty/unknown sections entirely.
- For tables: only list the most important tables (fact tables, key dimensions). Skip hidden or auxiliary tables.
- For important_columns: only list 3-5 key columns per table, not every column.
- Keep descriptions very short (under 10 words each).
- Prefer IDs and short names over long descriptions.
"""


# ── Tool Definitions ──────────────────────────────────────────────────────────

DISCOVERY_TOOLS = [
    {
        "name": "extract_schema",
        "description": (
            "Extract the raw schema from the connected data source. "
            "Returns tables, columns, relationships, sample data, and basic statistics. "
            "Call this first to understand what data is available."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sample_rows": {
                    "type": "integer",
                    "description": "Number of sample rows per table (default 10)",
                    "default": 10,
                },
            },
        },
    },
    {
        "name": "get_sample_data",
        "description": (
            "Get sample rows from a specific table. "
            "Use this to inspect a table's data more closely."
        ),
        "input_schema": {
            "type": "object",
            "required": ["table_name"],
            "properties": {
                "table_name": {"type": "string", "description": "Table name to sample"},
                "max_rows":   {"type": "integer", "description": "Max rows (default 20)", "default": 20},
            },
        },
    },
    {
        "name": "run_test_query",
        "description": (
            "Run a test query against the data source to validate a query template "
            "or explore data. Returns the first 10 rows of results."
        ),
        "input_schema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string", "description": "The query to execute (DAX or SQL)"},
            },
        },
    },
    {
        "name": "save_understanding",
        "description": (
            "Save or update the Model Understanding document. "
            "Call this whenever you have new information to persist. "
            "Pass the full JSON structure (not a partial patch)."
        ),
        "input_schema": {
            "type": "object",
            "required": ["understanding"],
            "properties": {
                "understanding": {
                    "type": "object",
                    "description": "The full Model Understanding JSON document",
                },
            },
        },
    },
    {
        "name": "get_understanding",
        "description": (
            "Load the current Model Understanding document from storage. "
            "Returns null if no understanding has been saved yet."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
]


# ── Discovery Agent ───────────────────────────────────────────────────────────

class DiscoveryAgent:
    """
    Conversational agent for building model understanding.

    Has its own conversation history, tools, and system prompt.
    Lives in the Data Understanding tab of the UI.
    """

    def __init__(self, source: DataSource, storage: SQLiteStorage,
                 model_id: str | None = None):
        self.source = source
        self.storage = storage
        self.model_id = model_id
        from config import DISCOVERY_API_KEY
        self.ai = anthropic.Anthropic(api_key=DISCOVERY_API_KEY)
        self.conv: list[dict] = []
        self._schema_cache: dict | None = None

    # ── Tool Execution ─────────────────────────────────────────────────────

    async def _handle_tool(self, name: str, inp: dict) -> str:
        if name == "extract_schema":
            sample_rows = inp.get("sample_rows", 10)
            extractor = SchemaExtractor(self.source)
            self._schema_cache = await extractor.extract(sample_rows=sample_rows)

            # Format for Claude — limit output size
            schema = self._schema_cache
            lines = [f"Source type: {schema['source_type']}",
                     f"Query language: {schema['query_language']}",
                     f"Tables: {len(schema['tables'])}",
                     f"Relationships: {len(schema['relationships'])}",
                     ""]

            for t in schema["tables"]:
                cols = t.get("columns", [])
                rows = t.get("row_count")
                row_info = f"  ({rows} rows)" if rows else ""
                lines.append(f"=== {t['name']}{row_info} ===")
                hidden = " [HIDDEN]" if t.get("is_hidden") else ""
                src = f" [from {t.get('source_file', '')}]" if t.get("source_file") else ""
                if hidden or src:
                    lines.append(f"  {hidden}{src}")

                for c in cols:
                    nullable = "?" if c.get("is_nullable") else ""
                    h = " [hidden]" if c.get("is_hidden") else ""
                    lines.append(f"  - {c['name']}: {c['data_type']}{nullable}{h}")

                # Show stats and samples
                stats = t.get("statistics", {})
                for col_name, s in list(stats.items())[:5]:
                    vals = s.get("sample_values", [])[:3]
                    if vals:
                        lines.append(f"    {col_name} samples: {', '.join(vals)}")

                lines.append("")

            if schema["relationships"]:
                lines.append("=== RELATIONSHIPS ===")
                for r in schema["relationships"]:
                    active = "" if r.get("is_active", True) else " [INACTIVE]"
                    lines.append(
                        f"  {r.get('from_table')}.{r.get('from_column')} → "
                        f"{r.get('to_table')}.{r.get('to_column')}{active}"
                    )

            measures = schema.get("measures", [])
            if measures:
                lines.append("")
                lines.append(f"=== DAX MEASURES ({len(measures)}) ===")
                for m in measures:
                    hidden = " [hidden]" if m.get("is_hidden") else ""
                    expr = (m.get("expression") or "")[:80]
                    lines.append(
                        f"  [{m.get('table', '')}] {m.get('name', '')}{hidden} = {expr}"
                    )

            return "\n".join(lines)

        elif name == "get_sample_data":
            table_name = inp["table_name"]
            max_rows = inp.get("max_rows", 20)
            rows = await self.source.get_sample_data(table_name, max_rows)
            if not rows:
                return f"No data found in table '{table_name}'."
            # Format as text table
            cols = list(rows[0].keys())
            lines = [" | ".join(cols)]
            lines.append("-" * len(lines[0]))
            for r in rows[:max_rows]:
                lines.append(" | ".join(str(r.get(c, ""))[:30] for c in cols))
            return "\n".join(lines)

        elif name == "run_test_query":
            query = inp["query"]
            result = await self.source.query(query)
            if not result.get("success"):
                return f"Query failed: {result.get('message', 'unknown error')}"
            rows = result.get("data", {}).get("rows", [])
            if not rows:
                return "Query returned 0 rows."
            # Show first 10 rows
            display_rows = rows[:10]
            cols = list(display_rows[0].keys())
            lines = [f"Returned {len(rows)} row(s). First {len(display_rows)}:",
                     " | ".join(c.strip("[]") for c in cols)]
            lines.append("-" * len(lines[-1]))
            for r in display_rows:
                lines.append(" | ".join(str(r.get(c, ""))[:25] for c in cols))
            return "\n".join(lines)

        elif name == "save_understanding":
            understanding_data = inp["understanding"]
            source_id = self.source.source_id()
            source_type = self.source.source_type()
            self.storage.save_model_understanding(
                source_id, understanding_data, source_type,
                model_id=self.model_id,
            )
            status = understanding_data.get("status", "draft")
            return f"Model understanding saved (status: {status})."

        elif name == "get_understanding":
            data = None
            if self.model_id:
                data = self.storage.load_model_understanding_by_model(self.model_id)
            if not data:
                source_id = self.source.source_id()
                data = self.storage.load_model_understanding(source_id)
            if not data:
                return "No model understanding saved yet."
            # Remove internal _meta before showing to agent
            display = {k: v for k, v in data.items() if not k.startswith("_")}
            return json.dumps(display, indent=2, ensure_ascii=False)

        return f"Unknown tool: {name}"

    # ── Chat Loop ──────────────────────────────────────────────────────────

    async def chat(self, msg: str) -> str:
        """
        Send a user message and return the agent's reply.

        Handles tool calls automatically, same pattern as the scenario agent.
        """
        from config import DISCOVERY_MODEL

        self.conv.append({"role": "user", "content": msg})

        truncation_retries = 0
        max_retries = 2

        while True:
            try:
                resp = self.ai.messages.create(
                    model=DISCOVERY_MODEL,
                    max_tokens=16384,
                    system=DISCOVERY_PROMPT,
                    tools=DISCOVERY_TOOLS,
                    messages=self.conv,
                )
            except Exception as e:
                print(f"[Discovery] API error: {e}")
                # Remove the last user message so conversation stays valid
                if self.conv and self.conv[-1]["role"] == "user":
                    self.conv.pop()
                raise

            # Handle max_tokens truncation — response may contain incomplete
            # tool_use blocks that would corrupt the conversation history.
            if resp.stop_reason == "max_tokens":
                truncation_retries += 1
                print(f"[Discovery] Response truncated (max_tokens). "
                      f"Retry {truncation_retries}/{max_retries}")

                # Check if there are any tool_use blocks in the truncated response
                has_tool_use = any(
                    getattr(b, "type", None) == "tool_use" for b in resp.content
                )

                if has_tool_use and truncation_retries <= max_retries:
                    # Don't append truncated tool_use — it can't be completed.
                    # Keep only text blocks and ask the model to retry concisely.
                    text_blocks = [b for b in resp.content
                                   if getattr(b, "type", None) == "text"]
                    if text_blocks:
                        self.conv.append({
                            "role": "assistant",
                            "content": text_blocks,
                        })
                    self.conv.append({
                        "role": "user",
                        "content": (
                            "Your previous response was truncated while calling a tool "
                            "because the output was too long. "
                            "Please try again with a much more compact approach:\n"
                            "- Minimize your text explanation (1-2 sentences max)\n"
                            "- In the JSON: no indentation, no optional/empty fields\n"
                            "- Only include fields you have actual values for\n"
                            "- Omit description fields if they are not essential"
                        ),
                    })
                    continue

                # Either no tool_use or retries exhausted — return what we have
                text = "".join(
                    b.text for b in resp.content if hasattr(b, "text")
                )
                if has_tool_use:
                    # Retries exhausted with tool_use still truncating.
                    # Drop the truncated response and tell the user.
                    print("[Discovery] Retries exhausted — asking user to simplify.")
                    self.conv.append({
                        "role": "assistant",
                        "content": [{"type": "text", "text":
                            "I'm having trouble saving the full model understanding "
                            "in one go because the document is too large. "
                            "Let me try saving it in a more compact format."}],
                    })
                    # Inject a system-level retry hint
                    self.conv.append({
                        "role": "user",
                        "content": (
                            "Please call save_understanding again but with a minimal "
                            "JSON: only include model_name, status, tables (names and "
                            "roles only), relationships, scenario_target, and "
                            "account_structure. Skip all other sections. "
                            "Use no whitespace in the JSON."
                        ),
                    })
                    truncation_retries = 0  # Reset for the minimal retry
                    continue

                # Pure text truncation — return partial text
                self.conv.append({"role": "assistant", "content": resp.content})
                return (text or "(Response was empty)") + "\n\n*(Response was truncated)*"

            # Reset truncation counter on successful response
            truncation_retries = 0

            self.conv.append({"role": "assistant", "content": resp.content})

            if resp.stop_reason == "tool_use":
                results = []
                for block in resp.content:
                    if block.type == "tool_use":
                        print(f"[Discovery] {block.name}({json.dumps(block.input)[:100]})")
                        try:
                            result = await self._handle_tool(block.name, block.input)
                        except Exception as e:
                            print(f"[Discovery] Tool error in {block.name}: {e}")
                            result = f"Error executing {block.name}: {e}"
                        results.append({
                            "type":        "tool_result",
                            "tool_use_id": block.id,
                            "content":     result,
                        })
                self.conv.append({"role": "user", "content": results})
                continue

            # end_turn
            text = "".join(b.text for b in resp.content if hasattr(b, "text"))
            return text

    def reset(self):
        """Clear conversation history."""
        self.conv = []
        self._schema_cache = None
        print("[Discovery] Conversation reset.")

    def get_model_understanding(self) -> ModelUnderstanding | None:
        """Load the current model understanding (prefers model_id, then source_id)."""
        data = None
        if self.model_id:
            data = self.storage.load_model_understanding_by_model(self.model_id)
        if not data:
            source_id = self.source.source_id()
            data = self.storage.load_model_understanding(source_id)
        if not data:
            return None
        # Remove internal metadata
        clean = {k: v for k, v in data.items() if not k.startswith("_")}
        return ModelUnderstanding.from_dict(clean)
