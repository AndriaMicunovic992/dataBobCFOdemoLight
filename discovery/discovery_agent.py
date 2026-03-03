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

DISCOVERY_PROMPT = """You are a data model analyst. Your job is to explore a data source
and build a complete "Model Understanding" document through conversation with the user.
This document will be used by a scenario planning agent to run financial what-if analyses.

== CORE PRINCIPLE ==
NEVER assume, hardcode, or guess any data-specific values. Every value (table names,
column names, account IDs, value type IDs, years, company IDs, grouping names) must
come from either: (a) querying the actual data, or (b) the user telling you.

== CHECKLIST ==
You must work through ALL of these items. Track progress and tell the user which items
are complete vs pending. The understanding is NOT ready until all items are covered.

1. [ ] GL FACT TABLE — Identify the main General Ledger / financial fact table.
       Which table? Which columns are amounts, dates, account IDs?
2. [ ] ACCOUNT DIMENSION — Find the account dimension table. Which columns contain
       grouping info (e.g. AccountGroup, ReportingGroup, StatementType)?
3. [ ] ACCOUNT GROUPS — Map group names to account IDs. Read the account table's
       grouping columns to auto-derive groups. Propose them to the user for confirmation.
4. [ ] VALUE TYPES — Identify what value types exist. Which column holds this?
       Query the data to discover the actual IDs and their meanings.
       ASK the user to confirm the mapping (e.g. "ID 1 = Actuals, ID 2 = Budget?").
5. [ ] GL DIMENSIONS — Map ALL foreign key columns on the GL fact table to their
       dimension tables. For each: column name, dimension table, label column.
       e.g. CompanyID → DimCompany.CompanyName, CostCenterID → DimCostCenter.Name
6. [ ] REPORTING STRUCTURES — Build P&L structure (required). Propose BS and CF
       structures if data supports it. Each structure has sections with account IDs
       and subtotal lines. Derive initial structure from account grouping columns,
       then let the user refine ("split OpEx into Personnel and Other", etc.).
7. [ ] fetch_baseline TEMPLATE — Build and TEST a working query template that fetches
       GL rows with runtime placeholders. Must return correct column aliases.
8. [ ] fetch_account_map TEMPLATE — Build and TEST a working query template that
       fetches account metadata (names, groups) for given account IDs.

== WORKFLOW ==
1. Call `extract_schema` to get the raw schema with sample data.
2. Analyze: identify the GL fact table, dimension tables, relationships.
3. Ask the user targeted questions (2-3 at a time) to fill gaps.
4. For the ACCOUNT DIMENSION: inspect it with `get_sample_data` or `run_test_query`
   to see what grouping columns exist and what values they contain. Use this to
   auto-propose account groups and reporting structures.
5. Build query templates and ALWAYS test with `run_test_query` before saving.
6. Call `save_understanding` with the full JSON once you have enough info.
7. The user can continue refining — call `save_understanding` again to update.

== FOR POWER BI MODELS ==
PBI models have rich metadata: relationships are auto-discovered, column types are known.
You can auto-infer most of the structure. Focus on business semantics.

== FOR EXCEL FILES ==
Excel has no relationships or type metadata. You must ask more questions about
which sheet is what, how sheets relate, and what columns mean.

== MODEL UNDERSTANDING JSON FORMAT ==
When calling save_understanding, provide JSON with this structure.
IMPORTANT: All values shown below as <DESCRIPTION> are placeholders — fill them
with ACTUAL values discovered from the data or confirmed by the user. Never copy
the placeholder text.

{
  "model_name": "<human-readable name from user>",
  "domain": "finance",
  "description": "<brief description>",
  "status": "draft",
  "tables": {
    "<actual table name>": {
      "role": "fact|dimension",
      "description": "<short desc>",
      "key_columns": ["<actual key column>"],
      "important_columns": {
        "<actual col>": {"purpose": "<desc>", "data_type": "string|int|float|date"}
      }
    }
  },
  "relationships": [
    {"from_table": "<fact>", "from_column": "<fk>", "to_table": "<dim>", "to_column": "<pk>"}
  ],
  "account_structure": {
    "account_table": "<actual account table name>",
    "account_id_column": "<actual id column>",
    "account_name_column": "<actual name column>",
    "grouping_columns": ["<actual grouping columns found in the data>"],
    "groups": {
      "<group name from data>": {"description": "<desc>", "account_ids": ["<actual IDs from data>"]}
    }
  },
  "gl_dimensions": [
    {"column": "<actual FK column>", "dimension_table": "<actual dim table>", "label": "<human label>", "label_column": "<actual label column>"}
  ],
  "reporting_structures": {
    "pl": {
      "name": "Profit & Loss",
      "sections": [
        {"name": "<section name>", "account_ids": ["<actual IDs>"], "sign": 1},
        {"name": "<subtotal name>", "type": "subtotal", "sum_of": ["<section refs>"]}
      ]
    }
  },
  "scenario_target": {
    "fact_table": "<actual fact table>",
    "date_column": "<actual date column>",
    "amount_columns": ["<actual amount columns>"],
    "scenario_type_column": "<actual value type column, or null if none>",
    "scenario_type_values": {"<label from user>": "<actual ID from data>"}
  },
  "query_language": "DAX|SQL",
  "query_templates": {
    "fetch_baseline": "<REQUIRED - see QUERY TEMPLATES section>",
    "fetch_account_map": "<REQUIRED - see QUERY TEMPLATES section>"
  },
  "sql_target": {
    "table_name": "<actual fact table name for SQL output>",
    "columns": ["<actual columns>"]
  }
}

== REPORTING STRUCTURES (CRITICAL) ==
The reporting_structures section defines how accounts are organized into financial
statements. This is CRITICAL for the scenario agent — it uses section names as
targets for adjustments (e.g. "increase Revenue by 10%").

How to build reporting structures:
1. Inspect the account dimension table for grouping columns (AccountGroup, ReportingGroup,
   StatementType, PLLine, etc.). Use get_sample_data or run_test_query.
2. Query distinct values of grouping columns to understand what groups exist.
3. For each group, get the list of account IDs that belong to it.
4. Propose a P&L structure to the user with sections and subtotals.
5. If the data supports it, also propose BS and CF structures.
6. Let the user refine: they may want different section names, splits, or subtotals.

Section format:
- Data sections: {"name": "Revenue", "account_ids": [4010, 4020, 4030], "sign": 1}
  - sign: 1 = positive in statement (revenue, assets), -1 = negative (costs, liabilities)
- Subtotal sections: {"name": "Gross Profit", "type": "subtotal", "sum_of": ["Revenue", "COGS"]}
  - sum_of references other section names; amounts are summed respecting each section's sign

== GL DIMENSIONS (CRITICAL) ==
The gl_dimensions section maps every FK column on the GL fact table to its dimension.
This tells the scenario agent what columns users can use to filter adjustments.

For each FK column on the GL fact table:
- column: the column name on the fact table (e.g. "CompanyID")
- dimension_table: the dimension table it points to (null if inline/no dimension)
- label: human-readable name (e.g. "Company")
- label_column: the name/label column in the dimension table (null if none)

The scenario agent uses these to let users say "increase revenue for Company A"
and know that CompanyID is the filter column.

== QUERY TEMPLATES (CRITICAL) ==
Templates define the STRUCTURE of data retrieval — NOT specific parameter values.
All parameters are filled at RUNTIME. Templates use Python format placeholders.

IMPORTANT: The baseline query must NOT filter by dimension columns (company, cost center,
department, etc.). It fetches ALL rows for a given year and value type. Dimension columns
are returned as OUTPUT columns so the scenario agent can use them for filter-based
adjustments at runtime. The user decides what to filter on when building scenarios.

--- fetch_baseline ---
Runtime placeholders (filled automatically):
  {year}          — fiscal year, selected by user at runtime
  {month_filter}  — auto-built; empty string for full year, or filter clause for months
  {value_type_id} — value type selected by user at runtime.
                    CRITICAL: The WHERE clause MUST include a filter on {value_type_id}.
                    Without it, the query returns data for ALL value types (actuals AND
                    budget AND forecast), producing duplicate/inflated results.

DO NOT include {company_id} or any other dimension filter in the WHERE clause.
The baseline fetches ALL data for the year/value_type combination.

The WHERE clause MUST filter on BOTH {year} AND {value_type_id}. Omitting either
causes the query to return data for multiple years or multiple value types.

Required output column aliases (EXACT names):
  "main_account_id"  — account/GL ID (integer)
  "accounting_date"  — date (YYYY-MM-DD)
  "amount"           — primary amount (number)
  "budget_amount"    — secondary amount (number, can be same as amount)
  Plus ALL additional FK columns the fact table has (company_id, currency_id,
  cost_object_id, cost_center_id, etc.) — these are OUTPUT columns, not filters.
  The scenario agent needs them for filter-based adjustments.

DAX example:
  EVALUATE SELECTCOLUMNS(
    FILTER('FactGL',
      YEAR('FactGL'[Date]) = {year}
      && 'FactGL'[ValueTypeID] = {value_type_id}
      {month_filter}
    ),
    "main_account_id", 'FactGL'[AccountID],
    "accounting_date", 'FactGL'[Date],
    "amount", 'FactGL'[Amount],
    "budget_amount", 'FactGL'[Amount],
    "company_id", 'FactGL'[CompanyID],
    "cost_center_id", 'FactGL'[CostCenterID]
  )

SQL example:
  SELECT account_id AS main_account_id, posting_date AS accounting_date,
         amount, amount AS budget_amount, company_id, cost_center_id
  FROM fact_gl
  WHERE YEAR(posting_date) = {year}
    AND value_type_id = {value_type_id} {month_filter}

--- fetch_account_map ---
Required placeholders:
  {account_ids}  — comma-separated integers

Required output column aliases (EXACT names):
  "id"    — account ID (integer)
  "nr"    — account number/code (string)
  "name"  — account name (string)
  "group" — reporting group (string, e.g. "Revenue", "COGS")

DAX example:
  EVALUATE SELECTCOLUMNS(
    FILTER('DimAccounts', 'DimAccounts'[ID] IN {{{account_ids}}}),
    "id", 'DimAccounts'[ID],
    "nr", 'DimAccounts'[Number],
    "name", 'DimAccounts'[Name],
    "group", 'DimAccounts'[Group]
  )

SQL example:
  SELECT id, number AS nr, name, reporting_group AS "group"
  FROM dim_accounts WHERE id IN ({account_ids})

== VALIDATION ==
Before saving, you MUST test query templates. But NEVER pick test values yourself.
1. ASK the user: "Which year and value type should I use to test the queries?"
   Wait for the user's answer before running any test query.
2. Build fetch_baseline template and test with run_test_query using the user's values.
3. Verify result has: main_account_id, accounting_date, amount, budget_amount
4. Verify the result contains data for ONLY the requested year and value type.
   If you see multiple years or value types, the template's WHERE clause is wrong.
5. Build fetch_account_map template and test it (use account IDs from step 2)
6. Verify result has: id, nr, name, group
7. Only save once BOTH templates return valid data

== RULES ==
- Always start with extract_schema before asking questions.
- Be concise — ask 2-3 questions at a time.
- After each save, show the checklist with current completion status.
- Save with status="draft". NEVER set status="confirmed" — user confirms via UI.
- When all 8 checklist items are done: "The model understanding is complete. You can
  confirm it using the Confirm button."
- Never save without working query_templates.
- NEVER assume or hardcode ANY values. All table names, column names, IDs, years,
  company IDs, value type mappings, and group names must come from the data or the user.
- Ensure fetch_baseline WHERE clause filters on BOTH {year} AND {value_type_id}.
- NEVER put {company_id} or any dimension filter in fetch_baseline WHERE clause.
- ONLY build the two templates in the checklist: fetch_baseline and fetch_account_map.
  Do NOT create additional templates (no fetch_pl_structure, no revenue_per_customer,
  no monthly_revenue_trend, etc.) unless the user explicitly asks for them.
- The query_templates dict in the saved JSON must contain EXACTLY two keys:
  "fetch_baseline" and "fetch_account_map". Nothing else.
- Keep JSON compact: no indentation, skip empty sections, short descriptions.
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
