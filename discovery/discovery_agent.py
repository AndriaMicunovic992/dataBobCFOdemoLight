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
7. Query templates for fetching budget/baseline data

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
4. After the user confirms key aspects, call `save_understanding` with the full JSON.
5. The user can continue refining — call `save_understanding` again to update.

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
  "description": "Brief description of the model",
  "status": "draft",
  "tables": {
    "TableName": {
      "role": "fact|dimension|bridge|lookup",
      "description": "What this table contains",
      "key_columns": ["id_col"],
      "important_columns": {
        "col_name": {"purpose": "description", "data_type": "string|int|float|date"}
      }
    }
  },
  "relationships": [
    {"from_table": "A", "from_column": "x", "to_table": "B", "to_column": "y"}
  ],
  "account_structure": {
    "account_table": "Dim Accounts",
    "account_id_column": "account_id",
    "account_name_column": "account_name",
    "grouping_columns": ["Reporting Group"],
    "groups": {
      "revenue": {"description": "Revenue accounts", "account_ids": [1, 2, 3]},
      "cogs": {"description": "Cost of goods sold", "account_ids": [4, 5]}
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
    "fetch_budget": "EVALUATE SELECTCOLUMNS(FILTER(...), ...)",
    "fetch_account_map": "EVALUATE SELECTCOLUMNS(...)"
  },
  "sql_target": {
    "table_name": "[Fakten Hauptbuch]",
    "columns": ["main_account_id", "company_id", "accounting_date", ...]
  },
  "cashflow_config": {
    "structure_table": "Dim Cashflow Struktur",
    "position_column": "Position Geldflussrechnung"
  },
  "customer_config": {
    "customer_table": "Dim Kunde",
    "customer_id_column": "customer_id",
    "invoice_table": "Fakten Rechnungszeile"
  }
}

== RULES ==
- Always start with extract_schema before asking questions.
- Be concise — don't overwhelm the user with questions. Ask 2-3 at a time.
- When you have enough information, save the understanding (even if partial — status="draft").
- Mark status="confirmed" only when the user explicitly confirms.
- If the user corrects something, update and re-save.
- For combined PBI+Excel sources, note which tables come from which source.
- Focus on what the scenario agent needs: fact table, accounts, amounts, dates, groups.
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

    def __init__(self, source: DataSource, storage: SQLiteStorage):
        self.source = source
        self.storage = storage
        self.ai = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
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
                source_id, understanding_data, source_type
            )
            status = understanding_data.get("status", "draft")
            return f"Model understanding saved (status: {status})."

        elif name == "get_understanding":
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
        from config import CLAUDE_MODEL

        self.conv.append({"role": "user", "content": msg})

        while True:
            resp = self.ai.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=2048,
                system=DISCOVERY_PROMPT,
                tools=DISCOVERY_TOOLS,
                messages=self.conv,
            )
            self.conv.append({"role": "assistant", "content": resp.content})

            if resp.stop_reason == "tool_use":
                results = []
                for block in resp.content:
                    if block.type == "tool_use":
                        print(f"[Discovery] {block.name}({json.dumps(block.input)[:100]})")
                        result = await self._handle_tool(block.name, block.input)
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
        """Load the current model understanding for the connected source."""
        source_id = self.source.source_id()
        data = self.storage.load_model_understanding(source_id)
        if not data:
            return None
        # Remove internal metadata
        clean = {k: v for k, v in data.items() if not k.startswith("_")}
        return ModelUnderstanding.from_dict(clean)
