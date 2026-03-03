"""
PromptBuilder — Generates scenario agent system prompts from ModelUnderstanding.

Combines:
  1. Model-specific sections (tables, accounts, relationships) — from ModelUnderstanding
  2. Workflow sections (staging, apply format, rules) — from static template
"""

from pathlib import Path
from discovery.model_understanding import ModelUnderstanding


# Load the static workflow template once
_TEMPLATE_DIR = Path(__file__).parent / "templates"
_WORKFLOW_TEMPLATE = (_TEMPLATE_DIR / "scenario_workflow.txt").read_text(encoding="utf-8")


class PromptBuilder:
    """Generates system prompts from ModelUnderstanding."""

    @staticmethod
    def build(mu: ModelUnderstanding) -> str:
        """Generate a complete system prompt from the model understanding."""
        sections = [
            PromptBuilder._intro(mu),
            PromptBuilder._data_model(mu),
            PromptBuilder._accounts(mu),
            PromptBuilder._measures_section(mu),
            PromptBuilder._customer_section(mu),
            _WORKFLOW_TEMPLATE,
            PromptBuilder._cashflow_section(mu),
        ]
        return "\n\n".join(s for s in sections if s)

    @staticmethod
    def _intro(mu: ModelUnderstanding) -> str:
        return (
            f"You are a financial scenario specialist for the "
            f"{mu.model_name} model."
        )

    @staticmethod
    def _data_model(mu: ModelUnderstanding) -> str:
        lines = ["== DATA MODEL =="]

        if mu.description:
            lines.append(mu.description)

        # Fact table
        ft = mu.fact_table
        if ft:
            lines.append(f"\nPrimary fact table: {ft}")
            tinfo = mu.get_table(ft)
            if tinfo:
                desc = tinfo.get("description", "")
                if desc:
                    lines.append(f"  {desc}")

        # Describe scenario target
        st = mu.scenario_target
        if st:
            lines.append(f"\nrun_query loads baseline data from {ft}:")
            stv = mu.scenario_type_values
            if stv:
                # Dynamically describe available value types
                base_key = next(
                    (k for k in ("budget", "forecast", "scenario_base")
                     if k in stv), next(iter(stv), None)
                )
                base_val = stv.get(base_key) if base_key else None
                actuals_val = stv.get("actuals")
                if base_val is not None:
                    base_label = base_key.replace("_", " ").title()
                    lines.append(
                        f"  1. {base_label} rows ({mu.scenario_type_column}="
                        f"{base_val}) for the requested year"
                    )
                if actuals_val is not None:
                    lines.append(
                        f"  2. BS/CF actuals ({mu.scenario_type_column}="
                        f"{actuals_val}) from the PRIOR year, aggregated by "
                        f"account × month (for accounts without baseline rows)"
                    )
                # List all available value types so the agent knows what's possible
                vt_desc = ", ".join(f"{k}={v}" for k, v in stv.items())
                lines.append(f"  Available value types: {vt_desc}")

            amt_cols = mu.amount_columns
            if amt_cols:
                lines.append(f"\nAmount columns: {', '.join(amt_cols)}")

        # Dimension tables
        dims = mu.get_tables_by_role("dimension")
        if dims:
            lines.append("\nDimension tables:")
            for d in dims:
                dinfo = mu.get_table(d) or {}
                desc = dinfo.get("description", "")
                lines.append(f"  - {d}" + (f": {desc}" if desc else ""))

        # Company filter
        if mu.company_id is not None:
            lines.append(
                f"\nDefault filter: {mu.company_column} = {mu.company_id}"
            )

        # List available custom query templates
        _reserved = {"fetch_baseline", "fetch_budget", "fetch_account_map"}
        custom_templates = [k for k in mu.query_templates
                           if k not in _reserved]
        if custom_templates:
            lines.append(
                "\nCustom query templates (use run_custom_query tool):"
            )
            for tname in custom_templates:
                lines.append(f"  - {tname}")

        return "\n".join(lines)

    @staticmethod
    def _accounts(mu: ModelUnderstanding) -> str:
        lines = ["== GL ACCOUNTS ==",
                 "Always call run_query before staging adjustments — the result "
                 "includes a full account breakdown with names and groups pulled "
                 "live from the data source."]

        structures = mu.account_structures
        for purpose, struct in structures.items():
            label = purpose.upper()
            table = struct.get("account_table", "")
            if table:
                lines.append(f"\n{label} account structure (table: {table}):")
            groups = struct.get("groups", {})
            if groups:
                lines.append(f"  Account groups for {label} adjustments:")
                for gname, ginfo in groups.items():
                    ids = ginfo.get("account_ids", [])
                    desc = ginfo.get("description", "")
                    id_str = ",".join(str(i) for i in ids[:5])
                    if len(ids) > 5:
                        id_str += f"... ({len(ids)} total)"
                    lines.append(
                        f'    account_group="{gname}" → {desc} (IDs: {id_str})'
                    )

        if structures:
            lines.append('\nSpecific subset: account_group="112,114" '
                        '(comma-separated IDs).')

        return "\n".join(lines)

    @staticmethod
    def _measures_section(mu: ModelUnderstanding) -> str:
        measures = mu.measures
        if not measures:
            return ""
        lines = [
            "== DAX MEASURES ==",
            "Available DAX measures (contain business logic, can be used in queries):",
        ]
        for name, info in measures.items():
            expr = (info.get("expression") or "")[:100]
            table = info.get("table", "")
            lines.append(f"  [{table}][{name}] = {expr}")
        return "\n".join(lines)

    @staticmethod
    def _customer_section(mu: ModelUnderstanding) -> str:
        if not mu.has_customer_dimension:
            return ""

        cc = mu.customer_config
        cust_table = cc.get("customer_table", "")
        inv_table = cc.get("invoice_table", "")

        return f"""== CUSTOMER SCENARIOS ==
Since budget rows may not have customer-level detail, customer scenarios
are modelled via GL accounts:
1. Call query_customers → get customer's actual revenue share % from prior year actuals
2. Calculate net GL impact: customer_share% × requested_change% = GL_adjustment%
3. Call run_query to load GL budget baseline
4. Apply calculated GL_adjustment% to revenue accounts

Customer data comes from {cust_table} (names) and {inv_table} (actuals).
Always show the user the maths before staging."""

    @staticmethod
    def _cashflow_section(mu: ModelUnderstanding) -> str:
        if not mu.has_cashflow:
            return ""

        return """== CASHFLOW & BALANCE SHEET ==
The loaded data includes BOTH P&L budget accounts AND BS/CF accounts (based on prior-
year actuals). You can adjust any account visible in the run_query results.

The UI automatically computes a cashflow statement impact from ALL staged adjustments:
- When the user clicks "Preview Impact", the preview shows TWO tabs:
  1. P&L tab — account-level delta table and waterfall chart
  2. Cashflow tab — derived cashflow statement (Operating / Investing / Financing / Net)
- Tell the user to check the Cashflow tab in Preview Impact to see the cash effect."""

    @staticmethod
    def build_tools(mu: ModelUnderstanding) -> list[dict]:
        """Build the tool definitions dynamically from ModelUnderstanding."""
        tools = [
            {
                "name": "run_query",
                "description": (
                    f"Fetch baseline data from {mu.fact_table or 'the fact table'}. "
                    f"Returns a summary; data stored internally. "
                    f"The year and value type default to the user's Load selector — "
                    f"only pass year to override."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "year":   {"type": "integer",
                                   "description": "Fiscal year. Omit to use the user's selected baseline year."},
                        "months": {"type": "array", "items": {"type": "integer"},
                                   "description": "Specific months 1-12. Omit for full year."},
                    },
                },
            },
        ]

        # Register custom query templates as a tool if any exist
        _reserved = {"fetch_baseline", "fetch_budget", "fetch_account_map"}
        custom_templates = [k for k in mu.query_templates
                           if k not in _reserved]
        if custom_templates:
            tools.append({
                "name": "run_custom_query",
                "description": (
                    "Execute a custom query template from the model understanding. "
                    "Use this for analytical queries beyond the baseline data. "
                    "Results are returned for analysis but do NOT replace the "
                    "loaded baseline data."
                ),
                "input_schema": {
                    "type": "object",
                    "required": ["template_name"],
                    "properties": {
                        "template_name": {
                            "type": "string",
                            "enum": custom_templates,
                            "description": "Name of the custom query template",
                        },
                        "year": {
                            "type": "integer",
                            "description": "Year filter (if template uses {year})",
                        },
                        "months": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": "Specific months 1-12",
                        },
                    },
                },
            })

        if mu.has_customer_dimension:
            cc = mu.customer_config
            inv_table = cc.get("invoice_table", "invoice lines")
            tools.append({
                "name": "query_customers",
                "description": (
                    f"Look up customers and their actual revenue share from {inv_table}. "
                    "Use when the user mentions a customer name or 'top N customers'. "
                    "Returns customer IDs, names, revenue totals, and % share."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "year":        {"type": "integer", "description": "Year for actuals lookup"},
                        "top_n":       {"type": "integer", "description": "Top N customers (default 10)"},
                        "search_name": {"type": "string",  "description": "Search by customer name"},
                    },
                },
            })

        return tools
