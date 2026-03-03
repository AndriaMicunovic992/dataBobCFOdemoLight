"""
PromptBuilder — Generates scenario agent system prompts from ModelUnderstanding.

Combines:
  1. Model-specific sections (tables, accounts, dimensions, structures) — from MU
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
            PromptBuilder._reporting_structures(mu),
            PromptBuilder._dimensions(mu),
            _WORKFLOW_TEMPLATE,
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
            lines.append(f"\nPrimary fact table (General Ledger): {ft}")
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

        return "\n".join(lines)

    @staticmethod
    def _reporting_structures(mu: ModelUnderstanding) -> str:
        """Describe P&L/BS/CF reporting structures for the agent."""
        structures = mu.reporting_structures
        if not structures:
            # Fall back to legacy account_groups
            groups = mu.account_groups
            if not groups:
                return ""
            lines = ["== ACCOUNT GROUPS ==",
                     "Available groups for account_group filter in adjustments:"]
            for gname, ginfo in groups.items():
                ids = ginfo.get("account_ids", [])
                desc = ginfo.get("description", "")
                id_str = ",".join(str(i) for i in ids[:5])
                if len(ids) > 5:
                    id_str += f"... ({len(ids)} total)"
                lines.append(
                    f'  account_group="{gname}" → {desc} (IDs: {id_str})'
                )
            return "\n".join(lines)

        lines = ["== REPORTING STRUCTURES ==",
                 "These define how GL accounts are organized into financial statements.",
                 "Use section names as account_group values in adjustments.",
                 ""]

        for key, struct in structures.items():
            name = struct.get("name", key.upper())
            lines.append(f"--- {name} ---")
            for sec in struct.get("sections", []):
                sec_name = sec["name"]
                if sec.get("type") == "subtotal":
                    refs = " + ".join(sec.get("sum_of", []))
                    lines.append(f"  {sec_name} = {refs} (subtotal)")
                else:
                    ids = sec.get("account_ids", [])
                    sign = sec.get("sign", 1)
                    sign_label = "+" if sign >= 0 else "-"
                    id_str = ",".join(str(i) for i in ids[:5])
                    if len(ids) > 5:
                        id_str += f"... ({len(ids)} total)"
                    lines.append(
                        f'  account_group="{sec_name}" [{sign_label}] → IDs: {id_str}'
                    )
            lines.append("")

        lines.append("You can also use comma-separated account IDs directly: "
                     'account_ids="112,114"')

        return "\n".join(lines)

    @staticmethod
    def _dimensions(mu: ModelUnderstanding) -> str:
        """Describe GL dimensions available for filter-based adjustments."""
        gl_dims = mu.gl_dimensions
        if not gl_dims:
            return ""

        lines = ["== GL DIMENSIONS ==",
                 "The GL fact table has these dimension columns. Users can filter",
                 "adjustments by any of these (e.g. 'increase revenue for Company A').",
                 "Use the column name as a filter key in adjustments.",
                 ""]

        for dim in gl_dims:
            col = dim.get("column", "")
            label = dim.get("label", col)
            dim_table = dim.get("dimension_table")
            label_col = dim.get("label_column")
            if dim_table and label_col:
                lines.append(f"  {col} ({label}) → {dim_table}.{label_col}")
            elif dim_table:
                lines.append(f"  {col} ({label}) → {dim_table}")
            else:
                lines.append(f"  {col} ({label})")

        return "\n".join(lines)

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

        return tools
