"""
scenario.py — Scenario calculation and SQL generation.

Provides:
  build_scenario()  — apply % adjustments to baseline rows
  make_sql()        — render rows as a SQL INSERT script
  save_sql()        — write the SQL script to the output folder

All model-specific values (company ID, account sets, table names, columns) are
passed as parameters from the ModelUnderstanding document. No hardcoded model-
specific constants — column lists are derived dynamically from data rows and/or
the sql_target.columns in ModelUnderstanding.
"""

import re
from datetime import datetime
from pathlib import Path

from config import OUTPUT_DIR


def build_scenario(rows: list[dict], adjustments: list[dict],
                   revenue_accs: set[int] | None = None,
                   cogs_accs: set[int] | None = None) -> list[dict]:
    """
    Apply a list of adjustments to budget rows.

    Each adjustment dict supports:
        months        — list of month numbers (1-12); omit for full year
        account_group — "revenue" | "cogs" | "all" | "112,114,..." (comma-separated IDs)
        pct_change    — float; percentage adjustment (positive = increase)
        abs_change    — float; absolute amount, distributed evenly across matching rows

    Exactly one of pct_change or abs_change should be provided per adjustment.

    Args:
        rows:         Budget baseline rows.
        adjustments:  List of adjustment dicts.
        revenue_accs: Set of revenue account IDs from ModelUnderstanding.
        cogs_accs:    Set of COGS account IDs from ModelUnderstanding.

    Returns a new list of row dicts with amount and budget_amount scaled.
    All other FK columns (currency_id, cost_object_id, …) are preserved as-is.
    """
    _rev = revenue_accs or set()
    _cogs = cogs_accs or set()

    scenario = [dict(r) for r in rows]   # shallow copy — values are primitives

    for adj in adjustments:
        months  = set(adj.get("months", []))
        grp     = adj.get("account_group", "all")

        if grp == "revenue":
            targets = _rev
        elif grp == "cogs":
            targets = _cogs
        elif grp == "all":
            targets = None
        else:
            targets = {int(x.strip()) for x in str(grp).split(",") if x.strip().isdigit()}

        # Identify matching rows
        matching = [
            r for r in scenario
            if (not months or int(r["date"][5:7]) in months)
            and (targets is None or r["account"] in targets)
        ]

        if "abs_change" in adj:
            # Distribute absolute amount evenly across matching rows
            n = len(matching)
            if n > 0:
                per_row = adj["abs_change"] / n
                for r in matching:
                    r["amount"]        = round(r["amount"]        + per_row, 2)
                    r["budget_amount"] = round(r["budget_amount"] + per_row, 2)
        else:
            # Percentage change
            pct = adj.get("pct_change", 0) / 100.0
            for r in matching:
                r["amount"]        = round(r["amount"]        * (1 + pct), 2)
                r["budget_amount"] = round(r["budget_amount"] * (1 + pct), 2)

    return scenario


def _sql_val(v) -> str:
    """Format a Python value as a SQL literal (NULL, string, or number)."""
    if v is None:          return "NULL"
    if isinstance(v, str): return f"'{v}'"
    return str(v)


def _derive_sql_columns(scenario: list[dict], company_id, scenario_id) -> list[str]:
    """
    Derive the INSERT column list dynamically from the actual row keys.

    Always includes the core columns (account, company, date, value_type,
    amount, budget_amount) in a stable order, then appends any additional
    FK columns found in the data rows (currency_id, cost_center_id, etc.).
    """
    # Core columns in fixed order — these are always present
    core = ["main_account_id", "company_id", "accounting_date", "value_type_id",
            "amount", "budget_amount"]

    # Collect all additional FK-like columns present across rows
    # Skip internal/display-only fields that aren't real DB columns
    skip = {"account", "date", "account_nr", "account_name", "account_grp",
            "cf_position", "cost_object_name", "main_account_id",
            "accounting_date", "budget_amount"}
    extra = set()
    for r in scenario:
        for k in r:
            if k not in skip and k not in ("amount",):
                extra.add(k)

    # Sort extras for deterministic output
    return core + sorted(extra)


def make_sql(scenario: list[dict], label: str, description: str = "",
             scenario_id: int = 3,
             target_table: str | None = None,
             company_id: int | None = None,
             columns: list[str] | None = None) -> str:
    """
    Render scenario rows as a complete SQL INSERT script.

    Args:
        scenario_id: The value_type_id written into every row.
        target_table: SQL table name. Required — no hardcoded default.
        company_id:   Company ID for the INSERT from ModelUnderstanding.
        columns:      Explicit list of SQL columns to include. If None, columns
                      are derived dynamically from the data row keys.

    Includes a header comment, a commented-out DELETE statement for safe
    re-loading, the INSERT VALUES block, and a verification SELECT.
    """
    if not target_table:
        raise ValueError(
            "target_table is required for SQL generation. "
            "Ensure sql_target.table_name is set in ModelUnderstanding."
        )
    _table = target_table
    _company = company_id if company_id is not None else 0

    # Determine column list: explicit > derived from rows
    if columns:
        sql_cols = list(columns)
    else:
        sql_cols = _derive_sql_columns(scenario, _company, scenario_id)

    dates       = sorted({r["date"] for r in scenario})
    sorted_rows = sorted(scenario, key=lambda r: (r["date"], r["account"]))

    col_str = ", ".join(sql_cols)

    lines = [
        "-- ================================================================",
        f"-- SCENARIO : {label}",
        f"-- Generated: {datetime.now():%Y-%m-%d %H:%M:%S}",
        f"-- Company  : company_id={_company}",
        f"-- Type     : value_type_id={scenario_id} (Scenario)",
        f"-- Rows     : {len(scenario)}",
    ]
    if description:
        lines += [f"-- {line}" for line in description.strip().split("\n")]
    lines += [
        "-- ================================================================",
        "",
        "-- DELETE existing rows for this scenario before re-loading:",
        f"-- DELETE FROM {_table}",
        f"-- WHERE company_id={_company} AND value_type_id={scenario_id}",
        "--   AND accounting_date IN ({});".format(", ".join(f"'{d}'" for d in dates)),
        "",
        f"INSERT INTO {_table} (",
        f"    {col_str}",
        ") VALUES",
    ]

    # Map column names to row values
    for i, r in enumerate(sorted_rows):
        sep = "," if i < len(sorted_rows) - 1 else ";"
        vals = []
        for col in sql_cols:
            if col == "main_account_id":
                vals.append(str(r["account"]))
            elif col == "company_id":
                vals.append(str(_company))
            elif col == "accounting_date":
                vals.append(f"'{r['date']}'")
            elif col == "value_type_id":
                vals.append(str(scenario_id))
            elif col == "amount":
                vals.append(str(r["amount"]))
            elif col == "budget_amount":
                vals.append(str(r["budget_amount"]))
            else:
                vals.append(_sql_val(r.get(col)))
        lines.append(f"    ({', '.join(vals)}){sep}")

    lines += [
        "",
        "-- Verify:",
        f"-- SELECT {', '.join(sql_cols[:5])}",
        f"-- FROM {_table}",
        f"-- WHERE company_id={_company} AND value_type_id={scenario_id}",
        "-- ORDER BY accounting_date, main_account_id;",
    ]
    return "\n".join(lines)


def save_sql(sql: str, label: str, scenario_id: int = 3,
             output_dir: Path | None = None) -> Path:
    """Write the SQL script to the output directory and return the file path."""
    _dir = output_dir or OUTPUT_DIR
    _dir.mkdir(parents=True, exist_ok=True)
    safe  = re.sub(r"[^a-zA-Z0-9_-]", "_", label)
    fname = f"scenario_{scenario_id}_{safe}_{datetime.now():%Y%m%d_%H%M%S}.sql"
    path  = _dir / fname
    path.write_text(sql, encoding="utf-8")
    return path
