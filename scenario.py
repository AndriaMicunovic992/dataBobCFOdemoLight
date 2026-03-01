"""
scenario.py — Scenario calculation and SQL generation.

Provides:
  build_scenario()  — apply % adjustments to budget rows
  make_sql()        — render rows as a SQL INSERT script
  save_sql()        — write the SQL script to the output folder

All model-specific values (company ID, account sets, table names) are passed
as parameters rather than imported from config, making this module model-agnostic.

Legacy imports from config are preserved for backward compatibility.
"""

import re
from datetime import datetime
from pathlib import Path

# Legacy imports — used only by callers that don't pass model params
from config import COMPANY_ID, REVENUE_ACCS, COGS_ACCS, OUTPUT_DIR


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
        revenue_accs: Set of revenue account IDs. Falls back to config.REVENUE_ACCS.
        cogs_accs:    Set of COGS account IDs. Falls back to config.COGS_ACCS.

    Returns a new list of row dicts with amount and budget_amount scaled.
    All other FK columns (currency_id, cost_object_id, …) are preserved as-is.
    """
    _rev = revenue_accs if revenue_accs is not None else REVENUE_ACCS
    _cogs = cogs_accs if cogs_accs is not None else COGS_ACCS

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


def make_sql(scenario: list[dict], label: str, description: str = "",
             scenario_id: int = 3,
             target_table: str | None = None,
             company_id: int | None = None,
             columns: list[str] | None = None) -> str:
    """
    Render scenario rows as a complete SQL INSERT script.

    Args:
        scenario_id: The value_type_id written into every row.
        target_table: SQL table name (default: "[Fakten Hauptbuch]").
        company_id:   Company ID for the INSERT (default: config.COMPANY_ID).
        columns:      List of columns to include. If None, uses the default set.

    Includes a header comment, a commented-out DELETE statement for safe
    re-loading, the INSERT VALUES block, and a verification SELECT.
    """
    _table = target_table or "[Fakten Hauptbuch]"
    _company = company_id if company_id is not None else COMPANY_ID

    dates       = sorted({r["date"] for r in scenario})
    sorted_rows = sorted(scenario, key=lambda r: (r["date"], r["account"]))

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
        "    main_account_id, company_id, accounting_date, value_type_id,",
        "    amount, budget_amount,",
        "    currency_id, settlement_type_id,",
        "    cost_object_id, item_group_id,",
        "    cost_center_id, project_id, it_category_id, financial_dimension_id",
        ") VALUES",
    ]

    for i, r in enumerate(sorted_rows):
        sep = "," if i < len(sorted_rows) - 1 else ";"
        lines.append(
            f"    ({r['account']}, {_company}, '{r['date']}', {scenario_id}, "
            f"{r['amount']}, {r['budget_amount']}, "
            f"{_sql_val(r.get('currency_id'))}, {_sql_val(r.get('settlement_type_id'))}, "
            f"{_sql_val(r.get('cost_object_id'))}, {_sql_val(r.get('item_group_id'))}, "
            f"{_sql_val(r.get('cost_center_id'))}, {_sql_val(r.get('project_id'))}, "
            f"{_sql_val(r.get('it_category_id'))}, {_sql_val(r.get('financial_dimension_id'))}){sep}"
        )

    lines += [
        "",
        "-- Verify:",
        f"-- SELECT main_account_id, accounting_date, cost_object_id, amount, budget_amount",
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
