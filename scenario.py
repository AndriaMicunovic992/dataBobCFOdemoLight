"""
scenario.py — Scenario calculation and SQL generation.

Provides:
  build_scenario()  — apply % adjustments to budget rows
  make_sql()        — render rows as a SQL INSERT script
  save_sql()        — write the SQL script to the output folder
"""

import re
from datetime import datetime
from pathlib import Path

from config import COMPANY_ID, REVENUE_ACCS, COGS_ACCS, OUTPUT_DIR


def build_scenario(rows: list[dict], adjustments: list[dict]) -> list[dict]:
    """
    Apply a list of adjustments to budget rows.

    Each adjustment dict supports:
        months        — list of month numbers (1-12); omit for full year
        account_group — "revenue" | "cogs" | "all" | "112,114,..." (comma-separated IDs)
        pct_change    — float; percentage adjustment (positive = increase)
        abs_change    — float; absolute CHF amount, distributed evenly across matching rows

    Exactly one of pct_change or abs_change should be provided per adjustment.

    Returns a new list of row dicts with amount and budget_amount scaled.
    All other FK columns (currency_id, cost_object_id, …) are preserved as-is.
    """
    scenario = [dict(r) for r in rows]   # shallow copy — values are primitives

    for adj in adjustments:
        months  = set(adj.get("months", []))
        grp     = adj.get("account_group", "all")

        if grp == "revenue":
            targets = REVENUE_ACCS
        elif grp == "cogs":
            targets = COGS_ACCS
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
             scenario_id: int = 3) -> str:
    """
    Render scenario rows as a complete SQL INSERT script.

    Args:
        scenario_id: The value_type_id written into every row.
                     Use 3 for the first scenario, 4 for the second, etc.

    Includes a header comment, a commented-out DELETE statement for safe
    re-loading, the INSERT VALUES block, and a verification SELECT.
    """
    dates       = sorted({r["date"] for r in scenario})
    sorted_rows = sorted(scenario, key=lambda r: (r["date"], r["account"]))

    lines = [
        "-- ================================================================",
        f"-- SCENARIO : {label}",
        f"-- Generated: {datetime.now():%Y-%m-%d %H:%M:%S}",
        f"-- Company  : Hans Kohler AG (company_id={COMPANY_ID})",
        f"-- Type     : value_type_id={scenario_id} (Scenario)",
        f"-- Rows     : {len(scenario)}",
    ]
    if description:
        lines += [f"-- {line}" for line in description.strip().split("\n")]
    lines += [
        "-- ================================================================",
        "",
        "-- DELETE existing rows for this scenario before re-loading:",
        "-- DELETE FROM [Fakten Hauptbuch]",
        f"-- WHERE company_id={COMPANY_ID} AND value_type_id={scenario_id}",
        "--   AND accounting_date IN ({});".format(", ".join(f"'{d}'" for d in dates)),
        "",
        "INSERT INTO [Fakten Hauptbuch] (",
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
            f"    ({r['account']}, {COMPANY_ID}, '{r['date']}', {scenario_id}, "
            f"{r['amount']}, {r['budget_amount']}, "
            f"{_sql_val(r['currency_id'])}, {_sql_val(r['settlement_type_id'])}, "
            f"{_sql_val(r['cost_object_id'])}, {_sql_val(r['item_group_id'])}, "
            f"{_sql_val(r['cost_center_id'])}, {_sql_val(r['project_id'])}, "
            f"{_sql_val(r['it_category_id'])}, {_sql_val(r['financial_dimension_id'])}){sep}"
        )

    lines += [
        "",
        "-- Verify:",
        "-- SELECT main_account_id, accounting_date, cost_object_id, amount, budget_amount",
        "-- FROM [Fakten Hauptbuch]",
        f"-- WHERE company_id={COMPANY_ID} AND value_type_id={scenario_id}",
        "-- ORDER BY accounting_date, main_account_id;",
    ]
    return "\n".join(lines)


def save_sql(sql: str, label: str, scenario_id: int = 3) -> Path:
    """Write the SQL script to the output directory and return the file path."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    safe  = re.sub(r"[^a-zA-Z0-9_-]", "_", label)
    fname = f"scenario_{scenario_id}_{safe}_{datetime.now():%Y%m%d_%H%M%S}.sql"
    path  = OUTPUT_DIR / fname
    path.write_text(sql, encoding="utf-8")
    return path
