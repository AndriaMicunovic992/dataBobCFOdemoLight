"""
scenario.py — Scenario calculation and SQL generation.

Provides:
  build_scenario()  — apply filter-based adjustments to baseline rows
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
from discovery.model_understanding import ModelUnderstanding


def build_scenario(rows: list[dict], adjustments: list[dict],
                   mu: ModelUnderstanding,
                   target_year: int | None = None) -> list[dict]:
    """
    Apply filter-based adjustments to baseline rows.

    Each adjustment dict supports:
        filters       — dict of column/value conditions (AND logic):
                        - "account_group": section name from reporting_structures
                          (resolved to account IDs)
                        - "account_ids": list or comma-separated string of account IDs
                        - any other key: matched directly against row column
                        Empty or missing filters → matches all rows.
        months        — list of month numbers (1-12); omit for full year
        pct_change    — float; percentage adjustment (positive = increase)
        abs_change    — float; absolute amount, distributed evenly across matching rows

    Exactly one of pct_change or abs_change should be provided per adjustment.

    Args:
        rows:         Baseline rows (from any year/value type).
        adjustments:  List of adjustment dicts.
        mu:           ModelUnderstanding for resolving account groups/sections.
        target_year:  If set and different from the baseline year, shift all
                      dates to this year (e.g. load 2025 actuals → apply as 2026).

    Returns a new list of row dicts with amount and budget_amount scaled.
    All other FK columns (currency_id, cost_object_id, ...) are preserved as-is.
    """
    scenario = [dict(r) for r in rows]   # shallow copy — values are primitives

    for adj in adjustments:
        months = set(adj.get("months", []))
        filters = adj.get("filters", {})

        # Pre-resolve account_group and account_ids filters into a target set
        account_targets = _resolve_account_filter(filters, mu)

        # Build non-account filters (everything except account_group/account_ids)
        row_filters = {k: v for k, v in filters.items()
                       if k not in ("account_group", "account_ids")}

        # Identify matching rows
        matching = []
        for r in scenario:
            # Month filter
            if months and int(r["date"][5:7]) not in months:
                continue
            # Account filter
            if account_targets is not None and r["account"] not in account_targets:
                continue
            # Dimension filters (any GL dimension column)
            if not _row_matches_filters(r, row_filters):
                continue
            matching.append(r)

        if "abs_change" in adj:
            n = len(matching)
            if n > 0:
                per_row = adj["abs_change"] / n
                for r in matching:
                    r["amount"]        = round(r["amount"]        + per_row, 2)
                    r["budget_amount"] = round(r["budget_amount"] + per_row, 2)
        else:
            pct = adj.get("pct_change", 0) / 100.0
            for r in matching:
                r["amount"]        = round(r["amount"]        * (1 + pct), 2)
                r["budget_amount"] = round(r["budget_amount"] * (1 + pct), 2)

    # Shift dates to target year if it differs from the baseline year
    if target_year and scenario:
        base_year = int(scenario[0]["date"][:4])
        if target_year != base_year:
            for r in scenario:
                r["date"] = str(target_year) + r["date"][4:]

    return scenario


def _resolve_account_filter(filters: dict,
                            mu: ModelUnderstanding) -> set[int] | None:
    """
    Resolve account-related filters into a set of account IDs.

    Handles:
      - "account_group": section name from reporting_structures or account_groups
      - "account_ids": direct list or comma-separated string
      - Both present: intersection
      - Neither: returns None (match all accounts)
    """
    from_group = None
    from_ids = None

    # Resolve account_group → account IDs via reporting_structures
    grp_name = filters.get("account_group")
    if grp_name:
        # First try reporting_structures sections
        from_group = mu.account_ids_for_section(grp_name)
        if not from_group:
            # Fall back to legacy account_groups
            legacy = mu.account_groups.get(grp_name, {})
            from_group = set(legacy.get("account_ids", []))
        if not from_group:
            # Group name not found — try parsing as comma-separated IDs
            from_group = _parse_id_list(grp_name)

    # Resolve account_ids → direct set
    raw_ids = filters.get("account_ids")
    if raw_ids is not None:
        if isinstance(raw_ids, list):
            from_ids = {int(x) for x in raw_ids}
        else:
            from_ids = _parse_id_list(str(raw_ids))

    if from_group is not None and from_ids is not None:
        return from_group & from_ids
    if from_group is not None:
        return from_group
    if from_ids is not None:
        return from_ids
    return None


def _parse_id_list(s: str) -> set[int]:
    """Parse a comma-separated string of IDs into a set of ints."""
    return {int(x.strip()) for x in s.split(",") if x.strip().isdigit()}


def _row_matches_filters(row: dict, filters: dict) -> bool:
    """Check if a row matches all non-account dimension filters."""
    for col, val in filters.items():
        row_val = row.get(col)
        if row_val is None:
            return False
        # Coerce types for comparison (row may have int, filter may have str)
        try:
            if type(row_val) != type(val):
                if isinstance(val, int):
                    row_val = int(row_val)
                elif isinstance(val, float):
                    row_val = float(row_val)
                else:
                    row_val = str(row_val)
                    val = str(val)
        except (ValueError, TypeError):
            return False
        if row_val != val:
            return False
    return True


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
