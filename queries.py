"""
queries.py — Model-agnostic query execution.

Replaces the hardcoded DAX queries in dax.py with template-based queries
driven by ModelUnderstanding. Supports both DAX (PBI) and SQL (Excel/DuckDB).

For backward compatibility, dax.py still exists and is used when no
ModelUnderstanding is available.
"""

from datasources.base import DataSource
from discovery.model_understanding import ModelUnderstanding


def _parse_response_rows(resp: dict) -> list[dict]:
    """
    Parse query response rows, normalizing column name formats.

    Handles:
      - "Table[column]" format (PBI DAX)
      - "[column]" format (PBI aliased)
      - "column" format (DuckDB/SQL)
    """
    raw_rows = resp.get("data", {}).get("rows", [])
    clean = []
    for r in raw_rows:
        row = {}
        for k, v in r.items():
            # Normalize: "Table[col]" → "col", "[col]" → "col"
            if "[" in k:
                col = k.split("[")[-1].rstrip("]")
            else:
                col = k
            row[col] = v
        clean.append(row)
    return clean


async def fetch_budget_generic(source: DataSource,
                                mu: ModelUnderstanding,
                                year: int,
                                months: list[int] | None = None) -> list[dict]:
    """
    Fetch budget/baseline data using query templates from ModelUnderstanding.

    This is the model-agnostic replacement for dax.fetch_budget().
    The query template must be stored in mu.query_templates["fetch_budget"].

    Returns rows in the standard format:
        {account, date, amount, budget_amount, currency_id, ..., account_nr, account_name, ...}
    """
    template = mu.get_query_template("fetch_budget")
    if not template:
        raise RuntimeError("No 'fetch_budget' query template in model understanding. "
                          "Please run model discovery first.")

    # Build month filter
    month_filter = ""
    if months and mu.query_language == "DAX":
        month_filter = " && (" + " || ".join(
            f"MONTH('{mu.fact_table}'[{mu.date_column}])={m}" for m in months
        ) + ")"
    elif months and mu.query_language == "SQL":
        month_list = ", ".join(str(m) for m in months)
        month_filter = f" AND EXTRACT(MONTH FROM {mu.date_column}) IN ({month_list})"

    # Fill template
    query = template.format(
        year=year,
        month_filter=month_filter,
        company_id=mu.company_id or "",
    )

    print(f"[Query] Fetching {year} budget" +
          (f" months={months}" if months else "") + "...")
    resp = await source.query(query)

    if not resp.get("success"):
        raise RuntimeError(f"Query failed: {resp.get('message', 'unknown error')}")

    rows = _parse_response_rows(resp)
    print(f"[Query] Got {len(rows)} rows")

    # Enrich with account metadata if template exists
    account_template = mu.get_query_template("fetch_account_map")
    if account_template and rows:
        account_ids = {r.get("main_account_id") or r.get("account")
                       for r in rows if r.get("main_account_id") or r.get("account")}
        account_ids.discard(None)
        acc_map = await fetch_account_map_generic(source, mu, account_ids)

        for r in rows:
            acc = r.get("main_account_id") or r.get("account")
            if acc is not None:
                acc = int(acc)
                info = acc_map.get(acc, {})
                r["account"] = acc
                r["account_nr"] = info.get("nr", str(acc))
                r["account_name"] = info.get("name", f"Account {acc}")
                r["account_grp"] = info.get("group", "")
                r["cf_position"] = info.get("cf_position", 0)

    return rows


async def fetch_account_map_generic(source: DataSource,
                                     mu: ModelUnderstanding,
                                     account_ids: set | None = None) -> dict[int, dict]:
    """
    Fetch GL account metadata using template from ModelUnderstanding.

    Returns: {account_id: {"nr": "320000", "name": "...", "group": "...", "cf_position": 0}}
    """
    template = mu.get_query_template("fetch_account_map")
    if not template:
        return {}

    if account_ids:
        ids_str = ", ".join(str(int(i)) for i in sorted(account_ids))
    else:
        ids_str = ""

    query = template.format(account_ids=ids_str)

    print(f"[Query] Fetching account metadata...")
    resp = await source.query(query)
    rows = _parse_response_rows(resp)

    result = {}
    for r in rows:
        aid = int(r.get("id", 0))
        result[aid] = {
            "nr":          str(r.get("nr", "") or ""),
            "name":        str(r.get("name", "") or ""),
            "group":       str(r.get("group", "") or ""),
            "cf_position": int(r.get("cf_position", 0) or 0),
        }

    print(f"[Query] Resolved {len(result)} account names")
    return result
