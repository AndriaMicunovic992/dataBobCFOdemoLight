"""
queries.py — Model-agnostic query execution.

Uses template-based queries driven by ModelUnderstanding.
Supports both DAX (PBI) and SQL (Excel/DuckDB).

The discovery agent should populate query_templates in the ModelUnderstanding
with working 'fetch_budget' and 'fetch_account_map' templates. If templates
are missing, fallback auto-builders construct queries from scenario_target
and account_structure metadata.
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


# ── Fallback Query Builders ──────────────────────────────────────────────────
# These construct query template strings from ModelUnderstanding metadata
# when the discovery agent didn't provide explicit query_templates.


def _auto_build_fetch_budget(mu: ModelUnderstanding) -> str | None:
    """
    Auto-build a fetch_budget query template from scenario_target metadata.

    Returns a template string with {year}, {month_filter}, {company_id}
    placeholders, or None if required metadata is missing.
    """
    ft = mu.fact_table
    dc = mu.date_column
    amt_cols = mu.amount_columns

    if not ft or not dc or not amt_cols:
        print("[Query] Cannot auto-build fetch_budget: missing fact_table, "
              "date_column, or amount_columns in model understanding.")
        return None

    # Find the account FK column in the fact table via relationships
    account_fk = None
    if mu.account_table:
        account_fk = mu.find_fk_column(ft, mu.account_table)

    # Fallback: look in table metadata for a column with "account" in purpose
    if not account_fk:
        tinfo = mu.get_table(ft)
        if tinfo:
            for cname, cmeta in tinfo.get("important_columns", {}).items():
                purpose = (cmeta.get("purpose", "") or "").lower()
                if "account" in purpose or "konto" in purpose:
                    account_fk = cname
                    break

    # Last resort: use the account_id_column from account_structure
    # (this is the PK in the dimension table, but in many models
    # the FK in the fact table has the same name)
    if not account_fk:
        account_fk = mu.account_id_column

    if not account_fk:
        print("[Query] Cannot auto-build fetch_budget: unable to determine "
              "account FK column in fact table.")
        return None

    lang = mu.query_language
    stc = mu.scenario_type_column
    stv = mu.scenario_type_values
    budget_val = stv.get("budget", stv.get("scenario_base"))
    cc = mu.company_column

    if lang == "DAX":
        return _build_dax_fetch_budget(
            ft, dc, amt_cols, account_fk, stc, budget_val, cc, mu
        )
    elif lang == "SQL":
        return _build_sql_fetch_budget(
            ft, dc, amt_cols, account_fk, stc, budget_val, cc, mu
        )
    else:
        print(f"[Query] Unknown query_language: {lang}")
        return None


def _build_dax_fetch_budget(ft, dc, amt_cols, account_fk,
                             stc, budget_val, cc, mu) -> str:
    """Build DAX EVALUATE SELECTCOLUMNS query for fetch_budget."""
    # Filter conditions
    filters = [f"YEAR('{ft}'[{dc}]) = {{year}}"]
    if cc:
        filters.append(f"'{ft}'[{cc}] = {{company_id}}")
    if stc and budget_val is not None:
        filters.append(f"'{ft}'[{stc}] = {budget_val}")

    filter_str = " && ".join(filters)

    # SELECTCOLUMNS aliases
    select_parts = [
        f'"main_account_id", \'{ft}\'[{account_fk}]',
        f'"accounting_date", \'{ft}\'[{dc}]',
        f'"amount", \'{ft}\'[{amt_cols[0]}]',
    ]
    if len(amt_cols) >= 2:
        select_parts.append(f'"budget_amount", \'{ft}\'[{amt_cols[1]}]')
    else:
        select_parts.append(f'"budget_amount", \'{ft}\'[{amt_cols[0]}]')

    # Add additional FK columns from table metadata if available
    tinfo = mu.get_table(ft)
    if tinfo:
        skip = {account_fk, dc} | set(amt_cols)
        if stc:
            skip.add(stc)
        if cc:
            skip.add(cc)
        for cname, cmeta in tinfo.get("important_columns", {}).items():
            if cname not in skip:
                # Pass through with original name as alias
                select_parts.append(f'"{cname}", \'{ft}\'[{cname}]')

    select_str = ", ".join(select_parts)

    template = (
        f"EVALUATE SELECTCOLUMNS("
        f"FILTER('{ft}', {filter_str} {{month_filter}}), "
        f"{select_str})"
    )

    print(f"[Query] Auto-built DAX fetch_budget template from metadata")
    return template


def _build_sql_fetch_budget(ft, dc, amt_cols, account_fk,
                             stc, budget_val, cc, mu) -> str:
    """Build SQL SELECT query for fetch_budget."""
    # Column aliases
    cols = [
        f"{account_fk} AS main_account_id",
        f"{dc} AS accounting_date",
        f"{amt_cols[0]} AS amount",
    ]
    if len(amt_cols) >= 2:
        cols.append(f"{amt_cols[1]} AS budget_amount")
    else:
        cols.append(f"{amt_cols[0]} AS budget_amount")

    # Add FK columns from table metadata
    tinfo = mu.get_table(ft)
    if tinfo:
        skip = {account_fk, dc} | set(amt_cols)
        if stc:
            skip.add(stc)
        if cc:
            skip.add(cc)
        for cname in tinfo.get("important_columns", {}):
            if cname not in skip:
                cols.append(cname)

    col_str = ", ".join(cols)

    # WHERE conditions
    wheres = [f"YEAR({dc}) = {{year}}"]
    if cc:
        wheres.append(f"{cc} = {{company_id}}")
    if stc and budget_val is not None:
        wheres.append(f"{stc} = {budget_val}")

    where_str = " AND ".join(wheres)

    template = f"SELECT {col_str} FROM {ft} WHERE {where_str} {{month_filter}}"

    print(f"[Query] Auto-built SQL fetch_budget template from metadata")
    return template


def _auto_build_fetch_account_map(mu: ModelUnderstanding) -> str | None:
    """
    Auto-build a fetch_account_map query template from account_structure metadata.

    Returns a template string with {account_ids} placeholder,
    or None if required metadata is missing.
    """
    acct_table = mu.account_table
    acct_id_col = mu.account_id_column
    acct_name_col = mu.account_name_column

    if not acct_table or not acct_id_col:
        print("[Query] Cannot auto-build fetch_account_map: missing "
              "account_table or account_id_column.")
        return None

    # Find grouping column
    grouping_cols = mu.account_structure.get("grouping_columns", [])
    group_col = grouping_cols[0] if grouping_cols else None

    # Find cashflow position column
    cf_col = None
    cf_config = mu.cashflow_config
    if cf_config.get("structure_table"):
        # CF position may be in a separate table — check if it's the same
        # as the account table or accessible via relationship
        cf_table = cf_config.get("structure_table", "")
        cf_position_col = cf_config.get("position_column", "")
        if cf_table == acct_table and cf_position_col:
            cf_col = cf_position_col

    lang = mu.query_language

    if lang == "DAX":
        return _build_dax_fetch_account_map(
            acct_table, acct_id_col, acct_name_col, group_col, cf_col
        )
    elif lang == "SQL":
        return _build_sql_fetch_account_map(
            acct_table, acct_id_col, acct_name_col, group_col, cf_col
        )
    else:
        return None


def _build_dax_fetch_account_map(acct_table, id_col, name_col,
                                  group_col, cf_col) -> str:
    """Build DAX EVALUATE SELECTCOLUMNS for account map."""
    select_parts = [
        f'"id", \'{acct_table}\'[{id_col}]',
        f'"nr", \'{acct_table}\'[{id_col}]',  # nr = same as id if no separate number col
    ]

    if name_col:
        select_parts.append(f'"name", \'{acct_table}\'[{name_col}]')
    else:
        select_parts.append(f'"name", \'{acct_table}\'[{id_col}]')

    if group_col:
        select_parts.append(f'"group", \'{acct_table}\'[{group_col}]')
    else:
        select_parts.append(f'"group", ""')

    if cf_col:
        select_parts.append(f'"cf_position", \'{acct_table}\'[{cf_col}]')
    else:
        select_parts.append(f'"cf_position", 0')

    select_str = ", ".join(select_parts)

    # DAX IN syntax needs {1, 2, 3} — curly braces around the list.
    # The template must contain {{{account_ids}}} for Python .format():
    #   {{ → literal {, {account_ids} → substituted value, }} → literal }
    # To avoid f-string/format escaping conflicts, use a variable:
    acct_placeholder = "{{{account_ids}}}"
    template = (
        f"EVALUATE SELECTCOLUMNS("
        f"FILTER('{acct_table}', "
        f"'{acct_table}'[{id_col}] IN {acct_placeholder}), "
        f"{select_str})"
    )

    print(f"[Query] Auto-built DAX fetch_account_map template from metadata")
    return template


def _build_sql_fetch_account_map(acct_table, id_col, name_col,
                                  group_col, cf_col) -> str:
    """Build SQL SELECT for account map."""
    cols = [
        f"{id_col} AS id",
        f"{id_col} AS nr",
    ]
    if name_col:
        cols.append(f"{name_col} AS name")
    else:
        cols.append(f"CAST({id_col} AS TEXT) AS name")

    if group_col:
        cols.append(f'"{group_col}" AS "group"')
    else:
        cols.append("'' AS \"group\"")

    if cf_col:
        cols.append(f"COALESCE({cf_col}, 0) AS cf_position")
    else:
        cols.append("0 AS cf_position")

    col_str = ", ".join(cols)
    template = f"SELECT {col_str} FROM {acct_table} WHERE {id_col} IN ({{account_ids}})"

    print(f"[Query] Auto-built SQL fetch_account_map template from metadata")
    return template


# ── Main Query Functions ─────────────────────────────────────────────────────


async def fetch_budget_generic(source: DataSource,
                                mu: ModelUnderstanding,
                                year: int,
                                months: list[int] | None = None) -> list[dict]:
    """
    Fetch budget/baseline data using query templates from ModelUnderstanding.

    If no explicit template exists, auto-builds one from scenario_target metadata.

    Returns rows in the standard format:
        {account, date, amount, budget_amount, currency_id, ..., account_nr, account_name, ...}
    """
    template = mu.get_query_template("fetch_budget")
    if not template:
        print("[Query] No fetch_budget template — auto-building from metadata...")
        template = _auto_build_fetch_budget(mu)
    if not template:
        raise RuntimeError(
            "No 'fetch_budget' query template in model understanding and "
            "unable to auto-build from metadata. Please go to Data Understanding "
            "and re-run discovery to generate query templates."
        )

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

    # Normalize standard fields: the rest of the codebase expects "account" and "date"
    # but the query template returns "main_account_id" and "accounting_date".
    for r in rows:
        # Normalize account
        acc = r.get("main_account_id") or r.get("account")
        if acc is not None:
            r["account"] = int(acc)

        # Normalize date: "accounting_date" → "date"
        if "accounting_date" in r and "date" not in r:
            date_val = r["accounting_date"]
            # Convert to YYYY-MM-DD string if needed
            if hasattr(date_val, "strftime"):
                r["date"] = date_val.strftime("%Y-%m-%d")
            else:
                r["date"] = str(date_val)[:10]  # truncate any time component
        elif "date" in r:
            date_val = r["date"]
            if hasattr(date_val, "strftime"):
                r["date"] = date_val.strftime("%Y-%m-%d")
            else:
                r["date"] = str(date_val)[:10]

        # Normalize amount fields: "budget_amount" may be missing, default to "amount"
        if "amount" in r and "budget_amount" not in r:
            r["budget_amount"] = r["amount"]

    # Enrich with account metadata
    if rows:
        account_ids = {r.get("account") for r in rows if r.get("account") is not None}
        account_ids.discard(None)
        if account_ids:
            acc_map = await fetch_account_map_generic(source, mu, account_ids)
            for r in rows:
                acc = r.get("account")
                if acc is not None:
                    info = acc_map.get(acc, {})
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

    If no explicit template exists, auto-builds one from account_structure metadata.

    Returns: {account_id: {"nr": "320000", "name": "...", "group": "...", "cf_position": 0}}
    """
    template = mu.get_query_template("fetch_account_map")
    if not template:
        print("[Query] No fetch_account_map template — auto-building from metadata...")
        template = _auto_build_fetch_account_map(mu)
    if not template:
        print("[Query] Cannot build account map — returning empty (no names/groups)")
        return {}

    if account_ids:
        ids_str = ", ".join(str(int(i)) for i in sorted(account_ids))
    else:
        ids_str = ""

    query = template.format(account_ids=ids_str)

    print(f"[Query] Fetching account metadata...")
    resp = await source.query(query)

    if not resp.get("success"):
        print(f"[Query] Account map query failed: {resp.get('message', 'unknown')}")
        return {}

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
