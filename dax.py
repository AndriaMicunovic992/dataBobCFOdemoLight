"""
dax.py — DAX query helpers for reading data from Power BI Desktop.

Provides:
  parse_pbi_response()    — convert raw MCP response to clean row dicts
  fetch_account_map()     — live lookup of GL account names/numbers from Dim Hauptkonto
  fetch_cost_object_map() — live lookup of cost-object names from Dim Kostenträger
  fetch_budget()          — fetch budget rows from Fakten Hauptbuch, enriched with
                            human-readable dimension names from the semantic model
"""

from pbi_client import PBIClient
from config import COMPANY_ID


def parse_pbi_response(resp: dict) -> list[dict]:
    """
    Convert a Power BI MCP DAX response to a list of clean row dicts.

    MCP returns column names in two formats:
      "Fakten Hauptbuch[column_name]"  (table-qualified)
      "[column_name]"                  (aliased)
    Both are normalised to plain string keys.
    """
    raw_rows = resp.get("data", {}).get("rows", [])
    clean = []
    for r in raw_rows:
        def col(name):
            return r.get(f"Fakten Hauptbuch[{name}]", r.get(f"[{name}]"))
        def int_or_none(v):
            return int(v) if v is not None else None
        def flt(v):
            return float(v) if v is not None else 0.0

        clean.append({
            "account":                int(col("main_account_id")),
            "date":                   str(col("accounting_date"))[:10],
            "amount":                 flt(col("amount")),
            "budget_amount":          flt(col("budget_amount")),
            "currency_id":            int_or_none(col("currency_id")),
            "settlement_type_id":     int_or_none(col("settlement_type_id")),
            "cost_object_id":         int_or_none(col("cost_object_id")),
            "item_group_id":          int_or_none(col("item_group_id")),
            "cost_center_id":         int_or_none(col("cost_center_id")),
            "project_id":             int_or_none(col("project_id")),
            "it_category_id":         int_or_none(col("it_category_id")),
            "financial_dimension_id": int_or_none(col("financial_dimension_id")),
        })
    return clean


async def fetch_account_map(pbi: PBIClient,
                            account_ids: set[int] | None = None) -> dict[int, dict]:
    """
    Fetch GL account metadata from Dim Hauptkonto.

    Args:
        pbi:         Connected PBIClient instance.
        account_ids: Optional set of main_account_id values to filter by.
                     If None, fetches all accounts in the table.

    Returns:
        {main_account_id: {"nr": "320000", "name": "Warenverkauf", "group": "Umsatz"}}
    """
    if account_ids is not None:
        ids_str   = ", ".join(str(i) for i in sorted(account_ids))
        filter_clause = f"FILTER('Dim Hauptkonto', 'Dim Hauptkonto'[main_account_id] IN {{{ids_str}}})"
        source = filter_clause
    else:
        source = "'Dim Hauptkonto'"

    query = f"""EVALUATE
SELECTCOLUMNS(
    {source},
    "id",          'Dim Hauptkonto'[main_account_id],
    "nr",          'Dim Hauptkonto'[Hauptkonto-Nr.],
    "name",        'Dim Hauptkonto'[Hauptkonto],
    "group",       'Dim Hauptkonto'[Reporting H2],
    "cf_position", 'Dim Hauptkonto'[Position Geldflussrechnung]
)
ORDER BY [id]"""

    n = len(account_ids) if account_ids else "all"
    print(f"[DAX] Fetching account names for {n} account(s)...")
    resp   = await pbi.dax(query)
    result = {}
    for r in resp.get("data", {}).get("rows", []):
        aid = int(r.get("[id]", 0))
        cf  = r.get("[cf_position]")
        result[aid] = {
            "nr":          str(r.get("[nr]",    "") or ""),
            "name":        str(r.get("[name]",  "") or ""),
            "group":       str(r.get("[group]", "") or ""),
            "cf_position": int(cf) if cf is not None else 0,
        }
    print(f"[DAX] Resolved {len(result)} account name(s)")
    return result


async def fetch_cost_object_map(pbi: PBIClient,
                                ids: set[int]) -> dict[int, str]:
    """
    Fetch human-readable labels for a set of cost_object_id values from Dim Kostenträger.

    Returns:
        {cost_object_id: "BusinessID – Name"}  (falls back to str(id) if not found)
    """
    if not ids:
        return {}

    ids_str = ", ".join(str(i) for i in sorted(ids))
    query = f"""EVALUATE
SELECTCOLUMNS(
    FILTER('Dim Kostenträger', 'Dim Kostenträger'[cost_object_id] IN {{{ids_str}}}),
    "id",          'Dim Kostenträger'[cost_object_id],
    "name",        'Dim Kostenträger'[Kostenträger],
    "business_id", 'Dim Kostenträger'[Kostenträger Business ID]
)"""

    print(f"[DAX] Fetching cost-object names for {len(ids)} ID(s)...")
    resp   = await pbi.dax(query)
    result = {}
    for r in resp.get("data", {}).get("rows", []):
        cid  = int(r.get("[id]", 0))
        name = r.get("[name]", "") or ""
        bid  = r.get("[business_id]", "") or ""
        result[cid] = f"{bid} – {name}" if (bid and name) else (name or bid or str(cid))
    print(f"[DAX] Resolved {len(result)} cost-object name(s)")
    return result


async def fetch_bs_baseline(pbi: PBIClient, actual_year: int,
                            target_year: int,
                            budget_account_ids: set[int],
                            months: list[int] | None = None) -> list[dict]:
    """
    Fetch prior-year actuals for BS/CF accounts that have no budget rows.

    Actuals are aggregated by account × month, then date-shifted to the target
    year.  Dimension FK columns (cost_object, cost_center, …) are set to NULL
    since the aggregation collapses them.

    Returns rows in the same format as parse_pbi_response() output.
    """
    ids_str = ", ".join(str(i) for i in sorted(budget_account_ids))

    month_filter = ""
    if months:
        month_filter = " && (" + " || ".join(
            f"MONTH('Fakten Hauptbuch'[accounting_date])={m}" for m in months
        ) + ")"

    query = f"""EVALUATE
SELECTCOLUMNS(
    FILTER(
        'Fakten Hauptbuch',
        'Fakten Hauptbuch'[company_id]    = {COMPANY_ID}
            && 'Fakten Hauptbuch'[value_type_id] = 1
            && YEAR('Fakten Hauptbuch'[accounting_date]) = {actual_year}
            && NOT('Fakten Hauptbuch'[main_account_id] IN {{{ids_str}}})
            {month_filter}
    ),
    "main_account_id", 'Fakten Hauptbuch'[main_account_id],
    "accounting_date", 'Fakten Hauptbuch'[accounting_date],
    "amount",          'Fakten Hauptbuch'[amount]
)"""

    print(f"[DAX] Fetching {actual_year} actuals for BS/CF accounts "
          f"(excluding {len(budget_account_ids)} budget accounts)...")
    resp = await pbi.dax(query)
    raw  = resp.get("data", {}).get("rows", [])

    # Aggregate by account × month
    agg: dict[tuple[int, int], float] = {}
    for r in raw:
        acc      = int(r.get("[main_account_id]", 0))
        date_str = str(r.get("[accounting_date]", ""))[:10]
        month    = int(date_str[5:7]) if len(date_str) >= 7 else 1
        key      = (acc, month)
        agg[key] = agg.get(key, 0.0) + float(r.get("[amount]", 0) or 0)

    rows = []
    for (acc, month), amount in sorted(agg.items()):
        amt = round(amount, 2)
        rows.append({
            "account":                acc,
            "date":                   f"{target_year}-{month:02d}-01",
            "amount":                 amt,
            "budget_amount":          amt,
            "currency_id":            None,
            "settlement_type_id":     None,
            "cost_object_id":         None,
            "item_group_id":          None,
            "cost_center_id":         None,
            "project_id":             None,
            "it_category_id":         None,
            "financial_dimension_id": None,
        })

    print(f"[DAX] BS baseline: {len(raw)} raw → {len(rows)} aggregated rows "
          f"({len({k[0] for k in agg})} accounts)")
    return rows


async def fetch_budget(pbi: PBIClient, year: int,
                       months: list[int] | None = None) -> list[dict]:
    """
    Fetch budget rows (value_type_id=2) from Fakten Hauptbuch for a given year,
    plus prior-year actuals for BS/CF accounts that have no budget rows.

    Each row is enriched with human-readable dimension values fetched live from
    the semantic model — no hardcoded mapping needed:

      account_nr        — D365 GL number, e.g. "320000"  (from Dim Hauptkonto)
      account_name      — GL account name, e.g. "Warenverkauf"
      account_grp       — Reporting H2 group, e.g. "Umsatz"
      cost_object_name  — readable label, e.g. "50001 – Industrieprodukte"
                          (from Dim Kostenträger, or None if no cost_object_id)

    The integer FK columns (account, cost_object_id, …) are left unchanged so
    make_sql() can still build the correct INSERT VALUES rows.
    """
    month_filter = ""
    if months:
        month_filter = " && (" + " || ".join(
            f"MONTH('Fakten Hauptbuch'[accounting_date])={m}" for m in months
        ) + ")"

    query = f"""EVALUATE
SELECTCOLUMNS(
    FILTER(
        'Fakten Hauptbuch',
        'Fakten Hauptbuch'[company_id]    = {COMPANY_ID}
            && 'Fakten Hauptbuch'[value_type_id] = 2
            && YEAR('Fakten Hauptbuch'[accounting_date]) = {year}
            {month_filter}
    ),
    "main_account_id",        'Fakten Hauptbuch'[main_account_id],
    "accounting_date",        'Fakten Hauptbuch'[accounting_date],
    "amount",                 'Fakten Hauptbuch'[amount],
    "budget_amount",          'Fakten Hauptbuch'[budget_amount],
    "currency_id",            'Fakten Hauptbuch'[currency_id],
    "settlement_type_id",     'Fakten Hauptbuch'[settlement_type_id],
    "cost_object_id",         'Fakten Hauptbuch'[cost_object_id],
    "item_group_id",          'Fakten Hauptbuch'[item_group_id],
    "cost_center_id",         'Fakten Hauptbuch'[cost_center_id],
    "project_id",             'Fakten Hauptbuch'[project_id],
    "it_category_id",         'Fakten Hauptbuch'[it_category_id],
    "financial_dimension_id", 'Fakten Hauptbuch'[financial_dimension_id]
)
ORDER BY [accounting_date], [main_account_id]"""

    suffix = f" months={months}" if months else ""
    print(f"[DAX] Fetching {year} budget{suffix}...")
    resp = await pbi.dax(query)

    if not resp.get("success"):
        raise RuntimeError(f"DAX failed: {resp.get('message', 'unknown error')}")

    rows = parse_pbi_response(resp)
    print(f"[DAX] Got {len(rows)} budget rows")

    # ── BS/CF baseline from prior-year actuals ────────────────────────────────
    budget_account_ids = {r["account"] for r in rows}
    bs_rows = await fetch_bs_baseline(pbi, year - 1, year,
                                      budget_account_ids, months)
    rows.extend(bs_rows)

    if rows:
        print(f"[DAX] Total: {len(rows)} rows (budget + BS baseline)")

    # ── Enrich: GL account names live from Dim Hauptkonto ─────────────────────
    account_ids = {r["account"] for r in rows}
    acc_map     = await fetch_account_map(pbi, account_ids)
    for r in rows:
        info = acc_map.get(r["account"], {})
        r["account_nr"]   = info.get("nr",    str(r["account"]))
        r["account_name"] = info.get("name",  f"Account {r['account']}")
        r["account_grp"]  = info.get("group", "")
        r["cf_position"]  = info.get("cf_position", 0)

    # ── Enrich: cost-object names live from Dim Kostenträger ──────────────────
    co_ids = {r["cost_object_id"] for r in rows if r["cost_object_id"] is not None}
    co_map = await fetch_cost_object_map(pbi, co_ids) if co_ids else {}
    for r in rows:
        cid = r["cost_object_id"]
        r["cost_object_name"] = co_map.get(cid, str(cid)) if cid is not None else None

    return rows


async def fetch_cashflow_structure(pbi: PBIClient) -> list[dict]:
    """
    Fetch the Dim Cashflow Struktur rows that define the cash-flow statement layout.

    Returns a list sorted by GruppeSort, each item containing:
        sort, display, gruppe, typ, invert, path_from, path_to
    """
    query = """EVALUATE
SELECTCOLUMNS(
    'Dim Cashflow Struktur',
    "sort",    'Dim Cashflow Struktur'[GruppeSort],
    "display", 'Dim Cashflow Struktur'[GruppeDisplay],
    "gruppe",  'Dim Cashflow Struktur'[Gruppe],
    "typ",     'Dim Cashflow Struktur'[Typ],
    "invert",  'Dim Cashflow Struktur'[Invert],
    "pfrom",   'Dim Cashflow Struktur'[PathFrom],
    "pto",     'Dim Cashflow Struktur'[PathTo]
)
ORDER BY [sort]"""

    print("[DAX] Fetching cashflow structure...")
    resp = await pbi.dax(query)
    rows = []
    for r in resp.get("data", {}).get("rows", []):
        rows.append({
            "sort":      int(r.get("[sort]",  0)),
            "display":   str(r.get("[display]", "")),
            "gruppe":    str(r.get("[gruppe]",  "")),
            "typ":       str(r.get("[typ]",     "")),
            "invert":    int(r.get("[invert]",  0) or 0),
            "path_from": int(r.get("[pfrom]",   0)),
            "path_to":   int(r.get("[pto]",     0)),
        })
    print(f"[DAX] Loaded {len(rows)} cashflow structure rows")
    return rows
