# Plan: Core Architecture Rethink

## Goal
Strip the app back to essentials and make the core loop work end-to-end:
**Connect → Discover → Understand → Load Baseline → Stage (filter-based) → Preview (P&L hierarchy) → SQL**

## Guiding Principles
- Remove everything not needed for core GL scenario flow
- Filter-based adjustments (any GL dimension), not hardcoded revenue/cogs
- Reporting structures (P&L, BS, CF) defined during discovery, driven by DimAccount columns
- Discovery has a checklist — MU is only complete when all items are covered

---

## Step 1: ModelUnderstanding restructure (`discovery/model_understanding.py`)

**Add:**
- `gl_dimensions` — list of dimensions connected to GL fact table:
  ```python
  # In raw JSON:
  "gl_dimensions": [
    {"column": "CompanyID", "dimension_table": "DimCompany", "label": "Company", "label_column": "CompanyName"},
    {"column": "MainAccountID", "dimension_table": "DimAccount", "label": "Account", "label_column": "AccountName"},
    {"column": "CostCenterID", "dimension_table": null, "label": "Cost Center", "label_column": null}
  ]
  ```
- `reporting_structures` — hierarchical P&L/BS/CF:
  ```python
  # In raw JSON:
  "reporting_structures": {
    "pl": {
      "name": "Profit & Loss",
      "sections": [
        {"name": "Gross Revenue", "account_ids": [4010, 4020, 4030], "sign": 1},
        {"name": "Discounts", "account_ids": [4100], "sign": -1},
        {"name": "Net Revenue", "type": "subtotal", "sum_of": ["Gross Revenue", "Discounts"]},
        {"name": "COGS", "account_ids": [5010, 5020], "sign": -1},
        {"name": "Gross Profit", "type": "subtotal", "sum_of": ["Net Revenue", "COGS"]},
        {"name": "OpEx", "account_ids": [6010, 6020, 6100], "sign": -1},
        {"name": "EBITDA", "type": "subtotal", "sum_of": ["Gross Profit", "OpEx"]}
      ]
    },
    "bs": { "name": "Balance Sheet", "sections": [...] },
    "cf": { "name": "Cash Flow", "sections": [...] }
  }
  ```

**Remove (strip for now):**
- `cashflow_config` property and `has_cashflow`
- `customer_config` property and `has_customer_dimension`
- `measures` section (can add back later if needed)

**Keep:**
- `account_structures` / `account_groups` — still needed for backward compat during transition
- `query_templates`, `scenario_target`, `filter_dimensions`, `relationships`
- `reporting_groups` (legacy, read from `reporting_structures` when available)

**New property accessors:**
- `gl_dimensions` → list of dimension dicts
- `reporting_structures` → dict of statement structures
- `get_reporting_structure(name)` → single structure (pl, bs, cf)
- `all_account_ids_for_group(group_name)` → set of IDs from reporting_structures sections

---

## Step 2: Discovery agent rework (`discovery/discovery_agent.py`)

**Rewrite DISCOVERY_PROMPT with checklist approach:**

The agent must work through and confirm each item:
1. **GL fact table identified** — which table, which columns are amounts/dates
2. **Account dimension found** — which table, which columns contain grouping info
3. **Account groups mapped** — agent reads DimAccount grouping columns, proposes groups with account IDs
4. **Value types identified** — actuals=?, budget=?, etc.
5. **GL dimensions mapped** — all FK columns on GL fact, their dimension tables, label columns
6. **Reporting structures proposed** — P&L (required), BS and CF (defined in conversation)
   - Agent proposes initial structure from account grouping columns
   - User refines ("split OpEx into Personnel and Other", "add EBITDA line")
7. **`fetch_baseline` template built and tested** — must return rows with correct aliases
8. **`fetch_account_map` template built and tested**

**Checklist tracking:**
- Agent maintains an internal checklist in conversation
- After each save, tells user which items are complete vs pending
- Understanding status stays "draft" until all 8 items are covered

**Key change:** Agent looks at DimAccount columns (AccountGroup, ReportingGroup, StatementType, etc.) to auto-derive:
- `account_structure.groups` — from grouping column values + account IDs
- `reporting_structures.pl` — from P&L-related groups
- User can override/refine any of these

**Remove from prompt:**
- Customer query template instructions (query_customers_top, query_customers_total)
- Cashflow config section
- Measures emphasis (keep as optional, not prominent)

---

## Step 3: Scenario math rework (`scenario.py`)

**Rewrite `build_scenario()`:**

Current signature:
```python
def build_scenario(rows, adjustments, revenue_accs=None, cogs_accs=None, target_year=None)
```

New signature:
```python
def build_scenario(rows, adjustments, mu, target_year=None)
```

**New adjustment format (filter-based):**
```python
{
  "description": "Increase Company A revenue 10%",
  "filters": {
    "account_group": "Gross Revenue",   # matches reporting_structures section name
    "company_id": 7                      # any GL dimension column
  },
  "pct_change": 10.0
  # OR "abs_change": 50000.0
}
```

**Matching logic:**
- For each filter key/value pair:
  - If key is `account_group` → resolve to account IDs from reporting_structures, match on `account` column
  - If key is `account_ids` → match directly on `account` column (comma-separated IDs or list)
  - Otherwise → match on the row's column (e.g., `company_id`, `cost_center_id`)
- Row matches only if ALL filters match (AND logic)
- `"filters": {}` or no filters → matches all rows (same as old "all" group)

**Remove:**
- `revenue_accs` / `cogs_accs` parameters (resolved from MU internally)
- Hardcoded "revenue"/"cogs"/"all" group name logic

**Keep:**
- `make_sql()` — unchanged
- `save_sql()` — unchanged
- `_derive_sql_columns()` — unchanged
- Date shifting logic — unchanged

---

## Step 4: Prompt builder rework (`prompts/builder.py`)

**Rewrite `build()` sections:**

- `_intro()` — keep as-is
- `_data_model()` — add GL dimensions, available filter columns
- `_accounts()` → `_reporting_structures()` — describe P&L/BS/CF hierarchy, explain which section names can be used in `account_group` filter
- **Remove:** `_measures_section()`, `_customer_section()`, `_cashflow_section()`
- **Add:** `_dimensions_section()` — list available GL dimensions for filter-based staging

**Rewrite `build_tools()`:**

Tools:
1. `run_query` — keep as-is (fetch baseline by year/type)
2. **Remove:** `run_custom_query` (add back later)
3. **Remove:** `query_customers` (add back later)

**Update stage block format in workflow template:**
```
```stage
{
  "description": "Increase Company A revenue by 10%",
  "adjustments": [
    {
      "filters": {"account_group": "Gross Revenue", "company_id": 7},
      "pct_change": 10.0,
      "months": [1,2,3]
    }
  ]
}
```
```

---

## Step 5: Agent simplification (`agent.py`)

**Remove:**
- `_handle_custom_query()` method
- `_handle_query_customers()` method
- `_format_custom_result()` method
- `cache_save` / `cache_load` imports (broken anyway — cache.py doesn't exist)

**Update:**
- `_handle_tool()` — only handles `run_query`
- `data_summary()` — rewrite to use reporting_structures for grouping instead of pl_groups/revenue/cogs
- `_build_dynamic_context()` — list available dimensions and reporting structure section names
- Apply block handler — pass `mu` to `build_scenario()` instead of `revenue_accs`/`cogs_accs`

**Fix:**
- Remove `from cache import cache_save, cache_load` (file doesn't exist)
- Store rows in `self.rows` only (no external cache needed for core flow)

---

## Step 6: Server cleanup (`server.py`)

**Simplify agent initialization:**
- `_init_agents()` — create fresh agents, no state preservation complexity
- `_refresh_scenario_agent()` — keep but simplify: just creates new Agent with current MU
- On model switch: fully reset everything (no state transfer)

**Clean endpoints:**
- Keep: `/api/connect/*`, `/api/discovery/chat`, `/api/chat`, `/api/model/*`, `/api/scenario/preview`, `/api/scenario/staged`
- `/api/scenario/preview` — pass `mu` to `build_scenario()` instead of `revenue_accs`/`cogs_accs`

---

## Step 7: UI cleanup (`ui.html`)

**Preview modal:**
- Render P&L from `reporting_structures.pl` — show sections with subtotals
- Remove cashflow tab (for now)
- Show baseline vs scenario columns with delta

**Strip:**
- Cashflow-related UI elements
- Customer-related UI elements
- Waterfall chart stub (simplify to just the delta table)

**Keep:**
- Discovery chat + scenario chat tabs
- Baseline selectors (type + year, scenario year)
- Staging list with remove buttons
- SQL file viewer
- Model management (create, switch, delete)

---

## Step 8: Workflow template update (`prompts/templates/scenario_workflow.txt`)

Update the stage/apply block format documentation to reflect filter-based adjustments:
- Explain available `filters` keys (account_group, any GL dimension column, account_ids)
- Show examples with multi-dimensional filters
- Remove references to old "revenue"/"cogs"/"all" account_group strings

---

## Implementation Order

1. **ModelUnderstanding** — add new properties, remove cashflow/customer
2. **scenario.py** — rewrite build_scenario() for filter-based adjustments
3. **Discovery prompt** — checklist approach, reporting structures, dimensions
4. **Prompt builder** — reporting structures + dimensions in prompt, strip extras
5. **agent.py** — strip customer/custom query, fix cache import, use new build_scenario
6. **server.py** — simplify init, clean preview endpoint
7. **UI** — P&L hierarchy preview, strip cashflow/customer/waterfall
8. **Workflow template** — update stage/apply format docs

Each step is independently testable. Steps 1-2 are the foundation; 3-5 are the agent layer; 6-8 are the interface layer.
