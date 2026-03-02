"""
agent.py — Claude AI agent for generating financial scenarios.

Provides:
  Agent   — wraps the Anthropic API, handles tool calls, stages adjustments,
            and generates SQL when the user explicitly applies a scenario.

The agent is model-agnostic: it receives a ModelUnderstanding document and
a DataSource, then generates its system prompt and tools dynamically.
A ModelUnderstanding is required — run the Discovery Agent first.
"""

import os
import re
import json

import anthropic

from config import SCENARIO_MODEL
from datasources.base import DataSource
from discovery.model_understanding import ModelUnderstanding
from prompts.builder import PromptBuilder
from cache import cache_save, cache_load
from queries import fetch_baseline
from scenario import build_scenario, make_sql, save_sql


# ── Helper functions ──────────────────────────────────────────────────────────

def data_summary(rows: list[dict],
                 mu: ModelUnderstanding) -> str:
    """
    Return a rich text summary of loaded budget rows.

    Account names, GL numbers, and reporting groups come from the enriched row
    fields added by fetch_baseline (live from the data source) — no static map
    needed here. Uses ModelUnderstanding's pl_groups to classify P&L vs BS accounts.
    """
    months:      set[str]         = set()
    acct_totals: dict[int, float] = {}
    acct_meta:   dict[int, dict]  = {}   # first-seen metadata per account

    for r in rows:
        months.add(r["date"][:7])
        acc = r["account"]
        acct_totals[acc] = acct_totals.get(acc, 0.0) + r["amount"]
        if acc not in acct_meta:
            acct_meta[acc] = {
                "nr":   r.get("account_nr",   str(acc)),
                "name": r.get("account_name", f"Account {acc}"),
                "grp":  r.get("account_grp",  "—"),
            }

    # Determine revenue/cogs account sets from ModelUnderstanding
    rev_accs  = mu.revenue_accounts()
    cogs_accs = mu.cogs_accounts()

    rev   = sum(v for acc, v in acct_totals.items() if acc in rev_accs)
    cogs  = sum(v for acc, v in acct_totals.items() if acc in cogs_accs)
    ms    = sorted(months)

    # Separate P&L vs BS accounts by group
    pl_groups = set(mu.pl_groups)

    pl_accts = {a for a in acct_totals if acct_meta[a]["grp"] in pl_groups}
    bs_accts = {a for a in acct_totals if a not in pl_accts}

    pl_total = sum(acct_totals[a] for a in pl_accts)
    bs_total = sum(acct_totals[a] for a in bs_accts)

    lines = [
        f"Data loaded: {len(rows)} rows | {len(acct_totals)} accounts "
        f"| {ms[0]} to {ms[-1]}",
        f"  P&L accounts : {len(pl_accts):>4}   (baseline {ms[0][:4]})",
        f"    Revenue    : {rev:>15,.0f}",
        f"    COGS       : {cogs:>15,.0f}",
        f"    P&L total  : {pl_total:>15,.0f}",
        f"  BS/CF accounts: {len(bs_accts):>4}   (prior-year actuals as baseline)",
        f"    BS total   : {bs_total:>15,.0f}",
        "",
        "Account breakdown:",
        f"  {'ID':>4}  {'GL-Nr.':>8}  {'Group':<26}  {'Name':<36}  {'Amount':>14}",
        "  " + "─" * 96,
    ]

    # P&L accounts first, then BS
    for acc in sorted(pl_accts):
        meta = acct_meta[acc]
        lines.append(
            f"  {acc:>4}  {meta['nr']:>8}  {meta['grp']:<26}  {meta['name']:<36}"
            f"  {acct_totals[acc]:>14,.0f}"
        )

    if bs_accts:
        lines.append("  " + "─" * 96)
        lines.append("  BS/CF accounts (prior-year actuals):")
        for acc in sorted(bs_accts):
            meta = acct_meta[acc]
            lines.append(
                f"  {acc:>4}  {meta['nr']:>8}  {meta['grp']:<26}  {meta['name']:<36}"
                f"  {acct_totals[acc]:>14,.0f}"
            )

    # Cost-object summary: collect names from enriched rows
    co_names: dict[int, str] = {}
    for r in rows:
        cid  = r.get("cost_object_id")
        name = r.get("cost_object_name")
        if cid is not None and name and cid not in co_names:
            co_names[cid] = name

    if co_names:
        lines += ["", f"  Cost objects ({len(co_names)} unique):"]
        for cid in sorted(co_names.keys())[:8]:
            lines.append(f"    {cid}: {co_names[cid]}")
        if len(co_names) > 8:
            lines.append(f"    … and {len(co_names) - 8} more")

    lines += ["", "Ready — tell me the adjustments to stage (P&L and/or BS accounts)."]
    return "\n".join(lines)


def extract_stage_block(text: str) -> dict | None:
    """Parse a ```stage JSON block from Claude's response."""
    m = re.search(r"```stage\s*(\{[\s\S]*?\})\s*```", text)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception as e:
        print(f"[Agent] Bad stage JSON: {e}\n{m.group(1)}")
        return None


def extract_apply_block(text: str) -> dict | None:
    """Parse an ```apply JSON block from Claude's response."""
    m = re.search(r"```apply\s*(\{[\s\S]*?\})\s*```", text)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception as e:
        print(f"[Agent] Bad apply JSON: {e}\n{m.group(1)}")
        return None


# ── Agent ─────────────────────────────────────────────────────────────────────

class Agent:
    """
    Wraps the Anthropic Claude API with tool-use for financial scenario generation.

    Adjustments are staged across multiple turns via ```stage blocks.
    SQL is only generated when the user triggers a ```apply block.
    Each applied scenario gets a unique, incrementing scenario_id (value_type_id).
    """

    def __init__(self, source: DataSource,
                 mu: ModelUnderstanding):
        if mu is None:
            raise ValueError(
                "ModelUnderstanding is required. "
                "Run the Discovery Agent first to build a model understanding."
            )
        self.source            = source
        self.mu                = mu
        from config import SCENARIO_API_KEY
        self.ai                = anthropic.Anthropic(api_key=SCENARIO_API_KEY)
        self.conv : list[dict] = []
        self.rows : list[dict] = []   # in-memory budget data
        self.staged: list[dict] = []  # staged adjustment groups [{description, adjustments}]
        self.next_scenario_id  = 3    # increments with each applied scenario
        self.base_type: str | None = None      # override for value_type (set by server)
        self.baseline_year: int | None = None  # year of data to load (set by server)
        self.scenario_year: int | None = None  # year the scenario applies to (set by server)

        # Build prompt and tools dynamically from ModelUnderstanding
        self._system_prompt = PromptBuilder.build(mu)
        self._tools = PromptBuilder.build_tools(mu)

    # ── Staged adjustments ────────────────────────────────────────────────────

    def get_staged(self) -> dict:
        """Return staged adjustments and metadata for the UI."""
        return {
            "staged":           self.staged,
            "next_id":          self.next_scenario_id,
            "adjustment_count": sum(len(s["adjustments"]) for s in self.staged),
        }

    def clear_staged(self):
        """Discard all staged adjustments."""
        self.staged = []
        print("[Agent] Staged adjustments cleared.")

    def remove_staged(self, index: int) -> bool:
        """Remove a single staged step by list index. Returns True if removed."""
        if 0 <= index < len(self.staged):
            removed = self.staged.pop(index)
            print(f"[Agent] Removed staged step {index}: {removed.get('description', '')!r}")
            return True
        return False

    # ── Tool execution ────────────────────────────────────────────────────────

    async def _handle_tool(self, name: str, inp: dict) -> str:
        if name == "run_query" or name == "run_dax_query":
            from datetime import datetime as _dt
            default_year = self.baseline_year or self.scenario_year or _dt.now().year
            year   = inp.get("year", default_year)
            months = inp.get("months")

            # Resolve value_type override from base_type setting
            vt_override = None
            if self.base_type:
                stv = self.mu.scenario_type_values
                vt_override = stv.get(self.base_type)

            rows = await fetch_baseline(
                self.source, self.mu, year, months,
                value_type_override=vt_override,
            )

            if not rows:
                return "Query returned no rows. Check the connection and filters."
            self.rows = rows
            cache_save(rows)
            return data_summary(rows, self.mu)

        if name == "query_customers":
            return await self._handle_query_customers(inp)

        return f"Unknown tool: {name}"

    async def _handle_query_customers(self, inp: dict) -> str:
        """Handle query_customers tool using ModelUnderstanding query templates."""
        from datetime import datetime as _dt
        base = self.baseline_year or self.scenario_year or _dt.now().year
        default_year = base - 1  # prior year for actuals
        year   = inp.get("year", default_year)
        top_n  = inp.get("top_n", 10)
        search = inp.get("search_name", "").strip()

        if self.mu.has_customer_dimension:
            # Use customer query templates from ModelUnderstanding
            template = self.mu.get_query_template("query_customers_top")
            if not template:
                return "No customer query template in model understanding."

            query = template.format(
                year=year, top_n=top_n,
                company_id=self.mu.company_id or "",
            )
            resp = await self.source.query(query)
            if not resp.get("success"):
                return f"Query failed: {resp.get('message', 'unknown')}"
            top_rows = resp.get("data", {}).get("rows", [])
            if not top_rows:
                return f"No customer revenue data found for {year}."

            # Format results — templates should return standard columns
            from queries import _parse_response_rows
            top_rows = _parse_response_rows(resp)

            # Get total revenue
            total_template = self.mu.get_query_template("query_customers_total")
            if total_template:
                resp2 = await self.source.query(total_template.format(
                    year=year, company_id=self.mu.company_id or ""
                ))
                total_rows = _parse_response_rows(resp2)
                total_rev = float(total_rows[0].get("total", 1)) if total_rows else 1
            else:
                total_rev = sum(float(r.get("revenue", 0)) for r in top_rows) or 1

            lines = [f"Top {top_n} customers by actual revenue {year} "
                     f"(total company: {total_rev:,.0f}):", ""]
            lines.append(f"  {'Rank':<5} {'ID':<13} {'Revenue':>14} {'Share':>7}  Name")
            lines.append("  " + "─" * 70)
            for rank, r in enumerate(top_rows, 1):
                cid   = int(r.get("customer_id", r.get("id", 0)))
                rev   = float(r.get("revenue", 0))
                share = rev / total_rev * 100
                name  = r.get("name", r.get("customer_name", "—"))
                lines.append(f"  {rank:<5} {cid:<13} {rev:>14,.0f} {share:>6.1f}%  {name}")

            return "\n".join(lines)

        return "No customer dimension configured in model understanding."

    # ── Main chat loop ────────────────────────────────────────────────────────

    async def chat(self, msg: str) -> str:
        """
        Send a user message, handle any tool calls, and return the agent's reply.

        - ```stage blocks accumulate adjustments in self.staged (no SQL yet)
        - ```apply blocks consume ALL of self.staged to generate and save one SQL file
        """
        if not self.rows:
            self.rows = cache_load()

        self.conv.append({"role": "user", "content": msg})

        while True:
            resp = self.ai.messages.create(
                model=SCENARIO_MODEL, max_tokens=1024,
                system=self._system_prompt,
                tools=self._tools,
                messages=self.conv,
            )
            self.conv.append({"role": "assistant", "content": resp.content})

            if resp.stop_reason == "tool_use":
                results = []
                for block in resp.content:
                    if block.type == "tool_use":
                        print(f"[Tool] {block.name}({block.input})")
                        try:
                            result = await self._handle_tool(block.name, block.input)
                        except Exception as e:
                            print(f"[Tool] Error in {block.name}: {e}")
                            result = f"Error executing {block.name}: {e}"
                        results.append({
                            "type":        "tool_result",
                            "tool_use_id": block.id,
                            "content":     result,
                        })
                self.conv.append({"role": "user", "content": results})
                continue

            # end_turn — extract text
            text = "".join(b.text for b in resp.content if hasattr(b, "text"))

            # ── Stage block: accumulate adjustments ──────────────────────────
            stage = extract_stage_block(text)
            if stage:
                self.staged.append(stage)
                adj_count = len(stage["adjustments"])
                total_adjs = sum(len(s["adjustments"]) for s in self.staged)
                print(f"[Stage] +{adj_count} adjustment(s). Total groups staged: {len(self.staged)}, total adjustments: {total_adjs}")
                clean = re.sub(r"```stage[\s\S]*?```", "", text).strip()
                return clean

            # ── Apply block: generate SQL from all staged adjustments ────────
            apply_spec = extract_apply_block(text)
            if apply_spec:
                if not self.staged:
                    return (re.sub(r"```apply[\s\S]*?```", "", text).strip() +
                            "\n\n⚠️  Nothing staged yet — describe your adjustments first.")
                if not self.rows:
                    return (re.sub(r"```apply[\s\S]*?```", "", text).strip() +
                            "\n\n⚠️  No baseline data loaded — please fetch data first.")

                # Flatten all staged adjustment groups
                all_adjs = []
                for s in self.staged:
                    all_adjs.extend(s["adjustments"])

                scenario_id = self.next_scenario_id
                print(f"[SQL] Applying {len(all_adjs)} adjustment(s) from "
                      f"{len(self.staged)} group(s) as scenario_id={scenario_id} ...")

                # Get model-specific params from ModelUnderstanding
                rev_accs = self.mu.revenue_accounts()
                cogs_accs = self.mu.cogs_accounts()
                target_table = self.mu.sql_target_table
                sql_columns = self.mu.sql_columns or None
                company_id = self.mu.company_id
                from config import OUTPUT_DIR
                output_dir = OUTPUT_DIR

                sc   = build_scenario(self.rows, all_adjs,
                                      revenue_accs=rev_accs,
                                      cogs_accs=cogs_accs,
                                      target_year=self.scenario_year)
                sql  = make_sql(sc, apply_spec["label"],
                                apply_spec.get("description", ""),
                                scenario_id=scenario_id,
                                target_table=target_table,
                                company_id=company_id,
                                columns=sql_columns)
                path = save_sql(sql, apply_spec["label"],
                                scenario_id=scenario_id,
                                output_dir=output_dir)
                dates = sorted({r["date"] for r in sc})

                # Advance counter and clear staging area for next scenario
                self.next_scenario_id += 1
                self.staged = []

                clean = re.sub(r"```apply[\s\S]*?```", "", text).strip()
                return (
                    f"{clean}\n\n"
                    f"✅ Scenario {scenario_id} SQL saved: {path}\n"
                    f"   {len(sc)} rows | {len(all_adjs)} adjustments | {', '.join(dates)}"
                )

            return text

    def reset(self):
        """Clear conversation history and staged adjustments (budget data is preserved)."""
        self.conv   = []
        self.staged = []
        print("[Agent] Conversation and staging area reset.")
