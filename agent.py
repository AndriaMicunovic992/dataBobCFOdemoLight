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
from queries import fetch_baseline
from scenario import build_scenario, make_sql, save_sql


# ── Helper functions ──────────────────────────────────────────────────────────

def data_summary(rows: list[dict],
                 mu: ModelUnderstanding) -> str:
    """
    Return a rich text summary of loaded baseline rows.

    Uses reporting_structures to group accounts into P&L sections.
    Falls back to legacy pl_groups if reporting_structures not available.
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

    ms = sorted(months)

    lines = [
        f"Data loaded: {len(rows)} rows | {len(acct_totals)} accounts "
        f"| {ms[0]} to {ms[-1]}",
        "",
    ]

    # Show reporting structure sections if available
    pl = mu.get_reporting_structure("pl")
    if pl:
        lines.append("P&L structure:")
        for sec in pl.get("sections", []):
            sec_name = sec["name"]
            if sec.get("type") == "subtotal":
                # Compute subtotal from referenced sections
                total = 0.0
                for ref in sec.get("sum_of", []):
                    ref_ids = mu.account_ids_for_section(ref)
                    for acc_id in ref_ids:
                        total += acct_totals.get(acc_id, 0.0)
                lines.append(f"  {sec_name:<30} {total:>14,.0f}  (subtotal)")
            else:
                sec_ids = set(sec.get("account_ids", []))
                total = sum(acct_totals.get(a, 0.0) for a in sec_ids)
                count = len(sec_ids & set(acct_totals.keys()))
                lines.append(f"  {sec_name:<30} {total:>14,.0f}  ({count} accounts)")
        lines.append("")
    else:
        # Legacy: show by account group
        pl_groups = mu.pl_groups
        if pl_groups:
            lines.append("Account groups:")
            for grp_name in sorted(pl_groups):
                grp_accts = {a for a in acct_totals if acct_meta[a]["grp"] == grp_name}
                total = sum(acct_totals[a] for a in grp_accts)
                lines.append(f"  {grp_name:<30} {total:>14,.0f}  ({len(grp_accts)} accounts)")
            lines.append("")

    # Account breakdown
    lines += [
        "Account breakdown:",
        f"  {'ID':>4}  {'GL-Nr.':>8}  {'Group':<26}  {'Name':<36}  {'Amount':>14}",
        "  " + "─" * 96,
    ]

    for acc in sorted(acct_totals.keys()):
        meta = acct_meta[acc]
        lines.append(
            f"  {acc:>4}  {meta['nr']:>8}  {meta['grp']:<26}  {meta['name']:<36}"
            f"  {acct_totals[acc]:>14,.0f}"
        )

    lines += ["", "Ready — tell me the adjustments to stage."]
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
        self.rows : list[dict] = []   # in-memory baseline data
        self.staged: list[dict] = []  # staged adjustment groups [{description, adjustments}]
        # Start scenario IDs after the highest known value_type_id
        stv = mu.scenario_type_values
        max_vt = max(stv.values()) if stv else 2
        self.next_scenario_id  = max_vt + 1  # increments with each applied scenario
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
            return data_summary(rows, self.mu)

        if name == "explore_data":
            query_text = inp.get("query", "")
            if not query_text:
                return "Error: query parameter is required."
            print(f"[Agent] explore_data: {query_text[:200]}...")
            try:
                resp = await self.source.query(query_text)
            except Exception as e:
                return f"Query error: {e}"
            if not resp.get("success"):
                return f"Query failed: {resp.get('message', 'unknown error')}"
            raw_rows = resp.get("data", {}).get("rows", [])
            if not raw_rows:
                return "Query returned no rows."
            # Normalize column names and limit output
            from queries import _parse_response_rows
            rows = _parse_response_rows(resp)
            if len(rows) > 200:
                rows = rows[:200]
            # Format as compact text table for the agent
            cols = list(rows[0].keys())
            lines = [" | ".join(cols)]
            for r in rows:
                lines.append(" | ".join(str(r.get(c, "")) for c in cols))
            return f"{len(rows)} rows returned:\n" + "\n".join(lines)

        return f"Unknown tool: {name}"

    def _build_dynamic_context(self) -> str:
        """Build a dynamic context section reflecting current UI selections."""
        from datetime import datetime as _dt
        parts = []
        if self.base_type:
            stv = self.mu.scenario_type_values
            vt_id = stv.get(self.base_type, "?")
            label = self.base_type.replace("_", " ").title()
            parts.append(f"Baseline type: {label} "
                         f"({self.mu.scenario_type_column}={vt_id})")

        bl_year = self.baseline_year or _dt.now().year
        sc_year = self.scenario_year or bl_year
        parts.append(f"Baseline year (Load): {bl_year}")
        parts.append(f"Scenario target year (Apply): {sc_year}")
        parts.append(f"When the user asks you to fetch data, use year={bl_year}. "
                     f"When generating SQL, adjustments target year {sc_year}.")

        if self.rows:
            parts.append(f"Data loaded: {len(self.rows)} rows in memory")
        else:
            parts.append("No data loaded yet — call run_query to fetch baseline")

        if self.staged:
            total_adj = sum(len(s["adjustments"]) for s in self.staged)
            parts.append(
                f"Staged: {len(self.staged)} groups, "
                f"{total_adj} total adjustments"
            )

        # Show available reporting structure section names
        sec_names = self.mu.all_reporting_section_names
        if sec_names:
            parts.append("Available account_group values for filters: "
                         + ", ".join(f'"{n}"' for n in sec_names))

        # Show available GL dimension columns
        dim_cols = self.mu.gl_dimension_columns
        if dim_cols:
            parts.append("Available dimension filter columns: "
                         + ", ".join(dim_cols))

        return "== CURRENT SESSION STATE ==\n" + "\n".join(parts)

    # ── Main chat loop ────────────────────────────────────────────────────────

    async def chat(self, msg: str) -> str:
        """
        Send a user message, handle any tool calls, and return the agent's reply.

        - ```stage blocks accumulate adjustments in self.staged (no SQL yet)
        - ```apply blocks consume ALL of self.staged to generate and save one SQL file
        """
        self.conv.append({"role": "user", "content": msg})

        # Augment system prompt with current session state
        system_prompt = self._system_prompt + "\n\n" + self._build_dynamic_context()

        while True:
            resp = self.ai.messages.create(
                model=SCENARIO_MODEL, max_tokens=1024,
                system=system_prompt,
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
                            "\n\nNothing staged yet — describe your adjustments first.")
                if not self.rows:
                    return (re.sub(r"```apply[\s\S]*?```", "", text).strip() +
                            "\n\nNo baseline data loaded — please fetch data first.")

                # Flatten all staged adjustment groups
                all_adjs = []
                for s in self.staged:
                    all_adjs.extend(s["adjustments"])

                scenario_id = self.next_scenario_id
                print(f"[SQL] Applying {len(all_adjs)} adjustment(s) from "
                      f"{len(self.staged)} group(s) as scenario_id={scenario_id} ...")

                # Get model-specific params from ModelUnderstanding
                target_table = self.mu.sql_target_table
                sql_columns = self.mu.sql_columns or None
                from config import OUTPUT_DIR
                output_dir = OUTPUT_DIR

                sc   = build_scenario(self.rows, all_adjs,
                                      mu=self.mu,
                                      target_year=self.scenario_year)
                sql  = make_sql(sc, apply_spec["label"],
                                apply_spec.get("description", ""),
                                scenario_id=scenario_id,
                                target_table=target_table,
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
                    f"Scenario {scenario_id} SQL saved: {path}\n"
                    f"   {len(sc)} rows | {len(all_adjs)} adjustments | {', '.join(dates)}"
                )

            return text

    def reset(self):
        """Clear conversation history and staged adjustments (baseline data is preserved)."""
        self.conv   = []
        self.staged = []
        print("[Agent] Conversation and staging area reset.")
