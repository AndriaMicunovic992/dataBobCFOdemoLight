"""
ModelUnderstanding — The central data structure for model knowledge.

This replaces ALL hardcoded model-specific constants. It flows through:
  DiscoveryAgent → PromptBuilder → ScenarioAgent → queries.py → scenario.py

The JSON structure is designed to be:
  1. Produced by the DiscoveryAgent (LLM + human conversation)
  2. Persisted to SQLiteStorage
  3. Consumed by the PromptBuilder to generate dynamic system prompts
  4. Read by queries.py to run the right queries
  5. Read by scenario.py to build the right SQL
"""

from __future__ import annotations
import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ModelUnderstanding:
    """
    Parsed, validated model understanding with accessor methods.

    The raw dict is the source of truth — accessor methods provide
    typed convenience access to the most commonly used fields.
    """

    raw: dict = field(default_factory=dict)

    # ── Core Identity ──────────────────────────────────────────────────────

    @property
    def model_name(self) -> str:
        return self.raw.get("model_name", "Unknown Model")

    @property
    def domain(self) -> str:
        return self.raw.get("domain", "generic")

    @property
    def description(self) -> str:
        return self.raw.get("description", "")

    @property
    def status(self) -> str:
        """draft | reviewed | confirmed"""
        return self.raw.get("status", "draft")

    @status.setter
    def status(self, value: str):
        self.raw["status"] = value

    # ── Tables ─────────────────────────────────────────────────────────────

    @property
    def tables(self) -> dict[str, dict]:
        """Table metadata keyed by table name."""
        return self.raw.get("tables", {})

    def get_table(self, name: str) -> dict | None:
        return self.tables.get(name)

    def get_tables_by_role(self, role: str) -> list[str]:
        """Get table names by role: 'fact', 'dimension', 'bridge', 'lookup'."""
        return [name for name, meta in self.tables.items()
                if meta.get("role") == role]

    # ── Scenario Target ────────────────────────────────────────────────────

    @property
    def scenario_target(self) -> dict:
        return self.raw.get("scenario_target", {})

    @property
    def fact_table(self) -> str:
        return self.scenario_target.get("fact_table", "")

    @property
    def date_column(self) -> str:
        return self.scenario_target.get("date_column", "")

    @property
    def amount_columns(self) -> list[str]:
        return self.scenario_target.get("amount_columns", ["amount"])

    @property
    def scenario_type_column(self) -> str:
        return self.scenario_target.get("scenario_type_column", "")

    @property
    def scenario_type_values(self) -> dict:
        """e.g. {"actuals": 1, "budget": 2, "scenario_base": 3}"""
        return self.scenario_target.get("scenario_type_values", {})

    # ── Account Structures ─────────────────────────────────────────────────

    @property
    def account_structures(self) -> dict[str, dict]:
        """
        Multiple account structures keyed by purpose (e.g. "pl", "cf").
        Backward-compatible: reads from `account_structures` or falls back to
        wrapping legacy `account_structure` as {"pl": ...}.
        """
        multi = self.raw.get("account_structures")
        if multi:
            return multi
        # Legacy fallback: single account_structure → wrap as "pl"
        single = self.raw.get("account_structure", {})
        if single:
            return {"pl": single}
        return {}

    @property
    def account_structure(self) -> dict:
        """Primary (first) account structure. Backward-compatible accessor."""
        structs = self.account_structures
        if not structs:
            return {}
        # Return "pl" if available, otherwise first entry
        return structs.get("pl", next(iter(structs.values())))

    @property
    def account_table(self) -> str:
        return self.account_structure.get("account_table", "")

    @property
    def account_id_column(self) -> str:
        return self.account_structure.get("account_id_column", "")

    @property
    def account_name_column(self) -> str:
        return self.account_structure.get("account_name_column", "")

    @property
    def account_groups(self) -> dict[str, dict]:
        """
        Named account groups with IDs and descriptions.
        e.g. {"revenue": {"description": "...", "account_ids": [112, 114, ...]}}
        """
        return self.account_structure.get("groups", {})

    def revenue_accounts(self) -> set[int]:
        grp = self.account_groups.get("revenue", {})
        return set(grp.get("account_ids", []))

    def cogs_accounts(self) -> set[int]:
        grp = self.account_groups.get("cogs", {})
        return set(grp.get("account_ids", []))

    # ── Filter Dimensions ──────────────────────────────────────────────────

    @property
    def filter_dimensions(self) -> dict:
        return self.raw.get("filter_dimensions", {})

    @property
    def company_id(self) -> int | None:
        """Default company filter value, if any."""
        cf = self.filter_dimensions.get("company", {})
        val = cf.get("default_value")
        return int(val) if val is not None else None

    @property
    def company_column(self) -> str:
        cf = self.filter_dimensions.get("company", {})
        return cf.get("column", "")

    # ── Reporting Groups ───────────────────────────────────────────────────

    @property
    def reporting_groups(self) -> dict:
        return self.raw.get("reporting_groups", {})

    @property
    def pl_groups(self) -> set[str]:
        return set(self.reporting_groups.get("pl_groups", []))

    @property
    def bs_groups(self) -> set[str]:
        return set(self.reporting_groups.get("bs_groups", []))

    # ── Relationships ──────────────────────────────────────────────────────

    @property
    def relationships(self) -> list[dict]:
        return self.raw.get("relationships", [])

    def find_fk_column(self, from_table: str, to_table: str) -> str | None:
        """
        Find FK column linking from_table → to_table via relationships.

        Checks both directions since relationship direction varies:
        some models store fact→dim, others dim→fact.
        Returns the column name on from_table's side.
        """
        for rel in self.relationships:
            ft = rel.get("from_table", "")
            fc = rel.get("from_column", "")
            tt = rel.get("to_table", "")
            tc = rel.get("to_column", "")
            if ft == from_table and tt == to_table:
                return fc
            if tt == from_table and ft == to_table:
                return tc
        return None

    # ── Query Templates ────────────────────────────────────────────────────

    @property
    def query_language(self) -> str:
        return self.raw.get("query_language", "DAX")

    @property
    def query_templates(self) -> dict[str, str]:
        return self.raw.get("query_templates", {})

    def get_query_template(self, name: str) -> str | None:
        return self.query_templates.get(name)

    # ── SQL Generation Config ──────────────────────────────────────────────

    @property
    def sql_target(self) -> dict:
        """Config for SQL INSERT generation."""
        return self.raw.get("sql_target", {})

    @property
    def sql_target_table(self) -> str:
        return self.sql_target.get("table_name", self.fact_table)

    @property
    def sql_columns(self) -> list[str]:
        return self.sql_target.get("columns", [])

    # ── Cashflow ───────────────────────────────────────────────────────────

    @property
    def cashflow_config(self) -> dict:
        return self.raw.get("cashflow_config", {})

    @property
    def has_cashflow(self) -> bool:
        return bool(self.cashflow_config.get("structure_table"))

    # ── Customer Dimension ─────────────────────────────────────────────────

    @property
    def customer_config(self) -> dict:
        return self.raw.get("customer_config", {})

    @property
    def has_customer_dimension(self) -> bool:
        return bool(self.customer_config.get("customer_table"))

    # ── DAX Measures ──────────────────────────────────────────────────────

    @property
    def measures(self) -> dict[str, dict]:
        """DAX measures: {measure_name: {expression, table, description}}"""
        return self.raw.get("measures", {})

    @property
    def has_measures(self) -> bool:
        return bool(self.measures)

    # ── Serialization ──────────────────────────────────────────────────────

    def to_json(self) -> str:
        return json.dumps(self.raw, indent=2, ensure_ascii=False)

    @classmethod
    def from_json(cls, data: str) -> ModelUnderstanding:
        return cls(raw=json.loads(data))

    @classmethod
    def from_dict(cls, data: dict) -> ModelUnderstanding:
        return cls(raw=data)

    def update(self, patch: dict) -> None:
        """Deep merge a patch into the raw understanding."""
        _deep_merge(self.raw, patch)


def _deep_merge(base: dict, patch: dict) -> None:
    """Recursively merge patch into base dict."""
    for key, value in patch.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
