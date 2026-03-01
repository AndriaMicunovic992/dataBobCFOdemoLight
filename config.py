"""
config.py — Infrastructure configuration (model-agnostic).

All model-specific values (COMPANY_ID, account sets, table names) now live
in the ModelUnderstanding document. This file only contains infrastructure
settings: paths, ports, Claude model, and the PBI MCP executable location.

Legacy constants (COMPANY_ID, REVENUE_ACCS, COGS_ACCS) are preserved for
backward compatibility but should not be used in new code.
"""

import os
from pathlib import Path

# ── Claude model ──────────────────────────────────────────────────────────
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")

# ── File paths ────────────────────────────────────────────────────────────
DATA_DIR   = Path(os.environ.get("DATA_DIR",
                  str(Path(__file__).parent.resolve() / "data")))
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR",
                  str(Path(__file__).parent.resolve() / "output")))
UPLOADS_DIR = Path(os.environ.get("UPLOADS_DIR",
                   str(Path(__file__).parent.resolve() / "uploads")))

# Main storage DB — replaces the old temp-dir cache
STORAGE_DB = DATA_DIR / "scenario_agent.db"

# ── Power BI MCP executable (only for PBI Desktop source) ─────────────────
POWERBI_EXE = os.environ.get("POWERBI_MCP_EXE",
    r"C:\Users\andri\.vscode\extensions"
    r"\analysis-services.powerbi-modeling-mcp-0.1.9-win32-x64"
    r"\server\powerbi-modeling-mcp.exe"
)

# ── Server ────────────────────────────────────────────────────────────────
HOST = os.environ.get("HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "5000"))

# ── Legacy constants (backward compat — use ModelUnderstanding instead) ───
COMPANY_ID   = 4
REVENUE_ACCS = {112, 114, 118, 119, 120, 121, 122, 123, 124, 130}
COGS_ACCS    = {126, 127}

# Legacy cache DB path (kept for cache.py backward compat)
import tempfile
CACHE_DB = Path(tempfile.gettempdir()) / "finance_scenario_cache.db"
