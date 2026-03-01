"""
config.py — Infrastructure configuration (model-agnostic).

All model-specific values (account sets, table names, etc.) live in the
ModelUnderstanding document. This file only contains infrastructure settings:
paths, ports, Claude model, and the PBI MCP executable location.
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
