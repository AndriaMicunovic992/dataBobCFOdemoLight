"""
config.py — Infrastructure configuration (model-agnostic).

All model-specific values (account sets, table names, etc.) live in the
ModelUnderstanding document. This file only contains infrastructure settings:
paths, ports, Claude model, and the PBI MCP executable location.
"""

import os
from pathlib import Path

# ── Load .env file if present (no external dependency) ───────────────
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _key, _, _val = _line.partition("=")
            _key = _key.strip()
            _val = _val.strip().strip("'\"")
            if _key:
                os.environ.setdefault(_key, _val)

# ── API Keys ─────────────────────────────────────────────────────────
DISCOVERY_API_KEY = os.environ.get(
    "DISCOVERY_API_KEY", os.environ.get("ANTHROPIC_API_KEY", ""))
SCENARIO_API_KEY = os.environ.get(
    "SCENARIO_API_KEY", os.environ.get("ANTHROPIC_API_KEY", ""))

# ── Claude models (per-agent) ────────────────────────────────────────
DISCOVERY_MODEL = os.environ.get("DISCOVERY_MODEL", "claude-sonnet-4-6")
SCENARIO_MODEL  = os.environ.get("SCENARIO_MODEL", "claude-sonnet-4-6")

# ── File paths ────────────────────────────────────────────────────────
DATA_DIR   = Path(os.environ.get("DATA_DIR",
                  str(Path(__file__).parent.resolve() / "data")))
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR",
                  str(Path(__file__).parent.resolve() / "output")))
UPLOADS_DIR = Path(os.environ.get("UPLOADS_DIR",
                   str(Path(__file__).parent.resolve() / "uploads")))

# Main storage DB — replaces the old temp-dir cache
STORAGE_DB = DATA_DIR / "scenario_agent.db"

# ── Power BI MCP executable (only for PBI Desktop source) ─────────────
# Set via POWERBI_MCP_EXE in .env or environment variable.
POWERBI_EXE = os.environ.get("POWERBI_MCP_EXE", "")

# ── Server ────────────────────────────────────────────────────────────
HOST = os.environ.get("HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "5000"))
