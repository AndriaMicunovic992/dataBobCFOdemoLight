#!/usr/bin/env python3
"""
Scenario Agent — Web Server
============================
Wraps the scenario and discovery agents in a Flask HTTP server.

Supports:
  - Power BI Desktop data sources
  - Excel file uploads
  - Combined PBI + Excel (composite) sources
  - Discovery agent for building model understanding
  - Scenario agent for financial scenario planning

Usage:
    pip install flask openpyxl duckdb
    python server.py
Then open http://localhost:5000 in your browser.
"""

import asyncio, os, sys, threading, json, webbrowser, shutil
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory

# Ensure the project directory is on sys.path
sys.path.insert(0, str(Path(__file__).parent))

from config import OUTPUT_DIR, HOST, PORT, STORAGE_DB, UPLOADS_DIR
from storage.sqlite_storage import SQLiteStorage
from datasources.base import DataSource
from datasources.pbi_desktop import PBIDesktopSource, list_pbi_instances
from datasources.factory import create_datasource, create_composite
from discovery.discovery_agent import DiscoveryAgent
from discovery.model_understanding import ModelUnderstanding
from scenario import build_scenario
from agent import Agent

app = Flask(__name__, static_folder=str(Path(__file__).parent))

# ── Shared state (thread-safe via asyncio lock) ──────────────────────────────
_loop   = asyncio.new_event_loop()
_lock   = threading.Lock()

_storage = SQLiteStorage(STORAGE_DB)

_source:          DataSource | None     = None   # current data source
_discovery_agent: DiscoveryAgent | None = None   # for Data Understanding tab
_scenario_agent:  Agent | None          = None   # for Scenario tab
_status = {"connected": False, "source_type": None, "message": "Not connected"}

# Cashflow structure cache (loaded once per PBI connection)
_cf_structure: list[dict] | None = None


def _run(coro):
    """Run a coroutine on the background event loop."""
    future = asyncio.run_coroutine_threadsafe(coro, _loop)
    return future.result(timeout=120)

def _start_loop():
    asyncio.set_event_loop(_loop)
    _loop.run_forever()

threading.Thread(target=_start_loop, daemon=True).start()


def _init_agents(source: DataSource, mu: ModelUnderstanding | None = None):
    """Initialize both agents for the given source."""
    global _discovery_agent, _scenario_agent
    _discovery_agent = DiscoveryAgent(source, _storage)
    _scenario_agent = Agent(source, mu)


def _load_mu_for_source(source: DataSource) -> ModelUnderstanding | None:
    """Try to load a saved ModelUnderstanding for the given source."""
    source_id = source.source_id()
    data = _storage.load_model_understanding(source_id)
    if not data:
        return None
    clean = {k: v for k, v in data.items() if not k.startswith("_")}
    return ModelUnderstanding.from_dict(clean)


# ── Routes: Static files ────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(str(Path(__file__).parent), "ui.html")


# ── Routes: PBI Desktop ─────────────────────────────────────────────────────

@app.route("/api/instances")
def get_instances():
    """
    Discover all open Power BI Desktop models.
    Returns a list of {display_name, connection_string, database, port}.
    """
    try:
        from config import POWERBI_EXE
        instances = _run(list_pbi_instances(POWERBI_EXE))
        return jsonify({"ok": True, "instances": instances})
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e),
                        "trace": traceback.format_exc(), "instances": []})


@app.route("/api/connect", methods=["POST"])
def connect():
    """
    Connect to a specific Power BI Desktop model.
    Body: { "connection_string": "...", "database": "..." }
    """
    global _source, _cf_structure
    with _lock:
        data     = request.get_json() or {}
        conn_str = data.get("connection_string", "").strip()
        db_guid  = data.get("database", "").strip()

        if not conn_str or not db_guid:
            return jsonify({"ok": False,
                            "message": "connection_string and database are required"}), 400

        try:
            from config import POWERBI_EXE
            source = PBIDesktopSource(POWERBI_EXE)
            _run(source.connect(connection_string=conn_str, database=db_guid))
            _source = source
            _cf_structure = None  # reset CF cache for new connection

            # Try to load existing model understanding
            mu = _load_mu_for_source(source)
            _init_agents(source, mu)

            _status["connected"]   = True
            _status["source_type"] = "pbi_desktop"
            _status["message"]     = "Connected to Power BI Desktop"
            return jsonify({"ok": True, "message": _status["message"],
                            "has_understanding": mu is not None})
        except Exception as e:
            _status["connected"]   = False
            _status["source_type"] = None
            _status["message"]     = str(e)
            # Still create agents so UI stays functional
            from config import POWERBI_EXE
            source = PBIDesktopSource(POWERBI_EXE)
            _source = source
            _init_agents(source)
            return jsonify({"ok": False, "message": f"Connection failed: {e}"})


# ── Routes: Excel Upload ─────────────────────────────────────────────────────

@app.route("/api/connect/excel", methods=["POST"])
def connect_excel():
    """
    Upload one or more Excel files as a data source.
    Accepts multipart/form-data with file fields.
    """
    global _source, _cf_structure
    with _lock:
        if "files" not in request.files and "file" not in request.files:
            return jsonify({"ok": False,
                            "message": "No files uploaded"}), 400

        files = request.files.getlist("files") or request.files.getlist("file")
        if not files:
            return jsonify({"ok": False, "message": "No files uploaded"}), 400

        # Save uploaded files
        UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
        saved_paths = []
        for f in files:
            if f.filename and f.filename.endswith((".xlsx", ".xls")):
                dest = UPLOADS_DIR / f.filename
                f.save(str(dest))
                saved_paths.append(dest)
                # Track in storage
                _storage.save_file(f.filename, "excel", str(dest))

        if not saved_paths:
            return jsonify({"ok": False,
                            "message": "No valid Excel files (.xlsx) found"}), 400

        try:
            source = create_datasource("excel", file_paths=saved_paths)
            _run(source.connect())
            _source = source
            _cf_structure = None

            mu = _load_mu_for_source(source)
            _init_agents(source, mu)

            _status["connected"]   = True
            _status["source_type"] = "excel"
            _status["message"]     = f"Connected to {len(saved_paths)} Excel file(s)"
            return jsonify({
                "ok": True,
                "message": _status["message"],
                "files": [p.name for p in saved_paths],
                "has_understanding": mu is not None,
            })
        except Exception as e:
            import traceback
            _status["connected"]   = False
            _status["source_type"] = None
            _status["message"]     = str(e)
            return jsonify({"ok": False, "message": f"Excel connection failed: {e}",
                            "trace": traceback.format_exc()})


# ── Routes: Status ───────────────────────────────────────────────────────────

@app.route("/api/status")
def status():
    return jsonify({
        "connected":          _status["connected"],
        "source_type":        _status.get("source_type"),
        "message":            _status["message"],
        "output_dir":         str(OUTPUT_DIR),
        "agent_ready":        _scenario_agent is not None,
        "has_understanding":  _has_confirmed_understanding(),
    })


def _has_confirmed_understanding() -> bool:
    if _source is None:
        return False
    data = _storage.load_model_understanding(_source.source_id())
    return data is not None and data.get("status") == "confirmed"


# ── Routes: Model Understanding ──────────────────────────────────────────────

@app.route("/api/model/understanding")
def get_understanding():
    """Return the current model understanding document."""
    with _lock:
        if _source is None:
            return jsonify({"ok": False, "data": None})
        data = _storage.load_model_understanding(_source.source_id())
        if not data:
            return jsonify({"ok": True, "data": None})
        clean = {k: v for k, v in data.items() if not k.startswith("_")}
        return jsonify({"ok": True, "data": clean})


@app.route("/api/model/status")
def model_status():
    """Check whether a confirmed understanding exists for the current source."""
    with _lock:
        if _source is None:
            return jsonify({"exists": False, "status": None})
        data = _storage.load_model_understanding(_source.source_id())
        if not data:
            return jsonify({"exists": False, "status": None})
        return jsonify({"exists": True, "status": data.get("status", "draft")})


@app.route("/api/model/refresh", methods=["POST"])
def refresh_understanding():
    """
    Reload model understanding and reinitialize the scenario agent.
    Call this after the discovery agent saves an updated understanding.
    """
    global _scenario_agent
    with _lock:
        if _source is None:
            return jsonify({"ok": False, "error": "No source connected"})
        mu = _load_mu_for_source(_source)
        if mu is not None:
            _scenario_agent = Agent(_source, mu)
            return jsonify({"ok": True, "status": "refreshed"})
        else:
            return jsonify({"ok": False, "error": "No understanding found"})


# ── Routes: Discovery Agent (Data Understanding tab) ─────────────────────────

@app.route("/api/discovery/chat", methods=["POST"])
def discovery_chat():
    """Chat endpoint for the discovery agent."""
    data = request.get_json()
    msg  = (data or {}).get("message", "").strip()
    if not msg:
        return jsonify({"ok": False, "error": "Empty message"}), 400

    with _lock:
        if _discovery_agent is None:
            return jsonify({"ok": False,
                            "error": "No data source connected. Connect to PBI or upload Excel first."}), 400
        try:
            reply = _run(_discovery_agent.chat(msg))
            return jsonify({"ok": True, "reply": reply})
        except Exception as e:
            import traceback
            return jsonify({"ok": False, "error": str(e),
                            "trace": traceback.format_exc()})


@app.route("/api/discovery/reset", methods=["POST"])
def discovery_reset():
    """Reset discovery agent conversation."""
    with _lock:
        if _discovery_agent:
            _discovery_agent.reset()
    return jsonify({"ok": True})


# ── Routes: Scenario Agent (Scenario tab) ────────────────────────────────────

@app.route("/api/chat", methods=["POST"])
def chat():
    global _scenario_agent, _source
    data = request.get_json()
    msg  = (data or {}).get("message", "").strip()
    if not msg:
        return jsonify({"ok": False, "error": "Empty message"}), 400

    with _lock:
        if _scenario_agent is None:
            # Auto-init with connected source or empty PBI source
            if _source is not None:
                mu = _load_mu_for_source(_source)
                _scenario_agent = Agent(_source, mu)
            else:
                # Legacy fallback — create empty PBI source
                from config import POWERBI_EXE
                _source = PBIDesktopSource(POWERBI_EXE)
                _scenario_agent = Agent(_source)

        try:
            reply = _run(_scenario_agent.chat(msg))
            sql_files = sorted(OUTPUT_DIR.glob("scenario_*.sql"),
                               key=lambda f: f.stat().st_mtime, reverse=True)
            latest_sql = str(sql_files[0]) if sql_files else None
            return jsonify({"ok": True, "reply": reply, "latest_sql": latest_sql})
        except Exception as e:
            import traceback
            return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()})


@app.route("/api/reset", methods=["POST"])
def reset():
    with _lock:
        if _scenario_agent:
            _scenario_agent.reset()
    return jsonify({"ok": True})


@app.route("/api/scenario/staged")
def get_staged():
    """Return the current staged adjustments and next scenario_id."""
    with _lock:
        if _scenario_agent is None:
            return jsonify({"staged": [], "next_id": 3, "adjustment_count": 0})
        return jsonify(_scenario_agent.get_staged())


@app.route("/api/scenario/clear", methods=["POST"])
def clear_staged():
    """Discard all staged adjustments without generating SQL."""
    with _lock:
        if _scenario_agent:
            _scenario_agent.clear_staged()
    return jsonify({"ok": True})


@app.route("/api/scenario/staged/<int:index>", methods=["DELETE"])
def remove_staged_step(index):
    """Remove a single staged step by its list index."""
    with _lock:
        if _scenario_agent is None:
            return jsonify({"ok": False, "error": "Agent not initialised"}), 400
        removed = _scenario_agent.remove_staged(index)
        if not removed:
            return jsonify({"ok": False, "error": f"No staged step at index {index}"}), 400
    return jsonify({"ok": True})


@app.route("/api/scenario/preview")
def scenario_preview():
    """
    Preview the GL impact of all staged adjustments.

    Applies adjustments to the in-memory budget rows and returns a pivot:
      accounts × months → {original, scenario, delta}
    plus column totals, row totals, and optionally cashflow impact.
    """
    with _lock:
        if _scenario_agent is None:
            return jsonify({"ok": False, "error": "Agent not initialised"})
        if not _scenario_agent.rows:
            return jsonify({"ok": False,
                            "error": "No budget data loaded — ask the agent to load the budget first."})
        if not _scenario_agent.staged:
            return jsonify({"ok": False, "error": "No adjustments staged yet."})

        # Get model params
        mu = _scenario_agent.mu
        rev_accs  = mu.revenue_accounts() if mu else None
        cogs_accs = mu.cogs_accounts() if mu else None

        # Flatten all staged groups into one adjustment list
        all_adjs = []
        for s in _scenario_agent.staged:
            all_adjs.extend(s["adjustments"])

        orig_rows = _scenario_agent.rows
        sc_rows   = build_scenario(orig_rows, all_adjs,
                                   revenue_accs=rev_accs,
                                   cogs_accs=cogs_accs)

        # Aggregate amounts by (account, YYYY-MM)
        orig_pivot: dict[tuple, float] = {}
        for r in orig_rows:
            key = (r["account"], r["date"][:7])
            orig_pivot[key] = orig_pivot.get(key, 0.0) + r["amount"]

        sc_pivot: dict[tuple, float] = {}
        for r in sc_rows:
            key = (r["account"], r["date"][:7])
            sc_pivot[key] = sc_pivot.get(key, 0.0) + r["amount"]

        account_ids = sorted({r["account"] for r in orig_rows})
        months      = sorted({r["date"][:7] for r in orig_rows})

        # Collect account metadata from enriched rows
        acc_meta: dict[int, dict] = {}
        for r in orig_rows:
            acc = r["account"]
            if acc not in acc_meta:
                acc_meta[acc] = {
                    "id":          acc,
                    "nr":          r.get("account_nr",   str(acc)),
                    "name":        r.get("account_name", f"Account {acc}"),
                    "grp":         r.get("account_grp",  ""),
                    "cf_position": r.get("cf_position",  0),
                }

        # Build per-account rows with per-month and total delta
        result_accounts = []
        for acc in account_ids:
            meta = acc_meta[acc]
            month_data = {}
            for m in months:
                key  = (acc, m)
                orig = round(orig_pivot.get(key, 0.0), 2)
                scen = round(sc_pivot.get(key, 0.0),   2)
                month_data[m] = {
                    "original": orig,
                    "scenario": scen,
                    "delta":    round(scen - orig, 2),
                }
            row_orig = sum(v["original"] for v in month_data.values())
            row_scen = sum(v["scenario"] for v in month_data.values())
            result_accounts.append({
                **meta,
                "months": month_data,
                "total":  {
                    "original": round(row_orig, 2),
                    "scenario": round(row_scen, 2),
                    "delta":    round(row_scen - row_orig, 2),
                },
            })

        # Column totals
        col_totals: dict[str, dict] = {}
        for m in months:
            c_orig = sum(orig_pivot.get((acc, m), 0.0) for acc in account_ids)
            c_scen = sum(sc_pivot.get((acc, m),  0.0) for acc in account_ids)
            col_totals[m] = {
                "original": round(c_orig, 2),
                "scenario": round(c_scen, 2),
                "delta":    round(c_scen - c_orig, 2),
            }

        grand_orig = sum(v["original"] for v in col_totals.values())
        grand_scen = sum(v["scenario"] for v in col_totals.values())
        col_totals["total"] = {
            "original": round(grand_orig, 2),
            "scenario": round(grand_scen, 2),
            "delta":    round(grand_scen - grand_orig, 2),
        }

        # ── Cashflow impact ──────────────────────────────────────────────────
        cf_result = None
        global _cf_structure
        try:
            if _cf_structure is None and _source is not None:
                # Try to fetch CF structure — only works for PBI sources
                if _status.get("source_type") == "pbi_desktop":
                    try:
                        from dax import fetch_cashflow_structure
                        from pbi_client import PBIClient
                        pbi_wrapper = PBIClient.__new__(PBIClient)
                        pbi_wrapper._source = _source
                        _cf_structure = _run(fetch_cashflow_structure(pbi_wrapper))
                    except Exception:
                        pass

            if _cf_structure:
                pos_orig:   dict[int, float] = {}
                pos_scen:   dict[int, float] = {}
                for r in result_accounts:
                    cfp = acc_meta.get(r["id"], {}).get("cf_position", 0)
                    if cfp:
                        pos_orig[cfp] = pos_orig.get(cfp, 0.0) + r["total"]["original"]
                        pos_scen[cfp] = pos_scen.get(cfp, 0.0) + r["total"]["scenario"]

                base_orig:   dict[int, float] = {}
                base_scen:   dict[int, float] = {}
                for cf in _cf_structure:
                    s = cf["sort"]
                    if cf["path_from"] == cf["path_to"]:
                        inv = -1 if cf["invert"] == 1 else 1
                        base_orig[s] = round(pos_orig.get(s, 0.0) * inv, 2)
                        base_scen[s] = round(pos_scen.get(s, 0.0) * inv, 2)

                cf_rows = []
                for cf in _cf_structure:
                    s = cf["sort"]
                    is_subtotal = cf["path_from"] != cf["path_to"]
                    if is_subtotal:
                        orig = round(sum(
                            base_orig.get(k, 0.0) for k in base_orig
                            if cf["path_from"] <= k <= cf["path_to"]
                        ), 2)
                        scen = round(sum(
                            base_scen.get(k, 0.0) for k in base_scen
                            if cf["path_from"] <= k <= cf["path_to"]
                        ), 2)
                    else:
                        orig = base_orig.get(s, 0.0)
                        scen = base_scen.get(s, 0.0)

                    cf_rows.append({
                        "sort":        s,
                        "display":     cf["display"],
                        "gruppe":      cf["gruppe"],
                        "original":    orig,
                        "scenario":    scen,
                        "delta":       round(scen - orig, 2),
                        "is_subtotal": is_subtotal,
                    })

                cf_result = cf_rows
        except Exception as e:
            print(f"[Preview] CF computation skipped: {e}")

        # Include pl_groups from model understanding for the UI
        pl_groups = None
        if mu is not None:
            pl_groups = mu.pl_groups

        return jsonify({
            "ok":        True,
            "months":    months,
            "accounts":  result_accounts,
            "totals":    col_totals,
            "cashflow":  cf_result,
            "pl_groups": pl_groups,
        })


# ── Routes: SQL Files ────────────────────────────────────────────────────────

@app.route("/api/files")
def list_files():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(OUTPUT_DIR.glob("scenario_*.sql"),
                   key=lambda f: f.stat().st_mtime, reverse=True)
    return jsonify([{
        "name":  f.name,
        "path":  str(f),
        "size":  f.stat().st_size,
        "mtime": f.stat().st_mtime,
    } for f in files[:20]])


@app.route("/api/file/<filename>")
def get_file(filename):
    f = OUTPUT_DIR / filename
    if not f.exists() or f.suffix != ".sql":
        return jsonify({"error": "Not found"}), 404
    return jsonify({"name": f.name, "content": f.read_text(encoding="utf-8")})


# ── Start ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERROR: set ANTHROPIC_API_KEY"); sys.exit(1)

    print(f"Starting Scenario Agent UI on http://{HOST}:{PORT}")
    print("Opening browser...")
    threading.Timer(1.0, lambda: webbrowser.open(f"http://{HOST}:{PORT}")).start()
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
