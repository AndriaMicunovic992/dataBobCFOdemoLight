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

_source:           DataSource | None     = None   # current data source
_discovery_agent:  DiscoveryAgent | None = None   # for Data Understanding tab
_scenario_agent:   Agent | None          = None   # for Scenario tab
_current_model_id: str | None            = None   # active model entity
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


def _init_agents(source: DataSource, mu: ModelUnderstanding | None = None,
                 model_id: str | None = None):
    """Initialize both agents for the given source."""
    global _discovery_agent, _scenario_agent
    _discovery_agent = DiscoveryAgent(source, _storage, model_id=model_id)
    if mu is not None:
        try:
            _scenario_agent = Agent(source, mu)
        except Exception as e:
            print(f"[Init] Scenario agent init failed: {e}")
            _scenario_agent = None
    else:
        _scenario_agent = None


def _refresh_scenario_agent(source: DataSource, mu: ModelUnderstanding):
    """Reinitialize scenario agent with new MU while preserving in-flight state."""
    global _scenario_agent
    old = _scenario_agent
    _scenario_agent = Agent(source, mu)
    if old is not None:
        _scenario_agent.rows = old.rows
        _scenario_agent.staged = old.staged
        _scenario_agent.next_scenario_id = old.next_scenario_id
        _scenario_agent.conv = old.conv
    # Re-apply server globals
    _scenario_agent.base_type = _scenario_base_type
    _scenario_agent.baseline_year = _baseline_year
    _scenario_agent.scenario_year = _scenario_year


def _try_reconnect_sources(model_id: str) -> DataSource | None:
    """Attempt to reconnect to a model's data sources. Returns DataSource or None."""
    sources = _storage.get_model_sources(model_id)
    for src in sources:
        src_type = src.get("source_type", "")
        source_id = src.get("source_id", "")

        if src_type == "pbi_desktop":
            db_guid = source_id.replace("pbi:", "") if source_id.startswith("pbi:") else ""
            if not db_guid:
                continue
            try:
                from config import POWERBI_EXE
                instances = _run(list_pbi_instances(POWERBI_EXE))
                for inst in instances:
                    if inst.get("database", "") == db_guid:
                        pbi_src = PBIDesktopSource(POWERBI_EXE)
                        _run(pbi_src.connect(
                            connection_string=inst["connection_string"],
                            database=db_guid
                        ))
                        print(f"[Reconnect] PBI matched: {inst.get('display_name')}")
                        return pbi_src
            except Exception as e:
                print(f"[Reconnect] PBI scan failed: {e}")

        elif src_type == "excel":
            try:
                file_paths = []
                with _storage._conn() as con:
                    rows = con.execute(
                        "SELECT file_path FROM uploaded_files WHERE model_id = ?",
                        (model_id,)
                    ).fetchall()
                for row in rows:
                    p = Path(row[0])
                    if p.exists():
                        file_paths.append(str(p))
                if file_paths:
                    excel_src = create_datasource("excel")
                    _run(excel_src.connect(files=file_paths))
                    print(f"[Reconnect] Excel: {len(file_paths)} file(s)")
                    return excel_src
            except Exception as e:
                print(f"[Reconnect] Excel failed: {e}")

    return None


def _load_mu(source: DataSource,
             model_id: str | None = None) -> ModelUnderstanding | None:
    """Load ModelUnderstanding.  Prefer *model_id*, fall back to *source_id*."""
    data = None
    if model_id:
        data = _storage.load_model_understanding_by_model(model_id)
    if not data and not model_id:
        # Only fall back to source_id when there's no model_id.
        # If model_id is set but has no understanding, the model genuinely
        # has no understanding yet — don't "borrow" from another model.
        data = _storage.load_model_understanding(source.source_id())
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
        if not POWERBI_EXE:
            return jsonify({
                "ok": False,
                "error": "POWERBI_MCP_EXE not configured. Set it in .env or as an environment variable.",
                "instances": [],
            })
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
    Body: { "connection_string": "...", "database": "...", "model_id": "..." }
    """
    global _source, _cf_structure, _current_model_id
    with _lock:
        data     = request.get_json() or {}
        conn_str = data.get("connection_string", "").strip()
        db_guid  = data.get("database", "").strip()
        model_id = data.get("model_id") or None  # optional — caller may specify

        if not conn_str or not db_guid:
            return jsonify({"ok": False,
                            "message": "connection_string and database are required"}), 400

        try:
            from config import POWERBI_EXE
            source = PBIDesktopSource(POWERBI_EXE)
            _run(source.connect(connection_string=conn_str, database=db_guid))
            source._display_name = data.get("display_name", conn_str)
            _source = source
            _cf_structure = None  # reset CF cache for new connection

            # Auto-detect existing model if model_id not provided
            matched_model = None
            if not model_id:
                matched_model = _storage.find_model_by_source_id(source.source_id())
                if matched_model:
                    model_id = matched_model["id"]

            _current_model_id = model_id

            # Try to load existing model understanding
            mu = _load_mu(source, model_id=model_id)
            _init_agents(source, mu, model_id=model_id)

            if model_id:
                _storage.touch_model(model_id)

            _status["connected"]   = True
            _status["source_type"] = "pbi_desktop"
            _status["message"]     = "Connected to Power BI Desktop"
            return jsonify({
                "ok": True,
                "message": _status["message"],
                "has_understanding": mu is not None,
                "model_id": model_id,
                "matched_model": matched_model,
            })
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
    Accepts multipart/form-data with file fields and optional model_id.
    """
    global _source, _cf_structure, _current_model_id
    with _lock:
        if "files" not in request.files and "file" not in request.files:
            return jsonify({"ok": False,
                            "message": "No files uploaded"}), 400

        files = request.files.getlist("files") or request.files.getlist("file")
        if not files:
            return jsonify({"ok": False, "message": "No files uploaded"}), 400

        model_id = request.form.get("model_id") or None

        # Save uploaded files
        UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
        saved_paths = []
        for f in files:
            if f.filename and f.filename.endswith((".xlsx", ".xls")):
                dest = UPLOADS_DIR / f.filename
                f.save(str(dest))
                saved_paths.append(dest)
                # Track in storage (proper method for externally saved files)
                _storage.track_uploaded_file(
                    f.filename, "excel", str(dest), model_id=model_id
                )

        if not saved_paths:
            return jsonify({"ok": False,
                            "message": "No valid Excel files (.xlsx) found"}), 400

        try:
            source = create_datasource("excel", file_paths=saved_paths)
            _run(source.connect())
            _source = source
            _cf_structure = None

            # Auto-detect existing model if model_id not provided
            matched_model = None
            if not model_id:
                matched_model = _storage.find_model_by_source_id(source.source_id())
                if matched_model:
                    model_id = matched_model["id"]

            _current_model_id = model_id

            mu = _load_mu(source, model_id=model_id)
            _init_agents(source, mu, model_id=model_id)

            if model_id:
                _storage.touch_model(model_id)

            _status["connected"]   = True
            _status["source_type"] = "excel"
            _status["message"]     = f"Connected to {len(saved_paths)} Excel file(s)"
            return jsonify({
                "ok": True,
                "message": _status["message"],
                "files": [p.name for p in saved_paths],
                "has_understanding": mu is not None,
                "model_id": model_id,
                "matched_model": matched_model,
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
        "model_id":           _current_model_id,
    })


def _has_confirmed_understanding() -> bool:
    if _source is None:
        return False
    data = None
    if _current_model_id:
        data = _storage.load_model_understanding_by_model(_current_model_id)
    if not data and not _current_model_id:
        data = _storage.load_model_understanding(_source.source_id())
    return data is not None and data.get("status") == "confirmed"


# ── Routes: Model Understanding ──────────────────────────────────────────────

@app.route("/api/model/understanding")
def get_understanding():
    """Return the current model understanding document."""
    with _lock:
        if _source is None:
            return jsonify({"ok": False, "data": None})
        data = None
        if _current_model_id:
            data = _storage.load_model_understanding_by_model(_current_model_id)
        if not data and not _current_model_id:
            data = _storage.load_model_understanding(_source.source_id())
        if not data:
            return jsonify({"ok": True, "data": None})
        clean = {k: v for k, v in data.items() if not k.startswith("_")}
        return jsonify({"ok": True, "data": clean})


@app.route("/api/model/status", methods=["GET"])
def model_status():
    """Check whether a confirmed understanding exists for the current model."""
    with _lock:
        if _source is None:
            return jsonify({"exists": False, "status": None})
        data = None
        if _current_model_id:
            data = _storage.load_model_understanding_by_model(_current_model_id)
        if not data and not _current_model_id:
            data = _storage.load_model_understanding(_source.source_id())
        if not data:
            return jsonify({"exists": False, "status": None})
        return jsonify({"exists": True, "status": data.get("status", "draft")})


@app.route("/api/model/status", methods=["POST"])
def update_model_status():
    """User-controlled status toggle (draft ↔ confirmed)."""
    global _scenario_agent
    data = request.get_json() or {}
    new_status = data.get("status", "").strip()
    if new_status not in ("draft", "confirmed"):
        return jsonify({"ok": False, "error": "status must be 'draft' or 'confirmed'"}), 400

    with _lock:
        # Load existing understanding
        mu_data = None
        if _current_model_id:
            mu_data = _storage.load_model_understanding_by_model(_current_model_id)
        if not mu_data and _source:
            mu_data = _storage.load_model_understanding(_source.source_id())
        if not mu_data:
            return jsonify({"ok": False, "error": "No model understanding found"}), 404

        meta = mu_data.get("_meta", {})
        clean = {k: v for k, v in mu_data.items() if not k.startswith("_")}
        clean["status"] = new_status

        _storage.save_model_understanding(
            meta.get("source_id", _source.source_id() if _source else ""),
            clean,
            source_type=meta.get("source_type", ""),
            model_id=_current_model_id,
        )

        # Reinit scenario agent if confirming
        if new_status == "confirmed" and _source:
            mu = _load_mu(_source, model_id=_current_model_id)
            if mu:
                _scenario_agent = Agent(_source, mu)

        return jsonify({"ok": True, "status": new_status})


@app.route("/api/model/overview")
def model_overview():
    """Return a structured UI-friendly view of the ModelUnderstanding."""
    with _lock:
        data = None
        if _current_model_id:
            data = _storage.load_model_understanding_by_model(_current_model_id)
        if not data and _source:
            data = _storage.load_model_understanding(_source.source_id())

        clean = {k: v for k, v in data.items() if not k.startswith("_")} if data else {}
        tables = clean.get("tables", {})

        # Sources: from model links + currently connected source
        sources = []
        linked_source_ids = set()
        if _current_model_id:
            for s in _storage.get_model_sources(_current_model_id):
                sid = s.get("source_id", "")
                linked_source_ids.add(sid)
                sources.append({
                    "link_id": s.get("id"),
                    "label": s.get("label", s.get("source_type", "")),
                    "source_type": s.get("source_type", ""),
                    "source_id": sid,
                    "connected": (_source is not None and
                                  _source.source_id() == sid),
                })
        # Always show the currently connected source even if not linked
        if _source is not None and _source.source_id() not in linked_source_ids:
            sources.append({
                "link_id": "",
                "label": _source.display_name(),
                "source_type": _source.source_type(),
                "source_id": _source.source_id(),
                "connected": True,
            })

        if not data and not sources:
            return jsonify({"ok": True, "data": None})

        overview = {
            "model_name": clean.get("model_name", ""),
            "description": clean.get("description", ""),
            "status": clean.get("status", "draft"),
            "query_language": clean.get("query_language", ""),
            "fact_tables": [
                {"name": n, "description": m.get("description", "")}
                for n, m in tables.items() if m.get("role") == "fact"
            ],
            "dimension_tables": [
                {"name": n, "description": m.get("description", "")}
                for n, m in tables.items() if m.get("role") == "dimension"
            ],
            "relationships": clean.get("relationships", []),
            "query_templates": list(clean.get("query_templates", {}).keys()),
            "account_structures": clean.get("account_structures", {}),
            "account_structure": clean.get("account_structure", {}),
            "filter_dimensions": list(clean.get("filter_dimensions", {}).keys()),
            "sources": sources,
            "measures": [
                {"name": name, "table": info.get("table", ""),
                 "expression": info.get("expression", "")}
                for name, info in clean.get("measures", {}).items()
            ],
        }
        return jsonify({"ok": True, "data": overview})


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
        mu = _load_mu(_source, model_id=_current_model_id)
        if mu is not None:
            _refresh_scenario_agent(_source, mu)
            return jsonify({"ok": True, "status": "refreshed"})
        else:
            return jsonify({"ok": False, "error": "No understanding found"})


@app.route("/api/model/understanding/patch", methods=["POST"])
def patch_understanding():
    """Apply a partial update to the current model understanding."""
    global _scenario_agent
    with _lock:
        if _source is None:
            return jsonify({"ok": False, "error": "No source connected"}), 400

        patch = request.get_json()
        if not patch:
            return jsonify({"ok": False, "error": "Empty patch"}), 400

        # Load current understanding
        mu_data = None
        if _current_model_id:
            mu_data = _storage.load_model_understanding_by_model(_current_model_id)
        if not mu_data and _source:
            mu_data = _storage.load_model_understanding(_source.source_id())
        if not mu_data:
            return jsonify({"ok": False, "error": "No model understanding found"}), 404

        meta = mu_data.get("_meta", {})
        clean = {k: v for k, v in mu_data.items() if not k.startswith("_")}

        # Each key in the patch represents a full section replacement.
        # Direct assignment instead of deep merge so that deletions
        # (e.g. removing a table) are properly reflected.
        for key, value in patch.items():
            clean[key] = value

        _storage.save_model_understanding(
            meta.get("source_id", _source.source_id() if _source else ""),
            clean,
            source_type=meta.get("source_type", ""),
            model_id=_current_model_id,
        )

        # Refresh scenario agent (preserving in-flight state)
        mu = _load_mu(_source, model_id=_current_model_id)
        if mu is not None:
            try:
                _refresh_scenario_agent(_source, mu)
            except Exception as e:
                print(f"[Patch] Scenario agent refresh failed: {e}")

        return jsonify({"ok": True})


# ── Routes: Schema Cache ─────────────────────────────────────────────────────

@app.route("/api/schema/cached")
def get_cached_schema():
    """Return the cached raw schema from the discovery agent (if available)."""
    with _lock:
        if _discovery_agent and _discovery_agent._schema_cache:
            schema = _discovery_agent._schema_cache
            # Return a slimmed-down version for the UI pickers
            tables = []
            for t in schema.get("tables", []):
                tables.append({
                    "name": t.get("name", ""),
                    "columns": [c.get("name", "") for c in t.get("columns", [])],
                    "is_hidden": t.get("is_hidden", False),
                })
            relationships = schema.get("relationships", [])
            measures = schema.get("measures", [])
            return jsonify({
                "ok": True,
                "tables": tables,
                "relationships": relationships,
                "measures": measures,
            })
        return jsonify({"ok": True, "tables": [], "relationships": [], "measures": []})


# ── Routes: Scenario Base Types ──────────────────────────────────────────────

_scenario_base_type: str | None = None  # auto-detect from ModelUnderstanding
_baseline_year: int | None = None      # year of data to load as baseline
_scenario_year: int | None = None      # year the scenario adjustments apply to


@app.route("/api/scenario/base-types")
def get_base_types():
    """Return available scenario base types from ModelUnderstanding."""
    with _lock:
        mu = None
        if _source:
            mu = _load_mu(_source, model_id=_current_model_id)
        if not mu:
            return jsonify({"ok": True, "types": [], "active": None})

        stv = mu.scenario_type_values
        types = [
            {"key": k, "value": v, "label": k.replace("_", " ").title()}
            for k, v in stv.items()
        ]
        # Auto-detect: prefer "actuals", then first available key
        active = _scenario_base_type
        if active is None and stv:
            active = "actuals" if "actuals" in stv else next(iter(stv))
        return jsonify({"ok": True, "types": types, "active": active})


@app.route("/api/scenario/set-base", methods=["POST"])
def set_base_type():
    """Set the active base type for scenario queries."""
    global _scenario_base_type
    with _lock:
        data = request.get_json() or {}
        _scenario_base_type = data.get("base_type") or None
        # Reset rows so next query uses the new base
        if _scenario_agent:
            _scenario_agent.rows = []
        return jsonify({"ok": True, "active": _scenario_base_type})


@app.route("/api/scenario/year")
def get_scenario_year():
    """Return the current baseline and scenario year settings."""
    from datetime import datetime
    current_year = datetime.now().year
    return jsonify({
        "ok": True,
        "baseline_year": _baseline_year or current_year,
        "scenario_year": _scenario_year or current_year,
    })


@app.route("/api/scenario/set-year", methods=["POST"])
def set_scenario_year():
    """Set baseline and/or scenario year. Clears data only when baseline year changes."""
    global _baseline_year, _scenario_year
    with _lock:
        data = request.get_json() or {}
        old_baseline = _baseline_year

        if "baseline_year" in data:
            _baseline_year = int(data["baseline_year"]) or None
        if "scenario_year" in data:
            _scenario_year = int(data["scenario_year"]) or None

        # Only clear loaded data when the baseline year changes
        # (scenario year changes don't require reloading data)
        if _baseline_year != old_baseline and _scenario_agent:
            _scenario_agent.rows = []

        return jsonify({
            "ok": True,
            "baseline_year": _baseline_year,
            "scenario_year": _scenario_year,
        })


# ── Routes: Model CRUD ────────────────────────────────────────────────────────

@app.route("/api/models")
def list_models():
    """List all saved models (most recently accessed first)."""
    models = _storage.list_models()
    # Enrich each model with its understanding status
    for m in models:
        mu_data = _storage.load_model_understanding_by_model(m["id"])
        if not mu_data:
            # Fallback: check linked sources for an understanding by source_id
            sources = _storage.get_model_sources(m["id"])
            for src in sources:
                sid = src.get("source_id", "")
                if sid:
                    mu_data = _storage.load_model_understanding(sid)
                    if mu_data:
                        # Retroactively link this understanding to the model
                        _storage.link_understanding_to_model(sid, m["id"])
                        break
        m["understanding_status"] = (
            mu_data.get("status", "draft") if mu_data else None
        )
    return jsonify({"ok": True, "models": models})


@app.route("/api/models", methods=["POST"])
def create_model():
    """Create a new model.  Body: { "name": "...", "description": "..." }"""
    data = request.get_json() or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "name is required"}), 400
    source_type = data.get("source_type", _status.get("source_type") or "")
    model_id = _storage.create_model(
        name, source_type=source_type,
        description=data.get("description", ""),
    )
    # If we have a connected source, link it to the new model
    if _source is not None:
        sid = _source.source_id()
        _storage.add_model_source(
            model_id, _source.source_type(), sid,
            label=name,
            config={"source_id": sid},
        )
        # NOTE: Do NOT link existing understandings here. A new model should
        # start with a blank understanding. The old model's understanding
        # belongs to the old model, not the new one.
    return jsonify({"ok": True, "model_id": model_id})


@app.route("/api/models/<model_id>")
def get_model(model_id):
    """Get model details + linked sources."""
    model = _storage.get_model(model_id)
    if not model:
        return jsonify({"ok": False, "error": "Model not found"}), 404
    sources = _storage.get_model_sources(model_id)
    mu_data = _storage.load_model_understanding_by_model(model_id)
    model["sources"] = sources
    model["understanding_status"] = (
        mu_data.get("status", "draft") if mu_data else None
    )
    return jsonify({"ok": True, "model": model})


@app.route("/api/models/<model_id>", methods=["PUT"])
def update_model(model_id):
    """Update model name/description.  Body: { "name": "...", "description": "..." }"""
    data = request.get_json() or {}
    _storage.update_model(model_id, **data)
    return jsonify({"ok": True})


@app.route("/api/models/<model_id>", methods=["DELETE"])
def delete_model(model_id):
    """Delete a model and unlink its sources/understandings."""
    _storage.delete_model(model_id)
    global _current_model_id
    if _current_model_id == model_id:
        _current_model_id = None
    return jsonify({"ok": True})


@app.route("/api/models/<model_id>/activate", methods=["POST"])
def activate_model(model_id):
    """
    Activate a model — set it as current, load its understanding,
    try to reconnect data sources, and reinitialise agents.
    """
    global _current_model_id, _scenario_agent, _source, _cf_structure
    global _scenario_base_type, _baseline_year, _scenario_year
    with _lock:
        model = _storage.get_model(model_id)
        if not model:
            return jsonify({"ok": False, "error": "Model not found"}), 404

        # Reset runtime parameters for the new model
        _scenario_base_type = None
        _baseline_year = None
        _scenario_year = None

        _current_model_id = model_id
        _storage.touch_model(model_id)

        # Retroactively link orphan understandings via source_ids
        sources = _storage.get_model_sources(model_id)
        for src in sources:
            sid = src.get("source_id", "")
            if sid:
                _storage.link_understanding_to_model(sid, model_id)

        # Try to reconnect data sources
        reconnected = False
        if _source is None or not _status.get("connected"):
            try:
                new_source = _try_reconnect_sources(model_id)
                if new_source:
                    _source = new_source
                    _cf_structure = None
                    _status.update({
                        "connected": True,
                        "source_type": new_source.source_type(),
                        "message": f"Reconnected ({new_source.source_type()})",
                    })
                    reconnected = True
            except Exception as e:
                print(f"[Activate] Reconnect failed: {e}")

        mu = None
        if _source is not None:
            mu = _load_mu(_source, model_id=model_id)
            _init_agents(_source, mu, model_id=model_id)

        return jsonify({
            "ok": True,
            "model": model,
            "has_understanding": mu is not None,
            "reconnected": reconnected,
            "connected": _status.get("connected", False),
        })


@app.route("/api/models/<model_id>/link-source", methods=["POST"])
def link_source_to_model(model_id):
    """Link the currently connected source to a model."""
    with _lock:
        model = _storage.get_model(model_id)
        if not model:
            return jsonify({"ok": False, "error": "Model not found"}), 404
        if _source is None:
            return jsonify({"ok": False, "error": "No source connected"}), 400

        data = request.get_json() or {}
        label = data.get("label", _source.source_type())
        sid = _source.source_id()
        link_id = _storage.add_model_source(
            model_id, _source.source_type(), sid,
            label=label,
            config={"source_id": sid},
        )

        global _current_model_id
        _current_model_id = model_id
        _storage.touch_model(model_id)

        # Reinitialise agents with model context
        mu = _load_mu(_source, model_id=model_id)
        _init_agents(_source, mu, model_id=model_id)

        return jsonify({"ok": True, "link_id": link_id})


@app.route("/api/models/<model_id>/sources/<link_id>", methods=["DELETE"])
def remove_model_source_link(model_id, link_id):
    """Unlink a data source from a model."""
    with _lock:
        model = _storage.get_model(model_id)
        if not model:
            return jsonify({"ok": False, "error": "Model not found"}), 404
        _storage.remove_model_source(link_id)
        return jsonify({"ok": True})


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

            # Auto-refresh scenario agent — preserve rows/staged/conv
            mu = _load_mu(_source, model_id=_current_model_id)
            if mu is not None:
                try:
                    _refresh_scenario_agent(_source, mu)
                except Exception:
                    pass  # MU may still be incomplete during discovery

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
            # Auto-init with connected source
            if _source is not None:
                mu = _load_mu(_source, model_id=_current_model_id)
                if mu is None:
                    return jsonify({
                        "ok": False,
                        "error": "No model understanding found. "
                                 "Go to Data Understanding first."
                    }), 400
                _scenario_agent = Agent(_source, mu)
            else:
                return jsonify({
                    "ok": False,
                    "error": "No data source connected. "
                             "Click Data Source to connect first."
                }), 400

        try:
            # Apply base type and year overrides
            _scenario_agent.base_type = _scenario_base_type
            _scenario_agent.baseline_year = _baseline_year
            _scenario_agent.scenario_year = _scenario_year
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

    Applies adjustments to the in-memory baseline rows and returns a pivot:
      accounts × months → {original, scenario, delta}
    plus column totals, row totals, and optionally cashflow impact.
    """
    with _lock:
        if _scenario_agent is None:
            return jsonify({"ok": False, "error": "Agent not initialised"})
        if not _scenario_agent.rows:
            return jsonify({"ok": False,
                            "error": "No baseline data loaded — ask the agent to fetch data first."})
        if not _scenario_agent.staged:
            return jsonify({"ok": False, "error": "No adjustments staged yet."})

        mu = _scenario_agent.mu
        rev_accs  = mu.revenue_accounts() if mu else None
        cogs_accs = mu.cogs_accounts() if mu else None

        print(f"[Preview] revenue_accounts={rev_accs}, cogs_accounts={cogs_accs}")

        # Flatten all staged groups into one adjustment list
        all_adjs = []
        for s in _scenario_agent.staged:
            all_adjs.extend(s["adjustments"])

        print(f"[Preview] Applying {len(all_adjs)} adjustment(s) to {len(_scenario_agent.rows)} rows")

        orig_rows = _scenario_agent.rows
        sc_rows   = build_scenario(orig_rows, all_adjs,
                                   revenue_accs=rev_accs,
                                   cogs_accs=cogs_accs,
                                   target_year=_scenario_year)

        # Diagnostic: check if adjustments actually changed anything
        changed_count = sum(1 for o, s in zip(orig_rows, sc_rows)
                           if abs(o["amount"] - s["amount"]) > 0.001)
        print(f"[Preview] Rows changed: {changed_count} / {len(orig_rows)}")

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
                # Cashflow structure should come from ModelUnderstanding
                # if the model has cashflow_config defined
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
            pl_groups = sorted(mu.pl_groups) if mu.pl_groups else None

        # Collect all actual account groups from the data
        actual_groups = sorted({
            meta.get("grp", "") for meta in acc_meta.values()
            if meta.get("grp")
        }) if acc_meta else []

        # Fallback: derive pl_groups from actual account data if MU has none
        if pl_groups is None and actual_groups:
            pl_groups = actual_groups

        # Diagnostic: check if pl_groups actually matches the data
        if pl_groups and actual_groups:
            matched = set(pl_groups) & set(actual_groups)
            unmatched_pl = set(pl_groups) - set(actual_groups)
            if not matched:
                print(f"[Preview] WARNING: pl_groups {pl_groups} do NOT match any "
                      f"account groups in data {actual_groups}")
            elif unmatched_pl:
                print(f"[Preview] Some pl_groups not in data: {unmatched_pl}")

        try:
            return jsonify({
                "ok":        True,
                "months":    months,
                "accounts":  result_accounts,
                "totals":    col_totals,
                "cashflow":  cf_result,
                "pl_groups": pl_groups,
            })
        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({"ok": False, "error": f"Preview failed: {str(e)}"})


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
    from config import DISCOVERY_API_KEY, SCENARIO_API_KEY
    if not (DISCOVERY_API_KEY or SCENARIO_API_KEY):
        print("ERROR: set at least one API key.")
        print("  Use ANTHROPIC_API_KEY, or DISCOVERY_API_KEY + SCENARIO_API_KEY")
        print("  Set them in .env or as environment variables.")
        sys.exit(1)

    print(f"Starting Scenario Agent UI on http://{HOST}:{PORT}")
    print("Opening browser...")
    threading.Timer(1.0, lambda: webbrowser.open(f"http://{HOST}:{PORT}")).start()
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
