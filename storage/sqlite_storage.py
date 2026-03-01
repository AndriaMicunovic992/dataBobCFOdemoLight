"""SQLite-backed persistent storage."""

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from storage.base import Storage


class SQLiteStorage(Storage):
    """
    Single-file SQLite storage for model understandings, cache, and files.

    Uses WAL mode for safe concurrent reads. All JSON data is stored as TEXT
    columns and parsed on load.
    """

    def __init__(self, db_path: Path, files_dir: Path | None = None):
        self.db_path = db_path
        self.files_dir = files_dir or db_path.parent / "output"
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.db_path), timeout=10)
        con.execute("PRAGMA journal_mode=WAL")
        con.row_factory = sqlite3.Row
        return con

    def _init_db(self):
        """Create tables if they don't exist."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as con:
            con.executescript("""
                CREATE TABLE IF NOT EXISTS model_understandings (
                    id          TEXT PRIMARY KEY,
                    source_type TEXT NOT NULL DEFAULT '',
                    source_id   TEXT NOT NULL,
                    version     INTEGER NOT NULL DEFAULT 1,
                    data        TEXT NOT NULL,
                    created_at  TEXT NOT NULL,
                    updated_at  TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_mu_source
                    ON model_understandings(source_id);

                CREATE TABLE IF NOT EXISTS cache (
                    key        TEXT PRIMARY KEY,
                    data       TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS uploaded_files (
                    id          TEXT PRIMARY KEY,
                    filename    TEXT NOT NULL,
                    file_type   TEXT NOT NULL DEFAULT '',
                    file_path   TEXT NOT NULL,
                    uploaded_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS models (
                    id               TEXT PRIMARY KEY,
                    name             TEXT NOT NULL,
                    description      TEXT NOT NULL DEFAULT '',
                    source_type      TEXT NOT NULL DEFAULT '',
                    created_at       TEXT NOT NULL,
                    updated_at       TEXT NOT NULL,
                    last_accessed_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_models_updated
                    ON models(updated_at DESC);

                CREATE TABLE IF NOT EXISTS model_sources (
                    id          TEXT PRIMARY KEY,
                    model_id    TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_id   TEXT NOT NULL,
                    label       TEXT NOT NULL DEFAULT '',
                    config      TEXT NOT NULL DEFAULT '{}',
                    linked_at   TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_model_sources_model
                    ON model_sources(model_id);
                CREATE INDEX IF NOT EXISTS idx_model_sources_sid
                    ON model_sources(source_id);
            """)

            # Add model_id to existing tables (idempotent migration)
            for table in ("model_understandings", "uploaded_files"):
                try:
                    con.execute(
                        f"ALTER TABLE {table} ADD COLUMN model_id TEXT DEFAULT NULL"
                    )
                except sqlite3.OperationalError:
                    pass  # Column already exists

            # Index for model_id lookups
            con.executescript("""
                CREATE INDEX IF NOT EXISTS idx_mu_model
                    ON model_understandings(model_id);
                CREATE INDEX IF NOT EXISTS idx_uf_model
                    ON uploaded_files(model_id);
            """)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    # ── Model Understandings ───────────────────────────────────────────────────

    def save_model_understanding(self, source_id: str, data: dict,
                                  source_type: str = "",
                                  model_id: str | None = None) -> str:
        """Persist a model understanding.

        Looks up existing row by *model_id* first (stable anchor), then falls
        back to *source_id* (legacy path).  New rows get *model_id* set when
        provided.  Returns the understanding row ID.
        """
        # Try model_id lookup first, then source_id
        existing = None
        if model_id:
            existing = self.load_model_understanding_by_model(model_id)
        if not existing:
            existing = self.load_model_understanding(source_id)

        now = self._now()

        if existing:
            # Update: increment version
            uid = existing["_meta"]["id"]
            version = existing["_meta"].get("version", 1) + 1
            with self._conn() as con:
                con.execute("""
                    UPDATE model_understandings
                    SET data = ?, version = ?, updated_at = ?, source_type = ?,
                        model_id = COALESCE(?, model_id)
                    WHERE id = ?
                """, (json.dumps(data), version, now, source_type, model_id, uid))
            print(f"[Storage] Updated model understanding {uid[:8]}... v{version}")
        else:
            # Insert new
            uid = str(uuid.uuid4())
            with self._conn() as con:
                con.execute("""
                    INSERT INTO model_understandings
                    (id, source_type, source_id, version, data, created_at, updated_at, model_id)
                    VALUES (?, ?, ?, 1, ?, ?, ?, ?)
                """, (uid, source_type, source_id, json.dumps(data), now, now, model_id))
            print(f"[Storage] Saved new model understanding {uid[:8]}...")

        return uid

    def load_model_understanding(self, source_id: str) -> dict | None:
        with self._conn() as con:
            row = con.execute("""
                SELECT id, source_type, source_id, version, data,
                       created_at, updated_at, model_id
                FROM model_understandings
                WHERE source_id = ?
                ORDER BY version DESC LIMIT 1
            """, (source_id,)).fetchone()

        if not row:
            return None

        result = json.loads(row["data"])
        result["_meta"] = {
            "id":          row["id"],
            "source_type": row["source_type"],
            "source_id":   row["source_id"],
            "version":     row["version"],
            "created_at":  row["created_at"],
            "updated_at":  row["updated_at"],
            "model_id":    row["model_id"],
        }
        return result

    def load_model_understanding_by_model(self, model_id: str) -> dict | None:
        """Load the latest model understanding linked to a model_id."""
        with self._conn() as con:
            row = con.execute("""
                SELECT id, source_type, source_id, version, data,
                       created_at, updated_at, model_id
                FROM model_understandings
                WHERE model_id = ?
                ORDER BY version DESC LIMIT 1
            """, (model_id,)).fetchone()

        if not row:
            return None

        result = json.loads(row["data"])
        result["_meta"] = {
            "id":          row["id"],
            "source_type": row["source_type"],
            "source_id":   row["source_id"],
            "version":     row["version"],
            "created_at":  row["created_at"],
            "updated_at":  row["updated_at"],
            "model_id":    row["model_id"],
        }
        return result

    def list_model_understandings(self) -> list[dict]:
        with self._conn() as con:
            rows = con.execute("""
                SELECT id, source_type, source_id, version, created_at, updated_at
                FROM model_understandings
                ORDER BY updated_at DESC
            """).fetchall()
        return [dict(r) for r in rows]

    def link_understanding_to_model(self, source_id: str,
                                     model_id: str) -> bool:
        """Retroactively set model_id on understandings that match a source_id."""
        now = self._now()
        with self._conn() as con:
            cur = con.execute("""
                UPDATE model_understandings
                SET model_id = ?, updated_at = ?
                WHERE source_id = ? AND (model_id IS NULL OR model_id = '')
            """, (model_id, now, source_id))
        updated = cur.rowcount
        if updated:
            print(f"[Storage] Linked {updated} understanding(s) for source "
                  f"{source_id[:16]}... to model {model_id[:8]}...")
        return updated > 0

    # ── Models (stable anchor) ─────────────────────────────────────────────────

    def create_model(self, name: str, source_type: str = "",
                     description: str = "") -> str:
        """Create a new model entry. Returns the model ID (UUID)."""
        uid = str(uuid.uuid4())
        now = self._now()
        with self._conn() as con:
            con.execute("""
                INSERT INTO models
                (id, name, description, source_type, created_at, updated_at, last_accessed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (uid, name, description, source_type, now, now, now))
        print(f"[Storage] Created model '{name}' ({uid[:8]}...)")
        return uid

    def get_model(self, model_id: str) -> dict | None:
        """Load a model by ID."""
        with self._conn() as con:
            row = con.execute(
                "SELECT * FROM models WHERE id = ?", (model_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_models(self) -> list[dict]:
        """List all models, most recently accessed first."""
        with self._conn() as con:
            rows = con.execute(
                "SELECT * FROM models ORDER BY last_accessed_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def update_model(self, model_id: str, **fields) -> None:
        """Update model fields (name, description, source_type)."""
        allowed = {"name", "description", "source_type"}
        updates = {k: v for k, v in fields.items() if k in allowed}
        if not updates:
            return
        updates["updated_at"] = self._now()
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [model_id]
        with self._conn() as con:
            con.execute(
                f"UPDATE models SET {set_clause} WHERE id = ?", values
            )

    def delete_model(self, model_id: str) -> None:
        """Delete a model and cascade to linked sources and understandings."""
        with self._conn() as con:
            con.execute("DELETE FROM model_sources WHERE model_id = ?", (model_id,))
            con.execute(
                "UPDATE model_understandings SET model_id = NULL WHERE model_id = ?",
                (model_id,),
            )
            con.execute(
                "UPDATE uploaded_files SET model_id = NULL WHERE model_id = ?",
                (model_id,),
            )
            con.execute("DELETE FROM models WHERE id = ?", (model_id,))
        print(f"[Storage] Deleted model {model_id[:8]}...")

    def touch_model(self, model_id: str) -> None:
        """Update last_accessed_at to now."""
        now = self._now()
        with self._conn() as con:
            con.execute(
                "UPDATE models SET last_accessed_at = ? WHERE id = ?",
                (now, model_id),
            )

    # ── Model Sources ──────────────────────────────────────────────────────────

    def add_model_source(self, model_id: str, source_type: str,
                         source_id: str, label: str = "",
                         config: dict | None = None) -> str:
        """Link a data source to a model. Returns the link ID."""
        uid = str(uuid.uuid4())
        now = self._now()
        config_json = json.dumps(config or {})
        with self._conn() as con:
            con.execute("""
                INSERT INTO model_sources
                (id, model_id, source_type, source_id, label, config, linked_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (uid, model_id, source_type, source_id, label, config_json, now))
        print(f"[Storage] Linked source '{label}' to model {model_id[:8]}...")
        return uid

    def get_model_sources(self, model_id: str) -> list[dict]:
        """Get all sources linked to a model."""
        with self._conn() as con:
            rows = con.execute(
                "SELECT * FROM model_sources WHERE model_id = ? ORDER BY linked_at",
                (model_id,),
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["config"] = json.loads(d.get("config", "{}"))
            result.append(d)
        return result

    def find_model_by_source_id(self, source_id: str) -> dict | None:
        """Find a model that has a source with this source_id linked.

        Returns the model dict or None. Used for auto-detecting existing
        models when a user connects a data source.
        """
        with self._conn() as con:
            row = con.execute("""
                SELECT m.* FROM models m
                JOIN model_sources ms ON ms.model_id = m.id
                WHERE ms.source_id = ?
                ORDER BY m.last_accessed_at DESC
                LIMIT 1
            """, (source_id,)).fetchone()
        return dict(row) if row else None

    def remove_model_source(self, link_id: str) -> None:
        """Unlink a source from a model."""
        with self._conn() as con:
            con.execute("DELETE FROM model_sources WHERE id = ?", (link_id,))

    # ── Cache ──────────────────────────────────────────────────────────────────

    def cache_save(self, key: str, data: Any) -> None:
        now = self._now()
        with self._conn() as con:
            con.execute("""
                INSERT OR REPLACE INTO cache (key, data, created_at)
                VALUES (?, ?, ?)
            """, (key, json.dumps(data), now))
        print(f"[Storage] Cached '{key}' ({len(json.dumps(data))} bytes)")

    def cache_load(self, key: str) -> Any | None:
        with self._conn() as con:
            row = con.execute(
                "SELECT data FROM cache WHERE key = ?", (key,)
            ).fetchone()
        if not row:
            return None
        return json.loads(row["data"])

    def cache_delete(self, key: str) -> None:
        with self._conn() as con:
            con.execute("DELETE FROM cache WHERE key = ?", (key,))

    # ── Files ──────────────────────────────────────────────────────────────────

    def save_file(self, filename: str, content: str, subdir: str = "",
                  model_id: str | None = None) -> Path:
        target_dir = self.files_dir / subdir if subdir else self.files_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / filename
        path.write_text(content, encoding="utf-8")

        # Track in DB
        now = self._now()
        uid = str(uuid.uuid4())
        with self._conn() as con:
            con.execute("""
                INSERT INTO uploaded_files
                (id, filename, file_type, file_path, uploaded_at, model_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (uid, filename, path.suffix, str(path), now, model_id))

        return path

    def track_uploaded_file(self, filename: str, file_type: str,
                            file_path: str,
                            model_id: str | None = None) -> str:
        """Track an externally saved file (e.g. Flask upload) in the DB.

        Unlike save_file() this does NOT write the file to disk — it only
        records the metadata.  Returns the file tracking ID.
        """
        uid = str(uuid.uuid4())
        now = self._now()
        with self._conn() as con:
            con.execute("""
                INSERT INTO uploaded_files
                (id, filename, file_type, file_path, uploaded_at, model_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (uid, filename, file_type, file_path, now, model_id))
        return uid

    def list_files(self, pattern: str = "*", subdir: str = "") -> list[Path]:
        target_dir = self.files_dir / subdir if subdir else self.files_dir
        if not target_dir.exists():
            return []
        return sorted(target_dir.glob(pattern),
                      key=lambda f: f.stat().st_mtime, reverse=True)
