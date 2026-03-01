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
            """)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    # ── Model Understandings ───────────────────────────────────────────────────

    def save_model_understanding(self, source_id: str, data: dict,
                                  source_type: str = "") -> str:
        existing = self.load_model_understanding(source_id)
        now = self._now()

        if existing:
            # Update: increment version
            uid = existing["id"]
            version = existing.get("version", 1) + 1
            with self._conn() as con:
                con.execute("""
                    UPDATE model_understandings
                    SET data = ?, version = ?, updated_at = ?, source_type = ?
                    WHERE id = ?
                """, (json.dumps(data), version, now, source_type, uid))
            print(f"[Storage] Updated model understanding {uid[:8]}... v{version}")
        else:
            # Insert new
            uid = str(uuid.uuid4())
            with self._conn() as con:
                con.execute("""
                    INSERT INTO model_understandings
                    (id, source_type, source_id, version, data, created_at, updated_at)
                    VALUES (?, ?, ?, 1, ?, ?, ?)
                """, (uid, source_type, source_id, json.dumps(data), now, now))
            print(f"[Storage] Saved new model understanding {uid[:8]}...")

        return uid

    def load_model_understanding(self, source_id: str) -> dict | None:
        with self._conn() as con:
            row = con.execute("""
                SELECT id, source_type, source_id, version, data, created_at, updated_at
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

    def save_file(self, filename: str, content: str, subdir: str = "") -> Path:
        target_dir = self.files_dir / subdir if subdir else self.files_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / filename
        path.write_text(content, encoding="utf-8")

        # Track in DB
        now = self._now()
        uid = str(uuid.uuid4())
        with self._conn() as con:
            con.execute("""
                INSERT INTO uploaded_files (id, filename, file_type, file_path, uploaded_at)
                VALUES (?, ?, ?, ?, ?)
            """, (uid, filename, path.suffix, str(path), now))

        return path

    def list_files(self, pattern: str = "*", subdir: str = "") -> list[Path]:
        target_dir = self.files_dir / subdir if subdir else self.files_dir
        if not target_dir.exists():
            return []
        return sorted(target_dir.glob(pattern),
                      key=lambda f: f.stat().st_mtime, reverse=True)
