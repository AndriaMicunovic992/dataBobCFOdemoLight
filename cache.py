"""
cache.py — Budget row cache for persisting between server restarts.

Uses SQLiteStorage as the backend (migrated from direct SQLite).
Provides the same cache_save / cache_load API for backward compatibility.

Row format: [{"account": 112, "date": "2026-01-01", "amount": 7837544.07, ...}, ...]
"""

from storage.sqlite_storage import SQLiteStorage
from config import STORAGE_DB

# Shared storage instance
_storage = SQLiteStorage(STORAGE_DB)

_CACHE_KEY = "budget_rows"


def cache_save(rows: list[dict], key: str = _CACHE_KEY):
    """Persist rows to the cache, replacing any previous data."""
    assert rows, "Refusing to save empty list"
    _storage.cache_save(key, rows)
    print(f"[Cache] Saved {len(rows)} rows")


def cache_load(key: str = _CACHE_KEY) -> list[dict]:
    """Load rows from the cache. Returns [] if cache is empty."""
    data = _storage.cache_load(key)
    if not data:
        return []
    rows = data if isinstance(data, list) else []
    if rows:
        print(f"[Cache] Loaded {len(rows)} rows")
    return rows


def cache_delete(key: str = _CACHE_KEY):
    """Remove cached rows."""
    _storage.cache_delete(key)
    print(f"[Cache] Deleted '{key}'")
