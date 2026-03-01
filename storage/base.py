"""Abstract storage interface."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class Storage(ABC):
    """Abstract storage for model understandings, cache, and files."""

    # ── Model Understandings ───────────────────────────────────────────────────

    @abstractmethod
    def save_model_understanding(self, source_id: str, data: dict,
                                  source_type: str = "",
                                  model_id: str | None = None) -> str:
        """Persist a model understanding. Returns the understanding ID."""

    @abstractmethod
    def load_model_understanding(self, source_id: str) -> dict | None:
        """Load the latest model understanding for a source. Returns None if not found."""

    @abstractmethod
    def load_model_understanding_by_model(self, model_id: str) -> dict | None:
        """Load the latest model understanding for a model_id. Returns None if not found."""

    @abstractmethod
    def list_model_understandings(self) -> list[dict]:
        """List all stored model understandings (summary info only)."""

    # ── Cache ──────────────────────────────────────────────────────────────────

    @abstractmethod
    def cache_save(self, key: str, data: Any) -> None:
        """Save data to cache under the given key."""

    @abstractmethod
    def cache_load(self, key: str) -> Any | None:
        """Load cached data by key. Returns None if not found."""

    @abstractmethod
    def cache_delete(self, key: str) -> None:
        """Delete a cache entry."""

    # ── Files ──────────────────────────────────────────────────────────────────

    @abstractmethod
    def save_file(self, filename: str, content: str, subdir: str = "") -> Path:
        """Save a text file and return its path."""

    @abstractmethod
    def list_files(self, pattern: str = "*", subdir: str = "") -> list[Path]:
        """List files matching a glob pattern."""
