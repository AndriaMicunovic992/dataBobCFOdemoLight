"""Abstract data source interface."""

from abc import ABC, abstractmethod


class DataSource(ABC):
    """
    Abstract interface for any tabular data source.

    Implementations exist for:
      - PBI Desktop (MCP-based, runs DAX)
      - Excel files (openpyxl + duckdb, runs SQL)
      - Composite (wraps multiple sources)
    """

    @abstractmethod
    async def connect(self, **kwargs) -> None:
        """Establish connection. Kwargs vary by implementation."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Release resources."""

    @abstractmethod
    async def query(self, query_text: str) -> dict:
        """
        Execute a query and return results.

        Returns:
            {"success": True, "data": {"rows": [...]}}
            or {"success": False, "message": "error description"}
        """

    @abstractmethod
    async def get_schema(self) -> dict:
        """
        Return schema metadata.

        Returns:
            {
                "tables": [
                    {
                        "name": "...",
                        "columns": [{"name": "...", "data_type": "...", "is_nullable": bool}],
                        "row_count": int | None,
                    },
                    ...
                ],
                "relationships": [
                    {
                        "from_table": "...", "from_column": "...",
                        "to_table": "...", "to_column": "...",
                        "cardinality": "many-to-one" | "one-to-one" | ...
                    },
                    ...
                ],
            }
        """

    @abstractmethod
    async def get_sample_data(self, table_name: str,
                               max_rows: int = 100) -> list[dict]:
        """Return sample rows from a table for model discovery."""

    @abstractmethod
    def source_type(self) -> str:
        """Return identifier: 'pbi_desktop', 'excel', 'composite'."""

    @abstractmethod
    def source_id(self) -> str:
        """Return a stable identifier for this specific source instance."""

    @abstractmethod
    def query_language(self) -> str:
        """Return the query language: 'DAX' or 'SQL'."""

    @abstractmethod
    def supports_writeback(self) -> bool:
        """Whether this source supports writing scenario data back."""

    def display_name(self) -> str:
        """Human-readable name for UI display. Default: source_id()."""
        return self.source_id()

    async def get_measures(self) -> list[dict]:
        """Return DAX measures from the model. Default: empty (non-PBI sources)."""
        return []
