"""
pbi_client.py — Backward-compatibility wrapper.

Re-exports PBIClient and list_pbi_instances from the new datasources package.
Existing code that imports from pbi_client continues to work unchanged.
"""

from datasources.pbi_desktop import PBIDesktopSource, list_pbi_instances as _list_instances
from config import POWERBI_EXE


class PBIClient:
    """
    Backward-compatible wrapper around PBIDesktopSource.

    Provides the same connect() / dax() / disconnect() interface that
    the rest of the codebase expects, backed by the new DataSource layer.
    """

    def __init__(self):
        self._source = PBIDesktopSource(POWERBI_EXE)
        self.connection_string = ""
        self.database_guid = ""

    async def connect(self, conn_str: str, db_guid: str):
        """Open an MCP session and connect to the given model."""
        await self._source.connect(
            connection_string=conn_str, database=db_guid
        )
        self.connection_string = conn_str
        self.database_guid = db_guid

    async def dax(self, query: str) -> dict:
        """Execute a DAX query and return the parsed JSON response."""
        return await self._source.query(query)

    async def disconnect(self):
        """Close the MCP session gracefully."""
        await self._source.disconnect()

    @property
    def source(self) -> PBIDesktopSource:
        """Access the underlying DataSource (for schema extraction etc.)."""
        return self._source


async def list_pbi_instances() -> list[dict]:
    """Discover all open Power BI Desktop models."""
    return await _list_instances(POWERBI_EXE)
