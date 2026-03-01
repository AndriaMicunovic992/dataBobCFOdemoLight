"""
Composite data source — wraps multiple DataSource instances.

Enables combining PBI Desktop + Excel uploads into a unified view.
Queries are routed to the correct underlying source based on table name.
"""

from datasources.base import DataSource


class CompositeSource(DataSource):
    """
    Wraps multiple DataSource instances into one unified interface.

    Schema merges all tables from all sources. Queries are routed to the
    source that owns the referenced table. The primary source (first one)
    determines the default query language and writeback capability.

    Usage:
        pbi = PBIDesktopSource(...)
        excel = ExcelSource()
        await pbi.connect(...)
        await excel.connect(files=[...])
        combo = CompositeSource([pbi, excel])
        await combo.connect()  # no-op, sources already connected
    """

    def __init__(self, sources: list[DataSource]):
        if not sources:
            raise ValueError("At least one source is required")
        self._sources = sources
        self._table_map: dict[str, DataSource] = {}  # table_name → source

    async def connect(self, **kwargs) -> None:
        """Build the table→source mapping from all connected sources."""
        self._table_map = {}
        for src in self._sources:
            schema = await src.get_schema()
            for t in schema.get("tables", []):
                tname = t["name"]
                if tname not in self._table_map:
                    self._table_map[tname] = src
                else:
                    # Prefix with source type if table name conflicts
                    prefixed = f"{src.source_type()}__{tname}"
                    self._table_map[prefixed] = src
                    print(f"[Composite] Table name conflict: '{tname}' "
                          f"→ '{prefixed}' for {src.source_type()}")

        print(f"[Composite] Unified schema: {len(self._table_map)} tables "
              f"from {len(self._sources)} sources")

    async def disconnect(self) -> None:
        for src in self._sources:
            await src.disconnect()
        self._table_map = {}

    async def query(self, query_text: str) -> dict:
        """
        Route query to the appropriate source.

        Heuristic: try the primary source first (index 0). If that fails,
        try secondary sources. For more precise routing, the caller should
        know which source owns the table being queried.
        """
        # Try primary source first
        result = await self._sources[0].query(query_text)
        if result.get("success"):
            return result

        # Try secondary sources
        for src in self._sources[1:]:
            result = await src.query(query_text)
            if result.get("success"):
                return result

        return result  # Return last failure

    async def query_source(self, source_index: int,
                           query_text: str) -> dict:
        """Query a specific source by index."""
        if 0 <= source_index < len(self._sources):
            return await self._sources[source_index].query(query_text)
        return {"success": False, "message": f"Invalid source index: {source_index}"}

    async def get_schema(self) -> dict:
        """Merge schemas from all sources."""
        all_tables = []
        all_rels = []

        for src in self._sources:
            schema = await src.get_schema()
            for t in schema.get("tables", []):
                t["_source_type"] = src.source_type()
                t["_source_id"] = src.source_id()
                all_tables.append(t)
            all_rels.extend(schema.get("relationships", []))

        return {"tables": all_tables, "relationships": all_rels}

    async def get_sample_data(self, table_name: str,
                               max_rows: int = 100) -> list[dict]:
        src = self._table_map.get(table_name)
        if src:
            return await src.get_sample_data(table_name, max_rows)
        # Fallback: try each source
        for s in self._sources:
            try:
                data = await s.get_sample_data(table_name, max_rows)
                if data:
                    return data
            except Exception:
                continue
        return []

    def source_type(self) -> str:
        return "composite"

    def source_id(self) -> str:
        parts = [s.source_id() for s in self._sources]
        return "composite:" + "+".join(parts)

    def query_language(self) -> str:
        """Primary source determines the default query language."""
        return self._sources[0].query_language()

    def supports_writeback(self) -> bool:
        """True if any source supports writeback."""
        return any(s.supports_writeback() for s in self._sources)

    @property
    def sources(self) -> list[DataSource]:
        """Access underlying sources."""
        return self._sources

    def get_source_for_table(self, table_name: str) -> DataSource | None:
        """Get the source that owns a specific table."""
        return self._table_map.get(table_name)
