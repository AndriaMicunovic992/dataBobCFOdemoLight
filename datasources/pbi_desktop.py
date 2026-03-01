"""
Power BI Desktop data source — MCP-based connection.

Wraps the existing PBI MCP client as a DataSource implementation.
Supports DAX queries, schema extraction, and sample data retrieval.
"""

import json
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from datasources.base import DataSource


class PBIDesktopSource(DataSource):
    """
    DataSource backed by a Power BI Desktop model via MCP subprocess.

    Usage:
        source = PBIDesktopSource(pbi_exe_path)
        await source.connect(connection_string="...", database="...")
        result = await source.query("EVALUATE ...")
        schema = await source.get_schema()
        await source.disconnect()
    """

    def __init__(self, pbi_exe: str):
        self._pbi_exe = pbi_exe
        self._session = None
        self._transport = None
        self._conn_str = ""
        self._db_guid = ""

    async def connect(self, **kwargs) -> None:
        connection_string = kwargs.get("connection_string", "")
        database = kwargs.get("database", "")
        if not connection_string or not database:
            raise ValueError("connection_string and database are required")

        params = StdioServerParameters(
            command=self._pbi_exe, args=["--start"], env={}
        )
        self._transport = stdio_client(params)
        r, w = await self._transport.__aenter__()
        self._session = ClientSession(r, w)
        await self._session.__aenter__()
        await self._session.initialize()

        res = await self._session.call_tool("connection_operations", {"request": {
            "operation":        "Connect",
            "connectionString": connection_string,
            "initialCatalog":   database,
        }})

        self._conn_str = connection_string
        self._db_guid = database
        text = res.content[0].text[:80] if res.content else "ok"
        print(f"[PBI] Connected: {text}")

    async def disconnect(self) -> None:
        try:
            if self._session:
                await self._session.__aexit__(None, None, None)
            if self._transport:
                await self._transport.__aexit__(None, None, None)
        except Exception:
            pass
        self._session = None
        self._transport = None

    async def query(self, query_text: str) -> dict:
        if not self._session:
            return {"success": False, "message": "Not connected"}
        res = await self._session.call_tool("dax_query_operations", {
            "request": {"operation": "Execute", "query": query_text}
        })
        raw = res.content[0].text if res.content else "{}"
        return json.loads(raw)

    async def get_schema(self) -> dict:
        """
        Extract full schema from the PBI model via MCP operations.

        Uses table_operations List, column_operations List, and
        relationship_operations List to enumerate the model metadata.
        """
        if not self._session:
            return {"tables": [], "relationships": []}

        # ── Tables ─────────────────────────────────────────────────────────
        raw = await self._session.call_tool("table_operations", {
            "request": {"operation": "List"}
        })
        table_data = json.loads(raw.content[0].text if raw.content else "{}")
        tables_raw = table_data.get("data", [])

        tables = []
        for t in tables_raw:
            tname = t.get("name", "")
            # Skip internal / system tables
            if tname.startswith("DateTableTemplate") or tname.startswith("LocalDateTable"):
                continue

            # Get columns for this table
            col_raw = await self._session.call_tool("column_operations", {
                "request": {"operation": "List", "tableName": tname}
            })
            col_data = json.loads(col_raw.content[0].text if col_raw.content else "{}")
            columns = []
            for c in col_data.get("data", []):
                columns.append({
                    "name":        c.get("name", ""),
                    "data_type":   c.get("dataType", "unknown"),
                    "is_nullable": c.get("isNullable", True),
                    "is_hidden":   c.get("isHidden", False),
                    "source_column": c.get("sourceColumn", ""),
                    "expression":  c.get("expression", ""),
                })

            tables.append({
                "name":      tname,
                "columns":   columns,
                "row_count": None,   # not available via MCP metadata
                "is_hidden": t.get("isHidden", False),
                "description": t.get("description", ""),
            })

        # ── Relationships ──────────────────────────────────────────────────
        rel_raw = await self._session.call_tool("relationship_operations", {
            "request": {"operation": "List"}
        })
        rel_data = json.loads(rel_raw.content[0].text if rel_raw.content else "{}")
        relationships = []
        for r in rel_data.get("data", []):
            relationships.append({
                "name":        r.get("name", ""),
                "from_table":  r.get("fromTable", ""),
                "from_column": r.get("fromColumn", ""),
                "to_table":    r.get("toTable", ""),
                "to_column":   r.get("toColumn", ""),
                "is_active":   r.get("isActive", True),
                "cross_filtering": r.get("crossFilteringBehavior", ""),
                "from_cardinality": r.get("fromCardinality", ""),
                "to_cardinality":   r.get("toCardinality", ""),
            })

        print(f"[PBI] Schema: {len(tables)} tables, {len(relationships)} relationships")
        return {"tables": tables, "relationships": relationships}

    async def get_measures(self) -> list[dict]:
        """Fetch DAX measures from the PBI model via MCP measure_operations."""
        if not self._session:
            return []
        try:
            raw = await self._session.call_tool("measure_operations", {
                "request": {"operation": "List"}
            })
            measure_data = json.loads(raw.content[0].text if raw.content else "{}")
            measures = []
            for m in measure_data.get("data", []):
                measures.append({
                    "name":        m.get("name", ""),
                    "expression":  m.get("expression", ""),
                    "table":       m.get("tableName", ""),
                    "description": m.get("description", ""),
                    "data_type":   m.get("dataType", ""),
                    "is_hidden":   m.get("isHidden", False),
                })
            print(f"[PBI] Measures: {len(measures)} found")
            return measures
        except Exception as e:
            print(f"[PBI] Could not fetch measures: {e}")
            return []

    async def get_sample_data(self, table_name: str,
                               max_rows: int = 100) -> list[dict]:
        query = f"EVALUATE TOPN({max_rows}, '{table_name}')"
        resp = await self.query(query)
        rows = resp.get("data", {}).get("rows", [])
        # Clean column names: "Table[Column]" → "Column"
        clean = []
        for r in rows:
            clean_row = {}
            for k, v in r.items():
                # Strip table prefix: "Fakten Hauptbuch[amount]" → "amount"
                col = k.split("[")[-1].rstrip("]") if "[" in k else k
                clean_row[col] = v
            clean.append(clean_row)
        return clean

    def source_type(self) -> str:
        return "pbi_desktop"

    def source_id(self) -> str:
        return f"pbi:{self._db_guid}" if self._db_guid else "pbi:not_connected"

    def query_language(self) -> str:
        return "DAX"

    def supports_writeback(self) -> bool:
        return True  # Scenarios can be written back via SQL INSERT


async def list_pbi_instances(pbi_exe: str) -> list[dict]:
    """
    Start a temporary MCP session to discover all open Power BI Desktop models.

    Returns a list of dicts:
        display_name, connection_string, database, port
    """
    params = StdioServerParameters(command=pbi_exe, args=["--start"], env={})
    transport = None
    session = None
    result: list[dict] = []

    try:
        transport = stdio_client(params)
        r, w = await transport.__aenter__()
        session = ClientSession(r, w)
        await session.__aenter__()
        await session.initialize()

        # Step 1 — list open PBI Desktop processes
        raw = await session.call_tool("connection_operations",
                    {"request": {"operation": "ListLocalInstances"}})
        data = json.loads(raw.content[0].text if raw.content else "{}")
        insts = data.get("data", [])
        print(f"[PBI] {len(insts)} Desktop instance(s) found")

        # Step 2 — for each instance resolve database GUID
        for inst in insts:
            conn_str = inst.get("connectionString", "")
            title = inst.get("parentWindowTitle", "Power BI Model")
            port = inst.get("port", 0)

            try:
                await session.call_tool("connection_operations",
                    {"request": {"operation": "Connect",
                                 "connectionString": conn_str}})

                db_raw = await session.call_tool("database_operations",
                              {"request": {"operation": "List"}})
                db_data = json.loads(db_raw.content[0].text if db_raw.content else "{}")

                for db in db_data.get("data", []):
                    db_id = db.get("id", db.get("name", ""))
                    result.append({
                        "display_name":      title,
                        "connection_string": conn_str,
                        "database":          db_id,
                        "port":              port,
                    })
                    print(f"[PBI]  -> '{title}'  port={port}  db={db_id[:8]}...")

            except Exception as e:
                print(f"[PBI] Could not get DB for '{title}': {e}")
                result.append({
                    "display_name":      title,
                    "connection_string": conn_str,
                    "database":          "",
                    "port":              port,
                })

    except Exception as e:
        print(f"[PBI] Discovery error: {e}")

    finally:
        try:
            if session:
                await session.__aexit__(None, None, None)
        except Exception:
            pass
        try:
            if transport:
                await transport.__aexit__(None, None, None)
        except Exception:
            pass

    return result
