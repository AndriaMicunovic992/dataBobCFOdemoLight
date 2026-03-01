"""
Deterministic schema extraction — no LLM needed.

Extracts raw schema metadata from any DataSource, including tables, columns,
relationships, row counts, sample data, and basic statistics.
"""

from datasources.base import DataSource


class SchemaExtractor:
    """
    Extracts raw schema and sample data from a DataSource.

    This is a deterministic step — no LLM involved. The output is a
    structured dict that the DiscoveryAgent feeds to Claude for analysis.
    """

    def __init__(self, source: DataSource):
        self.source = source

    async def extract(self, sample_rows: int = 10,
                      max_tables: int = 50) -> dict:
        """
        Extract full schema metadata with sample data.

        Args:
            sample_rows: Number of sample rows per table.
            max_tables:  Maximum tables to process (skip beyond this).

        Returns:
            {
                "source_type": "pbi_desktop" | "excel" | "composite",
                "query_language": "DAX" | "SQL",
                "tables": [
                    {
                        "name": "...",
                        "columns": [{"name": "...", "data_type": "...", ...}],
                        "row_count": int | None,
                        "sample_data": [{"col": value, ...}],
                        "statistics": {"col": {"distinct": N, "nulls": N, ...}},
                    },
                    ...
                ],
                "relationships": [...],
            }
        """
        schema = await self.source.get_schema()
        tables = schema.get("tables", [])[:max_tables]
        relationships = schema.get("relationships", [])

        enriched_tables = []
        for t in tables:
            tname = t["name"]

            # Get sample data
            try:
                samples = await self.source.get_sample_data(tname, sample_rows)
            except Exception as e:
                print(f"[Schema] Could not sample '{tname}': {e}")
                samples = []

            # Compute basic stats from sample
            stats = {}
            if samples:
                for col in t.get("columns", []):
                    col_name = col["name"]
                    values = [r.get(col_name) for r in samples]
                    non_null = [v for v in values if v is not None]
                    stats[col_name] = {
                        "sample_size":   len(values),
                        "null_count":    len(values) - len(non_null),
                        "distinct_count": len(set(str(v) for v in non_null)),
                        "sample_values":  [str(v) for v in non_null[:5]],
                    }

            enriched_tables.append({
                **t,
                "sample_data": samples[:sample_rows],
                "statistics":  stats,
            })

        result = {
            "source_type":    self.source.source_type(),
            "query_language": self.source.query_language(),
            "tables":         enriched_tables,
            "relationships":  relationships,
        }

        total_rows = sum(t.get("row_count", 0) or 0 for t in enriched_tables)
        print(f"[Schema] Extracted: {len(enriched_tables)} tables, "
              f"{len(relationships)} relationships, ~{total_rows} total rows")

        return result
