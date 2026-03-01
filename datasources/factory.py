"""Data source factory functions."""

from pathlib import Path

from datasources.base import DataSource
from datasources.pbi_desktop import PBIDesktopSource
from datasources.excel_source import ExcelSource
from datasources.composite_source import CompositeSource


def create_datasource(source_type: str, **config) -> DataSource:
    """
    Create a DataSource by type.

    Args:
        source_type: "pbi_desktop" or "excel"
        **config: Type-specific configuration:
            pbi_desktop: pbi_exe (str)
            excel:       (no config needed — files passed at connect time)
    """
    if source_type == "pbi_desktop":
        pbi_exe = config.get("pbi_exe", "")
        if not pbi_exe:
            raise ValueError("pbi_exe path required for pbi_desktop source")
        return PBIDesktopSource(pbi_exe)

    elif source_type == "excel":
        return ExcelSource()

    raise ValueError(f"Unknown source type: {source_type}")


def create_composite(sources: list[DataSource]) -> CompositeSource:
    """Create a composite source wrapping multiple sources."""
    return CompositeSource(sources)
