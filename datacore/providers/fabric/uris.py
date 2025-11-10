"""Utilities to parse Fabric Lakehouse URIs."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal, Optional
from urllib.parse import urlparse


FabricUriKind = Literal["tables", "files", "unknown"]


def _default_schema() -> str:
    return os.getenv("FABRIC_DEFAULT_SCHEMA", "dbo")


@dataclass(frozen=True)
class FabricLakehouseReference:
    """Structured representation of a ``lakehouse://`` URI."""

    raw_uri: str
    is_lakehouse: bool
    lakehouse: Optional[str]
    kind: FabricUriKind
    schema: Optional[str]
    table: Optional[str]
    subpath: Optional[str]

    @property
    def full_table_name(self) -> Optional[str]:
        if self.schema and self.table:
            return f"{self.schema}.{self.table}"
        return None


def parse_fabric_lakehouse_uri(uri: str) -> FabricLakehouseReference:
    """Parse Fabric Lakehouse URIs into structured components."""

    parsed = urlparse(uri)
    if parsed.scheme.lower() != "lakehouse":
        return FabricLakehouseReference(
            raw_uri=uri,
            is_lakehouse=False,
            lakehouse=None,
            kind="unknown",
            schema=None,
            table=None,
            subpath=None,
        )

    path_segments = [segment for segment in parsed.path.split("/") if segment]
    lakehouse = parsed.netloc or (path_segments.pop(0) if path_segments else None)
    kind: FabricUriKind = "unknown"
    schema: Optional[str] = None
    table: Optional[str] = None
    subpath: Optional[str] = None

    if path_segments:
        candidate = path_segments.pop(0)
        lowered = candidate.lower()
        if lowered == "tables":
            kind = "tables"
            remainder = path_segments
            if remainder:
                if len(remainder) == 1:
                    item = remainder[0]
                    if "." in item:
                        schema, table = item.split(".", 1)
                    else:
                        schema = _default_schema()
                        table = item
                else:
                    schema = remainder[0] or _default_schema()
                    table = remainder[1] if len(remainder) > 1 else None
        elif lowered == "files":
            kind = "files"
            remainder = [segment for segment in path_segments if segment]
            if remainder:
                subpath = "/".join(remainder)
    return FabricLakehouseReference(
        raw_uri=uri,
        is_lakehouse=True,
        lakehouse=lakehouse,
        kind=kind,
        schema=schema,
        table=table,
        subpath=subpath,
    )


def resolve_fabric_files_path(ref: FabricLakehouseReference) -> str:
    """Resolve ``lakehouse://.../files`` URIs to a physical path."""

    template = os.getenv("FABRIC_LAKEHOUSE_FILES_BASE")
    lakehouse = ref.lakehouse or "default"
    subpath = ref.subpath or ""

    if template:
        base = template.replace("{lakehouse}", lakehouse).replace("${lakehouse}", lakehouse)
        base = base.rstrip("/")
        return f"{base}/{subpath}" if subpath else base

    mount_base = os.getenv("FABRIC_LAKEHOUSE_MOUNT_BASE", "/lakehouse")
    base_path = f"{mount_base.rstrip('/')}/{lakehouse}/Files"
    if subpath:
        return f"{base_path}/{subpath}"
    return base_path
