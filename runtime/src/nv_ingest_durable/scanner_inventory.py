"""Adapter for PDF Corpus Scanner CSV inventories."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .coordinator import SourceDocument


def _true(value: str, *, default: bool = False) -> bool:
    normalized = (value or "").strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "yes", "y"}


@dataclass(frozen=True, slots=True)
class ScannerInventory:
    documents: tuple[SourceDocument, ...]
    total_rows: int
    document_rows: int
    duplicate_rows_filtered: int
    error_rows_filtered: int
    missing_paths_filtered: int


SCANNER_METADATA_FIELDS = (
    "relative_path",
    "directory",
    "filename",
    "bytes",
    "page_count",
    "text_native_pages",
    "scanned_candidate_pages",
    "mixed_pages",
    "uncertain_pages",
    "image_heavy_pages",
    "table_count",
    "image_count",
    "primary_stress",
    "visual_bucket",
    "balance_key",
    "scan_seconds",
)


def load_scanner_inventory(
    csv_path: Path,
    *,
    require_existing: bool = True,
    include_scan_errors: bool = False,
) -> ScannerInventory:
    documents: list[SourceDocument] = []
    seen_paths: set[Path] = set()
    total_rows = document_rows = duplicate_filtered = error_filtered = missing = 0

    with csv_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            total_rows += 1
            if (row.get("row_type") or "document").strip() != "document":
                continue
            document_rows += 1
            if not _true(row.get("duplicate_keep", ""), default=True):
                duplicate_filtered += 1
                continue
            if row.get("error", "").strip() and not include_scan_errors:
                error_filtered += 1
                continue

            raw_path = (
                row.get("duplicate_canonical_path", "").strip()
                or row.get("path", "").strip()
            )
            if not raw_path:
                missing += 1
                continue
            path = Path(raw_path).resolve()
            if require_existing and not path.is_file():
                missing += 1
                continue
            if path in seen_paths:
                duplicate_filtered += 1
                continue
            seen_paths.add(path)
            metadata: dict[str, Any] = {
                "scanner_csv": str(csv_path.resolve()),
                "duplicate_key": row.get("duplicate_key", ""),
                "duplicate_group_size": row.get("duplicate_group_size", "1"),
            }
            metadata.update(
                {
                    f"scanner_{field}": row.get(field, "")
                    for field in SCANNER_METADATA_FIELDS
                    if row.get(field, "") != ""
                }
            )
            documents.append(SourceDocument(path=path, metadata=metadata))

    return ScannerInventory(
        documents=tuple(documents),
        total_rows=total_rows,
        document_rows=document_rows,
        duplicate_rows_filtered=duplicate_filtered,
        error_rows_filtered=error_filtered,
        missing_paths_filtered=missing,
    )
