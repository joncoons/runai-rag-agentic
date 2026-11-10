"""Canonical element projection for page-oriented NeMo Retriever records."""

from __future__ import annotations

from typing import Any


_GRAPHIC_COLUMNS = ("chart", "infographic")


def _has_items(value: Any) -> bool:
    return isinstance(value, list) and any(isinstance(item, dict) for item in value)


def canonicalize_page_content(batch_df: Any) -> Any:
    """Apply canonical 26.5 element precedence before caption and embedding.

    Extraction graphs emit page rows that can contain native text or OCR text,
    a page image, and structured elements at the same time. The canonical
    NV-Ingest writer retains native PDF text but suppresses redundant OCR text
    on scanned pages. Charts and infographics replace the page image; tables
    coexist with an image because the visual and structured forms carry distinct
    information.

    The operation is idempotent so replay may safely reapply it after
    a durable serialization boundary.
    """
    import pandas as pd

    if not isinstance(batch_df, pd.DataFrame) or batch_df.empty:
        return batch_df

    projected = batch_df.copy(deep=True)
    for index, row in projected.iterrows():
        if any(_has_items(row.get(column)) for column in _GRAPHIC_COLUMNS):
            projected.at[index, "images"] = []

        metadata = row.get("metadata")
        if (
            isinstance(metadata, dict)
            and not metadata.get("has_text")
            and metadata.get("needs_ocr_for_text")
        ):
            projected.at[index, "text"] = ""

    return projected
