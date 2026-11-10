"""Explicit NeMo Retriever 26.8.2 blueprint registration for durable stages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse


BLUEPRINT_NAME = "durable-pdf-extraction-26.8.2"


def _http_url(name: str, value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{name} must be an absolute HTTP(S) URL")
    return value.rstrip("/")


@dataclass(frozen=True, slots=True)
class PdfExtractionBlueprintConfig:
    page_elements_url: str
    ocr_url: str
    table_structure_url: str = ""
    method: str = "pdfium_hybrid"
    extract_text: bool = True
    extract_images: bool = True
    extract_tables: bool = True
    extract_charts: bool = True
    extract_infographics: bool = True
    use_page_elements: bool = True
    use_table_structure: bool = True
    dpi: int = 200
    inference_batch_size: int = 8
    request_timeout_seconds: float = 120.0

    def __post_init__(self) -> None:
        _http_url("page_elements_url", self.page_elements_url)
        _http_url("ocr_url", self.ocr_url)
        if self.extract_tables and self.use_table_structure:
            _http_url("table_structure_url", self.table_structure_url)
        if self.dpi < 72 or self.inference_batch_size < 1:
            raise ValueError("dpi and inference_batch_size are outside safe bounds")
        if self.request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be positive")

    def normalized(self) -> dict[str, Any]:
        return {
            field: getattr(self, field)
            for field in self.__dataclass_fields__
        }


def register_pdf_extraction_blueprint(registry: Any, config: PdfExtractionBlueprintConfig):
    """Register the supported extraction-only 26.8.2 graph composition."""

    from nemo_retriever.graph.ingestor_runtime import build_graph
    from nemo_retriever.common.params import ExtractParams

    @registry.register(
        BLUEPRINT_NAME,
        description="Durable PDF extraction without caption, embed, or VDB effects",
        version="26.8.2",
        tags=["durable", "pdf", "extraction-only"],
        overwrite=True,
    )
    def blueprint():
        params = ExtractParams(
            method=config.method,
            extract_text=config.extract_text,
            extract_images=config.extract_images,
            extract_tables=config.extract_tables,
            extract_charts=config.extract_charts,
            extract_infographics=config.extract_infographics,
            use_page_elements=config.use_page_elements,
            use_table_structure=config.use_table_structure,
            dpi=config.dpi,
            inference_batch_size=config.inference_batch_size,
            request_timeout_s=config.request_timeout_seconds,
            page_elements_invoke_url=_http_url(
                "page_elements_url", config.page_elements_url
            ),
            ocr_invoke_url=_http_url("ocr_url", config.ocr_url),
            table_structure_invoke_url=(
                _http_url("table_structure_url", config.table_structure_url)
                if config.extract_tables and config.use_table_structure
                else None
            ),
        )
        return build_graph(
            extraction_mode="pdf",
            extract_params=params,
            split_config={},
            caption_params=None,
            embed_params=None,
            vdb_upload_params=None,
            stage_order=(),
        )

    return blueprint
