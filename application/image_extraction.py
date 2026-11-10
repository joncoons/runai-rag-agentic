"""Native NeMo image evidence within the outer durable extraction transaction.

Inline assets terminate before content explosion, embedding, captions or VDB
upload. The parent webpage materializer consumes the immutable evidence.
"""
import asyncio
import hashlib
from io import BytesIO
import json
import math
import os
from pathlib import Path
from urllib.parse import unquote, urlparse
from PIL import Image, ImageOps, __version__ as pillow_version
from collection_control.provenance import verified_file
from nv_ingest_durable.artifacts import JsonResultStager
from nv_ingest_durable.contracts import Stage, StageResult
from nv_ingest_durable.deferred_caption import retain
from nv_ingest_durable.stage_processing import downstream_envelope
from nv_ingest_durable.streams import stream_name

LIBRARY_VERSION = "26.8.2"
MAX_PIXELS = 32_000_000


def _hash(value):
    return "sha256:" + hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False,allow_nan=False).encode()).hexdigest()


def _config():
    return {"library_version":LIBRARY_VERSION,"extraction_mode":"image","method":"pdfium_hybrid",
        "extract_text":True,"extract_images":True,"extract_tables":True,"extract_charts":True,
        "extract_infographics":True,"use_page_elements":True,"use_table_structure":True,
        "ocr_version":"v2","dpi":200,"inference_batch_size":2,
        "split_config":{},"stage_order":[],"caption":False,"embedding":False,"vdb_upload":False,
        "optional_caption_handoff":{"minimum_policy":"native-caption-library","adapter_sha256":hashlib.sha256(Path(__file__).with_name("web_captions.py").read_bytes()).hexdigest()},
        "normalization":{"format":"PNG","mode":"RGB","exif_transpose":True,"alpha_background":"white",
            "multi_frame":"reject","max_pixels":MAX_PIXELS,"pillow_version":pillow_version,
            "svg":{"policy":"safe-svg-v2","renderer":"CairoSVG","renderer_version":__import__("importlib.metadata",fromlist=["version"]).version("CairoSVG"),"adapter_sha256":hashlib.sha256(Path(__file__).with_name("svg_images.py").read_bytes()).hexdigest()}}}


def _models():
    configured=os.environ.get("MODEL_CONTRACTS_FILE")
    path=Path(configured) if configured else Path("/app/deployment/application/workspaces-model-contracts.json")
    if not configured and not path.is_file():
        path=Path(__file__).resolve().parent.parent/"deployment/application/workspaces-model-contracts.json"
    models=json.loads(path.read_text())["extraction_model_contract"]
    selected={name:models[name] for name in ("page-elements","ocr","table-structure")}
    for model in selected.values():
        checkpoint=model.get("checkpoint_manifest_sha256","")
        if len(checkpoint)!=64 or any(c not in "0123456789abcdef" for c in checkpoint):
            raise ValueError("Native image model contract requires exact checkpoint hashes")
        image=model.get("platform_image_digest","")
        if not image.startswith("sha256:") or len(image)!=71 or any(c not in "0123456789abcdef" for c in image[7:]):
            raise ValueError("Native image model contract requires exact platform image hashes")
    return selected


def image_contract():
    """Return (model digest, config digest), including all three admitted models."""
    return _hash(_models()),_hash(_config())


def _json_value(value):
    """Sanitize native scalars/arrays without carrying inline raster bytes."""
    if isinstance(value,dict):
        return {str(k):_json_value(v) for k,v in value.items() if k not in ("image_b64","bytes")}
    if isinstance(value,(list,tuple)):
        return [_json_value(v) for v in value]
    if value is None or isinstance(value,(str,bool,int)):
        return value
    if isinstance(value,float):
        return value if math.isfinite(value) else None
    if hasattr(value,"tolist"):
        return _json_value(value.tolist())
    if hasattr(value,"item"):
        return _json_value(value.item())
    raise ValueError("Unsupported value in native image evidence: " + type(value).__name__)


def _has_error(value):
    if isinstance(value,dict):
        return bool(value.get("error")) or any(_has_error(v) for k,v in value.items() if k!="error")
    if isinstance(value,list):
        return any(_has_error(v) for v in value)
    return False


class NativeImageProcessor:
    def __init__(self,*,root,raw_root=None,runner=None,max_pixels=MAX_PIXELS):
        self.root=Path(root)
        self.raw_root=Path(raw_root) if raw_root is not None else self.root.parent/"raw"
        self.runner=runner
        self.max_pixels=max_pixels

    def _normalize(self,raw):
        svg=None
        if raw.lstrip(b'\xef\xbb\xbf \t\r\n').startswith(b'<'):
            from .svg_images import rasterize_svg
            raw,svg=rasterize_svg(raw,max_pixels=self.max_pixels)
        with Image.open(BytesIO(raw)) as image:
            if getattr(image,"n_frames",1)!=1:
                raise ValueError("Multi-frame images require an explicitly qualified frame policy")
            width,height=image.size
            if width<1 or height<1 or width*height>self.max_pixels:
                raise ValueError("Image exceeds admitted pixel dimensions")
            original_mode=image.mode;original_format=image.format
            orientation=image.getexif().get(274)
            image=ImageOps.exif_transpose(image)
            if image.mode in ("RGBA","LA") or (image.mode=="P" and "transparency" in image.info):
                rgba=image.convert("RGBA")
                normalized=Image.new("RGB",rgba.size,"white")
                normalized.paste(rgba,mask=rgba.getchannel("A"))
            else:
                normalized=image.convert("RGB")
            # Metadata is recorded in lineage rather than copied into raster bytes.
            normalized.info.clear()
            buf=BytesIO();normalized.save(buf,format="PNG")
            data=buf.getvalue()
            uri,sha=retain(self.raw_root,data,".png")
            return {"uri":uri,"sha256":sha,"width":normalized.width,"height":normalized.height,"mode":"RGB",
                "transform":{**_config()["normalization"],"original_mode":original_mode,"original_format":original_format,
                    "original_width":width,"original_height":height,"original_exif_orientation":orientation,**({"original_format":"SVG","original_mode":"vector","svg":svg} if svg is not None else {})}},str(Path(unquote(urlparse(uri).path)))

    def _run_native(self,path):
        from nemo_retriever.common.params import ExtractParams
        from nemo_retriever.graph import InprocessExecutor
        from nemo_retriever.graph.ingestor_runtime import build_graph
        from importlib.metadata import version
        actual=version("nemo-retriever")
        if actual!=LIBRARY_VERSION:
            raise ValueError("Native image library version differs from contract: "+actual)
        params=ExtractParams(method="pdfium_hybrid",extract_text=True,extract_images=True,extract_tables=True,
            extract_charts=True,extract_infographics=True,use_page_elements=True,use_table_structure=True,
            ocr_version="v2",dpi=200,inference_batch_size=_config()["inference_batch_size"],
            page_elements_invoke_url=os.environ["PAGE_ELEMENTS_URL"],ocr_invoke_url=os.environ["OCR_URL"],
            table_structure_invoke_url=os.environ["TABLE_STRUCTURE_URL"])
        graph=build_graph(extraction_mode="image",extract_params=params,split_config={},caption_params=None,
            embed_params=None,vdb_upload_params=None,stage_order=())
        return InprocessExecutor(graph,show_progress=False).ingest(path)

    async def process(self,envelope,revision):
        model_digest,config_digest=image_contract()
        if envelope.stage!=Stage.EXTRACTION or envelope.model_digest!=model_digest or envelope.config_digest!=config_digest:
            raise ValueError("Native image envelope differs from qualified image contract")
        if revision["document_id"]!=envelope.document_id or revision["raw_uri"]!=envelope.artifact_uri:
            raise ValueError("Native image source identity differs from revision")
        raw=await asyncio.to_thread(verified_file,self.raw_root,envelope.artifact_uri,revision["raw_sha256"])
        normalized,path=await asyncio.to_thread(self._normalize,raw)
        result=await asyncio.to_thread(self.runner or self._run_native,path)
        if isinstance(result,dict) and "result_data" in result:result=result["result_data"]
        if hasattr(result,"to_dict"):result=result.to_dict(orient="records")
        rows=_json_value(result)
        if not isinstance(rows,list) or len(rows)!=1 or not isinstance(rows[0],dict):
            raise ValueError("Native image extraction must return exactly one image frame")
        if _has_error(rows):raise ValueError("Native image extraction failed; inspect retained input and native stage error")
        row=rows[0]
        if row.get("page_number",1)!=1:
            raise ValueError("Native image extraction returned an invalid internal frame identity")
        detections=(row.get("page_elements_v3") or {}).get("detections",[])
        parts=[]
        if isinstance(row.get("text"),str) and row["text"].strip():parts.append(row["text"])
        structured={}
        for kind in ("table","chart","infographic"):
            entries=row.get(kind) or []
            if not isinstance(entries,list):raise ValueError("Native image structured output is not a collection")
            structured[kind]=entries
            for entry in entries:
                if isinstance(entry,dict) and isinstance(entry.get("text"),str) and entry["text"].strip():
                    parts.append(entry["text"])
        body={"schema_version":"native-web-image-evidence.v1","document_id":envelope.document_id,
            "source_revision_id":revision["source_revision_id"],"status":"complete",
            "raw_uri":revision["raw_uri"],"raw_sha256":revision["raw_sha256"],
            "original":{"raw_uri":revision["raw_uri"],"raw_sha256":revision["raw_sha256"]},
            "normalized":normalized,"native":{"library_version":LIBRARY_VERSION,"model_digest":model_digest,
                "config_digest":config_digest,"models":_models(),"config":_config(),
                "coordinate_frame":"normalized_image","internal_frame_number":1,
                "classes":sorted({d["label_name"] for d in detections if isinstance(d,dict) and isinstance(d.get("label_name"),str)}),
                "detections":detections,"text":row.get("text", ""),"structured":structured,"evidence_rows":rows},
            "text":"\n\n".join(parts)}
        artifact,_=await JsonResultStager(self.root).stage(envelope,body)
        notification=downstream_envelope(envelope,stage=Stage.INDEX,artifact_uri=artifact.final_uri,
            config_digest=config_digest,model_digest=model_digest)
        return StageResult(artifact=artifact,downstream=((stream_name("index"),notification),))
