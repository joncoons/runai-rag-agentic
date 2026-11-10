"""Reuse only a deterministic HTML transform; publication authority is never cached."""
import asyncio
import hashlib
import sys
from importlib.metadata import version
from functools import lru_cache
from pathlib import Path
from ingestion.document_formats import inspect_retained_html
from .repository import Conflict
from .provenance import canonical


def parser_identity():
    return (sys.version, version("beautifulsoup4"), version("soupsieve"))


@lru_cache(maxsize=1)
def normalizer_digest():
    import ingestion.document_formats as formats
    import application.web_acquisition as acquisition
    digest = hashlib.sha256(b'html-normalization-proof.v1\0')
    digest.update(canonical(parser_identity()))
    from application import html_disclosures, html_disclosures_v2, html_disclosures_v3, legal_text_budget
    for module in (formats, acquisition, html_disclosures, html_disclosures_v2, html_disclosures_v3, legal_text_budget):
        digest.update(Path(module.__file__).read_bytes())
        digest.update(b'\0')
    return digest.hexdigest()


async def verify_normalization(c, raw, native):
    raw_sha = hashlib.sha256(raw).hexdigest()
    code_sha = hashlib.sha256((normalizer_digest()+'\0'+native['transform']['version']+'\0'+native['transform'].get('input_contract_sha256','')).encode()).hexdigest()
    units_sha = hashlib.sha256(canonical(native)).hexdigest()
    available = await c.fetchval("SELECT to_regclass('collection_control.html_normalizations') IS NOT NULL")
    if available:
        saved = await c.fetchval('SELECT units_sha256 FROM collection_control.html_normalizations WHERE raw_sha256=$1 AND normalizer_sha256=$2', raw_sha, code_sha)
        if saved is not None:
            if saved != units_sha:
                raise Conflict('HTML unit differs from verified retained original page')
            return
    inspected = await asyncio.to_thread(inspect_retained_html, raw, html_version=native['transform']['version'],html_input_contract=native['transform'].get('input_contract_sha256'))
    expected = {'schema_version': 'native-source-units.v1', **{k: inspected[k] for k in ('source_kind', 'transform', 'units')}}
    if canonical(expected) != canonical(native):
        raise Conflict('HTML unit differs from retained original page')
    # Cache insertion must not introduce cross-collection unique-key waits.
    if available and await c.fetchval('SELECT pg_try_advisory_xact_lock(7271463921007)'):
        await c.execute('INSERT INTO collection_control.html_normalizations(raw_sha256,normalizer_sha256,units_sha256) VALUES($1,$2,$3) ON CONFLICT DO NOTHING', raw_sha, code_sha, units_sha)
        saved = await c.fetchval('SELECT units_sha256 FROM collection_control.html_normalizations WHERE raw_sha256=$1 AND normalizer_sha256=$2', raw_sha, code_sha)
        if saved != units_sha:
            raise Conflict('Conflicting deterministic HTML normalization receipt')
