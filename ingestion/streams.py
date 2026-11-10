"""Explicit stream isolation for application deployments sharing a broker."""
import os
import re

def stream_name(suffix):
    prefix=os.environ.get('INGEST_STREAM_PREFIX','ingest')
    if not re.fullmatch(r'[a-z][a-z0-9.-]{0,63}',prefix):raise ValueError('Invalid ingestion stream prefix')
    return prefix+'.'+suffix
