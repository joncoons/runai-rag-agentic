"""Supplement original Office structure with retained native visual evidence.

Rendering is a distinct versioned derivative. This adapter never publishes a VDB,
changes structural units, or generates captions.
"""
from __future__ import annotations
import asyncio
import hashlib
from io import BytesIO
import json
from pathlib import Path
import subprocess
import tempfile
import time
from urllib.parse import unquote,urlparse
import zipfile

from collection_control.artifacts import canonical
from collection_control.provenance import verified_file
from collection_control.office_visuals import (
    SCHEMA,RENDERER,MAX_FRAMES,office_inventory,prepare_office,normalized_png,
    raster_pages,frame_unit,sha,verify_office_visuals,
)


class OfficeVisualProcessor:
    def __init__(self,*,root,raw_root=None,renderer=None,runner=None):
        self.root=Path(root)
        self.raw_root=Path(raw_root) if raw_root is not None else self.root.parent/'raw'
        self.renderer=renderer
        self.runner=runner

    def _retain(self,raw,suffix):
        from nv_ingest_durable.deferred_caption import retain
        uri,digest=retain(self.raw_root,raw,suffix)
        return {'uri':uri,'sha256':digest,'size_bytes':len(raw)}

    def _render(self,raw,filename):
        """Bounded CPU render with a unique profile and exact admitted binary."""
        binary=Path(RENDERER['binary_path'])
        if sha(binary.read_bytes())!=RENDERER['binary_sha256']:
            raise ValueError('Office renderer binary differs from qualified contract')
        version=subprocess.run(['libreoffice','--version'],check=True,capture_output=True,text=True,timeout=10).stdout.strip()
        if version!='LibreOffice '+RENDERER['version']:
            raise ValueError('Office renderer version differs from qualified contract')
        suffix=Path(filename).suffix.lower()
        if suffix not in ('.docx','.pptx'):raise ValueError('Office renderer requires DOCX/PPTX')
        with tempfile.TemporaryDirectory(prefix='office-render-') as folder:
            root=Path(folder);source=root/('input'+suffix);source.write_bytes(raw)
            profile=(root/'profile').as_uri()
            result=subprocess.run(['libreoffice','-env:UserInstallation='+profile,
                '--headless','--norestore','--nodefault','--nolockcheck','--convert-to','pdf',
                '--outdir',str(root),str(source)],check=True,capture_output=True,timeout=60)
            output=root/'input.pdf'
            if not output.is_file() or output.stat().st_size>128*1024*1024:
                raise ValueError('Office renderer did not produce a bounded PDF')
            data=output.read_bytes()
            if not data.startswith(b'%PDF-'):raise ValueError('Office renderer returned invalid PDF')
            return data

    async def extract(self,raw,filename,source_units,*,raw_uri=None):
        """Return (supplemental proof, visual units), preserving original units."""
        if raw_uri is None:raise ValueError('Office visuals require original retained raw URI')
        return await asyncio.to_thread(self._extract,raw,filename,source_units,raw_uri)

    def _extract(self,raw,filename,source_units,raw_uri):
        from .image_extraction import NativeImageProcessor,_json_value,image_contract,_models,_config
        started=time.monotonic()
        def budget():
            if time.monotonic()-started>RENDERER['total_budget_seconds']:
                raise ValueError('Office visual extraction exceeded total render budget')
        if verified_file(self.raw_root,raw_uri,sha(raw))!=raw:raise ValueError('Office retained source differs')
        inventory=office_inventory(raw,filename,source_units)
        prepared=prepare_office(raw,inventory)
        source_binding=self._retain(prepared,Path(filename).suffix.lower())
        empty=inventory['source_kind']=='pptx' and not inventory['slides'] and not inventory['media'] and not inventory['visual_parts']
        pdf=None if empty else (self.renderer or self._render)(prepared,filename)
        budget()
        if not empty and (not isinstance(pdf,bytes) or len(pdf)>128*1024*1024 or not pdf.startswith(b'%PDF-')):
            raise ValueError('Office renderer did not return bounded PDF bytes')
        rendered_pdf=None if empty else self._retain(pdf,'.pdf')
        model,config=image_contract()
        proof={'schema_version':SCHEMA,'status':'complete','filename':filename,
            'original':{'uri':raw_uri,'sha256':sha(raw),'size_bytes':len(raw)},
            'source_units_sha256':sha(canonical(source_units)),'inventory':inventory,
            'prepared_source':source_binding,'rendered_pdf':rendered_pdf,'renderer':dict(RENDERER),
            'native':{'library_version':'26.8.2','model_digest':model,'config_digest':config,'models':_models(),'config':_config()},
            'frames':[]}
        output=[]
        runner=self.runner or NativeImageProcessor(root=self.root,raw_root=self.raw_root)._run_native
        def frame(identity,kind,scope,data,width,height,derivative):
            budget()
            if len(proof['frames'])>=MAX_FRAMES:raise ValueError('Office visual frame count exceeds bound')
            raster=self._retain(data,'.png')
            native=runner(str(Path(unquote(urlparse(raster['uri']).path))))
            budget()
            if isinstance(native,dict) and 'result_data' in native:native=native['result_data']
            if hasattr(native,'to_dict'):native=native.to_dict(orient='records')
            rows=_json_value(native)
            entry={'frame_id':identity,'kind':kind,'source':scope,'raster':raster,
                'width':width,'height':height,'derivative':derivative}
            # Validate all nested native errors even when no grounded text exists.
            unit=frame_unit(entry,rows)
            evidence={'schema_version':'native-office-frame.v1','frame_id':identity,
                'raster_sha256':sha(data),'model_digest':model,'config_digest':config,'rows':rows}
            entry['evidence']=self._retain(canonical(evidence),'.json')
            proof['frames'].append(entry)
            if unit is not None:output.append(unit)
        page_count=0
        for index,(data,width,height) in enumerate(() if empty else raster_pages(pdf),1):
            scope={'part':'word/document.xml','precision':'document','unit_ids':inventory['source_unit_ids']}
            if inventory['source_kind']=='pptx':
                if index>len(inventory['slides']):raise ValueError('Office renderer invented an extra slide')
                slide=inventory['slides'][index-1]
                scope={'part':slide['anchors']['part'],'precision':'slide','unit_ids':[slide['unit_id']]}
            frame('page:'+str(index),'rendered_page',scope,data,width,height,
                {'page_number':index,'coordinate_frame':'rendered_page_pixels'})
            page_count=index
        if inventory['source_kind']=='pptx' and page_count!=len(inventory['slides']):
            raise ValueError('Office renderer omitted original or hidden slides')
        with zipfile.ZipFile(BytesIO(raw)) as package:
            for media in inventory['media']:
                data,width,height=normalized_png(package.read(media['part']))
                for number,scope in enumerate(media['occurrences'],1):
                    frame('media:'+media['part']+':'+str(number),'embedded_media',scope,data,width,height,
                        {'package_part':media['part'],'coordinate_frame':'normalized_media_pixels'})
        proof['visual_units_sha256']=sha(canonical(output))
        budget()
        # Check the exact same admission routine used by the independent publisher.
        verified=verify_office_visuals(self.raw_root,proof,sha(raw),source_units)
        if canonical(verified)!=canonical(output):raise ValueError('Office visual verification differs')
        budget()
        return proof,output
