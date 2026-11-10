"""Fail closed on native stage error envelopes before derived searchable output."""
STAGE_FIELDS=('metadata','page_elements_v3','table_structure_v1','table_structure_ocr_v1','graphic_elements_ocr_v1','ocr')

def require_successful_upstream(frame):
    for _,row in frame.iterrows():
        for name in STAGE_FIELDS:
            value=row.get(name)
            if isinstance(value,dict) and value.get('error'):
                raise ValueError('Upstream stage error prevents qualification admission: '+name)
        for name in ('images','chart','infographic','table'):
            values=row.get(name)
            if isinstance(values,list) and any(isinstance(v,dict) and v.get('error') for v in values):
                raise ValueError('Upstream element error prevents qualification admission: '+name)
    return frame
