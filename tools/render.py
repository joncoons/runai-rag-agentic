"""Render JSON templates with explicit private values; no implicit environment expansion."""
import argparse,json,re
from pathlib import Path
TOKEN=re.compile(r"\$\{([A-Z0-9_]+)\}")
def render(value,values):
 if isinstance(value,dict):return {k:render(v,values) for k,v in value.items()}
 if isinstance(value,list):return [render(v,values) for v in value]
 if not isinstance(value,str):return value
 def replace(m):
  name=m.group(1)
  if name not in values or values[name] in (None,"") or str(values[name]).startswith("REPLACE_"):raise ValueError("Set private value: "+name)
  return str(values[name])
 return TOKEN.sub(replace,value)
if __name__=="__main__":
 p=argparse.ArgumentParser(description=__doc__);p.add_argument("template",type=Path);p.add_argument("--values",type=Path,required=True);p.add_argument("--output",type=Path,required=True);a=p.parse_args()
 data=render(json.loads(a.template.read_text()),json.loads(a.values.read_text()))
 a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(data,indent=2)+"\n")
