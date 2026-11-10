"""Render application configuration and split individual Run:ai workspace requests."""
import argparse,json
from pathlib import Path
from render import render
p=argparse.ArgumentParser();p.add_argument('--values',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
root=Path(__file__).resolve().parents[1];values=json.loads(a.values.read_text())
a.output.mkdir(parents=True,exist_ok=True)
for name in ('model-bindings','model-contracts'):
 data=render(json.loads((root/'config'/(name+'.json.template')).read_text()),values)
 out=a.output/'config'/(name+'.json');out.parent.mkdir(exist_ok=True);out.write_text(json.dumps(data,indent=2)+'\n')
(a.output/'config/location-roots.json').write_text('{}\n')
workspaces=render(json.loads((root/'deployment/runai/workspaces.json.template').read_text()),values)
for role,data in workspaces.items():
 out=a.output/'workspaces'/(role+'.json');out.parent.mkdir(exist_ok=True);out.write_text(json.dumps(data,indent=2)+'\n')
services=render(json.loads((root/'deployment/runai/services.json.template').read_text()),values)
(a.output/'services.json').write_text(json.dumps(services,indent=2)+'\n')
print('Rendered configuration, services and',len(workspaces),'workspace requests')
