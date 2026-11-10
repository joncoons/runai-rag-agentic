"""Operate a single explicitly identified native Run:ai workspace."""
import argparse,json,os,ssl,urllib.request,urllib.parse
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument("action",choices=["create","get","stop","run","delete"]);p.add_argument("--request",type=Path,required=True);p.add_argument("--workload-id");a=p.parse_args()
body=json.loads(a.request.read_text());base=os.environ["RUNAI_URL"].rstrip("/");token=os.environ["RUNAI_TOKEN"]
if not base.startswith("https://"):raise SystemExit("RUNAI_URL must use HTTPS")
ctx=ssl.create_default_context(cafile=os.environ.get("RUNAI_CA_FILE"))
def request(path,method="GET",data=None):
 req=urllib.request.Request(base+path,method=method,data=None if data is None else json.dumps(data).encode(),headers={"Authorization":"Bearer "+token,"Content-Type":"application/json"})
 with urllib.request.urlopen(req,context=ctx,timeout=60) as response:
  raw=response.read();return json.loads(raw) if raw else {}
path="/api/v1/workloads/workspaces"
if a.action=="create":
 if "${" in json.dumps(body):raise SystemExit("Render placeholders first")
 result=request(path,"POST",body);print(json.dumps({"name":body["name"],"workloadId":result.get("workloadId")}))
else:
 if not a.workload_id:raise SystemExit("--workload-id is required")
 path+="/"+urllib.parse.quote(a.workload_id,safe="");current=request(path)
 if current.get("name")!=body["name"] or str(current.get("projectId"))!=str(body["projectId"]):raise SystemExit("Workspace name/project mismatch")
 if a.action=="get":print(json.dumps({k:current.get(k) for k in ("name","actualPhase","desiredPhase")}))
 elif a.action=="delete":request(path,"DELETE");print("Workspace deleted; existing non-ephemeral PVCs retained")
 else:request(path+("/suspend" if a.action=="stop" else "/resume"),"POST");print("Requested",a.action,body["name"])
