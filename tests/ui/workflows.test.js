'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
function fixture(view){
 const nodes=new Map();for(const id of ['view','main','notice','connection'])nodes.set(id,{innerHTML:'',textContent:'',dataset:{},querySelectorAll:()=>[],focus(){}});
 const context=vm.createContext({console,URL,FormData,Intl,Date,location:{hash:'#'+view,origin:'http://fixture'},document:{getElementById:id=>nodes.get(id)||null,querySelectorAll:()=>[],addEventListener(){}},window:{addEventListener(){},scrollTo(){}},sessionStorage:{getItem(){return '[]'},setItem(){}},fetch:()=>new Promise(()=>{}),setInterval(){},setTimeout(){}});
 for(const file of ['workflows.js','app.js'])vm.runInContext(fs.readFileSync('application/static/'+file,'utf8'),context);
 return {run:code=>vm.runInContext(code,context),context,nodes};
}
test('three user-selected modes have distinct navigation, titles and input flows',()=>{
 const index=fs.readFileSync('application/static/index.html','utf8');
 for(const [route,title] of [['search','Search'],['research','Deep research'],['analysis','Document analysis']]){
  assert.match(index,new RegExp('href="#'+route+'"[^>]*>'+title));
  const f=fixture(route);f.run('render()');assert.equal(f.context.document.title,title+' · Agentic RAG');
 }
 const search=fixture('search').run('searchView()');assert.doesNotMatch(search,/mode-search|mode-deep/);
 const analysis=fixture('analysis').run('workflowView(route())');assert.match(analysis,/without adding it to a collection/);assert.match(analysis,/Private to this browser session/);
 const research=fixture('research').run('workflowView(route())');assert.match(research,/Local collections only/);assert.match(research,/Search budget/);
});
test('late polling cannot replace newer streamed report text',()=>{
 const f=fixture('research');const next={id:'one',report:'old',event_cursor:1,status:'running'},prior={id:'one',report:'new',event_cursor:2};
 f.context.next=next;f.context.prior=prior;const r=f.run('workflowAccept(next,prior)');assert.equal(r.report,'new');assert.equal(r.event_cursor,2);
});
test('document navigation hides stale details and discards a delayed response for another route',async()=>{
 const f=fixture('document/A');
 f.run("workflowState.document={id:'A',filename:'Private document A'};location.hash='#document/B';");
 assert.doesNotMatch(f.run('workflowView(route())'),/Private document A/);
 f.context.payload={id:'A',filename:'Private document A',run:null,promotions:[]};
 f.run("location.hash='#document/A';workflowState.document=null;workflowState.loading=false;workflowSession=async()=>({});api=()=>new Promise(resolve=>globalThis.finishRequest=resolve);globalThis.pendingRefresh=refreshWorkflows();");
 await Promise.resolve();f.run("location.hash='#document/B';finishRequest(payload);");await f.context.pendingRefresh;
 assert.notEqual(f.run('workflowState.document?.id'),'A');
});

test('owned document refresh enables actions that waited for document loading',async()=>{
 const f=fixture('analysis'),controls=[{disabled:true,dataset:{waitingDocument:''}},{disabled:true,dataset:{waitingDocument:''}}];
 f.nodes.get('view').querySelectorAll=selector=>selector==='[data-waiting-document]'?controls:[];
 f.nodes.get('view').querySelector=()=>null;
 for(const id of ['document-status','document-expiry','document-question','document-promotions'])f.nodes.set(id,{innerHTML:'',textContent:'',disabled:true});
 f.nodes.set('document-question-form',{querySelector:()=>({disabled:true})});
 f.context.payload={id:'A',filename:'Owned.pdf',ready:true,expires_at:'2027-01-01T00:00:00Z',run:null,promotions:[]};
 f.run("location.hash='#document/A';workflowState.loading=false;workflowSession=async()=>({});api=async()=>payload;globalThis.refresh=refreshWorkflows();");await f.context.refresh;
 assert.equal(controls.every(button=>!button.disabled),true);
});
test('expired document refresh clears cached private detail and report',async()=>{
 const f=fixture('analysis');
 f.run("location.hash='#document/A';workflowState.loading=false;workflowState.document={id:'A',filename:'Expired private detail'};workflowState.documentRun={id:'report',report:'Expired private report'};workflowSession=async()=>({});api=async()=>{const e=new Error('Document unavailable');e.status=404;throw e};globalThis.refresh=refreshWorkflows();");await f.context.refresh;
 assert.equal(f.run('workflowState.document'),null);assert.equal(f.run('workflowState.documentRun'),null);
 assert.doesNotMatch(f.nodes.get('view').innerHTML,/Expired private/);
});

test('web research is an unchecked per-job option and permits web-only scope only after opt-in',()=>{
 const f=fixture('research');f.run('workflowState.webAvailable=true;state.loaded=true;state.selected.clear();');
 let html=f.run('workflowView(route())');assert.match(html,/Include web search/);assert.doesNotMatch(html,/<input[^>]*id="research-web"[^>]*checked/);
 assert.match(html,/<button[^>]*disabled[^>]*>Start research/);
 f.run('workflowState.includeWeb=true;');html=f.run('workflowView(route())');assert.doesNotMatch(html,/<button[^>]*disabled[^>]*>Start research/);
});
test('web choice is included in submitted job and resets after successful admission',async()=>{
 const f=fixture('research');
 const node=(value='')=>({value,checked:false,dataset:{},disabled:false,events:{},querySelectorAll:()=>[],setAttribute(){},getAttribute(){return null},addEventListener(name,fn){this.events[name]=fn;}});
 const form=node(),question=node('Public research question'),budget=node('1'),web=node();web.checked=true;
 for(const [id,value] of [['research-form',form],['research-question',question],['research-budget',budget],['research-web',web]])f.nodes.set(id,value);
 let submitted;f.context.submit=async(url,body)=>{submitted=body;return {id:'job',include_web:body.include_web};};
 f.run('workflowState.includeWeb=true;workflowState.webAvailable=true;workflowWrite=submit;workflowKey=()=>"fixture";bindWorkflows();');
 await form.events.submit({preventDefault(){},currentTarget:form});assert.equal(submitted.include_web,true);assert.equal(submitted.collection_ids.length,0);assert.equal(f.run('workflowState.includeWeb'),false);
});
test('external sources are explicitly labeled in research reports',()=>{
 const f=fixture('research');f.context.run={id:'run',status:'completed',include_web:true,collection_ids:[],queries:['Q'],completed_queries:['Q'],sources:[{citation_id:'S1',title:'Public guide',uri:'https://public.example/guide',evidence_origin:'web_search'}],warnings:[],report:'Grounded report',search_budget:1};
 assert.match(f.run('workflowReport(run)'),/External web/);
});
