'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
function fixture(view){
 const nodes=new Map();for(const id of ['view','main','notice','connection'])nodes.set(id,{innerHTML:'',textContent:'',dataset:{},querySelectorAll:()=>[],focus(){}});
 const context=vm.createContext({console,URL,FormData,Intl,Date,location:{hash:'#'+view,origin:'http://fixture'},document:{getElementById:id=>nodes.get(id)||null,querySelectorAll:()=>[],addEventListener(){}},window:{addEventListener(){},scrollTo(){}},sessionStorage:{getItem(){return '[]'},setItem(){}},fetch:()=>new Promise(()=>{}),setInterval(){},setTimeout(){}});
 for(const file of ['workflows.js','app.js'])vm.runInContext(fs.readFileSync('application/static/'+file,'utf8'),context);
 return {run:code=>vm.runInContext(code,context),context};
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
