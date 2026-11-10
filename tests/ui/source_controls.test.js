'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('application/static/app.js', 'utf8');
function fixture(view='location') {
  const nodes = new Map(),intervals=[],documentEvents=new Map();
  const element = (id, value='') => { const node={value,checked:false,dataset:{},textContent:'',innerHTML:'',className:'',events:{},querySelectorAll:()=>[],addEventListener(name,fn){this.events[name]=fn;},setAttribute(){},focus(){}};nodes.set(id,node);return node; };
  for (const id of ['view','notice','connection','main']) element(id);
  const context=vm.createContext({console,URL,FormData,Intl,Date,location:{hash:'#'+view+'/c1',origin:'https://app.test'},document:{getElementById:id=>nodes.get(id)||null,querySelectorAll:()=>[],addEventListener(name,fn){documentEvents.set(name,fn);}},window:{addEventListener(){},scrollTo(){}},sessionStorage:{getItem(){return '[]';},setItem(){}},fetch:()=>new Promise(()=>{}),setInterval(fn,ms){intervals.push({fn,ms});},setTimeout(){}});
  vm.runInContext(source, context);
  vm.runInContext("state.collections=[{id:'c1',name:'Documents'}];state.loaded=true;",context);
  return {context,nodes,element,intervals,documentEvents,run:code=>vm.runInContext(code,context)};
}
function schedule(f,prefix){for(const [key,value] of Object.entries({enabled:'',timezone:'America/Chicago',start:'02:00',end:'06:00'}))f.element(prefix+'-'+key,value);}

test('location view offers registered roots, bounded options and optional PDF captions',()=>{
  const f=fixture();
  const html=f.run('locationView()');
  assert.match(html,/Connect location/);
  assert.match(html,/location-uri/);
  assert.match(html,/location-roots/);
  assert.match(html,/Caption eligible PDF images/);
  assert.match(html,/location-depth/);
  assert.match(html,/location-files/);
  assert.doesNotMatch(html,/<input[^>]*id="location-captions"[^>]*checked/);
  assert.doesNotMatch(html,/<input[^>]*id="location-source-enabled"[^>]*checked/);
  assert.match(f.run('collectionView()'),/#location\/c1/);
});

test('observed inventory is terminal and source cards distinguish location from website',()=>{
  const f=fixture('collection');
  assert.equal(f.run("sourceTerminal('observed')"),true);
  f.run("state.sources.set('c1',[{source_id:'s',name:'Reference',kind:'location',location:{uri:'location://docs/reference'},schedule:{enabled:false}}]);state.sourceRuns.set('s',[{state:'observed',counts:{missing:1}}]);");
  const html=f.run("sourcesView('c1')");
  assert.match(html,/Data sources/);
  assert.match(html,/Registered directory/);
  assert.match(html,/location:\/\/docs\/reference/);
  assert.match(html,/observed/);
  assert.doesNotMatch(html,/data-source-action="check"[^>]*disabled/);
});

test('location submission uses location API, defaults schedule off, and includes PDF caption choice',async()=>{
  const f=fixture();const form=f.element('location-form');
  for(const [id,value] of Object.entries({'location-name':'Reference','location-uri':'location://docs/reference','location-depth':'8','location-files':'128','location-hosts':'','form-error':'','location-capture-status':''}))f.element(id,value);
  f.element('location-recursive').checked=true;
  f.element('location-captions').checked=true;
  schedule(f,'location-source');
  const requests=[];
  f.context.fetch=async(url,options={})=>{requests.push({url,options});return {ok:true,json:async()=>url.endsWith('/location-sources')?{source:{source_id:'s'},run:{run_id:'r',state:'queued'}}:{roots:[{alias:'docs',uri:'location://docs/'}],sources:[]}};};
  f.run('bind()');
  await form.events.submit({preventDefault(){}});
  const request=requests.find(r=>r.url.endsWith('/location-sources'));
  assert.ok(request);
  const body=JSON.parse(request.options.body);
  assert.deepEqual(body,{name:'Reference',uri:'location://docs/reference',recursive:true,max_depth:8,max_files:128,allowed_hosts:[],captions_enabled:true,schedule:{enabled:false,timezone:'America/Chicago',start:'02:00',end:'06:00'}});
  assert.equal(f.context.location.hash,'#collection/c1');
});

test('website caption selection is separate from native image processing and reaches API',async()=>{
  const f=fixture('website');const form=f.element('website-form');
  for(const [id,value] of Object.entries({'website-name':'Docs','seed-urls':'https://docs.test/','allowed-hosts':'docs.test','page-prefixes':'https://docs.test/','crawl-depth':'1','crawl-pages':'10','included-extensions':'.pdf','ignored-extensions':'.svg','form-error':'','web-capture-status':''}))f.element(id,value);
  f.element('discover-images').checked=true;f.element('website-captions').checked=true;schedule(f,'new-source');
  const requests=[];f.context.fetch=async(url,options={})=>{requests.push({url,options});return {ok:true,json:async()=>options.method==='POST'?{source:{source_id:'s'},run:{run_id:'r',state:'queued'}}:{sources:[]}};};
  assert.match(f.run('websiteView()'),/Caption webpage images/);
  f.run('bind()');await form.events.submit({preventDefault(){}});
  const body=JSON.parse(requests.find(r=>r.options.method==='POST').options.body);
  assert.equal(body.captions_enabled,true);assert.equal(body.include_images,true);assert.deepEqual(body.page_prefixes,['https://docs.test/']);
});

test('website options cannot enable captions outside saved image acquisition scope',()=>{
  const f=fixture('collection');const dialog=f.element('dialog');
  dialog.querySelector=()=>({});dialog.showModal=()=>{};
  f.element('source-schedule-form');f.element('edit-source-images');
  f.context.savedSource={source_id:'s',name:'Docs',kind:'website',policy:{include_images:false},schedule:{enabled:false}};
  f.run("editSourceSchedule(savedSource,'c1')");
  assert.match(dialog.innerHTML,/<input[^>]*id="edit-source-captions"[^>]*disabled/);
});

test('background root discovery preserves an entered location URI',async()=>{
  const f=fixture();const form=f.element('location-form');const uri=f.element('location-uri','location://docs/typed/subpath');const roots=f.element('location-roots');
  f.context.testForm=form;
  f.context.fetch=async()=>({ok:true,json:async()=>({roots:[{alias:'docs',uri:'location://docs/'}]})});
  await f.run('loadLocationRoots(testForm)');
  assert.equal(uri.value,'location://docs/typed/subpath');
  assert.match(roots.innerHTML,/location:\/\/docs\//);
});

test('one-time admitted inputs are visible and can prefill source configuration',()=>{
 const f=fixture('collection');f.run("state.sources.set('c1',[]);state.oneTimeSources.set('c1',[{uri:'https://docs.test/guide',filename:'guide.html',source_kind:'html'}]);");
 assert.match(f.run("sourcesView('c1')"),/Set up rechecks/);
 f.run("state.sourceDrafts.set('c1',{uri:'https://docs.test/guide',filename:'guide.html',source_kind:'html'});");
 const html=f.run('websiteView()');assert.match(html,/value="guide.html"/);assert.match(html,/>https:\/\/docs.test\/guide<\/textarea>/);assert.match(html,/id="crawl-depth"[^>]*value="0"/);
});

test('source references are safe linked titles with pages, never full excerpts',()=>{
 const f=fixture('search');
 f.run("state.result={answer:'Answer [S1]',sources:[{citation_id:'S1',title:'Student cards',page:2,uri:'https://example.test/student',text:'PRIVATE FULL EXCERPT'},{citation_id:'S2',title:'Local file',uri:'javascript:alert(1)',text:'ANOTHER EXCERPT'}],warnings:[]}");
 const html=f.run('resultsView()');
 assert.doesNotMatch(html,/PRIVATE FULL EXCERPT|ANOTHER EXCERPT|href="javascript:/);
 assert.match(html,/href="https:\/\/example.test\/student"/);
 assert.match(html,/Student cards.*Page 2/);
});

test('missing or relative source URIs never fabricate links to the app host',()=>{
 const f=fixture('search');
 for(const value of [undefined,null,'','/relative-source'])assert.equal(f.run('safeUri('+JSON.stringify(value)+')'),null);
});

test('opaque downloaded filenames use the original webpage URL as link label',()=>{
 const f=fixture('search');f.run("state.result={answer:'Answer',sources:[{citation_id:'S1',title:'"+'a'.repeat(64)+".html',uri:'https://example.test/student-cards'}],warnings:[]}");
 const html=f.run('resultsView()');assert.match(html,/>https:\/\/example.test\/student-cards<\/a>/);assert.doesNotMatch(html,new RegExp('a'.repeat(64)));
});

test('caption rows show inference progress independently of publication',()=>{
 const f=fixture('collection');
 const html=f.run("imageOriginsView({image_origins:{webpage:{unique_images:10,associations:10,pending:10,queued:6,generating:1,generated:3,publishing:3,completed:0}}})");
 assert.match(html,/<th>Generated<\/th>/);assert.match(html,/<th>Awaiting publication<\/th>/);
 assert.match(html,/<td>6<\/td><td>1<\/td><td>3<\/td><td>3<\/td><td>0<\/td>/);
});
test('active run counts expose progress before a crawl finishes',()=>{
 const f=fixture();const text=f.run("sourceCounts({pages_captured:12,pages_published:9,assets_discovered:20,assets_downloaded:8,captions_generated:4,captions_published:0})");
 assert.match(text,/12 captured/);assert.match(text,/4 generated/);assert.match(text,/0 published/);
});


test('manual active crawls refresh on the visible collection without a schedule gate',()=>{
 const f=fixture('collection');f.context.calls=[];f.context.document.hidden=false;
 f.run("refreshCollections=()=>calls.push('collections');refreshSources=id=>calls.push(id);status=()=>calls.push('status');");
 f.intervals.find(t=>t.ms===5000).fn();assert.ok(f.context.calls.includes('c1'));
 f.context.calls.length=0;f.documentEvents.get('visibilitychange')();assert.ok(f.context.calls.includes('c1'));
});
test('pending publication is labeled as updates without denying published content',()=>{
 const f=fixture();const html=f.run("processingView({processing:{publication_pending:true,publication_pending_operations:3}})");
 assert.match(html,/Updates awaiting publication/);assert.match(html,/3 operation\(s\)/);assert.match(html,/Published content remains searchable/);assert.doesNotMatch(html,/<dt>Publication pending<\/dt>/);
});


test('caption generation and downstream publication have distinct counters',()=>{
 const f=fixture('collection');
 const html=f.run("processingView({processing:{captions_generated:25,captions_pending:0,captions_awaiting_publication:25}})");
 assert.match(html,/Captions awaiting publication/);
 assert.doesNotMatch(html,/Caption generation pending|Caption tasks pending/);
 const pending=f.run("processingView({processing:{captions_pending:3,captions_awaiting_publication:25}})");
 assert.match(pending,/Caption generation pending/);
 assert.match(pending,/Captions awaiting publication/);
});
