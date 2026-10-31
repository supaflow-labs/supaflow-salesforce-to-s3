const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const workflow = fs.readFileSync(path.join(__dirname, '../workflows/codex-review-gate.yml'), 'utf8');
const script = workflow.split('          script: |\n')[1].split('\n').map(l => l.slice(12)).join('\n');
const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
const summary = (state = {}, overrides = {}) => ({id:1,
  user:{login:'chatgpt-codex-connector[bot]',type:'Bot'},
  performed_via_github_app:{id:1144995},
  body:'<!-- codex-pull-request-review-summary -->\n<!-- codex-review-state:v1 '+JSON.stringify({code_review:{commit_sha:'head',status:'completed',has_unresolved_findings:false,...state}})+' -->',...overrides});
async function run({comments=[summary()],timeline=[],peerTimeline=[],peer=false,fail=false,event='issue_comment'}={}) {
  const statuses=[];
  const github={rest:{pulls:{get:async()=>({data:{head:{sha:'head'},base:{ref:'main'}}}),list:'pulls'},
    issues:{listComments:'comments',listEventsForTimeline:'timeline'},
    repos:{createCommitStatus:async s=>statuses.push(s)}},
    paginate:async(method,args)=>{
      if(fail)throw Error('API unavailable');
      if(method==='pulls')return [{number:1,head:{sha:'head'}},...(peer?[{number:2,head:{sha:'head'}}]:[])];
      if(method==='timeline')return args.issue_number===2?peerTimeline:timeline;
      return comments;
    }};
  const context={repo:{owner:'test',repo:'test'},eventName:event,payload:{issue:{number:1}},serverUrl:'https://github.com',runId:1};
  let error;try{await new AsyncFunction('github','context',script)(github,context);}catch(e){error=e.message;}
  return {statuses,error};
}
test('exact-head completed review succeeds',async()=>assert.deepEqual((await run()).statuses.map(s=>s.state),['pending','success']));
test('retargeted PR stays blocked on comments and manual reevaluation',async()=>{
  for(const event of ['issue_comment','workflow_dispatch','pull_request_target']){
    const r=await run({event,timeline:[{event:'base_ref_changed'}]});
    assert.equal(r.statuses.at(-1).state,'failure');assert.match(r.statuses.at(-1).description,/new PR/);
  }
});
test('retargeting away and back still requires a new PR',async()=>assert.equal((await run({timeline:[{event:'base_ref_changed'},{event:'base_ref_changed'}]})).statuses.at(-1).state,'failure'));
test('retargeted peer with same head cannot inherit success',async()=>assert.equal((await run({peer:true,peerTimeline:[{event:'base_ref_changed'}]})).statuses.at(-1).state,'failure'));
test('unrelated timeline events do not block',async()=>assert.equal((await run({timeline:[{event:'commented'},{event:'committed'}]})).statuses.at(-1).state,'success'));
test('missing stale running or unresolved reviews do not pass',async()=>{
  for(const comments of [[],[summary({commit_sha:'old'})],[summary({status:'running'})],[summary({has_unresolved_findings:true})]])assert.notEqual((await run({comments})).statuses.at(-1).state,'success');
});
test('forged bot evidence cannot pass',async()=>assert.equal((await run({comments:[summary({}, {performed_via_github_app:{id:1}})]})).statuses.at(-1).state,'pending'));
test('API failure leaves pending',async()=>{const r=await run({fail:true});assert.equal(r.statuses.at(-1).state,'pending');assert.equal(r.error,'API unavailable');});
test('privileged gate never checks out PR code',()=>{assert.ok(!workflow.includes('actions/checkout'));assert.ok(!script.includes('${{'));});
