import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const workflow = fs.readFileSync(new URL('../workflows/codex-review-gate.yml', import.meta.url), 'utf8');
const script = workflow.split('          script: |\n')[1].split('\n').map(l => l.slice(12)).join('\n');
const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
const summary = (state = {}, overrides = {}) => ({id:1,
  user:{login:'chatgpt-codex-connector[bot]',type:'Bot'},
  performed_via_github_app:{id:1144995},
  body:'<!-- codex-pull-request-review-summary -->\n<!-- codex-review-state:v1 '+JSON.stringify({code_review:{commit_sha:'head',status:'completed',has_unresolved_findings:false,...state}})+' -->',...overrides});
async function run({comments=[summary()],events=[],peerEvents=[],peer=false,fail=false,event='issue_comment',base='main',reviews=[],current={},approveFail=false}={}) {
  const statuses=[];
  const approvals=[];
  let gets=0;
  const github={rest:{pulls:{get:async()=>({data:{head:{sha:'head'},base:{ref:base},state:'open',draft:false,user:{login:'owner'},...(gets++ ? current : {})}}),list:'pulls',listReviews:'reviews',createReview:async r=>{if(approveFail)throw Error('Approval denied');approvals.push(r);}},
    issues:{listComments:'comments',listEvents:'events'},
    repos:{createCommitStatus:async s=>statuses.push(s)}},
    paginate:async(method,args)=>{
      if(fail)throw Error('API unavailable');
      if(method==='pulls')return [{number:1,head:{sha:'head'}},...(peer?[{number:2,head:{sha:'head'}}]:[])];
      if(method==='reviews')return reviews;
      if(method==='events')return args.issue_number===2?peerEvents:events;
      return comments;
    }};
  const context={repo:{owner:'test',repo:'test'},eventName:event,payload:{issue:{number:1}},serverUrl:'https://github.com',runId:1};
  let error;try{await new AsyncFunction('github','context',script)(github,context);}catch(e){error=e.message;}
  return {statuses,error,approvals};
}
test('exact-head completed review succeeds',async()=>assert.deepEqual((await run()).statuses.map(s=>s.state),['pending','success']));
test('retargeted PR stays blocked on comments and manual reevaluation',async()=>{
  for(const event of ['issue_comment','workflow_dispatch','pull_request_target']){
    const r=await run({event,events:[{event:'base_ref_changed'}]});
    assert.equal(r.statuses.at(-1).state,'failure');assert.match(r.statuses.at(-1).description,/new PR/);
  }
});
test('retargeting away and back still requires a new PR',async()=>assert.equal((await run({events:[{event:'base_ref_changed'},{event:'base_ref_changed'}]})).statuses.at(-1).state,'failure'));
test('retargeted peer with same head cannot inherit success',async()=>assert.equal((await run({peer:true,peerEvents:[{event:'base_ref_changed'}]})).statuses.at(-1).state,'failure'));
test('unrelated events do not block',async()=>assert.equal((await run({events:[{event:'commented'},{event:'committed'}]})).statuses.at(-1).state,'success'));
test('missing stale running or unresolved reviews do not pass',async()=>{
  for(const comments of [[],[summary({commit_sha:'old'})],[summary({status:'running'})],[summary({has_unresolved_findings:true})]])assert.notEqual((await run({comments})).statuses.at(-1).state,'success');
});
test('forged bot evidence cannot pass',async()=>assert.equal((await run({comments:[summary({}, {performed_via_github_app:{id:1}})]})).statuses.at(-1).state,'pending'));
test('API failure leaves pending',async()=>{const r=await run({fail:true});assert.equal(r.statuses.at(-1).state,'pending');assert.equal(r.error,'API unavailable');});
test('privileged gate never checks out PR code',()=>{assert.ok(!workflow.includes('actions/checkout'));assert.ok(!script.includes('${{'));});

test('automatic base changes require a new PR',async()=>assert.equal((await run({events:[{event:'automatic_base_change_succeeded'}]})).statuses.at(-1).state,'failure'));

test('prod remains status-only',async()=>assert.equal((await run({base:'prod'})).approvals.length,0));

for (const approvalBase of ['main']) {
test(approvalBase + ' — submits exact-head approval before successful status',async()=>{
  const r=await run({base:approvalBase});assert.equal(r.error,undefined);
  assert.equal(r.approvals.length,1);assert.equal(r.approvals[0].event,'APPROVE');
  assert.equal(r.approvals[0].commit_id,'head');assert.equal(r.statuses.at(-1).state,'success');
});
test(approvalBase + ' — missing stale running unresolved forged or retargeted evidence never approves',async()=>{
  for(const options of [{comments:[]},{comments:[summary({commit_sha:'old'})]},
    {comments:[summary({status:'running'})]},{comments:[summary({has_unresolved_findings:true})]},
    {comments:[summary({}, {performed_via_github_app:{id:1}})]},{events:[{event:'base_ref_changed'}]}]){
    assert.equal((await run({base:approvalBase,...options})).approvals.length,0);
  }
});
test(approvalBase + ' — changed closed draft or self-authored PR never approves',async()=>{
  for(const current of [{head:{sha:'new'}},{base:{ref:'prod'}},{state:'closed'},{draft:true},{user:{login:'github-actions[bot]'}}]){
    const r=await run({base:approvalBase,current});assert.equal(r.approvals.length,0);
    assert.notEqual(r.statuses.at(-1)?.state,'success');
  }
});
test(approvalBase + ' — approval API failure never publishes success',async()=>{
  const r=await run({base:approvalBase,approveFail:true});assert.equal(r.error,'Approval denied');
  assert.equal(r.statuses.at(-1).state,'pending');
});
test(approvalBase + ' — duplicate evaluation does not duplicate current approval; stale or dismissed approval is replaced',async()=>{
  const review={user:{login:'github-actions[bot]',type:'Bot'},state:'APPROVED',commit_id:'head'};
  assert.equal((await run({base:approvalBase,reviews:[review]})).approvals.length,0);
  for(const changed of [{commit_id:'old'},{state:'DISMISSED'}])
    assert.equal((await run({base:approvalBase,reviews:[{...review,...changed}]})).approvals.length,1);
});

}
