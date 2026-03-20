import express from "express";
import cors from "cors";
import multer from "multer";
import FormData from "form-data";
import fetch from "node-fetch";
import { randomUUID } from "crypto";
import fs from "fs";
import path from "path";

const app = express();
app.use(cors());
app.use(express.json({ limit: "10mb" }));

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize:20*1024*1024, fieldSize:10*1024*1024, fields:30, files:25 },
});

const DATA_DIR="./data", IMGS_DIR="./data/images", REFS_DIR="./data/refs", PROJECTS_DIR="./data/projects";
for (const d of [DATA_DIR,IMGS_DIR,REFS_DIR,PROJECTS_DIR]) if(!fs.existsSync(d))fs.mkdirSync(d,{recursive:true});

const apiKeyStore=new Map(), stopSignals=new Set();

function readJSON(p,fb){try{if(fs.existsSync(p))return JSON.parse(fs.readFileSync(p,"utf8"));}catch(_){}return fb;}
function writeJSON(p,d){try{fs.writeFileSync(p,JSON.stringify(d,null,2));}catch(_){}}
function projectPath(id){return path.join(PROJECTS_DIR,`${id}.json`);}
function loadProject(id){return readJSON(projectPath(id),null);}
function saveProject(proj){const{openaiKey,...safe}=proj;writeJSON(projectPath(proj.id),safe);}
function loadBuffer(p){try{if(p&&fs.existsSync(p))return fs.readFileSync(p);}catch(_){}return null;}
function saveImage(pid,pn,b64){const p=path.join(IMGS_DIR,`${pid}_p${String(pn).padStart(3,"0")}.png`);fs.writeFileSync(p,Buffer.from(b64,"base64"));return p;}
function saveRef(pid,type,id,buf){const p=path.join(REFS_DIR,`${pid}_${type}${id}.png`);fs.writeFileSync(p,buf);return p;}
function saveRefSheet(pid,cid,b64){const p=path.join(REFS_DIR,`${pid}_sheet_c${cid}.png`);fs.writeFileSync(p,Buffer.from(b64,"base64"));return p;}

function deleteProjectFiles(proj){
  for(const r of Object.values(proj.results||{})){if(r.imgPath)try{fs.unlinkSync(r.imgPath);}catch(_){}}
  for(const c of(proj.characters||[])){if(c.refPath)try{fs.unlinkSync(c.refPath);}catch(_){};if(c.refSheetPath)try{fs.unlinkSync(c.refSheetPath);}catch(_){}}
  for(const o of(proj.objects||[])){if(o.refPath)try{fs.unlinkSync(o.refPath);}catch(_){}}
  try{fs.unlinkSync(path.join(REFS_DIR,`${proj.id}_anchor.png`));}catch(_){}
  try{fs.unlinkSync(projectPath(proj.id));}catch(_){}
}

function listProjects(){
  try{return fs.readdirSync(PROJECTS_DIR).filter(f=>f.endsWith(".json")).map(f=>readJSON(path.join(PROJECTS_DIR,f),null)).filter(Boolean).sort((a,b)=>(b.updatedAt||b.createdAt)-(a.updatedAt||a.createdAt));}catch(_){return[];}
}

// Character DNA
function getActiveTraits(char,pn){
  const fixed=char.fixedTraits||[];
  const conditional=(char.conditionalTraits||[]).filter(ct=>ct.pages?ct.pages.includes(pn):ct.pageRange?pn>=ct.pageRange[0]&&pn<=ct.pageRange[1]:false).map(ct=>ct.trait);
  const forbidden=(char.forbiddenTraits||[]).filter(f=>{const ic=(char.conditionalTraits||[]).find(ct=>ct.trait.toLowerCase().includes(f.toLowerCase()));return ic?!conditional.includes(ic.trait):true;});
  return{fixed,conditional,forbidden};
}

// Emotion map
const EM={glad:"eyes bright, wide smile, body leaning forward",lykkelig:"eyes bright, wide smile",ler:"mouth open laughing, head tilted back",jubel:"arms raised, mouth wide open",trist:"eyes downcast, shoulders slumped",gråter:"eyes shut with tears, mouth open",redd:"eyes wide showing whites, body leaning back",skrekk:"eyes wide, hands raised, body recoiling",sint:"eyes narrowed, brows furrowed, hands clenched",overrasket:"eyes wide, eyebrows raised, mouth open",forvirret:"head tilted, one brow raised",bestemt:"eyes forward, jaw set, shoulders squared",flau:"eyes averted, sheepish smile",nysgjerrig:"eyes wide, head tilted, hand pointing",magisk:"eyes wide with wonder, hands extended"};
function emotionToVisual(text){const t=text.toLowerCase();for(const[w,s]of Object.entries(EM))if(t.includes(w))return s;return null;}

// Relations
function parseRelations(line){
  const rels=[];
  for(const m of line.matchAll(/#o(\d+)@#c(\d+)/gi))rels.push({type:"object_on_character",objId:m[1].padStart(2,"0"),charId:m[2].padStart(2,"0")});
  for(const m of line.matchAll(/#o(\d+)@scene/gi))rels.push({type:"object_in_scene",objId:m[1].padStart(2,"0")});
  for(const m of line.matchAll(/#c(\d+)>#o(\d+)/gi))rels.push({type:"character_reaches_object",charId:m[1].padStart(2,"0"),objId:m[2].padStart(2,"0")});
  for(const m of line.matchAll(/#c(\d+)>#c(\d+)/gi))rels.push({type:"character_reaches_character",fromCharId:m[1].padStart(2,"0"),toCharId:m[2].padStart(2,"0")});
  for(const m of line.matchAll(/#c(\d+)\+#c(\d+)/gi))rels.push({type:"characters_together",charId1:m[1].padStart(2,"0"),charId2:m[2].padStart(2,"0")});
  return rels;
}
function relToText(rels,chars,objs){
  const c=id=>chars.find(x=>x.id===id)?.name||`char${id}`;
  const o=id=>objs.find(x=>x.id===id)?.name||`obj${id}`;
  return rels.map(r=>{switch(r.type){case"object_on_character":return`${o(r.objId)} held by ${c(r.charId)}`;case"object_in_scene":return`${o(r.objId)} in background`;case"character_reaches_object":return`${c(r.charId)} reaching for ${o(r.objId)}`;case"character_reaches_character":return`${c(r.fromCharId)} helping ${c(r.toCharId)}`;case"characters_together":return`${c(r.charId1)} and ${c(r.charId2)} together`;default:return"";}}).filter(Boolean);
}

// Parsers
function parseManuscript(manus){const pages=[];for(const sec of manus.split(/(?=#p\d+)/i).filter(s=>s.trim())){const lines=sec.trim().split("\n").map(l=>l.trim()).filter(Boolean);const pm=lines[0]?.match(/#p(\d+)/i);if(!pm)continue;pages.push({pageNum:parseInt(pm[1]),storyText:lines.slice(1).join(" ").trim()});}return pages.sort((a,b)=>a.pageNum-b.pageNum);}

function parseDescriptions(desc,chars,objs){
  const result=new Map();let ls="",lc="medium",la="eye",lsc="ground";
  for(const raw of(desc||"").split("\n")){const line=raw.trim();if(!line)continue;const pm=line.match(/#p(\d+)/i);if(!pm)continue;const pn=parseInt(pm[1]);
  const ft=line.replace(/#[oc]\d+@#[oc]\d+/gi,"").replace(/#[oc]\d+@scene/gi,"").replace(/#[co]\d+>#[co]\d+/gi,"").replace(/#c\d+\+#c\d+/gi,"");
  const cr=[...ft.matchAll(/#c(\d+)/gi)].map(m=>m[1].padStart(2,"0")),or=[...ft.matchAll(/#o(\d+)/gi)].map(m=>m[1].padStart(2,"0"));
  const sc=line.match(/#sc\s+([^#]+?)(?=#\/|#[a-z]|$)/i);if(sc)ls=sc[1].trim();
  const cm=line.match(/#cam\s+(close|medium|wide)/i);if(cm)lc=cm[1].toLowerCase();
  const am=line.match(/#angle\s+(eye|low|high|above)/i);if(am)la=am[1].toLowerCase();
  const sm=line.match(/#scale\s+(ground|above-head|rooftop|over-neighborhood)/i);if(sm)lsc=sm[1].toLowerCase();
  const fm=line.match(/#focus\s+([^#]+?)(?=#\/|#[a-z]|$)/i);const si=line.indexOf("#/");
  result.set(pn,{charRefs:cr,objRefs:or,scene:ls,cam:lc,angle:la,scale:lsc,focus:fm?fm[1].trim():"",visualDesc:si!==-1?line.slice(si+2).trim():"",relations:parseRelations(line),usedChars:cr.map(r=>chars.find(c=>c.id===r)).filter(Boolean),usedObjs:or.map(r=>objs.find(o=>o.id===r)).filter(Boolean)});}
  return result;
}

function mergePages(msPages,descMap,world){
  let ls=world?.core_location||"",lc="medium",la="eye",lsc="ground";
  return msPages.map(mp=>{const d=descMap.get(mp.pageNum);if(d?.scene)ls=d.scene;if(d?.cam)lc=d.cam;if(d?.angle)la=d.angle;if(d?.scale)lsc=d.scale;
  return{pageNum:mp.pageNum,storyText:mp.storyText,charRefs:d?.charRefs||[],objRefs:d?.objRefs||[],scene:ls,cam:lc,angle:la,scale:lsc,focus:d?.focus||"",visualDesc:d?.visualDesc||"",relations:d?.relations||[],usedChars:d?.usedChars||[],usedObjs:d?.usedObjs||[]};});
}

function suggestVisual(page){const t=page.storyText.toLowerCase(),w=page.usedChars.map(c=>c.name).join(" and ")||"the character";
  if(/bomp|krasj|dundret|snublet|falt/.test(t))return`${w} crashes clumsily, dust cloud`;
  if(/fløy|flyr|svevde|løftet seg/.test(t))return`${w} in flight, ground far below`;
  if(/ler|jubel|klappet/.test(t))return`${w} laughing, arms raised`;
  if(/gråt|gråter/.test(t))return`${w} crying, hunched`;
  if(/redd|skrekk/.test(t))return`${w} frightened, recoiling`;
  if(/oppdaget|fant|plutselig/.test(t))return`${w} discovering, eyes wide`;
  if(/glitre|glitter/.test(t))return`${w} surrounded by sparkles`;
  return`${w} — ${page.storyText.slice(0,55)}`;}

const CAM={close:"extreme close-up",medium:"medium shot, upper body",wide:"wide shot, full bodies and environment"};
const ANG={eye:"eye-level",low:"low angle, heroic",high:"high angle",above:"bird's-eye"};
const SCL={"ground":"on solid ground","above-head":"elevated, sky below","rooftop":"rooftop level","over-neighborhood":"high above, streets far below"};

function buildPrompt(masterPrompt,page,idx,total,chars,objs,world){
  const style=masterPrompt?.trim()||"Detailed children's book illustration, warm colors";
  const pn=page.pageNum;
  const charBlocks=page.usedChars.map(c=>{const char=chars.find(x=>x.id===c.id)||c;const t=getActiveTraits(char,pn);const lines=[`CHARACTER ${char.name.toUpperCase()}: ${char.rigidDesc||""}`];if(t.fixed.length)lines.push(`  ALWAYS: ${t.fixed.join(", ")}`);if(t.conditional.length)lines.push(`  THIS PAGE: ${t.conditional.join(", ")}`);if(t.forbidden.length)lines.push(`  FORBIDDEN: ${t.forbidden.join(", ")}`);if(char.refSheetPath)lines.push(`  (master reference sheet provided)`);return lines.join("\n");});
  const objBlocks=page.usedObjs.map(o=>{const obj=objs.find(x=>x.id===o.id)||o;const rel=page.relations.find(r=>r.objId===obj.id);let b="";if(rel?.type==="object_on_character")b=` — held by ${chars.find(c=>c.id===rel.charId)?.name||rel.charId}`;else if(rel?.type==="object_in_scene")b=" — background";else if(rel?.type==="character_reaches_object")b=` — ${chars.find(c=>c.id===rel.charId)?.name||rel.charId} reaching for it`;return`OBJECT ${obj.name.toUpperCase()}: ${obj.rigidDesc||""}${b}`;});
  const wl=[];if(world){if(world.core_location)wl.push(`WORLD: ${world.world_name||""} — ${world.core_location}`);if(world.recurring_landmarks?.length)wl.push(`LANDMARKS: ${world.recurring_landmarks.join(", ")}`);if(world.season)wl.push(`SEASON: ${world.season}`);if(world.weather_baseline)wl.push(`WEATHER: ${world.weather_baseline}`);if(world.forbidden_environment_drift?.length)wl.push(`FORBIDDEN ENVIRONMENT: ${world.forbidden_environment_drift.join(", ")}`);}
  const em=emotionToVisual(page.storyText),vis=page.visualDesc||suggestVisual(page),rl=relToText(page.relations,chars,objs);
  const neg=["Do not add unlisted characters. Do not change fixed traits."];if(world?.forbidden_environment_drift?.length)neg.push(`Do not show: ${world.forbidden_environment_drift.join(", ")}.`);
  page.usedChars.forEach(c=>{const t=getActiveTraits(chars.find(x=>x.id===c.id)||c,pn);if(t.forbidden.length)neg.push(`${c.name} must NOT have: ${t.forbidden.join(", ")}.`);});
  return[`STYLE: ${style}.`,"--- CONSISTENCY ---","All characters must match master reference sheets. No drift. No missing fixed traits.","--- CHARACTERS ---",charBlocks.length?charBlocks.join("\n"):"No named characters.",objBlocks.length?"--- OBJECTS ---\n"+objBlocks.join("\n"):"",wl.length?"--- WORLD ---\n"+wl.join("\n"):"","--- SCENE ---",page.scene?`SCENE: ${page.scene}.`:"","--- COMPOSITION ---",[CAM[page.cam]||CAM.medium,ANG[page.angle]||ANG.eye,SCL[page.scale]||SCL.ground].join(". "),page.focus?`FOCUS: ${page.focus}.`:"",`IMAGE ${idx+1}/${total}.`,"--- ACTION ---",`ILLUSTRATION: ${vis}`,em?`EXPRESSION: ${em}.`:"",rl.length?`RELATIONSHIPS: ${rl.join(". ")}.`:"","--- AVOID ---",neg.join(" ")].filter(Boolean).join("\n").replace(/\n{3,}/g,"\n\n").trim();
}

async function generateMasterRefSheet(char,masterPrompt,apiKey,existingBuf){
  const style=masterPrompt?.trim()||"Detailed children's book illustration, warm colors";
  const prompt=[`MASTER CHARACTER REFERENCE SHEET for ${char.name}.`,`STYLE: ${style}.`,`CHARACTER: ${char.rigidDesc||char.name}`,char.fixedTraits?.length?`MUST SHOW ALL: ${char.fixedTraits.join(", ")}.`:"",char.forbiddenTraits?.length?`DO NOT INCLUDE: ${char.forbiddenTraits.join(", ")}.`:"","Show character upright, facing viewer, full body, plain white background. Clarity is priority."].filter(Boolean).join(" ");
  try{
    if(existingBuf){const form=new FormData();form.append("model","gpt-image-1");form.append("prompt",prompt);form.append("n","1");form.append("size","1024x1024");form.append("image[]",existingBuf,{filename:`${char.name}_ref.png`,contentType:"image/png",knownLength:existingBuf.length});const r=await fetch("https://api.openai.com/v1/images/edits",{method:"POST",headers:{Authorization:`Bearer ${apiKey}`,...form.getHeaders()},body:form});const d=JSON.parse(await r.text());if(d.error)throw new Error(d.error.message);return d.data[0].b64_json;}
    else{const r=await fetch("https://api.openai.com/v1/images/generations",{method:"POST",headers:{"Content-Type":"application/json",Authorization:`Bearer ${apiKey}`},body:JSON.stringify({model:"gpt-image-1",prompt,n:1,size:"1024x1024",output_format:"b64_json"})});const d=JSON.parse(await r.text());if(d.error)throw new Error(d.error.message);return d.data[0].b64_json;}
  }catch(err){console.error(`[refsheet] ${char.name}: ${err.message}`);return null;}
}

async function generateImage(prompt,refs,apiKey,attempt=0){
  const MAX=2;
  try{
    let rt;
    if(refs.length>0){const form=new FormData();form.append("model","gpt-image-1");form.append("prompt",prompt);form.append("n","1");form.append("size","1024x1024");let a=0;for(const ref of refs){if(!ref.buffer?.length)continue;form.append("image[]",ref.buffer,{filename:ref.name.replace(/\.[^.]+$/,".png"),contentType:"image/png",knownLength:ref.buffer.length});a++;}console.log(`[openai] /edits ${a} refs, ${prompt.length} chars`);const r=await fetch("https://api.openai.com/v1/images/edits",{method:"POST",headers:{Authorization:`Bearer ${apiKey}`,...form.getHeaders()},body:form});rt=await r.text();}
    else{console.log(`[openai] /generations ${prompt.length} chars`);const r=await fetch("https://api.openai.com/v1/images/generations",{method:"POST",headers:{"Content-Type":"application/json",Authorization:`Bearer ${apiKey}`},body:JSON.stringify({model:"gpt-image-1",prompt,n:1,size:"1024x1024",output_format:"b64_json"})});rt=await r.text();}
    let d;try{d=JSON.parse(rt);}catch(_){throw new Error(`Non-JSON: ${rt.slice(0,200)}`);}
    if(d.error)throw new Error(d.error.message||JSON.stringify(d.error));
    const b64=d.data?.[0]?.b64_json;if(!b64)throw new Error(`No image: ${rt.slice(0,200)}`);return b64;
  }catch(err){console.error(`[openai] attempt ${attempt+1}: ${err.message}`);if(attempt<MAX){await new Promise(res=>setTimeout(res,2000*(attempt+1)));return generateImage(prompt,refs,apiKey,attempt+1);}throw err;}
}

function buildRefs(page,crm,orm,anchorPath){
  const refs=[];
  for(const id of page.charRefs){if(crm[`sheet_${id}`])refs.push(crm[`sheet_${id}`]);else if(crm[id])refs.push(crm[id]);}
  for(const id of page.objRefs)if(orm[id])refs.push(orm[id]);
  for(const r of page.relations)if(r.objId&&orm[r.objId]&&!refs.find(x=>x.name===orm[r.objId].name))refs.push(orm[r.objId]);
  if(anchorPath&&refs.length<3){const b=loadBuffer(anchorPath);if(b)refs.push({buffer:b,name:"style_anchor.png"});}
  return refs;
}

async function runJob(projectId){
  const proj=loadProject(projectId);if(!proj)return;
  const apiKey=apiKeyStore.get(projectId);
  if(!apiKey){const p=loadProject(projectId);if(p){p.status="stopped";p.error="Ingen API-nøkkel — klikk Gjenstart.";p.updatedAt=Date.now();saveProject(p);}return;}
  proj.status="running";proj.startedAt=Date.now();proj.updatedAt=Date.now();saveProject(proj);
  const{masterPrompt,pages,characters,objects,world}=proj;

  // Phase 1: ref sheets
  const uc=[...characters];
  for(let i=0;i<uc.length;i++){
    if(stopSignals.has(projectId))break;
    const c=uc[i];if(c.refSheetPath&&fs.existsSync(c.refSheetPath))continue;
    if(!(c.fixedTraits?.length>0)&&!c.refPath)continue;
    const b64=await generateMasterRefSheet(c,masterPrompt,apiKey,loadBuffer(c.refPath));
    if(b64){uc[i]={...c,refSheetPath:saveRefSheet(projectId,c.id,b64)};console.log(`[refsheet] ${c.name} done`);}
  }
  const fp=loadProject(projectId);if(fp){fp.characters=uc;fp.updatedAt=Date.now();saveProject(fp);}

  // Phase 2: ref maps
  const crm={},orm={};
  for(const c of uc){const sb=loadBuffer(c.refSheetPath);if(sb)crm[`sheet_${c.id}`]={buffer:sb,name:`${c.name}_sheet.png`};const ob=loadBuffer(c.refPath);if(ob)crm[c.id]={buffer:ob,name:`${c.name}_orig.png`};}
  for(const o of(objects||[])){const b=loadBuffer(o.refPath);if(b)orm[o.id]={buffer:b,name:`${o.name}_ref.png`};}
  const anchorPath=path.join(REFS_DIR,`${projectId}_anchor.png`);let hasAnchor=fs.existsSync(anchorPath);

  // Phase 3: pages
  for(let i=0;i<pages.length;i++){
    if(stopSignals.has(projectId)){stopSignals.delete(projectId);const p=loadProject(projectId);if(p){p.status="stopped";p.updatedAt=Date.now();saveProject(p);}apiKeyStore.delete(projectId);return;}
    const page=pages[i];const cur=loadProject(projectId);
    if(cur?.results?.[page.pageNum]?.status==="done")continue;
    if(cur){cur.results=cur.results||{};cur.results[page.pageNum]={status:"generating",pageNum:page.pageNum};cur.updatedAt=Date.now();saveProject(cur);}
    try{
      const refs=buildRefs(page,crm,orm,hasAnchor?anchorPath:null);
      const prompt=buildPrompt(masterPrompt,page,i,pages.length,uc,objects||[],world);
      console.log(`[job ${projectId.slice(0,8)}] p${page.pageNum}/${pages.length} refs:${refs.length}`);
      const b64=await generateImage(prompt,refs,apiKey);
      const imgPath=saveImage(projectId,page.pageNum,b64);
      if(!hasAnchor){fs.writeFileSync(anchorPath,Buffer.from(b64,"base64"));hasAnchor=true;}
      const fresh=loadProject(projectId);
      if(fresh){fresh.results=fresh.results||{};fresh.results[page.pageNum]={status:"done",pageNum:page.pageNum,imgPath,storyText:page.storyText,visualDesc:page.visualDesc,scene:page.scene,charRefs:page.charRefs,cam:page.cam,angle:page.angle,scale:page.scale,focus:page.focus};fresh.progress=i+1;fresh.updatedAt=Date.now();saveProject(fresh);}
      console.log(`[job ${projectId.slice(0,8)}] p${page.pageNum} done ✓`);
    }catch(err){
      console.error(`[job ${projectId.slice(0,8)}] p${page.pageNum} FAILED: ${err.message}`);
      const fresh=loadProject(projectId);if(fresh){fresh.results=fresh.results||{};fresh.results[page.pageNum]={status:"error",pageNum:page.pageNum,error:err.message,storyText:page.storyText};fresh.progress=i+1;fresh.updatedAt=Date.now();saveProject(fresh);}
    }
  }
  const final=loadProject(projectId);
  if(final){final.status="done";final.completedAt=Date.now();final.successCount=Object.values(final.results||{}).filter(r=>r.status==="done").length;final.updatedAt=Date.now();saveProject(final);}
  apiKeyStore.delete(projectId);
  console.log(`[job ${projectId.slice(0,8)}] COMPLETE`);
}

// Routes
app.get("/health",(req,res)=>res.json({ok:true,version:"10.0"}));
app.get("/",(req,res)=>{const p="./frontend.html";if(fs.existsSync(p)){res.setHeader("Content-Type","text/html; charset=utf-8");res.send(fs.readFileSync(p,"utf8"));}else res.json({ok:true,msg:"StoryBook AI v10.0"});});
app.get("/image/:pid/:pn",(req,res)=>{const proj=loadProject(req.params.pid);if(!proj)return res.status(404).send("Not found");const result=proj.results?.[req.params.pn];if(!result?.imgPath)return res.status(404).send("Not found");const buf=loadBuffer(result.imgPath);if(!buf)return res.status(404).send("Missing");res.setHeader("Content-Type","image/png");res.setHeader("Cache-Control","private, max-age=3600");res.send(buf);});
app.post("/parse",(req,res)=>{const{manus,beskrivelse,characters=[],objects=[],world}=req.body;if(!manus)return res.status(400).json({error:"manus required"});const pages=mergePages(parseManuscript(manus),parseDescriptions(beskrivelse||"",characters,objects),world);res.json({pages:pages.map(p=>({pageNum:p.pageNum,storyText:p.storyText.slice(0,120),charRefs:p.charRefs,objRefs:p.objRefs,scene:p.scene,cam:p.cam,angle:p.angle,scale:p.scale,focus:p.focus,visualDesc:p.visualDesc,suggestedVisual:p.visualDesc||suggestVisual(p),relations:p.relations})),total:pages.length});});
app.get("/projects",(req,res)=>res.json({projects:listProjects().map(p=>({id:p.id,title:p.title,status:p.status,createdAt:p.createdAt,updatedAt:p.updatedAt,total:p.total,progress:p.progress||0,successCount:p.successCount||0,error:p.error}))}));
app.get("/projects/:id",(req,res)=>{const proj=loadProject(req.params.id);if(!proj)return res.status(404).json({error:"Not found"});const{openaiKey,...safe}=proj;res.json(safe);});

app.post("/projects",upload.fields([{name:"charImages",maxCount:20},{name:"objImages",maxCount:20}]),async(req,res)=>{
  try{
    const{openaiKey,masterPrompt,manus,beskrivelse,title}=req.body;
    if(!openaiKey)return res.status(400).json({error:"OpenAI API-nøkkel mangler"});
    if(!manus)return res.status(400).json({error:"Manus mangler"});
    let characters=[],objects=[],world=null;
    try{characters=JSON.parse(req.body.characters||"[]");}catch(_){}
    try{objects=JSON.parse(req.body.objects||"[]");}catch(_){}
    try{world=JSON.parse(req.body.world||"null");}catch(_){}
    const pid=randomUUID(),cf=req.files?.charImages||[],of=req.files?.objImages||[];
    const sc=characters.map(c=>{const f=cf.find(x=>x.originalname.startsWith(`c${c.id}_`));return{id:c.id,name:c.name,rigidDesc:c.rigidDesc||"",fixedTraits:c.fixedTraits||[],forbiddenTraits:c.forbiddenTraits||[],conditionalTraits:c.conditionalTraits||[],refPath:f?saveRef(pid,"c",c.id,f.buffer):null,refSheetPath:null};});
    const so=objects.map(o=>{const f=of.find(x=>x.originalname.startsWith(`o${o.id}_`));return{id:o.id,name:o.name,rigidDesc:o.rigidDesc||"",defaultBinding:o.defaultBinding||"none",refPath:f?saveRef(pid,"o",o.id,f.buffer):null};});
    const pages=mergePages(parseManuscript(manus),parseDescriptions(beskrivelse||"",sc,so),world);
    if(!pages.length)return res.status(400).json({error:"Ingen sider funnet"});
    const proj={id:pid,status:"queued",title:title||`Bok ${new Date().toLocaleDateString("nb-NO")}`,createdAt:Date.now(),updatedAt:Date.now(),masterPrompt,manus,beskrivelse,characters:sc,objects:so,world,pages,results:{},progress:0,total:pages.length};
    saveProject(proj);apiKeyStore.set(pid,openaiKey);
    runJob(pid).catch(err=>{const p=loadProject(pid);if(p){p.status="error";p.error=err.message;p.updatedAt=Date.now();saveProject(p);}apiKeyStore.delete(pid);});
    res.json({projectId:pid,total:pages.length});
  }catch(err){console.error("[POST /projects]",err.message);res.status(500).json({error:err.message});}
});

app.post("/projects/:id/stop",(req,res)=>{const proj=loadProject(req.params.id);if(!proj)return res.status(404).json({error:"Not found"});stopSignals.add(proj.id);res.json({ok:true});});

app.post("/projects/:id/rerun",async(req,res)=>{
  const proj=loadProject(req.params.id);if(!proj)return res.status(404).json({error:"Not found"});
  const{pageNums,openaiKey}=req.body;if(!openaiKey)return res.status(400).json({error:"API-nøkkel kreves"});
  if(pageNums?.length){proj.results=proj.results||{};for(const n of pageNums)proj.results[n]={status:"pending",pageNum:n};}
  proj.status="queued";proj.updatedAt=Date.now();saveProject(proj);apiKeyStore.set(proj.id,openaiKey);
  runJob(proj.id).catch(err=>{const p=loadProject(proj.id);if(p){p.status="error";p.error=err.message;p.updatedAt=Date.now();saveProject(p);}apiKeyStore.delete(proj.id);});
  res.json({ok:true});
});

app.post("/projects/:id/duplicate",(req,res)=>{
  const proj=loadProject(req.params.id);if(!proj)return res.status(404).json({error:"Not found"});
  const newId=randomUUID();
  saveProject({...proj,id:newId,status:"draft",title:(proj.title||"Bok")+" (kopi)",createdAt:Date.now(),updatedAt:Date.now(),characters:(proj.characters||[]).map(c=>({...c,refSheetPath:null})),results:{},progress:0});
  res.json({projectId:newId});
});

app.delete("/projects/:id",(req,res)=>{
  const proj=loadProject(req.params.id);if(!proj)return res.status(404).json({error:"Not found"});
  if(proj.status==="running"||proj.status==="queued")stopSignals.add(proj.id);
  deleteProjectFiles(proj);apiKeyStore.delete(proj.id);res.json({ok:true});
});

app.get("/projects/:id/export",(req,res)=>{
  const proj=loadProject(req.params.id);if(!proj)return res.status(404).json({error:"Not found"});
  if(req.query.type==="text"){const c=Object.values(proj.results||{}).filter(r=>r.status==="done").sort((a,b)=>a.pageNum-b.pageNum).map(r=>`[IMAGE: side_${String(r.pageNum).padStart(3,"0")}]\n${r.storyText}`).join("\n\n---\n\n");res.setHeader("Content-Type","text/plain; charset=utf-8");res.setHeader("Content-Disposition",`attachment; filename="${proj.title||"bok"}.txt"`);res.send(c);}
  else res.json({title:proj.title,results:Object.values(proj.results||{}).filter(r=>r.status==="done").sort((a,b)=>a.pageNum-b.pageNum).map(r=>({pageNum:r.pageNum,storyText:r.storyText,imageUrl:`/image/${proj.id}/${r.pageNum}`}))});
});

function recoverJobs(){try{let n=0;for(const f of fs.readdirSync(PROJECTS_DIR).filter(f=>f.endsWith(".json"))){const p=readJSON(path.join(PROJECTS_DIR,f),null);if(!p)continue;if(p.status==="running"||p.status==="queued"){p.status="stopped";p.error="Server restartet — klikk Gjenstart.";p.updatedAt=Date.now();writeJSON(path.join(PROJECTS_DIR,f),p);n++;}}if(n)console.log(`  Marked ${n} stopped`);}catch(_){}}

const PORT=process.env.PORT||3000;
app.listen(PORT,"0.0.0.0",()=>{console.log(`\n✅ StoryBook AI v10.0 on port ${PORT}`);recoverJobs();});
