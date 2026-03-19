import express from "express";
import cors from "cors";
import multer from "multer";
import FormData from "form-data";
import fetch from "node-fetch";
import { randomUUID } from "crypto";
import fs from "fs";
import path from "path";
import bcrypt from "bcryptjs";

const app = express();
app.use(cors());
app.use(express.json({ limit: "10mb" }));

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024, fieldSize: 10 * 1024 * 1024, fields: 30, files: 25 },
});

// ─── Directories ──────────────────────────────────────────────
const DATA_DIR    = "./data";
const IMGS_DIR    = "./data/images";
const REFS_DIR    = "./data/refs";
const USERS_DIR   = "./data/users";
const PROJECTS_DIR = "./data/projects";

for (const d of [DATA_DIR, IMGS_DIR, REFS_DIR, USERS_DIR, PROJECTS_DIR]) {
  if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
}

// ─── In-memory API key store ───────────────────────────────────
// API keys are NEVER written to disk. They live only in RAM.
// Key is provided by frontend at job start and at rerun.
// Deleted from memory when job completes or is stopped.
const apiKeyStore = new Map(); // projectId -> openaiKey

// ─── Stop signal set ───────────────────────────────────────────
// runJob() checks this between each image. Clean and simple.
const stopSignals = new Set(); // projectId

// ─── Persistence ──────────────────────────────────────────────
function readJSON(p, fb) {
  try { if (fs.existsSync(p)) return JSON.parse(fs.readFileSync(p, "utf8")); } catch (_) {}
  return fb;
}
function writeJSON(p, d) { try { fs.writeFileSync(p, JSON.stringify(d, null, 2)); } catch (_) {} }

// ─── Project store ─────────────────────────────────────────────
// Project = the authoritative data structure.
// openaiKey is EXCLUDED from disk serialization.

function projectPath(id) { return path.join(PROJECTS_DIR, `${id}.json`); }

function loadProject(id) {
  const p = readJSON(projectPath(id), null);
  return p;
}

function saveProject(proj) {
  // Strip API key before saving
  const { openaiKey, ...safe } = proj;
  writeJSON(projectPath(proj.id), safe);
}

function deleteProjectFiles(proj) {
  // Delete generated images
  for (const result of Object.values(proj.results || {})) {
    if (result.imgPath) { try { fs.unlinkSync(result.imgPath); } catch (_) {} }
  }
  // Delete reference images
  for (const c of (proj.characters || [])) {
    if (c.refPath) { try { fs.unlinkSync(c.refPath); } catch (_) {} }
  }
  for (const o of (proj.objects || [])) {
    if (o.refPath) { try { fs.unlinkSync(o.refPath); } catch (_) {} }
  }
  // Delete style anchor
  const anchorPath = path.join(REFS_DIR, `${proj.id}_anchor.png`);
  try { fs.unlinkSync(anchorPath); } catch (_) {}
  // Delete project file
  try { fs.unlinkSync(projectPath(proj.id)); } catch (_) {}
}

function listUserProjects(userId) {
  try {
    return fs.readdirSync(PROJECTS_DIR)
      .filter(f => f.endsWith(".json"))
      .map(f => readJSON(path.join(PROJECTS_DIR, f), null))
      .filter(p => p && p.userId === userId)
      .sort((a, b) => (b.updatedAt || b.createdAt) - (a.updatedAt || a.createdAt));
  } catch (_) { return []; }
}

function saveImage(projectId, pageNum, b64) {
  const p = path.join(IMGS_DIR, `${projectId}_p${String(pageNum).padStart(3, "0")}.png`);
  fs.writeFileSync(p, Buffer.from(b64, "base64"));
  return p;
}
function saveRef(projectId, type, id, buffer) {
  const p = path.join(REFS_DIR, `${projectId}_${type}${id}.png`);
  fs.writeFileSync(p, buffer);
  return p;
}
function loadBuffer(p) {
  try { if (p && fs.existsSync(p)) return fs.readFileSync(p); } catch (_) {}
  return null;
}

// ─── Users ────────────────────────────────────────────────────
function loadUsers() { return readJSON(path.join(USERS_DIR, "users.json"), {}); }
function saveUsers(u) { writeJSON(path.join(USERS_DIR, "users.json"), u); }

// ─── Character DNA helpers ────────────────────────────────────
function getActiveTraits(char, pageNum) {
  const fixed = char.fixedTraits || [];
  const conditional = (char.conditionalTraits || []).filter(ct => {
    if (ct.pages) return ct.pages.includes(pageNum);
    if (ct.pageRange) return pageNum >= ct.pageRange[0] && pageNum <= ct.pageRange[1];
    return false;
  }).map(ct => ct.trait);
  const forbidden = (char.forbiddenTraits || []).filter(f => {
    const inCond = (char.conditionalTraits || []).find(ct => ct.trait.toLowerCase().includes(f.toLowerCase()));
    if (inCond) return !conditional.includes(inCond.trait);
    return true;
  });
  return { fixed, conditional, forbidden };
}

// ─── Emotion → visual signals ─────────────────────────────────
const EMOTION_MAP = {
  glad: "eyes bright and crinkled, wide smile, shoulders relaxed, body leaning forward",
  lykkelig: "eyes bright and crinkled, wide smile, shoulders relaxed, body leaning forward",
  ler: "mouth open laughing, eyes nearly closed, head tilted back",
  jubel: "arms raised high, mouth wide open, eyes wide with joy",
  trist: "eyes downcast, corners of mouth pulled down, shoulders slumped",
  gråter: "eyes shut with tears on cheeks, mouth open in cry",
  redd: "eyes wide showing whites, mouth slightly open, body leaning back",
  skrekk: "eyes wide, mouth open in gasp, hands raised defensively, body recoiling",
  sint: "eyes narrowed, brows low, mouth pressed tight, hands clenched",
  overrasket: "eyes wide, eyebrows raised high, mouth forming an O, body slightly backward",
  forvirret: "head tilted, one brow raised, mouth slightly open, hand near chin",
  bestemt: "eyes forward and steady, jaw set, chin up, shoulders squared",
  flau: "eyes averted, hand behind head, small sheepish smile, shoulders hunched",
  nysgjerrig: "eyes wide and bright, head tilted forward, hand pointing or reaching",
  magisk: "eyes wide with wonder, mouth slightly open, hands extended with awe",
};
function emotionToVisual(text) {
  const t = text.toLowerCase();
  for (const [word, signal] of Object.entries(EMOTION_MAP)) {
    if (t.includes(word)) return signal;
  }
  return null;
}

// ─── Relation parser ──────────────────────────────────────────
function parseRelations(line) {
  const rels = [];
  for (const m of line.matchAll(/#o(\d+)@#c(\d+)/gi))
    rels.push({ type: "object_on_character", objId: m[1].padStart(2,"0"), charId: m[2].padStart(2,"0") });
  for (const m of line.matchAll(/#o(\d+)@scene/gi))
    rels.push({ type: "object_in_scene", objId: m[1].padStart(2,"0") });
  for (const m of line.matchAll(/#c(\d+)>#o(\d+)/gi))
    rels.push({ type: "character_reaches_object", charId: m[1].padStart(2,"0"), objId: m[2].padStart(2,"0") });
  for (const m of line.matchAll(/#c(\d+)>#c(\d+)/gi))
    rels.push({ type: "character_reaches_character", fromCharId: m[1].padStart(2,"0"), toCharId: m[2].padStart(2,"0") });
  for (const m of line.matchAll(/#c(\d+)\+#c(\d+)/gi))
    rels.push({ type: "characters_together", charId1: m[1].padStart(2,"0"), charId2: m[2].padStart(2,"0") });
  return rels;
}
function relationsToText(rels, characters, objects) {
  const c = id => characters.find(x => x.id === id)?.name || `char ${id}`;
  const o = id => objects.find(x => x.id === id)?.name    || `obj ${id}`;
  return rels.map(r => {
    switch (r.type) {
      case "object_on_character":         return `${o(r.objId)} is held by ${c(r.charId)}`;
      case "object_in_scene":             return `${o(r.objId)} is part of the environment`;
      case "character_reaches_object":    return `${c(r.charId)} is reaching for ${o(r.objId)}`;
      case "character_reaches_character": return `${c(r.fromCharId)} is helping or holding ${c(r.toCharId)}`;
      case "characters_together":         return `${c(r.charId1)} and ${c(r.charId2)} are together in scene`;
      default: return "";
    }
  }).filter(Boolean);
}

// ─── Markup parsers ───────────────────────────────────────────
function parseManuscript(manus) {
  const pages = [];
  for (const sec of manus.split(/(?=#p\d+)/i).filter(s => s.trim())) {
    const lines = sec.trim().split("\n").map(l => l.trim()).filter(Boolean);
    const pm = lines[0]?.match(/#p(\d+)/i);
    if (!pm) continue;
    pages.push({ pageNum: parseInt(pm[1]), storyText: lines.slice(1).join(" ").trim() });
  }
  return pages.sort((a, b) => a.pageNum - b.pageNum);
}

function parseDescriptions(desc, characters, objects) {
  const result = new Map();
  let lastScene = "", lastCam = "medium", lastAngle = "eye", lastScale = "ground";
  for (const rawLine of (desc || "").split("\n")) {
    const line = rawLine.trim();
    if (!line) continue;
    const pm = line.match(/#p(\d+)/i);
    if (!pm) continue;
    const pageNum = parseInt(pm[1]);
    const lineForTags = line.replace(/#[oc]\d+@#[oc]\d+/gi,"").replace(/#[oc]\d+@scene/gi,"").replace(/#[co]\d+>#[co]\d+/gi,"").replace(/#c\d+\+#c\d+/gi,"");
    const charRefs = [...lineForTags.matchAll(/#c(\d+)/gi)].map(m => m[1].padStart(2,"0"));
    const objRefs  = [...lineForTags.matchAll(/#o(\d+)/gi)].map(m => m[1].padStart(2,"0"));
    const scMatch    = line.match(/#sc\s+([^#]+?)(?=#\/|#[a-z]|$)/i);
    if (scMatch) lastScene = scMatch[1].trim();
    const camMatch   = line.match(/#cam\s+(close|medium|wide)/i);
    if (camMatch) lastCam = camMatch[1].toLowerCase();
    const angleMatch = line.match(/#angle\s+(eye|low|high|above)/i);
    if (angleMatch) lastAngle = angleMatch[1].toLowerCase();
    const scaleMatch = line.match(/#scale\s+(ground|above-head|rooftop|over-neighborhood)/i);
    if (scaleMatch) lastScale = scaleMatch[1].toLowerCase();
    const focusMatch = line.match(/#focus\s+([^#]+?)(?=#\/|#[a-z]|$)/i);
    const slashIdx   = line.indexOf("#/");
    result.set(pageNum, {
      charRefs, objRefs,
      scene: lastScene, cam: lastCam, angle: lastAngle, scale: lastScale,
      focus:      focusMatch ? focusMatch[1].trim() : "",
      visualDesc: slashIdx !== -1 ? line.slice(slashIdx + 2).trim() : "",
      relations:  parseRelations(line),
      usedChars:  charRefs.map(r => characters.find(c => c.id === r)).filter(Boolean),
      usedObjs:   objRefs.map(r  => objects.find(o  => o.id === r)).filter(Boolean),
    });
  }
  return result;
}

function mergePages(manuscriptPages, descMap, world) {
  let lastScene = world?.core_location || "", lastCam = "medium", lastAngle = "eye", lastScale = "ground";
  return manuscriptPages.map(mp => {
    const desc = descMap.get(mp.pageNum);
    if (desc?.scene) lastScene = desc.scene;
    if (desc?.cam)   lastCam   = desc.cam;
    if (desc?.angle) lastAngle = desc.angle;
    if (desc?.scale) lastScale = desc.scale;
    return {
      pageNum: mp.pageNum, storyText: mp.storyText,
      charRefs:   desc?.charRefs   || [],
      objRefs:    desc?.objRefs    || [],
      scene: lastScene, cam: lastCam, angle: lastAngle, scale: lastScale,
      focus:      desc?.focus      || "",
      visualDesc: desc?.visualDesc || "",
      relations:  desc?.relations  || [],
      usedChars:  desc?.usedChars  || [],
      usedObjs:   desc?.usedObjs   || [],
    };
  });
}

// ─── Visual suggester ─────────────────────────────────────────
function suggestVisual(page) {
  const t = page.storyText.toLowerCase();
  const who = page.usedChars.map(c => c.name).join(" and ") || "the character";
  if (/bomp|krasj|dundret|snublet|falt/.test(t))  return `${who} crashes clumsily, body hitting ground, dust cloud`;
  if (/fløy|flyr|svevde|løftet seg/.test(t))      return `${who} airborne in flight, wind in hair, ground far below`;
  if (/ler|jubel|klappet/.test(t))                 return `${who} — mouth open laughing, arms raised`;
  if (/gråt|gråter/.test(t))                       return `${who} — eyes shut with tears, hunched posture`;
  if (/redd|skrekk/.test(t))                       return `${who} — eyes wide showing whites, body recoiling`;
  if (/oppdaget|fant|plutselig/.test(t))           return `${who} — eyes wide on discovery, hand pointing`;
  if (/glitre|glitter/.test(t))                    return `${who} surrounded by glittering sparkles`;
  if (/tok imot|fanget|grep/.test(t))              return `${who} — arms outstretched catching, body braced`;
  return `${who} — ${page.storyText.slice(0, 55)}`;
}

// ─── Prompt builder ───────────────────────────────────────────
const CAMERAS = ["wide establishing shot","medium shot at eye level","close-up on faces","over-the-shoulder shot","low-angle hero view","bird's-eye view","intimate medium close-up","dramatic side profile"];
function cameraToText(cam, angle, scale) {
  const cMap = { close:"extreme close-up — faces fill the frame", medium:"medium shot — full upper body visible", wide:"wide shot — full bodies and environment visible" };
  const aMap = { eye:"straight-on eye-level", low:"low angle looking up — character appears large", high:"high angle looking down", above:"bird's-eye directly overhead" };
  const sMap = { "ground":"characters on solid ground", "above-head":"elevated above head height, sky and ground below", "rooftop":"at rooftop level — buildings below, open sky", "over-neighborhood":"high above neighborhood — streets and houses far below, dramatic aerial height" };
  return [cMap[cam]||cMap.medium, aMap[angle]||aMap.eye, sMap[scale]||sMap.ground].join(". ");
}

function buildPrompt(masterPrompt, page, pageIndex, totalPages, characters, objects, world) {
  const style = masterPrompt?.trim() || "Detailed children's book illustration, warm colors";
  const pn    = page.pageNum;

  const hardRules = "ABSOLUTE RULE: All characters must look identical to their reference images. No new traits. No missing fixed traits. No style drift. Draw only explicitly listed characters.";

  const charBlocks = page.usedChars.map(c => {
    const char   = characters.find(x => x.id === c.id) || c;
    const traits = getActiveTraits(char, pn);
    const lines  = [`CHARACTER ${char.name.toUpperCase()} (#c${char.id}): ${char.rigidDesc || char.description || ""}`];
    if (traits.fixed.length)       lines.push(`  ALWAYS PRESENT: ${traits.fixed.join(", ")}`);
    if (traits.conditional.length) lines.push(`  ONLY ON THIS PAGE: ${traits.conditional.join(", ")}`);
    if (traits.forbidden.length)   lines.push(`  FORBIDDEN THIS PAGE: ${traits.forbidden.join(", ")}`);
    return lines.join("\n");
  });

  const objBlocks = page.usedObjs.map(o => {
    const obj = objects.find(x => x.id === o.id) || o;
    const rel = page.relations.find(r => r.objId === obj.id || r.toObjId === obj.id);
    let binding = "";
    if (rel?.type === "object_on_character") binding = ` — held by ${characters.find(c=>c.id===rel.charId)?.name||rel.charId}`;
    else if (rel?.type === "object_in_scene") binding = " — part of background environment";
    else if (rel?.type === "character_reaches_object") binding = ` — ${characters.find(c=>c.id===rel.charId)?.name||rel.charId} reaching for it`;
    return `OBJECT ${obj.name.toUpperCase()} (#o${obj.id}): ${obj.rigidDesc || obj.description || ""}${binding}`;
  });

  const worldLines = [];
  if (world) {
    if (world.core_location) worldLines.push(`WORLD: ${world.world_name||"story world"} — ${world.core_location}`);
    if (world.recurring_landmarks?.length) worldLines.push(`LANDMARKS: ${world.recurring_landmarks.join(", ")}`);
    if (world.season)          worldLines.push(`SEASON: ${world.season}`);
    if (world.weather_baseline) worldLines.push(`WEATHER: ${world.weather_baseline}`);
    if (world.forbidden_environment_drift?.length) worldLines.push(`ENVIRONMENT FORBIDDEN: ${world.forbidden_environment_drift.join(", ")}`);
  }

  const emotional    = emotionToVisual(page.storyText);
  const visual       = page.visualDesc || suggestVisual(page);
  const relLines     = relationsToText(page.relations, characters, objects);
  const negatives    = ["Do not add characters not listed. Do not change any fixed trait."];
  if (world?.forbidden_environment_drift?.length) negatives.push(`Do not show: ${world.forbidden_environment_drift.join(", ")}.`);
  page.usedChars.forEach(c => { const t=getActiveTraits(characters.find(x=>x.id===c.id)||c,pn); if(t.forbidden.length) negatives.push(`${c.name} must NOT have: ${t.forbidden.join(", ")}.`); });

  return [
    `STYLE: ${style}.`,
    "", "--- CONSISTENCY RULES ---", hardRules,
    "", "--- CHARACTER DNA ---", charBlocks.length ? charBlocks.join("\n") : "No named characters — focus on environment.",
    objBlocks.length ? "\n--- OBJECT DNA ---\n" + objBlocks.join("\n") : "",
    worldLines.length ? "\n--- WORLD ANCHORS ---\n" + worldLines.join("\n") : "",
    "", "--- SCENE ---", page.scene ? `SCENE: ${page.scene}.` : "",
    "", "--- COMPOSITION ---", cameraToText(page.cam, page.angle, page.scale),
    page.focus ? `PRIMARY FOCUS: ${page.focus}.` : "",
    `IMAGE ${pageIndex + 1} OF ${totalPages}.`,
    "", "--- ACTION ---",
    `ILLUSTRATION: ${visual}`,
    emotional ? `EXPRESSION AND BODY LANGUAGE: ${emotional}.` : "",
    relLines.length ? `RELATIONSHIPS: ${relLines.join(". ")}.` : "",
    "", "--- NEGATIVE CONSTRAINTS ---", negatives.join(" "),
  ].filter(s => s !== null && s !== undefined).join("\n").replace(/\n{3,}/g, "\n\n").trim();
}

// ─── Reference strategy ───────────────────────────────────────
function buildRefs(page, charRefMap, objRefMap, styleAnchorPath) {
  const refs = [];
  for (const id of page.charRefs)  if (charRefMap[id]) refs.push(charRefMap[id]);
  for (const id of page.objRefs)   if (objRefMap[id])  refs.push(objRefMap[id]);
  for (const r of page.relations) {
    if (r.objId && objRefMap[r.objId] && !refs.find(x => x.name === objRefMap[r.objId].name))
      refs.push(objRefMap[r.objId]);
  }
  if (styleAnchorPath && refs.length < 4) {
    const buf = loadBuffer(styleAnchorPath);
    if (buf) refs.push({ buffer: buf, name: "style_anchor.png" });
  }
  return refs;
}

// ─── Image generation ─────────────────────────────────────────
async function generateImage(prompt, refs, apiKey, attempt = 0) {
  const MAX = 2;
  try {
    let responseText;
    if (refs.length > 0) {
      const form = new FormData();
      form.append("model", "gpt-image-1");
      form.append("prompt", prompt);
      form.append("n", "1");
      form.append("size", "1024x1024");
      let added = 0;
      for (const ref of refs) {
        if (!ref.buffer?.length) continue;
        form.append("image[]", ref.buffer, { filename: ref.name.replace(/\.[^.]+$/, ".png"), contentType: "image/png", knownLength: ref.buffer.length });
        added++;
      }
      console.log(`[openai] /edits — ${added} refs, prompt ${prompt.length} chars`);
      const r = await fetch("https://api.openai.com/v1/images/edits", { method:"POST", headers:{ Authorization:`Bearer ${apiKey}`, ...form.getHeaders() }, body:form });
      responseText = await r.text();
    } else {
      console.log(`[openai] /generations — prompt ${prompt.length} chars`);
      const r = await fetch("https://api.openai.com/v1/images/generations", { method:"POST", headers:{ "Content-Type":"application/json", Authorization:`Bearer ${apiKey}` }, body:JSON.stringify({ model:"gpt-image-1", prompt, n:1, size:"1024x1024", output_format:"b64_json" }) });
      responseText = await r.text();
    }
    let data;
    try { data = JSON.parse(responseText); } catch (_) { throw new Error(`Non-JSON: ${responseText.slice(0,200)}`); }
    if (data.error) throw new Error(data.error.message || JSON.stringify(data.error));
    const b64 = data.data?.[0]?.b64_json;
    if (!b64) throw new Error(`No image data: ${responseText.slice(0,200)}`);
    return b64;
  } catch (err) {
    console.error(`[openai] attempt ${attempt+1}: ${err.message}`);
    if (attempt < MAX) { await new Promise(res => setTimeout(res, 2000*(attempt+1))); return generateImage(prompt, refs, apiKey, attempt+1); }
    throw err;
  }
}

// ─── Job runner ───────────────────────────────────────────────
async function runJob(projectId) {
  const proj = loadProject(projectId);
  if (!proj) return;

  const apiKey = apiKeyStore.get(projectId);
  if (!apiKey) {
    console.error(`[job ${projectId}] No API key in memory — cannot run`);
    proj.status = "stopped"; proj.updatedAt = Date.now(); saveProject(proj);
    return;
  }

  proj.status = "running"; proj.startedAt = Date.now(); proj.updatedAt = Date.now(); saveProject(proj);

  const { masterPrompt, pages, characters, objects, world } = proj;

  const charRefMap = {};
  for (const c of (characters||[])) { const buf = loadBuffer(c.refPath); if (buf) charRefMap[c.id] = { buffer:buf, name:`char_${c.id}_${c.name}.png` }; }
  const objRefMap = {};
  for (const o of (objects||[])) { const buf = loadBuffer(o.refPath); if (buf) objRefMap[o.id] = { buffer:buf, name:`obj_${o.id}_${o.name}.png` }; }

  const anchorPath = path.join(REFS_DIR, `${projectId}_anchor.png`);
  let hasAnchor = fs.existsSync(anchorPath);

  for (let i = 0; i < pages.length; i++) {
    // Check stop signal between each image
    if (stopSignals.has(projectId)) {
      console.log(`[job ${projectId}] stopped at page ${pages[i].pageNum}`);
      stopSignals.delete(projectId);
      const p = loadProject(projectId);
      if (p) { p.status = "stopped"; p.updatedAt = Date.now(); saveProject(p); }
      apiKeyStore.delete(projectId);
      return;
    }

    const page = pages[i];
    const cur  = loadProject(projectId);
    if (cur?.results?.[page.pageNum]?.status === "done") continue;

    // Update status on disk
    if (cur) { cur.results = cur.results || {}; cur.results[page.pageNum] = { status:"generating", pageNum:page.pageNum }; cur.updatedAt = Date.now(); saveProject(cur); }

    try {
      const refs   = buildRefs(page, charRefMap, objRefMap, hasAnchor ? anchorPath : null);
      const prompt = buildPrompt(masterPrompt, page, i, pages.length, characters||[], objects||[], world);
      console.log(`[job ${projectId}] p${page.pageNum}/${pages.length} cam:${page.cam} scale:${page.scale} chars:${page.charRefs.join(",")||"none"}`);

      const b64     = await generateImage(prompt, refs, apiKey);
      const imgPath = saveImage(projectId, page.pageNum, b64);
      if (!hasAnchor) { fs.writeFileSync(anchorPath, Buffer.from(b64,"base64")); hasAnchor = true; }

      const fresh = loadProject(projectId);
      if (fresh) {
        fresh.results = fresh.results || {};
        fresh.results[page.pageNum] = { status:"done", pageNum:page.pageNum, imgPath, storyText:page.storyText, visualDesc:page.visualDesc, scene:page.scene, charRefs:page.charRefs, cam:page.cam, angle:page.angle, scale:page.scale, focus:page.focus };
        fresh.progress  = i + 1;
        fresh.updatedAt = Date.now();
        saveProject(fresh);
      }
      console.log(`[job ${projectId}] p${page.pageNum} done ✓`);
    } catch (err) {
      console.error(`[job ${projectId}] p${page.pageNum} FAILED: ${err.message}`);
      const fresh = loadProject(projectId);
      if (fresh) { fresh.results = fresh.results || {}; fresh.results[page.pageNum] = { status:"error", pageNum:page.pageNum, error:err.message, storyText:page.storyText }; fresh.progress = i+1; fresh.updatedAt = Date.now(); saveProject(fresh); }
    }
  }

  const finalProj = loadProject(projectId);
  if (finalProj) {
    const ok = Object.values(finalProj.results||{}).filter(r=>r.status==="done").length;
    finalProj.status = "done"; finalProj.completedAt = Date.now(); finalProj.successCount = ok; finalProj.updatedAt = Date.now();
    saveProject(finalProj);
  }
  apiKeyStore.delete(projectId);
  console.log(`[job ${projectId}] COMPLETE`);
}

// ─── Auth middleware ───────────────────────────────────────────
function auth(req, res, next) {
  const token = req.headers["x-user-token"];
  if (!token) return res.status(401).json({ error: "Ikke innlogget" });
  const user = Object.values(loadUsers()).find(u => u.token === token);
  if (!user) return res.status(401).json({ error: "Ugyldig token" });
  req.user = user;
  next();
}

function ownsProject(proj, user) {
  return proj.userId === user.id;
}

// ─── Export helpers ───────────────────────────────────────────
function burnTextOnCanvas(b64, storyText, pageNum, total) {
  // Server-side text burn not feasible without canvas lib.
  // Frontend handles burn. Server returns raw image + metadata.
  return b64;
}

function buildTextExport(proj) {
  const results = Object.values(proj.results||{})
    .filter(r => r.status === "done")
    .sort((a,b) => a.pageNum - b.pageNum);
  return results.map(r =>
    `[IMAGE: side_${String(r.pageNum).padStart(3,"0")}]\n${r.storyText}`
  ).join("\n\n---\n\n");
}

// ─── Routes ───────────────────────────────────────────────────

app.get("/health", (req, res) => {
  res.json({ ok:true, version:"8.0" });
});

app.get("/", (req, res) => {
  const p = "./frontend.html";
  if (fs.existsSync(p)) { res.setHeader("Content-Type","text/html; charset=utf-8"); res.send(fs.readFileSync(p,"utf8")); }
  else res.json({ ok:true, msg:"StoryBook AI v8.0" });
});

// ── Auth ──

app.post("/auth/register", async (req, res) => {
  const { username, password } = req.body;
  if (!username||!password) return res.status(400).json({ error:"Brukernavn og passord kreves" });
  if (username.length<3) return res.status(400).json({ error:"Minst 3 tegn" });
  if (password.length<6) return res.status(400).json({ error:"Minst 6 tegn" });
  const users = loadUsers();
  if (Object.values(users).find(u=>u.username.toLowerCase()===username.toLowerCase()))
    return res.status(400).json({ error:"Brukernavnet er tatt" });
  const id=randomUUID(), token=randomUUID();
  users[id]={ id, username, passwordHash:await bcrypt.hash(password,10), token, createdAt:Date.now() };
  saveUsers(users);
  res.json({ ok:true, token, username, userId:id });
});

app.post("/auth/login", async (req, res) => {
  const { username, password } = req.body;
  if (!username||!password) return res.status(400).json({ error:"Fyll inn begge felt" });
  const users = loadUsers();
  const user  = Object.values(users).find(u=>u.username.toLowerCase()===username.toLowerCase());
  if (!user||!await bcrypt.compare(password,user.passwordHash))
    return res.status(401).json({ error:"Feil brukernavn eller passord" });
  user.token = randomUUID(); saveUsers(users);
  res.json({ ok:true, token:user.token, username:user.username, userId:user.id });
});

app.get("/auth/me", auth, (req, res) => res.json({ ok:true, username:req.user.username, userId:req.user.id }));

// ── Parse (no auth required) ──

app.post("/parse", (req, res) => {
  const { manus, beskrivelse, characters=[], objects=[], world } = req.body;
  if (!manus) return res.status(400).json({ error:"manus required" });
  const msPages = parseManuscript(manus);
  const descMap = parseDescriptions(beskrivelse||"", characters, objects);
  const pages   = mergePages(msPages, descMap, world);
  res.json({
    pages: pages.map(p => ({
      pageNum: p.pageNum, storyText: p.storyText.slice(0,120),
      charRefs:p.charRefs, objRefs:p.objRefs, scene:p.scene,
      cam:p.cam, angle:p.angle, scale:p.scale, focus:p.focus,
      visualDesc:p.visualDesc, suggestedVisual:p.visualDesc||suggestVisual(p),
      relations:p.relations,
    })),
    total: pages.length,
  });
});

// ── Image serving — owner only ──

app.get("/image/:projectId/:pageNum", auth, (req, res) => {
  const proj = loadProject(req.params.projectId);
  if (!proj) return res.status(404).send("Not found");
  if (!ownsProject(proj, req.user)) return res.status(403).send("Forbidden");
  const result = proj.results?.[req.params.pageNum];
  if (!result?.imgPath) return res.status(404).send("Image not found");
  const buf = loadBuffer(result.imgPath);
  if (!buf) return res.status(404).send("File missing");
  res.setHeader("Content-Type","image/png");
  res.setHeader("Cache-Control","private, max-age=3600");
  res.send(buf);
});

// ── Projects CRUD ──

// GET /projects — list user's projects
app.get("/projects", auth, (req, res) => {
  const projects = listUserProjects(req.user.id).map(p => ({
    id:           p.id,
    title:        p.title,
    status:       p.status,
    createdAt:    p.createdAt,
    updatedAt:    p.updatedAt,
    total:        p.total,
    progress:     p.progress || 0,
    successCount: p.successCount || 0,
    hasResults:   Object.values(p.results||{}).some(r=>r.status==="done"),
  }));
  res.json({ projects });
});

// GET /projects/:id — full project (owner only)
app.get("/projects/:id", auth, (req, res) => {
  const proj = loadProject(req.params.id);
  if (!proj) return res.status(404).json({ error:"Project not found" });
  if (!ownsProject(proj, req.user)) return res.status(403).json({ error:"Forbidden" });
  // Never return openaiKey
  const { openaiKey, ...safe } = proj;
  res.json(safe);
});

// POST /projects — create and start
app.post("/projects",
  upload.fields([{ name:"charImages", maxCount:20 }, { name:"objImages", maxCount:20 }]),
  async (req, res) => {
    try {
      const { openaiKey, masterPrompt, manus, beskrivelse, title } = req.body;
      if (!openaiKey) return res.status(400).json({ error:"OpenAI API-nøkkel mangler" });
      if (!manus)     return res.status(400).json({ error:"Manus mangler" });

      // Auth is optional — allow anonymous for now, userId null
      let userId = null;
      const token = req.headers["x-user-token"];
      if (token) {
        const user = Object.values(loadUsers()).find(u=>u.token===token);
        if (user) userId = user.id;
      }
      if (!userId) return res.status(401).json({ error:"Innlogging kreves" });

      let characters=[], objects=[], world=null;
      try { characters = JSON.parse(req.body.characters||"[]"); } catch(_){}
      try { objects    = JSON.parse(req.body.objects   ||"[]"); } catch(_){}
      try { world      = JSON.parse(req.body.world     ||"null"); } catch(_){}

      const projectId = randomUUID();
      const charFiles = req.files?.charImages||[];
      const objFiles  = req.files?.objImages ||[];

      const savedChars = characters.map(c => {
        const file = charFiles.find(f => f.originalname.startsWith(`c${c.id}_`));
        return { id:c.id, name:c.name, rigidDesc:c.rigidDesc||"", fixedTraits:c.fixedTraits||[], forbiddenTraits:c.forbiddenTraits||[], conditionalTraits:c.conditionalTraits||[], refPath:file?saveRef(projectId,"c",c.id,file.buffer):null };
      });
      const savedObjs = objects.map(o => {
        const file = objFiles.find(f => f.originalname.startsWith(`o${o.id}_`));
        return { id:o.id, name:o.name, rigidDesc:o.rigidDesc||"", defaultBinding:o.defaultBinding||"none", refPath:file?saveRef(projectId,"o",o.id,file.buffer):null };
      });

      const msPages = parseManuscript(manus);
      const descMap = parseDescriptions(beskrivelse||"", savedChars, savedObjs);
      const pages   = mergePages(msPages, descMap, world);
      if (!pages.length) return res.status(400).json({ error:"Ingen sider funnet" });

      const proj = {
        id: projectId, userId, status:"queued",
        title: title || `Bok ${new Date().toLocaleDateString("nb-NO")}`,
        createdAt: Date.now(), updatedAt: Date.now(),
        masterPrompt, manus, beskrivelse,
        characters: savedChars, objects: savedObjs, world,
        pages, results:{}, progress:0, total:pages.length,
      };
      saveProject(proj);

      // Store API key in memory only
      apiKeyStore.set(projectId, openaiKey);

      runJob(projectId).catch(err => {
        console.error(`[job ${projectId}] fatal:`, err.message);
        const p = loadProject(projectId);
        if (p) { p.status="error"; p.error=err.message; p.updatedAt=Date.now(); saveProject(p); }
        apiKeyStore.delete(projectId);
      });

      res.json({ projectId, total:pages.length });
    } catch(err) { console.error("[POST /projects]",err.message); res.status(500).json({ error:err.message }); }
  }
);

// POST /projects/:id/stop
app.post("/projects/:id/stop", auth, (req, res) => {
  const proj = loadProject(req.params.id);
  if (!proj) return res.status(404).json({ error:"Not found" });
  if (!ownsProject(proj, req.user)) return res.status(403).json({ error:"Forbidden" });
  if (proj.status !== "running" && proj.status !== "queued")
    return res.status(400).json({ error:"Prosjektet kjører ikke" });
  stopSignals.add(proj.id);
  res.json({ ok:true, message:"Stopp-signal sendt" });
});

// POST /projects/:id/rerun — restart stopped/errored pages
app.post("/projects/:id/rerun", auth, async (req, res) => {
  const proj = loadProject(req.params.id);
  if (!proj) return res.status(404).json({ error:"Not found" });
  if (!ownsProject(proj, req.user)) return res.status(403).json({ error:"Forbidden" });

  const { pageNums, openaiKey } = req.body;
  if (!openaiKey) return res.status(400).json({ error:"API-nøkkel kreves for kjøring" });

  if (pageNums?.length) {
    for (const num of pageNums) {
      proj.results = proj.results || {};
      proj.results[num] = { status:"pending", pageNum:num };
    }
  }
  proj.status = "queued"; proj.updatedAt = Date.now(); saveProject(proj);
  apiKeyStore.set(proj.id, openaiKey);

  runJob(proj.id).catch(err => {
    const p = loadProject(proj.id);
    if (p) { p.status="error"; p.error=err.message; p.updatedAt=Date.now(); saveProject(p); }
    apiKeyStore.delete(proj.id);
  });

  res.json({ ok:true });
});

// POST /projects/:id/duplicate — creates new project with same inputs
app.post("/projects/:id/duplicate", auth, (req, res) => {
  const proj = loadProject(req.params.id);
  if (!proj) return res.status(404).json({ error:"Not found" });
  if (!ownsProject(proj, req.user)) return res.status(403).json({ error:"Forbidden" });

  // Copy characters with refPath references (reuse existing files — safe read-only)
  const newId = randomUUID();
  const copiedChars = (proj.characters||[]).map(c => ({ ...c }));
  const copiedObjs  = (proj.objects||[]).map(o => ({ ...o }));

  const newProj = {
    id:           newId,
    userId:       req.user.id,
    status:       "draft",
    title:        (proj.title||"Bok") + " (kopi)",
    createdAt:    Date.now(),
    updatedAt:    Date.now(),
    masterPrompt: proj.masterPrompt,
    manus:        proj.manus,
    beskrivelse:  proj.beskrivelse,
    characters:   copiedChars,
    objects:      copiedObjs,
    world:        proj.world,
    pages:        proj.pages,
    results:      {},
    progress:     0,
    total:        proj.total,
  };
  saveProject(newProj);
  res.json({ projectId: newId, project: newProj });
});

// DELETE /projects/:id
app.delete("/projects/:id", auth, (req, res) => {
  const proj = loadProject(req.params.id);
  if (!proj) return res.status(404).json({ error:"Not found" });
  if (!ownsProject(proj, req.user)) return res.status(403).json({ error:"Forbidden" });
  if (proj.status === "running" || proj.status === "queued") {
    stopSignals.add(proj.id); // stop if running
  }
  deleteProjectFiles(proj);
  apiKeyStore.delete(proj.id);
  res.json({ ok:true });
});

// GET /projects/:id/export?type=text|zip
app.get("/projects/:id/export", auth, async (req, res) => {
  const proj = loadProject(req.params.id);
  if (!proj) return res.status(404).json({ error:"Not found" });
  if (!ownsProject(proj, req.user)) return res.status(403).json({ error:"Forbidden" });

  const type = req.query.type || "text";

  if (type === "text") {
    const content = buildTextExport(proj);
    res.setHeader("Content-Type","text/plain; charset=utf-8");
    res.setHeader("Content-Disposition",`attachment; filename="${proj.title||"bok"}.txt"`);
    res.send(content);
    return;
  }

  // type === "zip" — send metadata JSON so frontend can build zip with images
  const results = Object.values(proj.results||{})
    .filter(r => r.status === "done")
    .sort((a,b) => a.pageNum - b.pageNum)
    .map(r => ({ pageNum:r.pageNum, storyText:r.storyText, imageUrl:`/image/${proj.id}/${r.pageNum}` }));
  res.json({ title:proj.title, results });
});

// ─── Resume queued/running jobs on startup ────────────────────
// Note: API keys are not in memory after restart.
// Jobs that were running will be set to "stopped" — user must rerun with key.
function recoverJobs() {
  try {
    const files = fs.readdirSync(PROJECTS_DIR).filter(f=>f.endsWith(".json"));
    for (const f of files) {
      const proj = readJSON(path.join(PROJECTS_DIR, f), null);
      if (!proj) continue;
      if (proj.status === "running" || proj.status === "queued") {
        proj.status    = "stopped";
        proj.updatedAt = Date.now();
        proj.error     = "Server restarted — klikk Gjenstart for å fortsette.";
        writeJSON(path.join(PROJECTS_DIR, f), proj);
        console.log(`  Marked ${proj.id.slice(0,8)} as stopped (no API key after restart)`);
      }
    }
  } catch (_) {}
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n✅ StoryBook AI v8.0 on port ${PORT}`);
  recoverJobs();
});
