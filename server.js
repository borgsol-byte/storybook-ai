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
  limits: {
    fileSize:  20 * 1024 * 1024,
    fieldSize: 10 * 1024 * 1024,
    fields: 30,
    files:  25,
  },
});

// ─── Directories ──────────────────────────────────────────────
const DATA_DIR     = "./data";
const IMGS_DIR     = "./data/images";
const REFS_DIR     = "./data/refs";
const USERS_DIR    = "./data/users";
const PROJECTS_DIR = "./data/projects";

for (const d of [DATA_DIR, IMGS_DIR, REFS_DIR, USERS_DIR, PROJECTS_DIR]) {
  if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
}

// ─── In-memory API key store ───────────────────────────────────
// Keys are NEVER written to disk. RAM only. Deleted on job end/stop/error.
const apiKeyStore  = new Map(); // projectId -> openaiKey
const stopSignals  = new Set(); // projectId -> stop requested

// ─── Persistence helpers ──────────────────────────────────────
function readJSON(p, fb) {
  try { if (fs.existsSync(p)) return JSON.parse(fs.readFileSync(p, "utf8")); } catch (_) {}
  return fb;
}
function writeJSON(p, d) {
  try { fs.writeFileSync(p, JSON.stringify(d, null, 2)); } catch (_) {}
}

function projectPath(id) { return path.join(PROJECTS_DIR, `${id}.json`); }

function loadProject(id) {
  return readJSON(projectPath(id), null);
}

function saveProject(proj) {
  const { openaiKey, ...safe } = proj; // never persist key
  writeJSON(projectPath(proj.id), safe);
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

function deleteProjectFiles(proj) {
  for (const r of Object.values(proj.results || {})) {
    if (r.imgPath) { try { fs.unlinkSync(r.imgPath); } catch (_) {} }
  }
  for (const c of (proj.characters || [])) {
    if (c.refPath) { try { fs.unlinkSync(c.refPath); } catch (_) {} }
    if (c.refSheetPath) { try { fs.unlinkSync(c.refSheetPath); } catch (_) {} }
  }
  for (const o of (proj.objects || [])) {
    if (o.refPath) { try { fs.unlinkSync(o.refPath); } catch (_) {} }
  }
  try { fs.unlinkSync(path.join(REFS_DIR, `${proj.id}_anchor.png`)); } catch (_) {}
  try { fs.unlinkSync(projectPath(proj.id)); } catch (_) {}
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

function saveRefSheet(projectId, charId, b64) {
  const p = path.join(REFS_DIR, `${projectId}_sheet_c${charId}.png`);
  fs.writeFileSync(p, Buffer.from(b64, "base64"));
  return p;
}

function loadBuffer(p) {
  try { if (p && fs.existsSync(p)) return fs.readFileSync(p); } catch (_) {}
  return null;
}

// ─── Users ────────────────────────────────────────────────────
function loadUsers() { return readJSON(path.join(USERS_DIR, "users.json"), {}); }
function saveUsers(u) { writeJSON(path.join(USERS_DIR, "users.json"), u); }

// ─── Character DNA ────────────────────────────────────────────
function getActiveTraits(char, pageNum) {
  const fixed = char.fixedTraits || [];
  const conditional = (char.conditionalTraits || [])
    .filter(ct => {
      if (ct.pages) return ct.pages.includes(pageNum);
      if (ct.pageRange) return pageNum >= ct.pageRange[0] && pageNum <= ct.pageRange[1];
      return false;
    })
    .map(ct => ct.trait);
  const forbidden = (char.forbiddenTraits || []).filter(f => {
    const inCond = (char.conditionalTraits || [])
      .find(ct => ct.trait.toLowerCase().includes(f.toLowerCase()));
    if (inCond) return !conditional.includes(inCond.trait);
    return true;
  });
  return { fixed, conditional, forbidden };
}

// ─── Emotion → visual signals ─────────────────────────────────
const EMOTION_MAP = {
  glad:       "eyes bright and crinkled, wide smile, shoulders relaxed, body leaning forward with energy",
  lykkelig:   "eyes bright and crinkled, wide smile, shoulders relaxed, body leaning forward",
  ler:        "mouth open laughing, eyes nearly closed, head tilted back, hands raised",
  jubel:      "arms raised high, mouth wide open, eyes wide with joy, body bouncing",
  trist:      "eyes downcast, corners of mouth pulled down, shoulders slumped forward",
  gråter:     "eyes shut with tears on cheeks, mouth open in cry, hands covering face",
  redd:       "eyes wide showing whites, mouth slightly open, shoulders raised, body leaning back",
  skrekk:     "eyes wide, mouth open in gasp, hands raised defensively, body recoiling",
  sint:       "eyes narrowed, brows furrowed low, mouth pressed tight, hands clenched",
  overrasket: "eyes wide, eyebrows raised high, mouth forming an O, body backward",
  forvirret:  "head tilted, one brow raised, mouth slightly open, hand near chin",
  bestemt:    "eyes steady forward, jaw set, chin up, shoulders back and squared",
  flau:       "eyes averted down, hand behind head, sheepish smile, shoulders hunched",
  nysgjerrig: "eyes wide and bright, head tilted forward, hand pointing or reaching",
  magisk:     "eyes wide with wonder, mouth slightly open, hands extended with awe",
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
  const c = id => characters.find(x => x.id === id)?.name || `char${id}`;
  const o = id => objects.find(x  => x.id === id)?.name   || `obj${id}`;
  return rels.map(r => {
    switch (r.type) {
      case "object_on_character":         return `${o(r.objId)} is held by or on ${c(r.charId)}`;
      case "object_in_scene":             return `${o(r.objId)} is part of the background environment`;
      case "character_reaches_object":    return `${c(r.charId)} is actively reaching for or grabbing ${o(r.objId)}`;
      case "character_reaches_character": return `${c(r.fromCharId)} is helping, holding, or saving ${c(r.toCharId)}`;
      case "characters_together":         return `${c(r.charId1)} and ${c(r.charId2)} appear together in the scene`;
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

    const lineForTags = line
      .replace(/#[oc]\d+@#[oc]\d+/gi, "")
      .replace(/#[oc]\d+@scene/gi, "")
      .replace(/#[co]\d+>#[co]\d+/gi, "")
      .replace(/#c\d+\+#c\d+/gi, "");

    const charRefs = [...lineForTags.matchAll(/#c(\d+)/gi)].map(m => m[1].padStart(2, "0"));
    const objRefs  = [...lineForTags.matchAll(/#o(\d+)/gi)].map(m => m[1].padStart(2, "0"));

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
  let lastScene = world?.core_location || "";
  let lastCam = "medium", lastAngle = "eye", lastScale = "ground";

  return manuscriptPages.map(mp => {
    const desc = descMap.get(mp.pageNum);
    if (desc?.scene) lastScene = desc.scene;
    if (desc?.cam)   lastCam   = desc.cam;
    if (desc?.angle) lastAngle = desc.angle;
    if (desc?.scale) lastScale = desc.scale;
    return {
      pageNum:    mp.pageNum,
      storyText:  mp.storyText,
      charRefs:   desc?.charRefs   || [],
      objRefs:    desc?.objRefs    || [],
      scene:      lastScene,
      cam:        lastCam,
      angle:      lastAngle,
      scale:      lastScale,
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
  const t   = page.storyText.toLowerCase();
  const who = page.usedChars.map(c => c.name).join(" and ") || "the character";
  if (/bomp|krasj|dundret|snublet|falt/.test(t))  return `${who} crashes clumsily, body hitting ground, dust cloud everywhere`;
  if (/fløy|flyr|svevde|løftet seg/.test(t))      return `${who} in flight, body horizontal, ground visible far below`;
  if (/ler|jubel|klappet/.test(t))                 return `${who} laughing, mouth open, arms raised in joy`;
  if (/gråt|gråter/.test(t))                       return `${who} crying, eyes shut with tears, hunched posture`;
  if (/redd|skrekk/.test(t))                       return `${who} frightened, eyes wide showing whites, body recoiling`;
  if (/oppdaget|fant|plutselig/.test(t))           return `${who} discovering something, eyes wide, hand pointing`;
  if (/glitre|glitter/.test(t))                    return `${who} surrounded by glittering sparkles in the air`;
  if (/tok imot|fanget|grep/.test(t))              return `${who} catching something, arms outstretched, body braced`;
  return `${who} — ${page.storyText.slice(0, 55)}`;
}

// ─── Camera / scale → text ────────────────────────────────────
const CAM_MAP   = { close:"extreme close-up, faces fill the frame", medium:"medium shot, full upper body visible", wide:"wide shot, full bodies and environment" };
const ANGLE_MAP = { eye:"straight-on eye-level", low:"low angle looking up, character appears large and heroic", high:"high angle looking down, character appears small", above:"bird's-eye view directly overhead" };
const SCALE_MAP = {
  "ground":             "characters on solid ground, normal environment scale",
  "above-head":         "characters elevated above head height, sky and ground visible below",
  "rooftop":            "characters at rooftop level, buildings below, open sky above",
  "over-neighborhood":  "characters high above the neighborhood, streets and houses far below, dramatic aerial height and depth",
};

function cameraToText(cam, angle, scale) {
  return [CAM_MAP[cam]||CAM_MAP.medium, ANGLE_MAP[angle]||ANGLE_MAP.eye, SCALE_MAP[scale]||SCALE_MAP.ground].join(". ");
}

// ─── Prompt builder (hierarchical) ───────────────────────────
function buildPrompt(masterPrompt, page, pageIndex, totalPages, characters, objects, world) {
  const style = masterPrompt?.trim() || "Detailed children's book illustration, warm colors";
  const pn    = page.pageNum;

  const hardRules = [
    "ABSOLUTE CONSISTENCY RULE: All characters must be identical to their master reference sheet image.",
    "No new traits. No missing fixed traits. No style drift. No character drift across images.",
    "Draw only the characters explicitly listed below. No extras.",
  ].join(" ");

  const charBlocks = page.usedChars.map(c => {
    const char   = characters.find(x => x.id === c.id) || c;
    const traits = getActiveTraits(char, pn);
    const lines  = [`CHARACTER ${char.name.toUpperCase()} (#c${char.id}): ${char.rigidDesc || char.description || ""}`];
    if (traits.fixed.length)       lines.push(`  ALWAYS PRESENT: ${traits.fixed.join(", ")}`);
    if (traits.conditional.length) lines.push(`  ONLY THIS PAGE: ${traits.conditional.join(", ")}`);
    if (traits.forbidden.length)   lines.push(`  FORBIDDEN THIS PAGE: ${traits.forbidden.join(", ")}`);
    if (char.refSheetPath) lines.push(`  (Master reference sheet provided — match exactly)`);
    return lines.join("\n");
  });

  const objBlocks = page.usedObjs.map(o => {
    const obj = objects.find(x => x.id === o.id) || o;
    const rel = page.relations.find(r => r.objId === obj.id);
    let binding = "";
    if (rel?.type === "object_on_character")      binding = ` — held by ${characters.find(c=>c.id===rel.charId)?.name||rel.charId}`;
    else if (rel?.type === "object_in_scene")     binding = " — part of the background";
    else if (rel?.type === "character_reaches_object") binding = ` — ${characters.find(c=>c.id===rel.charId)?.name||rel.charId} reaching for it`;
    return `OBJECT ${obj.name.toUpperCase()} (#o${obj.id}): ${obj.rigidDesc || obj.description || ""}${binding}`;
  });

  const worldLines = [];
  if (world) {
    if (world.core_location)                    worldLines.push(`WORLD: ${world.world_name||"story world"} — ${world.core_location}`);
    if (world.recurring_landmarks?.length)      worldLines.push(`LANDMARKS: ${world.recurring_landmarks.join(", ")}`);
    if (world.season)                           worldLines.push(`SEASON: ${world.season}`);
    if (world.weather_baseline)                 worldLines.push(`WEATHER: ${world.weather_baseline}`);
    if (world.forbidden_environment_drift?.length) worldLines.push(`ENVIRONMENT FORBIDDEN: ${world.forbidden_environment_drift.join(", ")}`);
  }

  const emotional  = emotionToVisual(page.storyText);
  const visual     = page.visualDesc || suggestVisual(page);
  const relLines   = relationsToText(page.relations, characters, objects);

  const negatives = ["Do not add characters not listed above.", "Do not change any fixed trait.", "Keep all character designs identical to their master reference sheets."];
  if (world?.forbidden_environment_drift?.length) negatives.push(`Do not show: ${world.forbidden_environment_drift.join(", ")}.`);
  page.usedChars.forEach(c => {
    const t = getActiveTraits(characters.find(x=>x.id===c.id)||c, pn);
    if (t.forbidden.length) negatives.push(`${c.name} must NOT have: ${t.forbidden.join(", ")}.`);
  });

  return [
    `STYLE: ${style}.`,
    "",
    "--- CONSISTENCY RULES ---",
    hardRules,
    "",
    "--- CHARACTER DNA ---",
    charBlocks.length ? charBlocks.join("\n") : "No named characters — focus on environment.",
    objBlocks.length  ? "\n--- OBJECT DNA ---\n"  + objBlocks.join("\n")  : "",
    worldLines.length ? "\n--- WORLD ANCHORS ---\n" + worldLines.join("\n") : "",
    "",
    "--- SCENE ---",
    page.scene ? `SCENE: ${page.scene}.` : "",
    "",
    "--- COMPOSITION ---",
    cameraToText(page.cam, page.angle, page.scale),
    page.focus ? `PRIMARY FOCUS: ${page.focus}.` : "",
    `IMAGE ${pageIndex + 1} OF ${totalPages}.`,
    "",
    "--- ACTION ---",
    `ILLUSTRATION: ${visual}`,
    emotional  ? `EXPRESSION AND BODY LANGUAGE: ${emotional}.` : "",
    relLines.length ? `RELATIONSHIPS: ${relLines.join(". ")}.` : "",
    "",
    "--- NEGATIVE CONSTRAINTS ---",
    negatives.join(" "),
  ].filter(s => s !== null).join("\n").replace(/\n{3,}/g, "\n\n").trim();
}

// ═══════════════════════════════════════════════════════════════
//  MASTER REFERENCE SHEET GENERATION
// ═══════════════════════════════════════════════════════════════
// Generates one image per character that shows all fixed traits together.
// This is then used as the PRIMARY reference in every image generation call.
// Previous-page rolling refs are SECONDARY to this master sheet.

async function generateMasterRefSheet(char, masterPrompt, apiKey, existingRefBuffer) {
  const style = masterPrompt?.trim() || "Detailed children's book illustration, warm colors";

  const prompt = [
    `MASTER CHARACTER REFERENCE SHEET for ${char.name}.`,
    `STYLE: ${style}.`,
    `CHARACTER: ${char.rigidDesc || char.description || char.name}`,
    char.fixedTraits?.length  ? `MUST SHOW ALL OF THESE TRAITS CLEARLY: ${char.fixedTraits.join(", ")}.` : "",
    char.forbiddenTraits?.length ? `DO NOT INCLUDE: ${char.forbiddenTraits.join(", ")}.` : "",
    `Show the character standing upright, facing slightly toward the viewer, neutral expression, full body visible, on a plain white or very simple background.`,
    `This is a reference sheet — clarity and accuracy of character design is the priority.`,
    `DO NOT add other characters. DO NOT add complex backgrounds.`,
  ].filter(Boolean).join(" ");

  const refs = [];
  if (existingRefBuffer) {
    refs.push({ buffer: existingRefBuffer, name: `${char.name}_original_ref.png` });
  }

  console.log(`[refsheet] generating master sheet for ${char.name}...`);

  try {
    if (refs.length > 0) {
      const form = new FormData();
      form.append("model", "gpt-image-1");
      form.append("prompt", prompt);
      form.append("n", "1");
      form.append("size", "1024x1024");
      for (const ref of refs) {
        form.append("image[]", ref.buffer, { filename: ref.name.replace(/\.[^.]+$/, ".png"), contentType: "image/png", knownLength: ref.buffer.length });
      }
      const r = await fetch("https://api.openai.com/v1/images/edits", {
        method: "POST",
        headers: { Authorization: `Bearer ${apiKey}`, ...form.getHeaders() },
        body: form,
      });
      const text = await r.text();
      const data = JSON.parse(text);
      if (data.error) throw new Error(data.error.message);
      return data.data[0].b64_json;
    } else {
      const r = await fetch("https://api.openai.com/v1/images/generations", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${apiKey}` },
        body: JSON.stringify({ model: "gpt-image-1", prompt, n: 1, size: "1024x1024", output_format: "b64_json" }),
      });
      const text = await r.text();
      const data = JSON.parse(text);
      if (data.error) throw new Error(data.error.message);
      return data.data[0].b64_json;
    }
  } catch (err) {
    console.error(`[refsheet] failed for ${char.name}: ${err.message}`);
    return null; // non-fatal — fall back to original ref
  }
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
        form.append("image[]", ref.buffer, {
          filename: ref.name.replace(/\.[^.]+$/, ".png"),
          contentType: "image/png",
          knownLength: ref.buffer.length,
        });
        added++;
      }
      console.log(`[openai] /edits — ${added} refs, prompt ${prompt.length} chars`);
      const r = await fetch("https://api.openai.com/v1/images/edits", {
        method: "POST",
        headers: { Authorization: `Bearer ${apiKey}`, ...form.getHeaders() },
        body: form,
      });
      responseText = await r.text();
    } else {
      console.log(`[openai] /generations — prompt ${prompt.length} chars`);
      const r = await fetch("https://api.openai.com/v1/images/generations", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${apiKey}` },
        body: JSON.stringify({ model: "gpt-image-1", prompt, n: 1, size: "1024x1024", output_format: "b64_json" }),
      });
      responseText = await r.text();
    }
    let data;
    try { data = JSON.parse(responseText); } catch (_) { throw new Error(`Non-JSON: ${responseText.slice(0, 200)}`); }
    if (data.error) throw new Error(data.error.message || JSON.stringify(data.error));
    const b64 = data.data?.[0]?.b64_json;
    if (!b64) throw new Error(`No image data: ${responseText.slice(0, 200)}`);
    return b64;
  } catch (err) {
    console.error(`[openai] attempt ${attempt + 1}: ${err.message}`);
    if (attempt < MAX) {
      await new Promise(res => setTimeout(res, 2000 * (attempt + 1)));
      return generateImage(prompt, refs, apiKey, attempt + 1);
    }
    throw err;
  }
}

// ─── Reference strategy ───────────────────────────────────────
// Priority: master ref sheet > original char ref > object refs > style anchor
// Style anchor (prev page) is last resort and only if few other refs exist.

function buildRefs(page, charRefMap, objRefMap, styleAnchorPath) {
  const refs = [];

  for (const charId of page.charRefs) {
    // Master ref sheet takes priority over original ref
    if (charRefMap[`sheet_${charId}`]) {
      refs.push(charRefMap[`sheet_${charId}`]);
    } else if (charRefMap[charId]) {
      refs.push(charRefMap[charId]);
    }
  }

  for (const objId of page.objRefs) {
    if (objRefMap[objId]) refs.push(objRefMap[objId]);
  }
  for (const r of page.relations) {
    if (r.objId && objRefMap[r.objId] && !refs.find(x => x.name === objRefMap[r.objId].name)) {
      refs.push(objRefMap[r.objId]);
    }
  }

  // Style anchor only if few other refs (don't overwhelm with too many images)
  if (styleAnchorPath && refs.length < 3) {
    const buf = loadBuffer(styleAnchorPath);
    if (buf) refs.push({ buffer: buf, name: "style_anchor.png" });
  }

  return refs;
}

// ─── Job runner ───────────────────────────────────────────────
async function runJob(projectId) {
  const proj = loadProject(projectId);
  if (!proj) return;

  const apiKey = apiKeyStore.get(projectId);
  if (!apiKey) {
    console.error(`[job ${projectId}] No API key — marking stopped`);
    proj.status = "stopped";
    proj.error  = "Server restarted — kjør på nytt for å fortsette.";
    proj.updatedAt = Date.now();
    saveProject(proj);
    return;
  }

  // Mark running
  proj.status    = "running";
  proj.startedAt = Date.now();
  proj.updatedAt = Date.now();
  saveProject(proj);

  const { masterPrompt, pages, characters, objects, world } = proj;

  // ── Phase 1: Generate master reference sheets ──────────────
  // One call per character with fixedTraits or an uploaded reference image.
  // Only generate if sheet doesn't already exist.
  console.log(`[job ${projectId}] Phase 1: generating master reference sheets...`);

  const updatedChars = [...characters];
  for (let i = 0; i < updatedChars.length; i++) {
    if (stopSignals.has(projectId)) break;
    const char = updatedChars[i];

    // Skip if sheet already exists
    if (char.refSheetPath && fs.existsSync(char.refSheetPath)) {
      console.log(`[refsheet] ${char.name} — already exists, skipping`);
      continue;
    }

    // Only generate sheet if there are fixed traits or an original ref to work from
    const hasTraits = (char.fixedTraits?.length || 0) > 0;
    const hasRef    = !!(char.refPath && fs.existsSync(char.refPath));
    if (!hasTraits && !hasRef) {
      console.log(`[refsheet] ${char.name} — no traits or ref, skipping`);
      continue;
    }

    const existingRefBuffer = hasRef ? loadBuffer(char.refPath) : null;
    const b64 = await generateMasterRefSheet(char, masterPrompt, apiKey, existingRefBuffer);

    if (b64) {
      const sheetPath = saveRefSheet(projectId, char.id, b64);
      updatedChars[i] = { ...char, refSheetPath: sheetPath };
      console.log(`[refsheet] ${char.name} — saved to ${sheetPath}`);
    }
  }

  // Save updated chars with sheet paths
  const freshAfterSheets = loadProject(projectId);
  if (freshAfterSheets) {
    freshAfterSheets.characters = updatedChars;
    freshAfterSheets.updatedAt = Date.now();
    saveProject(freshAfterSheets);
  }

  // ── Phase 2: Build ref maps ────────────────────────────────
  const charRefMap = {};
  for (const c of updatedChars) {
    const sheetBuf = loadBuffer(c.refSheetPath);
    if (sheetBuf) {
      charRefMap[`sheet_${c.id}`] = { buffer: sheetBuf, name: `${c.name}_master_sheet.png` };
    }
    const origBuf = loadBuffer(c.refPath);
    if (origBuf) {
      charRefMap[c.id] = { buffer: origBuf, name: `${c.name}_original.png` };
    }
  }

  const objRefMap = {};
  for (const o of (objects || [])) {
    const buf = loadBuffer(o.refPath);
    if (buf) objRefMap[o.id] = { buffer: buf, name: `${o.name}_ref.png` };
  }

  const anchorPath = path.join(REFS_DIR, `${projectId}_anchor.png`);
  let hasAnchor = fs.existsSync(anchorPath);

  // ── Phase 3: Generate pages ────────────────────────────────
  console.log(`[job ${projectId}] Phase 2: generating ${pages.length} pages...`);

  for (let i = 0; i < pages.length; i++) {
    if (stopSignals.has(projectId)) {
      stopSignals.delete(projectId);
      const p = loadProject(projectId);
      if (p) { p.status = "stopped"; p.updatedAt = Date.now(); saveProject(p); }
      apiKeyStore.delete(projectId);
      return;
    }

    const page = pages[i];
    const cur  = loadProject(projectId);
    if (cur?.results?.[page.pageNum]?.status === "done") continue;

    if (cur) {
      cur.results = cur.results || {};
      cur.results[page.pageNum] = { status: "generating", pageNum: page.pageNum };
      cur.updatedAt = Date.now();
      saveProject(cur);
    }

    try {
      const refs   = buildRefs(page, charRefMap, objRefMap, hasAnchor ? anchorPath : null);
      const prompt = buildPrompt(masterPrompt, page, i, pages.length, updatedChars, objects || [], world);

      console.log(`[job ${projectId}] p${page.pageNum}/${pages.length} cam:${page.cam} scale:${page.scale} chars:${page.charRefs.join(",") || "none"} refs:${refs.length}`);

      const b64     = await generateImage(prompt, refs, apiKey);
      const imgPath = saveImage(projectId, page.pageNum, b64);

      if (!hasAnchor) {
        fs.writeFileSync(anchorPath, Buffer.from(b64, "base64"));
        hasAnchor = true;
      }

      const fresh = loadProject(projectId);
      if (fresh) {
        fresh.results = fresh.results || {};
        fresh.results[page.pageNum] = {
          status:    "done",
          pageNum:   page.pageNum,
          imgPath,
          storyText: page.storyText,
          visualDesc: page.visualDesc,
          scene:     page.scene,
          charRefs:  page.charRefs,
          cam:       page.cam,
          angle:     page.angle,
          scale:     page.scale,
          focus:     page.focus,
        };
        fresh.progress  = i + 1;
        fresh.updatedAt = Date.now();
        saveProject(fresh);
      }
      console.log(`[job ${projectId}] p${page.pageNum} done ✓`);
    } catch (err) {
      console.error(`[job ${projectId}] p${page.pageNum} FAILED: ${err.message}`);
      const fresh = loadProject(projectId);
      if (fresh) {
        fresh.results = fresh.results || {};
        fresh.results[page.pageNum] = { status: "error", pageNum: page.pageNum, error: err.message, storyText: page.storyText };
        fresh.progress  = i + 1;
        fresh.updatedAt = Date.now();
        saveProject(fresh);
      }
    }
  }

  const finalProj = loadProject(projectId);
  if (finalProj) {
    const ok = Object.values(finalProj.results || {}).filter(r => r.status === "done").length;
    finalProj.status       = "done";
    finalProj.completedAt  = Date.now();
    finalProj.successCount = ok;
    finalProj.updatedAt    = Date.now();
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
  if (!user)  return res.status(401).json({ error: "Ugyldig token" });
  req.user = user;
  next();
}

function ownsProject(proj, user) {
  return proj.userId === user.id;
}

// ─── Text export ──────────────────────────────────────────────
function buildTextExport(proj) {
  return Object.values(proj.results || {})
    .filter(r => r.status === "done")
    .sort((a, b) => a.pageNum - b.pageNum)
    .map(r => `[IMAGE: side_${String(r.pageNum).padStart(3, "0")}]\n${r.storyText}`)
    .join("\n\n---\n\n");
}

// ─── Routes ───────────────────────────────────────────────────

app.get("/health", (req, res) => res.json({ ok: true, version: "9.0" }));

app.get("/", (req, res) => {
  const p = "./frontend.html";
  if (fs.existsSync(p)) {
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.send(fs.readFileSync(p, "utf8"));
  } else {
    res.json({ ok: true, msg: "StoryBook AI v9.0" });
  }
});

// Image — owner only
app.get("/image/:projectId/:pageNum", auth, (req, res) => {
  const proj = loadProject(req.params.projectId);
  if (!proj) return res.status(404).send("Not found");
  if (!ownsProject(proj, req.user)) return res.status(403).send("Forbidden");
  const result = proj.results?.[req.params.pageNum];
  if (!result?.imgPath) return res.status(404).send("Image not found");
  const buf = loadBuffer(result.imgPath);
  if (!buf) return res.status(404).send("File missing");
  res.setHeader("Content-Type", "image/png");
  res.setHeader("Cache-Control", "private, max-age=3600");
  res.send(buf);
});

// ── Auth ──

app.post("/auth/register", async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: "Brukernavn og passord kreves" });
  if (username.length < 3) return res.status(400).json({ error: "Minst 3 tegn" });
  if (password.length < 6) return res.status(400).json({ error: "Minst 6 tegn" });
  const users = loadUsers();
  if (Object.values(users).find(u => u.username.toLowerCase() === username.toLowerCase()))
    return res.status(400).json({ error: "Brukernavnet er tatt" });
  const id = randomUUID(), token = randomUUID();
  users[id] = { id, username, passwordHash: await bcrypt.hash(password, 10), token, createdAt: Date.now() };
  saveUsers(users);
  res.json({ ok: true, token, username, userId: id });
});

app.post("/auth/login", async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: "Fyll inn begge felt" });
  const users = loadUsers();
  const user  = Object.values(users).find(u => u.username.toLowerCase() === username.toLowerCase());
  if (!user || !await bcrypt.compare(password, user.passwordHash))
    return res.status(401).json({ error: "Feil brukernavn eller passord" });
  user.token = randomUUID();
  saveUsers(users);
  res.json({ ok: true, token: user.token, username: user.username, userId: user.id });
});

app.get("/auth/me", auth, (req, res) =>
  res.json({ ok: true, username: req.user.username, userId: req.user.id })
);

// ── Parse ──

app.post("/parse", (req, res) => {
  const { manus, beskrivelse, characters = [], objects = [], world } = req.body;
  if (!manus) return res.status(400).json({ error: "manus required" });
  const msPages = parseManuscript(manus);
  const descMap = parseDescriptions(beskrivelse || "", characters, objects);
  const pages   = mergePages(msPages, descMap, world);
  res.json({
    pages: pages.map(p => ({
      pageNum: p.pageNum, storyText: p.storyText.slice(0, 120),
      charRefs: p.charRefs, objRefs: p.objRefs,
      scene: p.scene, cam: p.cam, angle: p.angle, scale: p.scale, focus: p.focus,
      visualDesc: p.visualDesc, suggestedVisual: p.visualDesc || suggestVisual(p),
      relations: p.relations,
    })),
    total: pages.length,
  });
});

// ── Projects ──

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
    error:        p.error,
  }));
  res.json({ projects });
});

app.get("/projects/:id", auth, (req, res) => {
  const proj = loadProject(req.params.id);
  if (!proj) return res.status(404).json({ error: "Not found" });
  if (!ownsProject(proj, req.user)) return res.status(403).json({ error: "Forbidden" });
  const { openaiKey, ...safe } = proj;
  res.json(safe);
});

app.post("/projects",
  upload.fields([{ name: "charImages", maxCount: 20 }, { name: "objImages", maxCount: 20 }]),
  async (req, res) => {
    try {
      const { openaiKey, masterPrompt, manus, beskrivelse, title } = req.body;
      if (!openaiKey) return res.status(400).json({ error: "OpenAI API-nøkkel mangler" });
      if (!manus)     return res.status(400).json({ error: "Manus mangler" });

      const token = req.headers["x-user-token"];
      if (!token) return res.status(401).json({ error: "Innlogging kreves" });
      const user = Object.values(loadUsers()).find(u => u.token === token);
      if (!user) return res.status(401).json({ error: "Ugyldig token" });

      let characters = [], objects = [], world = null;
      try { characters = JSON.parse(req.body.characters || "[]"); } catch (_) {}
      try { objects    = JSON.parse(req.body.objects    || "[]"); } catch (_) {}
      try { world      = JSON.parse(req.body.world      || "null"); } catch (_) {}

      const projectId = randomUUID();
      const charFiles = req.files?.charImages || [];
      const objFiles  = req.files?.objImages  || [];

      const savedChars = characters.map(c => {
        const file = charFiles.find(f => f.originalname.startsWith(`c${c.id}_`));
        return {
          id: c.id, name: c.name,
          rigidDesc: c.rigidDesc || "",
          fixedTraits: c.fixedTraits || [],
          forbiddenTraits: c.forbiddenTraits || [],
          conditionalTraits: c.conditionalTraits || [],
          refPath: file ? saveRef(projectId, "c", c.id, file.buffer) : null,
          refSheetPath: null, // generated by runJob phase 1
        };
      });

      const savedObjs = objects.map(o => {
        const file = objFiles.find(f => f.originalname.startsWith(`o${o.id}_`));
        return {
          id: o.id, name: o.name,
          rigidDesc: o.rigidDesc || "",
          defaultBinding: o.defaultBinding || "none",
          refPath: file ? saveRef(projectId, "o", o.id, file.buffer) : null,
        };
      });

      const msPages = parseManuscript(manus);
      const descMap = parseDescriptions(beskrivelse || "", savedChars, savedObjs);
      const pages   = mergePages(msPages, descMap, world);
      if (!pages.length) return res.status(400).json({ error: "Ingen sider funnet" });

      const proj = {
        id: projectId, userId: user.id, status: "queued",
        title: title || `Bok ${new Date().toLocaleDateString("nb-NO")}`,
        createdAt: Date.now(), updatedAt: Date.now(),
        masterPrompt, manus, beskrivelse,
        characters: savedChars, objects: savedObjs, world,
        pages, results: {}, progress: 0, total: pages.length,
      };
      saveProject(proj);
      apiKeyStore.set(projectId, openaiKey);

      runJob(projectId).catch(err => {
        const p = loadProject(projectId);
        if (p) { p.status = "error"; p.error = err.message; p.updatedAt = Date.now(); saveProject(p); }
        apiKeyStore.delete(projectId);
      });

      res.json({ projectId, total: pages.length });
    } catch (err) {
      console.error("[POST /projects]", err.message);
      res.status(500).json({ error: err.message });
    }
  }
);

app.post("/projects/:id/stop", auth, (req, res) => {
  const proj = loadProject(req.params.id);
  if (!proj) return res.status(404).json({ error: "Not found" });
  if (!ownsProject(proj, req.user)) return res.status(403).json({ error: "Forbidden" });
  if (proj.status !== "running" && proj.status !== "queued")
    return res.status(400).json({ error: "Prosjektet kjører ikke" });
  stopSignals.add(proj.id);
  res.json({ ok: true });
});

app.post("/projects/:id/rerun", auth, async (req, res) => {
  const proj = loadProject(req.params.id);
  if (!proj) return res.status(404).json({ error: "Not found" });
  if (!ownsProject(proj, req.user)) return res.status(403).json({ error: "Forbidden" });

  const { pageNums, openaiKey } = req.body;
  if (!openaiKey) return res.status(400).json({ error: "API-nøkkel kreves" });

  if (pageNums?.length) {
    proj.results = proj.results || {};
    for (const num of pageNums) {
      proj.results[num] = { status: "pending", pageNum: num };
    }
  }
  proj.status = "queued"; proj.updatedAt = Date.now(); saveProject(proj);
  apiKeyStore.set(proj.id, openaiKey);

  runJob(proj.id).catch(err => {
    const p = loadProject(proj.id);
    if (p) { p.status = "error"; p.error = err.message; p.updatedAt = Date.now(); saveProject(p); }
    apiKeyStore.delete(proj.id);
  });

  res.json({ ok: true });
});

app.post("/projects/:id/duplicate", auth, (req, res) => {
  const proj = loadProject(req.params.id);
  if (!proj) return res.status(404).json({ error: "Not found" });
  if (!ownsProject(proj, req.user)) return res.status(403).json({ error: "Forbidden" });

  const newId  = randomUUID();
  const newProj = {
    id:           newId,
    userId:       req.user.id,
    status:       "draft",
    title:        (proj.title || "Bok") + " (kopi)",
    createdAt:    Date.now(),
    updatedAt:    Date.now(),
    masterPrompt: proj.masterPrompt,
    manus:        proj.manus,
    beskrivelse:  proj.beskrivelse,
    characters:   (proj.characters || []).map(c => ({ ...c, refSheetPath: null })), // sheets regenerated on next run
    objects:      (proj.objects    || []).map(o => ({ ...o })),
    world:        proj.world,
    pages:        proj.pages,
    results:      {},
    progress:     0,
    total:        proj.total,
  };
  saveProject(newProj);
  res.json({ projectId: newId, project: newProj });
});

app.delete("/projects/:id", auth, (req, res) => {
  const proj = loadProject(req.params.id);
  if (!proj) return res.status(404).json({ error: "Not found" });
  if (!ownsProject(proj, req.user)) return res.status(403).json({ error: "Forbidden" });
  if (proj.status === "running" || proj.status === "queued") {
    stopSignals.add(proj.id);
  }
  deleteProjectFiles(proj);
  apiKeyStore.delete(proj.id);
  res.json({ ok: true });
});

app.get("/projects/:id/export", auth, (req, res) => {
  const proj = loadProject(req.params.id);
  if (!proj) return res.status(404).json({ error: "Not found" });
  if (!ownsProject(proj, req.user)) return res.status(403).json({ error: "Forbidden" });

  const type = req.query.type || "text";
  if (type === "text") {
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${proj.title || "bok"}.txt"`);
    res.send(buildTextExport(proj));
  } else {
    const results = Object.values(proj.results || {})
      .filter(r => r.status === "done")
      .sort((a, b) => a.pageNum - b.pageNum)
      .map(r => ({ pageNum: r.pageNum, storyText: r.storyText, imageUrl: `/image/${proj.id}/${r.pageNum}` }));
    res.json({ title: proj.title, results });
  }
});

// ─── Startup recovery ──────────────────────────────────────────
function recoverJobs() {
  try {
    const files = fs.readdirSync(PROJECTS_DIR).filter(f => f.endsWith(".json"));
    let recovered = 0;
    for (const f of files) {
      const proj = readJSON(path.join(PROJECTS_DIR, f), null);
      if (!proj) continue;
      if (proj.status === "running" || proj.status === "queued") {
        proj.status    = "stopped";
        proj.error     = "Server restartet — klikk Gjenstart for å fortsette.";
        proj.updatedAt = Date.now();
        writeJSON(path.join(PROJECTS_DIR, f), proj);
        recovered++;
      }
    }
    if (recovered) console.log(`  Marked ${recovered} job(s) as stopped (no API key after restart)`);
  } catch (_) {}
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n✅ StoryBook AI v9.0 on port ${PORT}`);
  recoverJobs();
});
