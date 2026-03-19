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

// ─── Disk storage ──────────────────────────────────────────────

const DATA_DIR  = "./data";
const IMGS_DIR  = "./data/images";
const REFS_DIR  = "./data/refs";
const USERS_DIR = "./data/users";
for (const d of [DATA_DIR, IMGS_DIR, REFS_DIR, USERS_DIR]) {
  if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
}

function readJSON(p, fb) { try { if (fs.existsSync(p)) return JSON.parse(fs.readFileSync(p, "utf8")); } catch(_){} return fb; }
function writeJSON(p, d) { try { fs.writeFileSync(p, JSON.stringify(d, null, 2)); } catch(_){} }
function loadJobs() { return readJSON(path.join(DATA_DIR, "jobs.json"), {}); }
function saveJobs(jobs) {
  const safe = {};
  for (const [id, job] of Object.entries(jobs)) {
    safe[id] = {
      ...job,
      characters: (job.characters||[]).map(c => ({ id:c.id, name:c.name, rigidDesc:c.rigidDesc, fixedTraits:c.fixedTraits, forbiddenTraits:c.forbiddenTraits, conditionalTraits:c.conditionalTraits, refPath:c.refPath })),
      objects:    (job.objects||[]).map(o    => ({ id:o.id, name:o.name, rigidDesc:o.rigidDesc, defaultBinding:o.defaultBinding, refPath:o.refPath })),
      results: Object.fromEntries(Object.entries(job.results||{}).map(([k,r]) => [k, { ...r, b64:undefined }])),
    };
  }
  writeJSON(path.join(DATA_DIR, "jobs.json"), safe);
}
function saveImage(jobId, pageNum, b64) {
  const p = path.join(IMGS_DIR, `${jobId}_p${String(pageNum).padStart(3,"0")}.png`);
  fs.writeFileSync(p, Buffer.from(b64, "base64"));
  return p;
}
function saveRef(jobId, type, id, buffer) {
  const p = path.join(REFS_DIR, `${jobId}_${type}${id}.png`);
  fs.writeFileSync(p, buffer);
  return p;
}
function loadBuffer(p) { try { if (p && fs.existsSync(p)) return fs.readFileSync(p); } catch(_){} return null; }
const jobs = loadJobs();

// ─── User auth ─────────────────────────────────────────────────
function loadUsers() { return readJSON(path.join(USERS_DIR, "users.json"), {}); }
function saveUsers(u) { writeJSON(path.join(USERS_DIR, "users.json"), u); }
function getUserBooks(uid) { return readJSON(path.join(USERS_DIR, `${uid}_books.json`), []); }
function saveUserBooks(uid, books) { writeJSON(path.join(USERS_DIR, `${uid}_books.json`), books); }
function addJobToUser(uid, jobId, title) {
  const books = getUserBooks(uid);
  if (!books.find(b => b.jobId === jobId)) {
    books.unshift({ jobId, title, createdAt: Date.now() });
    saveUserBooks(uid, books);
  }
}

// ═══════════════════════════════════════════════════════════════
//  CHARACTER DNA
// ═══════════════════════════════════════════════════════════════
// conditionalTraits: [{pages:[1,2,3], trait:"..."}, {pageRange:[9,13], trait:"..."}]
// forbiddenTraits: ["wings", "crown"] (global unless overridden by conditional)

function getActiveTraits(char, pageNum) {
  const fixed = char.fixedTraits || [];
  const conditional = (char.conditionalTraits || []).filter(ct => {
    if (ct.pages) return ct.pages.includes(pageNum);
    if (ct.pageRange) return pageNum >= ct.pageRange[0] && pageNum <= ct.pageRange[1];
    return false;
  }).map(ct => ct.trait);

  // Forbidden unless activated by conditional
  const allConditionalTraits = (char.conditionalTraits || []).map(ct => ct.trait);
  const forbidden = (char.forbiddenTraits || []).filter(f => {
    // If it appears in a conditional, only forbidden outside that range
    const inConditional = (char.conditionalTraits || []).find(ct => ct.trait.toLowerCase().includes(f.toLowerCase()));
    if (inConditional) return !conditional.includes(inConditional.trait);
    return true;
  });

  return { fixed, conditional, forbidden };
}

// ═══════════════════════════════════════════════════════════════
//  EMOTION → VISUAL SIGNALS
// ═══════════════════════════════════════════════════════════════
const EMOTION_MAP = {
  glad:       "eyes bright and crinkled, wide smile, shoulders relaxed, body leaning forward with energy",
  lykkelig:   "eyes bright and crinkled, wide smile, shoulders relaxed, body leaning forward with energy",
  ler:        "mouth open laughing, eyes nearly closed, head tilted back, hands clapping or raised",
  jubel:      "arms raised high, mouth wide open, eyes wide with joy, body jumping or bouncing",
  trist:      "eyes downcast, corners of mouth pulled down, shoulders slumped forward, head low",
  gråter:     "eyes shut tight with tears on cheeks, mouth open in cry, hands covering face or hanging limp",
  savn:       "eyes cast down and to the side, small frown, arms crossed or hanging, still posture",
  redd:       "eyes wide open showing whites, mouth slightly open, shoulders raised to ears, body leaning back",
  skrekk:     "eyes wide open showing whites, mouth open in gasp, hands raised defensively, body recoiling",
  fare:       "eyes wide, body turning to flee, one foot lifted, arms raised in alarm",
  sint:       "eyes narrowed and brows furrowed low, mouth pressed tight, jaw set, hands clenched",
  frustrert:  "brows pinched, eyes tight, teeth gritted, hands in fists at sides",
  overrasket: "eyes wide, eyebrows raised high, mouth forming an O, body slightly backward",
  forvirret:  "head tilted to one side, one brow raised, mouth slightly open, hand near chin",
  bestemt:    "eyes steady and focused forward, jaw set, chin up, shoulders back and squared",
  konsentrert:"eyes narrowed in focus, mouth closed and tight, body angled toward the task",
  flau:       "eyes averted downward, one hand behind head, small sheepish smile, shoulders hunched",
  nysgjerrig: "eyes wide and bright, head tilted forward, one hand pointing or reaching out",
  magisk:     "eyes wide with wonder and slightly glowing expression, mouth slightly open, hands extended with awe",
};

function emotionToVisual(text) {
  const t = text.toLowerCase();
  for (const [word, signal] of Object.entries(EMOTION_MAP)) {
    if (t.includes(word)) return signal;
  }
  return null;
}

// ═══════════════════════════════════════════════════════════════
//  RELATION PARSER
// ═══════════════════════════════════════════════════════════════
// Supported: #o01@#c03  #o01@scene  #c01>#o01  #c03>#c02  #c01+#c02

function parseRelations(line) {
  const relations = [];

  // #o01@#c03 — object carried/held by character
  for (const m of line.matchAll(/#o(\d+)@#c(\d+)/gi)) {
    relations.push({ type: "object_on_character", objId: m[1].padStart(2,"0"), charId: m[2].padStart(2,"0") });
  }
  // #o01@scene — object is part of scene/environment
  for (const m of line.matchAll(/#o(\d+)@scene/gi)) {
    relations.push({ type: "object_in_scene", objId: m[1].padStart(2,"0") });
  }
  // #c01>#o01 — character grabs/reaches for object
  for (const m of line.matchAll(/#c(\d+)>#o(\d+)/gi)) {
    relations.push({ type: "character_reaches_object", charId: m[1].padStart(2,"0"), objId: m[2].padStart(2,"0") });
  }
  // #c03>#c02 — character helps/holds another character
  for (const m of line.matchAll(/#c(\d+)>#c(\d+)/gi)) {
    relations.push({ type: "character_reaches_character", fromCharId: m[1].padStart(2,"0"), toCharId: m[2].padStart(2,"0") });
  }
  // #c01+#c02 — characters appear together
  for (const m of line.matchAll(/#c(\d+)\+#c(\d+)/gi)) {
    relations.push({ type: "characters_together", charId1: m[1].padStart(2,"0"), charId2: m[2].padStart(2,"0") });
  }

  return relations;
}

function relationsToText(relations, characters, objects) {
  return relations.map(r => {
    const c  = id => characters.find(x => x.id === id)?.name || `character ${id}`;
    const o  = id => objects.find(x => x.id === id)?.name    || `object ${id}`;
    switch(r.type) {
      case "object_on_character":       return `${o(r.objId)} is held by or on ${c(r.charId)}`;
      case "object_in_scene":           return `${o(r.objId)} is visible as part of the environment`;
      case "character_reaches_object":  return `${c(r.charId)} is reaching for or grabbing ${o(r.objId)}`;
      case "character_reaches_character": return `${c(r.fromCharId)} is helping or holding ${c(r.toCharId)}`;
      case "characters_together":       return `${c(r.charId1)} and ${c(r.charId2)} are in the scene together`;
      default: return "";
    }
  }).filter(Boolean);
}

// ═══════════════════════════════════════════════════════════════
//  MARKUP PARSER v3
// ═══════════════════════════════════════════════════════════════

function parseManuscript(manus) {
  const pages = [];
  const sections = manus.split(/(?=#p\d+)/i).filter(s => s.trim());
  for (const sec of sections) {
    const lines = sec.trim().split("\n").map(l => l.trim()).filter(Boolean);
    if (!lines.length) continue;
    const pm = lines[0].match(/#p(\d+)/i);
    if (!pm) continue;
    pages.push({ pageNum: parseInt(pm[1]), storyText: lines.slice(1).join(" ").trim() });
  }
  return pages.sort((a,b) => a.pageNum - b.pageNum);
}

function parseDescriptions(desc, characters, objects) {
  const result  = new Map();
  let lastScene = "";
  let lastCam   = "medium";
  let lastAngle = "eye";
  let lastScale = "ground";

  for (const rawLine of desc.split("\n")) {
    const line = rawLine.trim();
    if (!line) continue;

    const pm = line.match(/#p(\d+)/i);
    if (!pm) continue;
    const pageNum = parseInt(pm[1]);

    // Remove relation tokens before extracting other tags
    // (so #o01@#c03 doesn't confuse charRef extraction)
    const lineForTags = line
      .replace(/#[oc]\d+@#[oc]\d+/gi, "")
      .replace(/#[oc]\d+@scene/gi, "")
      .replace(/#[co]\d+>[#co]\d+/gi, "")
      .replace(/#c\d+\+#c\d+/gi, "");

    const charRefs = [...lineForTags.matchAll(/#c(\d+)/gi)].map(m => m[1].padStart(2,"0"));
    const objRefs  = [...lineForTags.matchAll(/#o(\d+)/gi)].map(m => m[1].padStart(2,"0"));

    // #sc
    const scMatch = line.match(/#sc\s+([^#]+?)(?=#\/|#[a-z]|$)/i);
    if (scMatch) lastScene = scMatch[1].trim();

    // #cam
    const camMatch = line.match(/#cam\s+(close|medium|wide)/i);
    if (camMatch) lastCam = camMatch[1].toLowerCase();

    // #angle
    const angleMatch = line.match(/#angle\s+(eye|low|high|above)/i);
    if (angleMatch) lastAngle = angleMatch[1].toLowerCase();

    // #scale
    const scaleMatch = line.match(/#scale\s+(ground|above-head|rooftop|over-neighborhood)/i);
    if (scaleMatch) lastScale = scaleMatch[1].toLowerCase();

    // #focus
    const focusMatch = line.match(/#focus\s+([^#]+?)(?=#\/|#[a-z]|$)/i);
    const focus = focusMatch ? focusMatch[1].trim() : "";

    // #/ visual description
    const slashIdx = line.indexOf("#/");
    const visualDesc = slashIdx !== -1 ? line.slice(slashIdx + 2).trim() : "";

    // Relations
    const relations = parseRelations(line);

    result.set(pageNum, {
      charRefs,
      objRefs,
      scene:      lastScene,
      cam:        lastCam,
      angle:      lastAngle,
      scale:      lastScale,
      focus,
      visualDesc,
      relations,
      usedChars: charRefs.map(r => characters.find(c => c.id === r)).filter(Boolean),
      usedObjs:  objRefs.map(r  => objects.find(o  => o.id === r)).filter(Boolean),
    });
  }
  return result;
}

function mergePages(manuscriptPages, descMap, world) {
  let lastScene = world?.core_location || "";
  let lastCam   = "medium";
  let lastAngle = "eye";
  let lastScale = "ground";

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

// ═══════════════════════════════════════════════════════════════
//  VISUAL SUGGESTER
// ═══════════════════════════════════════════════════════════════

function suggestVisual(page) {
  const t   = page.storyText.toLowerCase();
  const who = page.usedChars.map(c => c.name).join(" and ") || "the character";

  if (/bomp|krasj|dundret|snublet|falt/.test(t)) return `${who} crashes clumsily — body hitting ground, limbs flailing, dust cloud`;
  if (/fløy|flyr|svevde|løftet seg/.test(t))     return `${who} airborne — body horizontal in flight, wind in hair, ground far below`;
  if (/ler|jubel|klappi/.test(t))                 return `${who} — mouth open laughing, eyes crinkled, arms raised`;
  if (/gråt|gråter/.test(t))                      return `${who} — eyes shut with tears, mouth in cry, hunched posture`;
  if (/redd|skrekk|rekulerte/.test(t))            return `${who} — eyes wide showing whites, body recoiling backward`;
  if (/oppdaget|fant|plutselig/.test(t))          return `${who} — eyes wide and fixed on discovery, hand pointing, leaning forward`;
  if (/glitre|glitter|sparkled/.test(t))          return `${who} surrounded by glittering sparkles filling the air`;
  if (/tok imot|fanget|grep/.test(t))             return `${who} — arms outstretched catching, focused expression, body braced`;
  if (/klappet|klaptet|strøk/.test(t))            return `${who} — hand gently touching other character, soft warm expression`;
  if (/snudde|snu seg/.test(t))                   return `${who} mid-turn — body half rotated, expression reacting`;
  return `${who} — ${page.storyText.slice(0, 50)}`;
}

// ═══════════════════════════════════════════════════════════════
//  CAMERA / SCALE → VISUAL LANGUAGE
// ═══════════════════════════════════════════════════════════════

function cameraToText(cam, angle, scale) {
  const camMap = {
    close:  "extreme close-up — face and shoulders fill the frame",
    medium: "medium shot — full upper body visible",
    wide:   "wide shot — full bodies and surrounding environment visible",
  };
  const angleMap = {
    eye:    "straight-on eye-level angle",
    low:    "low angle looking up — character appears powerful and large",
    high:   "high angle looking down — character appears small in environment",
    above:  "bird's-eye view directly overhead",
  };
  const scaleMap = {
    "ground":             "characters standing on solid ground, environment at normal scale",
    "above-head":         "characters elevated above head height — visible sky and ground below",
    "rooftop":            "characters at rooftop level — buildings below, open sky above, visible horizon",
    "over-neighborhood":  "characters high above the neighborhood — full streets and houses visible far below, dramatic aerial sense of height",
  };

  return [camMap[cam]||camMap.medium, angleMap[angle]||angleMap.eye, scaleMap[scale]||scaleMap.ground].join(". ");
}

// ═══════════════════════════════════════════════════════════════
//  PROMPT BUILDER — HIERARCHICAL
// ═══════════════════════════════════════════════════════════════

function buildPrompt(masterPrompt, page, pageIndex, totalPages, characters, objects, world) {
  const style = masterPrompt?.trim() || "Detailed children's book illustration, warm colors";
  const pn    = page.pageNum;

  // ── 1. Hard consistency rules ──
  const hardRules = [
    "ABSOLUTE RULE: All characters must look identical to their reference images.",
    "No new facial features. No missing fixed traits. No style drift between images.",
    "Draw only characters and objects explicitly listed below. No extras.",
  ];

  // ── 2. Active character DNA ──
  const charBlocks = page.usedChars.map(c => {
    const char    = characters.find(x => x.id === c.id) || c;
    const traits  = getActiveTraits(char, pn);
    const desc    = char.rigidDesc || char.description || "";

    const lines = [
      `CHARACTER ${char.name.toUpperCase()} (#c${char.id}): ${desc}`,
    ];
    if (traits.fixed.length)       lines.push(`  ALWAYS: ${traits.fixed.join(", ")}`);
    if (traits.conditional.length) lines.push(`  ON THIS PAGE: ${traits.conditional.join(", ")}`);
    if (traits.forbidden.length)   lines.push(`  NEVER ON THIS PAGE: ${traits.forbidden.join(", ")}`);

    return lines.join("\n");
  });

  // ── 3. Active object DNA and bindings ──
  const objBlocks = page.usedObjs.map(o => {
    const obj = objects.find(x => x.id === o.id) || o;
    const rel = page.relations.find(r =>
      (r.objId === obj.id) || (r.toObjId === obj.id)
    );
    let binding = "";
    if (rel?.type === "object_on_character") {
      const holder = characters.find(c => c.id === rel.charId)?.name || rel.charId;
      binding = ` — held by or on ${holder}`;
    } else if (rel?.type === "object_in_scene") {
      binding = " — visible as part of the environment background";
    } else if (rel?.type === "character_reaches_object") {
      const grabber = characters.find(c => c.id === rel.charId)?.name || rel.charId;
      binding = ` — ${grabber} is actively reaching for it`;
    }
    return `OBJECT ${obj.name.toUpperCase()} (#o${obj.id}): ${obj.rigidDesc || obj.description || ""}${binding}`;
  });

  // ── 4. World anchors ──
  const worldAnchors = [];
  if (world) {
    if (world.core_location)          worldAnchors.push(`WORLD: ${world.world_name || "story world"} — ${world.core_location}`);
    if (world.recurring_landmarks?.length) worldAnchors.push(`RECURRING LANDMARKS: ${world.recurring_landmarks.join(", ")}`);
    if (world.season)                 worldAnchors.push(`SEASON: ${world.season}`);
    if (world.weather_baseline)       worldAnchors.push(`WEATHER: ${world.weather_baseline}`);
    if (world.forbidden_environment_drift?.length) {
      worldAnchors.push(`ENVIRONMENT FORBIDDEN: ${world.forbidden_environment_drift.join(", ")}`);
    }
  }

  // ── 5. Scene ──
  const sceneBlock = page.scene
    ? `SCENE ENVIRONMENT: ${page.scene}.`
    : "";

  // ── 6. Camera / angle / scale / focus ──
  const cameraBlock = cameraToText(page.cam, page.angle, page.scale);
  const focusBlock  = page.focus
    ? `PRIMARY FOCUS / MAIN SUBJECT: ${page.focus}.`
    : "";

  // ── 7. Visible action and expression ──
  const emotional    = emotionToVisual(page.storyText);
  const visualDesc   = page.visualDesc || suggestVisual(page);
  const emotionBlock = emotional ? `EXPRESSION AND BODY LANGUAGE: ${emotional}.` : "";

  const relationLines = relationsToText(page.relations, characters, objects);
  const relationBlock = relationLines.length ? `RELATIONSHIPS IN SCENE: ${relationLines.join(". ")}.` : "";

  // ── 8. Negative constraints ──
  const negatives = [
    "Do not add any character not listed above.",
    "Do not change any fixed trait.",
  ];
  if (world?.forbidden_environment_drift?.length) {
    negatives.push(`Do not show: ${world.forbidden_environment_drift.join(", ")}.`);
  }
  // Add per-character forbidden traits
  page.usedChars.forEach(c => {
    const char   = characters.find(x => x.id === c.id) || c;
    const traits = getActiveTraits(char, pn);
    if (traits.forbidden.length) {
      negatives.push(`${char.name} must NOT have: ${traits.forbidden.join(", ")}.`);
    }
  });

  // ── Assemble ──
  return [
    `STYLE: ${style}.`,
    "",
    "--- CONSISTENCY RULES ---",
    hardRules.join(" "),
    "",
    "--- CHARACTER DNA ---",
    charBlocks.length ? charBlocks.join("\n") : "No named characters in this scene.",
    "",
    objBlocks.length ? "--- OBJECT DNA ---\n" + objBlocks.join("\n") : "",
    worldAnchors.length ? "\n--- WORLD ANCHORS ---\n" + worldAnchors.join("\n") : "",
    "",
    "--- SCENE ---",
    sceneBlock,
    "",
    "--- COMPOSITION ---",
    cameraBlock,
    focusBlock,
    `IMAGE ${pageIndex + 1} OF ${totalPages}.`,
    "",
    "--- ACTION ---",
    `ILLUSTRATION: ${visualDesc}`,
    emotionBlock,
    relationBlock,
    "",
    "--- NEGATIVE CONSTRAINTS ---",
    negatives.join(" "),
  ].filter(s => s !== null && s !== undefined).join("\n").replace(/\n{3,}/g, "\n\n").trim();
}

// ═══════════════════════════════════════════════════════════════
//  REFERENCE STRATEGY
// ═══════════════════════════════════════════════════════════════
// Priority: master char refs > master obj refs > style anchor (prev page)
// Previous page is secondary — never source of truth for character identity

function buildRefs(page, charRefMap, objRefMap, styleAnchorPath) {
  const refs = [];

  // 1. Master character references (highest priority)
  for (const charId of page.charRefs) {
    if (charRefMap[charId]) {
      refs.push({ ...charRefMap[charId], priority: "master_char" });
    }
  }

  // 2. Master object references
  for (const objId of page.objRefs) {
    if (objRefMap[objId]) {
      refs.push({ ...objRefMap[objId], priority: "master_obj" });
    }
    // Also include objects referenced in relations
  }
  for (const rel of page.relations) {
    const objId = rel.objId;
    if (objId && objRefMap[objId] && !refs.find(r => r.name === objRefMap[objId].name)) {
      refs.push({ ...objRefMap[objId], priority: "master_obj" });
    }
  }

  // 3. Style anchor last (lowest priority — can be omitted if many char refs already)
  if (styleAnchorPath && refs.length < 4) {
    const buf = loadBuffer(styleAnchorPath);
    if (buf) refs.push({ buffer: buf, name: "style_anchor.png", priority: "style" });
  }

  return refs;
}

// ═══════════════════════════════════════════════════════════════
//  IMAGE GENERATION
// ═══════════════════════════════════════════════════════════════

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
        if (!ref.buffer || !ref.buffer.length) continue;
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
    try { data = JSON.parse(responseText); } catch(_) { throw new Error(`Non-JSON: ${responseText.slice(0,200)}`); }
    if (data.error) throw new Error(data.error.message || JSON.stringify(data.error));
    const b64 = data.data?.[0]?.b64_json;
    if (!b64) throw new Error(`No image data: ${responseText.slice(0,200)}`);
    return b64;
  } catch (err) {
    console.error(`[openai] attempt ${attempt+1}: ${err.message}`);
    if (attempt < MAX) {
      await new Promise(res => setTimeout(res, 2000 * (attempt+1)));
      return generateImage(prompt, refs, apiKey, attempt+1);
    }
    throw err;
  }
}

// ═══════════════════════════════════════════════════════════════
//  JOB RUNNER
// ═══════════════════════════════════════════════════════════════

async function runJob(jobId) {
  const job = jobs[jobId];
  if (!job) return;
  job.status = "running"; job.startedAt = Date.now(); saveJobs(jobs);

  const { openaiKey, masterPrompt, pages, characters, objects, world } = job;

  // Build master ref maps
  const charRefMap = {};
  for (const c of (characters||[])) {
    const buf = loadBuffer(c.refPath);
    if (buf) charRefMap[c.id] = { buffer: buf, name: `char_${c.id}_${c.name}.png` };
  }
  const objRefMap = {};
  for (const o of (objects||[])) {
    const buf = loadBuffer(o.refPath);
    if (buf) objRefMap[o.id] = { buffer: buf, name: `obj_${o.id}_${o.name}.png` };
  }

  const anchorPath = path.join(REFS_DIR, `${jobId}_anchor.png`);
  let hasAnchor = fs.existsSync(anchorPath);

  for (let i = 0; i < pages.length; i++) {
    const page = pages[i];
    if (job.results[page.pageNum]?.status === "done") continue;
    job.results[page.pageNum] = { status: "generating", pageNum: page.pageNum }; saveJobs(jobs);

    try {
      const refs   = buildRefs(page, charRefMap, objRefMap, hasAnchor ? anchorPath : null);
      const prompt = buildPrompt(masterPrompt, page, i, pages.length, characters||[], objects||[], world);

      console.log(`[job ${jobId}] p${page.pageNum}/${pages.length} | cam:${page.cam} angle:${page.angle} scale:${page.scale} | chars:${page.charRefs.join(",")||"none"}`);

      const b64     = await generateImage(prompt, refs, openaiKey);
      const imgPath = saveImage(jobId, page.pageNum, b64);

      if (!hasAnchor) { fs.writeFileSync(anchorPath, Buffer.from(b64,"base64")); hasAnchor=true; }

      job.results[page.pageNum] = {
        status: "done", pageNum: page.pageNum, imgPath,
        storyText: page.storyText, visualDesc: page.visualDesc,
        scene: page.scene, charRefs: page.charRefs, cam: page.cam,
        angle: page.angle, scale: page.scale, focus: page.focus,
      };
      job.progress = i+1; saveJobs(jobs);
      console.log(`[job ${jobId}] p${page.pageNum} done ✓`);
    } catch (err) {
      console.error(`[job ${jobId}] p${page.pageNum} FAILED: ${err.message}`);
      job.results[page.pageNum] = { status:"error", pageNum:page.pageNum, error:err.message, storyText:page.storyText };
      job.progress = i+1; saveJobs(jobs);
    }
  }

  const ok = Object.values(job.results).filter(r => r.status==="done").length;
  job.status="done"; job.completedAt=Date.now(); job.successCount=ok; saveJobs(jobs);
  if (job.userId) addJobToUser(job.userId, jobId, job.title||"Bok");
  console.log(`[job ${jobId}] COMPLETE ${ok}/${pages.length}`);
}

// ═══════════════════════════════════════════════════════════════
//  AUTH
// ═══════════════════════════════════════════════════════════════

function authMiddleware(req, res, next) {
  const token = req.headers["x-user-token"];
  if (!token) return res.status(401).json({ error: "Ikke innlogget" });
  const user = Object.values(loadUsers()).find(u => u.token === token);
  if (!user) return res.status(401).json({ error: "Ugyldig token" });
  req.user = user; next();
}

// ═══════════════════════════════════════════════════════════════
//  ROUTES
// ═══════════════════════════════════════════════════════════════

app.get("/health", (req, res) => res.json({ ok:true, version:"7.0", jobs:Object.keys(jobs).length }));

app.get("/", (req, res) => {
  const p = "./frontend.html";
  if (fs.existsSync(p)) { res.setHeader("Content-Type","text/html; charset=utf-8"); res.send(fs.readFileSync(p,"utf8")); }
  else res.json({ ok:true, msg:"StoryBook AI v7.0" });
});

app.get("/image/:jobId/:pageNum", (req, res) => {
  const job = jobs[req.params.jobId];
  if (!job) return res.status(404).send("Not found");
  const result = job.results[req.params.pageNum];
  if (!result?.imgPath) return res.status(404).send("Image not found");
  const buf = loadBuffer(result.imgPath);
  if (!buf) return res.status(404).send("File missing");
  res.setHeader("Content-Type","image/png"); res.setHeader("Cache-Control","public, max-age=86400"); res.send(buf);
});

// Auth routes
app.post("/auth/register", async (req, res) => {
  const { username, password } = req.body;
  if (!username||!password) return res.status(400).json({ error:"Brukernavn og passord kreves" });
  if (username.length<3) return res.status(400).json({ error:"Brukernavn må være minst 3 tegn" });
  if (password.length<6) return res.status(400).json({ error:"Passord må være minst 6 tegn" });
  const users = loadUsers();
  if (Object.values(users).find(u => u.username.toLowerCase()===username.toLowerCase()))
    return res.status(400).json({ error:"Brukernavnet er tatt" });
  const id=randomUUID(), token=randomUUID();
  users[id]={ id, username, passwordHash: await bcrypt.hash(password,10), token, createdAt:Date.now() };
  saveUsers(users);
  res.json({ ok:true, token, username, userId:id });
});

app.post("/auth/login", async (req, res) => {
  const { username, password } = req.body;
  if (!username||!password) return res.status(400).json({ error:"Fyll inn begge felt" });
  const users = loadUsers();
  const user  = Object.values(users).find(u => u.username.toLowerCase()===username.toLowerCase());
  if (!user||!await bcrypt.compare(password, user.passwordHash))
    return res.status(401).json({ error:"Feil brukernavn eller passord" });
  user.token = randomUUID(); saveUsers(users);
  res.json({ ok:true, token:user.token, username:user.username, userId:user.id });
});

app.get("/auth/me", authMiddleware, (req, res) => res.json({ ok:true, username:req.user.username, userId:req.user.id }));

app.get("/library", authMiddleware, (req, res) => {
  const books = getUserBooks(req.user.id).map(b => {
    const job = jobs[b.jobId];
    return { ...b, status:job?.status||"unknown", total:job?.total||0, successCount:job?.successCount||0 };
  });
  res.json({ books });
});

// Parse route — server-side, returns preview data including new fields
app.post("/parse", (req, res) => {
  const { manus, beskrivelse, characters=[], objects=[], world } = req.body;
  if (!manus) return res.status(400).json({ error:"manus required" });
  const msPages  = parseManuscript(manus);
  const descMap  = parseDescriptions(beskrivelse||"", characters, objects);
  const pages    = mergePages(msPages, descMap, world);
  const preview  = pages.map(p => ({
    pageNum:    p.pageNum,
    storyText:  p.storyText.slice(0,120),
    charRefs:   p.charRefs,
    objRefs:    p.objRefs,
    scene:      p.scene,
    cam:        p.cam,
    angle:      p.angle,
    scale:      p.scale,
    focus:      p.focus,
    visualDesc: p.visualDesc,
    suggestedVisual: p.visualDesc || suggestVisual(p),
    relations:  p.relations,
  }));
  res.json({ pages: preview, total: preview.length });
});

// Create job — parses server-side, no pagesJson from client
app.post("/jobs",
  upload.fields([{ name:"charImages", maxCount:20 }, { name:"objImages", maxCount:20 }]),
  async (req, res) => {
    try {
      const { openaiKey, masterPrompt, manus, beskrivelse, title } = req.body;
      if (!openaiKey) return res.status(400).json({ error:"OpenAI API-nøkkel mangler" });
      if (!manus)     return res.status(400).json({ error:"Manus mangler" });

      let characters=[], objects=[], world=null;
      try { characters = JSON.parse(req.body.characters||"[]"); } catch(_){}
      try { objects    = JSON.parse(req.body.objects   ||"[]"); } catch(_){}
      try { world      = JSON.parse(req.body.world     ||"null"); } catch(_){}

      const jobId=randomUUID(), charFiles=req.files?.charImages||[], objFiles=req.files?.objImages||[];

      const savedChars = characters.map(c => {
        const file = charFiles.find(f => f.originalname.startsWith(`c${c.id}_`));
        return { id:c.id, name:c.name, rigidDesc:c.rigidDesc||c.description||"",
          fixedTraits:c.fixedTraits||[], forbiddenTraits:c.forbiddenTraits||[], conditionalTraits:c.conditionalTraits||[],
          refPath: file ? saveRef(jobId,"c",c.id,file.buffer) : null };
      });
      const savedObjs = objects.map(o => {
        const file = objFiles.find(f => f.originalname.startsWith(`o${o.id}_`));
        return { id:o.id, name:o.name, rigidDesc:o.rigidDesc||o.description||"",
          defaultBinding:o.defaultBinding||"none",
          refPath: file ? saveRef(jobId,"o",o.id,file.buffer) : null };
      });

      const msPages = parseManuscript(manus);
      const descMap = parseDescriptions(beskrivelse||"", savedChars, savedObjs);
      const pages   = mergePages(msPages, descMap, world);

      if (!pages.length) return res.status(400).json({ error:"Ingen sider funnet i manus" });

      let userId=null;
      const token=req.headers["x-user-token"];
      if (token) { const u=Object.values(loadUsers()).find(u=>u.token===token); if(u) userId=u.id; }

      jobs[jobId]={ id:jobId, status:"queued", createdAt:Date.now(),
        title:title||`Bok ${new Date().toLocaleDateString("nb-NO")}`, userId,
        openaiKey, masterPrompt, characters:savedChars, objects:savedObjs, world, pages,
        results:{}, progress:0, total:pages.length };
      saveJobs(jobs);

      runJob(jobId).catch(err => { if(jobs[jobId]){ jobs[jobId].status="error"; jobs[jobId].error=err.message; saveJobs(jobs); }});
      res.json({ jobId, total:pages.length });
    } catch(err) { console.error("[POST /jobs]",err.message); res.status(500).json({ error:err.message }); }
  }
);

app.get("/jobs/:jobId", (req, res) => {
  const job=jobs[req.params.jobId];
  if (!job) return res.status(404).json({ error:"Job not found" });
  const { openaiKey, characters, objects, pages, ...safe }=job;
  res.json({ ...safe, pageCount:pages.length,
    pages:pages.map(p=>({ pageNum:p.pageNum, storyText:p.storyText, visualDesc:p.visualDesc,
      scene:p.scene, charRefs:p.charRefs, objRefs:p.objRefs, cam:p.cam, angle:p.angle, scale:p.scale, focus:p.focus })) });
});

app.post("/jobs/:jobId/rerun", async (req, res) => {
  const job=jobs[req.params.jobId];
  if (!job) return res.status(404).json({ error:"Job not found" });
  const { pageNums }=req.body;
  if (!pageNums?.length) return res.status(400).json({ error:"pageNums required" });
  for (const num of pageNums) job.results[num]={ status:"pending", pageNum:num };
  job.status="running"; saveJobs(jobs);
  runJob(job.id).catch(err=>console.error("[rerun]",err.message));
  res.json({ ok:true, rerunning:pageNums });
});

app.get("/jobs", (req, res) => {
  res.json({ jobs: Object.values(jobs).map(j=>({ id:j.id, status:j.status, title:j.title, createdAt:j.createdAt, total:j.total, progress:j.progress, successCount:j.successCount })).sort((a,b)=>b.createdAt-a.createdAt).slice(0,50) });
});

// Start
const PORT=process.env.PORT||3000;
app.listen(PORT,"0.0.0.0",()=>{
  console.log(`\n✅ StoryBook AI v7.0 on port ${PORT}`);
  for (const job of Object.values(jobs)) {
    if (job.status==="running"||job.status==="queued") {
      console.log(`  Resuming ${job.id.slice(0,8)}...`);
      runJob(job.id).catch(err=>console.error("[resume]",err.message));
    }
  }
});
