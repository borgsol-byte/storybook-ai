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
app.use(express.json({ limit: "50mb" }));

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 },
});

// ─── Disk storage ─────────────────────────────────────────────

const DATA_DIR = "./data";
const IMGS_DIR = "./data/images";
const REFS_DIR = "./data/refs";
for (const d of [DATA_DIR, IMGS_DIR, REFS_DIR]) {
  if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
}

function loadJobs() {
  try {
    const f = path.join(DATA_DIR, "jobs.json");
    if (fs.existsSync(f)) return JSON.parse(fs.readFileSync(f, "utf8"));
  } catch (_) {}
  return {};
}

function saveJobs(jobs) {
  const safe = {};
  for (const [id, job] of Object.entries(jobs)) {
    safe[id] = {
      ...job,
      characters: (job.characters||[]).map(c => ({ id:c.id, name:c.name, description:c.description, refPath:c.refPath })),
      objects:    (job.objects||[]).map(o    => ({ id:o.id, name:o.name, description:o.description, refPath:o.refPath })),
      results: Object.fromEntries(
        Object.entries(job.results||{}).map(([k,r]) => [k, { ...r, b64:undefined }])
      ),
    };
  }
  try { fs.writeFileSync(path.join(DATA_DIR,"jobs.json"), JSON.stringify(safe)); } catch(_) {}
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

function loadBuffer(filePath) {
  try { if (filePath && fs.existsSync(filePath)) return fs.readFileSync(filePath); } catch(_) {}
  return null;
}

const jobs = loadJobs();

// ─── Markup Parser v2 ─────────────────────────────────────────
//
// MANUS format:
//   #p001
//   Historietekst her.
//
// BESKRIVELSE format:
//   #p001 #c01 #c02 #sc solrik eng, dag #/ Snufselufs lander klossete
//   #p005 #sc natt, måne          ← ny scene, ingen visuell beskrivelse → AI-forslag
//   #p006 #c01 #/ noe skjer       ← arver forrige #sc
//
// Regler:
//   - #sc varer til neste #sc
//   - #c arves IKKE — bare eksplisitt nevnte karakterer er med
//   - #/ starter visuell beskrivelse — alt etter er prompt-tekst
//   - Sider uten beskrivelse arver #sc og får intern foreslått beskrivelse

function parseManuscript(manus) {
  // Returns {pageNum, storyText}[]
  const pages = [];
  const sections = manus.split(/(?=#p\d+)/i).filter(s => s.trim());
  for (const sec of sections) {
    const lines = sec.trim().split("\n").map(l => l.trim()).filter(Boolean);
    if (!lines.length) continue;
    const pm = lines[0].match(/#p(\d+)/i);
    if (!pm) continue;
    const storyText = lines.slice(1).join(" ").trim();
    pages.push({ pageNum: parseInt(pm[1]), storyText });
  }
  return pages.sort((a,b) => a.pageNum - b.pageNum);
}

function parseDescriptions(desc, characters, objects) {
  // Returns Map<pageNum, {charRefs, objRefs, scene, visualDesc}>
  const result = new Map();
  let currentScene = "";

  const lines = desc.split("\n").map(l => l.trim()).filter(Boolean);

  for (const line of lines) {
    // Must start with #p
    const pm = line.match(/#p(\d+)/i);
    if (!pm) continue;
    const pageNum = parseInt(pm[1]);

    // Extract char refs
    const charRefs = [...line.matchAll(/#c(\d+)/gi)].map(m => m[1].padStart(2,"0"));

    // Extract obj refs
    const objRefs = [...line.matchAll(/#o(\d+)/gi)].map(m => m[1].padStart(2,"0"));

    // Extract #sc — everything between #sc and (#/ or end of line)
    const scMatch = line.match(/#sc\s+([^#]+?)(?=#\/|#[cop]\d|$)/i);
    if (scMatch) currentScene = scMatch[1].trim();

    // Extract visual description — everything after #/
    const slashIdx = line.indexOf("#/");
    const visualDesc = slashIdx !== -1 ? line.slice(slashIdx + 2).trim() : "";

    result.set(pageNum, {
      charRefs,
      objRefs,
      scene: currentScene,
      visualDesc,
      usedChars: charRefs.map(r => characters.find(c => c.id===r)).filter(Boolean),
      usedObjs:  objRefs.map(r  => objects.find(o  => o.id===r)).filter(Boolean),
    });
  }

  return result;
}

function mergePages(manuscriptPages, descMap, characters, objects) {
  // Carry forward the last known scene for pages not in descMap
  let lastScene = "";
  return manuscriptPages.map(mp => {
    const desc = descMap.get(mp.pageNum);
    if (desc?.scene) lastScene = desc.scene;
    return {
      pageNum:    mp.pageNum,
      storyText:  mp.storyText,
      charRefs:   desc?.charRefs   || [],
      objRefs:    desc?.objRefs    || [],
      scene:      desc?.scene      || lastScene,
      visualDesc: desc?.visualDesc || "",
      usedChars:  desc?.usedChars  || [],
      usedObjs:   desc?.usedObjs   || [],
    };
  });
}

// ─── Internal scene suggester (no API needed) ─────────────────

function suggestVisual(page) {
  // Simple keyword-based visual suggestion when no #/ description given
  const t = page.storyText.toLowerCase();
  const chars = page.usedChars.map(c => c.name).join(" og ") || "karakteren";

  if (/bomp|krasj|dalt|snublet|falt/.test(t)) return `${chars} faller eller krasjer klossete, støv og kaos`;
  if (/fløy|flyr|lufta|vingene/.test(t))       return `${chars} flyr gjennom lufta`;
  if (/lo|ler|glad|jubel/.test(t))             return `${chars} ler og er glad`;
  if (/gråt|trist|lei seg/.test(t))            return `${chars} ser trist ut`;
  if (/redd|skrekk|gjemte/.test(t))            return `${chars} ser redd ut`;
  if (/så|oppdaget|fant/.test(t))              return `${chars} oppdager noe og ser overrasket ut`;
  if (/sov|hvilte|stille/.test(t))             return `${chars} hviler i ro og stillhet`;
  if (/spiste|mat|sulten/.test(t))             return `${chars} spiser`;
  return `${chars} — ${page.storyText.slice(0, 60)}`;
}

// ─── Prompt Builder ───────────────────────────────────────────

const CAMERAS = [
  "wide establishing shot","medium shot, eye level","close-up on faces",
  "over-the-shoulder shot","low-angle hero view","bird's-eye view",
  "intimate medium close-up","dramatic side profile",
];

function buildPrompt(masterPrompt, page, i, total) {
  const style    = masterPrompt?.trim() || "Detailed children's book illustration, warm colors";
  const charDesc = page.usedChars.length > 0
    ? `CHARACTERS IN THIS SCENE ONLY (identical to reference images — same face, colors, clothing, zero variation): ${page.usedChars.map(c=>`${c.name}: ${c.description}`).join(". ")}.`
    : "No specific characters — focus on environment and action.";
  const objDesc  = page.usedObjs.length > 0
    ? `OBJECTS/PROPS: ${page.usedObjs.map(o=>`${o.name}: ${o.description}`).join(". ")}.` : "";
  const sceneDesc = page.scene
    ? `SCENE/ENVIRONMENT: ${page.scene}.` : "";
  const visual   = page.visualDesc || suggestVisual(page);
  const camera   = CAMERAS[i % CAMERAS.length];

  return [
    `ILLUSTRATION: ${visual}`,
    `STYLE: ${style}.`,
    charDesc,
    objDesc,
    sceneDesc,
    `COMPOSITION: ${camera}.`,
    `SCENE ${i+1} OF ${total}.`,
    `CRITICAL: Only the characters explicitly listed above appear in this image. Maintain exact same designs as reference images.`,
  ].filter(Boolean).join(" ");
}

// ─── Image Generation ─────────────────────────────────────────

async function generateImage(prompt, refs, apiKey, retries = 2) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      if (refs.length > 0) {
        // OpenAI /images/edits requires multipart with image as a file blob
        // Use node-fetch with FormData — append each image as a Blob with explicit type
        const form = new FormData();
        form.append("model", "gpt-image-1");
        form.append("prompt", prompt);
        form.append("n", "1");
        form.append("size", "1024x1024");

        for (const ref of refs) {
          // Ensure buffer is valid
          if (!ref.buffer || ref.buffer.length === 0) continue;
          form.append("image[]", ref.buffer, {
            filename: ref.name.endsWith(".png") ? ref.name : ref.name + ".png",
            contentType: "image/png",
            knownLength: ref.buffer.length,
          });
        }

        console.log(`[generateImage] sending ${refs.length} refs, prompt length: ${prompt.length}`);

        const r = await fetch("https://api.openai.com/v1/images/edits", {
          method: "POST",
          headers: {
            Authorization: `Bearer ${apiKey}`,
            ...form.getHeaders(),
          },
          body: form,
        });

        const text = await r.text();
        let d;
        try { d = JSON.parse(text); } catch(_) { throw new Error(`OpenAI non-JSON response: ${text.slice(0,200)}`); }
        if (d.error) throw new Error(d.error.message || JSON.stringify(d.error));
        return d.data[0].b64_json;

      } else {
        // No refs — use standard generations
        const r = await fetch("https://api.openai.com/v1/images/generations", {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization: `Bearer ${apiKey}` },
          body: JSON.stringify({
            model: "gpt-image-1",
            prompt,
            n: 1,
            size: "1024x1024",
            output_format: "b64_json",
          }),
        });
        const text = await r.text();
        let d;
        try { d = JSON.parse(text); } catch(_) { throw new Error(`OpenAI non-JSON: ${text.slice(0,200)}`); }
        if (d.error) throw new Error(d.error.message || JSON.stringify(d.error));
        return d.data[0].b64_json;
      }

    } catch (err) {
      console.error(`[generateImage] attempt ${attempt+1} failed: ${err.message}`);
      if (attempt === retries) throw err;
      await new Promise(res => setTimeout(res, 2000 * (attempt + 1)));
    }
  }
}

// ─── Job Runner ───────────────────────────────────────────────

async function runJob(jobId) {
  const job = jobs[jobId];
  if (!job) return;
  job.status = "running";
  job.startedAt = Date.now();
  saveJobs(jobs);

  const { openaiKey, masterPrompt, pages, characters, objects } = job;

  // Load ref buffers from disk
  const anchorRefs = {};
  for (const c of characters) {
    const buf = loadBuffer(c.refPath);
    if (buf) anchorRefs[c.id] = { buffer: buf, name: `char_${c.id}.png` };
  }
  for (const o of objects) {
    const buf = loadBuffer(o.refPath);
    if (buf) anchorRefs[`o${o.id}`] = { buffer: buf, name: `obj_${o.id}.png` };
  }

  const anchorPath = path.join(REFS_DIR, `${jobId}_anchor.png`);
  let hasAnchor = fs.existsSync(anchorPath);

  for (let i = 0; i < pages.length; i++) {
    const page = pages[i];
    if (job.results[page.pageNum]?.status === "done") continue;

    job.results[page.pageNum] = { status:"generating", pageNum:page.pageNum };
    saveJobs(jobs);

    try {
      // Only refs for chars/objects explicitly in this scene
      const refs = [];
      for (const charId of page.charRefs) {
        if (anchorRefs[charId]) refs.push(anchorRefs[charId]);
      }
      for (const objId of page.objRefs) {
        if (anchorRefs[`o${objId}`]) refs.push(anchorRefs[`o${objId}`]);
      }
      if (hasAnchor) {
        const buf = loadBuffer(anchorPath);
        if (buf) refs.push({ buffer: buf, name: "style_anchor.png" });
      }

      const prompt = buildPrompt(masterPrompt, page, i, pages.length);
      console.log(`[job ${jobId}] page ${page.pageNum}/${pages.length} | scene: ${page.scene.slice(0,40)} | chars: ${page.charRefs.join(",")}`);

      const b64 = await generateImage(prompt, refs, openaiKey);
      if (!b64) throw new Error("Empty response");

      const imgPath = saveImage(jobId, page.pageNum, b64);

      if (!hasAnchor) {
        fs.writeFileSync(anchorPath, Buffer.from(b64, "base64"));
        hasAnchor = true;
      }

      job.results[page.pageNum] = {
        status: "done", pageNum: page.pageNum, imgPath,
        storyText: page.storyText, visualDesc: page.visualDesc,
        scene: page.scene, charRefs: page.charRefs,
      };
      job.progress = i + 1;
      saveJobs(jobs);
      console.log(`[job ${jobId}] page ${page.pageNum} done ✓`);

    } catch (err) {
      console.error(`[job ${jobId}] page ${page.pageNum} error: ${err.message}`);
      job.results[page.pageNum] = {
        status: "error", pageNum: page.pageNum,
        error: err.message, storyText: page.storyText,
      };
      job.progress = i + 1;
      saveJobs(jobs);
    }
  }

  const ok = Object.values(job.results).filter(r => r.status==="done").length;
  job.status = "done"; job.completedAt = Date.now(); job.successCount = ok;
  saveJobs(jobs);
  console.log(`[job ${jobId}] complete — ${ok}/${pages.length}`);
}

// ─── Routes ───────────────────────────────────────────────────

app.get("/health", (req, res) => res.json({ ok:true, version:"5.0" }));

app.get("/", (req, res) => {
  const p = "./frontend.html";
  if (fs.existsSync(p)) {
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.send(fs.readFileSync(p, "utf8"));
  } else {
    res.json({ ok:true, msg:"StoryBook AI v5.0 — upload frontend.html" });
  }
});

// Serve image by jobId + pageNum
app.get("/image/:jobId/:pageNum", (req, res) => {
  const job = jobs[req.params.jobId];
  if (!job) return res.status(404).send("Not found");
  const result = job.results[req.params.pageNum];
  if (!result?.imgPath) return res.status(404).send("Image not found");
  const buf = loadBuffer(result.imgPath);
  if (!buf) return res.status(404).send("File missing");
  res.setHeader("Content-Type", "image/png");
  res.setHeader("Cache-Control", "public, max-age=86400");
  res.send(buf);
});

// Parse both manus and beskrivelse
app.post("/parse", (req, res) => {
  const { manus, beskrivelse, characters=[], objects=[] } = req.body;
  if (!manus) return res.status(400).json({ error:"manus required" });
  const manuscriptPages = parseManuscript(manus);
  const descMap = parseDescriptions(beskrivelse||"", characters, objects);
  const pages = mergePages(manuscriptPages, descMap, characters, objects);
  // Return pages with suggested visual if missing
  const preview = pages.map(p => ({
    ...p,
    suggestedVisual: p.visualDesc || suggestVisual(p),
  }));
  res.json({ pages: preview, total: pages.length });
});

// AI suggest via Claude
app.post("/suggest", async (req, res) => {
  const { pages, characters=[], objects=[], anthropicKey } = req.body;
  if (!anthropicKey) return res.status(400).json({ error:"anthropicKey required" });

  const charList  = characters.map(c=>`#c${c.id} = ${c.name}: ${c.description}`).join("\n");
  const objList   = objects.map(o=>`#o${o.id} = ${o.name}: ${o.description}`).join("\n");
  const pageList  = pages.map(p=>`#p${String(p.pageNum).padStart(3,"0")} [chars: ${p.charRefs.map(r=>"#c"+r).join(",")||"ingen"}, scene: ${p.scene||"ukjent"}]: ${p.storyText}`).join("\n");

  try {
    const r = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": anthropicKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 2000,
        messages: [{
          role: "user",
          content: `You are a children's book illustrator. Write a SHORT visual description (max 15 words) for each scene.

CHARACTERS:\n${charList || "none"}
OBJECTS:\n${objList || "none"}
PAGES:\n${pageList}

Rules:
- Use character names, not #c codes
- Describe the action/pose/expression visible in the image
- Do NOT describe style or lighting — only what characters/objects are doing
- Be specific and visual

Respond ONLY as JSON array:
[{"pageNum":1,"visualDesc":"Snufselufs sits on a cloud looking curiously downward"},...]`,
        }],
      }),
    });
    const d = await r.json();
    if (d.error) throw new Error(d.error.message || JSON.stringify(d.error));
    const text = d.content?.map(b=>b.text||"").join("")||"";
    const suggestions = JSON.parse(text.replace(/```json|```/g,"").trim());
    res.json({ suggestions });
  } catch(err) {
    console.error("[suggest]", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Create job
app.post("/jobs",
  upload.fields([{ name:"charImages", maxCount:20 }, { name:"objImages", maxCount:20 }]),
  async (req, res) => {
    try {
      const { openaiKey, masterPrompt, manus, beskrivelse, pagesJson } = req.body;
      if (!openaiKey) return res.status(400).json({ error:"openaiKey required" });

      let characters=[], objects=[];
      try { characters = JSON.parse(req.body.characters||"[]"); } catch(_) {}
      try { objects    = JSON.parse(req.body.objects   ||"[]"); } catch(_) {}

      const jobId     = randomUUID();
      const charFiles = req.files?.charImages || [];
      const objFiles  = req.files?.objImages  || [];

      characters = characters.map(c => {
        const file = charFiles.find(f => f.originalname.startsWith(`c${c.id}_`));
        return { id:c.id, name:c.name, description:c.description,
          refPath: file ? saveRef(jobId,"c",c.id,file.buffer) : null };
      });
      objects = objects.map(o => {
        const file = objFiles.find(f => f.originalname.startsWith(`o${o.id}_`));
        return { id:o.id, name:o.name, description:o.description,
          refPath: file ? saveRef(jobId,"o",o.id,file.buffer) : null };
      });

      let pages;
      if (pagesJson) {
        pages = JSON.parse(pagesJson);
      } else {
        const manuscriptPages = parseManuscript(manus||"");
        const descMap = parseDescriptions(beskrivelse||"", characters, objects);
        pages = mergePages(manuscriptPages, descMap, characters, objects);
      }

      if (!pages.length) return res.status(400).json({ error:"No pages found in manus" });

      jobs[jobId] = {
        id:jobId, status:"queued", createdAt:Date.now(),
        openaiKey, masterPrompt, characters, objects, pages,
        results:{}, progress:0, total:pages.length,
      };
      saveJobs(jobs);

      runJob(jobId).catch(err => {
        console.error(`[job ${jobId}] fatal:`, err.message);
        if (jobs[jobId]) { jobs[jobId].status="error"; jobs[jobId].error=err.message; saveJobs(jobs); }
      });

      res.json({ jobId, total:pages.length });
    } catch(err) {
      console.error("[/jobs]", err.message);
      res.status(500).json({ error:err.message });
    }
  }
);

app.get("/jobs/:jobId", (req, res) => {
  const job = jobs[req.params.jobId];
  if (!job) return res.status(404).json({ error:"Job not found" });
  const { openaiKey, characters, objects, pages, ...safe } = job;
  res.json({
    ...safe,
    pageCount: pages.length,
    pages: pages.map(p=>({
      pageNum:p.pageNum, storyText:p.storyText,
      visualDesc:p.visualDesc, scene:p.scene,
      charRefs:p.charRefs, objRefs:p.objRefs,
    })),
  });
});

app.post("/jobs/:jobId/rerun", async (req, res) => {
  const job = jobs[req.params.jobId];
  if (!job) return res.status(404).json({ error:"Job not found" });
  const { pageNums } = req.body;
  if (!pageNums?.length) return res.status(400).json({ error:"pageNums required" });
  for (const num of pageNums) job.results[num] = { status:"pending", pageNum:num };
  job.status = "running";
  saveJobs(jobs);
  runJob(job.id).catch(err => console.error(`[rerun] fatal:`, err.message));
  res.json({ ok:true, rerunning:pageNums });
});

app.get("/jobs", (req, res) => {
  const list = Object.values(jobs)
    .map(j=>({ id:j.id, status:j.status, createdAt:j.createdAt, total:j.total, progress:j.progress, successCount:j.successCount }))
    .sort((a,b)=>b.createdAt-a.createdAt).slice(0,20);
  res.json({ jobs:list });
});

// ─── Start ────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n✅ StoryBook AI v5.0 running on port ${PORT}`);
  console.log(`   Jobs on disk: ${Object.keys(jobs).length}`);
  for (const job of Object.values(jobs)) {
    if (job.status === "running" || job.status === "queued") {
      console.log(`   Resuming job ${job.id}...`);
      runJob(job.id).catch(err => console.error(`[resume] fatal:`, err.message));
    }
  }
});
