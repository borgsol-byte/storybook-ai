import express from "express";
import cors from "cors";
import multer from "multer";
import FormData from "form-data";
import fetch from "node-fetch";
import { randomUUID } from "crypto";
import fs from "fs";
import path from "path";

const app  = express();
app.use(cors());
app.use(express.json({ limit: "50mb" }));

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 },
});

// ─── Persistent storage (JSON files on disk) ─────────────────
// Railway has an ephemeral filesystem but persists within a session.
// Jobs survive server restarts within the same deployment.

const DATA_DIR = "./data";
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

function loadJobs() {
  try {
    const f = path.join(DATA_DIR, "jobs.json");
    if (fs.existsSync(f)) return JSON.parse(fs.readFileSync(f, "utf8"));
  } catch (_) {}
  return {};
}

function saveJobs(jobs) {
  try {
    fs.writeFileSync(path.join(DATA_DIR, "jobs.json"), JSON.stringify(jobs));
  } catch (_) {}
}

// In-memory job store (also persisted to disk)
const jobs = loadJobs();

// ─── Markup Parser ────────────────────────────────────────────
// Parses story with #p001, #c01, #o01 markers
// Format per page:
//   #p001 #c01 #c02 short visual description
//   story text for this page (shown to reader)
//
// Or just story text with #p001 as separator — app auto-suggests descriptions

function parseMarkup(markup, characters, objects) {
  const pages = [];
  // Split on #p followed by digits
  const sections = markup.split(/(?=#p\d+)/i).filter(s => s.trim());

  for (const section of sections) {
    const lines = section.trim().split("\n").map(l => l.trim()).filter(Boolean);
    if (!lines.length) continue;

    // First line contains the page marker and optional directives
    const firstLine = lines[0];
    const pageMatch = firstLine.match(/#p(\d+)/i);
    if (!pageMatch) continue;

    const pageNum = parseInt(pageMatch[1]);

    // Extract character refs: #c01, #c02 etc
    const charRefs = [...firstLine.matchAll(/#c(\d+)/gi)].map(m => m[1].padStart(2, "0"));

    // Extract object refs: #o01, #o02 etc
    const objRefs  = [...firstLine.matchAll(/#o(\d+)/gi)].map(m => m[1].padStart(2, "0"));

    // Visual description: everything after the markers on the first line
    const visualDesc = firstLine
      .replace(/#p\d+/gi, "")
      .replace(/#c\d+/gi, "")
      .replace(/#o\d+/gi, "")
      .trim();

    // Story text: remaining lines
    const storyText = lines.slice(1).join(" ").trim();

    // Resolve character names and descriptions
    const usedChars = charRefs
      .map(ref => characters.find(c => c.id === ref))
      .filter(Boolean);

    const usedObjs = objRefs
      .map(ref => objects.find(o => o.id === ref))
      .filter(Boolean);

    pages.push({
      pageNum,
      charRefs,
      objRefs,
      visualDesc,       // user-written or AI-suggested visual prompt
      storyText,        // text shown on the page
      usedChars,
      usedObjs,
    });
  }

  return pages.sort((a, b) => a.pageNum - b.pageNum);
}

// ─── Prompt Builder ───────────────────────────────────────────

const CAMERAS = [
  "wide establishing shot",
  "medium shot, eye level",
  "close-up on faces",
  "over-the-shoulder shot",
  "low-angle hero view",
  "bird's-eye view",
  "intimate medium close-up",
  "dramatic side profile",
];

const LIGHTING = [
  "warm golden hour light",
  "soft diffused daylight",
  "dramatic directional light",
  "cool silver moonlight",
  "cozy warm firelight",
  "misty morning atmosphere",
  "dappled forest sunlight",
  "magical glowing ambience",
];

function buildPrompt(masterPrompt, page, sceneIndex, totalScenes) {
  const style = masterPrompt?.trim() ||
    "Detailed children's book illustration, watercolor style, warm colors";

  // Build character description only for characters IN this scene
  const charDesc = page.usedChars.length > 0
    ? `CHARACTERS IN THIS SCENE (keep appearance 100% identical to reference images — same face, colors, clothing, proportions, zero variation): ${page.usedChars.map(c => `${c.name}: ${c.description}`).join(". ")}.`
    : "";

  const objDesc = page.usedObjs.length > 0
    ? `OBJECTS/SETTINGS IN THIS SCENE: ${page.usedObjs.map(o => `${o.name}: ${o.description}`).join(". ")}.`
    : "";

  const camera  = CAMERAS[sceneIndex % CAMERAS.length];
  const light   = LIGHTING[sceneIndex % LIGHTING.length];

  // Visual description is the core — put it first and make it prominent
  const visual = page.visualDesc || page.storyText;

  return [
    `ILLUSTRATION: ${visual}`,
    `STYLE: ${style}.`,
    charDesc,
    objDesc,
    `COMPOSITION: ${camera}. ${light}.`,
    `SCENE ${sceneIndex + 1} OF ${totalScenes}.`,
    `CRITICAL: Maintain exact same character designs as reference images. No redesign, no variation.`,
  ].filter(Boolean).join(" ");
}

// ─── AI Scene Suggester ───────────────────────────────────────
// Uses Claude to suggest visual descriptions for each page

async function suggestSceneDescriptions(pages, characters, objects, anthropicKey) {
  if (!anthropicKey) return null;

  const charList = characters.map(c => `#c${c.id} = ${c.name}: ${c.description}`).join("\n");
  const objList  = objects.map(o  => `#o${o.id} = ${o.name}: ${o.description}`).join("\n");

  const storyDump = pages.map(p =>
    `#p${String(p.pageNum).padStart(3,"0")}: ${p.storyText}`
  ).join("\n");

  const prompt = `You are a children's book illustrator writing visual scene descriptions.

CHARACTERS:
${charList}

OBJECTS/SETTINGS:
${objList}

STORY PAGES:
${storyDump}

For each page, write a SHORT visual description (max 20 words) describing exactly what should be illustrated.
- Use #c and #o references to specify which characters/objects appear
- Be visually specific: action, expression, position
- Do NOT repeat the story text literally — describe the visual

Respond ONLY with JSON array:
[{"pageNum": 1, "visualDesc": "#c01 flies awkwardly through pink glitter clouds, looking panicked", "charRefs": ["01"], "objRefs": []}, ...]`;

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
        messages: [{ role: "user", content: prompt }],
      }),
    });
    const d = await r.json();
    const text = d.content?.map(b => b.text || "").join("") || "";
    const clean = text.replace(/```json|```/g, "").trim();
    return JSON.parse(clean);
  } catch (_) {
    return null;
  }
}

// ─── Image Generation ─────────────────────────────────────────

async function generateImage(prompt, refBuffers, apiKey, retries = 2) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      if (refBuffers.length > 0) {
        const form = new FormData();
        form.append("model", "gpt-image-1");
        form.append("prompt", prompt);
        form.append("n", "1");
        form.append("size", "1024x1024");
        for (const ref of refBuffers) {
          form.append("image[]", ref.buffer, {
            filename: ref.name,
            contentType: "image/png",
          });
        }
        const r = await fetch("https://api.openai.com/v1/images/edits", {
          method: "POST",
          headers: { Authorization: `Bearer ${apiKey}`, ...form.getHeaders() },
          body: form,
        });
        const d = await r.json();
        if (d.error) throw new Error(d.error.message);
        return d.data[0].b64_json;
      } else {
        const r = await fetch("https://api.openai.com/v1/images/generations", {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization: `Bearer ${apiKey}` },
          body: JSON.stringify({
            model: "gpt-image-1", prompt, n: 1,
            size: "1024x1024", output_format: "b64_json",
          }),
        });
        const d = await r.json();
        if (d.error) throw new Error(d.error.message);
        return d.data[0].b64_json;
      }
    } catch (err) {
      console.error(`[generateImage] attempt ${attempt + 1} failed: ${err.message}`);
      if (attempt === retries) throw err;
      await new Promise(res => setTimeout(res, 2000 * (attempt + 1)));
    }
  }
}

// ─── Job Runner ───────────────────────────────────────────────
// Runs in background — survives client disconnect

async function runJob(jobId) {
  const job = jobs[jobId];
  if (!job) return;

  job.status   = "running";
  job.startedAt = Date.now();
  saveJobs(jobs);

  const { openaiKey, masterPrompt, pages, characters, objects, referenceFiles } = job;

  // Build anchor refs: first image of each character used across all pages
  // These never change — provide consistency baseline
  const anchorRefs = {};
  for (const char of characters) {
    if (char.fileData) {
      anchorRefs[char.id] = {
        buffer: Buffer.from(char.fileData, "base64"),
        name:   `char_${char.id}_${char.name}.png`,
      };
    }
  }
  for (const obj of objects) {
    if (obj.fileData) {
      anchorRefs[`o${obj.id}`] = {
        buffer: Buffer.from(obj.fileData, "base64"),
        name:   `obj_${obj.id}_${obj.name}.png`,
      };
    }
  }

  // First generated image acts as visual anchor for style consistency
  let styleAnchorB64 = null;

  for (let i = 0; i < pages.length; i++) {
    const page = pages[i];

    // Skip pages that are already done (for re-runs of specific pages)
    if (job.results[page.pageNum]?.status === "done") continue;

    job.results[page.pageNum] = { status: "generating", pageNum: page.pageNum };
    saveJobs(jobs);

    try {
      // Build ref list: only characters/objects IN this scene + style anchor
      const refs = [];

      // Add character reference images for chars in this scene
      for (const charId of page.charRefs) {
        if (anchorRefs[charId]) refs.push(anchorRefs[charId]);
      }
      // Add object reference images for objects in this scene
      for (const objId of page.objRefs) {
        if (anchorRefs[`o${objId}`]) refs.push(anchorRefs[`o${objId}`]);
      }
      // Add style anchor (first successfully generated image)
      if (styleAnchorB64) {
        refs.push({
          buffer: Buffer.from(styleAnchorB64, "base64"),
          name:   "style_anchor.png",
        });
      }

      const prompt = buildPrompt(masterPrompt, page, i, pages.length);
      console.log(`[job ${jobId}] page ${page.pageNum}/${pages.length}: generating...`);

      const b64 = await generateImage(prompt, refs, openaiKey);

      // First success becomes the style anchor
      if (!styleAnchorB64) styleAnchorB64 = b64;

      job.results[page.pageNum] = {
        status:    "done",
        pageNum:   page.pageNum,
        b64,
        storyText: page.storyText,
        visualDesc: page.visualDesc,
        prompt,
      };

      console.log(`[job ${jobId}] page ${page.pageNum} done ✓`);
    } catch (err) {
      console.error(`[job ${jobId}] page ${page.pageNum} error: ${err.message}`);
      job.results[page.pageNum] = {
        status:    "error",
        pageNum:   page.pageNum,
        error:     err.message,
        storyText: page.storyText,
      };
    }

    job.progress = i + 1;
    saveJobs(jobs);
  }

  const successCount = Object.values(job.results).filter(r => r.status === "done").length;
  job.status      = "done";
  job.completedAt = Date.now();
  job.successCount = successCount;
  saveJobs(jobs);
  console.log(`[job ${jobId}] complete — ${successCount}/${pages.length} images`);
}

// ─── Routes ───────────────────────────────────────────────────

app.get("/health", (req, res) => res.json({ ok: true, version: "4.0" }));

// Serve the frontend HTML
app.get("/", (req, res) => {
  const htmlPath = "./frontend.html";
  if (fs.existsSync(htmlPath)) {
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.send(fs.readFileSync(htmlPath, "utf8"));
  } else {
    res.json({ ok: true, message: "StoryBook AI backend v4.0 — frontend.html not found" });
  }
});

// Parse markup and return page structure (for preview)
app.post("/parse", (req, res) => {
  const { markup, characters = [], objects = [] } = req.body;
  if (!markup) return res.status(400).json({ error: "markup required" });
  const pages = parseMarkup(markup, characters, objects);
  res.json({ pages, total: pages.length });
});

// Suggest visual descriptions using Claude
app.post("/suggest", async (req, res) => {
  const { pages, characters = [], objects = [], anthropicKey } = req.body;
  if (!anthropicKey) return res.status(400).json({ error: "anthropicKey required" });
  const suggestions = await suggestSceneDescriptions(pages, characters, objects, anthropicKey);
  res.json({ suggestions });
});

// Create and start a new job
app.post(
  "/jobs",
  upload.fields([
    { name: "charImages",  maxCount: 20 },
    { name: "objImages",   maxCount: 20 },
    { name: "charMeta",    maxCount: 1  },
    { name: "objMeta",     maxCount: 1  },
  ]),
  async (req, res) => {
    try {
      const { openaiKey, masterPrompt, markup, pagesJson } = req.body;
      if (!openaiKey) return res.status(400).json({ error: "openaiKey required" });
      if (!markup && !pagesJson) return res.status(400).json({ error: "markup or pagesJson required" });

      // Parse character metadata
      let characters = [];
      let objects    = [];
      try { characters = JSON.parse(req.body.characters || "[]"); } catch (_) {}
      try { objects    = JSON.parse(req.body.objects    || "[]"); } catch (_) {}

      // Attach uploaded images to character/object definitions
      const charFiles = req.files?.charImages || [];
      const objFiles  = req.files?.objImages  || [];

      characters = characters.map(c => {
        const file = charFiles.find(f => f.originalname.startsWith(`c${c.id}_`));
        return { ...c, fileData: file ? file.buffer.toString("base64") : null };
      });

      objects = objects.map(o => {
        const file = objFiles.find(f => f.originalname.startsWith(`o${o.id}_`));
        return { ...o, fileData: file ? file.buffer.toString("base64") : null };
      });

      // Parse pages
      let pages;
      if (pagesJson) {
        pages = JSON.parse(pagesJson); // already parsed+edited by frontend
      } else {
        pages = parseMarkup(markup, characters, objects);
      }

      if (!pages.length) return res.status(400).json({ error: "No pages found in markup" });

      const jobId = randomUUID();
      jobs[jobId] = {
        id:          jobId,
        status:      "queued",
        createdAt:   Date.now(),
        openaiKey,
        masterPrompt,
        characters,
        objects,
        pages,
        results:     {},
        progress:    0,
        total:       pages.length,
      };
      saveJobs(jobs);

      // Start job in background (don't await)
      runJob(jobId).catch(err => {
        console.error(`[job ${jobId}] fatal:`, err);
        jobs[jobId].status = "error";
        jobs[jobId].error  = err.message;
        saveJobs(jobs);
      });

      res.json({ jobId, total: pages.length, pages });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  }
);

// Get job status and results
app.get("/jobs/:jobId", (req, res) => {
  const job = jobs[req.params.jobId];
  if (!job) return res.status(404).json({ error: "Job not found" });

  // Return job without sensitive data
  const { openaiKey, characters, objects, pages, ...safe } = job;
  res.json({
    ...safe,
    pageCount:  pages.length,
    charCount:  characters.length,
    // Include page structure without file data
    pages: pages.map(p => ({
      pageNum:   p.pageNum,
      storyText: p.storyText,
      visualDesc: p.visualDesc,
      charRefs:  p.charRefs,
      objRefs:   p.objRefs,
    })),
  });
});

// Re-run specific pages of an existing job
app.post("/jobs/:jobId/rerun", async (req, res) => {
  const job = jobs[req.params.jobId];
  if (!job) return res.status(404).json({ error: "Job not found" });

  const { pageNums } = req.body; // array of page numbers to re-run
  if (!pageNums?.length) return res.status(400).json({ error: "pageNums required" });

  // Mark selected pages as pending
  for (const num of pageNums) {
    if (job.results[num]) {
      job.results[num] = { status: "pending", pageNum: num };
    }
  }
  job.status = "running";
  saveJobs(jobs);

  // Re-run in background
  runJob(job.id).catch(err => {
    console.error(`[job ${job.id}] rerun fatal:`, err);
  });

  res.json({ ok: true, rerunning: pageNums });
});

// List all jobs (for returning to app)
app.get("/jobs", (req, res) => {
  const list = Object.values(jobs).map(j => ({
    id:          j.id,
    status:      j.status,
    createdAt:   j.createdAt,
    total:       j.total,
    progress:    j.progress,
    successCount: j.successCount,
  })).sort((a, b) => b.createdAt - a.createdAt).slice(0, 20);
  res.json({ jobs: list });
});

// ─── Start ────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n✅ StoryBook AI v4.0 running on port ${PORT}`);
  console.log(`   Jobs in memory: ${Object.keys(jobs).length}`);
});
