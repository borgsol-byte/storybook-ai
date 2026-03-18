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

// ─── KEY FIX: multer with high fieldSize ──────────────────────
// pagesJson is never sent from frontend anymore — parsed server-side
// Only small text fields + image files go through multer
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize:  20 * 1024 * 1024,   // 20MB per file
    fieldSize: 10 * 1024 * 1024,   // 10MB per text field
    fields:    30,
    files:     25,
  },
});

// ─── Disk storage ─────────────────────────────────────────────
const DATA_DIR  = "./data";
const IMGS_DIR  = "./data/images";
const REFS_DIR  = "./data/refs";
const USERS_DIR = "./data/users";

for (const d of [DATA_DIR, IMGS_DIR, REFS_DIR, USERS_DIR]) {
  if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
}

// ─── Persistence helpers ──────────────────────────────────────

function readJSON(filePath, fallback) {
  try {
    if (fs.existsSync(filePath)) return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (_) {}
  return fallback;
}

function writeJSON(filePath, data) {
  try { fs.writeFileSync(filePath, JSON.stringify(data, null, 2)); } catch (_) {}
}

function loadJobs() {
  return readJSON(path.join(DATA_DIR, "jobs.json"), {});
}

function saveJobs(jobs) {
  // Strip b64 and sensitive data before saving
  const safe = {};
  for (const [id, job] of Object.entries(jobs)) {
    safe[id] = {
      ...job,
      characters: (job.characters||[]).map(c => ({
        id: c.id, name: c.name, description: c.description, refPath: c.refPath,
      })),
      objects: (job.objects||[]).map(o => ({
        id: o.id, name: o.name, description: o.description, refPath: o.refPath,
      })),
      results: Object.fromEntries(
        Object.entries(job.results||{}).map(([k, r]) => [k, { ...r, b64: undefined }])
      ),
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

function loadBuffer(filePath) {
  try {
    if (filePath && fs.existsSync(filePath)) return fs.readFileSync(filePath);
  } catch (_) {}
  return null;
}

const jobs = loadJobs();

// ─── User auth ────────────────────────────────────────────────

function loadUsers() {
  return readJSON(path.join(USERS_DIR, "users.json"), {});
}

function saveUsers(users) {
  writeJSON(path.join(USERS_DIR, "users.json"), users);
}

function getUserBooks(userId) {
  return readJSON(path.join(USERS_DIR, `${userId}_books.json`), []);
}

function saveUserBooks(userId, books) {
  writeJSON(path.join(USERS_DIR, `${userId}_books.json`), books);
}

function addJobToUser(userId, jobId, title) {
  const books = getUserBooks(userId);
  if (!books.find(b => b.jobId === jobId)) {
    books.unshift({ jobId, title, createdAt: Date.now() });
    saveUserBooks(userId, books);
  }
}

// ─── Markup parser ────────────────────────────────────────────

function parseManuscript(manus) {
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
  return pages.sort((a, b) => a.pageNum - b.pageNum);
}

function parseDescriptions(desc, characters, objects) {
  const result = new Map();
  let currentScene = "";
  const lines = desc.split("\n").map(l => l.trim()).filter(Boolean);

  for (const line of lines) {
    const pm = line.match(/#p(\d+)/i);
    if (!pm) continue;
    const pageNum = parseInt(pm[1]);

    const charRefs = [...line.matchAll(/#c(\d+)/gi)].map(m => m[1].padStart(2, "0"));
    const objRefs  = [...line.matchAll(/#o(\d+)/gi)].map(m => m[1].padStart(2, "0"));

    // #sc — everything between #sc and (#/ or next # tag or end)
    const scMatch = line.match(/#sc\s+([^#]+?)(?=#\/|#[cop]\d|$)/i);
    if (scMatch) currentScene = scMatch[1].trim();

    // #/ — visual description
    const slashIdx = line.indexOf("#/");
    const visualDesc = slashIdx !== -1 ? line.slice(slashIdx + 2).trim() : "";

    result.set(pageNum, {
      charRefs,
      objRefs,
      scene: currentScene,
      visualDesc,
      usedChars: charRefs.map(r => characters.find(c => c.id === r)).filter(Boolean),
      usedObjs:  objRefs.map(r  => objects.find(o  => o.id === r)).filter(Boolean),
    });
  }
  return result;
}

function mergePages(manuscriptPages, descMap) {
  let lastScene = "";
  return manuscriptPages.map(mp => {
    const desc = descMap.get(mp.pageNum);
    if (desc?.scene) lastScene = desc.scene;
    return {
      pageNum:    mp.pageNum,
      storyText:  mp.storyText,
      charRefs:   desc?.charRefs   || [],
      objRefs:    desc?.objRefs    || [],
      scene:      lastScene,
      visualDesc: desc?.visualDesc || "",
      usedChars:  desc?.usedChars  || [],
      usedObjs:   desc?.usedObjs   || [],
    };
  });
}

// ─── Visual suggester ─────────────────────────────────────────

function suggestVisual(page) {
  const t   = page.storyText.toLowerCase();
  const who = page.usedChars.map(c => c.name).join(" and ") || "the character";
  if (/bomp|krasj|dalt|snublet|falt|dundret/.test(t)) return `${who} crashes clumsily, dust and chaos everywhere`;
  if (/fløy|flyr|lufta|vingene|svevde/.test(t))       return `${who} flying through the air`;
  if (/lo|ler|glad|jubel|lykkelig/.test(t))           return `${who} laughing with joy`;
  if (/gråt|trist|lei seg|savnet/.test(t))            return `${who} looking sad`;
  if (/redd|skrekk|gjemte|fare/.test(t))              return `${who} looking frightened`;
  if (/oppdaget|fant|plutselig|så/.test(t))           return `${who} discovering something with surprise`;
  if (/sov|hvilte|stille|rolig/.test(t))              return `${who} resting peacefully`;
  if (/spiste|mat|sulten/.test(t))                    return `${who} eating`;
  if (/glitter|glitre|lyste|funklet/.test(t))         return `${who} surrounded by magical glitter and sparkles`;
  if (/møttes|traff|fant hverandre/.test(t))          return `${who} meeting each other`;
  return `${who} — ${page.storyText.slice(0, 55)}`;
}

// ─── Prompt builder ───────────────────────────────────────────

const CAMERAS = [
  "wide establishing shot",
  "medium shot at eye level",
  "close-up on faces and expressions",
  "over-the-shoulder shot",
  "low-angle hero perspective",
  "bird's-eye view from above",
  "intimate medium close-up",
  "dramatic side profile",
];

function buildPrompt(masterPrompt, page, i, total) {
  const style = masterPrompt?.trim() ||
    "Detailed children's book illustration, warm colors, expressive characters";

  const charDesc = page.usedChars.length > 0
    ? `CHARACTERS IN THIS SCENE ONLY (maintain 100% identical appearance to reference images — same face shape, colors, clothing, proportions — zero variation): ${page.usedChars.map(c => `${c.name}: ${c.description}`).join(". ")}.`
    : "No named characters in this scene — focus on environment.";

  const objDesc  = page.usedObjs.length > 0
    ? `PROPS/OBJECTS: ${page.usedObjs.map(o => `${o.name}: ${o.description}`).join(". ")}.`
    : "";

  const sceneDesc = page.scene
    ? `ENVIRONMENT: ${page.scene}.`
    : "";

  const visual  = page.visualDesc || suggestVisual(page);
  const camera  = CAMERAS[i % CAMERAS.length];

  return [
    `ILLUSTRATION: ${visual}`,
    `STYLE: ${style}.`,
    charDesc,
    objDesc,
    sceneDesc,
    `COMPOSITION: ${camera}.`,
    `IMAGE ${i + 1} OF ${total}.`,
    `RULE: Only draw characters explicitly listed above. Keep all character designs identical to reference images.`,
  ].filter(Boolean).join(" ");
}

// ─── OpenAI image generation ──────────────────────────────────

async function generateImage(prompt, refs, apiKey, attempt = 0) {
  const MAX_RETRIES = 2;

  try {
    let responseText;

    if (refs.length > 0) {
      const form = new FormData();
      form.append("model", "gpt-image-1");
      form.append("prompt", prompt);
      form.append("n", "1");
      form.append("size", "1024x1024");

      let addedRefs = 0;
      for (const ref of refs) {
        if (!ref.buffer || ref.buffer.length === 0) continue;
        form.append("image[]", ref.buffer, {
          filename: ref.name.replace(/\.[^.]+$/, ".png"),
          contentType: "image/png",
          knownLength: ref.buffer.length,
        });
        addedRefs++;
      }

      console.log(`[openai] /edits — ${addedRefs} refs, prompt ${prompt.length} chars`);

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
        body: JSON.stringify({
          model: "gpt-image-1",
          prompt,
          n: 1,
          size: "1024x1024",
          output_format: "b64_json",
        }),
      });
      responseText = await r.text();
    }

    let data;
    try {
      data = JSON.parse(responseText);
    } catch (_) {
      throw new Error(`Non-JSON from OpenAI: ${responseText.slice(0, 300)}`);
    }

    if (data.error) {
      throw new Error(`OpenAI error: ${data.error.message || JSON.stringify(data.error)}`);
    }

    const b64 = data.data?.[0]?.b64_json;
    if (!b64) throw new Error(`No image data in response: ${responseText.slice(0, 200)}`);

    return b64;

  } catch (err) {
    console.error(`[openai] attempt ${attempt + 1} failed: ${err.message}`);
    if (attempt < MAX_RETRIES) {
      await new Promise(res => setTimeout(res, 2000 * (attempt + 1)));
      return generateImage(prompt, refs, apiKey, attempt + 1);
    }
    throw err;
  }
}

// ─── Job runner ───────────────────────────────────────────────

async function runJob(jobId) {
  const job = jobs[jobId];
  if (!job) return;

  job.status    = "running";
  job.startedAt = Date.now();
  saveJobs(jobs);

  const { openaiKey, masterPrompt, pages, characters, objects } = job;

  // Load character/object reference buffers from disk
  const charRefs = {};
  for (const c of (characters || [])) {
    const buf = loadBuffer(c.refPath);
    if (buf) charRefs[c.id] = { buffer: buf, name: `char_${c.id}.png` };
  }
  for (const o of (objects || [])) {
    const buf = loadBuffer(o.refPath);
    if (buf) charRefs[`o${o.id}`] = { buffer: buf, name: `obj_${o.id}.png` };
  }

  // Style anchor — first generated image, used to lock visual style
  const anchorPath = path.join(REFS_DIR, `${jobId}_anchor.png`);
  let hasAnchor    = fs.existsSync(anchorPath);

  for (let i = 0; i < pages.length; i++) {
    const page = pages[i];
    if (job.results[page.pageNum]?.status === "done") continue;

    job.results[page.pageNum] = { status: "generating", pageNum: page.pageNum };
    saveJobs(jobs);

    try {
      // Build refs: ONLY characters/objects explicitly in this scene
      const refs = [];
      for (const charId of (page.charRefs || [])) {
        if (charRefs[charId]) refs.push(charRefs[charId]);
      }
      for (const objId of (page.objRefs || [])) {
        if (charRefs[`o${objId}`]) refs.push(charRefs[`o${objId}`]);
      }
      // Add style anchor
      if (hasAnchor) {
        const buf = loadBuffer(anchorPath);
        if (buf) refs.push({ buffer: buf, name: "style_anchor.png" });
      }

      const scene   = (page.scene || "").slice(0, 40);
      const chars   = (page.charRefs || []).join(",") || "none";
      console.log(`[job ${jobId}] page ${page.pageNum}/${pages.length} | scene: ${scene} | chars: ${chars}`);

      const prompt = buildPrompt(masterPrompt, page, i, pages.length);
      const b64    = await generateImage(prompt, refs, openaiKey);

      // Save image to disk — free memory immediately
      const imgPath = saveImage(jobId, page.pageNum, b64);

      // Save first result as style anchor
      if (!hasAnchor) {
        fs.writeFileSync(anchorPath, Buffer.from(b64, "base64"));
        hasAnchor = true;
      }

      job.results[page.pageNum] = {
        status:     "done",
        pageNum:    page.pageNum,
        imgPath,
        storyText:  page.storyText,
        visualDesc: page.visualDesc,
        scene:      page.scene,
        charRefs:   page.charRefs,
      };
      job.progress = i + 1;
      saveJobs(jobs);
      console.log(`[job ${jobId}] page ${page.pageNum} done ✓`);

    } catch (err) {
      console.error(`[job ${jobId}] page ${page.pageNum} FAILED: ${err.message}`);
      job.results[page.pageNum] = {
        status:    "error",
        pageNum:   page.pageNum,
        error:     err.message,
        storyText: page.storyText,
      };
      job.progress = i + 1;
      saveJobs(jobs);
    }
  }

  const successCount = Object.values(job.results).filter(r => r.status === "done").length;
  job.status       = "done";
  job.completedAt  = Date.now();
  job.successCount = successCount;
  saveJobs(jobs);
  console.log(`[job ${jobId}] COMPLETE — ${successCount}/${pages.length} images`);

  // Link to user if userId present
  if (job.userId) {
    addJobToUser(job.userId, jobId, job.title || `Bok ${new Date().toLocaleDateString("nb-NO")}`);
  }
}

// ─── Auth middleware ──────────────────────────────────────────

function authMiddleware(req, res, next) {
  const token = req.headers["x-user-token"];
  if (!token) return res.status(401).json({ error: "Ikke innlogget" });
  const users = loadUsers();
  const user  = Object.values(users).find(u => u.token === token);
  if (!user)  return res.status(401).json({ error: "Ugyldig token" });
  req.user = user;
  next();
}

// ─── Routes ───────────────────────────────────────────────────

app.get("/health", (req, res) => {
  res.json({ ok: true, version: "6.0", jobs: Object.keys(jobs).length });
});

// Serve frontend
app.get("/", (req, res) => {
  const p = "./frontend.html";
  if (fs.existsSync(p)) {
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.send(fs.readFileSync(p, "utf8"));
  } else {
    res.json({ ok: true, msg: "StoryBook AI v6.0" });
  }
});

// Serve image
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

// ── Auth routes ──

app.post("/auth/register", async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: "Brukernavn og passord kreves" });
  if (username.length < 3)    return res.status(400).json({ error: "Brukernavn må være minst 3 tegn" });
  if (password.length < 6)    return res.status(400).json({ error: "Passord må være minst 6 tegn" });

  const users = loadUsers();
  if (Object.values(users).find(u => u.username.toLowerCase() === username.toLowerCase())) {
    return res.status(400).json({ error: "Brukernavnet er tatt" });
  }

  const id           = randomUUID();
  const passwordHash = await bcrypt.hash(password, 10);
  const token        = randomUUID();

  users[id] = { id, username, passwordHash, token, createdAt: Date.now() };
  saveUsers(users);

  res.json({ ok: true, token, username, userId: id });
});

app.post("/auth/login", async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: "Brukernavn og passord kreves" });

  const users = loadUsers();
  const user  = Object.values(users).find(u => u.username.toLowerCase() === username.toLowerCase());
  if (!user) return res.status(401).json({ error: "Feil brukernavn eller passord" });

  const ok = await bcrypt.compare(password, user.passwordHash);
  if (!ok) return res.status(401).json({ error: "Feil brukernavn eller passord" });

  // Rotate token on login
  user.token = randomUUID();
  saveUsers(users);

  res.json({ ok: true, token: user.token, username: user.username, userId: user.id });
});

app.get("/auth/me", authMiddleware, (req, res) => {
  res.json({ ok: true, username: req.user.username, userId: req.user.id });
});

// ── Book library ──

app.get("/library", authMiddleware, (req, res) => {
  const books = getUserBooks(req.user.id);
  // Enrich with job status
  const enriched = books.map(b => {
    const job = jobs[b.jobId];
    return {
      ...b,
      status:       job?.status       || "unknown",
      total:        job?.total        || 0,
      successCount: job?.successCount || 0,
    };
  });
  res.json({ books: enriched });
});

// ── Parse route ──

app.post("/parse", (req, res) => {
  const { manus, beskrivelse, characters = [], objects = [] } = req.body;
  if (!manus) return res.status(400).json({ error: "manus required" });

  const manuscriptPages = parseManuscript(manus);
  const descMap         = parseDescriptions(beskrivelse || "", characters, objects);
  const pages           = mergePages(manuscriptPages, descMap);

  const preview = pages.map(p => ({
    pageNum:         p.pageNum,
    storyText:       p.storyText,
    charRefs:        p.charRefs,
    objRefs:         p.objRefs,
    scene:           p.scene,
    visualDesc:      p.visualDesc,
    suggestedVisual: p.visualDesc || suggestVisual(p),
  }));

  res.json({ pages: preview, total: pages.length });
});

// ── Job routes ──
// KEY FIX: manus and beskrivelse are parsed SERVER-SIDE
// pagesJson is NEVER sent from frontend — eliminates the large field

app.post("/jobs",
  upload.fields([
    { name: "charImages", maxCount: 20 },
    { name: "objImages",  maxCount: 20 },
  ]),
  async (req, res) => {
    try {
      const { openaiKey, masterPrompt, manus, beskrivelse, title } = req.body;

      if (!openaiKey) return res.status(400).json({ error: "OpenAI API-nøkkel mangler" });
      if (!manus)     return res.status(400).json({ error: "Manus mangler" });

      let characters = [], objects = [];
      try { characters = JSON.parse(req.body.characters || "[]"); } catch (_) {}
      try { objects    = JSON.parse(req.body.objects    || "[]"); } catch (_) {}

      const jobId     = randomUUID();
      const charFiles = req.files?.charImages || [];
      const objFiles  = req.files?.objImages  || [];

      // Save reference images to disk, store path
      const savedChars = characters.map(c => {
        const file = charFiles.find(f => f.originalname.startsWith(`c${c.id}_`));
        return {
          id:          c.id,
          name:        c.name,
          description: c.description,
          refPath:     file ? saveRef(jobId, "c", c.id, file.buffer) : null,
        };
      });
      const savedObjs = objects.map(o => {
        const file = objFiles.find(f => f.originalname.startsWith(`o${o.id}_`));
        return {
          id:          o.id,
          name:        o.name,
          description: o.description,
          refPath:     file ? saveRef(jobId, "o", o.id, file.buffer) : null,
        };
      });

      // Parse entirely server-side — no pagesJson from client
      const manuscriptPages = parseManuscript(manus);
      const descMap         = parseDescriptions(beskrivelse || "", savedChars, savedObjs);
      const pages           = mergePages(manuscriptPages, descMap);

      if (!pages.length) return res.status(400).json({ error: "Ingen sider funnet i manus" });

      // Get userId from token if logged in
      let userId = null;
      const token = req.headers["x-user-token"];
      if (token) {
        const users = loadUsers();
        const user  = Object.values(users).find(u => u.token === token);
        if (user) userId = user.id;
      }

      jobs[jobId] = {
        id:           jobId,
        status:       "queued",
        createdAt:    Date.now(),
        title:        title || `Bok ${new Date().toLocaleDateString("nb-NO")}`,
        userId,
        openaiKey,
        masterPrompt,
        characters:   savedChars,
        objects:      savedObjs,
        pages,
        results:      {},
        progress:     0,
        total:        pages.length,
      };
      saveJobs(jobs);

      // Start in background
      runJob(jobId).catch(err => {
        console.error(`[job ${jobId}] fatal:`, err.message);
        if (jobs[jobId]) {
          jobs[jobId].status = "error";
          jobs[jobId].error  = err.message;
          saveJobs(jobs);
        }
      });

      res.json({ jobId, total: pages.length });

    } catch (err) {
      console.error("[POST /jobs]", err.message);
      res.status(500).json({ error: err.message });
    }
  }
);

app.get("/jobs/:jobId", (req, res) => {
  const job = jobs[req.params.jobId];
  if (!job) return res.status(404).json({ error: "Job not found" });

  const { openaiKey, characters, objects, pages, ...safe } = job;
  res.json({
    ...safe,
    pageCount: pages.length,
    pages: pages.map(p => ({
      pageNum:    p.pageNum,
      storyText:  p.storyText,
      visualDesc: p.visualDesc,
      scene:      p.scene,
      charRefs:   p.charRefs,
      objRefs:    p.objRefs,
    })),
  });
});

app.post("/jobs/:jobId/rerun", async (req, res) => {
  const job = jobs[req.params.jobId];
  if (!job) return res.status(404).json({ error: "Job not found" });

  const { pageNums } = req.body;
  if (!pageNums?.length) return res.status(400).json({ error: "pageNums required" });

  for (const num of pageNums) {
    job.results[num] = { status: "pending", pageNum: num };
  }
  job.status = "running";
  saveJobs(jobs);

  runJob(job.id).catch(err => console.error(`[rerun] fatal:`, err.message));
  res.json({ ok: true, rerunning: pageNums });
});

app.get("/jobs", (req, res) => {
  const list = Object.values(jobs)
    .map(j => ({
      id:           j.id,
      status:       j.status,
      title:        j.title,
      createdAt:    j.createdAt,
      total:        j.total,
      progress:     j.progress,
      successCount: j.successCount,
    }))
    .sort((a, b) => b.createdAt - a.createdAt)
    .slice(0, 50);
  res.json({ jobs: list });
});

// ─── Start ────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n✅ StoryBook AI v6.0 running on port ${PORT}`);
  console.log(`   Jobs: ${Object.keys(jobs).length}`);

  // Resume any interrupted jobs
  let resumed = 0;
  for (const job of Object.values(jobs)) {
    if (job.status === "running" || job.status === "queued") {
      console.log(`   Resuming job ${job.id.slice(0, 8)}...`);
      runJob(job.id).catch(err => console.error(`[resume] fatal:`, err.message));
      resumed++;
    }
  }
  if (resumed) console.log(`   Resumed ${resumed} job(s)`);
});
