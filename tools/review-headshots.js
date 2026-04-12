/**
 * Headshot review server.
 * Run: node tools/review-headshots.js
 * Open: http://localhost:3456
 */
const http = require('http');
const fs = require('fs');
const path = require('path');

const IMAGES_DIR = path.join(__dirname, '..', 'site', 'public', 'images', 'people');
const STATE_FILE = path.join(__dirname, '.headshot-review-state.json');
const PORT = 3456;

// Server-side approval state
function loadState() {
  try {
    return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  } catch {
    return { approved: [] };
  }
}

function saveState(state) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

function getReviewList() {
  const files = fs.readdirSync(IMAGES_DIR);
  const headshots = new Map();
  const mains = new Map();

  for (const f of files) {
    const ext = path.extname(f).toLowerCase();
    if (!['.jpg', '.jpeg', '.png', '.gif'].includes(ext)) continue;
    const name = path.basename(f, ext);
    if (name.endsWith('-headshot')) {
      headshots.set(name.replace(/-headshot$/, ''), f);
    } else {
      mains.set(name, f);
    }
  }

  const state = loadState();
  const approvedSet = new Set(state.approved);

  const items = [];
  for (const [slug, filename] of mains) {
    if (headshots.has(slug) && !approvedSet.has(slug)) {
      items.push({ slug, mainFile: filename, headshotFile: headshots.get(slug) });
    }
  }
  items.sort((a, b) => a.slug.localeCompare(b.slug));
  return { items, approvedCount: state.approved.length, totalCount: items.length + state.approved.length };
}

function serveFile(res, filePath, contentType) {
  try {
    const data = fs.readFileSync(filePath);
    res.writeHead(200, { 'Content-Type': contentType });
    res.end(data);
  } catch {
    res.writeHead(404);
    res.end('Not found');
  }
}

function buildPage() {
  const { items, approvedCount, totalCount } = getReviewList();
  const itemsJson = JSON.stringify(items);

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Headshot Review Tool</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/cropperjs/1.6.2/cropper.min.css">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #1a1a2e; color: #e0e0e0; }

  .header {
    position: sticky; top: 0; z-index: 100;
    background: #16213e; border-bottom: 1px solid #333;
    padding: 12px 24px; display: flex; align-items: center; gap: 16px;
  }
  .header h1 { font-size: 18px; font-weight: 600; }
  .progress { color: #888; font-size: 14px; }
  .progress .done { color: #4ade80; }

  .container { max-width: 900px; margin: 0 auto; padding: 20px; }

  .done-banner {
    background: #0a2e1a; border: 1px solid #166534; border-radius: 8px;
    padding: 24px; text-align: center; color: #4ade80; font-size: 18px; font-weight: 600;
  }

  .card {
    background: #16213e; border: 1px solid #333; border-radius: 8px;
    margin-bottom: 16px; overflow: hidden;
  }

  .card-header {
    padding: 12px 16px; display: flex; align-items: center; justify-content: space-between;
    background: #1a1a3e; border-bottom: 1px solid #333; cursor: pointer;
  }
  .card-header:hover { background: #222252; }
  .card-name { font-weight: 600; font-size: 15px; }
  .card-index { font-size: 13px; color: #666; }

  .card-body { display: none; padding: 16px; }
  .card.open .card-body { display: block; }

  .controls-row { display: flex; align-items: center; gap: 24px; margin-bottom: 16px; }
  .preview-row { display: flex; gap: 16px; align-items: center; }
  .preview-col { text-align: center; }
  .preview-col label { display: block; font-size: 12px; color: #888; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.05em; }

  .thumb-circle {
    width: 56px; height: 56px; border-radius: 50%; object-fit: cover;
    border: 2px solid #444; background: #222;
  }
  .thumb-circle.large { width: 100px; height: 100px; }

  .cropper-area { margin: 12px 0; }
  .cropper-area img { max-width: 100%; display: block; }

  .btn-row { display: flex; gap: 8px; align-items: center; }
  .btn {
    padding: 8px 18px; border: none; border-radius: 6px; cursor: pointer;
    font-size: 13px; font-weight: 500; transition: background 0.15s;
  }
  .btn-crop { background: #2563eb; color: #fff; }
  .btn-crop:hover { background: #1d4ed8; }
  .btn-approve { background: #16a34a; color: #fff; }
  .btn-approve:hover { background: #15803d; }
  .btn-reset { background: #444; color: #ddd; }
  .btn-reset:hover { background: #555; }

  .save-status { font-size: 12px; color: #4ade80; margin-left: 8px; }

  .bulk-actions { margin-bottom: 20px; display: flex; gap: 8px; }
  .btn-bulk { padding: 10px 20px; border: none; border-radius: 6px; cursor: pointer; font-size: 14px; font-weight: 600; }
  .btn-bulk.primary { background: #16a34a; color: #fff; }
  .btn-bulk.primary:hover { background: #15803d; }
  .btn-bulk.secondary { background: #444; color: #ddd; }
  .btn-bulk.secondary:hover { background: #555; }
</style>
</head>
<body>

<div class="header">
  <h1>Headshot Review</h1>
  <div class="progress"><span class="done" id="approvedCount">${approvedCount}</span> / <span id="totalCount">${totalCount}</span> approved &mdash; <span id="remainingCount">${items.length}</span> remaining</div>
</div>

<div class="container">
  <div class="bulk-actions">
    <button class="btn-bulk primary" onclick="approveAllRemaining()">Approve all remaining</button>
    <button class="btn-bulk secondary" onclick="expandAll()">Expand all</button>
    <button class="btn-bulk secondary" onclick="collapseAll()">Collapse all</button>
  </div>
  <div id="cards"></div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/cropperjs/1.6.2/cropper.min.js"></script>
<script>
const items = ${itemsJson};
let croppers = {};

function renderCards() {
  const container = document.getElementById('cards');
  if (items.length === 0) {
    container.innerHTML = '<div class="done-banner">All headshots reviewed!</div>';
    return;
  }
  container.innerHTML = items.map((item, i) => \`
    <div class="card \${i === 0 ? 'open' : ''}" id="card-\${item.slug}" data-index="\${i}">
      <div class="card-header" onclick="toggleCard('\${item.slug}')">
        <span class="card-name">\${item.slug.replace(/-/g, ' ')}</span>
        <span class="card-index">\${i + 1} of \${items.length}</span>
      </div>
      <div class="card-body">
        <div class="controls-row">
          <div class="preview-row">
            <div class="preview-col">
              <label>Auto-crop</label>
              <img class="thumb-circle large" id="preview-\${item.slug}"
                   src="/images/\${item.headshotFile}?t=\${Date.now()}" />
            </div>
            <div class="preview-col">
              <label>28px</label>
              <img class="thumb-circle" id="preview-sm-\${item.slug}"
                   src="/images/\${item.headshotFile}?t=\${Date.now()}" />
            </div>
          </div>
          <div class="btn-row">
            <button class="btn btn-approve" onclick="approveHeadshot('\${item.slug}')">Approve</button>
            <button class="btn btn-crop" onclick="initCropper('\${item.slug}')" id="adjust-\${item.slug}">Adjust crop</button>
            <button class="btn btn-crop" onclick="applyCrop('\${item.slug}')" id="apply-\${item.slug}" style="display:none;">Apply</button>
            <button class="btn btn-reset" onclick="resetCrop('\${item.slug}')" id="reset-\${item.slug}" style="display:none;">Cancel</button>
            <span class="save-status" id="saved-\${item.slug}" style="display:none;">Saved!</span>
          </div>
        </div>
        <div class="cropper-area" id="cropper-area-\${item.slug}">
          <img id="crop-img-\${item.slug}" src="/images/\${item.mainFile}" style="max-width:100%;" />
        </div>
      </div>
    </div>
  \`).join('');
}

function toggleCard(slug) {
  document.getElementById('card-' + slug).classList.toggle('open');
}

function initCropper(slug) {
  if (croppers[slug]) { croppers[slug].destroy(); delete croppers[slug]; }
  const img = document.getElementById('crop-img-' + slug);
  croppers[slug] = new Cropper(img, {
    aspectRatio: 1,
    viewMode: 1,
    autoCropArea: 0.6,
    movable: false,
    zoomable: false,
    rotatable: false,
    scalable: false,
  });
  document.getElementById('adjust-' + slug).style.display = 'none';
  document.getElementById('apply-' + slug).style.display = '';
  document.getElementById('reset-' + slug).style.display = '';
}

async function applyCrop(slug) {
  if (!croppers[slug]) return;
  await saveCropFromCropper(slug);
  const t = Date.now();
  const item = items.find(i => i.slug === slug);
  const hsFile = item ? item.headshotFile : slug + '-headshot.jpg';
  document.getElementById('preview-' + slug).src = '/images/' + hsFile + '?t=' + t;
  document.getElementById('preview-sm-' + slug).src = '/images/' + hsFile + '?t=' + t;
  document.getElementById('adjust-' + slug).style.display = '';
  document.getElementById('apply-' + slug).style.display = 'none';
  document.getElementById('reset-' + slug).style.display = 'none';
  showSaved(slug);
}

function resetCrop(slug) {
  if (croppers[slug]) { croppers[slug].destroy(); delete croppers[slug]; }
  document.getElementById('adjust-' + slug).style.display = '';
  document.getElementById('apply-' + slug).style.display = 'none';
  document.getElementById('reset-' + slug).style.display = 'none';
}

async function saveCropFromCropper(slug) {
  const cropper = croppers[slug];
  if (!cropper) return false;

  return new Promise((resolve) => {
    const canvas = cropper.getCroppedCanvas({ width: 150, height: 150 });
    canvas.toBlob(async (blob) => {
      const formData = new FormData();
      formData.append('image', blob, slug + '-headshot.jpg');
      formData.append('slug', slug);
      const res = await fetch('/save', { method: 'POST', body: formData });
      cropper.destroy();
      delete croppers[slug];
      resolve(res.ok);
    }, 'image/jpeg', 0.85);
  });
}

async function approveHeadshot(slug) {
  // If cropper is active, save the adjusted crop first
  if (croppers[slug]) {
    await saveCropFromCropper(slug);
  }

  // Tell server this slug is approved
  const res = await fetch('/approve', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ slug }),
  });

  if (res.ok) {
    // Remove card from DOM
    const card = document.getElementById('card-' + slug);
    card.style.transition = 'opacity 0.3s, max-height 0.3s';
    card.style.opacity = '0';
    card.style.maxHeight = card.offsetHeight + 'px';
    setTimeout(() => {
      card.style.maxHeight = '0';
      card.style.overflow = 'hidden';
      card.style.marginBottom = '0';
      card.style.padding = '0';
    }, 150);
    setTimeout(() => {
      card.remove();
      // Update counts
      const ac = document.getElementById('approvedCount');
      const rc = document.getElementById('remainingCount');
      ac.textContent = parseInt(ac.textContent) + 1;
      rc.textContent = parseInt(rc.textContent) - 1;
      // Open next card
      const remaining = document.querySelectorAll('.card');
      if (remaining.length > 0) {
        remaining[0].classList.add('open');
        remaining[0].scrollIntoView({ behavior: 'smooth', block: 'center' });
      } else {
        document.getElementById('cards').innerHTML = '<div class="done-banner">All headshots reviewed!</div>';
      }
    }, 400);

    showSaved(slug);
  }
}

async function approveAllRemaining() {
  const res = await fetch('/approve-all', { method: 'POST' });
  if (res.ok) {
    const ac = document.getElementById('approvedCount');
    const rc = document.getElementById('remainingCount');
    const tc = document.getElementById('totalCount');
    ac.textContent = tc.textContent;
    rc.textContent = '0';
    document.getElementById('cards').innerHTML = '<div class="done-banner">All headshots reviewed!</div>';
  }
}

function expandAll() { document.querySelectorAll('.card').forEach(c => c.classList.add('open')); }
function collapseAll() { document.querySelectorAll('.card').forEach(c => c.classList.remove('open')); }

function showSaved(slug) {
  const el = document.getElementById('saved-' + slug);
  if (el) { el.style.display = 'inline'; setTimeout(() => { el.style.display = 'none'; }, 2000); }
}

renderCards();
</script>
</body>
</html>`;
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'GET' && req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(buildPage());
    return;
  }

  if (req.method === 'GET' && req.url.startsWith('/images/')) {
    const filename = decodeURIComponent(req.url.replace(/\?.*$/, '').replace('/images/', ''));
    if (filename.includes('..') || filename.includes('/')) {
      res.writeHead(400); res.end('Bad path'); return;
    }
    const filePath = path.join(IMAGES_DIR, filename);
    const ext = path.extname(filename).toLowerCase();
    const types = { '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.gif': 'image/gif' };
    serveFile(res, filePath, types[ext] || 'application/octet-stream');
    return;
  }

  if (req.method === 'POST' && req.url === '/save') {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
      const body = Buffer.concat(chunks);
      const contentType = req.headers['content-type'] || '';
      const boundaryMatch = contentType.match(/boundary=(.+)/);
      if (!boundaryMatch) { res.writeHead(400); res.end('No boundary'); return; }

      const boundary = boundaryMatch[1];
      const parts = parseMultipart(body, boundary);

      const slugPart = parts.find(p => p.name === 'slug');
      const imagePart = parts.find(p => p.name === 'image');

      if (!slugPart || !imagePart) { res.writeHead(400); res.end('Missing data'); return; }

      const slug = slugPart.data.toString().trim();
      if (!/^[a-z0-9-]+$/.test(slug)) { res.writeHead(400); res.end('Bad slug'); return; }

      const outPath = path.join(IMAGES_DIR, slug + '-headshot.jpg');
      fs.writeFileSync(outPath, imagePart.data);
      console.log(`Saved crop: ${slug}-headshot.jpg`);

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    });
    return;
  }

  if (req.method === 'POST' && req.url === '/approve') {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
      try {
        const { slug } = JSON.parse(Buffer.concat(chunks).toString());
        if (!/^[a-z0-9-]+$/.test(slug)) { res.writeHead(400); res.end('Bad slug'); return; }

        const state = loadState();
        if (!state.approved.includes(slug)) {
          state.approved.push(slug);
          saveState(state);
          console.log(`Approved: ${slug} (${state.approved.length} total)`);
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
      } catch {
        res.writeHead(400); res.end('Bad JSON');
      }
    });
    return;
  }

  if (req.method === 'POST' && req.url === '/approve-all') {
    const { items } = getReviewList();
    const state = loadState();
    for (const item of items) {
      if (!state.approved.includes(item.slug)) {
        state.approved.push(item.slug);
      }
    }
    saveState(state);
    console.log(`Approved all remaining (${state.approved.length} total)`);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

function parseMultipart(body, boundary) {
  const parts = [];
  const boundaryBuf = Buffer.from('--' + boundary);

  let start = body.indexOf(boundaryBuf) + boundaryBuf.length;

  while (start < body.length) {
    if (body[start] === 0x0d && body[start + 1] === 0x0a) start += 2;

    const nextBoundary = body.indexOf(boundaryBuf, start);
    if (nextBoundary === -1) break;

    const partData = body.slice(start, nextBoundary);
    const headerEnd = partData.indexOf('\r\n\r\n');
    if (headerEnd === -1) { start = nextBoundary + boundaryBuf.length; continue; }

    const headerStr = partData.slice(0, headerEnd).toString();
    let data = partData.slice(headerEnd + 4);

    if (data.length >= 2 && data[data.length - 2] === 0x0d && data[data.length - 1] === 0x0a) {
      data = data.slice(0, data.length - 2);
    }

    const nameMatch = headerStr.match(/name="([^"]+)"/);
    if (nameMatch) {
      parts.push({ name: nameMatch[1], data, headers: headerStr });
    }

    start = nextBoundary + boundaryBuf.length;
  }

  return parts;
}

server.listen(PORT, () => {
  const { items, approvedCount, totalCount } = getReviewList();
  console.log(`\nHeadshot Review Tool running at http://localhost:${PORT}\n`);
  console.log(`${approvedCount} already approved, ${items.length} remaining out of ${totalCount} total`);
  console.log('\nWorkflow:');
  console.log('  1. First card auto-opens — check the auto-crop');
  console.log('  2. Looks good? Click "Approve" — saves & moves to next');
  console.log('  3. Need to adjust? Click "Adjust crop", drag the box, then "Approve"');
  console.log('  4. Refresh any time — approved items won\'t come back');
  console.log('  5. Ctrl+C when done\n');
});
