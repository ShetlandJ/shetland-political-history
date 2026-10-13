// Old wiki URLs (shetlandhistory.com/Page_Title, as Wikipedia and other sites still link them)
// → the new pages. Built from the wiki_page_title columns in shetland.db plus the wiki's own
// redirects (data/wiki_redirects.csv, frozen from shetland_history2), and appended to
// dist/_redirects after public/_redirects' hand-written rules. Cloudflare Pages allows 2,000
// static redirects.
import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';

const LIMIT = 2000;

// MediaWiki's wfUrlencode: rawurlencode, then leave ;:@$!*(),/~ as they are.
function wikiPath(title) {
  return '/' + encodeURIComponent(title)
    .replace(/%3B/g, ';').replace(/%40/g, '@').replace(/%24/g, '$').replace(/%2C/g, ',')
    .replace(/%2F/g, '/').replace(/%3A/g, ':').replace(/'/g, '%27');
}

function readCsv(file) {
  const rows = fs.readFileSync(file, 'utf8').trim().split('\n').slice(1);
  return rows.map(line => {
    const out = []; let cur = '', q = false;
    for (const ch of line) {
      if (ch === '"') q = !q;
      else if (ch === ',' && !q) { out.push(cur); cur = ''; }
      else cur += ch;
    }
    out.push(cur);
    return out;
  });
}

export function wikiRedirects(root) {
  const db = new Database(path.join(root, 'shetland.db'), { readonly: true });
  const target = new Map();  // wiki title (underscores) → new path
  const add = (title, to) => { if (title) { const t = title.replace(/ /g, '_'); if (!target.has(t)) target.set(t, to); } };

  // Election pages are generated for the first election of each title (election/[id].astro).
  for (const e of db.prepare('SELECT id, wiki_page_title FROM elections WHERE hidden = 0 ORDER BY election_date').all())
    add(e.wiki_page_title, `/election/${e.id}/`);
  for (const p of db.prepare('SELECT slug, wiki_page_title FROM people').all()) add(p.wiki_page_title, `/person/${p.slug}/`);
  for (const c of db.prepare('SELECT slug, wiki_page_title FROM constituencies').all()) add(c.wiki_page_title, `/constituency/${c.slug}/`);
  for (const r of db.prepare('SELECT slug, wiki_page_title FROM referenda').all()) add(r.wiki_page_title, `/referendum/${r.slug}/`);
  for (const c of db.prepare('SELECT name, slug FROM councils').all()) add(c.name, `/council/${c.slug}/`);
  add('Main_Page', '/');
  add('Home', '/');

  // Disambiguation pages ("James_Hunter" for James Hunter (i), (ii), ...) → a search for the name.
  const titles = db.prepare('SELECT wiki_page_title FROM people WHERE wiki_page_title IS NOT NULL').all().map(r => r.wiki_page_title);
  for (const t of titles) {
    const m = t.match(/^(.*)_\([ivx]+\)$/);
    if (m) add(m[1], `/search/?q=${encodeURIComponent(m[1].replace(/_/g, ' '))}`);
  }
  db.close();

  // The wiki's redirects ("Alex_Tulloch" → "Alexander_Tulloch"), where the target has a page.
  for (const [from, to] of readCsv(path.join(root, 'data', 'wiki_redirects.csv')))
    if (target.has(to)) add(from, target.get(to));

  const lines = [];
  for (const [title, to] of target) {
    const encoded = wikiPath(title);
    lines.push(`${encoded} ${to} 301`);
    const plain = '/' + title;
    if (plain !== encoded && /^[\x21-\x7e]+$/.test(plain)) lines.push(`${plain} ${to} 301`);
  }
  return lines;
}

export default function wikiRedirectsIntegration() {
  return {
    name: 'wiki-redirects',
    hooks: {
      'astro:build:done': ({ dir, logger }) => {
        const root = path.resolve(process.cwd(), '..');
        const file = path.join(dir.pathname, '_redirects');
        const existing = fs.existsSync(file) ? fs.readFileSync(file, 'utf8').trimEnd().split('\n') : [];
        const taken = new Set(existing.map(l => l.split(/\s+/)[0]));
        const lines = wikiRedirects(root).filter(l => !taken.has(l.split(' ')[0]));
        const all = [...existing, '', '# Old wiki URLs (site/wiki-redirects.mjs)', ...lines];
        const count = all.filter(l => l && !l.startsWith('#')).length;
        if (count > LIMIT) throw new Error(`_redirects has ${count} rules; Cloudflare Pages allows ${LIMIT}`);
        fs.writeFileSync(file, all.join('\n') + '\n');
        logger.info(`${lines.length} old wiki URLs redirected (${count} rules in _redirects)`);
      },
    },
  };
}
