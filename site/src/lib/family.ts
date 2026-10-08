/**
 * Family connections between councillors, from the relatives and family_links tables
 * (build.py step 7). Nodes are councillors (people slug) or relatives (relatives key); the two
 * namespaces don't overlap (build.py checks).
 */
import Database from 'better-sqlite3';
import path from 'path';
import ELK from 'elkjs/lib/elk.bundled.js';
import { shortestPath, role } from './kin';

const db = new Database(path.join(process.cwd(), '..', 'shetland.db'), { readonly: true });

export interface FamNode {
  id: string;            // people slug or relatives key
  name: string;
  born: string | null;
  died: string | null;
  sex: 'm' | 'f';
  councillor: boolean;
  bayanne: string | null;
  service: { council: string; from: string; to: string | null }[];  // councillors only
}

export interface FamLink {
  id: number;
  a: string;
  b: string;
  kind: 'parent' | 'spouse' | 'sibling';  // parent: a is a parent of b
  onChart: boolean;
  bayanne: string | null;                 // where the link was checked; null = not yet
  note: string | null;
}

export interface Family {
  id: string;            // anchor, from the councillors' surnames
  title: string;
  nodes: FamNode[];
  links: FamLink[];
}

const COUNCIL_SHORT: Record<string, string> = {
  'lerwick-town-council': 'ltc',
  'zetland-county-council': 'zcc',
  'shetland-islands-council': 'sic',
};

function loadAll(): { nodes: Map<string, FamNode>; links: FamLink[] } {
  const rows = db.prepare(`
    SELECT l.id, l.kind, l.on_chart, l.bayanne, l.note,
           COALESCE(pa.slug, l.a_relative) AS a, COALESCE(pb.slug, l.b_relative) AS b
    FROM family_links l
    LEFT JOIN people pa ON pa.id = l.a_person_id
    LEFT JOIN people pb ON pb.id = l.b_person_id
    ORDER BY l.id
  `).all() as any[];
  const links: FamLink[] = rows.map(r => ({
    id: r.id, a: r.a, b: r.b, kind: r.kind, onChart: !!r.on_chart, bayanne: r.bayanne, note: r.note,
  }));

  const nodes = new Map<string, FamNode>();
  for (const r of db.prepare('SELECT key, name, born, died, sex, bayanne_id FROM relatives').all() as any[]) {
    nodes.set(r.key, { id: r.key, name: r.name, born: r.born, died: r.died, sex: r.sex, councillor: false,
                       bayanne: r.bayanne_id, service: [] });
  }
  const slugs = [...new Set(links.flatMap(l => [l.a, l.b]))].filter(s => !nodes.has(s));
  const person = db.prepare('SELECT id, slug, name, born_date, died_date, bayanne_id FROM people WHERE slug = ?');
  const terms = db.prepare(`
    SELECT co.slug AS council, ct.start_date AS a, ct.end_date AS b
    FROM council_terms ct JOIN councils co ON co.id = ct.council_id
    WHERE ct.person_id = ? ORDER BY co.id, ct.start_date
  `);
  for (const slug of slugs) {
    const p = person.get(slug) as any;
    const service: FamNode['service'] = [];
    for (const t of terms.all(p.id) as any[]) {
      const council = COUNCIL_SHORT[t.council] ?? t.council;
      const last = service.findLast(s => s.council === council);
      // Back-to-back terms merge into one period of service.
      if (last && last.to !== null && t.a <= last.to) {
        if (t.b === null || t.b > last.to) last.to = t.b;
      } else {
        service.push({ council, from: t.a, to: t.b });
      }
    }
    // people has no sex column; every councillor on the chart so far is a man.
    nodes.set(slug, { id: slug, name: p.name, born: p.born_date, died: p.died_date, sex: 'm', councillor: true,
                      bayanne: p.bayanne_id, service });
  }
  return { nodes, links };
}

const surname = (name: string) => name.replace(/\s*\(.*\)$/, '').split(' ').pop()!;
export const year = (d: string | null) => (d ? d.slice(0, 4) : '');

let cache: Family[] | null = null;

/** Connected families, largest number of councillors first. */
export function getFamilies(): Family[] {
  if (cache) return cache;
  const { nodes, links } = loadAll();
  const adj = new Map<string, string[]>();
  for (const l of links) {
    adj.set(l.a, [...(adj.get(l.a) ?? []), l.b]);
    adj.set(l.b, [...(adj.get(l.b) ?? []), l.a]);
  }
  const seen = new Set<string>();
  const families: Family[] = [];
  for (const start of adj.keys()) {
    if (seen.has(start)) continue;
    const ids: string[] = [];
    const queue = [start];
    seen.add(start);
    while (queue.length) {
      const n = queue.shift()!;
      ids.push(n);
      for (const m of adj.get(n) ?? []) if (!seen.has(m)) { seen.add(m); queue.push(m); }
    }
    const members = ids.map(id => nodes.get(id)!);
    const counts = new Map<string, number>();
    for (const m of members.filter(m => m.councillor)) counts.set(surname(m.name), (counts.get(surname(m.name)) ?? 0) + 1);
    const names = [...counts].sort((x, y) => y[1] - x[1] || x[0].localeCompare(y[0])).map(([s]) => s);
    const title = names.length > 3 ? `${names.slice(0, 3).join(', ')} and others` : names.join(', ').replace(/, ([^,]*)$/, ' and $1');
    const set = new Set(ids);
    families.push({
      id: names.slice(0, 2).join('-').toLowerCase(),
      title,
      nodes: members,
      links: links.filter(l => set.has(l.a)),
    });
  }
  families.sort((x, y) => y.nodes.filter(n => n.councillor).length - x.nodes.filter(n => n.councillor).length);
  const used = new Set<string>();
  for (const f of families) {
    let id = f.id, i = 2;
    while (used.has(id)) id = `${f.id}-${i++}`;
    used.add(id);
    f.id = id;
  }
  cache = families;
  return families;
}

export interface Connection {
  slug: string;
  name: string;
  steps: { role: string; name: string; slug: string | null }[];  // role of the person before, then who
  unchecked: number;
}

/** Other councillors in this councillor's family, nearest first, up to `max` links away. */
export function getConnectionsForPerson(slug: string, max = 6): Connection[] {
  const f = getFamilies().find(f => f.nodes.some(n => n.id === slug && n.councillor));
  if (!f) return [];
  const byId = new Map(f.nodes.map(n => [n.id, n]));
  const out: Connection[] = [];
  for (const other of f.nodes.filter(n => n.councillor && n.id !== slug)) {
    const path = shortestPath(f.links, slug, other.id);
    if (!path || path.length > max) continue;
    out.push({
      slug: other.id,
      name: other.name,
      steps: path.map(s => {
        const to = byId.get(s.to)!;
        return { role: role(s.from, byId.get(s.from)!.sex, s.link), name: to.name, slug: to.councillor ? to.id : null };
      }),
      unchecked: path.filter(s => !s.link.bayanne).length,
    });
  }
  return out.sort((a, b) => a.steps.length - b.steps.length || a.name.localeCompare(b.name));
}

// ---------------------------------------------------------------------------
// Family chart: ELK layered layout. Couples meet at a small union node that their children hang
// from; siblings whose parents aren't recorded hang from a bracket node.

export interface ChartEdge { d: string; kind: 'parent' | 'spouse' | 'sibling'; links: number[]; verified: boolean; }
export interface ChartLayout {
  width: number;
  height: number;
  boxes: { id: string; x: number; y: number }[];
  joins: { x: number; y: number; kind: 'spouse' | 'sibling'; links: number[] }[];
  edges: ChartEdge[];
}
export const BOX_W = 156;
export const BOX_H = 48;

export async function layoutChart(f: Family): Promise<ChartLayout> {
  const parents = new Map<string, FamLink[]>();
  for (const l of f.links.filter(l => l.kind === 'parent')) parents.set(l.b, [...(parents.get(l.b) ?? []), l]);
  const spouses = f.links.filter(l => l.kind === 'spouse');

  // Layers follow birth year (a generation is roughly 25 years), so people with no recorded
  // parents don't all pile into the top layer.
  const GEN = 25;
  const bornY = new Map(f.nodes.map(n => [n.id, n.born ? +n.born.slice(0, 4) : NaN]));
  for (const n of f.nodes) if (isNaN(bornY.get(n.id)!)) {
    // No birth year: place a generation after a parent, or alongside a spouse or sibling.
    const p = f.links.find(l => l.kind === 'parent' && l.b === n.id && !isNaN(bornY.get(l.a)!));
    const o = f.links.find(l => l.kind !== 'parent' && (l.a === n.id || l.b === n.id));
    bornY.set(n.id, p ? bornY.get(p.a)! + 30 : o ? bornY.get(o.a === n.id ? o.b : o.a) ?? 1800 : 1800);
  }
  const yOf = (year: number) => Math.round((year - 1650) / GEN) * 100;
  const elkNodes: any[] = f.nodes.map(n => ({ id: n.id, width: BOX_W, height: BOX_H, y: yOf(bornY.get(n.id)!), x: 0 }));
  const elkEdges: any[] = [];
  const meta = new Map<string, { kind: ChartEdge['kind']; links: FamLink[] }>();
  const joinMeta = new Map<string, { kind: 'spouse' | 'sibling'; links: FamLink[] }>();
  const edge = (src: string, dst: string, kind: ChartEdge['kind'], links: FamLink[]) => {
    const id = `e${elkEdges.length}`;
    elkEdges.push({ id, sources: [src], targets: [dst] });
    meta.set(id, { kind, links });
  };

  // Couples: both spouses feed a union node; children of both hang from it.
  const viaUnion = new Set<number>();
  for (const s of spouses) {
    const u = `u${s.id}`;
    const kids = [...parents.entries()]
      .filter(([, ps]) => ps.some(p => p.a === s.a) && ps.some(p => p.a === s.b))
      .map(([child, ps]) => ({ child, ps: ps.filter(p => p.a === s.a || p.a === s.b) }));
    const all = [s, ...kids.flatMap(k => k.ps)];
    elkNodes.push({ id: u, width: 8, height: 8, x: 0, y: Math.max(yOf(bornY.get(s.a)!), yOf(bornY.get(s.b)!)) + 50 });
    joinMeta.set(u, { kind: 'spouse', links: all });
    edge(s.a, u, 'spouse', [s, ...kids.flatMap(k => k.ps.filter(p => p.a === s.a))]);
    edge(s.b, u, 'spouse', [s, ...kids.flatMap(k => k.ps.filter(p => p.a === s.b))]);
    for (const k of kids) {
      edge(u, k.child, 'parent', k.ps);
      k.ps.forEach(p => viaUnion.add(p.id));
    }
  }
  for (const l of f.links.filter(l => l.kind === 'parent' && !viaUnion.has(l.id))) edge(l.a, l.b, 'parent', [l]);

  // Siblings with no shared recorded parent: group them under a bracket node.
  const shareParent = (a: string, b: string) =>
    (parents.get(a) ?? []).some(p => (parents.get(b) ?? []).some(q => q.a === p.a));
  const group = new Map<string, string>();
  const find = (x: string): string => (group.get(x) ?? x) === x ? x : find(group.get(x)!);
  const sibs = f.links.filter(l => l.kind === 'sibling' && !shareParent(l.a, l.b));
  for (const l of sibs) group.set(find(l.a), find(l.b));
  const groups = new Map<string, Set<string>>();
  for (const l of sibs) for (const n of [l.a, l.b]) {
    const g = find(n);
    groups.set(g, (groups.get(g) ?? new Set()).add(n));
  }
  for (const [g, members] of groups) {
    const s = `s-${g}`;
    const gl = sibs.filter(l => members.has(l.a));
    elkNodes.push({ id: s, width: 8, height: 8, x: 0, y: Math.min(...[...members].map(m => yOf(bornY.get(m)!))) - 50 });
    joinMeta.set(s, { kind: 'sibling', links: gl });
    for (const m of members) edge(s, m, 'sibling', gl.filter(l => l.a === m || l.b === m));
  }

  const elk = new ELK();
  const out = await elk.layout({
    id: 'root',
    layoutOptions: {
      'elk.algorithm': 'layered',
      'elk.direction': 'DOWN',
      'elk.edgeRouting': 'ORTHOGONAL',
      'elk.layered.layering.strategy': 'INTERACTIVE',
      'elk.layered.cycleBreaking.strategy': 'INTERACTIVE',
      'elk.spacing.nodeNode': '20',
      'elk.layered.spacing.nodeNodeBetweenLayers': '36',
      'elk.layered.spacing.edgeNodeBetweenLayers': '14',
      'elk.layered.nodePlacement.strategy': 'NETWORK_SIMPLEX',
      'elk.padding': '[top=16,left=16,bottom=16,right=16]',
    },
    children: elkNodes,
    edges: elkEdges,
  });

  const boxes: ChartLayout['boxes'] = [];
  const joins: ChartLayout['joins'] = [];
  for (const c of out.children ?? []) {
    const j = joinMeta.get(c.id);
    if (j) joins.push({ x: c.x! + 4, y: c.y! + 4, kind: j.kind, links: j.links.map(l => l.id) });
    else boxes.push({ id: c.id, x: c.x!, y: c.y! });
  }
  const edges: ChartEdge[] = (out.edges ?? []).map((e: any) => {
    const m = meta.get(e.id)!;
    const s = e.sections[0];
    const pts = [s.startPoint, ...(s.bendPoints ?? []), s.endPoint];
    return {
      d: 'M' + pts.map((p: any) => `${Math.round(p.x)} ${Math.round(p.y)}`).join(' L'),
      kind: m.kind,
      links: m.links.map(l => l.id),
      verified: m.links.every(l => l.bayanne),
    };
  });
  return { width: Math.ceil(out.width!), height: Math.ceil(out.height!), boxes, joins, edges };
}

// ---------------------------------------------------------------------------
// Timeline: one row per person, ordered so families sit together: spouses next to each other,
// then children by birth, then siblings, then parents.

export function timelineOrder(f: Family): string[] {
  const byId = new Map(f.nodes.map(n => [n.id, n]));
  const born = (id: string) => byId.get(id)!.born ?? '9999';
  const rel = (id: string, kind: string, dir?: 'up' | 'down') => f.links
    .filter(l => l.kind === kind && (dir === 'up' ? l.b === id : dir === 'down' ? l.a === id : l.a === id || l.b === id))
    .map(l => (l.a === id ? l.b : l.a))
    .sort((x, y) => born(x).localeCompare(born(y)));
  const order: string[] = [];
  const seen = new Set<string>();
  const visit = (id: string) => {
    if (seen.has(id)) return;
    seen.add(id);
    order.push(id);
    const couple = [id];
    for (const s of rel(id, 'spouse')) if (!seen.has(s)) { seen.add(s); order.push(s); couple.push(s); }
    for (const s of couple) for (const c of rel(s, 'parent', 'down')) visit(c);
    for (const s of couple) for (const b of rel(s, 'sibling')) visit(b);
    for (const s of couple) for (const p of rel(s, 'parent', 'up')) visit(p);
  };
  // Start from the oldest person with no recorded parents.
  const roots = f.nodes.filter(n => !f.links.some(l => l.kind === 'parent' && l.b === n.id))
    .sort((x, y) => (x.born ?? '9999').localeCompare(y.born ?? '9999'));
  for (const r of roots) visit(r.id);
  for (const n of f.nodes) visit(n.id);
  return order;
}
