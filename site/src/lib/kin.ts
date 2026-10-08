/**
 * Pure helpers for family connections, shared by the build (person pages) and the browser
 * (/connections path finder). No database access here.
 */
export interface KinLink { id: number; a: string; b: string; kind: 'parent' | 'spouse' | 'sibling'; bayanne: string | null; }
export interface Step { from: string; to: string; link: KinLink; }

/** Fewest links from one person to another, or null if they aren't connected. */
export function shortestPath(links: KinLink[], from: string, to: string): Step[] | null {
  const prev = new Map<string, Step | null>([[from, null]]);
  const queue = [from];
  while (queue.length) {
    const k = queue.shift()!;
    if (k === to) break;
    for (const l of links) {
      const n = l.a === k ? l.b : l.b === k ? l.a : null;
      if (n && !prev.has(n)) { prev.set(n, { from: k, to: n, link: l }); queue.push(n); }
    }
  }
  if (!prev.has(to) || from === to) return null;
  const steps: Step[] = [];
  for (let s = prev.get(to); s; s = prev.get(s.from)) steps.unshift(s);
  return steps;
}

/** What `cur` is to the next person along a link: "son of", "wife of", "brother of". */
export function role(cur: string, sex: 'm' | 'f', link: KinLink): string {
  const f = sex === 'f';
  if (link.kind === 'spouse') return f ? 'wife of' : 'husband of';
  if (link.kind === 'sibling') return f ? 'sister of' : 'brother of';
  return link.a === cur ? (f ? 'mother of' : 'father of') : (f ? 'daughter of' : 'son of');
}
