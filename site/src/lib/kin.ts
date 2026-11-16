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

function greats(n: number): string {
  return n <= 0 ? '' : n === 1 ? 'great-' : `${n}x great-`;
}

/** Blood relationship of someone `u` generations up and then `d` down from you, in their sex. */
function bloodTerm(u: number, d: number, f: boolean): string | null {
  if (d === 0) {
    if (u === 1) return f ? 'mother' : 'father';
    return `${greats(u - 2)}grand${f ? 'mother' : 'father'}`;
  }
  if (u === 0) {
    if (d === 1) return f ? 'daughter' : 'son';
    return `${greats(d - 2)}grand${f ? 'daughter' : 'son'}`;
  }
  if (u === 1 && d === 1) return f ? 'sister' : 'brother';
  if (d === 1) return `${greats(u - 2)}${f ? 'aunt' : 'uncle'}`;
  if (u === 1) return `${greats(d - 2)}${f ? 'niece' : 'nephew'}`;
  const degree = Math.min(u, d) - 1, removed = Math.abs(u - d);
  const ord = ['first', 'second', 'third', 'fourth', 'fifth'][degree - 1] ?? `${degree}th`;
  const rem = removed === 0 ? '' : removed === 1 ? ' once removed' : removed === 2 ? ' twice removed' : ` ${removed} times removed`;
  return `${ord} cousin${rem}`;
}

/** Generations up then down along blood links, or null if the steps aren't a simple up-then-down. */
function upDown(steps: Step[]): { u: number; d: number } | null {
  let u = 0, d = 0, prevSibling = false;
  for (const s of steps) {
    const k = s.link.kind;
    if (k === 'spouse') return null;
    if (k === 'sibling') {
      // A sibling's sibling is still a sibling: only the first of a run moves across.
      if (!prevSibling) { if (d > 0) return null; u++; d++; }
      prevSibling = true;
      continue;
    }
    prevSibling = false;
    if (s.link.b === s.from) { if (d > 0) return null; u++; }  // to a parent
    else d++;                                                   // to a child
  }
  return { u, d };
}

/**
 * What the last person on the path is to the first: "3x great-uncle", "son-in-law",
 * "wife's first cousin", "niece's husband". Null when the path is too tangled to name (marriage in the middle).
 */
export function kinTerm(steps: Step[], sexOf: (id: string) => 'm' | 'f'): string | null {
  if (!steps.length) return null;
  const last = steps[steps.length - 1];
  const f = sexOf(last.to) === 'f';
  const spouses = steps.filter(s => s.link.kind === 'spouse').length;
  if (spouses === 0) {
    const ud = upDown(steps);
    return ud && bloodTerm(ud.u, ud.d, f);
  }
  if (spouses > 1) return null;
  if (steps.length === 1) return f ? 'wife' : 'husband';
  if (steps[0].link.kind === 'spouse') {
    // Your spouse's relative.
    const ud = upDown(steps.slice(1));
    if (!ud) return null;
    if (ud.u === 1 && ud.d === 0) return f ? 'mother-in-law' : 'father-in-law';
    if (ud.u === 1 && ud.d === 1) return f ? 'sister-in-law' : 'brother-in-law';
    const t = bloodTerm(ud.u, ud.d, f);
    return t && `${sexOf(steps[0].to) === 'f' ? 'wife' : 'husband'}'s ${t}`;
  }
  if (last.link.kind === 'spouse') {
    // Spouse of your relative.
    const ud = upDown(steps.slice(0, -1));
    if (!ud) return null;
    if (ud.u === 0 && ud.d === 1) return f ? 'daughter-in-law' : 'son-in-law';
    if (ud.u === 1 && ud.d === 1) return f ? 'sister-in-law' : 'brother-in-law';
    const t = bloodTerm(ud.u, ud.d, sexOf(last.from) === 'f');
    return t && `${t}'s ${f ? 'wife' : 'husband'}`;
  }
  return null;
}
