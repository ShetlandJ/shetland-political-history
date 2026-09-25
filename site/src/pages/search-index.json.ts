import type { APIRoute } from 'astro';
import { getAllPeople, getAllElections, getAllConstituencies, getAllCouncils } from '../lib/db';
import Database from 'better-sqlite3';
import path from 'path';

// The search index as a separate static file, so browsers can cache it between visits
// instead of re-downloading it inside every search page.
export const GET: APIRoute = () => {
  const db = new Database(path.join(process.cwd(), '..', 'shetland.db'), { readonly: true });

  // Build search index at build time
  const base = import.meta.env.BASE_URL.replace(/\/$/, '');

  const people = getAllPeople().map(p => {
    const fullText = [p.intro, p.biography].filter(Boolean).join(' ').replace(/\[person:[^\]]+:([^\]]+)\]/g, '$1');
    return {
      type: 'person',
      name: p.name,
      slug: p.slug,
      meta: [p.born_date?.substring(0, 4), p.died_date?.substring(0, 4), p.birth_place].filter(Boolean).join(' · '),
      extra: fullText,
      url: `${base}/person/${p.slug}`,
    };
  });

  // For elections, include all candidate names as searchable text
  const elections = getAllElections();
  const councils = getAllCouncils();
  const seenPages = new Set<string>();
  const electionItems: any[] = [];
  for (const e of elections) {
    if (seenPages.has(e.wiki_page_title)) continue;
    seenPages.add(e.wiki_page_title);
    const council = councils.find(c => c.id === e.council_id);
    // Get all candidate names for this election's page
    const candidateNames = db.prepare(`
      SELECT DISTINCT c.candidate_name
      FROM candidacies c
      JOIN elections e ON c.election_id = e.id
      WHERE e.wiki_page_title = ? AND e.hidden = 0
    `).all(e.wiki_page_title).map((r: any) => r.candidate_name).join(' ');
    electionItems.push({
      type: 'election',
      name: e.wiki_page_title.replace(/_/g, ' '),
      slug: '',
      meta: [council?.name, e.election_type, e.election_date?.substring(0, 4)].filter(Boolean).join(' · '),
      extra: candidateNames,
      url: `${base}/election/${e.id}`,
    });
  }

  const constituencies = getAllConstituencies().map(c => ({
    type: 'constituency',
    name: c.name,
    slug: c.slug,
    meta: c.council_name,
    extra: '',
    url: `${base}/constituency/${c.slug}`,
  }));

  const searchIndex = [...people, ...electionItems, ...constituencies];

  return new Response(JSON.stringify(searchIndex), { headers: { 'Content-Type': 'application/json' } });
};
