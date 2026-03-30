import Database from 'better-sqlite3';
import path from 'path';

const dbPath = path.join(process.cwd(), 'shetland.db');
const db = new Database(dbPath, { readonly: true });

export interface Council {
  id: number;
  name: string;
  slug: string;
  level: string;
}

export interface Constituency {
  id: number;
  council_id: number;
  name: string;
  slug: string;
  description: string | null;
  wiki_page_title: string | null;
}

export interface Person {
  id: number;
  name: string;
  slug: string;
  born_date: string | null;
  died_date: string | null;
  birth_place: string | null;
  death_place: string | null;
  intro: string | null;
  biography: string | null;
  image_ref: string | null;
  headshot_ref: string | null;
  bayanne_id: string | null;
  wiki_page_title: string | null;
  categories: string | null;
  born_in_shetland: number | null;
  died_in_shetland: number | null;
}

export interface Election {
  id: number;
  council_id: number;
  constituency_id: number | null;
  election_date: string | null;
  election_type: string;
  electorate: number | null;
  turnout: number | null;
  turnout_pct: number | null;
  notes: string | null;
  wiki_page_title: string;
}

export interface Candidacy {
  id: number;
  election_id: number;
  person_id: number | null;
  candidate_name: string;
  party: string | null;
  votes: number | null;
  votes_text: string | null;
  elected: number;
  position: number | null;
  role: string | null;
}

// Councils
export function getAllCouncils(): Council[] {
  return db.prepare('SELECT * FROM councils ORDER BY id').all() as Council[];
}

export function getCouncilBySlug(slug: string): Council | undefined {
  return db.prepare('SELECT * FROM councils WHERE slug = ?').get(slug) as Council | undefined;
}

// Elections
export function getElectionsForCouncil(councilId: number): (Election & { constituency_name?: string })[] {
  return db.prepare(`
    SELECT e.*, con.name as constituency_name
    FROM elections e
    LEFT JOIN constituencies con ON e.constituency_id = con.id
    WHERE e.council_id = ? AND e.hidden = 0
    ORDER BY e.election_date, con.name
  `).all(councilId) as any[];
}

export function getElectionById(id: number): (Election & { council_name: string; council_slug: string; constituency_name?: string }) | undefined {
  return db.prepare(`
    SELECT e.*, co.name as council_name, co.slug as council_slug, con.name as constituency_name
    FROM elections e
    JOIN councils co ON e.council_id = co.id
    LEFT JOIN constituencies con ON e.constituency_id = con.id
    WHERE e.id = ?
  `).get(id) as any;
}

export function getAllElections(): Election[] {
  return db.prepare('SELECT * FROM elections WHERE hidden = 0 ORDER BY election_date').all() as Election[];
}

// Group elections by wiki_page_title (for multi-constituency elections shown on one page)
export function getElectionsByPage(wikiPageTitle: string): (Election & { constituency_name?: string; constituency_slug?: string; constituency_display_name?: string })[] {
  return db.prepare(`
    SELECT e.*, con.name as constituency_name, con.slug as constituency_slug, e.constituency_display_name
    FROM elections e
    LEFT JOIN constituencies con ON e.constituency_id = con.id
    WHERE e.wiki_page_title = ? AND e.hidden = 0
    ORDER BY COALESCE(e.constituency_display_name, con.name)
  `).all(wikiPageTitle) as any[];
}

// Candidacies
export function getCandidaciesForElection(electionId: number): (Candidacy & { person_slug?: string })[] {
  return db.prepare(`
    SELECT c.*, p.slug as person_slug
    FROM candidacies c
    LEFT JOIN people p ON c.person_id = p.id
    WHERE c.election_id = ?
    ORDER BY c.position
  `).all(electionId) as any[];
}

// People
export function getAllPeople(): Person[] {
  return db.prepare('SELECT * FROM people ORDER BY name').all() as Person[];
}

export function getPersonBySlug(slug: string): Person | undefined {
  return db.prepare('SELECT * FROM people WHERE slug = ?').get(slug) as Person | undefined;
}

export function getCandidaciesForPerson(personId: number): (Candidacy & {
  election_date: string;
  election_type: string;
  wiki_page_title: string;
  council_name: string;
  council_slug: string;
  constituency_name: string | null;
  election_id: number;
})[] {
  return db.prepare(`
    SELECT c.*, e.election_date, e.election_type, e.wiki_page_title,
           co.name as council_name, co.slug as council_slug,
           con.name as constituency_name
    FROM candidacies c
    JOIN elections e ON c.election_id = e.id
    JOIN councils co ON e.council_id = co.id
    LEFT JOIN constituencies con ON e.constituency_id = con.id
    WHERE c.person_id = ? AND e.hidden = 0
    ORDER BY e.election_date
  `).all(personId) as any[];
}

// Constituencies
export function getAllConstituencies(): (Constituency & { council_name: string; council_slug: string })[] {
  return db.prepare(`
    SELECT c.*, co.name as council_name, co.slug as council_slug
    FROM constituencies c
    JOIN councils co ON c.council_id = co.id
    ORDER BY co.name, c.name
  `).all() as any[];
}

export function getConstituencyBySlug(slug: string): (Constituency & { council_name: string; council_slug: string }) | undefined {
  return db.prepare(`
    SELECT c.*, co.name as council_name, co.slug as council_slug
    FROM constituencies c
    JOIN councils co ON c.council_id = co.id
    WHERE c.slug = ?
  `).get(slug) as any;
}

export function getElectionsForConstituency(constituencyId: number): Election[] {
  return db.prepare(`
    SELECT e.*
    FROM elections e
    WHERE e.constituency_id = ? AND e.hidden = 0
    ORDER BY e.election_date
  `).all(constituencyId) as Election[];
}

// Distinct election pages for a council (grouped by wiki_page_title)
export function getDistinctElectionPages(councilId: number): { wiki_page_title: string; election_date: string; election_type: string; min_id: number }[] {
  return db.prepare(`
    SELECT wiki_page_title, MIN(election_date) as election_date, election_type, MIN(id) as min_id
    FROM elections
    WHERE council_id = ? AND hidden = 0
    GROUP BY wiki_page_title
    ORDER BY MIN(election_date)
  `).all(councilId) as any[];
}

interface ElectionNavEntry {
  wiki_page_title: string;
  election_date: string;
  election_type: string;
  min_id: number;
}

interface ElectionNav {
  prev: ElectionNavEntry | null;
  next: ElectionNavEntry | null;
  prevGeneral: ElectionNavEntry | null;
  nextGeneral: ElectionNavEntry | null;
}

export function getElectionNavigation(councilId: number, currentPageTitle: string, electionType: string): ElectionNav {
  // Get all elections for this council, ordered chronologically
  const allPages = db.prepare(`
    SELECT wiki_page_title, MIN(election_date) as election_date, election_type, MIN(id) as min_id
    FROM elections
    WHERE council_id = ? AND hidden = 0
    GROUP BY wiki_page_title
    ORDER BY MIN(election_date)
  `).all(councilId) as ElectionNavEntry[];

  const idx = allPages.findIndex(p => p.wiki_page_title === currentPageTitle);

  // Same-type prev/next (by-elections navigate between by-elections, generals between generals)
  const sameType = allPages.filter(p => p.election_type === electionType);
  const sameIdx = sameType.findIndex(p => p.wiki_page_title === currentPageTitle);

  const prev = sameIdx > 0 ? sameType[sameIdx - 1] : null;
  const next = sameIdx < sameType.length - 1 ? sameType[sameIdx + 1] : null;

  // For by-elections: find the surrounding general elections
  // For generals: find surrounding generals (same as prev/next) — but flag if by-elections exist between
  let prevGeneral: ElectionNavEntry | null = null;
  let nextGeneral: ElectionNavEntry | null = null;

  if (electionType === 'by-election') {
    // Find nearest general before and after this election
    for (let i = idx - 1; i >= 0; i--) {
      if (allPages[i].election_type === 'general') { prevGeneral = allPages[i]; break; }
    }
    for (let i = idx + 1; i < allPages.length; i++) {
      if (allPages[i].election_type === 'general') { nextGeneral = allPages[i]; break; }
    }
  }

  return { prev, next, prevGeneral, nextGeneral };
}

export function getCouncilStats(): { name: string; slug: string; level: string; election_count: number; earliest: string | null; latest: string | null; person_count: number }[] {
  return db.prepare(`
    SELECT co.name, co.slug, co.level,
           COUNT(DISTINCT e.wiki_page_title) as election_count,
           MIN(e.election_date) as earliest,
           MAX(e.election_date) as latest,
           (SELECT COUNT(DISTINCT c.person_id) FROM candidacies c JOIN elections e2 ON c.election_id = e2.id WHERE e2.council_id = co.id AND e2.hidden = 0 AND c.person_id IS NOT NULL) as person_count
    FROM councils co
    LEFT JOIN elections e ON e.council_id = co.id AND e.hidden = 0
    GROUP BY co.id
    ORDER BY co.id
  `).all() as any[];
}

export function getElectionCount(): number {
  return (db.prepare('SELECT COUNT(DISTINCT wiki_page_title) as c FROM elections WHERE hidden = 0').get() as any).c;
}

export function getPersonCount(): number {
  return (db.prepare('SELECT COUNT(*) as c FROM people').get() as any).c;
}

export function getCandidacyCount(): number {
  return (db.prepare('SELECT COUNT(*) as c FROM candidacies WHERE election_id IN (SELECT id FROM elections WHERE hidden = 0)').get() as any).c;
}

// Referenda
// Leadership roles
export interface LeadershipRole {
  id: number;
  council_id: number;
  person_id: number | null;
  person_name: string;
  role: string;
  start_year: string | null;
  end_year: string | null;
}

export function getLeadershipRoles(councilId?: number): (LeadershipRole & { council_name: string; council_slug: string; person_slug?: string })[] {
  const where = councilId ? 'WHERE lr.council_id = ?' : '';
  const params = councilId ? [councilId] : [];
  return db.prepare(`
    SELECT lr.*, co.name as council_name, co.slug as council_slug, p.slug as person_slug
    FROM leadership_roles lr
    JOIN councils co ON lr.council_id = co.id
    LEFT JOIN people p ON lr.person_id = p.id
    ${where}
    ORDER BY co.id, lr.start_year
  `).all(...params) as any[];
}

export function getLeadershipRolesForPerson(personId: number): (LeadershipRole & { council_name: string; council_slug: string })[] {
  return db.prepare(`
    SELECT lr.*, co.name as council_name, co.slug as council_slug
    FROM leadership_roles lr
    JOIN councils co ON lr.council_id = co.id
    WHERE lr.person_id = ?
    ORDER BY lr.start_year
  `).all(personId) as any[];
}

export interface Referendum {
  id: number;
  title: string;
  slug: string;
  date: string | null;
  question: string | null;
  description: string | null;
  turnout_pct: number | null;
  wiki_page_title: string | null;
}

export interface ReferendumResult {
  id: number;
  referendum_id: number;
  question_label: string | null;
  option_name: string;
  votes: number | null;
  percentage: number | null;
  won: number;
}

export function getAllReferenda(): Referendum[] {
  return db.prepare('SELECT * FROM referenda ORDER BY date').all() as Referendum[];
}

export function getReferendumBySlug(slug: string): Referendum | undefined {
  return db.prepare('SELECT * FROM referenda WHERE slug = ?').get(slug) as Referendum | undefined;
}

export function getReferendumResults(referendumId: number): ReferendumResult[] {
  return db.prepare('SELECT * FROM referendum_results WHERE referendum_id = ? ORDER BY question_label, won DESC').all(referendumId) as ReferendumResult[];
}

export interface Tenure {
  constituency_name: string;
  constituency_slug: string;
  council_name: string;
  council_slug: string;
  start_year: string;
  end_year: string;
  predecessor_name: string | null;
  predecessor_slug: string | null;
  predecessor_image: string | null;
  successor_name: string | null;
  successor_slug: string | null;
  successor_image: string | null;
}

/**
 * For a given person, compute their tenures in each constituency,
 * along with who preceded and succeeded them.
 */
export function getTenuresForPerson(personId: number): Tenure[] {
  // Get all constituencies this person won elections in
  const constituencies = db.prepare(`
    SELECT DISTINCT e.constituency_id, con.name as constituency_name, con.slug as constituency_slug,
           co.name as council_name, co.slug as council_slug
    FROM candidacies c
    JOIN elections e ON c.election_id = e.id
    JOIN councils co ON e.council_id = co.id
    LEFT JOIN constituencies con ON e.constituency_id = con.id
    WHERE c.person_id = ? AND c.elected = 1 AND e.constituency_id IS NOT NULL AND e.hidden = 0
    GROUP BY e.constituency_id
  `).all(personId) as any[];

  const tenures: Tenure[] = [];

  for (const con of constituencies) {
    if (!con.constituency_id) continue;

    // Get all winners for this constituency in date order
    const winners = db.prepare(`
      SELECT c.person_id, p.name as person_name, p.slug as person_slug,
             e.election_date, e.election_type
      FROM candidacies c
      JOIN elections e ON c.election_id = e.id
      LEFT JOIN people p ON c.person_id = p.id
      WHERE e.constituency_id = ? AND c.elected = 1 AND e.hidden = 0
      ORDER BY e.election_date
    `).all(con.constituency_id) as any[];

    // Find all consecutive stints (split when someone else held the seat in between)
    const stints: { firstIdx: number; lastIdx: number }[] = [];
    let stintFirst = -1;
    let stintLast = -1;

    for (let i = 0; i < winners.length; i++) {
      if (winners[i].person_id === personId) {
        if (stintFirst === -1) {
          stintFirst = i;
        }
        stintLast = i;
      } else if (stintFirst !== -1) {
        // Someone else won — end the current stint
        stints.push({ firstIdx: stintFirst, lastIdx: stintLast });
        stintFirst = -1;
        stintLast = -1;
      }
    }
    if (stintFirst !== -1) {
      stints.push({ firstIdx: stintFirst, lastIdx: stintLast });
    }

    for (const stint of stints) {
      const startYear = winners[stint.firstIdx].election_date?.substring(0, 4) || '?';
      let endYear: string;
      if (stint.lastIdx < winners.length - 1) {
        endYear = winners[stint.lastIdx + 1].election_date?.substring(0, 4) || '?';
      } else {
        endYear = winners[stint.lastIdx].election_date?.substring(0, 4) || '?';
      }

      let predecessor_name: string | null = null;
      let predecessor_slug: string | null = null;
      for (let i = stint.firstIdx - 1; i >= 0; i--) {
        if (winners[i].person_id !== personId) {
          predecessor_name = winners[i].person_name || winners[i].candidate_name;
          predecessor_slug = winners[i].person_slug;
          break;
        }
      }

      let successor_name: string | null = null;
      let successor_slug: string | null = null;
      for (let i = stint.lastIdx + 1; i < winners.length; i++) {
        if (winners[i].person_id !== personId) {
          successor_name = winners[i].person_name || winners[i].candidate_name;
          successor_slug = winners[i].person_slug;
          break;
        }
      }

      tenures.push({
        constituency_name: con.constituency_name,
        constituency_slug: con.constituency_slug,
        council_name: con.council_name,
        council_slug: con.council_slug,
        start_year: startYear,
        end_year: endYear,
        predecessor_name,
        predecessor_slug,
        successor_name,
        successor_slug,
      });
    }
  }

  tenures.sort((a, b) => (a.start_year || '').localeCompare(b.start_year || ''));
  return tenures;
}

/**
 * Get leadership succession for a person — prev/next in their leadership role(s),
 * plus the full list of all leaders for that council.
 */
export interface LeadershipSuccession {
  role: string;
  council_name: string;
  council_slug: string;
  start_year: string;
  end_year: string;
  predecessor: { name: string; slug: string | null; start_year: string; end_year: string } | null;
  successor: { name: string; slug: string | null; start_year: string; end_year: string } | null;
  allLeaders: { name: string; slug: string | null; start_year: string; end_year: string; isCurrent: boolean }[];
}

export function getLeadershipSuccessionForPerson(personId: number): LeadershipSuccession[] {
  // Get this person's leadership roles
  const personRoles = db.prepare(`
    SELECT lr.*, co.name as council_name, co.slug as council_slug
    FROM leadership_roles lr
    JOIN councils co ON lr.council_id = co.id
    WHERE lr.person_id = ?
    ORDER BY lr.start_year
  `).all(personId) as any[];

  const results: LeadershipSuccession[] = [];

  for (const role of personRoles) {
    // Get all leaders with same role in same council
    const allLeaders = db.prepare(`
      SELECT lr.person_name, lr.person_id, lr.start_year, lr.end_year, p.slug as person_slug
      FROM leadership_roles lr
      LEFT JOIN people p ON lr.person_id = p.id
      WHERE lr.council_id = ? AND lr.role = ?
      ORDER BY lr.start_year
    `).all(role.council_id, role.role) as any[];

    const idx = allLeaders.findIndex((l: any) => l.person_id === personId && l.start_year === role.start_year);

    const prev = idx > 0 ? allLeaders[idx - 1] : null;
    const next = idx < allLeaders.length - 1 ? allLeaders[idx + 1] : null;

    results.push({
      role: role.role,
      council_name: role.council_name,
      council_slug: role.council_slug,
      start_year: role.start_year,
      end_year: role.end_year,
      predecessor: prev ? { name: prev.person_name, slug: prev.person_slug, start_year: prev.start_year, end_year: prev.end_year } : null,
      successor: next ? { name: next.person_name, slug: next.person_slug, start_year: next.start_year, end_year: next.end_year } : null,
      allLeaders: allLeaders.map((l: any) => ({
        name: l.person_name,
        slug: l.person_slug,
        start_year: l.start_year,
        end_year: l.end_year,
        isCurrent: l.person_id === personId && l.start_year === role.start_year,
      })),
    });
  }

  return results;
}

/**
 * Get all elected members for a constituency, in date order.
 */
export function getConstituencyMembers(constituencyId: number): { name: string; slug: string | null; start_year: string; end_year: string }[] {
  const winners = db.prepare(`
    SELECT c.person_id, p.name as person_name, p.slug as person_slug,
           e.election_date, e.election_type
    FROM candidacies c
    JOIN elections e ON c.election_id = e.id
    LEFT JOIN people p ON c.person_id = p.id
    WHERE e.constituency_id = ? AND c.elected = 1 AND e.hidden = 0
    ORDER BY e.election_date
  `).all(constituencyId) as any[];

  // Collapse consecutive wins by same person into a single entry
  const members: { name: string; slug: string | null; start_year: string; end_year: string }[] = [];
  let current: any = null;

  for (let i = 0; i < winners.length; i++) {
    const w = winners[i];
    const year = w.election_date?.substring(0, 4) || '?';

    if (current && current.person_id === w.person_id && w.person_id != null) {
      // Extend the current entry
      current.end_year = year;
    } else {
      if (current) {
        members.push({ name: current.name, slug: current.slug, start_year: current.start_year, end_year: current.end_year });
      }
      current = { person_id: w.person_id, name: w.person_name || 'Unknown', slug: w.person_slug, start_year: year, end_year: year };
    }
  }
  if (current) {
    members.push({ name: current.name, slug: current.slug, start_year: current.start_year, end_year: current.end_year });
  }

  return members;
}

/**
 * Get constituency ID by slug.
 */
export function getConstituencyIdBySlug(slug: string): number | null {
  const row = db.prepare('SELECT id FROM constituencies WHERE slug = ?').get(slug) as any;
  return row?.id ?? null;
}
