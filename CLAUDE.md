# Shetland Political History

## What This Is

A static site replacing a MediaWiki installation (shetlandhistory.com) that was running MW 1.27 on PHP 7.0, getting hammered by bots at 100% resource usage on shared hosting (cPanel/Namecheap).

The project extracts Shetland political data from the MediaWiki MySQL database into a structured SQLite database, then generates a static site with Astro, deployable to Cloudflare Pages for free.

This is 17 years of research — historical accuracy matters above all else. Never invent data. If something looks wrong, verify against the wiki source.

## Architecture

```
new-site/
├── parse_wiki.py       # Python script: reads MediaWiki MySQL → writes SQLite
├── add_modern_sic.py   # Supplements parser with 2017/2022 SIC elections + by-elections (not in wiki)
├── copy_images.py      # Copies person photos + headshots from MW images dir to site
├── schema.sql          # SQLite schema definition
├── shetland.db         # Generated SQLite database (output of parse_wiki.py)
└── site/               # Astro static site
    ├── shetland.db     # Copy of DB used at build time (must be copied before build)
    ├── src/
    │   ├── components/
    │   │   └── ExternalLink.astro  # External link component with icon
    │   ├── lib/db.ts   # SQLite query layer (read-only, used at build time)
    │   ├── layouts/Base.astro  # Global layout with dark mode, sticky header, mobile nav
    │   └── pages/
    │       ├── index.astro           # Homepage with council cards and intro
    │       ├── search.astro          # Client-side search (index baked in at build)
    │       ├── people.astro          # A-Z people listing
    │       ├── constituencies.astro  # Constituencies grouped by council
    │       ├── referenda.astro       # All 6 referenda with results
    │       ├── anomalies.astro       # Data quality checks
    │       ├── council/[slug].astro  # Election list for a council
    │       ├── election/[id].astro   # Election results with prev/next nav
    │       ├── person/[slug].astro   # Biography + dynamic career + succession boxes + photos
    │       ├── constituency/[slug].astro  # Historical representatives
    │       └── referendum/[slug].astro    # Individual referendum detail
    ├── public/images/people/   # Person photos + headshot thumbnails
    └── dist/                   # Built static output (~1004 pages)
```

## Data Source

Three MediaWiki MySQL dumps exist locally. We use **shetland_history2** (prefix `mwfn_`) — it has the most pages (2,067) and most up-to-date content. DB1 (`mwni_`) is used as reference for Bayanne ID corrections.

| DB | Prefix | Pages | Revisions | Notes |
|---|---|---|---|---|
| shetland_history | mwni_ | 1,713 | 7,370 | Full edit history, older content. Bayanne IDs more reliable. |
| **shetland_history2** | **mwfn_** | **2,067** | **3,507** | **Active — newest content** |
| shetland_history3 | mw4x_ | 1,509 | 1,510 | Oldest, minimal |

## Database Schema

- **councils** — Lerwick Town Council, Zetland County Council, SIC, Parliament of GB, Parliament of UK
- **constituencies** — 64+ electoral wards/divisions (includes new 2017/2022 SIC wards)
- **people** — 535 councillors/politicians with intro, biography, birth/death dates+places, image_ref, headshot_ref, Bayanne ID
- **elections** — ~1,300 rows (one per constituency result; multi-constituency elections create multiple rows sharing a wiki_page_title). Has `hidden` column for erroneous records. `constituency_display_name` stores historical names that differ from current (e.g. "Walls North" → now "Sandness"). `electorate_detail` stores breakdown like "107 men, 19 women". `replaced_person`/`replaced_person_id` for by-elections.
- **candidacies** — ~2,500 individual candidacy records with votes, party, elected status. Some candidate_names contain `[url display]` external links (Bayanne) — rendered via ExternalLink component, not stripped.
- **referenda** — 6 referenda (1975 EEC, 1979 devolution, 1997 devolution x2 questions, 2011 AV, 2014 indyref, 2016 EU)
- **referendum_results** — Vote counts per option per question

Person linkage: ~86% of candidacies are linked to person records via:
1. Wiki link matching (primary)
2. MediaWiki redirect map (294 redirects, e.g. "William Jamieson Adie" → "William Adie")
3. Middle-name / abbreviation matching (e.g. "Thomas M. Y. Manson" → "Thomas Manson (ii)")
4. Name propagation (if same candidate_name is linked in 4 elections but not the 5th, propagate)
5. Dead-person unlinking (18 impossible links removed where person died before election)
6. Manual candidacy corrections (e.g. Robert Anderson 1956 → Robert Anderson (i))
7. Manual person data corrections (e.g. David Harbison birth/death dates from DB1)

## Parser Pipeline (parse_wiki.py)

Steps in order:
1. Create councils
2. Extract elections from navigation templates (canonical election list)
3. Create constituencies from category pages
4. Import people (intro, biography, birth/death dates+places, image_ref, Bayanne ID)
4a2. Extract headshots from succession templates on OTHER people's pages
4b. Build redirect map (294 MediaWiki redirects)
4c. Fix 34 verified Bayanne ID corrections (cross-referenced against live Bayanne site 2026-03-26)
5. Import elections and candidacies
6. Validate person-candidacy links (unlink dead/unborn/underage)
6a. Manual candidacy corrections (wiki_page_title + candidate_name → correct person)
6a2. Manual person data corrections (birth/death dates from other sources)
6b. Hide erroneous elections (e.g. fake 1844 by-election)
6c. Middle-name / abbreviation matching
6d. Propagate person_id by exact candidate name
7. Import referenda (6 referendum pages with result tables)

### How elections are discovered
The parser uses **MediaWiki navigation templates** as the canonical list of elections (ElectionResultsLTC, ElectionResultsZCC, ElectionResultsSIC, ElectionResultsGB, ElectionResultsUK, ElectionResults). Only pages referenced in these templates are imported.

### Key parsing patterns
- Election tables: `{| class="wikitable"` blocks with candidate rows
- Electorate/turnout: `Electorate: 3861<br />\nTurnout: 1696 (43.9%)`
- Person birth/death/places: `(b. 10 November [[1775]], Lerwick, d. 17 February [[1841]], Lerwick)` — handles year-only, wiki-linked years, mixed formats
- Intro vs Biography: split on `==Biography==` heading; intro has `(b. ..., d. ...)` parenthetical stripped since it's shown structured
- Images: `image_ref` from body content only (NOT from `{{ }}` templates — those are other people's headshots)
- Headshots: extracted from succession templates — `[[Person Link]]..[[File:Headshot.png]]` within `{{ }}` blocks. Requires protecting wiki link pipes from cell splitting.
- Bayanne IDs: extracted from `personID=I\d+` in external links
- Referenda: parsed from pages in `[[Category:Referenda]]`, supports multi-question (1997 had 2 questions)
- Notes with by-election references: linkified with fuzzy matching on place name + year
- Constituency display names: `===[[Sandness (Constituency)|Walls North]]===` → stored in `constituency_display_name`, shown as heading with "now Sandness" note
- Electorate detail: `Electorate: 126 (107 men, 19 women)` → `electorate_detail` column
- Disambiguation notices stripped from intros: "For other people with the same name, see X." and `__NOTOC__`
- External links in candidate names preserved as `[url display]` for rendering via ExternalLink component

### Image handling
- **Main photos** (`image_ref`): extracted from page body only, ignoring `{{ }}` templates. Stored as `{slug}.{ext}` in `public/images/people/`.
- **Headshots** (`headshot_ref`): extracted from succession templates on other people's pages. Stored as `{slug}-headshot.{ext}`. Used in succession boxes.
- `copy_images.py` handles the MediaWiki hashed directory lookup (`images/a/ab/Filename.ext` via MD5).
- 32 main images are missing from the MW images directory (different filenames or not extracted).

## Modern Elections (add_modern_sic.py)

SIC elections from 2017+ and by-elections from 2019+ are NOT in the wiki database. They are added by `add_modern_sic.py` which inserts directly into SQLite. Data sourced from Wikipedia and official SIC results pages.

- SIC Election May 2017 (7 wards)
- Shetland Central By-Election November 2019
- Lerwick South By-Election November 2019
- SIC Election May 2022 (7 wards, 2 uncontested)
- North Isles By-Election August 2022
- Shetland West By-Election November 2022
- Shetland North By-Election January 2025

## Frontend Features

- **Dark mode** via `prefers-color-scheme`
- **Sticky header** with mobile hamburger menu
- **Succession boxes** on person pages showing predecessor/successor with headshot thumbnails. Same-council tenures stack without repeating the header.
- **Election navigation** — prev/next within same type; by-election pages also show links to surrounding general elections; labels show "(by)" for by-elections
- **Constituency historical names** — election headings show the name used at the time (e.g. "Walls North") with "now Sandness" note, linking to the current constituency page
- **Client-side search** — full-text: includes person bios, candidate names on election pages, and full names from intros. Shows snippets for body-text matches.
- **Referenda** section with multi-question support
- **Anomalies page** for data quality review
- **Notes linkification** — "see X By-Election Y" in election notes becomes clickable (with fuzzy matching)
- **External links** via `ExternalLink.astro` component
- Fonts: Libre Baskerville (headings) + Source Sans 3 (body)

## Commands

### Full rebuild from scratch
```bash
cd /Users/james/projects/shetland_history/new-site
python3 parse_wiki.py           # Parse wiki → SQLite
python3 add_modern_sic.py       # Add 2017+ SIC elections
python3 copy_images.py          # Copy photos from MW images dir
cp shetland.db site/shetland.db # Copy DB to site
cd site && npm run build         # Build static site
```

### Preview locally
```bash
cd site
npx astro preview
```

### Dependencies
- Python: `mysql-connector-python`
- Node: `better-sqlite3`, `astro`
- Local MySQL with shetland_history2 database imported
- MediaWiki images directory at `/Users/james/projects/shetland_history/images/`

## Deployment

Deployed to GitHub Pages. The site deploys automatically on push to master. The SQLite DB (`shetland.db`) must not be gitignored — it's committed to the repo and copied to `site/shetland.db` before build. After pushing, verify the build succeeded.

## LTC Composition Model

### Goal
Answer the question "who were the councillors on date X?" via the council composition page (`council-composition.astro`). The current approach uses a cohort-based model to simulate composition from election results, but this is being replaced with a `council_terms` table of confirmed service periods.

### Council structure
- **Pre-1876**: Triennial elections, full council replacement (all 11-12 members elected at once)
- **1874**: Exceptional dual election — disputed procedures, two votes in two rooms. Second group prevailed. 11 councillors.
- **1876 reform**: New system — 12 members, 3 cohorts of 4, one cohort rotates annually
- **Post-1876**: 4 vacancies per year at general election, unless extra vacancies from by-election re-standings or mid-term departures

### Key rules discovered from newspaper research
- **By-election rule**: Co-opted/by-elected members must re-stand at the next general election, creating an extra vacancy. They then get a fresh 3-year term starting from that general.
- **Declining office**: A nominated candidate could decline to accept office (Arthur Laurenson 1879, James Goudie 1880, Arthur Hay 1884). The vacancy carries forward to the next general.
- **Short-term fills**: When a general has extra vacancies, the person filling the vacancy gets a term aligned to the original cohort cycle, NOT a fresh 3-year term (e.g. Tulloch 1881 filling the 1879-cohort Laurenson vacancy, re-standing 1882).
- **Co-option at council meetings**: Many "by-elections" were actually council co-options at meetings, not public polls (e.g. Duncan 1884, Robertson & Anderson 1886).

### Confirmed mid-term departures (not in wiki election data)
| Person | Date | Reason | Source |
|---|---|---|---|
| Thomas Cameron | Sept 1883 | Retired | Profile intro |
| William Duncan (i) | 12 Jul 1886 | Resigned | Profile intro |
| John Harrison (i) | Oct 1886 | Disqualified | Newspaper 23 Oct 1886 |
| James Hunter (ii) | early 1887 | Unknown (replaced by Porteous Mar 1887 by-election) | Needs research |
| Alexander Mitchell (i) | before Oct 1889 | Retired (re-elected Nov 1888, retired before next election) | Newspaper 26 Oct 1889 |
| Laurence Stove | 12 Apr 1889 | Died | Death date |
| William MacDougall | Apr 1912 | Resigned | Profile intro |

### Confirmed DB corrections
| Election | Fix | Source |
|---|---|---|
| Nov 1884 general (id=33) | Date: 1884-11-04 | Newspaper 8 Nov 1884 |
| Nov 1884 by-election (id=34) | Date: 1884-11-22 (council co-option) | Newspaper 22 Nov 1884 |
| Nov 1884 general | Arthur Hay elected=0 (declined office) | His letter, 8 Nov 1884 |
| Nov 1886 by-election (id=36) | replaced_person: William Duncan, also: John Harrison | Newspaper 23 Oct 1886 |
| 1876-1885 LTC | 4 "William Duncan" candidacies relinked from Duncan (ii) to Duncan (i) | Duncan (i) profile + Duncan (ii) was Scalloway merchant |
| Nov 1886 general | Short-term fill: Jamieson (not Stove). Stove stays in 1886 cohort. | Newspaper 6 Oct 1888 (lists Jamieson as retiring 1888) |
| Nov 1887 general | Short-term fill: Anderson (not Charles Robertson). Anderson re-stands 1888. | Newspaper 6 Oct 1888 ("one of five elected last year") + Anderson re-elected 1888 |

### Research methodology for composition anomalies
1. Query the composition model for years showing != 12 members
2. Check who's in each cohort and whether anyone has >4 or <4
3. Read person profiles for resignation/retirement mentions
4. Search newspapers (britishnewspaperarchive.co.uk) for: council meeting reports near election dates, "Municipal Election" notices listing vacancies, nomination notices, and councillor swearing-in reports
5. Cross-reference: retiring councillors listed in papers vs expiring cohort members; number of vacancies stated vs number elected
6. Key search terms: "Town Council" + year, "Municipal Election" + year, "Commissioners of Police" (LTC's other title)

### Confirmed newspaper vacancy notices
| Date | Source | Vacancies | Retiring | Extra |
|---|---|---|---|---|
| 6 Oct 1888 | Newspaper | 4 | Mitchell, Tulloch jr, Jamieson | +1 short-term re-standing from 1887 five |
| 26 Oct 1889 | Newspaper | 5 | Leisk, Halcrow, Harrison | +2: Mitchell retirement, Stove death |
| 25 Oct 1890 | Newspaper | 4-5 | John Robertson (chief mag), Charles Robertson, Robertson jr, Porteous | +1 Chief Magistrate retirement (may be same as Robertson) |
| 12 Oct 1912 | Newspaper | 5 | Stout, Laing, Smith, Loggie (by rotation) | +1: MacDougall resigned. All 5 got full terms, no short-term re-standing at 1913. |
| Nov 1913 | Newspaper | 4 | (regular rotation) | Confirms no extra vacancy — MacDougall's seat absorbed |
| 24 Oct 1914 | Newspaper | 4 | Ganson, W. Sinclair, Smith, Ratter | Normal rotation, council at 11 |
| 9 Oct 1919 | Newspaper | 7 (8 if Goodlad resigns) | Sinclair, Manson, Henderson, Ramsay, Robertson, Stout, Laing | Post-WWI: 2 by rotation + 5 ad interim |
| 18 Oct 1919 | Official notice | 7 | Laing, Ramsay (rotation); Sinclair, Henderson, Stout, Manson, Robertson (ad interim) | "unique in the history of the burgh" |
| 30 Oct 1919 | Newspaper | 7 | Same as above | "six years since a municipal contest" — 6 of 7 re-stand (not Henderson) |

### Manual cohort corrections in composition model
The redistribution heuristic (`pop()` = lowest votes) doesn't always match the council's actual short-term fill assignment. These corrections swap the wrongly-assigned and correctly-assigned members between cohorts, confirmed from newspaper vacancy notices:
- **1886 general**: Jamieson was short-term fill (not Stove). Stove served until death Apr 1889.
- **1887 general**: Anderson was short-term fill (not Charles Robertson). Anderson re-stood and won 1888.

### Current state and next steps (as of 2026-04-06)

Two composition pages exist:
- `/council-composition` — original cohort model (complex, ~350 lines of simulation logic)
- `/council-composition-v2` — reads from `council_terms` table (simple SQL queries, ~100 lines)
V2 is the future. The terms generator (`tools/generate_ltc_terms.py`) is the single place to fix anomalies. Confirmed terms (`confirmed=1`) are preserved across regenerations. 247 terms confirmed through Jan 1878.

**Period status:**
- **Pre-1876**: Correct (triennial full-replacement elections). Confirmed.
- **1877-1878**: Correct (12). Confirmed. Newspaper Oct 1878: 4 vacancies, normal rotation.
- **1879**: Confirmed at 11. Laurenson declined office. Newspaper Oct 1879: 4 retiring incl Laurenson.
- **1880**: Confirmed. General=11 (Goudie declined), by-election=12 (Duncan replaced Goudie). Newspaper Oct 1880: 4 retiring + "a vacancy to fill up" from Laurenson's unfilled seat = 5 vacancies.
- **1881-1883**: Correct (12) in v2/terms. V1 model shows 11 at 1883 (terms generator handles this better).
- **1884-1886**: V2 shows 12 at 1884-1885, 13 at 1886. Hay declined 1884, Duncan/Harrison departures 1886. By-election gap-fill issue at 1886 persists.
- **1887-1889**: V2 shows 12 (terms generator handles correctly). V1 model shows 11 (dedup issue).
- **1890-1895**: Correct (12) — fixed by Mitchell departure + cohort corrections.
- **1896-1907**: Unresearched.
- **1908-1914**: Shows 11. Confirmed correct — MacDougall resigned Apr 1912, vacancy absorbed. Council ran at 11 from 1912 until 1919 reset.
- **1915-1918 (WWI)**: By-elections only, no generals. Multiple wartime co-options.
- **1919**: Correct (12) — post-WWI reset with hardcoded cohort assignments.
- **1920-1928**: Correct (12).
- **1929-1931**: Shows 11. Unresearched — likely a mid-term departure cascading. 1930 newspaper confirms 4 normal vacancies ("personnel remains as before"), so council was genuinely 12 at that point. Model disagrees.
- **1932**: Correct (12) — Campbell (i) fix + skip-redistribute.
- **1933**: Shows 13 in v1, 13 in v2. Cascade from 1929 deficit. Newspaper confirms 4 vacancies.
- **1934**: Correct (12) — Sandison death + skip-redistribute for 1932.
- **1935+**: Unresearched. WWII era has many anomalies (13s/14s from 1937-1946).
- **1957-1958**: Unresearched 13s.

**Key patterns:**
- When 5 elected and next general has only 4: all 5 got full terms → add to `SKIP_REDISTRIBUTE` (1883, 1912, 1932).
- Declined office: Laurenson 1879, Goudie 1880, Hay 1884 → handled via `declinedOffice` set in composition model and `DECLINED_OFFICE` in terms generator.
- By-election replacing already-departed person: cap additions at council size 12.
- `council_terms` table with `confirmed` flag is the path forward — fix in one place, display reads from table.

## Known Issues / TODO
- [ ] Deploy to Cloudflare Pages
- [ ] 354 candidacies remain unlinked — mostly SIC candidates without person pages (deliberate) and party names being parsed as candidates in Westminster elections
- [ ] 32 person photos missing from MW images directory
- [ ] 11 people with zero candidacies are pre-1707 politicians whose elections aren't in the dataset
- [ ] Westminster election parser: party names (Labour, Conservative, etc.) sometimes parsed as candidate names
- [ ] Some people missing birth/death places — genuinely absent from wiki source, not a parsing bug
