# Shetland Political History

## What This Is

A static site replacing a MediaWiki installation (shetlandhistory.com) that was running MW 1.27 on PHP 7.0, getting hammered by bots at 100% resource usage on shared hosting (cPanel/Namecheap).

The project extracts Shetland political data from the MediaWiki MySQL database into a structured SQLite database, then generates a static site with Astro, deployable to Cloudflare Pages for free.

This is 17 years of research — historical accuracy matters above all else. Never invent data. If something looks wrong, verify against the wiki source.

**The DB is built, never edited.** `shetland.db` is the output of `python3 build.py`, which loads a frozen text baseline (`data/baseline.sql`) and applies the source-cited correction scripts and `data/` files on top. Any direct edit to `shetland.db` is lost on the next build, and CI (`build.py --check`) fails the deploy if the committed DB doesn't match its sources. To change data, add to a correction script (or `data/` file), run `python3 build.py`, commit both.

## Architecture

```
new-site/
├── build.py            # THE build: baseline + corrections + terms + checks → shetland.db
├── data/
│   ├── baseline.sql        # Frozen extraction (wiki parse + every script fix to 2026-09-25). Text, diffable.
│   ├── ltc_terms.csv       # LTC membership ledger — source of truth for who sat when (hand-edited)
│   ├── party_aliases.csv   # Party label spelling/markup variants
│   ├── not_seated.csv      # Elected but never took a seat (declined office, invalid 1874 group, office elections)
│   └── council_size.csv    # Researched exceptions to LTC's 12 seats (empty for now)
├── fix_minute_book.py  # Correction script: LTC minute book (run by build.py)
├── fix_newspapers.py   # Correction script: newspaper evidence (run by build.py)
├── fix_sic_by_elections.py  # Correction script: who the modern SIC by-elections replaced (run by build.py)
├── tools/generate_ltc_terms.py  # Drafting aid: cohort model → CSV draft of LTC terms (not part of the build)
├── parse_wiki.py, add_*.py, populate_*.py, ...  # Provenance: produced data/baseline.sql. Do not re-run.
├── copy_images.py      # Copies person photos + headshots from MW images dir to site
├── schema.sql          # SQLite schema definition
├── shetland.db         # Built by build.py. Committed (CI reads it). Never edit directly.
└── site/               # Astro static site
    ├── src/
    │   ├── components/
    │   │   ├── ExternalLink.astro  # External link component with icon
    │   │   └── PartyChart.astro    # Seats-by-party stacked step chart (council pages), from council_terms
    │   ├── lib/db.ts   # SQLite query layer (reads ../shetland.db at build time)
    │   ├── layouts/Base.astro  # Global layout with dark mode, sticky header, mobile nav
    │   └── pages/
    │       ├── index.astro           # Homepage with council cards and intro
    │       ├── search.astro          # Client-side search; fetches /search-index.json (cacheable)
    │       ├── search-index.json.ts  # Search index, built as a static JSON file
    │       ├── people.astro          # A-Z people listing
    │       ├── constituencies.astro  # Constituencies grouped by council
    │       ├── referenda.astro       # All 6 referenda with results
    │       ├── data-review.astro     # Data quality checks, incl. council membership checks (term_issues)
    │       ├── council-terms.astro   # "Who served when": pick council + date → members, party tally, party over time
    │       ├── council-composition.astro  # LTC election-by-election grid (from council_terms)
    │       ├── zcc-composition.astro # ZCC ward-by-election grid (from council_terms)
    │       ├── council/[slug].astro  # Election list for a council
    │       ├── election/[id].astro   # Election results with prev/next nav
    │       ├── person/[slug].astro   # Biography + career + "Served" periods (council_terms) + succession boxes + photos
    │       ├── council/[slug].astro  # Election list; party chart for LTC and SIC
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
  - **Gotcha**: LTC elections have `constituency_id = NULL` (burgh-wide). SIC generals now all have wards (populate_missing_constituencies.py), and every SIC by-election now has a ward (`fix_sic_by_elections.py` set ten from their titles).
- **candidacies** — ~2,500 individual candidacy records with votes, party, elected status. Some candidate_names contain `[url display]` external links (Bayanne) — rendered via ExternalLink component, not stripped.
- **referenda** — 6 referenda (1975 EEC, 1979 devolution, 1997 devolution x2 questions, 2011 AV, 2014 indyref, 2016 EU)
- **referendum_results** — Vote counts per option per question
- **council_terms** — one row per period of service on LTC, ZCC or SIC. Built by `build.py`: LTC from `data/ltc_terms.csv`; ZCC/SIC derived from ward results (a general replaces every seat; a by-election replaces the named member, or the only member of a single-member ward; deaths end terms). `candidacy_id` is the win that gave the seat — party comes from there. `end_date` is exclusive, NULL = still serving. Serving on X: `start_date <= X AND (end_date IS NULL OR end_date > X)`.
- **term_issues** — checks over council_terms (oversize/short council, overlapping terms, over-filled wards, unplaceable by-elections, serving after death, elected with no term). The research to-do list; shown on /data-review.

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

### Build the DB
```bash
cd /Users/james/projects/shetland_history/new-site
python3 build.py            # data/baseline.sql → corrections → party aliases → council_terms → term_issues
python3 build.py --check    # CI runs this: fails if shetland.db doesn't match its sources
cd site && npm run build    # Build static site (reads ../shetland.db directly)
```

To add a correction: put it in the relevant `fix_*.py` (idempotent, guard on the old value, cite the source in the docstring) or a new script added to `CORRECTIONS` in build.py. To change LTC membership: edit `data/ltc_terms.csv` (set `confirmed=1` and `source` when a row is confirmed from a primary source), then rebuild and check /data-review.

The old pipeline (`parse_wiki.py` → `add_modern_sic.py` → `populate_*.py` → ...) produced `data/baseline.sql` and is kept as provenance. Don't re-run it: `parse_wiki.py` deletes `shetland.db`, and that's how 274 confirmed LTC terms were lost in April 2026 (recovered from commit 38f2f89 into the ledger). `copy_images.py` is still how photos get into `site/public/images/people/`.

### Preview locally
```bash
cd site
npx astro preview
```

### Dependencies
- Python 3 standard library only for `build.py` (`mysql-connector-python` only for the retired `parse_wiki.py`)
- Node: `better-sqlite3`, `astro`
- MediaWiki images directory at `/Users/james/projects/shetland_history/images/`

## Deployment

**Cloudflare Pages** builds and serves shetlandhistory.com from master (project `shetland-political-history`, also at shetland-political-history.pages.dev). The build command is set in the Cloudflare dashboard and should be:

```
python3 build.py --check && cd site && npm ci && npm run build
```
with output directory `site/dist`. The `--check` stops a stale DB from deploying.

`.github/workflows/check.yml` runs the same check plus a site build on every push and PR. It doesn't deploy. The old GitHub Pages deploy workflow (with its `/shetland-political-history` base path and sed link rewriting) was removed on 2026-09-25: github.io only redirected to the custom domain. The GitHub Pages setting on the repo can be switched off.

The SQLite DB (`shetland.db`) must not be gitignored — it's committed and read directly by the site build via `../shetland.db`. After pushing, verify the Cloudflare build succeeded.

## LTC Composition Model

### Goal
Answer the question "who were the councillors on date X?" — now answered by `/council-terms` (any council, any date, with party) from `council_terms`. For LTC the source is the ledger `data/ltc_terms.csv`; the cohort model below survives only as `tools/generate_ltc_terms.py`, a drafting aid.

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
| Nov 1884 general | Arthur Hay stays elected=1 (topped the poll) with a declined-office note; he's in `data/not_seated.csv`, so he gets no term | His letter, 8 Nov 1884; fix_newspapers.py |
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

### Current state (as of 2026-09-25)

`data/ltc_terms.csv` holds 649 LTC terms, 263 confirmed (everything starting up to Nov 1883), each with a `source`. The 274 terms confirmed in April 2026 were recovered from commit 38f2f89 and reconciled with later corrections:
- 1826/1844 election dates, Andrew Duncan (ii) 1829, Magnus Burns 1830 → from the minute book.
- **1874**: the April "confirmed" rows held both rival groups (23 rows). Minute book p258 records the first group's election as invalid, so the ledger has only the second group's 11.
- **May 1844**: the generator had given Joseph Leask a second seat. It was an election to the office of Junior Bailie; he already sat.
- 1881 cohort ends at the 4 Nov 1884 general (Shetland Times 1 Nov 1884).

Rows after Nov 1883 are the generator's draft (`confirmed=0`). Two generator bugs were fixed while drafting: death dates were looked up by name (an unlinked 1951 "James Inkster" inherited a 1927 death), and `MANUAL_DEPARTURES` applied to every later term of the same person (William Sinclair's 1921 retirement also ended his 1929 and 1938 terms — this was the cause of the "1929–1931 shows 11" anomaly).

The open research list is `term_issues` on /data-review. For LTC: short periods (1885–89, 1895–1905, 1912, 1921, 1934–36, 1951–52, 1967 — often genuine vacancies before a by-election or general); oversize 1913–1919, 1934–1946 and 1958–1965; and 30 overlapping terms, which are where the cohort model kept someone sitting who had already left (e.g. Arthur Johnson's repeated co-options, 1941–1958).

**1912–1914 needs a decision**: the notes above say both "all 5 got full terms" (Oct 1912: council back to 12) and "council at 11 from 1912 until 1919". The ledger currently has 12 from Nov 1912 and 13 from Nov 1913. Once settled, record it in the ledger (and `data/council_size.csv` if the council really ran at 11).

**Key patterns (for editing the ledger):**
- When 5 elected and next general has only 4: all 5 got full terms (1883, 1912, 1932).
- Declined office: Laurenson 1879, Goudie 1880, Hay 1884 → listed in `data/not_seated.csv`.
- A sitting councillor winning a by-election for an office (Bailie) takes no new seat.

## Learnings and gotchas

### Data
- **Look people up by `person_id`, never by name.** Name lookups caused the James Inkster death bug and the William Sinclair departure bug (see "Current state" above). Unlinked candidacies share names with linked people.
- **Election ids are stable now** (frozen baseline), so `data/` files can safely refer to `election_id`. The ledger stores both `election_id` and `election` (wiki title), and `build.py` fails if they disagree.
- **`replaced_person` placeholders:** `[unfilled seat]` (the seat was empty at the general for lack of nominations, and the by-election filled it) and `[voided election re-run]` are markers, not people. Use the same convention for new cases (e.g. North Isles Aug 2022).
- **Ward seat counts** = winners at the last general + seats filled since via `[unfilled seat]` by-elections. Multi-member SIC wards (2007+) need `replaced_person` on every by-election, or the ward shows over-full.
- **Office elections are not seats.** A sitting councillor elected Bailie at a by-election (Joseph Leask, May 1844) takes no new term. Detected by `candidacies.role` not being NULL or `councillor`.
- **Party is per candidacy** (the label they won under). A mid-term change of party can't be represented yet; if one turns up, it needs a column on `council_terms`.
- **Party aliases are for spelling and markup only.** Don't merge genuinely different labels (Labour vs Independent Labour vs Socialist, Liberal Democrats vs Scottish Liberal Democrats): they're historical record.
- **Before trusting a `confirmed` flag, check it against later corrections.** The April 1874 rows were "confirmed" but double-counted both groups.
- **Constituency slugs aren't unique across councils** (10 pairs: `bressay`, `delting-north`, `lerwick-central`, `lerwick-north`, `lerwick-south`, `yell-south`, … — ZCC and SIC wards with the same name). `/constituency/[slug]` renders only one of each pair (the build warns "conflicts with higher priority route"), so the other ward's page is missing and links to it land on the wrong council's ward.
- **SIC by-elections are matched to the ward in their title.** `fix_sic_by_elections.py` fixed ten that had no ward or an out-of-date one. ZCC "Northmavine South County Council By-Election February 1951" is correctly on Northmavine North (David Walker's seat, which Joseph Peterson then held): the wiki title is wrong, not the data.
- **Council end dates:** terms use 1975-05-15 for LTC/ZCC abolition, but `history.astro` says the councils ran "until August 1975". Unresolved.

### Site
- **The site is served at the domain root**, so plain `/path` hrefs are fine. Some pages still prefix `import.meta.env.BASE_URL` (now always `/`); harmless.
- **Working pages** (`/data-review`, and any future review pages) pass `noindex` to `Base` and are filtered out of the sitemap in `astro.config.mjs`.
- **Party chart colours** are validated as a set, in stack order, for colour-blind separation in light and dark mode (dataviz skill validator). Labour sits at the base so its seats read directly. To add a party, re-run the validator on the new order rather than picking a colour by eye.
- **Elements created at runtime don't get Astro's scoped styles.** Style them with `:global(...)`.

### Process
- The drift check compares full dumps. After any change to `data/` or a correction script, run `python3 build.py` and commit `shetland.db` with it.
- When a check on /data-review is resolved, fix the ledger or add a correction. Don't add special cases to `build.py`'s checks to make the issue go away.

## Known Issues / TODO
- [ ] Work through the 70 `term_issues` on /data-review (planned: a private review page where James records a verdict per issue, keyed by council + kind + date + person, for Claude to turn into ledger edits)
- [ ] Decide LTC council size for 1912–1914 (see "Current state")
- [ ] SIC by-elections Sep 1993 (Jonathan Wills, Whiteness Weisdale & Tingwall) and Mar 2002 (Joseph G. Simpson, Whalsay & Skerries): wards now set from the titles, and the replaced member is inferred as the sitting member. Confirm who it was.
- [ ] Make constituency slugs unique across councils (see Learnings); needs redirects for any existing URLs that change
- [ ] Set the Cloudflare Pages build command (see Deployment) and switch off GitHub Pages
- [ ] Junk candidacy rows from parsing: `Image:cross.gif` (Lerwick Twageos 1988 by-election), `Unknown` (1722 election, with "James Moodie" in the party column). Fix with a correction script.
- [ ] 420 candidacies unlinked — mostly SIC candidates without person pages (deliberate)
- [ ] 32 person photos missing from MW images directory
- [ ] 11 people with zero candidacies are pre-1707 politicians whose elections aren't in the dataset
- [ ] Some people missing birth/death places — genuinely absent from wiki source, not a parsing bug
- Parser fixes (e.g. the old Westminster party-name issue, now fixed) can no longer go in `parse_wiki.py`: they're corrections on top of the baseline.
