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
│   ├── not_seated.csv      # Elected but never took a seat (declined office, invalid 1874 group, office elections, ZCC double returns)
│   ├── council_size.csv    # Researched exceptions to LTC's 12 seats (empty for now)
│   ├── citations.csv       # Sources: newspaper articles/issues and minute-book pages (hand-edited)
│   ├── citation_links.csv  # What each citation supports: a ledger term, an election, a person fact
│   └── searches.csv        # Searches that found nothing
├── fix_minute_book.py  # Correction script: LTC minute book (run by build.py)
├── fix_newspapers.py   # Correction script: newspaper evidence (run by build.py)
├── fix_sic_by_elections.py  # Correction script: who the modern SIC by-elections replaced (run by build.py)
├── fix_spelling.py     # Correction script: typos in biographies and election notes (run by build.py)
├── fix_parse_errors.py # Correction script: wiki markup / parse slips left in the baseline (run by build.py)
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
- **citations** — one row per source: a newspaper article (`st-19640508-p4-a055`), a whole issue (`st-19381029`) or a minute-book page (`mb-p258`). From `data/citations.csv`; `build.py` makes the `citation` text and the BNA `url` from the parts. Backfilled 2026-09-28 from the `research/bna/` links and the ledger's `source` strings (`tools/backfill_citations.py`, provenance only).
- **citation_links** — what each citation supports: a `term_id`, an `election_id`, or a `person_id` + `field` (e.g. `died_date`). `basis` is `read`, `inferred` or empty (the backfilled links aren't reviewed yet). From `data/citation_links.csv`.
- **searches** — searches that found nothing, from `data/searches.csv`, so they aren't re-run.
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
| William Duncan (i) | 12 Jul 1886 | Resigned (letter read 12 Jul; refusal to reconsider reported 15 Oct; end date open) | Profile intro; Shetland Times 17 Jul and 16 Oct 1886 |
| John Harrison (i) | after 15 Oct 1886 | Disqualified ("will be", cause and day not found) | Shetland Times 16 Oct 1886 |
| James Hunter (ii) | 4 Jan 1887 | Resigned on moving to the Union Bank at Portsoy (Porteous elected 18 Mar 1887) | Shetland Times 11 Dec 1886 and 8 Jan 1887 |
| Alexander Mitchell (i) | Oct 1889 (probably Fri 18th) | Resigned (re-elected Nov 1888) | Shetland Times 19 and 26 Oct 1889 |
| Laurence Stove | 12 Apr 1889 | Died | Death date |
| John Irvine (iii) | 7 Dec 1909 | Resigned (Loggie appointed Tue 4 Jan 1910) | Shetland Times 11 Dec 1909 and 8 Jan 1910 |
| William MacDougall | 8 Oct 1912 | Resigned (an April 1912 resignation was withdrawn) | Shetland Times 6 Apr and 12 Oct 1912 |

### Confirmed DB corrections
| Election | Fix | Source |
|---|---|---|
| Nov 1884 general (id=33) | Date: 1884-11-04 | Newspaper 8 Nov 1884 |
| Nov 1884 by-election (id=34) | Date: 1884-11-22 (council co-option). Too late: he was elected between 11 and 14 Nov and sat on 18 Nov (ST 15 and 22 Nov 1884); day in open-questions | Newspaper 22 Nov 1884 |
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
| 12 Oct 1912 | Newspaper | 5 | Stout, J. Smith, J. Laing, Loggie (automatic; Loggie in place of Provost A. Laing, who stayed on) | +1: MacDougall resigned 8 Oct. Council 12 after the election. |
| 11 Oct 1913 | Newspaper | 4 | Provost A. Laing, Bailie Goodlad, A. Smith, W. S. Smith | Goodlad a year early; Laing did not re-stand |
| 24 Oct 1914 | Newspaper | 4 | Ganson, W. Sinclair, J. Smith, Ratter | J. Smith's 1912 seat was a two-year one. All returned unopposed. Council at 12 |
| 9 Oct 1919 | Newspaper | 7 (8 if Goodlad resigns) | Sinclair, Manson, Henderson, Ramsay, Robertson, Stout, Laing | Post-WWI: 2 by rotation + 5 ad interim |
| 18 Oct 1919 | Official notice | 7 | Laing, Ramsay (rotation); Sinclair, Henderson, Stout, Manson, Robertson (ad interim) | "unique in the history of the burgh" |
| 30 Oct 1919 | Newspaper | 7 | Same as above | "six years since a municipal contest" — 6 of 7 re-stand (not Henderson) |

### Manual cohort corrections in composition model
The redistribution heuristic (`pop()` = lowest votes) doesn't always match the council's actual short-term fill assignment. These corrections swap the wrongly-assigned and correctly-assigned members between cohorts, confirmed from newspaper vacancy notices:
- **1886 general**: Jamieson was short-term fill (not Stove). Stove served until death Apr 1889.
- **1887 general**: Anderson was short-term fill (not Charles Robertson). Anderson re-stood and won 1888.

### Current state (as of 2026-09-25)

`data/ltc_terms.csv` holds 650 LTC terms, 538 confirmed (everything starting up to Nov 1883, plus most of 1885–1923, 1929–38, 1941, 1945–54, 1955–65 and 1966–71), each with a `source`. The 274 terms confirmed in April 2026 were recovered from commit 38f2f89 and reconciled with later corrections:
- 1826/1844 election dates, Andrew Duncan (ii) 1829, Magnus Burns 1830 → from the minute book.
- **1874**: the April "confirmed" rows held both rival groups (23 rows). Minute book p258 records the first group's election as invalid, so the ledger has only the second group's 11.
- **May 1844**: the generator had given Joseph Leask a second seat. It was an election to the office of Junior Bailie; he already sat.
- 1881 cohort ends at the 4 Nov 1884 general (Shetland Times 1 Nov 1884).

Rows after Nov 1883 are the generator's draft (`confirmed=0`). Two generator bugs were fixed while drafting: death dates were looked up by name (an unlinked 1951 "James Inkster" inherited a 1927 death), and `MANUAL_DEPARTURES` applied to every later term of the same person (William Sinclair's 1921 retirement also ended his 1929 and 1938 terms — this was the cause of the "1929–1931 shows 11" anomaly).

The open research list is `term_issues` on /data-review. For LTC: short periods (1884, 1886–87 and 1889, all dated vacancies; 1961, which waits on James Daniel's unconfirmed 1962 resignation row); no oversize rows left; and no overlapping terms left (1922 settled). For ZCC: no overlaps left (Peterson's and Leslie's were parse errors, `fix_parse_errors.py` #7–8; Sinclair's 1919 double return is in `not_seated.csv`, #9); the by-election issues were cleared on 2026-09-28 (`research/bna/zcc-by-elections.md`).

**1915–1916 settled (2026-09-28)**: three dated wartime vacancies (Stout died 10 Apr 1915, Sinclair elected 27 Apr; Grierson died 3 Jul, C. B. Stout 3 Aug 1915; Laurenson died 14 Jul, Henderson 1 Aug 1916). W. S. Smith resigned 7 Nov 1916, not 5 Dec. The Shetland Times for 24 Apr–12 Jun 1915 isn't digitised; the Shetland News (`BL/0003210`) filled the gap. Evidence: `research/bna/ltc-1915-1916.md`.

**1919–1921 settled (2026-09-28)**: Goodlad, Ratter, Ganson and W. Sinclair sat on to Nov 1920 and J. Smith (i) to Nov 1921 (holdover rows confirmed). J. M. Goodlad was co-opted for Pottinger (retired, day unknown) on Tue 7 Jun 1921, not the 11th (`fix_newspapers.py` #20); Bailie W. Sinclair's resignation was read on Tue 4 Oct 1921. Evidence: `research/bna/ltc-1919-1921.md`.

**1934–1938 settled (2026-09-28)**: all dated vacancies. Johnston resigned 9 Oct 1934 and Sandison was killed 30 Sep 1934 (both had two-year 1932 seats); Cogle resigned 5 May 1936 (not July); co-options 7 Jul, 4 Aug and 10 Nov 1936. Evidence: `research/bna/ltc-1934-1938.md`.

**1936–1945 rows confirmed (2026-09-28)**: Halcrow died 24 Dec 1940 (Williamson co-opted 7 Jan 1941); Clausen resigned 4 Jan 1938 on moving to Thurso and Dalziel was co-opted Tue 1 Feb 1938 (not the 3rd); W. G. Smith died 28 Feb 1942; Laing retired from 29 Nov 1945 (Johnston co-opted 10 Dec). This cleared the Nov 1936, Oct 1937 and Jul 1938 rows. Evidence: `research/bna/ltc-1936-1945.md`.

**1940–1945 settled (2026-09-28)**: W. Sinclair resigned 24 May 1940 (seat declared vacant in June) and Mouat was co-opted Tue 2 Jul 1940 (not 6 Aug); T. A. Sinclair co-opted 31 Mar 1942 (not 9 Apr); Gear, Inkster and Prophet Smith resigned before their successors' co-options (2 Feb 1943, 3 Oct 1944, 9 Jan 1945); Morrison co-opted Tue 6 Feb 1945; the Nov 1945 general was Tue 6 Nov, not Mon 5th (`fix_newspapers.py` #19). A size-short run is hidden once every row sitting through it is confirmed, so confirming both ends of each row is what clears a genuine vacancy. Evidence: `research/bna/ltc-1940-1945.md`.

**1895–1901 settled (2026-09-28)**: Hunter had the short seat in 1895, Halcrow retired a year early in 1897 while Provost Leisk stayed on, and Kay volunteered to retire in 1900 to make up the third. Evidence: `research/bna/ltc-1895-1901.md`.

**1894–1910 settled (2026-09-28)**: the 1895, 1899, 1901 and 1905 short rows are genuine vacancies after deaths, now fully confirmed. The Nov 1901 general was Tue 5 Nov, not Fri 1st (`fix_newspapers.py` #21). Provost Goudie stayed on 1906–07: the Clerk said a Provost "had to serve a period of three years" from his election as Provost, so J. T. J. Sinclair, lowest of the 1904 winners, retired in 1906 in his place. Irvine (iii) resigned 7 Dec 1909 and Loggie was appointed Tue 4 Jan 1910 (#22). Evidence: `research/bna/ltc-1894-1907.md`.

**1905–1910 settled (2026-09-28)**: Provost Porteous stayed on from 1908 to 1910. Ganson retired in his place in 1908, and J. Smith retired a year early in 1909. Evidence: `research/bna/ltc-1908-1910.md`.

**1874–1883 sourced (2026-09-28)**: every election and the 38 April-2026 rows from 1876 now cite the Shetland Times results, nominations and retiring lists; all agreed except the Nov 1881 general, Tue 1 Nov not the 8th (`fix_newspapers.py` #25). 1877–1883 were all unopposed. The paper came out on Mondays until 15 Mar 1875. Evidence: `research/bna/ltc-1874-1883.md`.

**1885–1889 settled (2026-09-28)**: Stove's 1883 seat ran to 1886 (the confirmed row had 1885) and Jamieson's 1886 seat to 1888. Hunter (ii) resigned 4 Jan 1887; Robertson and Anderson were co-opted 19 Nov 1886 and Porteous 18 Mar 1887 (not the 24th); Mitchell resigned Oct 1889. The council met on Friday evenings then. Evidence: `research/bna/ltc-1885-1889.md`.

**1965–1970 settled (2026-09-28)**: the 1967 and 1970 short rows are genuine vacancies, now fully confirmed. Provost Nicolson resigned from 27 Apr 1967 and Halcrow was co-opted at the statutory meeting Fri 5 May 1967 (not the 12th); R. A. Anderson died Sun 25 Jun 1967 (not the 26th, `fix_newspapers.py` #24) and Cumming was co-opted Tue 8 Aug. Evidence: `research/bna/ltc-1965-1970.md`.

**1966–1973 settled (2026-09-28)**: Provost Eric Gray stayed on 1969–71 (W. A. Smith retired in his place in 1969), Halcrow's 1968 seat ended 1970, Provost W. A. Smith stayed on in 1972 (Tait and Peterson retired a year early), and Eric Gray and Butler had two-year seats from 1971. J. R. Smith resigned 14 Apr 1970 and Adair was co-opted 8 May 1970 (not 8 Aug). 1971–73 were unopposed. Evidence: `research/bna/ltc-1969-1973.md`.

**1920–1923 settled (2026-09-28)**: Duffin's 1920 seat ended in 1922 (he retired by rotation a year early, reason not stated) and A. S. Manson's 1921 seat ran to 1923. 1922 was unopposed. This cleared the 1922 overlap. Evidence: `research/bna/ltc-1920-1923.md`.

**1912–1914 settled (2026-09-27)**: the council had 12 members throughout, from the Shetland Times retiring lists. Evidence with BNA links: `research/bna/ltc-1912-1914.md`.

**1941 settled (2026-09-27)**: Irvine (ii) and Linklater resigned in Aug 1941 and were replaced by co-option on 7 Oct; this removed the wartime 13-member rows. Evidence: `research/bna/ltc-wartime-1941.md`.

**1945–1953 settled (2026-09-27)**: from the retiring lists each year. The lowest winners got short seats, Treasurer Ollason was kept on to 1951 and Provost R. A. Anderson to 1953, the Nov 1948 election was put back to May 1949, and Shearer resigned in June 1947 (Dalziel co-opted). Evidence: `research/bna/ltc-1945-1953.md`.

**1945–1956 vacancies settled (2026-09-28)**: the 1945, 1947 and 1955 short rows are genuine vacancies (Laing, Shearer, Halcrow), now fully confirmed. David Gray (ii) died Mon 4 Mar 1946; the Nov 1946 general was Tue 5 Nov, not Mon 4th (`fix_newspapers.py` #23); Brownlie resigned, his letter read at the 13 Sep 1949 meeting that co-opted Johnson. Evidence: `research/bna/ltc-1945-1956.md`.

**1955–1965 settled (2026-09-27)**: 12 throughout apart from dated vacancies. The 1957 seat the wiki gives Grace Halcrow was Andrew Nicolson's (`fix_newspapers.py` #5). Evidence: `research/bna/ltc-1955-1965.md`.

**Key patterns (for editing the ledger):**
- When 5 elected and next general has only 4: all 5 got full terms (1883, 1912, 1932).
- After uncontested elections (no votes to rank by), the council **drew lots** for the order of retirement (1958, 1959). Otherwise short seats went to the lowest-placed winners (Paton 1960). Don't assume 3-year terms after 1955: take each end date from the next retiring lists.
- Declined office: Goudie 1880, Hay 1884 → listed in `data/not_seated.csv`. Laurenson 1879 is there too, but he refused re-nomination rather than declining a seat (ST 1 Nov 1879; open question).
- A sitting councillor winning a by-election for an office (Bailie) takes no new seat.

## Learnings and gotchas

### Data
- **Look people up by `person_id`, never by name.** Name lookups caused the James Inkster death bug and the William Sinclair departure bug (see "Current state" above). Unlinked candidacies share names with linked people.
- **Election ids are stable now** (frozen baseline), so `data/` files can safely refer to `election_id`. The ledger stores both `election_id` and `election` (wiki title), and `build.py` fails if they disagree.
- **`replaced_person` placeholders:** `[unfilled seat]` (the seat was empty at the general for lack of nominations, and the by-election filled it), `[voided election re-run]` and `[double return]` (the winner of two wards sat for the other, so this seat was never taken up; the win is in `data/not_seated.csv`, e.g. William Sinclair, Burra 1919) are markers, not people. Use the same convention for new cases (e.g. North Isles Aug 2022).
- **Ward seat counts** = winners at the last general + seats filled since via `[unfilled seat]` by-elections. Multi-member SIC wards (2007+) need `replaced_person` on every by-election, or the ward shows over-full.
- **`replaced_person_id` can point at a namesake from another era** (the parser matched names across centuries: a 1914-born GP for a councillor who died in 1920). `build.py` only matches by name for unlinked terms, so a wrong link shows up as "replaced member ... is not sitting" on /data-review. Fix the link in `fix_parse_errors.py`, not the check.
- **Office elections are not seats.** A sitting councillor elected Bailie at a by-election (Joseph Leask, May 1844) takes no new term. Detected by `candidacies.role` not being NULL or `councillor`.
- **Party is per candidacy** (the label they won under). A mid-term change of party can't be represented yet; if one turns up, it needs a column on `council_terms`.
- **Party aliases are for spelling and markup only.** Don't merge genuinely different labels (Labour vs Independent Labour vs Socialist, Liberal Democrats vs Scottish Liberal Democrats): they're historical record.
- **Month-precision dates:** when a date can't be found, use the 1st of the month and start the ledger `source` with "Month precision only" (e.g. James Daniel's July 1962 resignation). Leave the row `confirmed=0`.
- **Before trusting a `confirmed` flag, check it against later corrections.** The April 1874 rows were "confirmed" but double-counted both groups.
- **Constituency slugs aren't unique across councils** (10 pairs: `bressay`, `delting-north`, `lerwick-central`, `lerwick-north`, `lerwick-south`, `yell-south`, … — ZCC and SIC wards with the same name). `/constituency/[slug]` renders only one of each pair (the build warns "conflicts with higher priority route"), so the other ward's page is missing and links to it land on the wrong council's ward.
- **SIC by-elections are matched to the ward in their title.** `fix_sic_by_elections.py` fixed ten that had no ward or an out-of-date one. ZCC "Northmavine South County Council By-Election February 1951" is correctly on Northmavine North (David Walker's seat, which Joseph Peterson then held): the wiki title is wrong, not the data.
- **ZCC result tables in the wiki**: check the loser's votes against the majority. Twice (Walls 1935, Whalsay 1938) the wiki gave the loser the majority figure (85, 121) instead of his vote (4, 32). A wiki "unopposed" can hide a contest (Dunrossness North 1922, Yell South 1938), and a whole ward's figures can be another ward's (Walls 1928, Yell North 1938).
- **An elected member who couldn't sit** (ineligible, e.g. James A. Jamieson, Sandness 1932, who held a Council post) goes in `data/not_seated.csv`, with the re-run as a `[voided election re-run]` by-election.
- **Council end dates:** terms use 1975-05-15 for LTC/ZCC abolition, but `history.astro` says the councils ran "until August 1975". Unresolved.

### Site
- **The site is served at the domain root**, so plain `/path` hrefs are fine. Some pages still prefix `import.meta.env.BASE_URL` (now always `/`); harmless.
- **Working pages** (`/data-review`, and any future review pages) pass `noindex` to `Base` and are filtered out of the sitemap in `astro.config.mjs`.
- **Party chart colours** are validated as a set, in stack order, for colour-blind separation in light and dark mode (dataviz skill validator). Labour sits at the base so its seats read directly. To add a party, re-run the validator on the new order rather than picking a colour by eye.
- **Elements created at runtime don't get Astro's scoped styles.** Style them with `:global(...)`.

### Process
- The drift check compares full dumps. After any change to `data/` or a correction script, run `python3 build.py` and commit `shetland.db` with it.
- When a check on /data-review is resolved, fix the ledger or add a correction. Don't add special cases to `build.py`'s checks to make the issue go away.
- Newspaper research: the `/bna` skill (`.claude/skills/bna/`) searches the British Newspaper Archive through James's logged-in Chrome. James often asks for the search plan rather than the search ("what do you propose searching to find X?"). Then give the issues, date ranges, keywords and what to look for, in priority order, without opening the browser.
- BNA findings are saved as evidence in `research/bna/<topic>.md`: one entry per article, with the citation, the BNA viewer link (`image-viewer?issue=BL%2F0000666%2FYYYYMMDD&page=NNNN&article=NNN`), a short quote and the ledger edit it supports. James checks them there.
- Anything a BNA run can't settle, or that needs James's call, goes in `research/bna/open-questions.md` with its options and the ledger row it affects. James answers there in batches.
- Whenever research contradicts the wiki, the notes or a confirmed ledger row, add an entry to `research/corrections-log.md` (what we had, what the sources show, whether the site text is fixed). James uses it to update his own understanding. Flag it in the reply too.

## Known Issues / TODO
- [ ] Work through the 7 `term_issues` on /data-review (planned: a private review page where James records a verdict per issue, keyed by council + kind + date + person, for Claude to turn into ledger edits)
- [ ] SIC by-elections Sep 1993 (Jonathan Wills, Whiteness Weisdale & Tingwall) and Mar 2002 (Joseph G. Simpson, Whalsay & Skerries): wards now set from the titles, and the replaced member is inferred as the sitting member. Confirm who it was.
- [ ] Make constituency slugs unique across councils (see Learnings); needs redirects for any existing URLs that change
- [ ] Set the Cloudflare Pages build command (see Deployment) and switch off GitHub Pages
- [ ] Junk candidacy rows from parsing: `Image:cross.gif` (Lerwick Twageos 1988 by-election), `Unknown` (1722 election, with "James Moodie" in the party column). Fix with a correction script.
- [ ] 420 candidacies unlinked — mostly SIC candidates without person pages (deliberate)
- [ ] 32 person photos missing from MW images directory
- [ ] 11 people with zero candidacies are pre-1707 politicians whose elections aren't in the dataset
- [ ] Some people missing birth/death places — genuinely absent from wiki source, not a parsing bug
- Parser fixes (e.g. the old Westminster party-name issue, now fixed) can no longer go in `parse_wiki.py`: they're corrections on top of the baseline.
