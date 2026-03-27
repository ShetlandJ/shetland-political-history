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
- **elections** — ~1,300 rows (one per constituency result; multi-constituency elections create multiple rows sharing a wiki_page_title). Has `hidden` column for erroneous records.
- **candidacies** — ~2,500 individual candidacy records with votes, party, elected status
- **referenda** — 6 referenda (1975 EEC, 1979 devolution, 1997 devolution x2 questions, 2011 AV, 2014 indyref, 2016 EU)
- **referendum_results** — Vote counts per option per question

Person linkage: ~86% of candidacies are linked to person records via:
1. Wiki link matching (primary)
2. MediaWiki redirect map (294 redirects, e.g. "William Jamieson Adie" → "William Adie")
3. Middle-name / abbreviation matching (e.g. "Thomas M. Y. Manson" → "Thomas Manson (ii)")
4. Name propagation (if same candidate_name is linked in 4 elections but not the 5th, propagate)
5. Dead-person unlinking (18 impossible links removed where person died before election)

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
6. Validate person-candidacy links (unlink dead/unborn)
6a. Manual candidacy corrections
6b. Hide erroneous elections
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
- Notes with by-election references: linkified with fuzzy matching

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
- **Election navigation** (prev/next within same council + type)
- **Client-side search** (full index baked into the page at build time)
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

Not yet deployed. Plan is Cloudflare Pages — just push the `site/dist/` output.

## Known Issues / TODO
- [ ] Deploy to Cloudflare Pages
- [ ] 354 candidacies remain unlinked — mostly SIC candidates without person pages (deliberate) and party names being parsed as candidates in Westminster elections
- [ ] 32 person photos missing from MW images directory
- [ ] 11 people with zero candidacies are pre-1707 politicians whose elections aren't in the dataset
- [ ] Westminster election parser: party names (Labour, Conservative, etc.) sometimes parsed as candidate names
- [ ] Some people missing birth/death places — genuinely absent from wiki source, not a parsing bug
