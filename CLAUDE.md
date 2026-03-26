# Shetland Political History

## What This Is

A static site replacing a MediaWiki installation (shetlandhistory.com) that was running MW 1.27 on PHP 7.0, getting hammered by bots at 100% resource usage on shared hosting (cPanel/Namecheap).

The project extracts Shetland political data from the MediaWiki MySQL database into a structured SQLite database, then generates a static site with Astro, deployable to Cloudflare Pages for free.

## Architecture

```
new-site/
├── parse_wiki.py       # Python script: reads MediaWiki MySQL → writes SQLite
├── schema.sql          # SQLite schema definition
├── shetland.db         # Generated SQLite database (output of parse_wiki.py)
└── site/               # Astro static site
    ├── shetland.db     # Copy of DB used at build time (must be copied before build)
    ├── src/
    │   ├── lib/db.ts   # SQLite query layer (read-only, used at build time)
    │   ├── layouts/Base.astro
    │   └── pages/      # Static page generators
    │       ├── index.astro
    │       ├── search.astro
    │       ├── people.astro
    │       ├── constituencies.astro
    │       ├── council/[slug].astro
    │       ├── election/[id].astro
    │       ├── person/[slug].astro
    │       └── constituency/[slug].astro
    └── dist/           # Built static output (~996 pages)
```

## Data Source

Three MediaWiki MySQL dumps exist locally. We use **shetland_history2** (prefix `mwfn_`) — it has the most pages (2,067) and most up-to-date content.

| DB | Prefix | Pages | Revisions | Notes |
|---|---|---|---|---|
| shetland_history | mwni_ | 1,713 | 7,370 | Full edit history, older content |
| **shetland_history2** | **mwfn_** | **2,067** | **3,507** | **Active — newest content** |
| shetland_history3 | mw4x_ | 1,509 | 1,510 | Oldest, minimal |

## Database Schema

- **councils** — Lerwick Town Council, Zetland County Council, SIC, Parliament of GB, Parliament of UK
- **constituencies** — 64 electoral wards/divisions
- **people** — 535 councillors/politicians with intro, biography, birth/death dates, Bayanne ID
- **elections** — 1,240 rows (one per constituency result; multi-constituency elections like County Council generals create multiple rows sharing a wiki_page_title)
- **candidacies** — 2,429 individual candidacy records with votes, party, elected status

Person linkage: 86% of candidacies are linked to person records via wiki links + a redirect map (294 MediaWiki redirects resolved).

## Parser Details (parse_wiki.py)

### How elections are discovered
The parser uses **MediaWiki navigation templates** as the canonical list of elections (ElectionResultsLTC, ElectionResultsZCC, ElectionResultsSIC, ElectionResultsGB, ElectionResultsUK, ElectionResults). Only pages referenced in these templates are imported.

### Key parsing patterns
- Election tables: `{| class="wikitable"` blocks with candidate rows
- Electorate/turnout: `Electorate: 3861<br />\nTurnout: 1696 (43.9%)`
- Person birth/death: `(b. 10 November [[1775]], Lerwick, d. 17 February [[1841]])` — handles year-only and wiki-linked years
- Intro vs Biography: split on `==Biography==` heading; intro has `(b. ..., d. ...)` parenthetical stripped since it's shown structured
- Bayanne IDs: extracted from `personID=I\d+` in external links
- Redirect map: MediaWiki redirects (e.g. "William Jamieson Adie" → "William Adie") resolved for candidate matching

### Known issues
- 351 candidacies remain unlinked (people who stood once without a wiki page)
- Bayanne IDs: 34 mismatches between DB1 and DB2 — neither is fully reliable. DB1 IDs appear more correct based on spot checks. Needs cross-referencing against live Bayanne site.
- UK election parser: some older elections with non-standard markup may have missing party data

## Commands

### Re-run the parser
```bash
cd /Users/james/projects/shetland_history/new-site
python3 parse_wiki.py
```

### Build the site
```bash
cp shetland.db site/shetland.db
cd site
npm run build
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

## Deployment

Not yet deployed. Plan is Cloudflare Pages — just push the `site/dist/` output.

## TODO
- [ ] Cross-reference Bayanne IDs against live Bayanne site to fix the 34 mismatches
- [ ] Import councillor photos from MediaWiki images directory (800MB tar.gz downloading)
- [ ] Deploy to Cloudflare Pages
- [ ] Consider what to do with the 351 unlinked candidacies
