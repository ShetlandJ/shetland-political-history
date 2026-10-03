---
name: bna
description: Work the research queue ("/bna next") or look something up in the British Newspaper Archive (mainly the Shetland Times) through James's logged-in Chrome, and report back cited findings for the council ledger, correction scripts or person biographies. Trigger on "/bna <question>", "check the BNA for", "search the Shetland Times for", or a request to confirm a council date, vacancy, co-option, retiring list or a councillor's life details from newspapers.
---

# /bna: cited newspaper lookups

James is logged in to britishnewspaperarchive.com in Chrome. You drive that session with the
Claude in Chrome tools to answer a bounded research question, and report findings with exact
citations. You **propose** ledger/correction edits; you don't make them unless James says so
(queue items are pre-authorised).

The order of work is set by cost. Always try the cheaper step first:

1. **Search-result snippets**: text only, cheap. Often enough on their own.
2. **Article OCR** from the OCR endpoint (section 3): text, medium.
3. **Zoomed screenshot** of the article: images, expensive. Last resort.

For "what did the council do across these months" questions, skip search: list each issue's
articles from its manifest and read the council report's OCR directly (section 3).

## Reference files

Read the one that fits the question before planning; they live next to this file
(`.claude/skills/bna/`):

- `queue.md`: the `/bna next` workflow (apply answers, edit, rebuild, tick, commit).
- `ltc-timing.md`: Lerwick Town Council meeting days, retiring lists, co-options, election days.
- `zcc-timing.md`: Zetland County Council meeting days, appointments, by-elections, nominations.
- `biographies.md`: person research: death notices, obituaries, nominations, letters, checks,
  and how a biography is written into `fix_newspapers.py`.
- `viewer.md`: reading through the BNA viewer's Articles panel, when the endpoints aren't enough.

## Setup

- Load every browser tool you need in one ToolSearch call:
  `select:mcp__claude-in-chrome__tabs_context_mcp,mcp__claude-in-chrome__navigate,mcp__claude-in-chrome__computer,mcp__claude-in-chrome__find,mcp__claude-in-chrome__javascript_tool,mcp__claude-in-chrome__browser_batch,mcp__claude-in-chrome__tabs_close_mcp`
- `tabs_context_mcp` with `createIfEmpty: true`, and use the tab it gives you. Close it at the end.
- Use `browser_batch` whenever you can predict two or more steps (navigate + wait + extract).
- If a page shows a login or subscribe wall, stop and tell James. Never enter credentials.
- Before searching, check `data/citations.csv` and `data/searches.csv`: the article may already be
  on file, or the search already run and found nothing.

## Two ways James uses this

- **Run a search**: "/bna when was Williamson co-opted in 1941?" Do the lookup and report.
- **Propose a search**: "what do you propose searching to find X?" James often wants the plan,
  not the lookup, because he searches himself. Answer with the search plan only (issues and
  date ranges in priority order, keywords, what to look for in each, and why), using the timing
  files and what the ledger already tells you. Don't open the browser unless he asks.
- **`/bna next`**: work the queue; follow `queue.md`.

## 1. Plan the searches

Turn the question into one or more narrow searches: keywords plus a tight date range. Narrow
dates beat clever keywords, because the OCR is patchy (names split: "Sin clair", "Halerow").

Search URL (all parameters matter):

```
https://www.britishnewspaperarchive.com/search-newspapers/results?keywords=<kw>&newspaper=shetland%20times&startdate=YYYY-MM-DD&enddate=YYYY-MM-DD&exactdate=true&o=date&d=asc
```

`o=date&d=asc` gives oldest first.

**Coverage.** The Shetland Times in the BNA ends with 2000 (manifests 404 from 2001). It has gaps:
nothing 24 Apr–12 Jun 1915, 9 Feb 1918, or mid-May–June 1932. The Shetland News covers the 1915
gap, with Town and County reports on p4 or p8. If a week returns 0 results, try
`newspaper=shetland%20news` (viewer code `0003210` instead of `0000666`).

**Publication day.** For a single issue, set startdate = enddate = the publication day:
**Monday to 15 Mar 1875**, Saturday from 20 Mar 1875 to at least Feb 1943, **Friday by Oct 1944**.

Keywords are ANDed, and a long phrase often returns nothing ("town council election tuesday"
found 0 in a week that had the answer). Use one or two words and filter the snippets in JS
instead. A quoted name (`"john m. laurenson"`) is the exception: it narrows well. Only 12
results show per page, so a common word over a two-week range (`tuesday`: 60 hits) hides most of
them: narrow the dates or add a second word (`town election` worked well for 1953–64).

## 2. Read the snippets

Navigate, wait about **6–8 seconds** (results render late, and at 3–4 seconds the page is often
still empty; `get_page_text` fails on this page), then read the results. Best: this helper pairs
each result with its issue, page and article id, so you get the citation and the snippet together.
Page navigation clears `window`, so define it in every batch that navigates:

```js
window._rs = () => { const seen = new Set(); const out = [];
  for (const a of document.querySelectorAll('a[href*="viewer"]')) {
    const m = a.getAttribute('href').match(/(\d{8}).page.(\d+).article.(\d+)/);
    if (!m || seen.has(m[0])) continue; seen.add(m[0]);
    let el = a; while (el && !/Added on/.test(el.innerText)) el = el.parentElement;
    const txt = el ? el.innerText.replace(/Lerwick, Shetland, Scotland[\s\S]*/, '').replace(/\s+/g, ' ').slice(0, 200) : '';
    out.push(m[1] + ' p' + m[2] + ' a' + m[3] + ': ' + txt); }
  return out.join('\n').replace(/[?&=]/g, ' '); };
document.body.innerText.match(/\d+.\d+ of \d+ results/) + '\n' +
  _rs().split('\n').filter(s => /tuesday|poll|elect/i.test(s)).join('\n').slice(0, 1400)
```

A `null` count means the page hadn't rendered yet (or there were no results): wait and re-run.

Tool output is **truncated at about 1,000–1,500 characters**, so read long output in chunks.
The `.replace(/[?&=]/g, ' ')` stops the tool from blocking output that looks like query strings.

Each result gives: headline, the first ~300 characters of OCR, date, page. If a snippet answers
the question (it did for the Jan 1941 Williamson co-option), stop here and cite it.

## 3. Read the article OCR

From any britishnewspaperarchive.com tab (the fetches use James's session), two same-origin
endpoints give you the text directly. `code` is `0000666` for the Shetland Times, `0003210` for
the Shetland News.

- **Issue manifest**: `/titan/marshal/obscura/api/manifest/{code}/YYYYMMDD` lists every article
  in the issue: `structures[]` with `@id` ending `artNNNN`, `label` (the OCR headline) and
  `canvases[0]`, whose `_NNNN.jp2` is the page. **A 404 means the issue isn't digitised**, which
  proves a gap. Use it to find a council report without searching, and to see all its sub-headings.
- **Article OCR**: `/titan/marshal/obscura/api/ocr/lines/BL/{code}/YYYYMMDD/NNN/PPPP` (3-digit
  article, 4-digit page) returns JSON lines; join `LineText`.

```js
window._ocr = async (ds, art, page, code='0000666') => { const r = await fetch(`/titan/marshal/obscura/api/ocr/lines/BL/${code}/${ds}/${String(art).padStart(3,'0')}/${String(page).padStart(4,'0')}`); if(!r.ok) return '[ocr '+r.status+']'; return (await r.json()).map(x=>x.LineText).join(' ').replace(/-\s+(?=[a-z])/g,'').replace(/\s+/g,' ').trim(); };
window._arts = async (ds, code='0000666') => { const r = await fetch(`/titan/marshal/obscura/api/manifest/${code}/${ds}`); if(!r.ok) return null; return (await r.json()).structures.map(s=>({a:+(s['@id'].match(/art(\d+)/)||[])[1], l:String(s.label||'').trim(), p:+((s.canvases||[''])[0].match(/_(\d{4})\.jp2/)||[])[1]})); };
```

The search results page can't be fetched this way (it's built in the browser); search still
needs navigate + wait.

**Finding council reports in a manifest.** A report is a heading article followed by sub-heading
articles with consecutive ids on the same page (ALL-CAPS labels: "FINANCIAL", "CHIEF CONSTABLE'S
REPORT"). It ends at a village heading (WHALSAY, SANDWICK), a church or school report, casualty
news or shipping news. Headline OCR is poor: match loosely, e.g.
`/(town|county)\s*c\S{0,3}n\S{0,2}l|[lz]etland count/i` (seen: "Ceuncil", "Couneil", "Gouneil",
"Co;mcil", "Connc", "Letland County"), and exclude other burghs ("Leith", "Wick", "Finchley") and
p1 notices. In 1915 the County report is headed "Meeting of County Council". Now and then the
heading is merged into the article before it ("Local and District News ... Zetland County
Council"): `search(/monthly meeting/i)` in that article's text.

**Getting long text out.** Tool output is capped at about 1,000 characters per call, but each
`browser_batch` item gets its own cap. Keep the text in `window`, and read it with a batch of
10–15 `javascript_tool` calls that each return the next 950-character slice. Any script that runs
over about 45 seconds times out the call: start long loops without awaiting them, write results
to `window` or `localStorage` (which survives navigation), and check back. Don't try other ways
out: posting to a local server brings up a Chrome local-network prompt, and clipboard copies made
with simulated key presses never reach the Mac.

## 4. Screenshot (last resort)

Only when the OCR is too garbled to trust on the point that matters (a name, a number, a date),
or when layout matters (a table). Never screenshot full pages to read text.

**Crop the page image directly** (works where the viewer stays black). The OCR lines endpoint
gives each line's pixel box (`XTopLeft`, `YTopLeft`, `XBottomRight`, `YBottomRight`), so find the
lines you need, then navigate the tab to the IIIF image service for just that region and take one
`screenshot` at `scale: 0.5–0.6`:

```
https://www.britishnewspaperarchive.com/titan/marshal/obscura/api/image/0000666%2fYYYY%2fMMDD%2f0000666_YYYYMMDD_PPPP.jp2/x,y,w,h/full/0/default.jpg
```

A region of about 700 × 400–700 pixels holds a results table. For a single damaged digit, ask for a
small region scaled up (`.../x,y,200,50/800,/0/default.jpg`). The image page is same-origin, so the
OCR fetches still work from it. Lines in a multi-column article sometimes span columns (1910): find
the column from the lines that name a candidate. A column that runs into the binding can't be read;
try the Shetland News for the same week. The viewer (`viewer.md`) is the fallback.

## Record what you find

When findings are going into the repo (always for queue items and biographies):

- **Evidence** in `research/bna/<topic>.md`: one entry per article, with the citation, the viewer
  link `https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2FYYYYMMDD&page=NNNN&article=NNN`,
  a short quote, and the edit it supports. `ltc-1912-1914.md` is the model.
- `data/citations.csv`: one row per article, id `st-YYYYMMDD-pN-aNNN` (`sn-` for the Shetland
  News; `st-YYYYMMDD` for a whole issue; `mb-pN` for a minute-book page). `summary` is the short
  quote or paraphrase; `evidence_file` the research file. `build.py` builds the citation text and
  BNA link from the parts, and fails if the id doesn't match them or the date isn't a
  publication day.
- `data/citation_links.csv`: one row per fact the article supports. `target_type` is `term`
  (`person_slug@start_date` of the ledger row), `election` (the election id, or its exact
  `wiki_page_title` for an election a correction script creates) or `person` (`slug:field`, field
  one of `born_date`, `died_date`, `birth_place`, `death_place`, `intro`, `biography`). `basis` is
  `read` when the paper states the fact, `inferred` when it's worked out from it, with a `note`
  saying how.
- `data/searches.csv`: every search that found nothing useful, with keywords, date range, the
  question and what came back.
- Anything that contradicts the wiki, CLAUDE.md or a confirmed ledger row goes in
  `research/corrections-log.md` and is flagged in the reply.
- If the evidence is ambiguous on the point that decides an edit, don't guess. Leave the row
  alone, record it in the evidence file, and add it to `research/bna/open-questions.md` (date
  raised, the question, the options, the evidence file, the row). James answers there in batches.

## Reporting

For each question, report:

- **Answer**, in a sentence.
- **Citation**: Shetland Times, Saturday D Month YYYY, page N (article NNN), plus the meeting
  date if it's a meeting report ("meeting of Tuesday 7 Jan 1941").
- **Evidence**: a short quote (under 15 words) or a paraphrase. Don't reproduce whole articles.
- **What it changes**: the ledger row (`data/ltc_terms.csv`) or correction (`fix_newspapers.py`)
  you'd edit, and how, with the source string you'd write. Look people up by `person_slug` /
  `person_id`, never by name.
- **Anything ambiguous**, flagged for James rather than resolved by you.

For a sweep (for example the retiring notices for every year 1885–1975), write findings to a
scratch file as you go and give James a summary at the end, not a running commentary.

## Limits

- Targeted lookups at a normal reading pace, in James's session, for his research. No bulk
  downloading, no scraping loops over hundreds of pages.
- A manifest sweep (contents lists only) is fine for a question that covers a date range, such as
  every meeting in 1914–18 (227 issues). Keep it to that range, pace it at about one request a
  second, and fetch OCR only for the articles you need.
- Keep quotes short in anything you write back to the repo or the chat.
- After 2–3 searches that turn up nothing, report what you tried (keywords, date ranges) and
  suggest where to look next, rather than widening endlessly.
